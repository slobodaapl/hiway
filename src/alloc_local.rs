use core::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};
use std::{
    any::Any,
    collections::{HashMap, VecDeque},
    sync::Arc,
    vec::Vec,
};

use tokio::sync::Notify;

use crate::{
    grant::{Grant, GrantNode, Lease, Limits, Permission, Rights, StreamLimits},
    synchronization::{AtomicBool, AtomicUsize, Mutex, MutexGuard, Ordering, TryLockError},
    CloseReason, EventId, EventPort, EventReceiver, EventSender, EventSpec, OwnedPortBinding, Port,
    ReceiveError, SendError, StreamItem, SubscriptionRole, TopicError, TopicTypeMismatch,
    TrySendError,
};

/// Physical bounds for one retained stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamConfig {
    /// Maximum retained payload items.
    pub capacity: usize,
    /// Maximum live subscriptions, excluding receiver-handle clones.
    pub subscribers: usize,
    /// Maximum pending send and receive operations.
    pub waiters: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            capacity: 64,
            subscribers: 16,
            waiters: 32,
        }
    }
}

pub(crate) struct Scope {
    streams: Mutex<HashMap<EventId, Arc<dyn Maintenance>>>,
}

trait Maintenance: Any + Send + Sync {
    fn maintain(&self);
}

/// Administrative owner of a scoped set of streams.
///
/// Components bind against derived grants, not this owner. Stream allocation
/// and child allowances both reserve resources from the owner's limits.
pub struct DynamicFabric {
    root: Grant,
}

impl Default for DynamicFabric {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicFabric {
    /// Creates an empty scope with the default finite allowance.
    ///
    /// # Panics
    ///
    /// Panics if the default allowance is invalid.
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(Limits::default()).expect("default limits are valid")
    }

    /// Creates an empty scope with explicit finite allowances.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError::InvalidConfig`] if an allowance exceeds the
    /// addressable allocation limit.
    pub fn with_limits(limits: Limits) -> Result<Self, TopicError> {
        limits.validate()?;
        let scope = Arc::new(Scope {
            streams: Mutex::new(HashMap::new()),
        });
        Ok(Self {
            root: Grant::root(scope, limits),
        })
    }

    /// Declares a typed stream. An identical declaration is idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError::Capacity`] if the owner cannot reserve the stream
    /// or allocate its storage. Rejects zero capacity, unaddressable bounds,
    /// incompatible declarations, and reopening a closed stream. Reusing an
    /// identity with another payload type returns [`TopicError::TypeMismatch`].
    ///
    /// # Panics
    ///
    /// Panics if an internal panic poisoned the registry or stream mutex.
    pub fn create_stream<E>(&self, config: StreamConfig) -> Result<(), TopicError>
    where
        E: EventSpec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        let allowance = Limits {
            streams: 1,
            retained_items: config.capacity,
            subscriptions: config.subscribers,
            waiters: config.waiters,
            ..Limits::ZERO
        };
        allowance.validate()?;
        if config.capacity == 0 {
            return Err(TopicError::InvalidConfig);
        }
        let check_existing = |existing: Arc<dyn Maintenance>| {
            let erased: &dyn Any = existing.as_ref();
            let stream = erased
                .downcast_ref::<Stream<E::Payload>>()
                .ok_or(TopicError::TypeMismatch(TopicTypeMismatch { id: E::ID }))?;
            if stream.config == config
                && stream.wire_major == E::WIRE_MAJOR
                && !stream.lock().closed
            {
                Ok(())
            } else {
                Err(TopicError::InvalidConfig)
            }
        };
        let existing = self.root.scope.streams.lock().unwrap().get(&E::ID).cloned();
        if let Some(existing) = existing {
            return check_existing(existing);
        }
        let allocation = self.root.try_charge(allowance)?;
        let mut entries: VecDeque<Entry<E::Payload>> = VecDeque::new();
        entries
            .try_reserve_exact(config.capacity)
            .map_err(|_| TopicError::Capacity)?;
        let mut subscribers = Vec::new();
        subscribers
            .try_reserve_exact(config.subscribers)
            .map_err(|_| TopicError::Capacity)?;
        subscribers.resize_with(config.subscribers, || None);
        let stream = Arc::new(Stream {
            config,
            wire_major: E::WIRE_MAJOR,
            state: Mutex::new(State {
                entries,
                subscribers,
                tail: 0,
                next_cursor: 0,
                closed: false,
                needs_room: false,
            }),
            changed: Arc::new(Notify::new()),
            contended: AtomicBool::new(false),
            deferred: AtomicBool::new(false),
            waiters: AtomicUsize::new(0),
            _allocation: allocation,
        });
        let mut streams = self.root.scope.streams.lock().unwrap();
        if let Some(existing) = streams.get(&E::ID).cloned() {
            drop(streams);
            return check_existing(existing);
        }
        streams.try_reserve(1).map_err(|_| TopicError::Capacity)?;
        streams.insert(E::ID, stream);
        Ok(())
    }

    /// Closes a declared stream and releases its retained payloads.
    /// Existing endpoints stay closed; declaring the identity again cannot reopen it.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError`] if the identity is unknown, its declaration is
    /// incompatible, or a notification registration cannot be reserved.
    ///
    /// # Panics
    ///
    /// Panics if an internal panic poisoned the registry or stream mutex.
    pub fn close_stream<E>(&self) -> Result<(), TopicError>
    where
        E: EventSpec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        let stream = self.root.stream::<E>(Rights::PUBLISH)?;
        let retired = {
            let mut state = stream.lock();
            state.closed = true;
            state.dirty = true;
            (
                core::mem::take(&mut state.entries),
                core::mem::take(&mut state.subscribers),
            )
        };
        drop(retired);
        Ok(())
    }

    /// Reserves an attenuated grant for a component.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError::Denied`] for unknown events. Invalid allowances
    /// return [`TopicError::InvalidConfig`]; insufficient owner capacity or
    /// permission storage returns [`TopicError::Capacity`].
    ///
    /// # Panics
    ///
    /// Panics if an internal panic poisoned an administration mutex.
    pub fn grant(&self, permissions: &[Permission], limits: Limits) -> Result<Grant, TopicError> {
        let streams = self.root.scope.streams.lock().unwrap();
        if permissions
            .iter()
            .any(|permission| !streams.contains_key(&permission.event))
        {
            return Err(TopicError::Denied);
        }
        drop(streams);
        self.root.restrict(permissions, limits)
    }

    /// Returns physical stream reservations plus outstanding child allowances.
    #[must_use]
    pub fn usage(&self) -> Limits {
        self.root.usage()
    }

    /// Reclaims consumed storage and dispatches deferred wakeups for this scope.
    /// Call from a trusted application loop when using strict `try_*` operations.
    /// This method may allocate, wait for administration locks, and run callbacks.
    /// Work visits at most the configured streams, retained slots, and grant nodes.
    ///
    /// # Panics
    /// Panics if an administration mutex was poisoned.
    pub fn maintain(&self) {
        let streams: Vec<_> = self
            .root
            .scope
            .streams
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for stream in streams {
            stream.maintain();
        }
        self.root.maintain_tree();
    }
}

impl Grant {
    fn stream<E>(&self, rights: Rights) -> Result<Arc<Stream<E::Payload>>, TopicError>
    where
        E: EventSpec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        self.check(E::ID, rights)?;
        let stream = self
            .scope
            .streams
            .lock()
            .unwrap()
            .get(&E::ID)
            .cloned()
            .ok_or(TopicError::Denied)?;
        let stream: Arc<dyn Any + Send + Sync> = stream;
        let stream = stream
            .downcast::<Stream<E::Payload>>()
            .map_err(|_| TopicError::TypeMismatch(TopicTypeMismatch { id: E::ID }))?;
        if stream.wire_major != E::WIRE_MAJOR {
            return Err(TopicError::InvalidConfig);
        }
        self.register_stream(E::ID, &stream.changed)?;
        Ok(stream)
    }

    /// Resolves the publication capability without creating a stream.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError`] for missing publication rights, revocation, an
    /// unknown or incompatible declaration, exhausted notification slots, or a
    /// zero event-item allowance.
    ///
    /// # Panics
    ///
    /// Panics if an internal panic poisoned an administration mutex.
    pub fn sender<E>(&self) -> Result<DynamicSender<E>, TopicError>
    where
        E: EventSpec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        let stream = self.stream::<E>(Rights::PUBLISH)?;
        if self.stream_limits::<E>()?.retained_items == 0 {
            return Err(TopicError::Capacity);
        }
        Ok(DynamicSender {
            stream,
            grant: self.clone(),
            marker: PhantomData,
        })
    }

    /// Acquires a subscription at the current tail. Clones share its cursor.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError`] for missing role rights, revocation, a closed or
    /// incompatible stream, or exhausted subscription/notification capacity.
    /// Required membership needs both observation and required-receiver rights.
    ///
    /// # Panics
    ///
    /// Panics if an internal panic poisoned an administration or stream mutex.
    pub fn subscribe<E>(&self, role: SubscriptionRole) -> Result<DynamicReceiver<E>, TopicError>
    where
        E: EventSpec + 'static,
        E::Payload: Send + Sync + 'static,
    {
        let rights = match role {
            SubscriptionRole::Observer => Rights::OBSERVE,
            SubscriptionRole::Required => Rights::OBSERVE.union(Rights::REQUIRED),
        };
        let stream = self.stream::<E>(rights)?;
        let lease = self.try_charge_stream(
            E::ID,
            StreamLimits {
                subscriptions: 1,
                ..StreamLimits::ZERO
            },
        )?;
        let operation = self.enter(E::ID, rights)?;
        let key = {
            let mut state = stream.lock();
            if state.closed {
                return Err(TopicError::Denied);
            }
            let index = state
                .subscribers
                .iter()
                .position(Option::is_none)
                .ok_or(TopicError::Capacity)?;
            let id = state.next_cursor;
            let next_id = id.checked_add(1).ok_or(TopicError::Capacity)?;
            let next = state.tail;
            state.next_cursor = next_id;
            state.subscribers[index] = Some(Cursor {
                id,
                next,
                role,
                grant: self.node.clone(),
            });
            state.dirty = true;
            CursorKey { index, id }
        };
        drop(operation);
        Ok(DynamicReceiver {
            inner: Arc::new(ReceiverInner {
                stream,
                grant: self.clone(),
                key,
                _lease: lease,
            }),
            marker: PhantomData,
        })
    }
}

impl<E> OwnedPortBinding<E> for Grant
where
    E: EventSpec + 'static,
    E::Payload: Send + Sync + 'static,
{
    type Sender = DynamicSender<E>;
    type Receiver = DynamicReceiver<E>;

    fn sender_owned(&self) -> Result<Self::Sender, TopicError> {
        self.sender::<E>()
    }

    fn subscribe_owned(&self, role: SubscriptionRole) -> Result<Self::Receiver, TopicError> {
        self.subscribe::<E>(role)
    }
}

struct Entry<T> {
    sequence: u64,
    value: Arc<T>,
    lease: Lease,
}

type ReservedPayload<T> = (Arc<T>, Lease);

#[derive(Clone, Copy)]
struct CursorKey {
    index: usize,
    id: u64,
}

struct Cursor {
    id: u64,
    next: u64,
    role: SubscriptionRole,
    grant: Arc<GrantNode>,
}

struct State<T> {
    entries: VecDeque<Entry<T>>,
    subscribers: Vec<Option<Cursor>>,
    tail: u64,
    next_cursor: u64,
    closed: bool,
    needs_room: bool,
}

impl<T> State<T> {
    fn required_needs(&self, sequence: u64) -> bool {
        self.subscribers.iter().flatten().any(|cursor| {
            cursor.role == SubscriptionRole::Required
                && cursor.next <= sequence
                && !cursor.grant.is_revoked()
        })
    }

    fn consumed(&self, sequence: u64) -> bool {
        self.subscribers
            .iter()
            .flatten()
            .all(|cursor| cursor.next > sequence || cursor.grant.is_revoked())
    }
}

struct Stream<T> {
    config: StreamConfig,
    wire_major: crate::WireMajor,
    state: Mutex<State<T>>,
    changed: Arc<Notify>,
    contended: AtomicBool,
    deferred: AtomicBool,
    waiters: AtomicUsize,
    _allocation: Lease,
}

impl<T> Stream<T> {
    fn lock(&self) -> StateGuard<'_, T> {
        StateGuard {
            stream: self,
            guard: Some(self.state.lock().unwrap()),
            dirty: false,
            notify: true,
        }
    }

    fn try_lock(&self) -> Result<StateGuard<'_, T>, ()> {
        let guard = match self.state.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.contended.store(true, Ordering::SeqCst);
                match self.state.try_lock() {
                    Ok(guard) => guard,
                    Err(TryLockError::Poisoned(error)) => error.into_inner(),
                    Err(TryLockError::WouldBlock) => return Err(()),
                }
            }
        };
        Ok(StateGuard {
            stream: self,
            guard: Some(guard),
            dirty: false,
            notify: true,
        })
    }

    fn try_lock_deferred(&self) -> Result<StateGuard<'_, T>, ()> {
        let mut state = self.try_lock()?;
        state.notify = false;
        Ok(state)
    }

    fn maintain(&self) {
        self.retire_consumed();
        let retired = self.try_lock().ok().and_then(|mut state| {
            if state.needs_room
                && state.entries.len() == self.config.capacity
                && state
                    .entries
                    .front()
                    .is_some_and(|entry| !state.required_needs(entry.sequence))
            {
                state.needs_room = false;
                state.dirty = true;
                state.entries.pop_front()
            } else {
                state.needs_room = false;
                None
            }
        });
        drop(retired);
        if self.deferred.swap(false, Ordering::AcqRel) {
            self.changed.notify_waiters();
        }
    }

    fn waiter<'a>(&'a self, grant: &Grant, event: EventId) -> Result<Waiter<'a, T>, TopicError> {
        let lease = grant.try_charge_stream(
            event,
            StreamLimits {
                waiters: 1,
                ..StreamLimits::ZERO
            },
        )?;
        self.waiters
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.config.waiters).then_some(current + 1)
            })
            .map_err(|_| TopicError::Capacity)?;
        Ok(Waiter {
            stream: self,
            _lease: lease,
        })
    }

    fn retire_consumed(&self) {
        for _ in 0..self.config.capacity {
            let retired = {
                let Ok(mut state) = self.try_lock() else {
                    return;
                };
                if !state
                    .entries
                    .front()
                    .is_some_and(|entry| state.consumed(entry.sequence))
                {
                    return;
                }
                state.dirty = true;
                state.entries.pop_front()
            };
            drop(retired);
        }
    }
}

struct StateGuard<'a, T> {
    stream: &'a Stream<T>,
    guard: Option<MutexGuard<'a, State<T>>>,
    dirty: bool,
    notify: bool,
}

impl<T> Deref for StateGuard<'_, T> {
    type Target = State<T>;

    fn deref(&self) -> &State<T> {
        self.guard.as_ref().unwrap()
    }
}

impl<T> DerefMut for StateGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut State<T> {
        self.guard.as_mut().unwrap()
    }
}

impl<T> Drop for StateGuard<'_, T> {
    fn drop(&mut self) {
        drop(self.guard.take());
        let dirty = self.stream.contended.swap(false, Ordering::SeqCst) || self.dirty;
        if !self.notify {
            if dirty {
                self.stream.deferred.store(true, Ordering::Release);
            }
        } else if self.stream.deferred.swap(false, Ordering::AcqRel) || dirty {
            self.stream.changed.notify_waiters();
        }
    }
}

impl<T: Send + Sync + 'static> Maintenance for Stream<T> {
    fn maintain(&self) {
        self.maintain();
    }
}

struct Waiter<'a, T> {
    stream: &'a Stream<T>,
    _lease: Lease,
}

impl<T> Drop for Waiter<'_, T> {
    fn drop(&mut self) {
        self.stream.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// An owned, scoped publisher. Clones share its grant's allowance.
pub struct DynamicSender<E: EventSpec> {
    stream: Arc<Stream<E::Payload>>,
    grant: Grant,
    marker: PhantomData<fn() -> E>,
}

impl<E: EventSpec> Clone for DynamicSender<E> {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            grant: self.grant.clone(),
            marker: PhantomData,
        }
    }
}

impl<E> DynamicSender<E>
where
    E: EventSpec + 'static,
    E::Payload: Send + Sync + 'static,
{
    /// Prepares owned storage and charges this publisher's event allowance.
    /// Preparation may allocate, reclaim eligible storage, and dispatch wakeups.
    ///
    /// # Errors
    /// Returns the original payload if capacity, authority, or lifecycle prevents preparation.
    pub fn prepare(
        &self,
        value: E::Payload,
    ) -> Result<PreparedPublication<'_, E>, TrySendError<E::Payload>> {
        let (value, lease) = self.reserve(value)?;
        Ok(PreparedPublication {
            sender: self,
            value: Arc::new(value),
            lease,
        })
    }

    fn reserve(&self, value: E::Payload) -> Result<(E::Payload, Lease), TrySendError<E::Payload>> {
        // Capacity rejection can park a future too. Validate through the cutoff
        // gate, then leave it before reclamation can run application destructors.
        let operation = match self.grant.enter(E::ID, Rights::PUBLISH) {
            Ok(operation) => operation,
            Err(TopicError::Contended | TopicError::Capacity) => {
                return Err(TrySendError::Contended(value));
            }
            Err(_) => return Err(TrySendError::Revoked(value)),
        };
        drop(operation);
        for _ in 0..=self.stream.config.capacity {
            if self.grant.is_revoked() {
                return Err(TrySendError::Revoked(value));
            }
            {
                let Ok(state) = self.stream.try_lock() else {
                    return Err(TrySendError::Contended(value));
                };
                if state.closed {
                    return Err(TrySendError::Closed(value));
                }
                if state.tail == u64::MAX {
                    return Err(TrySendError::SequenceExhausted(value));
                }
                if state.entries.len() == self.stream.config.capacity
                    && state
                        .entries
                        .front()
                        .is_some_and(|entry| state.required_needs(entry.sequence))
                {
                    return Err(TrySendError::Full(value));
                }
            }
            match self.grant.try_charge_stream(
                E::ID,
                StreamLimits {
                    retained_items: 1,
                    ..StreamLimits::ZERO
                },
            ) {
                Ok(lease) => return Ok((value, lease)),
                Err(TopicError::Revoked) => return Err(TrySendError::Revoked(value)),
                Err(_) => {
                    let retired = {
                        let Ok(mut state) = self.stream.try_lock() else {
                            return Err(TrySendError::Contended(value));
                        };
                        if state.closed {
                            return Err(TrySendError::Closed(value));
                        }
                        if state
                            .entries
                            .iter()
                            .find(|entry| entry.lease.belongs_to(&self.grant.node))
                            .is_none_or(|entry| state.required_needs(entry.sequence))
                        {
                            return Err(TrySendError::Full(value));
                        }
                        state.dirty = true;
                        state.entries.pop_front()
                    };
                    drop(retired);
                }
            }
        }
        Err(TrySendError::Full(value))
    }

    fn commit(
        &self,
        value: Arc<E::Payload>,
        lease: Lease,
        deferred: bool,
    ) -> Result<(), TrySendError<ReservedPayload<E::Payload>>> {
        let entered = if deferred {
            self.grant.enter_deferred(E::ID, Rights::PUBLISH)
        } else {
            self.grant.enter(E::ID, Rights::PUBLISH)
        };
        let operation = match entered {
            Ok(operation) => operation,
            Err(TopicError::Contended | TopicError::Capacity) => {
                return Err(TrySendError::Contended((value, lease)))
            }
            Err(_) => return Err(TrySendError::Revoked((value, lease))),
        };
        let retired = {
            let locked = if deferred {
                self.stream.try_lock_deferred()
            } else {
                self.stream.try_lock()
            };
            let Ok(mut state) = locked else {
                return Err(TrySendError::Contended((value, lease)));
            };
            if state.closed {
                return Err(TrySendError::Closed((value, lease)));
            }
            let sequence = state.tail;
            let Some(next) = sequence.checked_add(1) else {
                return Err(TrySendError::SequenceExhausted((value, lease)));
            };
            let retired = if state.entries.len() == self.stream.config.capacity {
                if state
                    .entries
                    .front()
                    .is_some_and(|entry| state.required_needs(entry.sequence))
                {
                    return Err(TrySendError::Full((value, lease)));
                }
                if deferred {
                    state.needs_room = true;
                    return Err(TrySendError::MaintenanceRequired((value, lease)));
                }
                state.entries.pop_front()
            } else {
                None
            };
            state.entries.push_back(Entry {
                sequence,
                value,
                lease,
            });
            state.tail = next;
            state.dirty = true;
            retired
        };
        drop(operation);
        drop(retired);
        Ok(())
    }

    /// Attempts admission, including allocation, reclamation, and wake callbacks.
    /// Every rejection returns the original, unaccepted payload.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] when required retention or the publisher's
    /// quota prevents admission, or [`TrySendError::Contended`] when stream
    /// state is busy. Closure, revocation, and sequence exhaustion are terminal.
    pub fn send_now(&self, value: E::Payload) -> Result<(), TrySendError<E::Payload>> {
        let (value, lease) = self.reserve(value)?;
        self.commit(Arc::new(value), lease, false).map_err(|error| {
            error.map(|(value, lease)| {
                let value = recover_payload(value);
                drop(lease);
                value
            })
        })
    }

    /// Waits for local acceptance. No remote or processing acknowledgement is implied.
    ///
    /// # Errors
    ///
    /// Returns the unaccepted payload in [`SendError`] on closure, revocation,
    /// sequence exhaustion, or unavailable waiter capacity.
    pub async fn send(&self, value: E::Payload) -> Result<(), SendError<E::Payload>> {
        let mut value = value;
        let mut waiter = None;
        loop {
            let changed = self.stream.changed.notified();
            let mut changed = core::pin::pin!(changed);
            if waiter.is_some() {
                changed.as_mut().enable();
            }
            match self.send_now(value) {
                Ok(()) => return Ok(()),
                Err(
                    TrySendError::Full(rejected)
                    | TrySendError::Contended(rejected)
                    | TrySendError::MaintenanceRequired(rejected),
                ) => {
                    value = rejected;
                }
                Err(TrySendError::Revoked(value)) => return Err(SendError::Revoked(value)),
                Err(TrySendError::SequenceExhausted(value)) => {
                    return Err(SendError::SequenceExhausted(value))
                }
                Err(TrySendError::Closed(value)) => return Err(SendError::Closed(value)),
                Err(TrySendError::WaitersFull(value)) => return Err(SendError::WaitersFull(value)),
            }
            if waiter.is_none() {
                waiter = Some(match self.stream.waiter(&self.grant, E::ID) {
                    Ok(waiter) => waiter,
                    Err(TopicError::Revoked) => return Err(SendError::Revoked(value)),
                    Err(_) => return Err(SendError::WaitersFull(value)),
                });
                continue;
            }
            changed.await;
        }
    }
}

impl<E: EventSpec> Port for DynamicSender<E> {}

/// Prepared storage charged to one scoped sender. Holding it retains only that sender's credit.
/// Dropping or recovering it cancels preparation and may run cleanup callbacks.
#[must_use = "prepared payloads are not admitted until try_send succeeds"]
pub struct PreparedPublication<'a, E: EventSpec> {
    sender: &'a DynamicSender<E>,
    value: Arc<E::Payload>,
    lease: Lease,
}

impl<E> PreparedPublication<'_, E>
where
    E: EventSpec + 'static,
    E::Payload: Send + Sync + 'static,
{
    /// Admits without allocation, destruction, or wake callbacks.
    /// Call [`DynamicFabric::maintain`] from the owner's application loop for progress.
    ///
    /// # Errors
    /// Rejection returns this preparation unchanged. `MaintenanceRequired` means
    /// an eligible retained entry must be reclaimed before retrying.
    pub fn try_send(self) -> Result<(), TrySendError<Self>> {
        let Self {
            sender,
            value,
            lease,
        } = self;
        sender.commit(value, lease, true).map_err(|error| {
            error.map(|(value, lease)| Self {
                sender,
                value,
                lease,
            })
        })
    }

    /// Cancels preparation and returns the unaccepted payload.
    #[must_use]
    pub fn into_inner(self) -> E::Payload {
        let value = recover_payload(self.value);
        drop(self.lease);
        value
    }
}

impl<E> crate::PreparedSend<E> for PreparedPublication<'_, E>
where
    E: EventSpec + 'static,
    E::Payload: Send + Sync + 'static,
{
    fn try_send(self) -> Result<(), TrySendError<Self>> {
        self.try_send()
    }
    fn into_inner(self) -> E::Payload {
        self.into_inner()
    }
}

impl<E> EventSender<E> for DynamicSender<E>
where
    E: EventSpec + 'static,
    E::Payload: Send + Sync + 'static,
{
    type Prepared<'a> = PreparedPublication<'a, E>;

    fn prepare(&self, value: E::Payload) -> Result<Self::Prepared<'_>, TrySendError<E::Payload>> {
        self.prepare(value)
    }

    fn send_now(&self, value: E::Payload) -> Result<(), TrySendError<E::Payload>> {
        self.send_now(value)
    }

    async fn send(&self, value: E::Payload) -> Result<(), SendError<E::Payload>> {
        self.send(value).await
    }
}

impl<E> EventPort<E> for DynamicSender<E>
where
    E: EventSpec + 'static,
    E::Payload: Send + Sync + 'static,
{
    type Sender = Self;

    fn event_sender(&self) -> &Self {
        self
    }
}

struct ReceiverInner<T> {
    stream: Arc<Stream<T>>,
    grant: Grant,
    key: CursorKey,
    _lease: Lease,
}

impl<T> Drop for ReceiverInner<T> {
    fn drop(&mut self) {
        let cursor = {
            let mut state = self.stream.lock();
            let cursor = state
                .subscribers
                .get_mut(self.key.index)
                .and_then(Option::take);
            state.dirty = true;
            cursor
        };
        drop(cursor);
        self.stream.retire_consumed();
    }
}

/// An owned subscription. Cloning this handle shares a single receive cursor.
pub struct DynamicReceiver<E: EventSpec> {
    inner: Arc<ReceiverInner<E::Payload>>,
    marker: PhantomData<fn() -> E>,
}

impl<E: EventSpec> Clone for DynamicReceiver<E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            marker: PhantomData,
        }
    }
}

impl<E> DynamicReceiver<E>
where
    E: EventSpec + 'static,
    E::Payload: Send + Sync + 'static,
{
    /// Receives a shared payload or gap, then reclaims storage and dispatches wakeups.
    ///
    /// # Errors
    ///
    /// Returns [`ReceiveError::Contended`] when stream state is busy, or
    /// [`ReceiveError::Closed`] when the stream closes or the grant is revoked.
    ///
    /// # Panics
    ///
    /// Panics if an internal cursor or retained-sequence invariant is violated.
    pub fn recv_now(&self) -> Result<Option<StreamItem<Arc<E::Payload>>>, ReceiveError> {
        let item = self.try_recv();
        self.inner.stream.maintain();
        self.inner.grant.maintain_operations();
        item
    }

    /// Receives without allocation, reclamation, or wake callbacks.
    /// The scope owner must call [`DynamicFabric::maintain`] for deferred progress.
    ///
    /// # Errors
    /// Returns contention or termination without advancing the cursor.
    ///
    /// # Panics
    /// Panics if an internal retained-sequence invariant is violated.
    pub fn try_recv(&self) -> Result<Option<StreamItem<Arc<E::Payload>>>, ReceiveError> {
        let operation = self
            .inner
            .grant
            .enter_deferred(E::ID, Rights::OBSERVE)
            .map_err(|error| {
                if matches!(error, TopicError::Contended | TopicError::Capacity) {
                    ReceiveError::Contended
                } else {
                    ReceiveError::Closed(CloseReason::Revoked)
                }
            })?;
        let item = {
            let mut state = self
                .inner
                .stream
                .try_lock_deferred()
                .map_err(|()| ReceiveError::Contended)?;
            if state.closed {
                return Err(ReceiveError::Closed(CloseReason::Closed));
            }
            let key = self.inner.key;
            let cursor = state.subscribers[key.index]
                .as_ref()
                .filter(|cursor| cursor.id == key.id)
                .ok_or(ReceiveError::Closed(CloseReason::Closed))?;
            let next = cursor.next;
            let head = state
                .entries
                .front()
                .map_or(state.tail, |entry| entry.sequence);
            if next < head {
                state.subscribers[key.index].as_mut().unwrap().next = head;
                state.dirty = true;
                Some(StreamItem::Gap {
                    from: next,
                    to: head,
                })
            } else if next < state.tail {
                let index = usize::try_from(next - head).expect("retained sequence fits capacity");
                let entry = &state.entries[index];
                let item = StreamItem::Data {
                    sequence: next,
                    value: entry.value.clone(),
                };
                state.subscribers[key.index].as_mut().unwrap().next = next + 1;
                state.dirty = true;
                Some(item)
            } else {
                None
            }
        };
        drop(operation);
        Ok(item)
    }

    /// Waits for data, a gap, or revocation using one bounded waiter record.
    ///
    /// # Errors
    ///
    /// Returns [`ReceiveError::Closed`] on closure or revocation, or
    /// [`ReceiveError::WaitersFull`] if no bounded waiter record is available.
    ///
    /// # Panics
    ///
    /// Panics if an internal cursor or retained-sequence invariant is violated.
    pub async fn recv(&self) -> Result<StreamItem<Arc<E::Payload>>, ReceiveError> {
        let mut waiter = None;
        loop {
            let changed = self.inner.stream.changed.notified();
            let mut changed = core::pin::pin!(changed);
            if waiter.is_some() {
                changed.as_mut().enable();
            }
            match self.recv_now() {
                Ok(Some(item)) => return Ok(item),
                Ok(None) | Err(ReceiveError::Contended) => {}
                Err(error) => return Err(error),
            }
            if waiter.is_none() {
                waiter = Some(self.inner.stream.waiter(&self.inner.grant, E::ID).map_err(
                    |error| {
                        if error == TopicError::Revoked {
                            ReceiveError::Closed(CloseReason::Revoked)
                        } else {
                            ReceiveError::WaitersFull
                        }
                    },
                )?);
                continue;
            }
            changed.await;
        }
    }
}

fn recover_payload<T>(value: Arc<T>) -> T {
    Arc::try_unwrap(value).unwrap_or_else(|_| unreachable!("rejected payload escaped"))
}

impl<E: EventSpec> Port for DynamicReceiver<E> {}

impl<E> EventReceiver<E> for DynamicReceiver<E>
where
    E: EventSpec + 'static,
    E::Payload: Send + Sync + 'static,
{
    type Value = Arc<E::Payload>;

    fn event_try_recv(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError> {
        self.try_recv()
    }

    fn event_recv_now(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError> {
        self.recv_now()
    }

    async fn event_recv(&self) -> Result<StreamItem<Self::Value>, ReceiveError> {
        self.recv().await
    }
}
