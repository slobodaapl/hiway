use core::{
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use crate::locking::{DefaultMutex, Lock, LockFamily, Spin};
use atomic_waker::AtomicWaker;
#[cfg(not(loom))]
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use crate::{
    error::{CloseReason, ReceiveError, SendError, TopicError, TrySendError},
    Delivery, EventSpec, EventValue,
};

/// Whether a receiver may retain data needed by future publications.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SubscriptionRole {
    /// May fall behind; missing sequences are reported as gaps.
    #[default]
    Observer,
    /// Prevents overwriting data until receipt or detachment.
    Required,
}

/// A sequenced value or a half-open range of missed sequences.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamItem<T> {
    /// An accepted publication.
    Data {
        /// Sequence assigned at stream admission.
        sequence: u64,
        /// Owned payload or backend-owned payload handle.
        value: T,
    },
    /// Publications no longer retained for this observer.
    Gap {
        /// First missed sequence, inclusive.
        from: u64,
        /// First sequence after the gap, exclusive.
        to: u64,
    },
}

impl<T> StreamItem<T> {
    /// Maps data without changing its sequence or a gap's boundaries.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> StreamItem<U> {
        match self {
            Self::Data { sequence, value } => StreamItem::Data {
                sequence,
                value: map(value),
            },
            Self::Gap { from, to } => StreamItem::Gap { from, to },
        }
    }
}

/// A received payload held directly, without allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayloadValue<T>(pub T);

impl<T> PayloadValue<T> {
    /// Returns the owned payload.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for PayloadValue<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// Marker implemented by typed endpoints and generated ports.
pub trait Port {}

/// A payload prepared for callback-free, nonwaiting admission.
pub trait PreparedSend<E: EventSpec>: Sized {
    /// Attempts admission. Rejection retains the prepared payload and reservation.
    ///
    /// # Errors
    /// Returns capacity, contention, maintenance, or lifecycle rejection.
    fn try_send(self) -> Result<(), TrySendError<Self>>;

    /// Cancels preparation and returns the payload. May reclaim storage and wake waiters.
    fn into_inner(self) -> E::Payload;
}

/// Publication into a single typed stream.
pub trait EventSender<E: EventSpec>: Clone {
    /// Prepared payload and its bounded reservation, tied to this sender.
    type Prepared<'a>: PreparedSend<E>
    where
        Self: 'a;

    /// Allocates and reserves storage before strict admission. May run cleanup callbacks.
    ///
    /// # Errors
    /// Returns the unaccepted payload if storage or authority is unavailable.
    fn prepare(&self, payload: E::Payload) -> Result<Self::Prepared<'_>, TrySendError<E::Payload>>;

    /// Attempts admission, returning ownership on rejection.
    ///
    /// # Errors
    ///
    /// Returns the backend's capacity, contention, lifecycle, waiter, or
    /// sequence rejection together with the unaccepted payload.
    fn send_now(&self, payload: E::Payload) -> Result<(), TrySendError<E::Payload>>;

    /// Waits for local admission. Rejection returns the unaccepted payload.
    /// Success does not acknowledge processing; dropping a pending future
    /// drops its payload and cancels its waiter.
    ///
    /// # Errors
    ///
    /// Returns the unaccepted payload if local admission is rejected.
    fn send(&self, payload: E::Payload) -> impl Future<Output = Result<(), SendError<E::Payload>>>;
}

/// Event sender for generic code that must move send operations between tasks.
///
/// This opt-in contract provides a `Send` future; [`EventSender`] still supports
/// local futures.
pub trait SendEventSender<E: EventSpec>: EventSender<E> {
    /// Waits for local admission using a future that can move between tasks.
    ///
    /// # Errors
    ///
    /// Returns the unaccepted payload if local admission is rejected.
    fn send_future(
        &self,
        payload: E::Payload,
    ) -> impl Future<Output = Result<(), SendError<E::Payload>>> + Send;
}

/// The publication capability declared by a port.
pub trait EventPort<E: EventSpec>: Port {
    /// Backend sender retained by this port.
    type Sender: EventSender<E>;

    /// Borrows the sender for this event.
    fn event_sender(&self) -> &Self::Sender;
}

/// Prepared storage selected by a port's sender backend.
pub type PortPreparation<'a, P, E> = <<P as EventPort<E>>::Sender as EventSender<E>>::Prepared<'a>;

/// The receive capability declared by a port.
pub trait EventReceiver<E: EventSpec>: Port {
    /// Owned payload or a backend-owned payload handle.
    type Value: Deref<Target = E::Payload>;

    /// Receives without allocation, cleanup, or wake callbacks. Requires caller maintenance.
    ///
    /// # Errors
    /// Returns contention or termination without advancing the cursor.
    fn event_try_recv(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError>;

    /// Receives data or a gap without waiting for a publication.
    ///
    /// # Errors
    ///
    /// Returns the backend's contention, waiter-capacity, or termination error.
    fn event_recv_now(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError>;

    /// Waits for data, a gap, or termination.
    ///
    /// # Errors
    ///
    /// Returns the backend's contention, waiter-capacity, or termination error.
    fn event_recv(&self) -> impl Future<Output = Result<StreamItem<Self::Value>, ReceiveError>>;
}

/// Event receiver for generic code that must move receive operations between tasks.
///
/// This opt-in contract provides a `Send` future; [`EventReceiver`] still supports
/// local futures.
pub trait SendEventReceiver<E: EventSpec>: EventReceiver<E> {
    /// Waits for an event using a future that can move between tasks.
    ///
    /// # Errors
    ///
    /// Returns the backend's contention, waiter-capacity, or termination error.
    fn event_recv_future(
        &self,
    ) -> impl Future<Output = Result<StreamItem<Self::Value>, ReceiveError>> + Send;
}

/// Event-value syntax shared by generated and developer-owned ports.
pub trait PortExt: Port {
    /// Prepares a tagged payload for strict admission through [`PreparedSend::try_send`].
    ///
    /// # Errors
    /// Propagates the backend's preparation rejection with the original payload.
    fn prepare<E>(
        &self,
        event: EventValue<E>,
    ) -> Result<PortPreparation<'_, Self, E>, TrySendError<E::Payload>>
    where
        E: EventSpec,
        Self: EventPort<E>,
    {
        self.event_sender().prepare(event.into_inner())
    }

    /// Receives without allocation, cleanup, or wake callbacks. Requires caller maintenance.
    ///
    /// # Errors
    /// Propagates the backend's contention or termination error.
    fn try_recv<E>(
        &self,
    ) -> Result<Option<StreamItem<<Self as EventReceiver<E>>::Value>>, ReceiveError>
    where
        E: EventSpec,
        Self: EventReceiver<E>,
    {
        <Self as EventReceiver<E>>::event_try_recv(self)
    }

    /// Attempts local admission of a tagged payload.
    ///
    /// # Errors
    ///
    /// Propagates [`EventSender::send_now`] rejection with the original payload.
    fn publish_now<E>(&self, event: EventValue<E>) -> Result<(), TrySendError<E::Payload>>
    where
        E: EventSpec,
        Self: EventPort<E>,
    {
        <Self as EventPort<E>>::event_sender(self).send_now(event.into_inner())
    }

    /// Waits for local admission of a tagged payload.
    ///
    /// # Errors
    ///
    /// Propagates [`EventSender::send`] rejection with the original payload.
    fn publish<E>(
        &self,
        event: EventValue<E>,
    ) -> impl Future<Output = Result<(), SendError<E::Payload>>>
    where
        E: EventSpec,
        Self: EventPort<E>,
    {
        async move {
            <Self as EventPort<E>>::event_sender(self)
                .send(event.into_inner())
                .await
        }
    }

    /// Waits for local admission through a sender whose future can move between tasks.
    ///
    /// # Errors
    ///
    /// Propagates [`SendEventSender::send_future`] rejection with the original payload.
    fn publish_send<'a, E>(
        &'a self,
        event: EventValue<E>,
    ) -> impl Future<Output = Result<(), SendError<E::Payload>>> + Send
    where
        E: EventSpec,
        Self: EventPort<E>,
        <Self as EventPort<E>>::Sender: SendEventSender<E> + 'a,
    {
        <Self as EventPort<E>>::event_sender(self).send_future(event.into_inner())
    }

    /// Receives data or a gap for one declared event.
    ///
    /// # Errors
    ///
    /// Propagates [`EventReceiver::event_recv_now`] errors.
    fn recv_now<E>(
        &self,
    ) -> Result<Option<StreamItem<<Self as EventReceiver<E>>::Value>>, ReceiveError>
    where
        E: EventSpec,
        Self: EventReceiver<E>,
    {
        <Self as EventReceiver<E>>::event_recv_now(self)
    }

    /// Waits for one declared event or termination.
    ///
    /// # Errors
    ///
    /// Propagates [`EventReceiver::event_recv`] errors.
    fn recv<E>(
        &self,
    ) -> impl Future<Output = Result<StreamItem<<Self as EventReceiver<E>>::Value>, ReceiveError>>
    where
        E: EventSpec,
        Self: EventReceiver<E>,
    {
        <Self as EventReceiver<E>>::event_recv(self)
    }

    /// Waits for one declared event through a receiver whose future can move between tasks.
    ///
    /// # Errors
    ///
    /// Propagates [`SendEventReceiver::event_recv_future`] errors.
    fn recv_send<E>(
        &self,
    ) -> impl Future<Output = Result<StreamItem<<Self as EventReceiver<E>>::Value>, ReceiveError>> + Send
    where
        Self: SendEventReceiver<E>,
        E: EventSpec,
    {
        <Self as SendEventReceiver<E>>::event_recv_future(self)
    }
}

impl<P: Port + ?Sized> PortExt for P {}

/// Publishes a tagged payload through a generic port.
///
/// # Errors
///
/// Returns the unaccepted payload if the endpoint closes, its authority is
/// revoked, waiter storage is exhausted, or its sequence cannot advance.
pub async fn publish<P, E>(port: &P, event: EventValue<E>) -> Result<(), SendError<E::Payload>>
where
    P: EventPort<E> + ?Sized,
    E: EventSpec,
{
    port.publish(event).await
}

/// Binding whose endpoints may borrow caller-owned stream storage.
///
/// `#[port]` without `factory` infers its endpoint types through this trait.
/// A port can outlive the binding wrapper, but cannot outlive borrowed storage:
///
/// ```compile_fail,E0597
/// use hiway::{events, port, StaticFabric, StaticStream};
/// #[events]
/// enum Events { Value(u16) }
/// #[port(send(events::Value))]
/// struct Publisher;
/// let publisher = {
///     let stream = StaticStream::<events::Value, 2>::new();
///     Publisher::bind(&StaticFabric::new(&stream)).unwrap()
/// };
/// publisher.publish_now_value(42).unwrap();
/// ```
///
/// Binding a port exposes only its declared capabilities:
///
/// ```compile_fail,E0308
/// use hiway::{events, graph, port, StaticStream};
/// #[events]
/// enum Events { First(u16), Second(u16) }
/// #[graph(first = (events::First, 2), second = (events::Second, 2))]
/// struct Graph;
/// #[port(send(events::First))]
/// struct Publisher;
/// let first = StaticStream::new();
/// let second = StaticStream::new();
/// let publisher = Publisher::bind(&Graph::new(&first, &second)).unwrap();
/// publisher.publish_now(events::Second(42)).unwrap();
/// ```
pub trait PortBinding<'a, E: EventSpec> {
    /// Sender for the selected stream.
    type Sender: EventSender<E>;
    /// Subscription that owns its membership lifetime.
    type Receiver: EventReceiver<E>;

    /// Resolves the publication capability.
    ///
    /// # Errors
    ///
    /// Returns a binding error for invalid configuration, exhausted allowances,
    /// missing authority, revocation, or an incompatible payload type.
    fn sender(&self) -> Result<Self::Sender, TopicError>;
    /// Acquires membership with the requested backpressure role.
    ///
    /// # Errors
    ///
    /// Returns a binding error if the event or role is unavailable, its
    /// authority is revoked, or bounded subscription storage is exhausted.
    fn subscribe(&self, role: SubscriptionRole) -> Result<Self::Receiver, TopicError>;
}

/// Binding whose endpoints own their storage and membership handles.
pub trait OwnedPortBinding<E: EventSpec> {
    /// Sender retained by an owned port.
    type Sender: EventSender<E>;
    /// Receiver retained by an owned port.
    type Receiver: EventReceiver<E>;

    /// Resolves an owned publication capability.
    ///
    /// # Errors
    ///
    /// Returns a binding error for invalid configuration, exhausted allowances,
    /// missing authority, revocation, or an incompatible payload type.
    fn sender_owned(&self) -> Result<Self::Sender, TopicError>;
    /// Acquires owned membership with the requested backpressure role.
    ///
    /// # Errors
    ///
    /// Returns a binding error if the event or role is unavailable, its
    /// authority is revoked, or bounded subscription storage is exhausted.
    fn subscribe_owned(&self, role: SubscriptionRole) -> Result<Self::Receiver, TopicError>;
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SubscriberKey {
    index: usize,
    generation: u64,
}

#[derive(Clone, Copy)]
struct Cursor {
    generation: u64,
    next: u64,
    role: SubscriptionRole,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Interest {
    Send = 1,
    Receive = 2,
}

struct Waiter {
    occupied: AtomicBool,
    interest: AtomicU8,
    waker: AtomicWaker,
}

impl Waiter {
    #[cfg(not(loom))]
    const fn new() -> Self {
        Self {
            occupied: AtomicBool::new(false),
            interest: AtomicU8::new(0),
            waker: AtomicWaker::new(),
        }
    }

    #[cfg(loom)]
    fn new() -> Self {
        Self {
            occupied: AtomicBool::new(false),
            interest: AtomicU8::new(0),
            waker: AtomicWaker::new(),
        }
    }
}

struct State<T: Copy, const CAP: usize, const SUBS: usize> {
    delivery: Delivery,
    values: [Option<T>; CAP],
    tail: u64,
    cursors: [Option<Cursor>; SUBS],
    next_generation: u64,
    closed: Option<CloseReason>,
    changed: u8,
}

impl<T: Copy, const CAP: usize, const SUBS: usize> State<T, CAP, SUBS> {
    const fn new(delivery: Delivery) -> Self {
        Self {
            delivery,
            values: [None; CAP],
            tail: 0,
            cursors: [None; SUBS],
            next_generation: 0,
            closed: None,
            changed: 0,
        }
    }

    fn subscribe(&mut self, role: SubscriptionRole) -> Result<SubscriberKey, TopicError> {
        if let Some(reason) = self.closed {
            return Err(if reason == CloseReason::Revoked {
                TopicError::Revoked
            } else {
                TopicError::Denied
            });
        }
        if self.delivery == Delivery::Latest && role == SubscriptionRole::Required {
            return Err(TopicError::InvalidConfig);
        }
        let index = self
            .cursors
            .iter()
            .position(Option::is_none)
            .ok_or(TopicError::Capacity)?;
        let generation = self.next_generation;
        self.next_generation = generation.checked_add(1).ok_or(TopicError::Capacity)?;
        self.cursors[index] = Some(Cursor {
            generation,
            next: if self.delivery == Delivery::Latest {
                self.tail.saturating_sub(1)
            } else {
                self.tail
            },
            role,
        });
        Ok(SubscriberKey { index, generation })
    }

    fn send_now(&mut self, value: T) -> Result<(), TrySendError<T>> {
        if let Some(reason) = self.closed {
            return Err(if reason == CloseReason::Revoked {
                TrySendError::Revoked(value)
            } else {
                TrySendError::Closed(value)
            });
        }
        let next = self
            .tail
            .checked_add(1)
            .ok_or(TrySendError::SequenceExhausted(value))?;
        if self.tail >= CAP as u64 {
            let overwritten = self.tail - CAP as u64;
            if self.cursors.iter().flatten().any(|cursor| {
                cursor.role == SubscriptionRole::Required && cursor.next <= overwritten
            }) {
                return Err(TrySendError::Full(value));
            }
        }
        let index = usize::try_from(self.tail % CAP as u64)
            .expect("sequence remainder is smaller than usize capacity");
        self.values[index] = Some(value);
        self.tail = next;
        self.changed |= Interest::Receive as u8;
        Ok(())
    }

    fn recv_now(
        &mut self,
        key: SubscriberKey,
    ) -> Result<Option<StreamItem<PayloadValue<T>>>, ReceiveError> {
        if let Some(reason) = self.closed {
            return Err(ReceiveError::Closed(reason));
        }
        let cursor = self
            .cursors
            .get_mut(key.index)
            .and_then(Option::as_mut)
            .filter(|cursor| cursor.generation == key.generation)
            .ok_or(ReceiveError::Closed(CloseReason::Closed))?;
        if self.delivery == Delivery::Latest {
            cursor.next = cursor.next.max(self.tail.saturating_sub(1));
        }
        let oldest = self.tail.saturating_sub(CAP as u64);
        if cursor.next < oldest {
            let from = cursor.next;
            cursor.next = oldest;
            self.changed |= Interest::Send as u8;
            return Ok(Some(StreamItem::Gap { from, to: oldest }));
        }
        if cursor.next == self.tail {
            return Ok(None);
        }
        let sequence = cursor.next;
        let index = usize::try_from(sequence % CAP as u64)
            .expect("sequence remainder is smaller than usize capacity");
        let value = self.values[index].expect("retained sequence has a value");
        cursor.next += 1;
        self.changed |= Interest::Send as u8;
        Ok(Some(StreamItem::Data {
            sequence,
            value: PayloadValue(value),
        }))
    }
}

/// A bounded retained stream in caller-owned, allocation-free storage.
///
/// `Copy` is a backend requirement, not an event-contract requirement.
/// Strict `try_*` methods acquire the stream lock once or return contention.
/// Async polling parks on contention using bounded caller-owned waiter slots.
/// Synchronous convenience and administration may wait; do not preempt them with
/// another waiting operation on the same stream. Strict operations defer wakeups
/// until maintenance; interrupt handlers must use `try_*` only.
/// No payload callbacks or waker operations run under the stream lock.
pub struct StaticStream<
    E: EventSpec,
    const CAP: usize,
    const SUBS: usize = 8,
    const WAITERS: usize = 16,
    M: LockFamily = Spin,
> where
    E::Payload: Copy,
{
    state: M::Lock<State<E::Payload, CAP, SUBS>>,
    waiters: [Waiter; WAITERS],
    contended: AtomicBool,
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily>
    StaticStream<E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    /// Creates storage using the host's synchronization policy.
    ///
    /// # Panics
    /// Panics if `CAP` is zero or a latest-state stream has `CAP != 1`.
    pub fn with_lock(lock: M) -> Self {
        assert!(CAP > 0, "stream capacity must be greater than zero");
        assert!(
            !matches!(E::DELIVERY, Delivery::Latest) || CAP == 1,
            "latest-state streams require capacity one"
        );
        Self {
            state: lock.wrap(State::new(E::DELIVERY)),
            waiters: core::array::from_fn(|_| Waiter::new()),
            contended: AtomicBool::new(false),
        }
    }

    fn try_lock(
        &self,
    ) -> Option<impl core::ops::DerefMut<Target = State<E::Payload, CAP, SUBS>> + '_> {
        self.state.try_lock()
    }

    fn with<R>(&self, access: impl FnOnce(&mut State<E::Payload, CAP, SUBS>) -> R) -> R {
        let mut state = self.state.lock();
        let result = access(&mut state);
        let changed = core::mem::take(&mut state.changed);
        drop(state);
        self.notify(changed);
        result
    }

    fn try_with<R>(
        &self,
        access: impl FnOnce(&mut State<E::Payload, CAP, SUBS>) -> R,
    ) -> Option<R> {
        let mut state = self.try_lock().or_else(|| {
            self.contended.store(true, Ordering::SeqCst);
            self.try_lock()
        })?;
        let result = access(&mut state);
        let changed = core::mem::take(&mut state.changed);
        drop(state);
        self.notify(changed);
        Some(result)
    }

    fn notify(&self, changed: u8) {
        let contended = self.contended.swap(false, Ordering::SeqCst);
        for slot in &self.waiters {
            if contended || slot.interest.load(Ordering::Acquire) & changed != 0 {
                slot.waker.wake();
            }
        }
    }

    fn register(
        &self,
        id: &mut Option<usize>,
        interest: Interest,
        waker: &Waker,
    ) -> Result<(), ()> {
        let index = if let Some(index) = *id {
            index
        } else {
            let index = self
                .waiters
                .iter()
                .position(|slot| {
                    slot.occupied
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                })
                .ok_or(())?;
            *id = Some(index);
            index
        };
        let slot = &self.waiters[index];
        slot.interest.store(interest as u8, Ordering::Release);
        slot.waker.register(waker);
        Ok(())
    }

    fn poll_with<R>(
        &self,
        id: &mut Option<usize>,
        interest: Interest,
        waker: &Waker,
        mut access: impl FnMut(&mut State<E::Payload, CAP, SUBS>) -> Poll<R>,
    ) -> Result<Poll<R>, ()> {
        let mut result = Poll::Pending;
        // Keep payloads out of the lock helper's return value.
        let mut poll = |state: &mut State<E::Payload, CAP, SUBS>| match access(state) {
            Poll::Ready(value) => {
                result = Poll::Ready(value);
                true
            }
            Poll::Pending => false,
        };
        if !self.try_with(&mut poll).unwrap_or(false) {
            self.register(id, interest, waker)?;
            let _ = self.try_with(poll);
        }
        if result.is_ready() {
            self.cancel(id.take());
        }
        Ok(result)
    }

    /// Dispatches wakeups deferred by strict operations. May wait and run callbacks.
    pub fn maintain(&self) {
        self.with(|_| ());
    }

    /// Borrows a sender. The stream owner controls who receives this handle.
    pub const fn sender(&self) -> StaticSender<'_, E, CAP, SUBS, WAITERS, M> {
        StaticSender { stream: self }
    }

    /// Starts an ordered subscription at the current tail. Latest-state
    /// observers replay the current snapshot, then coalesce unread updates.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError::Capacity`] if subscriber slots or generation IDs
    /// are exhausted, [`TopicError::Revoked`] after revocation, or
    /// [`TopicError::Denied`] after another closure. Latest-state streams reject
    /// required subscriptions with [`TopicError::InvalidConfig`].
    pub fn subscribe(
        &self,
        role: SubscriptionRole,
    ) -> Result<StaticReceiver<'_, E, CAP, SUBS, WAITERS, M>, TopicError> {
        let key = self.with(|state| state.subscribe(role))?;
        Ok(StaticReceiver { stream: self, key })
    }

    /// Terminates the stream, clears retained data, and wakes pending work.
    /// The first close reason is retained.
    pub fn close(&self, reason: CloseReason) {
        self.with(|state| {
            state.closed.get_or_insert(reason);
            state.values = [None; CAP];
            state.changed = u8::MAX;
        });
    }

    fn cancel(&self, id: Option<usize>) {
        if let Some(index) = id {
            let slot = &self.waiters[index];
            drop(slot.waker.take());
            slot.occupied.store(false, Ordering::Release);
        }
    }
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize>
    StaticStream<E, CAP, SUBS, WAITERS>
where
    E::Payload: Copy,
{
    /// Creates an empty stream. Zero retained capacity is invalid.
    ///
    /// # Panics
    ///
    /// Panics if `CAP` is zero or a latest-state stream has `CAP != 1`.
    #[must_use]
    #[cfg(not(loom))]
    pub const fn new() -> Self {
        assert!(CAP > 0, "stream capacity must be greater than zero");
        assert!(
            !matches!(E::DELIVERY, Delivery::Latest) || CAP == 1,
            "latest-state streams require capacity one"
        );
        Self {
            state: DefaultMutex::new(State::new(E::DELIVERY)),
            waiters: [const { Waiter::new() }; WAITERS],
            contended: AtomicBool::new(false),
        }
    }

    /// Creates an empty stream using instrumented synchronization.
    ///
    /// # Panics
    /// Panics if `CAP` is zero or a latest-state stream has `CAP != 1`.
    #[must_use]
    #[cfg(loom)]
    pub fn new() -> Self {
        assert!(CAP > 0, "stream capacity must be greater than zero");
        assert!(
            !matches!(E::DELIVERY, Delivery::Latest) || CAP == 1,
            "latest-state streams require capacity one"
        );
        Self {
            state: DefaultMutex::new(State::new(E::DELIVERY)),
            waiters: core::array::from_fn(|_| Waiter::new()),
            contended: AtomicBool::new(false),
        }
    }
}

impl<
        E: EventSpec,
        const CAP: usize,
        const SUBS: usize,
        const WAITERS: usize,
        M: LockFamily + Default,
    > Default for StaticStream<E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    fn default() -> Self {
        Self::with_lock(M::default())
    }
}

/// A borrowed publication capability for one static stream.
pub struct StaticSender<
    'a,
    E: EventSpec,
    const CAP: usize,
    const SUBS: usize = 8,
    const WAITERS: usize = 16,
    M: LockFamily = Spin,
> where
    E::Payload: Copy,
{
    stream: &'a StaticStream<E, CAP, SUBS, WAITERS, M>,
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Clone
    for StaticSender<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Copy
    for StaticSender<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
}

impl<
        'a,
        E: EventSpec,
        const CAP: usize,
        const SUBS: usize,
        const WAITERS: usize,
        M: LockFamily,
    > StaticSender<'a, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    /// Attempts admission without waiting, allocation, or wake callbacks.
    /// The stream owner must service [`StaticStream::maintain`] for pending futures.
    ///
    /// # Errors
    /// Returns the original payload on contention, capacity, or lifecycle rejection.
    pub fn try_send(&self, payload: E::Payload) -> Result<(), TrySendError<E::Payload>> {
        self.stream
            .try_lock()
            .ok_or(TrySendError::Contended(payload))?
            .send_now(payload)
    }

    /// Attempts admission without waiting for receiver capacity.
    ///
    /// # Errors
    ///
    /// Returns the original payload if required retention fills the stream,
    /// the stream closes or is revoked, or its sequence cannot advance.
    pub fn send_now(&self, payload: E::Payload) -> Result<(), TrySendError<E::Payload>> {
        self.stream.with(|state| state.send_now(payload))
    }

    /// Waits for admission using bounded, per-future waiter storage.
    pub fn send(&self, payload: E::Payload) -> StaticSendFuture<'a, E, CAP, SUBS, WAITERS, M> {
        StaticSendFuture {
            stream: self.stream,
            payload: Some(payload),
            waiter: None,
        }
    }
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily>
    EventSender<E> for StaticSender<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    type Prepared<'a>
        = StaticPublication<'a, E, CAP, SUBS, WAITERS, M>
    where
        Self: 'a;

    fn prepare(&self, value: E::Payload) -> Result<Self::Prepared<'_>, TrySendError<E::Payload>> {
        Ok(StaticPublication {
            sender: *self,
            value,
        })
    }

    fn send_now(&self, payload: E::Payload) -> Result<(), TrySendError<E::Payload>> {
        self.send_now(payload)
    }
    fn send(&self, payload: E::Payload) -> impl Future<Output = Result<(), SendError<E::Payload>>> {
        self.send(payload)
    }
}

impl<E, const CAP: usize, const SUBS: usize, const WAITERS: usize, M> SendEventSender<E>
    for StaticSender<'_, E, CAP, SUBS, WAITERS, M>
where
    E: EventSpec,
    E::Payload: Copy + Send,
    M: LockFamily,
    StaticStream<E, CAP, SUBS, WAITERS, M>: Sync,
{
    fn send_future(
        &self,
        payload: E::Payload,
    ) -> impl Future<Output = Result<(), SendError<E::Payload>>> + Send {
        self.send(payload)
    }
}

/// A static payload prepared without allocation. Admission still validates stream state.
#[must_use = "prepared payloads are not admitted until try_send succeeds"]
pub struct StaticPublication<
    'a,
    E: EventSpec,
    const CAP: usize,
    const SUBS: usize,
    const WAITERS: usize,
    M: LockFamily = Spin,
> where
    E::Payload: Copy,
{
    sender: StaticSender<'a, E, CAP, SUBS, WAITERS, M>,
    value: E::Payload,
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily>
    PreparedSend<E> for StaticPublication<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    fn try_send(self) -> Result<(), TrySendError<Self>> {
        self.sender
            .try_send(self.value)
            .map_err(|error| error.map(|value| Self { value, ..self }))
    }
    fn into_inner(self) -> E::Payload {
        self.value
    }
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Port
    for StaticSender<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily>
    EventPort<E> for StaticSender<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    type Sender = Self;
    fn event_sender(&self) -> &Self {
        self
    }
}

/// An owned subscription to caller-owned stream storage.
pub struct StaticReceiver<
    'a,
    E: EventSpec,
    const CAP: usize,
    const SUBS: usize = 8,
    const WAITERS: usize = 16,
    M: LockFamily = Spin,
> where
    E::Payload: Copy,
{
    stream: &'a StaticStream<E, CAP, SUBS, WAITERS, M>,
    key: SubscriberKey,
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily>
    StaticReceiver<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    /// Receives without waiting, allocation, cleanup, or wake callbacks.
    /// The stream owner must service [`StaticStream::maintain`] for pending futures.
    ///
    /// # Errors
    /// Returns contention or the stream's termination reason.
    pub fn try_recv(&self) -> Result<Option<StreamItem<PayloadValue<E::Payload>>>, ReceiveError> {
        self.stream
            .try_lock()
            .ok_or(ReceiveError::Contended)?
            .recv_now(self.key)
    }

    /// Receives the next retained value or missing range.
    ///
    /// # Errors
    ///
    /// Returns [`ReceiveError::Closed`] with the stream's termination reason.
    pub fn recv_now(&self) -> Result<Option<StreamItem<PayloadValue<E::Payload>>>, ReceiveError> {
        self.stream.with(|state| state.recv_now(self.key))
    }

    /// Waits for data, a gap, or termination.
    #[must_use]
    pub fn recv(&self) -> StaticReceiveFuture<'_, '_, E, CAP, SUBS, WAITERS, M> {
        StaticReceiveFuture {
            receiver: self,
            waiter: None,
        }
    }
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Drop
    for StaticReceiver<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    fn drop(&mut self) {
        self.stream.with(|state| {
            if state.cursors[self.key.index]
                .is_some_and(|cursor| cursor.generation == self.key.generation)
            {
                state.cursors[self.key.index] = None;
                state.changed |= Interest::Send as u8;
            }
        });
    }
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Port
    for StaticReceiver<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily>
    EventReceiver<E> for StaticReceiver<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    type Value = PayloadValue<E::Payload>;
    fn event_try_recv(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError> {
        self.try_recv()
    }
    fn event_recv_now(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError> {
        self.recv_now()
    }
    fn event_recv(&self) -> impl Future<Output = Result<StreamItem<Self::Value>, ReceiveError>> {
        self.recv()
    }
}

impl<E, const CAP: usize, const SUBS: usize, const WAITERS: usize, M> SendEventReceiver<E>
    for StaticReceiver<'_, E, CAP, SUBS, WAITERS, M>
where
    E: EventSpec,
    E::Payload: Copy,
    M: LockFamily,
    StaticStream<E, CAP, SUBS, WAITERS, M>: Sync,
{
    fn event_recv_future(
        &self,
    ) -> impl Future<Output = Result<StreamItem<Self::Value>, ReceiveError>> + Send {
        self.recv()
    }
}

/// A pending static publication. Dropping it removes only its waiter and
/// drops the unaccepted payload. An error returns that payload to the caller.
pub struct StaticSendFuture<
    'a,
    E: EventSpec,
    const CAP: usize,
    const SUBS: usize = 8,
    const WAITERS: usize = 16,
    M: LockFamily = Spin,
> where
    E::Payload: Copy,
{
    stream: &'a StaticStream<E, CAP, SUBS, WAITERS, M>,
    payload: Option<E::Payload>,
    waiter: Option<usize>,
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Unpin
    for StaticSendFuture<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Future
    for StaticSendFuture<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    type Output = Result<(), SendError<E::Payload>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let value = this
            .payload
            .as_ref()
            .expect("completed send was polled again");
        let result = match this.stream.poll_with(
            &mut this.waiter,
            Interest::Send,
            context.waker(),
            |state| match state.send_now(*value) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(TrySendError::Full(_)) => Poll::Pending,
                Err(TrySendError::Revoked(value)) => Poll::Ready(Err(SendError::Revoked(value))),
                Err(TrySendError::SequenceExhausted(value)) => {
                    Poll::Ready(Err(SendError::SequenceExhausted(value)))
                }
                Err(error) => Poll::Ready(Err(SendError::Closed(error.into_inner()))),
            },
        ) {
            Ok(result) => result,
            Err(()) => Poll::Ready(Err(SendError::WaitersFull(*value))),
        };
        if result.is_ready() {
            this.payload = None;
        }
        result
    }
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Drop
    for StaticSendFuture<'_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    fn drop(&mut self) {
        self.stream.cancel(self.waiter.take());
    }
}

/// A pending static receive. Concurrent futures have distinct waiter records.
pub struct StaticReceiveFuture<
    'a,
    'stream,
    E: EventSpec,
    const CAP: usize,
    const SUBS: usize = 8,
    const WAITERS: usize = 16,
    M: LockFamily = Spin,
> where
    E::Payload: Copy,
{
    receiver: &'a StaticReceiver<'stream, E, CAP, SUBS, WAITERS, M>,
    waiter: Option<usize>,
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Future
    for StaticReceiveFuture<'_, '_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    type Output = Result<StreamItem<PayloadValue<E::Payload>>, ReceiveError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.receiver.stream.poll_with(
            &mut this.waiter,
            Interest::Receive,
            context.waker(),
            |state| match state.recv_now(this.receiver.key) {
                Ok(Some(item)) => Poll::Ready(Ok(item)),
                Err(error) => Poll::Ready(Err(error)),
                Ok(None) => Poll::Pending,
            },
        ) {
            Ok(Poll::Ready(result)) => Poll::Ready(result),
            Ok(Poll::Pending) => Poll::Pending,
            Err(()) => Poll::Ready(Err(ReceiveError::WaitersFull)),
        }
    }
}

impl<E: EventSpec, const CAP: usize, const SUBS: usize, const WAITERS: usize, M: LockFamily> Drop
    for StaticReceiveFuture<'_, '_, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    fn drop(&mut self) {
        self.receiver.stream.cancel(self.waiter.take());
    }
}

/// A single-event binding into caller-owned static storage.
pub struct StaticFabric<
    'a,
    E: EventSpec,
    const CAP: usize,
    const SUBS: usize = 8,
    const WAITERS: usize = 16,
    M: LockFamily = Spin,
> where
    E::Payload: Copy,
{
    stream: &'a StaticStream<E, CAP, SUBS, WAITERS, M>,
}

impl<
        'a,
        E: EventSpec,
        const CAP: usize,
        const SUBS: usize,
        const WAITERS: usize,
        M: LockFamily,
    > StaticFabric<'a, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    /// Borrows one retained stream.
    pub const fn new(stream: &'a StaticStream<E, CAP, SUBS, WAITERS, M>) -> Self {
        Self { stream }
    }
}

impl<
        'a,
        E: EventSpec,
        const CAP: usize,
        const SUBS: usize,
        const WAITERS: usize,
        M: LockFamily,
    > PortBinding<'a, E> for StaticFabric<'a, E, CAP, SUBS, WAITERS, M>
where
    E::Payload: Copy,
{
    type Sender = StaticSender<'a, E, CAP, SUBS, WAITERS, M>;
    type Receiver = StaticReceiver<'a, E, CAP, SUBS, WAITERS, M>;

    fn sender(&self) -> Result<Self::Sender, TopicError> {
        Ok(self.stream.sender())
    }
    fn subscribe(&self, role: SubscriptionRole) -> Result<Self::Receiver, TopicError> {
        self.stream.subscribe(role)
    }
}

/// A named event sender retained independently of its fabric.
pub struct Topic<E: EventSpec, S: EventSender<E>> {
    sender: S,
    marker: PhantomData<fn() -> E>,
}

impl<E: EventSpec, S: EventSender<E>> Topic<E, S> {
    /// Retains the supplied sender.
    pub const fn new(sender: S) -> Self {
        Self {
            sender,
            marker: PhantomData,
        }
    }
    /// Clones the existing capability, without acquiring another allowance.
    pub fn sender(&self) -> S {
        self.sender.clone()
    }
    /// Attempts local admission.
    ///
    /// # Errors
    ///
    /// Propagates [`EventSender::send_now`] rejection with the original payload.
    pub fn send_now(&self, payload: E::Payload) -> Result<(), TrySendError<E::Payload>> {
        self.sender.send_now(payload)
    }
    /// Waits for local admission.
    ///
    /// # Errors
    ///
    /// Returns the unaccepted payload if the endpoint closes, its authority is
    /// revoked, waiter storage is exhausted, or its sequence cannot advance.
    pub async fn send(&self, payload: E::Payload) -> Result<(), SendError<E::Payload>> {
        self.sender.send(payload).await
    }
}

impl<E: EventSpec, S: EventSender<E>> Clone for Topic<E, S> {
    fn clone(&self) -> Self {
        Self::new(self.sender.clone())
    }
}

#[cfg(test)]
#[path = "tests_local.rs"]
mod tests;
