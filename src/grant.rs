use core::ops::BitOr;
use std::{
    sync::{Arc, Weak},
    vec::Vec,
};

use crate::synchronization::Snapshot;
use tokio::sync::Notify;

use crate::{
    alloc_local::Scope,
    synchronization::{AtomicBool, AtomicUsize, Mutex, Ordering},
    EventId, EventSpec, TopicError,
};

const REVOKED: usize = 1 << (usize::BITS - 1);
const ACTIVE_COUNT: usize = REVOKED - 1;

/// Finite allowances shared by all handles to a grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Distinct streams and notification registrations.
    pub streams: usize,
    /// Descendant grants, including their reserved descendant allowance.
    pub grants: usize,
    /// Live subscriptions.
    pub subscriptions: usize,
    /// Total item allowance: event reservations plus generic transport storage.
    pub retained_items: usize,
    /// Total waiter allowance: event reservations plus transport/control waits.
    pub waiters: usize,
    /// Attached transport sessions.
    pub connections: usize,
    /// Transport-owned encoded bytes.
    pub bytes: usize,
}

impl Limits {
    /// No resource allowance.
    pub const ZERO: Self = Self {
        streams: 0,
        grants: 0,
        subscriptions: 0,
        retained_items: 0,
        waiters: 0,
        connections: 0,
        bytes: 0,
    };

    /// Rejects capacities which cannot describe an addressable allocation.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError::InvalidConfig`] if any allowance exceeds
    /// [`isize::MAX`]. Zero allowances are valid and permit no usage.
    pub fn validate(self) -> Result<(), TopicError> {
        if self
            .values()
            .iter()
            .any(|&value| value > isize::MAX as usize)
        {
            Err(TopicError::InvalidConfig)
        } else {
            Ok(())
        }
    }

    fn values(self) -> [usize; 7] {
        [
            self.streams,
            self.grants,
            self.subscriptions,
            self.retained_items,
            self.waiters,
            self.connections,
            self.bytes,
        ]
    }

    fn from_values(values: [usize; 7]) -> Self {
        Self {
            streams: values[0],
            grants: values[1],
            subscriptions: values[2],
            retained_items: values[3],
            waiters: values[4],
            connections: values[5],
            bytes: values[6],
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            streams: 64,
            grants: 128,
            subscriptions: 1024,
            retained_items: 65536,
            waiters: 4096,
            connections: 64,
            bytes: 64 * 1024 * 1024,
        }
    }
}

/// Operations permitted for one event within one scope.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rights(u8);

impl Rights {
    /// Admit payloads to the stream.
    pub const PUBLISH: Self = Self(1);
    /// Observe the stream without imposing backpressure.
    pub const OBSERVE: Self = Self(2);
    /// Establish a subscription that can impose backpressure.
    pub const REQUIRED: Self = Self(4);

    /// Combines permitted operations.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Tests whether every requested operation is permitted.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl BitOr for Rights {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

/// Reserved data-plane capacity for one event inside a grant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamLimits {
    /// Publications retained by transport storage.
    pub retained_items: usize,
    /// Independent receive cursors.
    pub subscriptions: usize,
    /// Pending publications and receives.
    pub waiters: usize,
}

impl StreamLimits {
    /// No data-plane capacity.
    pub const ZERO: Self = Self {
        retained_items: 0,
        subscriptions: 0,
        waiters: 0,
    };

    fn as_limits(self) -> Limits {
        Limits {
            retained_items: self.retained_items,
            subscriptions: self.subscriptions,
            waiters: self.waiters,
            ..Limits::ZERO
        }
    }
}

/// An event-specific permission requested when deriving a grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Permission {
    /// Event identity, resolved only inside this grant's scope.
    pub event: EventId,
    /// Permitted operations.
    pub rights: Rights,
    /// Capacity reserved for this event, never borrowed by another event.
    pub limits: StreamLimits,
}

impl Permission {
    /// Requests operations with zero capacity. Use [`Self::with_limits`] to reserve it.
    #[must_use]
    pub const fn new<E: EventSpec>(rights: Rights) -> Self {
        Self {
            event: E::ID,
            rights,
            limits: StreamLimits::ZERO,
        }
    }

    /// Reserves event capacity within the grant's total [`Limits`].
    #[must_use]
    pub const fn with_limits(mut self, limits: StreamLimits) -> Self {
        self.limits = limits;
        self
    }
}

/// Scoped authority. Clones share permissions, quotas, and revocation state.
#[derive(Clone)]
pub struct Grant {
    pub(crate) scope: Arc<Scope>,
    pub(crate) node: Arc<GrantNode>,
}

impl Grant {
    pub(crate) fn root(scope: Arc<Scope>, limits: Limits) -> Self {
        Self {
            scope,
            node: GrantNode::root(limits),
        }
    }

    /// Reserves a child allowance and attenuates event permissions.
    ///
    /// The parent's charge includes all child allowances and one grant slot.
    /// Revocation does not return that reservation while child handles remain.
    /// Event reservations come from matching parent event pools; the generic
    /// remainder comes only from the parent's generic pool. No capacity moves
    /// between events or between data and transport/control storage.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError::InvalidConfig`] for invalid limits or conflicting
    /// duplicate event reservations,
    /// [`TopicError::Denied`] for permissions the parent does not hold, or
    /// [`TopicError::Revoked`] when this grant or an ancestor is revoked.
    /// [`TopicError::Capacity`] means the permission list exceeds
    /// `limits.streams`, event reservations exceed the child's totals, or the
    /// parent cannot reserve every matching pool plus the child grant slot.
    /// Failed requests retain no reservations.
    pub fn restrict(&self, permissions: &[Permission], limits: Limits) -> Result<Self, TopicError> {
        Ok(Self {
            scope: self.scope.clone(),
            node: self.node.restrict(permissions, limits)?,
        })
    }

    /// Returns this grant's finite allowance.
    #[must_use]
    pub fn limits(&self) -> Limits {
        self.node.limits
    }

    /// Returns the reserved data-plane allowance for an event, including after revocation.
    ///
    /// # Errors
    /// Returns [`TopicError::Denied`] if the event is absent from this grant.
    ///
    /// # Panics
    /// Panics only if an internal immutable permission index is inconsistent.
    pub fn stream_limits<E: EventSpec>(&self) -> Result<StreamLimits, TopicError> {
        let index = self.node.event_index(E::ID)?;
        Ok(self.node.permissions.as_ref().unwrap()[index]
            .permission
            .limits)
    }

    /// Samples direct event usage plus reservations for descendants of that event.
    /// Fields are sampled independently; this is not an admission check.
    ///
    /// # Errors
    /// Returns [`TopicError::Denied`] if the event is absent from this grant.
    pub fn stream_usage<E: EventSpec>(&self) -> Result<StreamLimits, TopicError> {
        let index = self.node.event_index(E::ID)?;
        let usage = &self.node.pool(Some(index)).usage;
        Ok(StreamLimits {
            retained_items: usage[3].load(Ordering::Acquire),
            subscriptions: usage[2].load(Ordering::Acquire),
            waiters: usage[4].load(Ordering::Acquire),
        })
    }

    /// Returns direct usage plus child reservations.
    ///
    /// Each field is sampled separately; concurrent activity can change fields
    /// between reads. This snapshot must not be used to authorize operations.
    #[must_use]
    pub fn usage(&self) -> Limits {
        self.node.usage()
    }

    /// Returns whether this grant or an ancestor has been revoked.
    #[must_use]
    pub fn is_revoked(&self) -> bool {
        self.node.is_revoked()
    }

    /// Stops admission and waits for entered operations in the subtree to end.
    ///
    /// One independent control waiter is available per grant. Once revocation
    /// starts, cancelling this future leaves it in effect and frees its waiter.
    /// Repeating a completed revocation succeeds. Completion establishes an
    /// admission cutoff; already delivered values are not retracted.
    ///
    /// # Errors
    ///
    /// Returns [`TopicError::Capacity`] if another revocation of this grant
    /// already owns its control waiter and the cutoff has not completed.
    /// Exhausted data allowances do not prevent revocation.
    pub async fn revoke(&self) -> Result<(), TopicError> {
        self.node.revoke().await
    }

    pub(crate) fn check(&self, event: EventId, right: Rights) -> Result<(), TopicError> {
        self.node.check(event, right)
    }

    pub(crate) fn enter(&self, event: EventId, right: Rights) -> Result<Operation, TopicError> {
        self.node.enter(event, right)
    }

    pub(crate) fn enter_deferred(
        &self,
        event: EventId,
        right: Rights,
    ) -> Result<Operation, TopicError> {
        self.node.enter_mode(event, right, false)
    }

    pub(crate) fn maintain_operations(&self) {
        self.node.changed.notify_waiters();
    }

    pub(crate) fn maintain_tree(&self) {
        let mut nodes = vec![self.node.clone()];
        let mut index = 0;
        while index < nodes.len() {
            let node = nodes[index].clone();
            node.changed.notify_waiters();
            nodes.extend(node.children.load().iter().filter_map(Weak::upgrade));
            index += 1;
        }
    }

    pub(crate) fn try_charge(&self, usage: Limits) -> Result<Lease, TopicError> {
        self.node.try_charge(usage)
    }

    pub(crate) fn try_charge_stream(
        &self,
        event: EventId,
        usage: StreamLimits,
    ) -> Result<Lease, TopicError> {
        self.node.try_charge_stream(event, usage)
    }

    #[cfg(feature = "tokio-io")]
    pub(crate) fn revocation_changed(&self) -> &Notify {
        &self.node.revocation_changed
    }

    pub(crate) fn register_stream(
        &self,
        event: EventId,
        notify: &Arc<Notify>,
    ) -> Result<(), TopicError> {
        self.node.register_stream(event, notify)
    }
}

pub(crate) struct GrantNode {
    limits: Limits,
    pool: Pool,
    permissions: Option<Box<[EventAllowance]>>,
    parent_reservation: Option<Lease>,
    _event_reservations: Box<[Lease]>,
    state: AtomicUsize,
    revoker: AtomicBool,
    revocation_complete: AtomicBool,
    revocation_changed: Notify,
    changed: Notify,
    administration: Mutex<()>,
    children: Snapshot<Vec<Weak<GrantNode>>>,
    streams: Snapshot<Vec<(EventId, Weak<Notify>)>>,
}

struct Pool {
    limits: Limits,
    usage: [AtomicUsize; 7],
}

impl Pool {
    fn new(limits: Limits) -> Self {
        Self {
            limits,
            usage: core::array::from_fn(|_| AtomicUsize::new(0)),
        }
    }
}

struct EventAllowance {
    permission: Permission,
    pool: Pool,
}

fn partition(limits: Limits, permissions: &[Permission]) -> Result<Limits, TopicError> {
    let mut generic = limits;
    for (index, permission) in permissions.iter().enumerate() {
        if let Some(previous) = permissions[..index]
            .iter()
            .find(|entry| entry.event == permission.event)
        {
            if previous.limits != permission.limits {
                return Err(TopicError::InvalidConfig);
            }
            continue;
        }
        generic.retained_items = generic
            .retained_items
            .checked_sub(permission.limits.retained_items)
            .ok_or(TopicError::Capacity)?;
        generic.subscriptions = generic
            .subscriptions
            .checked_sub(permission.limits.subscriptions)
            .ok_or(TopicError::Capacity)?;
        generic.waiters = generic
            .waiters
            .checked_sub(permission.limits.waiters)
            .ok_or(TopicError::Capacity)?;
    }
    Ok(generic)
}

impl GrantNode {
    fn root(limits: Limits) -> Arc<Self> {
        Self::new(limits, limits, None, None, Box::default())
    }

    fn new(
        limits: Limits,
        generic: Limits,
        permissions: Option<Box<[EventAllowance]>>,
        parent_reservation: Option<Lease>,
        event_reservations: Box<[Lease]>,
    ) -> Arc<Self> {
        Arc::new(Self {
            limits,
            pool: Pool::new(generic),
            permissions,
            parent_reservation,
            _event_reservations: event_reservations,
            state: AtomicUsize::new(0),
            revoker: AtomicBool::new(false),
            revocation_complete: AtomicBool::new(false),
            revocation_changed: Notify::new(),
            changed: Notify::new(),
            administration: Mutex::new(()),
            children: Snapshot::from_pointee(Vec::new()),
            streams: Snapshot::from_pointee(Vec::new()),
        })
    }

    fn restrict(
        self: &Arc<Self>,
        permissions: &[Permission],
        limits: Limits,
    ) -> Result<Arc<Self>, TopicError> {
        limits.validate()?;
        if permissions.len() > limits.streams {
            return Err(TopicError::Capacity);
        }
        for permission in permissions {
            self.check(permission.event, permission.rights)?;
        }
        let generic = partition(limits, permissions)?;
        let mut reservation = if self.permissions.is_none() {
            limits
        } else {
            generic
        };
        reservation.grants = reservation
            .grants
            .checked_add(1)
            .ok_or(TopicError::InvalidConfig)?;
        let reservation = self.try_charge(reservation)?;
        let mut permissions = permissions.to_vec();
        permissions.sort_unstable_by_key(|permission| permission.event.as_u128());
        permissions.dedup_by(|later, earlier| {
            if later.event != earlier.event {
                return false;
            }
            earlier.rights = earlier.rights.union(later.rights);
            true
        });
        let mut event_reservations = Vec::new();
        if self.permissions.is_some() {
            for permission in &permissions {
                event_reservations
                    .push(self.try_charge_stream(permission.event, permission.limits)?);
            }
        }
        let child = Self::new(
            limits,
            generic,
            Some(
                permissions
                    .into_iter()
                    .map(|permission| EventAllowance {
                        pool: Pool::new(permission.limits.as_limits()),
                        permission,
                    })
                    .collect(),
            ),
            Some(reservation),
            event_reservations.into_boxed_slice(),
        );
        let administration = self.administration.lock().unwrap();
        if self.is_revoked() {
            drop(administration);
            return Err(TopicError::Revoked);
        }
        let mut children = self.children.load_full().as_ref().clone();
        children.retain(|child| child.strong_count() > 0);
        children.push(Arc::downgrade(&child));
        self.children.store(Arc::new(children));
        drop(administration);
        // This RMW and revocation share a modification order at every ancestor.
        // Either registration sees revocation, or revocation acquires publication
        // of this child before loading its descendant snapshots.
        let mut ancestor = self.as_ref();
        loop {
            if ancestor.state.fetch_or(0, Ordering::AcqRel) & REVOKED != 0 {
                return Err(TopicError::Revoked);
            }
            match &ancestor.parent_reservation {
                Some(parent) => ancestor = &parent.node,
                None => break,
            }
        }
        Ok(child)
    }

    pub(crate) fn check(&self, event: EventId, right: Rights) -> Result<(), TopicError> {
        if self.is_revoked() {
            return Err(TopicError::Revoked);
        }
        if let Some(permissions) = &self.permissions {
            let index = permissions
                .binary_search_by_key(&event.as_u128(), |entry| entry.permission.event.as_u128())
                .map_err(|_| TopicError::Denied)?;
            if !permissions[index].permission.rights.contains(right) {
                return Err(TopicError::Denied);
            }
        }
        Ok(())
    }

    pub(crate) fn is_revoked(&self) -> bool {
        let mut node = self;
        loop {
            if node.state.load(Ordering::Acquire) & REVOKED != 0 {
                return true;
            }
            match &node.parent_reservation {
                Some(parent) => node = &parent.node,
                None => return false,
            }
        }
    }

    pub(crate) fn enter(
        self: &Arc<Self>,
        event: EventId,
        right: Rights,
    ) -> Result<Operation, TopicError> {
        self.enter_mode(event, right, true)
    }

    fn enter_mode(
        self: &Arc<Self>,
        event: EventId,
        right: Rights,
        notify: bool,
    ) -> Result<Operation, TopicError> {
        self.check(event, right)?;
        // This RMW and revocation's fetch_or share one modification order:
        // admission is counted before the cutoff or rejected after it.
        let advance = |state| {
            if state & REVOKED == 0 && state & ACTIVE_COUNT < ACTIVE_COUNT {
                Some(state + 1)
            } else {
                None
            }
        };
        let entered = if notify {
            self.state
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, advance)
        } else {
            let state = self.state.load(Ordering::Acquire);
            let next = advance(state).ok_or(if state & REVOKED != 0 {
                TopicError::Revoked
            } else {
                TopicError::Capacity
            })?;
            self.state
                .compare_exchange(state, next, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| TopicError::Contended)?;
            Ok(state)
        };
        entered.map_err(|state| {
            if state & REVOKED != 0 {
                TopicError::Revoked
            } else {
                TopicError::Capacity
            }
        })?;
        let operation = Operation {
            node: self.clone(),
            notify,
        };
        if self.is_revoked() {
            return Err(TopicError::Revoked);
        }
        Ok(operation)
    }

    pub(crate) fn try_charge(self: &Arc<Self>, usage: Limits) -> Result<Lease, TopicError> {
        self.charge(None, usage)
    }

    fn event_index(&self, event: EventId) -> Result<usize, TopicError> {
        let permissions = self.permissions.as_ref().ok_or(TopicError::Denied)?;
        permissions
            .binary_search_by_key(&event.as_u128(), |entry| entry.permission.event.as_u128())
            .map_err(|_| TopicError::Denied)
    }

    fn try_charge_stream(
        self: &Arc<Self>,
        event: EventId,
        usage: StreamLimits,
    ) -> Result<Lease, TopicError> {
        let index = self.event_index(event)?;
        self.charge(Some(index), usage.as_limits())
    }

    fn pool(&self, event: Option<usize>) -> &Pool {
        event.map_or(&self.pool, |index| {
            &self.permissions.as_ref().unwrap()[index].pool
        })
    }

    fn charge(self: &Arc<Self>, event: Option<usize>, usage: Limits) -> Result<Lease, TopicError> {
        if self.is_revoked() {
            return Err(TopicError::Revoked);
        }
        let requested = usage.values();
        let pool = self.pool(event);
        let limits = pool.limits.values();
        for index in 0..requested.len() {
            if requested[index] == 0 {
                continue;
            }
            if pool.usage[index]
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(requested[index])
                        .filter(|sum| *sum <= limits[index])
                })
                .is_err()
            {
                for (counter, amount) in pool.usage.iter().zip(requested).take(index) {
                    if amount > 0 {
                        counter.fetch_sub(amount, Ordering::AcqRel);
                    }
                }
                if requested[..index].iter().any(|&amount| amount > 0) {
                    self.notify_release(event);
                }
                return Err(TopicError::Capacity);
            }
        }
        let lease = Lease {
            node: self.clone(),
            usage,
            event,
        };
        if self.is_revoked() {
            return Err(TopicError::Revoked);
        }
        Ok(lease)
    }

    pub(crate) fn usage(&self) -> Limits {
        Limits::from_values(core::array::from_fn(|index| {
            self.pool.usage[index].load(Ordering::Acquire)
                + self
                    .permissions
                    .iter()
                    .flatten()
                    .map(|entry| entry.pool.usage[index].load(Ordering::Acquire))
                    .sum::<usize>()
        }))
    }

    fn register_stream(&self, event: EventId, notify: &Arc<Notify>) -> Result<(), TopicError> {
        let administration = self.administration.lock().unwrap();
        if self.is_revoked() {
            return Err(TopicError::Revoked);
        }
        let mut streams = self.streams.load_full().as_ref().clone();
        streams.retain(|(_, stream)| stream.strong_count() > 0);
        let weak = Arc::downgrade(notify);
        if streams
            .iter()
            .any(|(id, stream)| *id == event && stream.ptr_eq(&weak))
        {
            return Ok(());
        }
        if streams.len() >= self.limits.streams {
            return Err(TopicError::Capacity);
        }
        streams.push((event, weak));
        self.streams.store(Arc::new(streams));
        drop(administration);
        if self.is_revoked() {
            notify.notify_waiters();
            return Err(TopicError::Revoked);
        }
        Ok(())
    }

    fn notify(&self) {
        self.changed.notify_waiters();
        for (_, stream) in self.streams.load().iter() {
            if let Some(stream) = stream.upgrade() {
                stream.notify_waiters();
            }
        }
    }

    fn notify_release(&self, event: Option<usize>) {
        self.changed.notify_waiters();
        if let Some(index) = event {
            let id = self.permissions.as_ref().unwrap()[index].permission.event;
            for (_, stream) in self.streams.load().iter().filter(|(event, _)| *event == id) {
                if let Some(stream) = stream.upgrade() {
                    stream.notify_waiters();
                }
            }
        }
    }

    async fn revoke(self: &Arc<Self>) -> Result<(), TopicError> {
        if self.revocation_complete.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.revoker.swap(true, Ordering::AcqRel) {
            return Err(TopicError::Capacity);
        }
        let _waiter = Revoker { node: self.clone() };
        let mut nodes = vec![self.clone()];
        let mut index = 0;
        while index < nodes.len() {
            let node = nodes[index].clone();
            if node.state.fetch_or(REVOKED, Ordering::AcqRel) & REVOKED == 0 {
                node.revocation_changed.notify_waiters();
            }
            node.notify();
            nodes.extend(node.children.load().iter().filter_map(Weak::upgrade));
            index += 1;
        }
        for node in &nodes {
            loop {
                let changed = node.changed.notified();
                let mut changed = core::pin::pin!(changed);
                changed.as_mut().enable();
                if node.state.load(Ordering::Acquire) & ACTIVE_COUNT == 0 {
                    break;
                }
                changed.await;
            }
        }
        for node in nodes {
            node.revocation_complete.store(true, Ordering::Release);
        }
        Ok(())
    }
}

pub(crate) struct Lease {
    node: Arc<GrantNode>,
    usage: Limits,
    event: Option<usize>,
}

impl Lease {
    pub(crate) fn belongs_to(&self, node: &Arc<GrantNode>) -> bool {
        Arc::ptr_eq(&self.node, node)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let amounts = self.usage.values();
        for (counter, amount) in self.node.pool(self.event).usage.iter().zip(amounts) {
            if amount > 0 {
                counter.fetch_sub(amount, Ordering::AcqRel);
            }
        }
        if amounts.iter().any(|&amount| amount > 0) {
            self.node.notify_release(self.event);
        }
    }
}

pub(crate) struct Operation {
    node: Arc<GrantNode>,
    notify: bool,
}

impl Drop for Operation {
    fn drop(&mut self) {
        self.node.state.fetch_sub(1, Ordering::Release);
        if self.notify {
            self.node.changed.notify_waiters();
        }
    }
}

struct Revoker {
    node: Arc<GrantNode>,
}

impl Drop for Revoker {
    fn drop(&mut self) {
        self.node.revoker.store(false, Ordering::Release);
    }
}

#[cfg(test)]
#[path = "tests_grant.rs"]
mod tests;
