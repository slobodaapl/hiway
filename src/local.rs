use core::{
    cell::RefCell,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use critical_section::Mutex as CriticalMutex;
use heapless::{Deque, Vec};

use crate::{
    error::{SendError, TopicError, TrySendError},
    transform::TransformOp,
    EventSpec, EventValue,
};

/// Default capacity for one caller-owned inbox.
pub const DEFAULT_INBOX_CAPACITY: usize = 64;

/// Default number of blocked senders remembered by an inbox.
pub const DEFAULT_INBOX_WAITERS: usize = 4;

struct CriticalCell<T>(CriticalMutex<RefCell<T>>);

impl<T> CriticalCell<T> {
    const fn new(value: T) -> Self {
        Self(CriticalMutex::new(RefCell::new(value)))
    }

    fn with<R>(&self, access: impl FnOnce(&T) -> R) -> R {
        critical_section::with(|critical| access(&*self.0.borrow(critical).borrow()))
    }

    fn with_mut<R>(&self, access: impl FnOnce(&mut T) -> R) -> R {
        critical_section::with(|critical| access(&mut *self.0.borrow(critical).borrow_mut()))
    }
}

/// Policy applied when a subscriber inbox is full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryPolicy {
    /// Keep the sender pending until the inbox accepts the value.
    Reliable,
    /// Discard the new value when the inbox is full.
    DropNewest,
    /// Discard the oldest value before inserting the new value.
    DropOldest,
    /// Keep only the most recent value.
    Latest,
}

/// Result of attempting to push one value into a target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushResult {
    /// The target accepted the value.
    Accepted,
    /// The target was closed.
    Closed,
    /// The target was full under reliable delivery.
    Full,
    /// The target dropped the value under its delivery policy.
    Dropped,
}

/// Allocation-free subscriber target contract.
pub trait Target<P> {
    /// Returns whether this target has been closed.
    fn is_closed(&self) -> bool {
        false
    }

    /// Returns whether a reliable value can be accepted now.
    fn can_accept(&self, policy: DeliveryPolicy) -> bool;

    /// Pushes one value according to the selected policy.
    fn push(&self, payload: P, policy: DeliveryPolicy) -> PushResult;

    /// Registers a sender waker for reliable delivery.
    /// Returns `false` when bounded waiter storage cannot retain the waker.
    fn register_waker(&self, waker: &Waker) -> bool;

    /// Removes a previously registered sender waker.
    fn unregister_waker(&self, _waker: &Waker) {}

    /// Wakes senders waiting on this target.
    fn wake_waiters(&self);
}

/// A cloneable caller-owned target handle.
pub type TargetHandle<'a, P> = &'a (dyn Target<P> + Sync);

impl<P> Target<P> for &(dyn Target<P> + Sync) {
    fn is_closed(&self) -> bool {
        (**self).is_closed()
    }

    fn can_accept(&self, policy: DeliveryPolicy) -> bool {
        (**self).can_accept(policy)
    }

    fn push(&self, payload: P, policy: DeliveryPolicy) -> PushResult {
        (**self).push(payload, policy)
    }

    fn register_waker(&self, waker: &Waker) -> bool {
        (**self).register_waker(waker)
    }

    fn unregister_waker(&self, waker: &Waker) {
        (**self).unregister_waker(waker)
    }

    fn wake_waiters(&self) {
        (**self).wake_waiters()
    }
}

struct InboxState<T, const CAPACITY: usize, const WAITERS: usize> {
    queue: Deque<T, CAPACITY>,
    closed: bool,
    receiver_waker: Option<Waker>,
    sender_wakers: Vec<Waker, WAITERS>,
}

/// A caller-owned, bounded, no-alloc inbox.
pub struct Inbox<
    T,
    const CAPACITY: usize = DEFAULT_INBOX_CAPACITY,
    const WAITERS: usize = DEFAULT_INBOX_WAITERS,
> {
    state: CriticalCell<InboxState<T, CAPACITY, WAITERS>>,
}

impl<T, const CAPACITY: usize, const WAITERS: usize> Inbox<T, CAPACITY, WAITERS> {
    /// Creates an empty inbox in caller-owned storage.
    #[must_use]
    pub const fn new() -> Self {
        assert!(CAPACITY > 0, "inbox capacity must be greater than zero");
        Self {
            state: CriticalCell::new(InboxState {
                queue: Deque::new(),
                closed: false,
                receiver_waker: None,
                sender_wakers: Vec::new(),
            }),
        }
    }

    /// Creates a cloneable target handle for this inbox.
    #[must_use]
    pub const fn handle(&self) -> InboxRef<'_, T, CAPACITY, WAITERS> {
        InboxRef { inbox: self }
    }

    /// Returns the number of queued values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state.with(|state| state.queue.len())
    }

    /// Returns whether no value is queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether the queue is full.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.state.with(|state| state.queue.is_full())
    }

    /// Receives one value without waiting.
    pub fn try_recv(&self) -> Option<T> {
        let (value, senders) = self.state.with_mut(|state| {
            let value = state.queue.pop_front();
            let senders = if value.is_some() {
                take_wakers(&mut state.sender_wakers)
            } else {
                Vec::new()
            };
            (value, senders)
        });
        wake_all(senders);
        value
    }

    /// Receives one value using a core future.
    pub fn recv(&self) -> impl Future<Output = Option<T>> + '_ {
        core::future::poll_fn(move |context| self.poll_recv(context))
    }

    /// Drains currently queued values in FIFO order.
    pub fn drain(&self, mut consume: impl FnMut(T)) -> usize {
        let mut count = 0;
        while let Some(value) = self.try_recv() {
            consume(value);
            count += 1;
        }
        count
    }

    /// Closes the inbox and wakes pending operations.
    pub fn close(&self) {
        let (queued, receiver, senders) = self.state.with_mut(|state| {
            state.closed = true;
            (
                core::mem::take(&mut state.queue),
                state.receiver_waker.take(),
                take_wakers(&mut state.sender_wakers),
            )
        });
        drop(queued);
        if let Some(waker) = receiver {
            waker.wake();
        }
        wake_all(senders);
    }

    /// Polls for one value without selecting an executor.
    pub fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Option<T>> {
        let mut receiver_waker = Some(context.waker().clone());
        let (result, stale, senders) = self.state.with_mut(|state| {
            if let Some(value) = state.queue.pop_front() {
                (
                    Poll::Ready(Some(value)),
                    state.receiver_waker.take(),
                    take_wakers(&mut state.sender_wakers),
                )
            } else if state.closed {
                (Poll::Ready(None), state.receiver_waker.take(), Vec::new())
            } else {
                let stale = state
                    .receiver_waker
                    .replace(receiver_waker.take().expect("receiver waker is available"));
                (Poll::Pending, stale, Vec::new())
            }
        });
        drop(receiver_waker);
        drop(stale);
        wake_all(senders);
        result
    }

    fn can_accept(&self, policy: DeliveryPolicy) -> bool {
        self.state.with(|state| {
            !state.closed && (policy != DeliveryPolicy::Reliable || !state.queue.is_full())
        })
    }

    fn is_closed(&self) -> bool {
        self.state.with(|state| state.closed)
    }

    fn push_value(&self, value: T, policy: DeliveryPolicy) -> PushResult {
        let mut discarded = Deque::new();
        let (result, receiver) = self.state.with_mut(|state| {
            if state.closed {
                let _ = discarded.push_back(value);
                (PushResult::Closed, None)
            } else if state.queue.is_full() {
                match policy {
                    DeliveryPolicy::Reliable => {
                        let _ = discarded.push_back(value);
                        (PushResult::Full, None)
                    }
                    DeliveryPolicy::DropNewest => {
                        let _ = discarded.push_back(value);
                        (PushResult::Dropped, None)
                    }
                    DeliveryPolicy::DropOldest => {
                        if let Some(oldest) = state.queue.pop_front() {
                            let _ = discarded.push_back(oldest);
                        }
                        let result = push_or_discard(&mut state.queue, value, &mut discarded);
                        (result, state.receiver_waker.take())
                    }
                    DeliveryPolicy::Latest => {
                        core::mem::swap(&mut state.queue, &mut discarded);
                        let result = push_or_discard(&mut state.queue, value, &mut discarded);
                        (result, state.receiver_waker.take())
                    }
                }
            } else {
                let result = push_or_discard(&mut state.queue, value, &mut discarded);
                (result, state.receiver_waker.take())
            }
        });
        drop(discarded);
        if matches!(result, PushResult::Accepted) {
            if let Some(waker) = receiver {
                waker.wake();
            }
        }
        result
    }

    fn register_sender_waker(&self, waker: &Waker) -> bool {
        let mut candidate = Some(waker.clone());
        let accepted = self.state.with_mut(|state| {
            if state.closed {
                return true;
            }
            if state
                .sender_wakers
                .iter()
                .any(|existing| existing.will_wake(waker))
            {
                return true;
            }
            let value = candidate.take().expect("sender waker is available");
            match state.sender_wakers.push(value) {
                Ok(()) => true,
                Err(value) => {
                    candidate = Some(value);
                    false
                }
            }
        });
        drop(candidate);
        accepted
    }

    fn unregister_sender_waker(&self, waker: &Waker) {
        self.state.with_mut(|state| {
            if let Some(index) = state
                .sender_wakers
                .iter()
                .position(|existing| existing.will_wake(waker))
            {
                state.sender_wakers.swap_remove(index);
            }
        });
    }

    fn wake_sender_waiters(&self) {
        let senders = self
            .state
            .with_mut(|state| take_wakers(&mut state.sender_wakers));
        wake_all(senders);
    }
}

impl<T, const CAPACITY: usize, const WAITERS: usize> Default for Inbox<T, CAPACITY, WAITERS> {
    fn default() -> Self {
        Self::new()
    }
}

fn take_wakers<const N: usize>(wakers: &mut Vec<Waker, N>) -> Vec<Waker, N> {
    core::mem::replace(wakers, Vec::new())
}

fn wake_all<const N: usize>(wakers: Vec<Waker, N>) {
    for waker in wakers {
        waker.wake();
    }
}

fn push_or_discard<T, const CAPACITY: usize>(
    queue: &mut Deque<T, CAPACITY>,
    value: T,
    discarded: &mut Deque<T, CAPACITY>,
) -> PushResult {
    match queue.push_back(value) {
        Ok(()) => PushResult::Accepted,
        Err(value) => {
            let _ = discarded.push_back(value);
            PushResult::Dropped
        }
    }
}

/// A cloneable borrowed inbox endpoint.
#[derive(Clone, Copy)]
pub struct InboxRef<'a, T, const CAPACITY: usize, const WAITERS: usize> {
    inbox: &'a Inbox<T, CAPACITY, WAITERS>,
}

impl<T, const CAPACITY: usize, const WAITERS: usize> InboxRef<'_, T, CAPACITY, WAITERS> {
    /// Returns the underlying inbox.
    #[must_use]
    pub fn inbox(&self) -> &Inbox<T, CAPACITY, WAITERS> {
        self.inbox
    }
}

impl<T, const CAPACITY: usize, const WAITERS: usize> Target<T> for Inbox<T, CAPACITY, WAITERS> {
    fn is_closed(&self) -> bool {
        Self::is_closed(self)
    }

    fn can_accept(&self, policy: DeliveryPolicy) -> bool {
        Self::can_accept(self, policy)
    }

    fn push(&self, payload: T, policy: DeliveryPolicy) -> PushResult {
        self.push_value(payload, policy)
    }

    fn register_waker(&self, waker: &Waker) -> bool {
        self.register_sender_waker(waker)
    }

    fn unregister_waker(&self, waker: &Waker) {
        self.unregister_sender_waker(waker)
    }

    fn wake_waiters(&self) {
        self.wake_sender_waiters()
    }
}

impl<T, const CAPACITY: usize, const WAITERS: usize> Target<T>
    for InboxRef<'_, T, CAPACITY, WAITERS>
{
    fn is_closed(&self) -> bool {
        self.inbox.is_closed()
    }

    fn can_accept(&self, policy: DeliveryPolicy) -> bool {
        self.inbox.can_accept(policy)
    }

    fn push(&self, payload: T, policy: DeliveryPolicy) -> PushResult {
        self.inbox.push_value(payload, policy)
    }

    fn register_waker(&self, waker: &Waker) -> bool {
        self.inbox.register_sender_waker(waker)
    }

    fn unregister_waker(&self, waker: &Waker) {
        self.inbox.unregister_sender_waker(waker)
    }

    fn wake_waiters(&self) {
        self.inbox.wake_sender_waiters()
    }
}

/// Receiver behavior shared by caller-owned and allocating inbox backends.
pub trait Receiver<T> {
    /// Target handle used by a fabric subscription.
    type Target: Clone;

    /// Fixed queue capacity when the receiver exposes one.
    const CAPACITY: Option<usize> = None;

    /// Returns a cloneable target handle.
    fn target(&self) -> Self::Target;

    /// Receives one value without waiting.
    fn try_recv(&self) -> Option<T>;

    /// Polls for one value.
    fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Option<T>>;
}

impl<'a, T: Send, const CAPACITY: usize, const WAITERS: usize> Receiver<T>
    for InboxRef<'a, T, CAPACITY, WAITERS>
{
    type Target = TargetHandle<'a, T>;
    const CAPACITY: Option<usize> = Some(CAPACITY);

    fn target(&self) -> Self::Target {
        self.inbox
    }

    fn try_recv(&self) -> Option<T> {
        self.inbox.try_recv()
    }

    fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Option<T>> {
        self.inbox.poll_recv(context)
    }
}

/// Future returned by [`receive`].
pub struct Receive<'a, R, T> {
    receiver: &'a R,
    marker: PhantomData<fn() -> T>,
}

impl<'a, R, T> Future for Receive<'a, R, T>
where
    R: Receiver<T>,
{
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.receiver.poll_recv(context)
    }
}

/// Builds an executor-neutral receive future.
pub fn receive<R, T>(receiver: &R) -> Receive<'_, R, T>
where
    R: Receiver<T>,
{
    Receive {
        receiver,
        marker: PhantomData,
    }
}

/// A target mapper stored by caller-owned component storage.
pub struct MappedTarget<P, T, H> {
    target: H,
    map: fn(P) -> T,
}

impl<P, T, H: Clone> Clone for MappedTarget<P, T, H> {
    fn clone(&self) -> Self {
        Self {
            target: self.target.clone(),
            map: self.map,
        }
    }
}

impl<P, T, H> MappedTarget<P, T, H> {
    /// Creates a mapped target from an endpoint handle and a pure mapper.
    #[must_use]
    pub const fn new(target: H, map: fn(P) -> T) -> Self {
        Self { target, map }
    }
}

impl<P, T, H> Target<P> for MappedTarget<P, T, H>
where
    H: Target<T>,
{
    fn is_closed(&self) -> bool {
        self.target.is_closed()
    }

    fn can_accept(&self, policy: DeliveryPolicy) -> bool {
        self.target.can_accept(policy)
    }

    fn push(&self, payload: P, policy: DeliveryPolicy) -> PushResult {
        self.target.push((self.map)(payload), policy)
    }

    fn register_waker(&self, waker: &Waker) -> bool {
        self.target.register_waker(waker)
    }

    fn unregister_waker(&self, waker: &Waker) {
        self.target.unregister_waker(waker)
    }

    fn wake_waiters(&self) {
        self.target.wake_waiters()
    }
}

/// A no-alloc route with caller-owned subscriber storage.
pub struct HeaplessRoute<'target, S: EventSpec, const SUBSCRIBERS: usize> {
    subscribers: CriticalCell<Vec<StaticSubscriber<'target, S::Payload>, SUBSCRIBERS>>,
    next_subscriber: CriticalCell<u64>,
    marker: PhantomData<fn() -> S>,
}

#[derive(Clone)]
struct StaticSubscriber<'a, P> {
    id: u64,
    target: TargetHandle<'a, P>,
    policy: DeliveryPolicy,
}

impl<'target, S: EventSpec, const SUBSCRIBERS: usize> HeaplessRoute<'target, S, SUBSCRIBERS> {
    /// Creates empty caller-owned route storage.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            subscribers: CriticalCell::new(Vec::new()),
            next_subscriber: CriticalCell::new(1),
            marker: PhantomData,
        }
    }

    /// Returns a copyable typed route handle.
    #[must_use]
    pub fn handle<'route>(&'route self) -> HeaplessRouteRef<'route, 'target, S, SUBSCRIBERS> {
        HeaplessRouteRef { route: self }
    }

    /// Returns a typed sender handle.
    #[must_use]
    pub fn sender<'route>(&'route self) -> HeaplessSender<'route, 'target, S, SUBSCRIBERS>
    where
        S::Payload: Clone,
    {
        Sender::new(self.handle())
    }

    /// Connects a caller-owned target.
    pub fn subscribe<'route>(
        &'route self,
        target: TargetHandle<'target, S::Payload>,
        policy: DeliveryPolicy,
    ) -> Result<HeaplessSubscription<'route, 'target, S, SUBSCRIBERS>, TopicError> {
        let id = self.next_subscriber.with_mut(|next| {
            let id = *next;
            *next = id.wrapping_add(1);
            id
        });
        self.subscribers
            .with_mut(|subscribers| subscribers.push(StaticSubscriber { id, target, policy }))
            .map_err(|_| TopicError::Capacity)?;
        Ok(HeaplessSubscription { route: self, id })
    }

    fn snapshot(&self) -> Vec<StaticSubscriber<'target, S::Payload>, SUBSCRIBERS>
    where
        S::Payload: Clone,
    {
        self.subscribers.with(|subscribers| {
            let mut snapshot = Vec::new();
            for subscriber in subscribers.iter() {
                let _ = snapshot.push(subscriber.clone());
            }
            snapshot
        })
    }

    fn try_send_inner(&self, payload: S::Payload) -> Result<(), TrySendError<S::Payload>>
    where
        S::Payload: Clone,
    {
        let subscribers = self.snapshot();
        if subscribers.is_empty() {
            return Ok(());
        }
        for subscriber in &subscribers {
            if subscriber.target.is_closed() {
                return Err(TrySendError::Closed(payload));
            }
            if subscriber.policy == DeliveryPolicy::Reliable
                && !subscriber.target.can_accept(subscriber.policy)
            {
                return Err(TrySendError::Full(payload));
            }
        }

        let mut remaining = Some(payload);
        for (index, subscriber) in subscribers.iter().enumerate() {
            let value = if index + 1 == subscribers.len() {
                remaining
                    .take()
                    .expect("final route target owns the payload")
            } else {
                remaining
                    .as_ref()
                    .expect("route payload remains until the final target")
                    .clone()
            };
            match subscriber.target.push(value, subscriber.policy) {
                PushResult::Accepted | PushResult::Dropped => {}
                PushResult::Closed | PushResult::Full => {
                    return Err(TrySendError::PushFailed);
                }
            }
        }
        Ok(())
    }

    fn poll_send_inner(
        &self,
        context: &mut Context<'_>,
        payload: &mut Option<S::Payload>,
    ) -> Poll<Result<(), SendError>>
    where
        S::Payload: Clone,
    {
        let subscribers = self.snapshot();
        if subscribers
            .iter()
            .any(|subscriber| subscriber.target.is_closed())
        {
            return Poll::Ready(Err(SendError::Closed));
        }
        if subscribers.iter().any(|subscriber| {
            subscriber.policy == DeliveryPolicy::Reliable
                && !subscriber.target.can_accept(subscriber.policy)
        }) {
            let waiters_full = subscribers.iter().any(|subscriber| {
                subscriber.policy == DeliveryPolicy::Reliable
                    && !subscriber.target.register_waker(context.waker())
            });
            if waiters_full {
                for subscriber in &subscribers {
                    if subscriber.policy == DeliveryPolicy::Reliable {
                        subscriber.target.unregister_waker(context.waker());
                    }
                }
                return Poll::Ready(Err(SendError::WaitersFull));
            }
            return Poll::Pending;
        }
        let value = payload.take().expect("pending send retains its payload");
        match self.try_send_inner(value) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(TrySendError::Full(value)) => {
                *payload = Some(value);
                let waiters_full = subscribers.iter().any(|subscriber| {
                    subscriber.policy == DeliveryPolicy::Reliable
                        && !subscriber.target.register_waker(context.waker())
                });
                if waiters_full {
                    for subscriber in &subscribers {
                        if subscriber.policy == DeliveryPolicy::Reliable {
                            subscriber.target.unregister_waker(context.waker());
                        }
                    }
                    Poll::Ready(Err(SendError::WaitersFull))
                } else {
                    Poll::Pending
                }
            }
            Err(TrySendError::Closed(_)) => Poll::Ready(Err(SendError::Closed)),
            Err(TrySendError::PushFailed) => Poll::Ready(Err(SendError::PushFailed)),
        }
    }

    fn unsubscribe(&self, id: u64) {
        let removed = self.subscribers.with_mut(|subscribers| {
            subscribers
                .iter()
                .position(|subscriber| subscriber.id == id)
                .map(|index| subscribers.swap_remove(index))
        });
        if let Some(removed) = removed {
            removed.target.wake_waiters();
        }
    }
}

impl<'target, S: EventSpec, const SUBSCRIBERS: usize> Default
    for HeaplessRoute<'target, S, SUBSCRIBERS>
{
    fn default() -> Self {
        Self::new()
    }
}

/// Copyable handle for a [`HeaplessRoute`].
///
/// The route borrow lifetime and the target-storage lifetime are independent:
/// a short-lived sender may borrow a route whose caller-owned targets live
/// longer.
pub struct HeaplessRouteRef<'route, 'target, S: EventSpec, const SUBSCRIBERS: usize> {
    route: &'route HeaplessRoute<'target, S, SUBSCRIBERS>,
}

impl<S: EventSpec, const SUBSCRIBERS: usize> Copy for HeaplessRouteRef<'_, '_, S, SUBSCRIBERS> {}

impl<S: EventSpec, const SUBSCRIBERS: usize> Clone for HeaplessRouteRef<'_, '_, S, SUBSCRIBERS> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'route, 'target, S: EventSpec, const SUBSCRIBERS: usize>
    HeaplessRouteRef<'route, 'target, S, SUBSCRIBERS>
{
    /// Connects a caller-owned target.
    pub fn subscribe(
        &self,
        target: TargetHandle<'target, S::Payload>,
        policy: DeliveryPolicy,
    ) -> Result<HeaplessSubscription<'route, 'target, S, SUBSCRIBERS>, TopicError> {
        self.route.subscribe(target, policy)
    }
}

/// Subscription guard for a heapless route.
pub struct HeaplessSubscription<'route, 'target, S: EventSpec, const SUBSCRIBERS: usize> {
    route: &'route HeaplessRoute<'target, S, SUBSCRIBERS>,
    id: u64,
}

impl<S: EventSpec, const SUBSCRIBERS: usize> Drop for HeaplessSubscription<'_, '_, S, SUBSCRIBERS> {
    fn drop(&mut self) {
        self.route.unsubscribe(self.id);
    }
}

/// Common route operations used by senders.
pub trait Route<S: EventSpec> {
    /// Sends without waiting.
    fn try_send(&self, payload: S::Payload) -> Result<(), TrySendError<S::Payload>>;

    /// Polls a reliable send.
    fn poll_send(
        &self,
        context: &mut Context<'_>,
        payload: &mut Option<S::Payload>,
    ) -> Poll<Result<(), SendError>>;

    /// Removes a sender waker retained by this route.
    fn unregister_waker(&self, _waker: &Waker) {}
}

/// A zero-storage route for a graph whose single target is fixed at
/// construction time.
pub struct DirectRoute<S: EventSpec, T: Target<S::Payload>> {
    target: T,
    policy: DeliveryPolicy,
    marker: PhantomData<fn() -> S>,
}

impl<S: EventSpec, T: Target<S::Payload>> DirectRoute<S, T> {
    /// Creates a route with one caller-owned target.
    #[must_use]
    pub const fn new(target: T, policy: DeliveryPolicy) -> Self {
        Self {
            target,
            policy,
            marker: PhantomData,
        }
    }

    /// Returns a borrowed sender for this fixed route.
    #[must_use]
    pub fn sender(&self) -> DirectSender<'_, S, T> {
        Sender::new(DirectRouteRef { route: self })
    }
}

/// Borrowed handle for a [`DirectRoute`].
pub struct DirectRouteRef<'a, S: EventSpec, T: Target<S::Payload>> {
    route: &'a DirectRoute<S, T>,
}

impl<S: EventSpec, T: Target<S::Payload>> Copy for DirectRouteRef<'_, S, T> {}

impl<S: EventSpec, T: Target<S::Payload>> Clone for DirectRouteRef<'_, S, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: EventSpec, T: Target<S::Payload>> Route<S> for DirectRouteRef<'_, S, T> {
    fn try_send(&self, payload: S::Payload) -> Result<(), TrySendError<S::Payload>> {
        if self.route.target.is_closed() {
            return Err(TrySendError::Closed(payload));
        }
        if self.route.policy == DeliveryPolicy::Reliable
            && !self.route.target.can_accept(self.route.policy)
        {
            return Err(TrySendError::Full(payload));
        }
        match self.route.target.push(payload, self.route.policy) {
            PushResult::Accepted | PushResult::Dropped => Ok(()),
            PushResult::Closed | PushResult::Full => Err(TrySendError::PushFailed),
        }
    }

    fn poll_send(
        &self,
        context: &mut Context<'_>,
        payload: &mut Option<S::Payload>,
    ) -> Poll<Result<(), SendError>> {
        if self.route.target.is_closed() {
            return Poll::Ready(Err(SendError::Closed));
        }
        if self.route.policy == DeliveryPolicy::Reliable
            && !self.route.target.can_accept(self.route.policy)
        {
            if !self.route.target.register_waker(context.waker()) {
                return Poll::Ready(Err(SendError::WaitersFull));
            }
            return Poll::Pending;
        }
        let value = payload.take().expect("pending direct send retains payload");
        match self.route.target.push(value, self.route.policy) {
            PushResult::Accepted | PushResult::Dropped => Poll::Ready(Ok(())),
            PushResult::Closed => Poll::Ready(Err(SendError::Closed)),
            PushResult::Full => Poll::Ready(Err(SendError::PushFailed)),
        }
    }

    fn unregister_waker(&self, waker: &Waker) {
        self.route.target.unregister_waker(waker)
    }
}

impl<'route, 'target, S: EventSpec, const SUBSCRIBERS: usize> Route<S>
    for HeaplessRouteRef<'route, 'target, S, SUBSCRIBERS>
where
    S::Payload: Clone,
{
    fn try_send(&self, payload: S::Payload) -> Result<(), TrySendError<S::Payload>> {
        self.route.try_send_inner(payload)
    }

    fn poll_send(
        &self,
        context: &mut Context<'_>,
        payload: &mut Option<S::Payload>,
    ) -> Poll<Result<(), SendError>> {
        self.route.poll_send_inner(context, payload)
    }

    fn unregister_waker(&self, waker: &Waker) {
        let subscribers = self.route.snapshot();
        for subscriber in subscribers {
            if subscriber.policy == DeliveryPolicy::Reliable {
                subscriber.target.unregister_waker(waker);
            }
        }
    }
}

/// Typed sender over any route implementation.
pub struct Sender<S: EventSpec, R: Route<S> + Clone> {
    route: R,
    marker: PhantomData<fn() -> S>,
}

impl<S: EventSpec, R: Route<S> + Clone> Clone for Sender<S, R> {
    fn clone(&self) -> Self {
        Self {
            route: self.route.clone(),
            marker: PhantomData,
        }
    }
}

impl<S: EventSpec, R: Route<S> + Clone> Sender<S, R> {
    /// Creates a sender from a route handle.
    #[must_use]
    pub const fn new(route: R) -> Self {
        Self {
            route,
            marker: PhantomData,
        }
    }

    /// Returns the contract identity carried by this sender.
    #[must_use]
    pub const fn id(&self) -> crate::EventId {
        S::ID
    }

    /// Sends without waiting.
    pub fn try_send(&self, payload: S::Payload) -> Result<(), TrySendError<S::Payload>> {
        self.route.try_send(payload)
    }

    /// Sends with a concrete core future.
    pub fn send(&self, payload: S::Payload) -> SendFuture<'_, S, Self> {
        SendFuture {
            sender: self,
            payload: Some(payload),
            waker: None,
        }
    }
}

impl<S: EventSpec, R: Route<S> + Clone> EventSender<S> for Sender<S, R> {
    fn try_send(&self, payload: S::Payload) -> Result<(), TrySendError<S::Payload>> {
        self.route.try_send(payload)
    }

    fn poll_send(
        &self,
        context: &mut Context<'_>,
        payload: &mut Option<S::Payload>,
    ) -> Poll<Result<(), SendError>> {
        self.route.poll_send(context, payload)
    }

    fn unregister_waker(&self, waker: &Waker) {
        self.route.unregister_waker(waker)
    }
}

/// Typed sender behavior shared by static and allocating backends.
pub trait EventSender<S: EventSpec>: Clone {
    /// Sends without waiting.
    fn try_send(&self, payload: S::Payload) -> Result<(), TrySendError<S::Payload>>;

    /// Polls a reliable send.
    fn poll_send(
        &self,
        context: &mut Context<'_>,
        payload: &mut Option<S::Payload>,
    ) -> Poll<Result<(), SendError>>;

    /// Removes a sender waker retained by this sender's route.
    fn unregister_waker(&self, _waker: &Waker) {}

    /// Sends with a concrete core future.
    fn send(&self, payload: S::Payload) -> SendFuture<'_, S, Self>
    where
        Self: Sized,
    {
        SendFuture {
            sender: self,
            payload: Some(payload),
            waker: None,
        }
    }
}

/// Generic event publication capability implemented by generated ports.
pub trait EventPort<S: EventSpec>: Port {
    /// Backend sender retained by this sink.
    type Sender: EventSender<S>;

    /// Returns the backend sender for this event.
    fn event_sender(&self) -> &Self::Sender;
}

/// Typed event receive capability implemented only for declared events.
pub trait EventReceiver<S: EventSpec>: Port {
    /// Receives one event payload without waiting.
    fn event_try_recv(&self) -> Option<S::Payload>;

    /// Receives one event payload using an executor-neutral future.
    fn event_recv(&self) -> impl Future<Output = Option<S::Payload>> + '_;
}

/// Typed publication and reception for any port implementation.
pub trait PortExt: Port {
    /// Sends one event without waiting.
    fn try_publish<E>(&self, event: EventValue<E>) -> Result<(), TrySendError<E::Payload>>
    where
        E: EventSpec,
        Self: EventPort<E>,
    {
        <Self as EventPort<E>>::event_sender(self).try_send(event.into_inner())
    }

    /// Sends one event with a concrete core future.
    fn publish<E>(&self, event: EventValue<E>) -> SendFuture<'_, E, <Self as EventPort<E>>::Sender>
    where
        E: EventSpec,
        Self: EventPort<E>,
    {
        <Self as EventPort<E>>::event_sender(self).send(event.into_inner())
    }

    /// Receives one event payload without waiting.
    fn try_recv<E>(&self) -> Option<E::Payload>
    where
        E: EventSpec,
        Self: EventReceiver<E>,
    {
        <Self as EventReceiver<E>>::event_try_recv(self)
    }

    /// Receives one event payload using an executor-neutral future.
    fn recv<E>(&self) -> impl Future<Output = Option<E::Payload>> + '_
    where
        E: EventSpec,
        Self: EventReceiver<E>,
    {
        <Self as EventReceiver<E>>::event_recv(self)
    }
}

impl<P: Port + ?Sized> PortExt for P {}

/// Publishes one event through a generic [`EventPort`].
///
/// The tagged value selects the event contract. This helper is allocation-free
/// and executor-neutral; the caller decides how to handle [`SendError`].
pub async fn publish<S, E>(sink: &S, event: EventValue<E>) -> Result<(), SendError>
where
    E: EventSpec,
    S: EventPort<E>,
{
    <S as PortExt>::publish(sink, event).await
}

/// Future returned by [`EventSender::send`].
pub struct SendFuture<'a, S: EventSpec, E: EventSender<S>> {
    sender: &'a E,
    payload: Option<S::Payload>,
    waker: Option<Waker>,
}

impl<S: EventSpec, E: EventSender<S>> Unpin for SendFuture<'_, S, E> {}

impl<S: EventSpec, E: EventSender<S>> Future for SendFuture<'_, S, E> {
    type Output = Result<(), SendError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(previous) = this.waker.as_ref() {
            if !previous.will_wake(context.waker()) {
                this.sender.unregister_waker(previous);
            }
        }
        this.waker = Some(context.waker().clone());
        let result = this.sender.poll_send(context, &mut this.payload);
        if result.is_ready() {
            if let Some(waker) = this.waker.take() {
                this.sender.unregister_waker(&waker);
            }
        }
        result
    }
}

impl<S: EventSpec, E: EventSender<S>> Drop for SendFuture<'_, S, E> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            self.sender.unregister_waker(&waker);
        }
    }
}

/// Typed topic wrapper over a sender handle.
pub struct Topic<S: EventSpec, E: EventSender<S>> {
    sender: E,
    marker: PhantomData<fn() -> S>,
}

impl<S: EventSpec, E: EventSender<S>> Clone for Topic<S, E> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            marker: PhantomData,
        }
    }
}

impl<S: EventSpec, E: EventSender<S>> Topic<S, E> {
    /// Creates a topic from a sender handle.
    #[must_use]
    pub const fn new(sender: E) -> Self {
        Self {
            sender,
            marker: PhantomData,
        }
    }

    /// Returns a sender handle.
    #[must_use]
    pub fn sender(&self) -> E {
        self.sender.clone()
    }

    /// Sends without waiting.
    pub fn try_send(&self, payload: S::Payload) -> Result<(), TrySendError<S::Payload>> {
        self.sender.try_send(payload)
    }

    /// Sends with a concrete core future.
    pub fn send(&self, payload: S::Payload) -> SendFuture<'_, S, E> {
        self.sender.send(payload)
    }
}

/// A fabric binding for one event specification.
pub trait PortBinding<'a, S: EventSpec> {
    /// Typed sender handle.
    type Sender: EventSender<S> + Clone;
    /// Subscription guard returned by this backend.
    type Subscription;

    /// Resolves a sender.
    fn sender(&self) -> Result<Self::Sender, TopicError>;

    /// Registers one mapped subscriber target.
    fn subscribe_mapped<T, H>(
        &self,
        target: &'a MappedTarget<S::Payload, T, H>,
        policy: DeliveryPolicy,
    ) -> Result<Self::Subscription, TopicError>
    where
        T: 'a,
        H: Target<T> + Clone + Sync + 'a;
}

/// Binding contract for an allocating backend.
///
/// The extra thread-safety bounds apply only to backends that share endpoint
/// storage across threads. They are deliberately separate from the no-alloc
/// [`PortBinding`] contract.
pub trait AllocPortBinding<'a, S: EventSpec>
where
    S::Payload: Send + 'static,
{
    /// Typed sender handle.
    type Sender: EventSender<S> + Clone;
    /// Subscription guard returned by this backend.
    type Subscription;

    /// Resolves a sender.
    fn sender(&self) -> Result<Self::Sender, TopicError>;

    /// Registers one mapped subscriber target.
    fn subscribe_mapped<T, H>(
        &self,
        target: &'a MappedTarget<S::Payload, T, H>,
        policy: DeliveryPolicy,
    ) -> Result<Self::Subscription, TopicError>
    where
        T: Send + 'static,
        H: Target<T> + Clone + Send + Sync + 'static;
}

/// Creates receiver storage owned by a generated port.
///
/// This contract is intentionally separate from [`PortBinding`]: implementing
/// it may allocate, but allocation is a backend choice.
pub trait OwnedStorage<T, const CAPACITY: usize> {
    /// Receiver value retained by the generated endpoint.
    type Receiver: Receiver<T> + 'static;

    /// Creates an empty receiver.
    fn new_receiver() -> Self::Receiver;
}

/// Owns senders and subscription guards for a generated port.
///
/// Returned associated values must not borrow the fabric. This lets a port
/// instance own its complete subscription lifetime and drop it as one value.
pub trait OwnedPortBinding<S: EventSpec>
where
    S::Payload: Send + 'static,
{
    /// Typed sender retained by an owned port.
    type Sender: EventSender<S> + 'static;
    /// Subscription guard retained by an owned port.
    type Subscription: 'static;

    /// Resolves a sender that owns its route handle.
    fn sender_owned(&self) -> Result<Self::Sender, TopicError>;

    /// Registers a mapped target without borrowing the fabric in the result.
    fn subscribe_owned<T, H>(
        &self,
        target: &MappedTarget<S::Payload, T, H>,
        policy: DeliveryPolicy,
    ) -> Result<Self::Subscription, TopicError>
    where
        T: Send + 'static,
        H: Target<T> + Clone + Send + Sync + 'static;
}

/// Receiver storage and subscription owned for one event by a generated port.
pub struct OwnedEndpoint<F, S, const CAPACITY: usize>
where
    S: EventSpec,
    F: OwnedStorage<S::Payload, CAPACITY> + OwnedPortBinding<S>,
{
    receiver: <F as OwnedStorage<S::Payload, CAPACITY>>::Receiver,
    _subscription: <F as OwnedPortBinding<S>>::Subscription,
}

impl<F, S, const CAPACITY: usize> OwnedEndpoint<F, S, CAPACITY>
where
    S: EventSpec,
    F: OwnedStorage<S::Payload, CAPACITY> + OwnedPortBinding<S>,
    <<F as OwnedStorage<S::Payload, CAPACITY>>::Receiver as Receiver<S::Payload>>::Target:
        Target<S::Payload> + Clone + Send + Sync + 'static,
{
    /// Creates receiver storage and subscribes it to one event route.
    pub fn bind(factory: &F) -> Result<Self, crate::PortError> {
        let receiver = <F as OwnedStorage<S::Payload, CAPACITY>>::new_receiver();
        if let Some(actual) =
            <<F as OwnedStorage<S::Payload, CAPACITY>>::Receiver as Receiver<S::Payload>>::CAPACITY
        {
            if actual != CAPACITY {
                return Err(crate::PortError::ReceiverCapacity {
                    expected: CAPACITY,
                    actual,
                });
            }
        }
        let target = MappedTarget::new(receiver.target(), identity::<S::Payload>);
        let subscription = <F as OwnedPortBinding<S>>::subscribe_owned(
            factory,
            &target,
            DeliveryPolicy::Reliable,
        )?;
        Ok(Self {
            receiver,
            _subscription: subscription,
        })
    }

    /// Receives one payload without waiting.
    pub fn try_recv(&self) -> Option<S::Payload> {
        self.receiver.try_recv()
    }

    /// Receives one payload using an executor-neutral future.
    pub fn recv(
        &self,
    ) -> Receive<'_, <F as OwnedStorage<S::Payload, CAPACITY>>::Receiver, S::Payload> {
        receive(&self.receiver)
    }
}

fn identity<T>(value: T) -> T {
    value
}

/// A fabric value that can own or borrow any storage backend.
pub struct Hiway<F> {
    fabric: F,
}

impl<F> Hiway<F> {
    /// Wraps a caller-owned or allocating fabric backend.
    #[must_use]
    pub const fn with_fabric(fabric: F) -> Self {
        Self { fabric }
    }

    /// Borrows the underlying fabric backend.
    #[must_use]
    pub fn fabric(&self) -> &F {
        &self.fabric
    }

    /// Resolves a typed sender.
    pub fn sender<'a, S>(&'a self) -> Result<<F as PortBinding<'a, S>>::Sender, TopicError>
    where
        S: EventSpec,
        F: PortBinding<'a, S>,
    {
        self.fabric.sender()
    }

    /// Resolves a typed topic.
    pub fn topic<'a, S>(&'a self) -> Result<Topic<S, <F as PortBinding<'a, S>>::Sender>, TopicError>
    where
        S: EventSpec,
        F: PortBinding<'a, S>,
    {
        Ok(Topic::new(self.sender()?))
    }

    /// Registers a mapped subscriber target.
    pub fn subscribe_mapped<'a, S, T, H>(
        &'a self,
        target: &'a MappedTarget<S::Payload, T, H>,
        policy: DeliveryPolicy,
    ) -> Result<<F as PortBinding<'a, S>>::Subscription, TopicError>
    where
        S: EventSpec,
        F: PortBinding<'a, S>,
        T: 'a,
        H: Target<T> + Clone + Sync + 'a,
    {
        self.fabric.subscribe_mapped(target, policy)
    }
}

/// Storage for a subscriber-side transform input.
pub struct TransformStorage<I, P>
where
    I: Receiver<P>,
    I::Target: Target<P>,
{
    inbox: I,
    target: MappedTarget<P, P, I::Target>,
}

impl<I, P> TransformStorage<I, P>
where
    I: Receiver<P>,
    I::Target: Target<P>,
{
    /// Builds transform storage from explicit caller-owned parts.
    #[must_use]
    pub const fn from_parts(inbox: I, target: MappedTarget<P, P, I::Target>) -> Self {
        Self { inbox, target }
    }

    /// Creates transform storage around one caller-owned receiver.
    #[must_use]
    pub fn new(inbox: I) -> Self {
        let target = MappedTarget::new(inbox.target(), |payload| payload);
        Self { inbox, target }
    }

    /// Returns the receiver storage.
    #[must_use]
    pub fn receiver(&self) -> &I {
        &self.inbox
    }

    #[cfg(feature = "std")]
    pub(crate) fn target(&self) -> &MappedTarget<P, P, I::Target> {
        &self.target
    }
}

/// Subscriber-side typed transform runner.
pub struct TransformSubscriber<'fabric, 'target, F, Input, Output, T, I>
where
    Input: EventSpec,
    Output: EventSpec,
    I: Receiver<Input::Payload>,
    I::Target: Target<Input::Payload> + Sync,
    F: 'fabric + PortBinding<'target, Input> + PortBinding<'target, Output>,
    T: TransformOp<Input::Payload, Output = Output::Payload>,
{
    storage: &'target TransformStorage<I, Input::Payload>,
    _subscription: <F as PortBinding<'target, Input>>::Subscription,
    output: <F as PortBinding<'target, Output>>::Sender,
    transform: T,
    _fabric: PhantomData<&'fabric F>,
}

impl<'fabric, 'target, F, Input, Output, T, I>
    TransformSubscriber<'fabric, 'target, F, Input, Output, T, I>
where
    Input: EventSpec,
    Output: EventSpec,
    I: Receiver<Input::Payload>,
    I::Target: Target<Input::Payload> + Sync,
    F: 'fabric + PortBinding<'target, Input> + PortBinding<'target, Output>,
    T: TransformOp<Input::Payload, Output = Output::Payload>,
{
    /// Connects one bounded input receiver to one typed output sender.
    pub fn new(
        fabric: &'fabric F,
        storage: &'target TransformStorage<I, Input::Payload>,
        transform: T,
    ) -> Result<Self, TopicError> {
        let subscription = <F as PortBinding<'target, Input>>::subscribe_mapped::<
            Input::Payload,
            I::Target,
        >(fabric, &storage.target, DeliveryPolicy::Reliable)?;
        let output = <F as PortBinding<'target, Output>>::sender(fabric)?;
        Ok(Self {
            storage,
            _subscription: subscription,
            output,
            transform,
            _fabric: PhantomData,
        })
    }

    /// Runs until the input closes or output delivery fails.
    pub async fn run(self) -> Result<(), SendError> {
        while let Some(input) = receive(self.storage.receiver()).await {
            let output = self.transform.apply(input).await;
            self.output.send(output).await?;
        }
        Ok(())
    }
}

/// Single-event heapless fabric helper.
pub struct StaticFabric<'route, 'target, S: EventSpec, const SUBSCRIBERS: usize> {
    route: &'route HeaplessRoute<'target, S, SUBSCRIBERS>,
}

impl<'route, 'target, S: EventSpec, const SUBSCRIBERS: usize>
    StaticFabric<'route, 'target, S, SUBSCRIBERS>
{
    /// Creates a fabric binding around a caller-owned route.
    #[must_use]
    pub const fn new(route: &'route HeaplessRoute<'target, S, SUBSCRIBERS>) -> Self {
        Self { route }
    }
}

impl<'route, 'target, S: EventSpec, const SUBSCRIBERS: usize> PortBinding<'target, S>
    for StaticFabric<'route, 'target, S, SUBSCRIBERS>
where
    S::Payload: Clone,
{
    type Sender = HeaplessSender<'route, 'target, S, SUBSCRIBERS>;
    type Subscription = HeaplessSubscription<'route, 'target, S, SUBSCRIBERS>;

    fn sender(&self) -> Result<Self::Sender, TopicError> {
        Ok(self.route.sender())
    }

    fn subscribe_mapped<T, H>(
        &self,
        target: &'target MappedTarget<S::Payload, T, H>,
        policy: DeliveryPolicy,
    ) -> Result<Self::Subscription, TopicError>
    where
        T: 'target,
        H: Target<T> + Clone + Sync + 'target,
    {
        self.route.subscribe(target, policy)
    }
}

/// Sender for a heapless route.
///
/// The first lifetime is the sender's route borrow; the second is the
/// lifetime of caller-owned subscriber targets.
pub type HeaplessSender<'route, 'target, S, const SUBSCRIBERS: usize> =
    Sender<S, HeaplessRouteRef<'route, 'target, S, SUBSCRIBERS>>;

/// Sender for a fixed one-target route.
pub type DirectSender<'a, S, T> = Sender<S, DirectRouteRef<'a, S, T>>;

/// Marker implemented by generated endpoints.
pub trait Port {}
