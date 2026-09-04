use std::{
    any::Any,
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
    task::{Context, Poll, Waker},
};

use arc_swap::ArcSwap;
use heapless::Deque;

use crate::{
    error::{SendError, TopicError, TrySendError},
    local::{
        AllocPortBinding, DeliveryPolicy, EventSender, MappedTarget, OwnedPortBinding,
        OwnedStorage, PushResult, Receiver, Route, Sender, Target, Topic, TransformStorage,
    },
    transform::TransformOp,
    EventSpec,
};

/// A dynamically allocated, thread-safe bounded inbox.
pub struct SharedInbox<T, const CAPACITY: usize = 64> {
    state: Arc<Mutex<SharedInboxState<T, CAPACITY>>>,
}

/// Shared target handle used by the allocating backend.
pub struct SharedTarget<P>(Arc<dyn Target<P> + Send + Sync>);

impl<P> Clone for SharedTarget<P> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<P> Target<P> for SharedTarget<P> {
    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    fn can_accept(&self, policy: DeliveryPolicy) -> bool {
        self.0.can_accept(policy)
    }

    fn push(&self, payload: P, policy: DeliveryPolicy) -> PushResult {
        self.0.push(payload, policy)
    }

    fn register_waker(&self, waker: &Waker) -> bool {
        self.0.register_waker(waker)
    }

    fn unregister_waker(&self, waker: &Waker) {
        self.0.unregister_waker(waker)
    }

    fn wake_waiters(&self) {
        self.0.wake_waiters()
    }
}

struct SharedInboxState<T, const CAPACITY: usize> {
    queue: Deque<T, CAPACITY>,
    closed: bool,
    receiver_waker: Option<Waker>,
    sender_wakers: Vec<Waker>,
}

impl<T, const CAPACITY: usize> Clone for SharedInbox<T, CAPACITY> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T, const CAPACITY: usize> SharedInbox<T, CAPACITY> {
    /// Creates an empty dynamically allocated inbox.
    #[must_use]
    pub fn new() -> Self {
        assert!(CAPACITY > 0, "inbox capacity must be greater than zero");
        Self {
            state: Arc::new(Mutex::new(SharedInboxState {
                queue: Deque::new(),
                closed: false,
                receiver_waker: None,
                sender_wakers: Vec::new(),
            })),
        }
    }

    /// Returns the number of queued values.
    #[must_use]
    pub fn len(&self) -> usize {
        lock_mutex(&self.state).queue.len()
    }

    /// Returns whether the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Receives one value without waiting.
    pub fn try_recv(&self) -> Option<T> {
        let (value, senders) = {
            let mut state = lock_mutex(&self.state);
            let value = state.queue.pop_front();
            let senders = if value.is_some() {
                std::mem::take(&mut state.sender_wakers)
            } else {
                Vec::new()
            };
            (value, senders)
        };
        wake_all(senders);
        value
    }

    /// Receives one value using the core future.
    pub fn recv(&self) -> impl std::future::Future<Output = Option<T>> + '_
    where
        T: Send + 'static,
    {
        crate::receive(self)
    }

    /// Closes the inbox and wakes pending operations.
    pub fn close(&self) {
        let (queued, receiver, senders) = {
            let mut state = lock_mutex(&self.state);
            state.closed = true;
            (
                std::mem::take(&mut state.queue),
                state.receiver_waker.take(),
                std::mem::take(&mut state.sender_wakers),
            )
        };
        drop(queued);
        if let Some(waker) = receiver {
            waker.wake();
        }
        wake_all(senders);
    }

    /// Polls for one value.
    pub fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Option<T>> {
        let mut receiver_waker = Some(context.waker().clone());
        let (result, stale, senders) = {
            let mut state = lock_mutex(&self.state);
            if let Some(value) = state.queue.pop_front() {
                (
                    Poll::Ready(Some(value)),
                    state.receiver_waker.take(),
                    std::mem::take(&mut state.sender_wakers),
                )
            } else if state.closed {
                (Poll::Ready(None), state.receiver_waker.take(), Vec::new())
            } else {
                let stale = state
                    .receiver_waker
                    .replace(receiver_waker.take().expect("receiver waker is available"));
                (Poll::Pending, stale, Vec::new())
            }
        };
        drop(receiver_waker);
        drop(stale);
        wake_all(senders);
        result
    }

    fn can_accept(&self, policy: DeliveryPolicy) -> bool {
        let state = lock_mutex(&self.state);
        !state.closed && (policy != DeliveryPolicy::Reliable || !state.queue.is_full())
    }

    fn is_closed(&self) -> bool {
        lock_mutex(&self.state).closed
    }

    fn push_value(&self, value: T, policy: DeliveryPolicy) -> PushResult {
        let mut discarded = Deque::new();
        let (result, receiver) = {
            let mut state = lock_mutex(&self.state);
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
                        std::mem::swap(&mut state.queue, &mut discarded);
                        let result = push_or_discard(&mut state.queue, value, &mut discarded);
                        (result, state.receiver_waker.take())
                    }
                }
            } else {
                let result = push_or_discard(&mut state.queue, value, &mut discarded);
                (result, state.receiver_waker.take())
            }
        };
        drop(discarded);
        if matches!(result, PushResult::Accepted) {
            if let Some(waker) = receiver {
                waker.wake();
            }
        }
        result
    }

    fn register_sender_waker(&self, waker: &Waker) -> bool {
        let candidate = waker.clone();
        let mut state = lock_mutex(&self.state);
        if state.closed {
            drop(state);
            drop(candidate);
            return true;
        }
        if !state
            .sender_wakers
            .iter()
            .any(|existing| existing.will_wake(waker))
        {
            state.sender_wakers.push(candidate);
            return true;
        }
        drop(state);
        drop(candidate);
        true
    }

    fn unregister_sender_waker(&self, waker: &Waker) {
        let mut state = lock_mutex(&self.state);
        state
            .sender_wakers
            .retain(|existing| !existing.will_wake(waker));
    }

    fn wake_sender_waiters(&self) {
        let senders = {
            let mut state = lock_mutex(&self.state);
            std::mem::take(&mut state.sender_wakers)
        };
        wake_all(senders);
    }
}

impl<T, const CAPACITY: usize> Default for SharedInbox<T, CAPACITY> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static, const CAPACITY: usize> Target<T> for SharedInbox<T, CAPACITY> {
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

impl<T: Send + 'static, const CAPACITY: usize> Receiver<T> for SharedInbox<T, CAPACITY> {
    type Target = SharedTarget<T>;
    const CAPACITY: Option<usize> = Some(CAPACITY);

    fn target(&self) -> Self::Target {
        SharedTarget(Arc::new(self.clone()))
    }

    fn try_recv(&self) -> Option<T> {
        self.try_recv()
    }

    fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Option<T>> {
        self.poll_recv(context)
    }
}

type DynamicTarget<P> = Arc<dyn Target<P> + Send + Sync>;

struct DynamicSubscriber<P> {
    id: u64,
    target: DynamicTarget<P>,
    policy: DeliveryPolicy,
}

struct DynamicSnapshot<P> {
    entries: Arc<[DynamicSubscriber<P>]>,
}

struct DynamicRouteInner<S: EventSpec> {
    subscribers: ArcSwap<DynamicSnapshot<S::Payload>>,
    send_gate: Mutex<()>,
    next_subscriber: AtomicU64,
}

impl<S: EventSpec> DynamicRouteInner<S> {
    fn new() -> Self {
        Self {
            subscribers: ArcSwap::from_pointee(DynamicSnapshot {
                entries: Arc::from(Vec::<DynamicSubscriber<S::Payload>>::new()),
            }),
            send_gate: Mutex::new(()),
            next_subscriber: AtomicU64::new(1),
        }
    }
}

/// An allocation-backed dynamic route.
pub struct DynamicRoute<S: EventSpec> {
    inner: Arc<DynamicRouteInner<S>>,
}

impl<S: EventSpec> Clone for DynamicRoute<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S: EventSpec> DynamicRoute<S> {
    fn subscribe_shared(
        &self,
        target: DynamicTarget<S::Payload>,
        policy: DeliveryPolicy,
    ) -> DynamicSubscription<S> {
        let id = self.inner.next_subscriber.fetch_add(1, Ordering::Relaxed);
        self.inner.subscribers.rcu(|snapshot| {
            let mut next = Vec::with_capacity(snapshot.entries.len() + 1);
            next.extend(snapshot.entries.iter().map(|subscriber| DynamicSubscriber {
                id: subscriber.id,
                target: Arc::clone(&subscriber.target),
                policy: subscriber.policy,
            }));
            next.push(DynamicSubscriber {
                id,
                target: Arc::clone(&target),
                policy,
            });
            Arc::new(DynamicSnapshot {
                entries: Arc::from(next),
            })
        });
        DynamicSubscription {
            route: self.clone(),
            id,
        }
    }

    fn has_capacity(&self) -> bool {
        self.inner
            .subscribers
            .load()
            .entries
            .iter()
            .all(|subscriber| {
                subscriber.policy != DeliveryPolicy::Reliable
                    || subscriber.target.can_accept(subscriber.policy)
            })
    }

    fn register_waker(&self, waker: &Waker) -> bool {
        let mut retained = true;
        for subscriber in self.inner.subscribers.load().entries.iter() {
            if subscriber.policy == DeliveryPolicy::Reliable {
                retained &= subscriber.target.register_waker(waker);
            }
        }
        retained
    }

    fn unregister_waker(&self, waker: &Waker) {
        for subscriber in self.inner.subscribers.load().entries.iter() {
            if subscriber.policy == DeliveryPolicy::Reliable {
                subscriber.target.unregister_waker(waker);
            }
        }
    }

    fn unsubscribe(&self, id: u64) {
        let mut removed = None;
        self.inner.subscribers.rcu(|snapshot| {
            let Some(found) = snapshot
                .entries
                .iter()
                .find(|subscriber| subscriber.id == id)
            else {
                return Arc::clone(snapshot);
            };
            removed = Some(Arc::clone(&found.target));
            let mut next = Vec::with_capacity(snapshot.entries.len().saturating_sub(1));
            next.extend(
                snapshot
                    .entries
                    .iter()
                    .filter(|subscriber| subscriber.id != id)
                    .map(|subscriber| DynamicSubscriber {
                        id: subscriber.id,
                        target: Arc::clone(&subscriber.target),
                        policy: subscriber.policy,
                    }),
            );
            Arc::new(DynamicSnapshot {
                entries: Arc::from(next),
            })
        });
        if let Some(target) = removed {
            target.wake_waiters();
        }
    }
}

impl<S: EventSpec> Route<S> for DynamicRoute<S>
where
    S: Send + Sync + 'static,
    S::Payload: Clone + Send + 'static,
{
    fn try_send(&self, payload: S::Payload) -> Result<(), TrySendError<S::Payload>> {
        let _gate = lock_mutex(&self.inner.send_gate);
        let subscribers = self.inner.subscribers.load();
        if subscribers.entries.is_empty() {
            return Ok(());
        }
        for subscriber in subscribers.entries.iter() {
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
        for (index, subscriber) in subscribers.entries.iter().enumerate() {
            let value = if index + 1 == subscribers.entries.len() {
                remaining
                    .take()
                    .expect("final dynamic target owns the payload")
            } else {
                remaining
                    .as_ref()
                    .expect("dynamic route payload remains until the final target")
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

    fn poll_send(
        &self,
        context: &mut Context<'_>,
        payload: &mut Option<S::Payload>,
    ) -> Poll<Result<(), SendError>> {
        if self
            .inner
            .subscribers
            .load()
            .entries
            .iter()
            .any(|subscriber| subscriber.target.is_closed())
        {
            return Poll::Ready(Err(SendError::Closed));
        }
        if !self.has_capacity() {
            if !self.register_waker(context.waker()) {
                self.unregister_waker(context.waker());
                return Poll::Ready(Err(SendError::WaitersFull));
            }
            if !self.has_capacity() {
                return Poll::Pending;
            }
        }
        let value = payload
            .take()
            .expect("pending dynamic send retains payload");
        match self.try_send(value) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(TrySendError::Full(value)) => {
                *payload = Some(value);
                if !self.register_waker(context.waker()) {
                    self.unregister_waker(context.waker());
                    Poll::Ready(Err(SendError::WaitersFull))
                } else {
                    Poll::Pending
                }
            }
            Err(TrySendError::Closed(_)) => Poll::Ready(Err(SendError::Closed)),
            Err(TrySendError::PushFailed) => Poll::Ready(Err(SendError::PushFailed)),
        }
    }

    fn unregister_waker(&self, waker: &Waker) {
        DynamicRoute::unregister_waker(self, waker)
    }
}

/// Subscription guard for a dynamic route.
pub struct DynamicSubscription<S: EventSpec> {
    route: DynamicRoute<S>,
    id: u64,
}

impl<S: EventSpec> Drop for DynamicSubscription<S> {
    fn drop(&mut self) {
        self.route.unsubscribe(self.id);
    }
}

/// Sender handle for a dynamic route.
pub type DynamicSender<S> = Sender<S, DynamicRoute<S>>;

type RegistryValue = Arc<dyn Any + Send + Sync>;

/// Allocation-backed dynamic fabric.
pub struct DynamicFabric {
    topics: RwLock<HashMap<crate::EventId, RegistryValue>>,
}

impl DynamicFabric {
    /// Creates an empty dynamic fabric.
    #[must_use]
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
        }
    }

    fn route<S: EventSpec>(&self) -> Result<DynamicRoute<S>, TopicError> {
        let id = S::ID;
        if let Some(existing) = read_lock(&self.topics).get(&id).cloned() {
            return downcast_route(existing, id);
        }
        let mut topics = write_lock(&self.topics);
        if let Some(existing) = topics.get(&id).cloned() {
            return downcast_route(existing, id);
        }
        let inner = Arc::new(DynamicRouteInner::<S>::new());
        topics.insert(id, inner.clone());
        Ok(DynamicRoute { inner })
    }

    /// Resolves a dynamic typed sender.
    pub fn sender<S: EventSpec>(&self) -> Result<DynamicSender<S>, TopicError> {
        Ok(Sender::new(self.route::<S>()?))
    }

    /// Resolves a dynamic typed topic.
    pub fn topic<S: EventSpec>(&self) -> Result<Topic<S, DynamicSender<S>>, TopicError> {
        Ok(Topic::new(self.sender::<S>()?))
    }

    /// Registers a mapped subscriber in the dynamic backend.
    pub fn subscribe_mapped<'a, S, T, H>(
        &'a self,
        target: &'a MappedTarget<S::Payload, T, H>,
        policy: DeliveryPolicy,
    ) -> Result<DynamicSubscription<S>, TopicError>
    where
        S: EventSpec,
        T: Send + 'static,
        H: Target<T> + Clone + Send + Sync + 'static,
    {
        <Self as AllocPortBinding<'a, S>>::subscribe_mapped(self, target, policy)
    }
}

impl Default for DynamicFabric {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, S: EventSpec> AllocPortBinding<'a, S> for DynamicFabric {
    type Sender = DynamicSender<S>;
    type Subscription = DynamicSubscription<S>;

    fn sender(&self) -> Result<Self::Sender, TopicError> {
        self.sender::<S>()
    }

    fn subscribe_mapped<T, H>(
        &self,
        target: &'a MappedTarget<S::Payload, T, H>,
        policy: DeliveryPolicy,
    ) -> Result<Self::Subscription, TopicError>
    where
        T: Send + 'static,
        H: Target<T> + Clone + Send + Sync + 'static,
    {
        let route = self.route::<S>()?;
        let target: DynamicTarget<S::Payload> = Arc::new((*target).clone());
        Ok(route.subscribe_shared(target, policy))
    }
}

impl<T, const CAPACITY: usize> OwnedStorage<T, CAPACITY> for DynamicFabric
where
    T: Send + 'static,
{
    type Receiver = SharedInbox<T, CAPACITY>;

    fn new_receiver() -> Self::Receiver {
        SharedInbox::new()
    }
}

impl<S: EventSpec> OwnedPortBinding<S> for DynamicFabric
where
    S::Payload: Send + 'static,
{
    type Sender = DynamicSender<S>;
    type Subscription = DynamicSubscription<S>;

    fn sender_owned(&self) -> Result<Self::Sender, TopicError> {
        self.sender::<S>()
    }

    fn subscribe_owned<T, H>(
        &self,
        target: &MappedTarget<S::Payload, T, H>,
        policy: DeliveryPolicy,
    ) -> Result<Self::Subscription, TopicError>
    where
        T: Send + 'static,
        H: Target<T> + Clone + Send + Sync + 'static,
    {
        let route = self.route::<S>()?;
        let target: DynamicTarget<S::Payload> = Arc::new((*target).clone());
        Ok(route.subscribe_shared(target, policy))
    }
}

impl<F> crate::Hiway<F> {
    /// Resolves a sender through an allocating backend.
    pub fn alloc_sender<'a, S>(
        &'a self,
    ) -> Result<<F as AllocPortBinding<'a, S>>::Sender, TopicError>
    where
        S: EventSpec,
        F: AllocPortBinding<'a, S>,
    {
        <F as AllocPortBinding<'a, S>>::sender(self.fabric())
    }

    /// Resolves a topic through an allocating backend.
    pub fn alloc_topic<'a, S>(
        &'a self,
    ) -> Result<Topic<S, <F as AllocPortBinding<'a, S>>::Sender>, TopicError>
    where
        S: EventSpec,
        F: AllocPortBinding<'a, S>,
    {
        Ok(Topic::new(self.alloc_sender()?))
    }

    /// Registers a mapped subscriber through an allocating backend.
    pub fn alloc_subscribe_mapped<'a, S, T, H>(
        &'a self,
        target: &'a MappedTarget<S::Payload, T, H>,
        policy: DeliveryPolicy,
    ) -> Result<<F as AllocPortBinding<'a, S>>::Subscription, TopicError>
    where
        S: EventSpec,
        F: AllocPortBinding<'a, S>,
        T: Send + 'a + 'static,
        H: Target<T> + Clone + Send + Sync + 'a + 'static,
    {
        <F as AllocPortBinding<'a, S>>::subscribe_mapped(self.fabric(), target, policy)
    }
}

/// Subscriber-side transform runner for an allocating fabric.
pub struct AllocTransformSubscriber<'fabric, 'target, F, Input, Output, T, I>
where
    Input: EventSpec,
    Output: EventSpec,
    I: Receiver<Input::Payload>,
    I::Target: Target<Input::Payload> + Send + Sync + 'static,
    F: 'fabric + AllocPortBinding<'target, Input> + AllocPortBinding<'target, Output>,
    T: TransformOp<Input::Payload, Output = Output::Payload>,
{
    storage: &'target TransformStorage<I, Input::Payload>,
    _subscription: <F as AllocPortBinding<'target, Input>>::Subscription,
    output: <F as AllocPortBinding<'target, Output>>::Sender,
    transform: T,
    _fabric: core::marker::PhantomData<&'fabric F>,
}

impl<'fabric, 'target, F, Input, Output, T, I>
    AllocTransformSubscriber<'fabric, 'target, F, Input, Output, T, I>
where
    Input: EventSpec,
    Output: EventSpec,
    I: Receiver<Input::Payload>,
    I::Target: Target<Input::Payload> + Send + Sync + 'static,
    F: 'fabric + AllocPortBinding<'target, Input> + AllocPortBinding<'target, Output>,
    T: TransformOp<Input::Payload, Output = Output::Payload>,
{
    /// Connects one bounded input receiver to one typed output sender.
    pub fn new(
        fabric: &'fabric F,
        storage: &'target TransformStorage<I, Input::Payload>,
        transform: T,
    ) -> Result<Self, TopicError> {
        let subscription = <F as AllocPortBinding<'target, Input>>::subscribe_mapped::<
            Input::Payload,
            I::Target,
        >(fabric, storage.target(), DeliveryPolicy::Reliable)?;
        let output = <F as AllocPortBinding<'target, Output>>::sender(fabric)?;
        Ok(Self {
            storage,
            _subscription: subscription,
            output,
            transform,
            _fabric: core::marker::PhantomData,
        })
    }

    /// Runs until the input closes or output delivery fails.
    pub async fn run(self) -> Result<(), SendError> {
        while let Some(input) = crate::receive(self.storage.receiver()).await {
            let output = self.transform.apply(input).await;
            EventSender::send(&self.output, output).await?;
        }
        Ok(())
    }
}

impl crate::Hiway<DynamicFabric> {
    /// Creates a Hiway backed by dynamic std storage.
    #[must_use]
    pub fn new() -> Self {
        Self::with_fabric(DynamicFabric::new())
    }
}

impl Default for crate::Hiway<DynamicFabric> {
    fn default() -> Self {
        Self::new()
    }
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

fn wake_all(wakers: Vec<Waker>) {
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

fn downcast_route<S: EventSpec>(
    entry: RegistryValue,
    id: crate::EventId,
) -> Result<DynamicRoute<S>, TopicError> {
    Arc::downcast::<DynamicRouteInner<S>>(entry)
        .map(|inner| DynamicRoute { inner })
        .map_err(|_| TopicError::TypeMismatch(crate::TopicTypeMismatch { id }))
}
