use core::{
    future::{poll_fn, Future},
    marker::PhantomData,
    mem,
    task::{Context, Poll, Waker},
};

#[cfg(all(not(loom), not(feature = "std")))]
use core::cell::RefCell;

use heapless::{Deque, Vec};

use crate::{
    event::{EventTag, TransformInput},
    stage, Batch, HiwayEvent, Identity, OutputFull, Pipeline, PipelineExt, ReadyFull, RecvError,
    Stage, SubscribersFull, Then,
};

pub const DEFAULT_OUTPUTS: usize = 4;
pub const DEFAULT_READY: usize = 8;
pub const DEFAULT_FRAMES: usize = 4;
pub const DEFAULT_SUBSCRIBERS: usize = 4;

type Frame<E, const OUTPUTS: usize, const READY: usize> = Deque<Batch<E, OUTPUTS>, READY>;

const fn ensure_send<F: Future + Send>(future: F) -> F {
    future
}

struct StateCell<T> {
    #[cfg(loom)]
    inner: loom::sync::Mutex<T>,
    #[cfg(all(not(loom), feature = "std"))]
    inner: std::sync::Mutex<T>,
    #[cfg(all(not(loom), not(feature = "std")))]
    inner: critical_section::Mutex<RefCell<T>>,
}

impl<T> StateCell<T> {
    #[cfg(not(loom))]
    const fn new(value: T) -> Self {
        Self {
            #[cfg(feature = "std")]
            inner: std::sync::Mutex::new(value),
            #[cfg(not(feature = "std"))]
            inner: critical_section::Mutex::new(RefCell::new(value)),
        }
    }

    #[cfg(loom)]
    fn new(value: T) -> Self {
        Self {
            inner: loom::sync::Mutex::new(value),
        }
    }

    fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        #[cfg(loom)]
        {
            let mut state = self.inner.lock().expect("hiway state mutex poisoned");
            f(&mut state)
        }

        #[cfg(all(not(loom), feature = "std"))]
        {
            let mut state = self.inner.lock().expect("hiway state mutex poisoned");
            f(&mut state)
        }

        #[cfg(all(not(loom), not(feature = "std")))]
        {
            critical_section::with(|critical| {
                let mut state = self.inner.borrow(critical).borrow_mut();
                f(&mut state)
            })
        }
    }
}

#[derive(Clone, Copy)]
enum Route {
    All,
    Tagged(usize),
}

impl Route {
    fn matches(self, tag: Option<usize>) -> bool {
        match self {
            Self::All => true,
            Self::Tagged(expected) => tag == Some(expected),
        }
    }
}

struct SubscriberSlot<E, const OUTPUTS: usize, const READY: usize, const FRAMES: usize> {
    active: bool,
    generation: u64,
    route: Route,
    lagged: u64,
    pending: Frame<E, OUTPUTS, READY>,
    frames: Deque<Frame<E, OUTPUTS, READY>, FRAMES>,
    waker: Option<Waker>,
}

impl<E, const OUTPUTS: usize, const READY: usize, const FRAMES: usize>
    SubscriberSlot<E, OUTPUTS, READY, FRAMES>
{
    const fn new(generation: u64) -> Self {
        Self {
            active: false,
            generation,
            route: Route::All,
            lagged: 0,
            pending: Deque::new(),
            frames: Deque::new(),
            waker: None,
        }
    }
}

struct State<
    E,
    const OUTPUTS: usize,
    const READY: usize,
    const FRAMES: usize,
    const SUBSCRIBERS: usize,
> {
    completed: usize,
    preparing: usize,
    subscribers: [SubscriberSlot<E, OUTPUTS, READY, FRAMES>; SUBSCRIBERS],
}

impl<
        E,
        const OUTPUTS: usize,
        const READY: usize,
        const FRAMES: usize,
        const SUBSCRIBERS: usize,
    > State<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>
{
    const fn new() -> Self {
        Self {
            completed: 0,
            preparing: 0,
            subscribers: [const { SubscriberSlot::new(0) }; SUBSCRIBERS],
        }
    }
}

#[derive(Clone, Copy)]
struct Target {
    slot: usize,
    generation: u64,
    route: Route,
}

struct Delivery<E, const OUTPUTS: usize, const READY: usize> {
    target: Target,
    batch: Batch<E, OUTPUTS>,
}

struct PreparationGuard<
    'a,
    E,
    const OUTPUTS: usize,
    const READY: usize,
    const FRAMES: usize,
    const SUBSCRIBERS: usize,
> {
    state: &'a StateCell<State<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>>,
    armed: bool,
}

impl<
        E,
        const OUTPUTS: usize,
        const READY: usize,
        const FRAMES: usize,
        const SUBSCRIBERS: usize,
    > Drop for PreparationGuard<'_, E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>
{
    fn drop(&mut self) {
        if self.armed {
            self.state.with(|state| state.preparing -= 1);
        }
    }
}

/// Bounded typed event bus with a compile-time asynchronous pipeline.
///
/// Transform work, event tags, event clones, and routed fanout run in the
/// publishing task. Hiway core performs no heap allocation. User transforms,
/// tags, clones, destructors, and wakers may allocate or panic.
///
/// `FRAMES` must be positive, including when `SUBSCRIBERS` is zero:
///
/// ```compile_fail
/// use hiway::Bus;
///
/// let _ = Bus::<u8, 1, 1, 0, 0>::new();
/// ```
pub struct Bus<
    E,
    const OUTPUTS: usize = DEFAULT_OUTPUTS,
    const READY: usize = DEFAULT_READY,
    const FRAMES: usize = DEFAULT_FRAMES,
    const SUBSCRIBERS: usize = DEFAULT_SUBSCRIBERS,
    P = Identity,
> {
    pub(crate) pipeline: P,
    state: StateCell<State<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>>,
}

impl<
        E,
        const OUTPUTS: usize,
        const READY: usize,
        const FRAMES: usize,
        const SUBSCRIBERS: usize,
    > Bus<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS, Identity>
where
    E: Send,
{
    /// Creates a bus that passes every event through unchanged.
    ///
    /// # Panics
    ///
    /// Panics when `OUTPUTS`, `READY`, or `FRAMES` is zero.
    #[must_use]
    #[cfg(not(loom))]
    pub const fn new() -> Self {
        Self::with_pipeline(Identity)
    }

    /// Creates a bus that passes every event through unchanged.
    ///
    /// # Panics
    ///
    /// Panics when `OUTPUTS`, `READY`, or `FRAMES` is zero.
    #[must_use]
    #[cfg(loom)]
    pub fn new() -> Self {
        Self::with_pipeline(Identity)
    }

    /// Creates a bus with one statically typed pipeline value.
    ///
    /// # Panics
    ///
    /// Panics when `OUTPUTS`, `READY`, or `FRAMES` is zero.
    #[must_use]
    #[cfg(not(loom))]
    pub const fn with_pipeline<Q>(pipeline: Q) -> Bus<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS, Q>
    where
        Q: Pipeline<E, OUTPUTS>,
    {
        assert!(
            OUTPUTS > 0,
            "event output capacity must be greater than zero"
        );
        assert!(READY > 0, "ready capacity must be greater than zero");
        assert!(FRAMES > 0, "frame capacity must be greater than zero");

        Bus {
            pipeline,
            state: StateCell::new(State::new()),
        }
    }

    /// Creates a bus with one statically typed pipeline value.
    ///
    /// # Panics
    ///
    /// Panics when `OUTPUTS`, `READY`, or `FRAMES` is zero.
    #[must_use]
    #[cfg(loom)]
    pub fn with_pipeline<Q>(pipeline: Q) -> Bus<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS, Q>
    where
        Q: Pipeline<E, OUTPUTS>,
    {
        assert!(
            OUTPUTS > 0,
            "event output capacity must be greater than zero"
        );
        assert!(READY > 0, "ready capacity must be greater than zero");
        assert!(FRAMES > 0, "frame capacity must be greater than zero");

        Bus {
            pipeline,
            state: StateCell::new(State::new()),
        }
    }
}

impl<
        E,
        P,
        const OUTPUTS: usize,
        const READY: usize,
        const FRAMES: usize,
        const SUBSCRIBERS: usize,
    > Bus<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS, P>
{
    /// Appends one typed transform stage and returns the resulting static bus.
    ///
    /// This consumes the bus because the pipeline is part of its concrete type.
    /// It performs no allocation and is available without `std`. Existing
    /// capacity and prepared state are moved unchanged; prepared batches are
    /// not run through the appended transform.
    #[must_use]
    pub fn transform<V, F, Fut, O>(
        self,
        transform: F,
    ) -> Bus<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS, Then<P, Stage<F, V>>>
    where
        E: Send,
        V: TransformInput<E> + Send,
        P: PipelineExt + Pipeline<E, OUTPUTS>,
        F: Fn(V) -> Fut + Sync,
        Fut: Future<Output = O> + Send,
        O: IntoIterator<Item = E>,
        O::IntoIter: Send,
    {
        Bus {
            pipeline: self.pipeline.then(stage(transform)),
            state: self.state,
        }
    }

    /// Transforms and prepares one publication for the next tick.
    ///
    /// After transformation, Hiway reserves `READY` capacity and snapshots
    /// active subscriber generations/routes. Variant filtering and the minimum
    /// required event clones run outside synchronization. Preparation commit
    /// order defines concurrent publication order. A successful call does not
    /// make events receivable until [`Bus::tick`] runs.
    ///
    /// A subscriber racing with the preparation snapshot may receive or miss
    /// the publication. A subscriber created after this future completes always
    /// misses it.
    ///
    /// # Errors
    ///
    /// Returns [`PublishError::OutputFull`] when the final pipeline output
    /// exceeds `OUTPUTS`, or [`PublishError::ReadyFull`] when completed
    /// publications plus active preparations already occupy `READY`.
    ///
    /// # Panics
    ///
    /// User conversions, transforms, tags, clones, and event destructors may
    /// panic. A tag or clone panic releases its preparation reservation and
    /// commits nothing.
    pub fn publish<'a, V>(
        &'a self,
        event: V,
    ) -> impl Future<Output = Result<(), PublishError<E, OUTPUTS>>> + Send + 'a
    where
        E: HiwayEvent + 'a,
        V: Into<E> + Send + 'a,
        P: Pipeline<E, OUTPUTS> + 'a,
    {
        ensure_send(async move {
            let batch = self
                .pipeline
                .apply(event.into())
                .await
                .map_err(|OutputFull| PublishError::OutputFull)?;

            self.try_submit(batch).map_err(PublishError::ReadyFull)
        })
    }

    /// Tries to prepare an already transformed publication.
    ///
    /// This performs routing and fanout without running the transform pipeline.
    ///
    /// # Errors
    ///
    /// Returns [`ReadyFull`] when completed publications plus active
    /// preparations already occupy `READY`. [`ReadyFull::into_batch`] returns
    /// the exact untouched batch for another retry.
    ///
    /// # Panics
    ///
    /// User event tags, clones, and destructors may panic. A tag or clone panic
    /// releases its preparation reservation and commits nothing.
    pub fn try_submit(&self, batch: Batch<E, OUTPUTS>) -> Result<(), ReadyFull<E, OUTPUTS>>
    where
        E: HiwayEvent,
    {
        if batch.is_empty() {
            return Ok(());
        }

        let reservation = self.state.with(|state| {
            if state.completed.saturating_add(state.preparing) >= READY {
                return None;
            }

            state.preparing += 1;
            let mut deliveries = Vec::<Delivery<E, OUTPUTS, READY>, SUBSCRIBERS>::new();
            for (slot, subscriber) in state.subscribers.iter().enumerate() {
                if subscriber.active {
                    deliveries
                        .push(Delivery {
                            target: Target {
                                slot,
                                generation: subscriber.generation,
                                route: subscriber.route,
                            },
                            batch: Batch::new(),
                        })
                        .unwrap_or_else(|_| unreachable!("one target per subscriber"));
                }
            }
            Some(deliveries)
        });
        let Some(mut deliveries) = reservation else {
            return Err(ReadyFull::new(batch));
        };
        let routed = deliveries
            .iter()
            .any(|delivery| matches!(delivery.target.route, Route::Tagged(_)));

        let mut guard = PreparationGuard {
            state: &self.state,
            armed: true,
        };

        for event in batch {
            let tag = routed.then(|| event.__hiway_tag());
            let matching = deliveries
                .iter()
                .filter(|delivery| delivery.target.route.matches(tag))
                .count();
            if matching == 0 {
                continue;
            }

            let mut remaining = matching;
            for delivery in &mut deliveries {
                if !delivery.target.route.matches(tag) {
                    continue;
                }

                if remaining == 1 {
                    delivery.batch.push(event).unwrap_or_else(|_| {
                        unreachable!("a prepared batch preserves output capacity")
                    });
                    break;
                }
                delivery
                    .batch
                    .push(event.clone())
                    .unwrap_or_else(|_| unreachable!("a prepared batch preserves output capacity"));
                remaining -= 1;
            }
        }

        self.state.with(|state| {
            for delivery in &mut deliveries {
                if delivery.batch.is_empty() {
                    continue;
                }

                let subscriber = &mut state.subscribers[delivery.target.slot];
                if !subscriber.active || subscriber.generation != delivery.target.generation {
                    continue;
                }

                let batch = mem::take(&mut delivery.batch);
                subscriber.pending.push_back(batch).unwrap_or_else(|_| {
                    unreachable!("preparation reservations bound pending storage")
                });
            }
            state.completed += 1;
            state.preparing -= 1;
        });
        guard.armed = false;

        drop(deliveries);
        Ok(())
    }

    /// Commits every completed publication currently prepared as one frame.
    ///
    /// This operation only moves prepared batches, evicts old frames, and
    /// wakes receivers. It never waits for a producer or inspects, clones, or
    /// transforms events. Frame work is bounded by active subscribers; the
    /// fixed `SUBSCRIBERS` slots are scanned once. User event destructors and
    /// wakers run outside the state critical section.
    ///
    /// `READY` counts completed publications plus active preparation
    /// reservations. This method drains only the completed subset and returns
    /// `false` when every reserved publication is still being prepared.
    ///
    /// # Panics
    ///
    /// User event destructors and wakers may panic after the state transition
    /// that selected them has committed.
    #[must_use]
    pub fn tick(&self) -> bool {
        let mut discarded = Vec::<Frame<E, OUTPUTS, READY>, SUBSCRIBERS>::new();
        let mut wake = Vec::<Waker, SUBSCRIBERS>::new();
        let committed = self.state.with(|state| {
            if state.completed == 0 {
                return false;
            }
            state.completed = 0;

            for subscriber in &mut state.subscribers {
                if subscriber.pending.is_empty() {
                    continue;
                }

                let pending = mem::take(&mut subscriber.pending);
                if subscriber.frames.is_full() {
                    let evicted = subscriber
                        .frames
                        .pop_front()
                        .expect("a full frame queue has an oldest frame");
                    subscriber.lagged = subscriber.lagged.saturating_add(1);
                    discarded
                        .push(evicted)
                        .unwrap_or_else(|_| unreachable!("one discarded frame per subscriber"));
                }
                subscriber
                    .frames
                    .push_back(pending)
                    .unwrap_or_else(|_| unreachable!("an evicted frame leaves one free slot"));
                if let Some(waker) = subscriber.waker.take() {
                    wake.push(waker)
                        .unwrap_or_else(|_| unreachable!("one waker per subscriber"));
                }
            }
            true
        });

        drop(discarded);
        for waker in wake {
            waker.wake();
        }
        committed
    }

    /// Subscribes to the full event enum.
    ///
    /// A publication racing with this call may include or exclude the new
    /// subscriber. Publications whose `publish().await` already completed are
    /// never replayed.
    ///
    /// # Errors
    ///
    /// Returns [`SubscribersFull`] when every compile-time slot is occupied.
    pub fn subscribe(
        &self,
    ) -> Result<Subscription<'_, E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>, SubscribersFull> {
        self.subscribe_with_route(Route::All)
    }

    /// Subscribes to one payload type. Filtering occurs before subscriber
    /// storage, event cloning, waking, and lag accounting.
    ///
    /// # Errors
    ///
    /// Returns [`SubscribersFull`] when every compile-time slot is occupied.
    ///
    /// # Panics
    ///
    /// A user-provided [`EventTag`] implementation may panic while selecting
    /// the route. The derive-generated implementation does not call user code.
    pub fn consume<V>(
        &self,
    ) -> Result<TypedSubscription<'_, E, V, OUTPUTS, READY, FRAMES, SUBSCRIBERS>, SubscribersFull>
    where
        E: HiwayEvent,
        V: EventTag<E> + TryFrom<E, Error = E>,
    {
        let inner = self.subscribe_with_route(Route::Tagged(V::__hiway_tag()))?;
        Ok(TypedSubscription {
            inner,
            payload: PhantomData,
        })
    }

    fn subscribe_with_route(
        &self,
        route: Route,
    ) -> Result<Subscription<'_, E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>, SubscribersFull> {
        self.state.with(|state| {
            let slot = state
                .subscribers
                .iter()
                .position(|slot| !slot.active)
                .ok_or(SubscribersFull)?;
            let subscriber = &mut state.subscribers[slot];
            subscriber.generation = subscriber.generation.wrapping_add(1);
            subscriber.active = true;
            subscriber.route = route;

            Ok(Subscription {
                state: &self.state,
                slot,
            })
        })
    }
}

impl<E> Default for Bus<E>
where
    E: Send,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Error returned while preparing a publication.
pub enum PublishError<E, const OUTPUTS: usize> {
    /// The final transform output exceeded `OUTPUTS`.
    OutputFull,
    /// The publication could not reserve preparation capacity.
    ReadyFull(ReadyFull<E, OUTPUTS>),
}

impl<E, const OUTPUTS: usize> core::fmt::Debug for PublishError<E, OUTPUTS> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OutputFull => f.write_str("OutputFull"),
            Self::ReadyFull(error) => f.debug_tuple("ReadyFull").field(error).finish(),
        }
    }
}

impl<E, const OUTPUTS: usize> core::fmt::Display for PublishError<E, OUTPUTS> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OutputFull => OutputFull.fmt(f),
            Self::ReadyFull(error) => error.fmt(f),
        }
    }
}

#[cfg(feature = "std")]
impl<E, const OUTPUTS: usize> std::error::Error for PublishError<E, OUTPUTS> {}

/// Raw subscriber receiving every committed event.
pub struct Subscription<
    'a,
    E,
    const OUTPUTS: usize,
    const READY: usize,
    const FRAMES: usize,
    const SUBSCRIBERS: usize,
> {
    state: &'a StateCell<State<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>>,
    slot: usize,
}

enum Receive<E> {
    Event(E),
    Lagged(u64),
    Empty,
}

fn receive<E, const OUTPUTS: usize, const READY: usize, const FRAMES: usize>(
    subscriber: &mut SubscriberSlot<E, OUTPUTS, READY, FRAMES>,
) -> (Receive<E>, Option<Waker>) {
    let result = if subscriber.lagged != 0 {
        Receive::Lagged(mem::take(&mut subscriber.lagged))
    } else {
        loop {
            let Some(frame) = subscriber.frames.front_mut() else {
                break Receive::Empty;
            };
            if frame.is_empty() {
                let _ = subscriber.frames.pop_front();
                continue;
            }
            let event = frame.front_mut().and_then(Batch::pop_front);
            if let Some(event) = event {
                if frame.front().is_some_and(Batch::is_empty) {
                    let _ = frame.pop_front();
                }
                if frame.is_empty() {
                    let _ = subscriber.frames.pop_front();
                }
                break Receive::Event(event);
            }
            let _ = frame.pop_front();
        }
    };

    let stale = match &result {
        Receive::Event(_) | Receive::Lagged(_) => subscriber.waker.take(),
        Receive::Empty => None,
    };
    (result, stale)
}

impl<
        E,
        const OUTPUTS: usize,
        const READY: usize,
        const FRAMES: usize,
        const SUBSCRIBERS: usize,
    > Subscription<'_, E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>
where
    E: HiwayEvent,
{
    /// Receives the next committed event.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Lagged`] when committed frames overwrite this
    /// subscriber's unread input.
    ///
    /// # Panics
    ///
    /// Cloning or dropping the caller-provided waker may panic. Those
    /// operations run outside state synchronization. Cancelling a pending
    /// receive may retain one waker until another receive, tick, or
    /// subscription drop.
    pub async fn recv(&mut self) -> Result<E, RecvError> {
        poll_fn(|context| self.poll_recv(context)).await
    }

    /// Tries to receive the next committed event without registering a waker.
    /// Any waker retained by a cancelled [`Subscription::recv`] is removed.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Lagged`] when committed frames overwrite this
    /// subscriber's unread input.
    pub fn try_recv(&mut self) -> Result<Option<E>, RecvError> {
        let (result, stale) = self.state.with(|state| {
            let subscriber = &mut state.subscribers[self.slot];
            let (result, stale) = receive(subscriber);
            (result, stale.or_else(|| subscriber.waker.take()))
        });
        drop(stale);
        match result {
            Receive::Event(event) => Ok(Some(event)),
            Receive::Lagged(frames) => Err(RecvError::Lagged(frames)),
            Receive::Empty => Ok(None),
        }
    }

    fn poll_recv(&mut self, context: &mut Context<'_>) -> Poll<Result<E, RecvError>> {
        let (first, stale) = self
            .state
            .with(|state| receive(&mut state.subscribers[self.slot]));
        drop(stale);
        match first {
            Receive::Event(event) => return Poll::Ready(Ok(event)),
            Receive::Lagged(frames) => return Poll::Ready(Err(RecvError::Lagged(frames))),
            Receive::Empty => {}
        }

        let mut replacement = Some(context.waker().clone());
        let (second, stale, replaced) = self.state.with(|state| {
            let subscriber = &mut state.subscribers[self.slot];
            let (second, stale) = receive(subscriber);
            let replaced = if matches!(second, Receive::Empty) {
                let candidate = replacement.take().expect("the replacement waker exists");
                if subscriber
                    .waker
                    .as_ref()
                    .is_none_or(|waker| !waker.will_wake(&candidate))
                {
                    subscriber.waker.replace(candidate)
                } else {
                    Some(candidate)
                }
            } else {
                None
            };
            (second, stale, replaced)
        });
        drop(replacement);
        drop(stale);
        drop(replaced);

        match second {
            Receive::Event(event) => Poll::Ready(Ok(event)),
            Receive::Lagged(frames) => Poll::Ready(Err(RecvError::Lagged(frames))),
            Receive::Empty => Poll::Pending,
        }
    }
}

impl<
        E,
        const OUTPUTS: usize,
        const READY: usize,
        const FRAMES: usize,
        const SUBSCRIBERS: usize,
    > Drop for Subscription<'_, E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>
{
    fn drop(&mut self) {
        let old = self.state.with(|state| {
            let generation = state.subscribers[self.slot].generation.wrapping_add(1);
            mem::replace(
                &mut state.subscribers[self.slot],
                SubscriberSlot::new(generation),
            )
        });
        drop(old);
    }
}

/// Subscriber receiving one pre-routed payload type.
pub struct TypedSubscription<
    'a,
    E,
    V,
    const OUTPUTS: usize,
    const READY: usize,
    const FRAMES: usize,
    const SUBSCRIBERS: usize,
> where
    E: HiwayEvent,
    V: EventTag<E> + TryFrom<E, Error = E>,
{
    inner: Subscription<'a, E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>,
    payload: PhantomData<fn() -> V>,
}

impl<
        E,
        V,
        const OUTPUTS: usize,
        const READY: usize,
        const FRAMES: usize,
        const SUBSCRIBERS: usize,
    > TypedSubscription<'_, E, V, OUTPUTS, READY, FRAMES, SUBSCRIBERS>
where
    E: HiwayEvent,
    V: EventTag<E> + TryFrom<E, Error = E>,
{
    fn into_payload(event: E) -> V {
        match V::try_from(event) {
            Ok(payload) => payload,
            Err(_other) => unreachable!("the bus routed an event to the wrong payload"),
        }
    }

    /// Receives the next payload routed to this subscription.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Lagged`] when committed frames overwrite unread
    /// input for this payload route.
    ///
    /// # Panics
    ///
    /// Panics if user-provided [`EventTag`] and [`TryFrom`] implementations
    /// disagree about the routed payload. Waker clone/drop operations have the
    /// same panic boundary as [`Subscription::recv`].
    pub async fn recv(&mut self) -> Result<V, RecvError> {
        Ok(Self::into_payload(self.inner.recv().await?))
    }

    /// Tries to receive the next routed payload without registering a waker.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Lagged`] when committed frames overwrite unread
    /// input for this payload route.
    ///
    /// # Panics
    ///
    /// Panics if user-provided [`EventTag`] and [`TryFrom`] implementations
    /// disagree about the routed payload.
    pub fn try_recv(&mut self) -> Result<Option<V>, RecvError> {
        self.inner
            .try_recv()
            .map(|event| event.map(Self::into_payload))
    }
}
