use core::{future::Future, marker::PhantomData, pin::Pin};
use std::{
    boxed::Box,
    sync::{Arc, PoisonError, RwLock},
    vec::Vec,
};

use crate::{
    event::{EventTag, TransformInput},
    Batch, Bus, HiwayEvent, OutputFull, Pipeline, PublishError, ReadyFull, SubscribersFull,
    Subscription, TypedSubscription, DEFAULT_FRAMES, DEFAULT_OUTPUTS, DEFAULT_READY,
    DEFAULT_SUBSCRIBERS,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type BoxEvents<E> = Box<dyn Iterator<Item = E> + Send + 'static>;

trait ErasedStage<E>: Send + Sync {
    fn apply(&self, event: E) -> BoxFuture<'_, BoxEvents<E>>;
}

struct RuntimeStage<F, V> {
    transform: F,
    input: PhantomData<fn(V)>,
}

impl<E, V, F, Fut, Produced> ErasedStage<E> for RuntimeStage<F, V>
where
    E: Send + 'static,
    V: TransformInput<E> + Send + 'static,
    F: Fn(V) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Produced> + Send + 'static,
    Produced: IntoIterator<Item = E> + 'static,
    Produced::IntoIter: Send + 'static,
{
    fn apply(&self, event: E) -> BoxFuture<'_, BoxEvents<E>> {
        Box::pin(async move {
            let output: BoxEvents<E> = match V::try_from_event(event) {
                Ok(input) => Box::new((self.transform)(input).await.into_iter()),
                Err(other) => Box::new(core::iter::once(other)),
            };
            output
        })
    }
}

type StageList<E> = Arc<[Arc<dyn ErasedStage<E>>]>;

struct DynamicPipeline<E> {
    stages: RwLock<StageList<E>>,
}

impl<E> DynamicPipeline<E> {
    fn new() -> Self {
        Self {
            stages: RwLock::new(Arc::from(Vec::new())),
        }
    }

    fn add<V, F, Fut, Produced>(&self, transform: F)
    where
        E: Send + 'static,
        V: TransformInput<E> + Send + 'static,
        F: Fn(V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Produced> + Send + 'static,
        Produced: IntoIterator<Item = E> + 'static,
        Produced::IntoIter: Send + 'static,
    {
        let mut stages = self.stages.write().unwrap_or_else(PoisonError::into_inner);
        let mut next: Vec<Arc<dyn ErasedStage<E>>> = Vec::with_capacity(stages.len() + 1);
        next.extend(stages.iter().cloned());
        next.push(Arc::new(RuntimeStage {
            transform,
            input: PhantomData,
        }));
        *stages = Arc::from(next);
    }
}

impl<E, const OUTPUTS: usize> Pipeline<E, OUTPUTS> for DynamicPipeline<E>
where
    E: Send + 'static,
{
    async fn apply<'a>(&'a self, event: E) -> Result<Batch<E, OUTPUTS>, OutputFull>
    where
        E: 'a,
    {
        let stages = {
            let stages = self.stages.read().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(&*stages)
        };
        let mut batch = Batch::new();
        let mut stack: Vec<(usize, BoxEvents<E>)> =
            vec![(0, Box::new(core::iter::once(event)) as BoxEvents<E>)];

        while let Some((stage_index, mut events)) = stack.pop() {
            while let Some(event) = events.next() {
                if stage_index == stages.len() {
                    batch.push(event)?;
                    continue;
                }

                stack.push((stage_index, events));
                stack.push((stage_index + 1, stages[stage_index].apply(event).await));
                break;
            }
        }

        Ok(batch)
    }
}

/// Runtime-injectable `std` adapter over the bounded portable bus.
pub struct DynamicBus<
    E,
    const OUTPUTS: usize = DEFAULT_OUTPUTS,
    const READY: usize = DEFAULT_READY,
    const FRAMES: usize = DEFAULT_FRAMES,
    const SUBSCRIBERS: usize = DEFAULT_SUBSCRIBERS,
> {
    inner: Bus<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS, DynamicPipeline<E>>,
}

impl<
        E,
        const OUTPUTS: usize,
        const READY: usize,
        const FRAMES: usize,
        const SUBSCRIBERS: usize,
    > DynamicBus<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>
where
    E: HiwayEvent + 'static,
{
    /// Creates an empty runtime transform chain.
    ///
    /// Dynamic mode is opt-in and uses `Vec`, `Arc`, boxed futures, and boxed
    /// iterators. Each publication clones one `Arc` stage-list snapshot.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Bus::with_pipeline(DynamicPipeline::new()),
        }
    }

    /// Appends one asynchronous typed transform stage.
    ///
    /// Other event variants pass through unchanged. Return no events to drop
    /// the input, one to preserve or replace it, or several to expand it.
    pub fn transform<V, F, Fut, Produced>(&self, transform: F)
    where
        V: TransformInput<E> + Send + 'static,
        F: Fn(V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Produced> + Send + 'static,
        Produced: IntoIterator<Item = E> + 'static,
        Produced::IntoIter: Send + 'static,
    {
        self.inner.pipeline.add(transform);
    }

    /// Transforms and prepares one publication for the next tick.
    ///
    /// # Errors
    ///
    /// Returns [`PublishError`] when the final transform output exceeds
    /// `OUTPUTS`, or when completed publications plus active preparations
    /// occupy `READY`.
    pub fn publish<'a, V>(
        &'a self,
        event: V,
    ) -> impl Future<Output = Result<(), PublishError<E, OUTPUTS>>> + Send + 'a
    where
        E: HiwayEvent + 'a,
        V: Into<E> + Send + 'a,
    {
        self.inner.publish(event)
    }

    /// Tries to submit an already transformed publication without rerunning
    /// runtime stages.
    ///
    /// # Errors
    ///
    /// Returns [`ReadyFull`] with the exact untouched batch when `READY` cannot
    /// admit another preparation.
    pub fn try_submit(&self, batch: Batch<E, OUTPUTS>) -> Result<(), ReadyFull<E, OUTPUTS>> {
        self.inner.try_submit(batch)
    }

    /// Moves every completed publication into subscriber frames.
    ///
    /// This never waits for producers, runs transforms, inspects tags, or
    /// clones events.
    #[must_use]
    pub fn tick(&self) -> bool {
        self.inner.tick()
    }

    /// Subscribes to the full event enum using the preparation snapshot rule.
    ///
    /// # Errors
    ///
    /// Returns [`SubscribersFull`] when every compile-time slot is occupied.
    pub fn subscribe(
        &self,
    ) -> Result<Subscription<'_, E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>, SubscribersFull> {
        self.inner.subscribe()
    }

    /// Subscribes to one payload type routed before storage and fanout.
    ///
    /// # Errors
    ///
    /// Returns [`SubscribersFull`] when every compile-time slot is occupied.
    pub fn consume<V>(
        &self,
    ) -> Result<TypedSubscription<'_, E, V, OUTPUTS, READY, FRAMES, SUBSCRIBERS>, SubscribersFull>
    where
        V: EventTag<E> + TryFrom<E, Error = E>,
    {
        self.inner.consume()
    }
}

impl<
        E,
        const OUTPUTS: usize,
        const READY: usize,
        const FRAMES: usize,
        const SUBSCRIBERS: usize,
    > Default for DynamicBus<E, OUTPUTS, READY, FRAMES, SUBSCRIBERS>
where
    E: HiwayEvent + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
