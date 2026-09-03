use core::{future::Future, marker::PhantomData};

use heapless::Deque;

use crate::{event::TransformInput, OutputFull};

use sealed::Flow;

const fn ensure_send<F: Future + Send>(future: F) -> F {
    future
}

/// Ordered bounded output from a complete pipeline or direct batch operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch<E, const CAPACITY: usize> {
    events: Deque<E, CAPACITY>,
}

impl<E, const CAPACITY: usize> Batch<E, CAPACITY> {
    /// Creates an empty batch, which drops the input event.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            events: Deque::new(),
        }
    }

    /// Collects ordered events without truncation.
    ///
    /// # Errors
    ///
    /// Returns [`OutputFull`] when the iterator exceeds `CAPACITY`.
    pub fn try_from_iter<I>(events: I) -> Result<Self, OutputFull>
    where
        I: IntoIterator<Item = E>,
    {
        let mut batch = Self::new();
        for event in events {
            batch.push(event)?;
        }
        Ok(batch)
    }

    /// Appends one event.
    ///
    /// # Errors
    ///
    /// Returns [`OutputFull`] when the batch already contains `CAPACITY`
    /// events.
    pub fn push(&mut self, event: E) -> Result<(), OutputFull> {
        self.events.push_back(event).map_err(|_event| OutputFull)
    }

    /// Returns whether the transform dropped its input without replacement.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns the number of emitted events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub(crate) fn pop_front(&mut self) -> Option<E> {
        self.events.pop_front()
    }
}

impl<E, const CAPACITY: usize> Default for Batch<E, CAPACITY> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, const CAPACITY: usize> IntoIterator for Batch<E, CAPACITY> {
    type Item = E;
    type IntoIter = <Deque<E, CAPACITY> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.events.into_iter()
    }
}

/// Compile-time asynchronous event pipeline.
///
/// Implementations return ordinary [`Future`] values. Hiway never spawns them,
/// so the application chooses the executor. Only final output is bounded; a
/// large intermediate iterator may consume one poll and an infinite iterator
/// may never yield. Implement this trait directly for an advanced
/// whole-pipeline escape hatch.
pub trait Pipeline<E: Send, const OUTPUTS: usize>: Sync {
    /// Applies the complete pipeline to one event.
    fn apply<'a>(
        &'a self,
        event: E,
    ) -> impl Future<Output = Result<Batch<E, OUTPUTS>, OutputFull>> + Send + 'a
    where
        E: 'a;
}

/// Pipeline that passes every event through unchanged.
#[derive(Clone, Copy, Debug, Default)]
pub struct Identity;

impl<E: Send, const OUTPUTS: usize> Pipeline<E, OUTPUTS> for Identity {
    fn apply<'a>(
        &'a self,
        event: E,
    ) -> impl Future<Output = Result<Batch<E, OUTPUTS>, OutputFull>> + Send + 'a
    where
        E: 'a,
    {
        ensure_send(async move {
            let mut batch = Batch::new();
            let mut sink = BatchSink { batch: &mut batch };
            self.apply_to(event, &mut sink).await?;
            Ok(batch)
        })
    }
}

/// One typed transform stage.
#[must_use = "a transform stage has no effect until installed on a bus or composed"]
pub struct Stage<F, V> {
    transform: F,
    input: PhantomData<fn(V)>,
}

/// Creates a stage from an asynchronous closure or function.
///
/// The input parameter selects one event payload. Unmatched event variants pass
/// through unchanged. Any ordered `IntoIterator` of complete events is valid
/// output. Built-in composition streams each output depth-first into the
/// remaining stages; only the complete pipeline's final [`Batch`] is bounded by
/// `OUTPUTS`.
pub fn stage<V, F, Fut>(transform: F) -> Stage<F, V>
where
    F: Fn(V) -> Fut,
    Fut: Future,
{
    Stage {
        transform,
        input: PhantomData,
    }
}

impl<E, V, F, Fut, O, const OUTPUTS: usize> Pipeline<E, OUTPUTS> for Stage<F, V>
where
    E: Send,
    V: TransformInput<E> + Send,
    F: Fn(V) -> Fut + Sync,
    Fut: Future<Output = O> + Send,
    O: IntoIterator<Item = E>,
    O::IntoIter: Send,
{
    fn apply<'a>(
        &'a self,
        event: E,
    ) -> impl Future<Output = Result<Batch<E, OUTPUTS>, OutputFull>> + Send + 'a
    where
        E: 'a,
    {
        ensure_send(async move {
            let mut batch = Batch::new();
            let mut sink = BatchSink { batch: &mut batch };
            self.apply_to(event, &mut sink).await?;
            Ok(batch)
        })
    }
}

/// Two built-in pipelines evaluated depth-first in sequence.
///
/// Regrouping built-in stages preserves output, error, transform invocation,
/// and user-effect order.
#[must_use = "a composed pipeline has no effect until installed on a bus"]
pub struct Then<A, B> {
    first: A,
    second: B,
}

impl<E, A, B, const OUTPUTS: usize> Pipeline<E, OUTPUTS> for Then<A, B>
where
    E: Send,
    A: sealed::Flow<E>,
    B: sealed::Flow<E>,
{
    async fn apply<'a>(&'a self, event: E) -> Result<Batch<E, OUTPUTS>, OutputFull>
    where
        E: 'a,
    {
        let mut batch = Batch::new();
        let mut sink = BatchSink { batch: &mut batch };
        self.apply_to(event, &mut sink).await?;
        Ok(batch)
    }
}

/// Compile-time composition for Hiway's built-in pipelines.
///
/// Direct [`Pipeline`] implementations are complete, terminal pipelines. Build
/// composable pipelines from [`Identity`], [`stage`], and [`Then`].
///
/// ```compile_fail
/// use core::future::Future;
/// use hiway::{Batch, Identity, OutputFull, Pipeline, PipelineExt};
///
/// struct Whole;
///
/// impl Pipeline<u8, 1> for Whole {
///     fn apply<'a>(
///         &'a self,
///         event: u8,
///     ) -> impl Future<Output = Result<Batch<u8, 1>, OutputFull>> + Send + 'a {
///         async move { Batch::try_from_iter([event]) }
///     }
/// }
///
/// let _ = Identity.then(Whole);
/// ```
#[allow(private_bounds)]
pub trait PipelineExt: sealed::Composable + Sized {
    /// Sends every output from this pipeline through `next` in order.
    fn then<P: sealed::Composable>(self, next: P) -> Then<Self, P> {
        Then {
            first: self,
            second: next,
        }
    }
}

impl<T> PipelineExt for T where T: sealed::Composable {}

mod sealed {
    use core::future::Future;

    use crate::OutputFull;

    pub trait Composable {}

    pub trait Continuation<E: Send> {
        fn emit<'a>(
            &'a mut self,
            event: E,
        ) -> impl Future<Output = Result<(), OutputFull>> + Send + 'a
        where
            E: 'a;
    }

    pub trait Flow<E: Send>: Sync {
        fn apply_to<'a, C>(
            &'a self,
            event: E,
            continuation: &'a mut C,
        ) -> impl Future<Output = Result<(), OutputFull>> + Send + 'a
        where
            E: 'a,
            C: Continuation<E> + Send;
    }
}

impl sealed::Composable for Identity {}

impl<F, V> sealed::Composable for Stage<F, V> {}

impl<A, B> sealed::Composable for Then<A, B>
where
    A: sealed::Composable,
    B: sealed::Composable,
{
}

struct BatchSink<'a, E, const CAPACITY: usize> {
    batch: &'a mut Batch<E, CAPACITY>,
}

impl<E: Send, const CAPACITY: usize> sealed::Continuation<E> for BatchSink<'_, E, CAPACITY> {
    fn emit<'a>(&'a mut self, event: E) -> impl Future<Output = Result<(), OutputFull>> + Send + 'a
    where
        E: 'a,
    {
        let batch = &mut *self.batch;
        async move { batch.push(event) }
    }
}

impl<E: Send> sealed::Flow<E> for Identity {
    fn apply_to<'a, C>(
        &'a self,
        event: E,
        continuation: &'a mut C,
    ) -> impl Future<Output = Result<(), OutputFull>> + Send + 'a
    where
        E: 'a,
        C: sealed::Continuation<E> + Send,
    {
        continuation.emit(event)
    }
}

impl<E, V, F, Fut, O> sealed::Flow<E> for Stage<F, V>
where
    E: Send,
    V: TransformInput<E> + Send,
    F: Fn(V) -> Fut + Sync,
    Fut: Future<Output = O> + Send,
    O: IntoIterator<Item = E>,
    O::IntoIter: Send,
{
    fn apply_to<'a, C>(
        &'a self,
        event: E,
        continuation: &'a mut C,
    ) -> impl Future<Output = Result<(), OutputFull>> + Send + 'a
    where
        E: 'a,
        C: sealed::Continuation<E> + Send,
    {
        ensure_send(async move {
            match V::try_from_event(event) {
                Ok(input) => {
                    let output = (self.transform)(input).await.into_iter();
                    for event in output {
                        continuation.emit(event).await?;
                    }
                    Ok(())
                }
                Err(other) => continuation.emit(other).await,
            }
        })
    }
}

struct ThenContinuation<'a, B, C> {
    flow: &'a B,
    continuation: &'a mut C,
}

impl<E, B, C> sealed::Continuation<E> for ThenContinuation<'_, B, C>
where
    E: Send,
    B: sealed::Flow<E>,
    C: sealed::Continuation<E> + Send,
{
    fn emit<'a>(&'a mut self, event: E) -> impl Future<Output = Result<(), OutputFull>> + Send + 'a
    where
        E: 'a,
    {
        self.flow.apply_to(event, self.continuation)
    }
}

impl<E, A, B> sealed::Flow<E> for Then<A, B>
where
    E: Send,
    A: sealed::Flow<E>,
    B: sealed::Flow<E>,
{
    fn apply_to<'a, C>(
        &'a self,
        event: E,
        continuation: &'a mut C,
    ) -> impl Future<Output = Result<(), OutputFull>> + Send + 'a
    where
        E: 'a,
        C: sealed::Continuation<E> + Send,
    {
        ensure_send(async move {
            let mut second = ThenContinuation {
                flow: &self.second,
                continuation,
            };
            self.first.apply_to(event, &mut second).await
        })
    }
}
