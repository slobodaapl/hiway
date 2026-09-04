#![allow(clippy::manual_async_fn)]

use core::future::Future;

/// A typed subscriber-side transformation.
///
/// Implement this trait in an application or event crate to publish reusable
/// transform components. The returned future may borrow the transform, but it
/// is never boxed by Hiway.
pub trait TransformOp<I> {
    /// Payload type emitted by this transform.
    type Output;

    /// Applies the transform to one input payload.
    fn apply(&self, input: I) -> impl Future<Output = Self::Output> + '_;

    /// Composes this transform with a second transform.
    fn then<N>(self, next: N) -> Then<Self, N>
    where
        Self: Sized,
        N: TransformOp<Self::Output>,
    {
        Then {
            first: self,
            second: next,
        }
    }
}

/// A synchronous closure transform.
#[must_use]
pub struct Transform<F> {
    function: F,
}

impl<F> Transform<F> {
    /// Wraps a synchronous closure without allocating per event.
    pub fn new(function: F) -> Self {
        Self { function }
    }

    /// Wraps an asynchronous closure without allocating per event.
    pub fn new_async(function: F) -> AsyncTransform<F> {
        AsyncTransform { function }
    }
}

impl<I, O, F> TransformOp<I> for Transform<F>
where
    I: 'static,
    F: Fn(I) -> O,
{
    type Output = O;

    fn apply(&self, input: I) -> impl Future<Output = Self::Output> + '_ {
        async move { (self.function)(input) }
    }
}

/// An asynchronous closure transform.
#[must_use]
pub struct AsyncTransform<F> {
    function: F,
}

impl<F> AsyncTransform<F> {
    /// Wraps an asynchronous closure without allocating per event.
    pub fn new(function: F) -> Self {
        Self { function }
    }
}

impl<I, O, F, Fut> TransformOp<I> for AsyncTransform<F>
where
    I: 'static,
    F: Fn(I) -> Fut,
    Fut: Future<Output = O>,
{
    type Output = O;

    fn apply(&self, input: I) -> impl Future<Output = Self::Output> + '_ {
        async move { (self.function)(input).await }
    }
}

/// Two statically composed transforms.
#[must_use]
pub struct Then<First, Second> {
    first: First,
    second: Second,
}

impl<I, First, Second> TransformOp<I> for Then<First, Second>
where
    I: 'static,
    First: TransformOp<I>,
    Second: TransformOp<First::Output>,
{
    type Output = Second::Output;

    fn apply(&self, input: I) -> impl Future<Output = Self::Output> + '_ {
        async move {
            let intermediate = self.first.apply(input).await;
            self.second.apply(intermediate).await
        }
    }
}
