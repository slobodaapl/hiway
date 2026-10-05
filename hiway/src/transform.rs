use core::ops::AsyncFn;

/// A typed subscriber-side transformation.
///
/// Implement this trait in an application or event crate to publish reusable
/// transform components. The returned future may borrow the transform, but
/// it is never boxed by Hiway.
#[allow(async_fn_in_trait)]
pub trait TransformOp<I> {
    /// Payload type emitted by this transform.
    type Output;

    /// Applies the transform to one input payload.
    async fn apply(&self, input: I) -> Self::Output;

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
    F: Fn(I) -> O,
{
    type Output = O;

    async fn apply(&self, input: I) -> Self::Output {
        (self.function)(input)
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

impl<I, O, F> TransformOp<I> for AsyncTransform<F>
where
    F: AsyncFn(I) -> O,
{
    type Output = O;

    async fn apply(&self, input: I) -> Self::Output {
        (self.function)(input).await
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
    First: TransformOp<I>,
    Second: TransformOp<First::Output>,
{
    type Output = Second::Output;

    async fn apply(&self, input: I) -> Self::Output {
        let intermediate = self.first.apply(input).await;
        self.second.apply(intermediate).await
    }
}
