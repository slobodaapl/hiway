use core::task::{Context, Poll};

/// Executor-neutral driver for a transport link.
///
/// A link owns its socket or peer state and is polled by the application. The
/// router never spawns it and never chooses Tokio, async-std, or another
/// executor.
pub trait Link: Send {
    /// Driver-specific failure.
    type Error;

    /// Advances the link driver.
    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}
