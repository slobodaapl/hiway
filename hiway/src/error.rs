use core::fmt;

use crate::Batch;

/// A pipeline produced more final events than `OUTPUTS` permits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputFull;

/// A completed publication could not reserve space for preparation.
pub struct ReadyFull<E, const OUTPUTS: usize> {
    pub(crate) batch: Batch<E, OUTPUTS>,
}

impl<E, const OUTPUTS: usize> ReadyFull<E, OUTPUTS> {
    pub(crate) const fn new(batch: Batch<E, OUTPUTS>) -> Self {
        Self { batch }
    }

    /// Returns the exact untouched transformed batch for retry.
    #[must_use]
    pub fn into_batch(self) -> Batch<E, OUTPUTS> {
        self.batch
    }
}

impl<E, const OUTPUTS: usize> fmt::Debug for ReadyFull<E, OUTPUTS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadyFull").finish_non_exhaustive()
    }
}

/// Every compile-time subscriber slot is occupied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscribersFull;

/// Error returned while receiving committed events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecvError {
    /// The subscriber missed committed tick frames.
    Lagged(u64),
}

impl fmt::Display for OutputFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("transform output capacity exceeded")
    }
}

impl<E, const OUTPUTS: usize> fmt::Display for ReadyFull<E, OUTPUTS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ready publication capacity exceeded")
    }
}

impl fmt::Display for SubscribersFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("subscriber capacity exceeded")
    }
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lagged(frames) => write!(f, "subscriber lagged; missed frames: {frames}"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for OutputFull {}

#[cfg(feature = "std")]
impl<E, const OUTPUTS: usize> std::error::Error for ReadyFull<E, OUTPUTS> {}

#[cfg(feature = "std")]
impl std::error::Error for SubscribersFull {}

#[cfg(feature = "std")]
impl std::error::Error for RecvError {}
