use core::fmt;

use crate::EventId;

/// The registry already contains a different payload type for an event ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicTypeMismatch {
    /// Conflicting identity.
    pub id: EventId,
}

impl fmt::Display for TopicTypeMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "event ID {} is already bound to another payload type",
            self.id
        )
    }
}

/// Error returned while resolving a typed topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicError {
    /// An existing event ID was requested with an incompatible payload type.
    TypeMismatch(TopicTypeMismatch),
    /// Caller-provided route storage has no free subscriber slot.
    Capacity,
}

impl fmt::Display for TopicError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch(error) => error.fmt(formatter),
            Self::Capacity => formatter.write_str("route subscriber storage is full"),
        }
    }
}

/// A nonblocking send could not fit in a reliable subscriber inbox.
pub enum TrySendError<T> {
    /// The original payload is returned for retry.
    Full(T),
    /// The target was closed; the original payload is returned.
    Closed(T),
    /// A target rejected a value after the route's preflight check.
    PushFailed,
}

impl<T> TrySendError<T> {
    /// Returns the payload that was not accepted.
    ///
    /// # Panics
    ///
    /// Panics for [`Self::PushFailed`], because the target consumed ownership
    /// before rejecting the value.
    #[must_use]
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(value) | Self::Closed(value) => value,
            Self::PushFailed => panic!("rejected payload is not recoverable"),
        }
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("TrySendError::Full(..)"),
            Self::Closed(_) => formatter.write_str("TrySendError::Closed(..)"),
            Self::PushFailed => formatter.write_str("TrySendError::PushFailed"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("reliable subscriber inbox is full"),
            Self::Closed(_) => formatter.write_str("subscriber target is closed"),
            Self::PushFailed => formatter.write_str("subscriber target rejected the payload"),
        }
    }
}

/// Error returned by an asynchronous reliable send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendError {
    /// A target could not retain another sender waker.
    WaitersFull,
    /// The target was closed before the send could be delivered.
    Closed,
    /// A target rejected a value after the route's preflight check.
    PushFailed,
}

impl fmt::Display for SendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WaitersFull => formatter.write_str("sender waiter storage is full"),
            Self::Closed => formatter.write_str("subscriber target is closed"),
            Self::PushFailed => formatter.write_str("subscriber target rejected the payload"),
        }
    }
}

/// Error returned while binding a generated port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortError {
    /// A declared event could not be resolved in the local registry.
    Topic(TopicError),
    /// A factory returned receiver storage with the wrong fixed capacity.
    ReceiverCapacity {
        /// Capacity declared by the port.
        expected: usize,
        /// Capacity exposed by the receiver.
        actual: usize,
    },
}

impl From<TopicError> for PortError {
    fn from(error: TopicError) -> Self {
        Self::Topic(error)
    }
}

impl fmt::Display for PortError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topic(error) => error.fmt(formatter),
            Self::ReceiverCapacity { expected, actual } => write!(
                formatter,
                "receiver capacity {actual} does not match port capacity {expected}"
            ),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for TopicTypeMismatch {}

#[cfg(feature = "std")]
impl std::error::Error for TopicError {}

#[cfg(feature = "std")]
impl<T> std::error::Error for TrySendError<T> {}

#[cfg(feature = "std")]
impl std::error::Error for SendError {}

#[cfg(feature = "std")]
impl std::error::Error for PortError {}
