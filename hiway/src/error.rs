use core::fmt;

use crate::EventId;

/// An event identity was resolved with a different payload type.
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

/// A binding could not be acquired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopicError {
    /// Concurrent admission changed the capability state; retry without waiting.
    Contended,
    /// The identity already names a different payload type.
    TypeMismatch(TopicTypeMismatch),
    /// The configured resource allowance is exhausted.
    Capacity,
    /// The grant does not permit this operation.
    Denied,
    /// The grant has been revoked.
    Revoked,
    /// The requested bounds or configuration are invalid.
    InvalidConfig,
}

impl fmt::Display for TopicError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contended => formatter.write_str("capability state is contended"),
            Self::TypeMismatch(error) => error.fmt(formatter),
            Self::Capacity => formatter.write_str("resource allowance is exhausted"),
            Self::Denied => formatter.write_str("operation is not permitted by this grant"),
            Self::Revoked => formatter.write_str("grant has been revoked"),
            Self::InvalidConfig => formatter.write_str("invalid stream configuration"),
        }
    }
}

/// Why a stream or endpoint terminated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseReason {
    /// Its owner closed it.
    Closed,
    /// Its authority was revoked.
    Revoked,
    /// Its transport disconnected.
    Disconnected,
}

impl fmt::Display for CloseReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Closed => "closed",
            Self::Revoked => "revoked",
            Self::Disconnected => "disconnected",
        })
    }
}

/// A rejected publication. Every variant retains the unaccepted payload.
#[non_exhaustive]
pub enum TrySendError<T> {
    /// A required receiver prevents overwriting retained data.
    Full(T),
    /// Another operation owns the stream's synchronization state.
    Contended(T),
    /// Eligible retained storage needs caller-driven reclamation before retrying.
    MaintenanceRequired(T),
    /// The endpoint is closed.
    Closed(T),
    /// The endpoint's authority has been revoked.
    Revoked(T),
    /// No bounded waiter record is available.
    WaitersFull(T),
    /// The sequence counter cannot advance without wrapping.
    SequenceExhausted(T),
}

impl<T> TrySendError<T> {
    /// Maps the rejected payload without changing the rejection reason.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> TrySendError<U> {
        match self {
            Self::Full(value) => TrySendError::Full(map(value)),
            Self::Contended(value) => TrySendError::Contended(map(value)),
            Self::MaintenanceRequired(value) => TrySendError::MaintenanceRequired(map(value)),
            Self::Closed(value) => TrySendError::Closed(map(value)),
            Self::Revoked(value) => TrySendError::Revoked(map(value)),
            Self::WaitersFull(value) => TrySendError::WaitersFull(map(value)),
            Self::SequenceExhausted(value) => TrySendError::SequenceExhausted(map(value)),
        }
    }

    /// Returns the payload that was not accepted.
    #[must_use]
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(value)
            | Self::Contended(value)
            | Self::MaintenanceRequired(value)
            | Self::Closed(value)
            | Self::Revoked(value)
            | Self::WaitersFull(value)
            | Self::SequenceExhausted(value) => value,
        }
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Full(_) => "TrySendError::Full(..)",
            Self::Contended(_) => "TrySendError::Contended(..)",
            Self::MaintenanceRequired(_) => "TrySendError::MaintenanceRequired(..)",
            Self::Closed(_) => "TrySendError::Closed(..)",
            Self::Revoked(_) => "TrySendError::Revoked(..)",
            Self::WaitersFull(_) => "TrySendError::WaitersFull(..)",
            Self::SequenceExhausted(_) => "TrySendError::SequenceExhausted(..)",
        })
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Full(_) => "required receiver has exhausted stream capacity",
            Self::Contended(_) => "stream state is contended",
            Self::MaintenanceRequired(_) => "retained storage needs maintenance",
            Self::Closed(_) => "endpoint is closed",
            Self::Revoked(_) => "endpoint authority has been revoked",
            Self::WaitersFull(_) => "waiter storage is full",
            Self::SequenceExhausted(_) => "stream sequence is exhausted",
        })
    }
}

/// An asynchronous publication rejected before acceptance, retaining its payload.
#[derive(Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendError<T = ()> {
    /// The endpoint is closed.
    Closed(T),
    /// The endpoint's authority has been revoked.
    Revoked(T),
    /// No bounded waiter record is available.
    WaitersFull(T),
    /// The sequence counter cannot advance without wrapping.
    SequenceExhausted(T),
}

impl<T> SendError<T> {
    /// Returns the payload that was not accepted.
    #[must_use]
    pub fn into_inner(self) -> T {
        match self {
            Self::Closed(value)
            | Self::Revoked(value)
            | Self::WaitersFull(value)
            | Self::SequenceExhausted(value) => value,
        }
    }

    /// Maps the rejected payload without changing the rejection reason.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> SendError<U> {
        match self {
            Self::Closed(value) => SendError::Closed(map(value)),
            Self::Revoked(value) => SendError::Revoked(map(value)),
            Self::WaitersFull(value) => SendError::WaitersFull(map(value)),
            Self::SequenceExhausted(value) => SendError::SequenceExhausted(map(value)),
        }
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Closed(_) => "SendError::Closed(..)",
            Self::Revoked(_) => "SendError::Revoked(..)",
            Self::WaitersFull(_) => "SendError::WaitersFull(..)",
            Self::SequenceExhausted(_) => "SendError::SequenceExhausted(..)",
        })
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Closed(_) => "endpoint is closed",
            Self::Revoked(_) => "endpoint authority has been revoked",
            Self::WaitersFull(_) => "waiter storage is full",
            Self::SequenceExhausted(_) => "stream sequence is exhausted",
        })
    }
}

/// A receive operation could not proceed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReceiveError {
    /// Another operation owns the stream's synchronization state.
    Contended,
    /// No bounded waiter record is available.
    WaitersFull,
    /// The endpoint terminated with this reason.
    Closed(CloseReason),
}

impl fmt::Display for ReceiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contended => formatter.write_str("stream state is contended"),
            Self::WaitersFull => formatter.write_str("waiter storage is full"),
            Self::Closed(reason) => write!(formatter, "endpoint terminated: {reason}"),
        }
    }
}

/// An event declared by a port could not be bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PortError {
    /// The binding was rejected.
    Topic(TopicError),
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
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for TopicTypeMismatch {}
#[cfg(feature = "std")]
impl std::error::Error for TopicError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TypeMismatch(error) => Some(error),
            _ => None,
        }
    }
}
#[cfg(feature = "std")]
impl<T> std::error::Error for TrySendError<T> {}
#[cfg(feature = "std")]
impl<T> std::error::Error for SendError<T> {}
#[cfg(feature = "std")]
impl std::error::Error for ReceiveError {}
#[cfg(feature = "std")]
impl std::error::Error for PortError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Topic(error) => Some(error),
        }
    }
}
