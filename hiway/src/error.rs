use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    Closed,
    Lagged(u64),
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "event bus is closed"),
            Self::Lagged(n) => write!(f, "subscriber lagged; missed frames: {n}"),
        }
    }
}

impl Error for RecvError {}

impl From<tokio::sync::broadcast::error::RecvError> for RecvError {
    fn from(value: tokio::sync::broadcast::error::RecvError) -> Self {
        match value {
            tokio::sync::broadcast::error::RecvError::Closed => Self::Closed,
            tokio::sync::broadcast::error::RecvError::Lagged(n) => Self::Lagged(n),
        }
    }
}
