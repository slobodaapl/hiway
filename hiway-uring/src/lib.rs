//! One caller-driven `io_uring` domain with fixed storage and no executor.
#![cfg(target_os = "linux")]
#![deny(unsafe_op_in_unsafe_fn)]

mod diagnostics;
mod driver;
pub use diagnostics::setup_diagnostic;
pub use driver::{Budget, Driver, Pool, Progress, Schedule, Work};
