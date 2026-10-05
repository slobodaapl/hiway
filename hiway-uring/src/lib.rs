//! One caller-driven `io_uring` domain with fixed storage and no executor.
#![cfg(target_os = "linux")]
#![deny(unsafe_op_in_unsafe_fn)]

mod driver;
pub use driver::Driver;
