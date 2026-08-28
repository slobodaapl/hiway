#![forbid(unsafe_code)]

//! `hiway` is a typed in-process event bus. Publishers prepare transformed
//! event batches asynchronously. A central tick commits every ready publication
//! as one frame, then every subscriber receives its events independently.
//!
//! # Tiny example
//!
//! ```no_run
//! use hiway::{Bus, HiwayEvent};
//!
//! #[derive(Clone, Debug, PartialEq, Eq)]
//! struct TextEvent {
//!     text: String,
//! }
//!
//! #[derive(Clone, Debug, HiwayEvent)]
//! enum Events {
//!     Text(TextEvent),
//! }
//!
//! # async fn example() -> Result<(), hiway::RecvError> {
//! let bus = Bus::<Events>::new();
//! let mut texts = bus.consume::<TextEvent>();
//!
//! bus.publish(TextEvent { text: "hello".into() }).await;
//! assert!(bus.tick());
//! assert_eq!(texts.recv().await?.text, "hello");
//! # Ok(())
//! # }
//! ```

extern crate self as hiway;

mod bus;
mod error;
mod event;
mod hub;
mod transform;

#[cfg(feature = "dispatch")]
mod dispatch;

pub use bus::{Bus, Subscription, TypedSubscription};
pub use error::RecvError;
pub use event::HiwayEvent;
pub use hiway_macros::HiwayEvent;
pub use hub::Hiway;

#[doc(hidden)]
pub mod __private {
    pub use crate::event::TransformInput;
}

#[cfg(feature = "dispatch")]
pub use dispatch::{Dispatcher, Listener};

#[cfg(feature = "dispatch")]
pub use dptree;
