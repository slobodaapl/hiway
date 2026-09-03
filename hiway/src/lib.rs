#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

//! `hiway` is a bounded, typed, in-process event bus. Static transforms stream
//! outputs depth-first and enforce capacity only at the final batch.
//! Publishers prepare routed subscriber batches; a synchronous tick moves every
//! completed publication into subscriber frame queues.
//!
//! The static core performs no internal heap allocation and does not choose an
//! executor. User transforms, conversions, tags, clones, destructors, and
//! wakers may allocate or panic.
//!
//! ```
//! use hiway::{Bus, HiwayEvent};
//!
//! #[derive(Clone, Debug, PartialEq, Eq)]
//! struct Text(&'static str);
//!
//! #[derive(Clone, Debug, HiwayEvent)]
//! enum Events {
//!     Text(Text),
//! }
//!
//! # async fn example() {
//! let bus = Bus::default().transform(async |mut text: Text| {
//!     text.0 = "transformed";
//!     [text.into()]
//! });
//! let mut texts = bus.consume::<Text>().unwrap();
//!
//! bus.publish(Text("input")).await.unwrap();
//! assert!(bus.tick());
//! assert_eq!(texts.try_recv().unwrap(), Some(Text("transformed")));
//! # }
//! ```

extern crate self as hiway;

mod bus;
#[cfg(feature = "dynamic")]
mod dynamic;
mod error;
mod event;
mod pipeline;

pub use bus::{
    Bus, PublishError, Subscription, TypedSubscription, DEFAULT_FRAMES, DEFAULT_OUTPUTS,
    DEFAULT_READY, DEFAULT_SUBSCRIBERS,
};
#[cfg(feature = "dynamic")]
pub use dynamic::DynamicBus;
pub use error::{OutputFull, ReadyFull, RecvError, SubscribersFull};
pub use event::HiwayEvent;
pub use hiway_macros::HiwayEvent;
pub use pipeline::{stage, Batch, Identity, Pipeline, PipelineExt, Stage, Then};

#[doc(hidden)]
pub mod __private {
    pub use crate::event::{EventTag, TransformInput};
}
