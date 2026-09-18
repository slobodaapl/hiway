#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]
//!
//! # Design
//!
//! Event identities are static. Stream membership, grants, and links are
//! dynamic. Equal event IDs in two [`DynamicFabric`] instances do not connect
//! them.
//!
//! A dynamic fabric owns stream storage. A [`Grant`] names the events a
//! component may access and reserves the retained items, subscriptions, and
//! waiters needed by those permissions. The fabric also keeps a separate pool
//! for grants, IPC frame storage, and driver waiters. A permission with zero
//! limits does not reserve capacity for publishing or subscription binding.
//!
//! Publication succeeds at local admission. It does not acknowledge remote
//! receipt, application processing, or durable storage. Receivers return
//! [`StreamItem::Data`] or [`StreamItem::Gap`]. Observers may lose retained
//! data; required receivers prevent overwriting data they still need until
//! receipt, detachment, revocation, or stream closure.
//!
//! Cancelling a pending send drops its payload and removes its waiter. It does
//! not undo an accepted publication. Receiver clones compete on one cursor.
//! Dropping the last receiver detaches its subscription. Revocation stops new
//! admissions through the grant subtree and waits for entered admissions to
//! settle. It does not reclaim caller-owned handles or erase data already
//! received.
//!
//! ## No-std storage
//!
//! [`StaticStream`] owns inline retained storage. [`StaticFabric`] adapts a
//! borrowed stream, and `#[graph]` combines bounded event bindings without
//! allocation. The owner calls `maintain` to dispatch deferred wakeups.
//! `try_send` and `try_recv` perform one bounded lock attempt and report
//! contention. Waiting operations register bounded inline waiters before
//! parking.
//!
//! ## Unix links
//!
//! `UnixLink` connects two already-authorized endpoints through separate data
//! and control sockets. It does not transfer authority on the wire. Both legs
//! charge the same host-provided grant. The library does not create a listener,
//! choose a socket path, or provide process sandboxing.
//!
//! ## Wire contracts
//!
//! [`WireMajor`] identifies a breaking wire generation. [`SchemaRevision`]
//! identifies a compatible revision within that generation. [`WireCodec`],
//! [`validate_schema`], and [`validate_evolution`] keep those changes explicit.
//! Payload types do not identify routes, and Hiway does not introduce a Serde
//! dependency or a serialized runtime endpoint.

extern crate self as hiway;

#[cfg(all(test, not(feature = "std")))]
extern crate std;

#[cfg(feature = "std")]
mod alloc_local;
mod error;
mod event;
#[cfg(feature = "std")]
mod grant;
mod link;
mod local;
mod metadata;
mod schema;
#[cfg(feature = "std")]
mod synchronization;
pub mod transform;
#[cfg(all(feature = "tokio-io", unix))]
mod unix;
mod wire;

pub use event::{EventId, EventSpec, EventValue};
pub use hiway_macros::{events, graph, port};
pub use link::Link;
pub use metadata::{DedupKey, EventMetadata, OriginId, SchemaRevision, WireEnvelope, WireMajor};
pub use schema::{
    validate_evolution, validate_schema, FieldKind, FieldPresence, FieldSpec, Schema, SchemaError,
};
pub use transform::{AsyncTransform, Then, Transform, TransformOp};
pub use wire::{EnvelopeHeader, WireCodec, WireError, ENVELOPE_HEADER_BYTES};

#[cfg(feature = "std")]
pub use alloc_local::{
    DynamicFabric, DynamicReceiver, DynamicSender, PreparedPublication, StreamConfig,
};

#[cfg(feature = "std")]
pub use grant::{Grant, Limits, Permission, Rights, StreamLimits};

#[cfg(all(feature = "tokio-io", unix))]
pub use unix::{IpcError, UnixLink};

pub use local::{
    publish, EventPort, EventReceiver, EventSender, OwnedPortBinding, PayloadValue, Port,
    PortBinding, PortExt, PortPreparation, PreparedSend, StaticFabric, StaticPublication,
    StaticReceiveFuture, StaticReceiver, StaticSendFuture, StaticSender, StaticStream, StreamItem,
    SubscriptionRole, Topic,
};

pub use error::{
    CloseReason, PortError, ReceiveError, SendError, TopicError, TopicTypeMismatch, TrySendError,
};
