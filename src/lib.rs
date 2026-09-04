#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

//! Typed event specifications and a process-local router.
//!
//! The event contract is static; topic and subscriber membership are dynamic.
//! Static event identities are independent from payload types, so distinct
//! signals may share `()` or any other payload.

extern crate self as hiway;

#[cfg(feature = "std")]
mod alloc_local;
#[cfg(feature = "std")]
mod broker;
mod error;
mod event;
mod link;
mod local;
mod metadata;
mod schema;
pub mod transform;
#[cfg(all(feature = "std", unix))]
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
    AllocTransformSubscriber, DynamicFabric, DynamicSender, SharedInbox, SharedTarget,
};

#[cfg(feature = "std")]
pub use broker::{
    Broker, BrokerError, ClientId, ClientSnapshot, RouteKey, RoutedEnvelope, DEFAULT_SEEN_ORIGINS,
};

#[cfg(all(feature = "std", unix))]
pub use unix::{UnixFrame, UnixLink};

pub use local::{
    publish, receive, AllocPortBinding, DeliveryPolicy, DirectRoute, DirectRouteRef, DirectSender,
    EventPort, EventReceiver, EventSender, HeaplessRoute, HeaplessRouteRef, HeaplessSender,
    HeaplessSubscription, Hiway, Inbox, InboxRef, MappedTarget, OwnedEndpoint, OwnedPortBinding,
    OwnedStorage, Port, PortBinding, PortExt, PushResult, Receive, Receiver, Route, SendFuture,
    Sender, StaticFabric, Target, TargetHandle, Topic, TransformStorage, TransformSubscriber,
    DEFAULT_INBOX_CAPACITY, DEFAULT_INBOX_WAITERS,
};

pub use error::{PortError, SendError, TopicError, TopicTypeMismatch, TrySendError};
