use core::{fmt, marker::PhantomData};

use crate::metadata::{SchemaRevision, WireMajor};

/// Stable identity for one event specification.
///
/// By default, the declaration macro derives this value from the Rust module
/// path, enum name, and variant. A variant's `#[event(id = "...")]` overrides
/// that input with an explicit protocol name that survives source refactoring.
/// Payload types are excluded: two distinct events may carry the same payload.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct EventId(u128);

impl EventId {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

    /// Computes the stable FNV-1a-128 identity for an event name.
    #[must_use]
    pub const fn from_name(name: &str) -> Self {
        let bytes = name.as_bytes();
        let mut hash = Self::OFFSET;
        let mut index = 0;
        while index < bytes.len() {
            hash = (hash ^ bytes[index] as u128).wrapping_mul(Self::PRIME);
            index += 1;
        }
        Self(hash)
    }

    /// Returns the raw 128-bit identity.
    #[must_use]
    pub const fn as_u128(self) -> u128 {
        self.0
    }

    /// Reconstructs an identity received from a wire envelope.
    #[must_use]
    pub const fn from_u128(value: u128) -> Self {
        Self(value)
    }
}

impl fmt::Debug for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("EventId").field(&self.0).finish()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:032x}", self.0)
    }
}

/// Delivery semantics fixed by the event declaration and provisioned endpoints.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Delivery {
    /// Sequenced actions or dependent updates. Missing publications are visible.
    #[default]
    Ordered,
    /// Complete, independent snapshots. Unread values may be replaced; new
    /// subscribers receive the current value. Only observer subscriptions apply.
    Latest,
}

impl Delivery {
    pub(crate) fn follows(self, previous: u64, next: u64) -> bool {
        match self {
            Self::Ordered => previous.checked_add(1) == Some(next),
            Self::Latest => next > previous,
        }
    }
}

/// A routable event declaration.
pub trait EventSpec {
    /// Payload carried by this event specification.
    type Payload;

    /// Stable identity used by local and remote routing domains.
    const ID: EventId;

    /// Breaking wire generation. Compatible schema revisions keep this value.
    const WIRE_MAJOR: WireMajor = WireMajor(1);

    /// Non-breaking schema revision within [`Self::WIRE_MAJOR`].
    const SCHEMA_REVISION: SchemaRevision = SchemaRevision(1);

    /// Ordered actions by default; opt into `Latest` only for independently
    /// usable snapshots. Changing this contract requires a new identity or wire
    /// major, including when the encoded payload layout stays the same.
    const DELIVERY: Delivery = Delivery::Ordered;
}

/// A payload tagged with the event specification that selects its route.
pub struct EventValue<E: EventSpec> {
    payload: E::Payload,
    marker: PhantomData<fn() -> E>,
}

impl<E: EventSpec> EventValue<E> {
    /// Tags one payload with its event specification.
    #[must_use]
    pub const fn new(payload: E::Payload) -> Self {
        Self {
            payload,
            marker: PhantomData,
        }
    }

    /// Removes the event tag and returns the payload.
    #[must_use]
    pub fn into_inner(self) -> E::Payload {
        self.payload
    }
}

impl<E> Clone for EventValue<E>
where
    E: EventSpec,
    E::Payload: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.payload.clone())
    }
}

impl<E> Copy for EventValue<E>
where
    E: EventSpec,
    E::Payload: Copy,
{
}

impl<E> fmt::Debug for EventValue<E>
where
    E: EventSpec,
    E::Payload: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("EventValue")
            .field(&self.payload)
            .finish()
    }
}

impl<E> PartialEq for EventValue<E>
where
    E: EventSpec,
    E::Payload: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.payload == other.payload
    }
}

impl<E> Eq for EventValue<E>
where
    E: EventSpec,
    E::Payload: Eq,
{
}
