use crate::EventId;

/// Wire compatibility generation for one event identity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct WireMajor(pub u16);

/// Schema revision within one wire-major generation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct SchemaRevision(pub u32);

/// Stable origin identity used for deduplication and loop prevention.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct OriginId([u8; 16]);

impl OriginId {
    /// The zero origin for local-only messages and tests.
    pub const ZERO: Self = Self([0; 16]);

    /// Creates an origin from its wire bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the wire bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Metadata carried with every routed event.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventMetadata {
    /// Origin process or producer identity.
    pub origin: OriginId,
    /// Monotonic sequence within the origin.
    pub sequence: u64,
    /// Origin clock reading in the origin's declared clock domain.
    pub origin_time: u64,
    /// Remaining hop/time budget. `None` means no expiry was requested.
    pub ttl: Option<u32>,
    /// Optional broker commit sequence for one fabric.
    pub fabric_sequence: Option<u64>,
}

impl EventMetadata {
    /// Creates metadata for one origin sequence.
    #[must_use]
    pub const fn new(origin: OriginId, sequence: u64, origin_time: u64) -> Self {
        Self {
            origin,
            sequence,
            origin_time,
            ttl: None,
            fabric_sequence: None,
        }
    }

    /// Returns the deduplication key.
    #[must_use]
    pub const fn dedup_key(self) -> DedupKey {
        DedupKey {
            origin: self.origin,
            sequence: self.sequence,
        }
    }

    /// Decrements a finite TTL, returning whether forwarding remains allowed.
    pub const fn consume_hop(&mut self) -> bool {
        match self.ttl {
            None => true,
            Some(0) => false,
            Some(ttl) => {
                self.ttl = Some(ttl - 1);
                true
            }
        }
    }
}

/// Stable deduplication and loop-prevention key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DedupKey {
    /// Origin identity.
    pub origin: OriginId,
    /// Origin-local sequence.
    pub sequence: u64,
}

/// Borrowed wire envelope. The payload codec owns the bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireEnvelope<'a> {
    /// Event route identity.
    pub event: EventId,
    /// Breaking wire generation.
    pub wire_major: WireMajor,
    /// Compatible schema revision.
    pub schema_revision: SchemaRevision,
    /// Cross-fabric metadata.
    pub metadata: EventMetadata,
    /// Encoded payload bytes.
    pub payload: &'a [u8],
}

impl<'a> WireEnvelope<'a> {
    /// Creates a borrowed envelope for one typed event payload.
    #[must_use]
    pub const fn new(
        event: EventId,
        wire_major: WireMajor,
        schema_revision: SchemaRevision,
        metadata: EventMetadata,
        payload: &'a [u8],
    ) -> Self {
        Self {
            event,
            wire_major,
            schema_revision,
            metadata,
            payload,
        }
    }
}
