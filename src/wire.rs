use core::fmt;

use crate::{EventId, EventMetadata, EventSpec, OriginId, SchemaRevision, WireEnvelope, WireMajor};

/// Fixed byte length of an encoded envelope header.
pub const ENVELOPE_HEADER_BYTES: usize = 70;

/// Failure while encoding or decoding one event payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WireError {
    /// The caller-provided output buffer is too small.
    BufferTooSmall,
    /// The bytes do not describe a valid payload for the event schema.
    InvalidPayload,
    /// The reader does not support the supplied schema revision.
    UnsupportedRevision,
    /// The envelope header is shorter than the protocol requires.
    Truncated,
    /// A header contains a value outside the protocol's valid range.
    InvalidHeader,
}

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BufferTooSmall => "output buffer is too small",
            Self::InvalidPayload => "payload is invalid",
            Self::UnsupportedRevision => "schema revision is unsupported",
            Self::Truncated => "wire envelope is truncated",
            Self::InvalidHeader => "wire envelope header is invalid",
        })
    }
}

#[cfg(feature = "std")]
impl std::error::Error for WireError {}

/// Fixed-size header preceding one encoded payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvelopeHeader {
    /// Event route identity.
    pub event: EventId,
    /// Breaking wire generation.
    pub wire_major: WireMajor,
    /// Compatible schema revision.
    pub schema_revision: SchemaRevision,
    /// Cross-fabric metadata.
    pub metadata: EventMetadata,
    /// Number of payload bytes following this header.
    pub payload_len: u32,
}

impl EnvelopeHeader {
    /// Builds a header directly from an event contract and encoded length.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::InvalidHeader`] if `payload_len` exceeds [`u32::MAX`].
    pub fn for_event<S: EventSpec>(
        metadata: crate::EventMetadata,
        payload_len: usize,
    ) -> Result<Self, WireError> {
        let payload_len = u32::try_from(payload_len).map_err(|_| WireError::InvalidHeader)?;
        Ok(Self {
            event: S::ID,
            wire_major: S::WIRE_MAJOR,
            schema_revision: S::SCHEMA_REVISION,
            metadata,
            payload_len,
        })
    }

    /// Builds a header from a borrowed envelope.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::InvalidHeader`] if the payload length exceeds [`u32::MAX`].
    pub fn from_envelope(envelope: &WireEnvelope<'_>) -> Result<Self, WireError> {
        let payload_len =
            u32::try_from(envelope.payload.len()).map_err(|_| WireError::InvalidHeader)?;
        Ok(Self {
            event: envelope.event,
            wire_major: envelope.wire_major,
            schema_revision: envelope.schema_revision,
            metadata: envelope.metadata,
            payload_len,
        })
    }

    /// Encodes this header into a fixed-size caller buffer.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::BufferTooSmall`] if `output` is shorter than
    /// [`ENVELOPE_HEADER_BYTES`], or [`WireError::InvalidHeader`] if a present
    /// TTL or fabric sequence uses the maximum integer value reserved for absence.
    pub fn encode(self, output: &mut [u8]) -> Result<(), WireError> {
        if output.len() < ENVELOPE_HEADER_BYTES {
            return Err(WireError::BufferTooSmall);
        }
        if self.metadata.ttl == Some(u32::MAX) || self.metadata.fabric_sequence == Some(u64::MAX) {
            return Err(WireError::InvalidHeader);
        }
        let mut cursor = 0;
        put(&mut output[cursor..], &self.event.as_u128().to_le_bytes());
        cursor += 16;
        put(&mut output[cursor..], &self.wire_major.0.to_le_bytes());
        cursor += 2;
        put(&mut output[cursor..], &self.schema_revision.0.to_le_bytes());
        cursor += 4;
        put(&mut output[cursor..], &self.metadata.origin.as_bytes());
        cursor += 16;
        put(&mut output[cursor..], &self.metadata.sequence.to_le_bytes());
        cursor += 8;
        put(
            &mut output[cursor..],
            &self.metadata.origin_time.to_le_bytes(),
        );
        cursor += 8;
        put(
            &mut output[cursor..],
            &self.metadata.ttl.unwrap_or(u32::MAX).to_le_bytes(),
        );
        cursor += 4;
        put(
            &mut output[cursor..],
            &self
                .metadata
                .fabric_sequence
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        cursor += 8;
        put(&mut output[cursor..], &self.payload_len.to_le_bytes());
        Ok(())
    }

    /// Decodes a header without allocating.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::Truncated`] if `input` is shorter than
    /// [`ENVELOPE_HEADER_BYTES`].
    pub fn decode(input: &[u8]) -> Result<Self, WireError> {
        if input.len() < ENVELOPE_HEADER_BYTES {
            return Err(WireError::Truncated);
        }
        let mut cursor = 0;
        let event = EventId::from_u128(u128::from_le_bytes(read::<16>(input, &mut cursor)));
        let wire_major = WireMajor(u16::from_le_bytes(read::<2>(input, &mut cursor)));
        let schema_revision = SchemaRevision(u32::from_le_bytes(read::<4>(input, &mut cursor)));
        let origin = OriginId::new(read::<16>(input, &mut cursor));
        let sequence = u64::from_le_bytes(read::<8>(input, &mut cursor));
        let origin_time = u64::from_le_bytes(read::<8>(input, &mut cursor));
        let ttl = match u32::from_le_bytes(read::<4>(input, &mut cursor)) {
            u32::MAX => None,
            value => Some(value),
        };
        let fabric_sequence = match u64::from_le_bytes(read::<8>(input, &mut cursor)) {
            u64::MAX => None,
            value => Some(value),
        };
        let payload_len = u32::from_le_bytes(read::<4>(input, &mut cursor));
        Ok(Self {
            event,
            wire_major,
            schema_revision,
            metadata: EventMetadata {
                origin,
                sequence,
                origin_time,
                ttl,
                fabric_sequence,
            },
            payload_len,
        })
    }
}

fn put(output: &mut [u8], value: &[u8]) {
    output[..value.len()].copy_from_slice(value);
}

fn read<const N: usize>(input: &[u8], cursor: &mut usize) -> [u8; N] {
    let mut value = [0; N];
    value.copy_from_slice(&input[*cursor..*cursor + N]);
    *cursor += N;
    value
}

/// Event-specific tagged wire codec.
pub trait WireCodec: EventSpec {
    /// Returns the encoded payload length.
    fn encoded_len(payload: &Self::Payload) -> usize;

    /// Encodes one payload into the caller-provided buffer.
    ///
    /// # Errors
    ///
    /// Implementations return [`WireError::BufferTooSmall`] if `output` cannot
    /// hold the encoding, or [`WireError::InvalidPayload`] if the payload
    /// cannot be represented by the event schema.
    fn encode(payload: &Self::Payload, output: &mut [u8]) -> Result<usize, WireError>;

    /// Decodes one payload from a compatible schema revision.
    ///
    /// # Errors
    ///
    /// Implementations return [`WireError::UnsupportedRevision`] if `revision`
    /// is unsupported, or [`WireError::InvalidPayload`] if `bytes` do not encode
    /// a valid payload for that revision.
    fn decode(bytes: &[u8], revision: SchemaRevision) -> Result<Self::Payload, WireError>;
}
