use core::fmt;

use crate::{EventId, SchemaRevision, WireMajor};

/// Wire field kind used by a schema manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FieldKind {
    /// Boolean value.
    Bool,
    /// Unsigned integer.
    Unsigned,
    /// Signed integer.
    Signed,
    /// UTF-8 text.
    Text,
    /// Opaque bytes.
    Bytes,
    /// Nested event or message identified by its stable ID.
    Message(EventId),
}

/// Presence contract for one field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FieldPresence {
    /// The field must be present in every encoded value.
    Required,
    /// Readers may safely omit the field.
    Optional,
    /// Readers may omit the field and use the declared default.
    Defaulted,
}

/// One tagged field declaration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FieldSpec {
    /// Stable field number.
    pub tag: u32,
    /// Wire-level kind.
    pub kind: FieldKind,
    /// Whether absence is compatible.
    pub presence: FieldPresence,
}

/// Borrowed schema manifest for one event generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Schema<'a> {
    /// Event identity.
    pub event: EventId,
    /// Breaking wire generation.
    pub wire_major: WireMajor,
    /// Compatible revision.
    pub revision: SchemaRevision,
    /// Active fields.
    pub fields: &'a [FieldSpec],
    /// Removed field tags that remain reserved.
    pub reserved_tags: &'a [u32],
}

/// Schema compatibility failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaError {
    /// A schema contains duplicate active or reserved tags.
    DuplicateTag(u32),
    /// A field changed its wire kind without a major bump.
    FieldKindChanged(u32),
    /// A field became required in a compatible revision.
    NewlyRequired(u32),
    /// A field that was required became omittable in a compatible revision.
    RequiredFieldMadeOptional(u32),
    /// A removed tag was not reserved.
    RemovedTagNotReserved(u32),
    /// A tag reserved by the previous revision was reused.
    ReservedTagReused(u32),
    /// The event identity changed.
    EventChanged,
    /// A breaking wire generation changed without a separate route.
    MajorChanged,
}

impl fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTag(tag) => write!(formatter, "schema tag {tag} is duplicated"),
            Self::FieldKindChanged(tag) => {
                write!(formatter, "field kind changed for schema tag {tag}")
            }
            Self::NewlyRequired(tag) => {
                write!(formatter, "schema tag {tag} became required")
            }
            Self::RequiredFieldMadeOptional(tag) => {
                write!(formatter, "required schema tag {tag} became optional")
            }
            Self::RemovedTagNotReserved(tag) => {
                write!(formatter, "removed schema tag {tag} is not reserved")
            }
            Self::ReservedTagReused(tag) => {
                write!(formatter, "reserved schema tag {tag} was reused")
            }
            Self::EventChanged => formatter.write_str("schema event identity changed"),
            Self::MajorChanged => formatter.write_str("schema wire major changed"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for SchemaError {}

/// Validates one schema's local tag invariants.
///
/// # Errors
///
/// Returns [`SchemaError::DuplicateTag`] when a tag appears more than once
/// across active fields and reserved tags.
pub fn validate_schema(schema: &Schema<'_>) -> Result<(), SchemaError> {
    for (index, field) in schema.fields.iter().enumerate() {
        if schema.fields[index + 1..]
            .iter()
            .any(|other| other.tag == field.tag)
            || schema.reserved_tags.contains(&field.tag)
        {
            return Err(SchemaError::DuplicateTag(field.tag));
        }
    }
    for (index, tag) in schema.reserved_tags.iter().enumerate() {
        if schema.reserved_tags[index + 1..]
            .iter()
            .any(|other| other == tag)
        {
            return Err(SchemaError::DuplicateTag(*tag));
        }
    }
    Ok(())
}

/// Checks whether `next` can be read by a reader of `previous`.
///
/// # Errors
///
/// Returns [`SchemaError`] if either schema has duplicate tags, the event or
/// wire major changes, a reserved tag is reused, a removed tag is not reserved,
/// a field changes kind, or a field becomes required or ceases to be required.
pub fn validate_evolution(previous: &Schema<'_>, next: &Schema<'_>) -> Result<(), SchemaError> {
    validate_schema(previous)?;
    validate_schema(next)?;
    if previous.event != next.event {
        return Err(SchemaError::EventChanged);
    }
    if previous.wire_major != next.wire_major {
        return Err(SchemaError::MajorChanged);
    }

    for tag in previous.reserved_tags {
        if next.fields.iter().any(|field| field.tag == *tag) {
            return Err(SchemaError::ReservedTagReused(*tag));
        }
    }

    for old in previous.fields {
        let Some(current) = next.fields.iter().find(|field| field.tag == old.tag) else {
            if !next.reserved_tags.contains(&old.tag) {
                return Err(SchemaError::RemovedTagNotReserved(old.tag));
            }
            continue;
        };
        if old.kind != current.kind {
            return Err(SchemaError::FieldKindChanged(old.tag));
        }
        match (old.presence, current.presence) {
            (FieldPresence::Required, FieldPresence::Optional | FieldPresence::Defaulted) => {
                return Err(SchemaError::RequiredFieldMadeOptional(old.tag));
            }
            (FieldPresence::Optional | FieldPresence::Defaulted, FieldPresence::Required) => {
                return Err(SchemaError::NewlyRequired(old.tag));
            }
            _ => {}
        }
    }

    for field in next.fields {
        if !previous.fields.iter().any(|old| old.tag == field.tag)
            && field.presence == FieldPresence::Required
        {
            return Err(SchemaError::NewlyRequired(field.tag));
        }
    }
    Ok(())
}
