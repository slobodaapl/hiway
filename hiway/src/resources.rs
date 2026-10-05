use core::ops::Deref;

use crate::{grant::Lease, DecodeContext, DecodeError, Grant, Limits, TopicError, WireCodec};

/// A movable reservation for decoded storage or retained application resources.
/// Revocation stops new reservations; it does not release live resources.
#[must_use = "dropping a reservation returns its allowance"]
pub struct ResourceReservation {
    _lease: Lease,
}

impl ResourceReservation {
    /// Transfers this reservation into the resource's owner without allocating.
    /// Reserve before constructing `value`. The declared cost must cover its
    /// storage and external resources through their last use.
    pub fn attach<T>(self, value: T) -> Resource<T> {
        Resource {
            value,
            _reservation: self,
        }
    }
}

/// An owned resource and its quota reservation. Moving this value, or retaining
/// it through `Arc`, keeps the charge. The value is destroyed before its quota
/// is returned. Keep the owner alive through external use, such as GPU completion.
/// Cloning an inner allocation separately requires a separate reservation.
pub struct Resource<T> {
    value: T,
    _reservation: ResourceReservation,
}

impl<T> Deref for Resource<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: core::fmt::Debug> core::fmt::Debug for Resource<T> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_tuple("Resource")
            .field(&self.value)
            .finish()
    }
}

impl Grant {
    /// Reserves generic resource items and bytes before allocation. Costs share
    /// the grant's generic pool with transport storage, never an event's reserved
    /// data allowance. Releasing transport credit does not release this charge.
    ///
    /// # Errors
    /// Returns `Capacity` if either allowance is exhausted, or `Revoked` if this
    /// grant or an ancestor has stopped admission. Failure retains no charge.
    pub fn reserve_resources(
        &self,
        retained_items: usize,
        bytes: usize,
    ) -> Result<ResourceReservation, TopicError> {
        self.try_charge(Limits {
            retained_items,
            bytes,
            ..Limits::ZERO
        })
        .map(|lease| ResourceReservation { _lease: lease })
    }
}

impl DecodeContext for Grant {
    fn decode<E: WireCodec>(
        &self,
        bytes: &[u8],
        revision: crate::SchemaRevision,
    ) -> Result<E::Payload, DecodeError> {
        E::decode_with_resources(bytes, revision, self)
    }
}
