use super::{Contract, Direction, Reservation};
use crate::{
    grant::{Lease, Operation},
    EventSpec, Grant, Limits, Rights, TopicError,
};

/// Grant-backed transport storage. The driver retains this guard through the
/// last terminal completion, including after logical revocation or link drop.
pub struct GrantReservation {
    grant: Grant,
    contract: Contract,
    direction: Direction,
    bytes: usize,
    rights: Rights,
    _lease: Lease,
}

/// Admission cutoff guard for one endpoint poll or submission.
pub struct GrantAccess {
    _operation: Operation,
}

impl Grant {
    /// Reserves two connections, one in-flight frame, one future waiter, and
    /// the backend's declared arena bytes from this grant's transport pool.
    ///
    /// # Errors
    /// Returns authority rejection or `Capacity` when the grant cannot reserve the resources.
    pub fn reserve_transport<E: EventSpec>(
        &self,
        direction: Direction,
        bytes: usize,
    ) -> Result<GrantReservation, TopicError> {
        let rights = match direction {
            Direction::Import => Rights::PUBLISH,
            Direction::Export => Rights::OBSERVE,
        };
        let _access = self.enter(E::ID, rights)?;
        let lease = self.try_charge(Limits {
            connections: 2,
            retained_items: 1,
            waiters: 1,
            bytes,
            ..Limits::ZERO
        })?;
        Ok(GrantReservation {
            grant: self.clone(),
            contract: Contract::of::<E>(),
            direction,
            bytes,
            rights,
            _lease: lease,
        })
    }
}

impl Reservation for GrantReservation {
    type Access<'a> = GrantAccess;
    fn check(
        &self,
        contract: Contract,
        direction: Direction,
        bytes: usize,
    ) -> Result<(), TopicError> {
        if contract != self.contract || direction != self.direction {
            return Err(TopicError::Denied);
        }
        if bytes > self.bytes {
            return Err(TopicError::Capacity);
        }
        Ok(())
    }
    fn enter(&self) -> Result<GrantAccess, TopicError> {
        self.grant
            .enter(self.contract.event, self.rights)
            .map(|operation| GrantAccess {
                _operation: operation,
            })
    }
    fn is_revoked(&self) -> bool {
        self.grant.is_revoked()
    }
}
