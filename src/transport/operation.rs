//! Completion ownership, independent of the submission mechanism.

/// A slot and its nonwrapping reuse generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpId {
    pub slot: u32,
    pub generation: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Available,
    Prepared,
    Submitted,
    Completed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cancellation {
    None,
    Requested,
    Submitted,
    Completed,
}

/// One fixed operation slot. Cancellation completion never stands in for
/// the original operation's terminal completion.
pub struct Operation {
    id: OpId,
    phase: Phase,
    cancellation: Cancellation,
}

impl Operation {
    pub const fn new(slot: u32) -> Self {
        Self {
            id: OpId {
                slot,
                generation: 0,
            },
            phase: Phase::Available,
            cancellation: Cancellation::None,
        }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }
    pub fn id(&self) -> OpId {
        self.id
    }

    /// Returns `None` when occupied or when the generation space is exhausted.
    pub fn prepare(&mut self) -> Option<OpId> {
        if self.phase != Phase::Available {
            return None;
        }
        self.id.generation = self.id.generation.checked_add(1)?;
        self.phase = Phase::Prepared;
        Some(self.id)
    }

    pub fn accepted(&mut self, id: OpId) -> bool {
        if id != self.id || self.phase != Phase::Prepared {
            return false;
        }
        self.phase = Phase::Submitted;
        true
    }

    /// Logical closure can abandon a preparation, but not submitted storage.
    pub fn cancel(&mut self) {
        match self.phase {
            Phase::Prepared => self.phase = Phase::Completed,
            Phase::Submitted if self.cancellation == Cancellation::None => {
                self.cancellation = Cancellation::Requested;
            }
            _ => {}
        }
    }

    pub fn cancellation(&self) -> Option<OpId> {
        (self.cancellation == Cancellation::Requested).then_some(self.id)
    }

    pub fn cancel_accepted(&mut self, id: OpId) -> bool {
        if self.cancellation() != Some(id) {
            return false;
        }
        self.cancellation = Cancellation::Submitted;
        true
    }

    pub fn cancel_completed(&mut self, id: OpId) -> bool {
        if self.id != id || self.cancellation != Cancellation::Submitted {
            return false;
        }
        self.cancellation = Cancellation::Completed;
        true
    }

    pub fn completed(&mut self, id: OpId) -> bool {
        if self.id != id || self.phase != Phase::Submitted {
            return false;
        }
        self.phase = Phase::Completed;
        if self.cancellation == Cancellation::Requested {
            self.cancellation = Cancellation::None;
        }
        true
    }

    /// Releases the slot only after all accepted requests have terminated.
    pub fn reclaim(&mut self) -> bool {
        if self.phase != Phase::Completed || self.cancellation == Cancellation::Submitted {
            return false;
        }
        self.phase = Phase::Available;
        self.cancellation = Cancellation::None;
        true
    }
}
