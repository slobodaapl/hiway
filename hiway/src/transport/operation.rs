//! Completion ownership, independent of the submission mechanism.

/// A slot and its wrapping reuse generation.
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
/// Backends must retire completion notifications before an ID is reused.
pub struct Operation {
    id: OpId,
    phase: Phase,
    cancellation: Cancellation,
}

impl Operation {
    #[must_use]
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

    #[must_use]
    pub fn phase(&self) -> Phase {
        self.phase
    }
    #[must_use]
    pub fn id(&self) -> OpId {
        self.id
    }

    /// Returns `None` when occupied. Generations wrap only after reclamation.
    pub fn prepare(&mut self) -> Option<OpId> {
        if self.phase != Phase::Available {
            return None;
        }
        self.id.generation = self.id.generation.wrapping_add(1);
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

    #[must_use]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn near_wrap() -> Operation {
        Operation {
            id: OpId {
                slot: 7,
                generation: u32::MAX - 1,
            },
            phase: Phase::Available,
            cancellation: Cancellation::None,
        }
    }

    #[test]
    fn generation_reuse_requires_original_and_cancel_retirement() {
        for cancel_first in [false, true] {
            let mut operation = near_wrap();
            let last = operation.prepare().unwrap();
            assert_eq!(last.generation, u32::MAX);
            assert!(operation.prepare().is_none());
            assert!(operation.accepted(last));
            operation.cancel();
            assert!(operation.cancel_accepted(last));
            assert!(operation.prepare().is_none());
            if cancel_first {
                assert!(operation.cancel_completed(last));
            } else {
                assert!(operation.completed(last));
            }
            assert!(!operation.reclaim());
            assert!(operation.prepare().is_none());
            if cancel_first {
                assert!(operation.completed(last));
            } else {
                assert!(operation.cancel_completed(last));
            }
            assert!(operation.prepare().is_none());
            assert!(operation.reclaim());

            let next = operation.prepare().unwrap();
            assert_eq!(
                next,
                OpId {
                    slot: 7,
                    generation: 0
                }
            );
            assert!(operation.accepted(next));
            operation.cancel();
            assert!(operation.cancel_accepted(next));
            assert!(!operation.completed(last));
            assert!(!operation.cancel_completed(last));
            assert!(!operation.reclaim());
            assert!(operation.completed(next));
            assert!(operation.cancel_completed(next));
            assert!(operation.reclaim());
            assert_eq!(operation.prepare().unwrap().generation, 1);
        }
    }

    #[test]
    fn completed_or_abandoned_last_generation_remains_reusable() {
        for accepted in [false, true] {
            let mut operation = near_wrap();
            let last = operation.prepare().unwrap();
            if accepted {
                assert!(operation.accepted(last));
                operation.cancel();
                assert_eq!(operation.cancellation(), Some(last));
                assert!(operation.completed(last));
                assert_eq!(operation.cancellation(), None);
            } else {
                operation.cancel();
            }
            assert!(operation.reclaim());
            let next = operation.prepare().unwrap();
            assert_eq!(
                next,
                OpId {
                    slot: 7,
                    generation: 0
                }
            );
            assert!(!operation.accepted(last));
            assert!(operation.accepted(next));
            assert!(!operation.completed(last));
            assert!(operation.completed(next));
            assert!(operation.reclaim());
        }
    }
}
