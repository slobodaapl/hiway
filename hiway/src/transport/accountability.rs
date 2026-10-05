use super::{Contract, Direction, Error};

/// Host-assigned identity within one host accountability domain. Neither field
/// is read from a peer. Reusing an ID requires a fresh generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Connection {
    pub id: u64,
    pub generation: u64,
}

/// Transport lifecycle, not application processing acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Import: local destination accepted. Export: encoded frame accepted for I/O.
    Admitted,
    /// Import: credit sent. Export: frame sent and matching credit received.
    Completed,
    /// A terminal framing, codec, endpoint or I/O failure.
    Rejected(Error),
    /// Local link closure, revocation or future cancellation. The last observed
    /// sequence is context; its frame may already have completed.
    Cancelled,
}

/// Attribution comes from the provisioned connection and event contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record {
    pub connection: Connection,
    pub contract: Contract,
    pub direction: Direction,
    /// Most recently observed frame sequence; absent before a complete header
    /// or source item. A sequence is data, never an authority credential.
    pub sequence: Option<u64>,
    pub outcome: Outcome,
}

/// Fixed FIFO. Overflow discards the oldest record and increments a saturating
/// loss count. Recording and draining allocate nothing and execute no callbacks.
pub struct Records<T: Copy, const CAP: usize> {
    entries: [Option<T>; CAP],
    head: usize,
    len: usize,
    lost: u64,
}

impl<T: Copy, const CAP: usize> Default for Records<T, CAP> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy, const CAP: usize> Records<T, CAP> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: [None; CAP],
            head: 0,
            len: 0,
            lost: 0,
        }
    }

    pub fn push(&mut self, record: T) {
        if CAP == 0 {
            self.add_lost(1);
            return;
        }
        if self.len == CAP {
            self.entries[self.head] = Some(record);
            self.head = (self.head + 1) % CAP;
            self.add_lost(1);
        } else {
            self.entries[(self.head + self.len) % CAP] = Some(record);
            self.len += 1;
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let record = self.entries[self.head].take();
        self.head = (self.head + 1) % CAP;
        self.len -= 1;
        record
    }

    #[must_use]
    pub const fn lost(&self) -> u64 {
        self.lost
    }

    pub(crate) fn add_lost(&mut self, count: u64) {
        self.lost = self.lost.saturating_add(count);
    }
}

#[derive(Clone, Copy)]
struct FrameRecord {
    sequence: Option<u64>,
    outcome: Outcome,
}

/// One provisioned connection's bounded history. At most eight records survive;
/// drain regularly and inspect [`Self::lost`] to detect missing history.
pub struct Accountability {
    connection: Connection,
    contract: Contract,
    direction: Direction,
    sequence: Option<u64>,
    terminal: bool,
    records: Records<FrameRecord, 8>,
}

impl Accountability {
    #[must_use]
    pub const fn new(connection: Connection, contract: Contract, direction: Direction) -> Self {
        Self {
            connection,
            contract,
            direction,
            sequence: None,
            terminal: false,
            records: Records::new(),
        }
    }

    pub fn pop(&mut self) -> Option<Record> {
        self.records.pop().map(|record| Record {
            connection: self.connection,
            contract: self.contract,
            direction: self.direction,
            sequence: record.sequence,
            outcome: record.outcome,
        })
    }

    #[must_use]
    pub const fn lost(&self) -> u64 {
        self.records.lost()
    }

    pub(crate) fn observe(&mut self, sequence: u64) {
        self.sequence = Some(sequence);
    }

    pub(crate) fn record(&mut self, sequence: Option<u64>, outcome: Outcome) {
        if !self.terminal {
            self.records.push(FrameRecord { sequence, outcome });
        }
    }

    pub(crate) fn admitted(&mut self) {
        self.record(self.sequence, Outcome::Admitted);
    }

    pub(crate) fn completed(&mut self, sequence: u64) {
        self.record(Some(sequence), Outcome::Completed);
    }

    pub(crate) fn finish(&mut self, error: Error) {
        self.record(self.sequence, Outcome::Rejected(error));
        self.terminal = true;
    }

    pub(crate) fn cancel(&mut self) {
        self.record(self.sequence, Outcome::Cancelled);
        self.terminal = true;
    }
}
