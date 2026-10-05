use super::{
    Accountability, Connection, Contract, Error, OpId, Record, CREDIT_BYTES, HEADER_BYTES,
};
use crate::SchemaRevision;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Import,
    Export,
}

/// Each lane permits at most one accepted operation. Control send and receive
/// share a socket but have independent ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum Lane {
    Data,
    CreditSend,
    CreditReceive,
}
impl Lane {
    pub const ALL: [Self; 3] = [Self::Data, Self::CreditSend, Self::CreditReceive];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Receive {
        lane: Lane,
        offset: usize,
        length: usize,
    },
    Transmit {
        lane: Lane,
        offset: usize,
        length: usize,
    },
    Provide,
    Admit {
        length: usize,
        revision: SchemaRevision,
    },
    Cancel {
        lane: Lane,
        target: OpId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoResult {
    Bytes(usize),
    Retry,
    Failed(i32),
}

#[derive(Clone, Copy)]
pub enum Input<'a> {
    Accepted {
        lane: Lane,
        op: OpId,
    },
    Completed {
        op: OpId,
        result: IoResult,
        buffer: &'a [u8],
    },
    Provided {
        sequence: u64,
        length: usize,
    },
    Admitted,
    Revoked,
    Closed(Error),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Source,
    Sending,
    Credit,
    Header,
    Payload,
    Admission,
}

/// Deterministic HWY1 protocol. Storage and execution remain outside the model.
/// An effect stays pending until `Accepted`; acceptance is not completion.
/// Backends must retire all completion notifications before reusing an `OpId`.
pub struct Protocol {
    contract: Contract,
    direction: Direction,
    capacity: usize,
    stage: Stage,
    active: [Option<(OpId, Effect)>; 3],
    offsets: [usize; 3],
    length: usize,
    sequence: u64,
    previous: Option<u64>,
    revision: SchemaRevision,
    credited: bool,
    closed: Option<Error>,
    accountability: Option<Accountability>,
}

impl Protocol {
    /// Creates a protocol with a fixed frame-buffer capacity.
    ///
    /// # Errors
    /// Returns `Capacity` if the buffer cannot hold a header or a wire-sized payload.
    pub fn new(contract: Contract, direction: Direction, capacity: usize) -> Result<Self, Error> {
        if capacity < HEADER_BYTES || capacity - HEADER_BYTES > u32::MAX as usize {
            return Err(Error::Capacity);
        }
        Ok(Self {
            contract,
            direction,
            capacity,
            stage: match direction {
                Direction::Import => Stage::Header,
                Direction::Export => Stage::Source,
            },
            active: [None; 3],
            offsets: [0; 3],
            length: HEADER_BYTES,
            sequence: 0,
            previous: None,
            revision: contract.revision,
            credited: false,
            closed: None,
            accountability: None,
        })
    }

    #[must_use]
    pub fn contract(&self) -> Contract {
        self.contract
    }
    #[must_use]
    pub fn closed(&self) -> Option<Error> {
        self.closed
    }

    /// Creates a tracked protocol with host-supplied attribution and at most
    /// eight retained records. The wire format remains unchanged.
    ///
    /// # Errors
    /// Returns the same capacity errors as [`Self::new`].
    pub fn with_connection(
        contract: Contract,
        direction: Direction,
        capacity: usize,
        connection: Connection,
    ) -> Result<Self, Error> {
        let mut protocol = Self::new(contract, direction, capacity)?;
        protocol.accountability = Some(Accountability::new(connection, contract, direction));
        Ok(protocol)
    }

    pub fn pop_record(&mut self) -> Option<Record> {
        self.accountability.as_mut().and_then(Accountability::pop)
    }

    #[must_use]
    pub fn lost_records(&self) -> u64 {
        self.accountability.as_ref().map_or(0, Accountability::lost)
    }

    /// Associates a source item before invoking its codec, including encode failure.
    pub fn observe_sequence(&mut self, sequence: u64) {
        if let Some(accountability) = &mut self.accountability {
            accountability.observe(sequence);
        }
    }

    #[must_use]
    pub fn effect(&self, lane: Lane) -> Option<Effect> {
        if let Some((target, _)) = self.active[lane as usize] {
            return self.closed.map(|_| Effect::Cancel { lane, target });
        }
        if self.closed.is_some() {
            return None;
        }
        let offset = self.offsets[lane as usize];
        match lane {
            Lane::Data => match self.stage {
                Stage::Source => Some(Effect::Provide),
                Stage::Sending => Some(Effect::Transmit {
                    lane,
                    offset,
                    length: self.length - offset,
                }),
                Stage::Header => Some(Effect::Receive {
                    lane,
                    offset,
                    length: self.capacity - offset,
                }),
                Stage::Payload => Some(Effect::Receive {
                    lane,
                    offset,
                    length: self.length - offset,
                }),
                Stage::Admission if !self.credited => Some(Effect::Admit {
                    length: self.length - HEADER_BYTES,
                    revision: self.revision,
                }),
                _ => None,
            },
            Lane::CreditSend if self.direction == Direction::Import && self.credited => {
                Some(Effect::Transmit {
                    lane,
                    offset,
                    length: CREDIT_BYTES - offset,
                })
            }
            Lane::CreditSend => None,
            Lane::CreditReceive => Some(Effect::Receive {
                lane,
                offset,
                length: if self.direction == Direction::Export {
                    CREDIT_BYTES - offset
                } else {
                    1
                },
            }),
        }
    }

    #[must_use]
    pub fn credit(&self) -> [u8; CREDIT_BYTES] {
        let mut credit = [1; CREDIT_BYTES];
        credit[1..].copy_from_slice(&self.previous.unwrap_or(self.sequence).to_le_bytes());
        credit
    }

    /// Completions without a matching active ID have no effect. Invalid current input
    /// closes the protocol; closure never discards outstanding operation IDs.
    ///
    /// # Errors
    /// Returns framing, I/O, sequence, capacity or lifecycle errors from the input.
    pub fn input(&mut self, input: Input<'_>) -> Result<(), Error> {
        let result = self.transition(input);
        if let Err(error) = result {
            self.closed.get_or_insert(error);
        }
        if let (Some(error), Some(accountability)) = (self.closed, &mut self.accountability) {
            if matches!(
                input,
                Input::Revoked | Input::Closed(Error::Closed | Error::Revoked)
            ) {
                accountability.cancel();
            } else {
                accountability.finish(error);
            }
        }
        result
    }

    fn transition(&mut self, input: Input<'_>) -> Result<(), Error> {
        match input {
            Input::Revoked => {
                self.closed.get_or_insert(Error::Revoked);
            }
            Input::Closed(error) => {
                self.closed.get_or_insert(error);
            }
            Input::Accepted { lane, op } => {
                let effect = self.effect(lane).ok_or(Error::Protocol)?;
                if !matches!(effect, Effect::Receive { .. } | Effect::Transmit { .. }) {
                    return Err(Error::Protocol);
                }
                if self.active.iter().flatten().any(|(id, _)| *id == op) {
                    return Err(Error::Protocol);
                }
                self.active[lane as usize] = Some((op, effect));
            }
            Input::Completed { op, result, buffer } => {
                let Some(index) = self
                    .active
                    .iter()
                    .position(|active| active.is_some_and(|(id, _)| id == op))
                else {
                    return Ok(());
                };
                let (_, effect) = self.active[index].take().expect("matched operation");
                if self.closed.is_some() {
                    return Ok(());
                }
                let count = match result {
                    IoResult::Retry => return Ok(()),
                    IoResult::Failed(code) => return Err(Error::Io(code)),
                    IoResult::Bytes(0) => return Err(Error::Closed),
                    IoResult::Bytes(count) => count,
                };
                let (Effect::Receive {
                    length: requested, ..
                }
                | Effect::Transmit {
                    length: requested, ..
                }) = effect
                else {
                    unreachable!()
                };
                if count > requested {
                    return Err(Error::Protocol);
                }
                self.offsets[index] += count;
                self.progress(Lane::ALL[index], buffer)?;
            }
            Input::Provided { sequence, length } => {
                if let Some(error) = self.closed {
                    return Err(error);
                }
                if self.stage != Stage::Source || length < HEADER_BYTES || length > self.capacity {
                    return Err(Error::Protocol);
                }
                self.sequence = sequence;
                self.observe_sequence(sequence);
                self.length = length;
                self.offsets[0] = 0;
                self.credited = false;
                self.stage = Stage::Sending;
                if let Some(accountability) = &mut self.accountability {
                    accountability.admitted();
                }
            }
            Input::Admitted => {
                if let Some(error) = self.closed {
                    return Err(error);
                }
                if self.stage != Stage::Admission || self.credited {
                    return Err(Error::Protocol);
                }
                self.previous = Some(self.sequence);
                if let Some(accountability) = &mut self.accountability {
                    accountability.admitted();
                }
                self.credited = true;
                self.offsets[Lane::CreditSend as usize] = 0;
                self.offsets[Lane::Data as usize] = 0;
                self.length = HEADER_BYTES;
                self.stage = Stage::Header;
            }
        }
        Ok(())
    }

    fn progress(&mut self, lane: Lane, buffer: &[u8]) -> Result<(), Error> {
        let offset = self.offsets[lane as usize];
        match lane {
            Lane::Data => match self.stage {
                Stage::Sending if offset == self.length => {
                    self.stage = if self.credited {
                        Stage::Source
                    } else {
                        Stage::Credit
                    };
                    if self.credited {
                        if let Some(accountability) = &mut self.accountability {
                            accountability.completed(self.sequence);
                        }
                    }
                }
                Stage::Header if offset >= HEADER_BYTES => {
                    let header = buffer.get(..HEADER_BYTES).ok_or(Error::Protocol)?;
                    self.observe_sequence(u64::from_le_bytes(header[26..34].try_into().unwrap()));
                    if header[..4] != *b"HWY1"
                        || header[4..20] != self.contract.event.as_u128().to_le_bytes()
                        || header[20..22] != self.contract.major.0.to_le_bytes()
                    {
                        return Err(Error::Protocol);
                    }
                    self.revision =
                        SchemaRevision(u32::from_le_bytes(header[22..26].try_into().unwrap()));
                    self.sequence = u64::from_le_bytes(header[26..34].try_into().unwrap());
                    let length = u32::from_le_bytes(header[34..38].try_into().unwrap()) as usize;
                    self.length = HEADER_BYTES.checked_add(length).ok_or(Error::Capacity)?;
                    if self.length > self.capacity {
                        return Err(Error::Capacity);
                    }
                    // One frame may be in flight until admission returns credit.
                    if offset > self.length {
                        return Err(Error::Protocol);
                    }
                    if self
                        .previous
                        .is_some_and(|last| !self.contract.delivery.follows(last, self.sequence))
                    {
                        return Err(Error::Protocol);
                    }
                    self.stage = if offset == self.length {
                        Stage::Admission
                    } else {
                        Stage::Payload
                    };
                }
                Stage::Payload if offset == self.length => self.stage = Stage::Admission,
                Stage::Header | Stage::Payload | Stage::Sending => {}
                _ => return Err(Error::Protocol),
            },
            Lane::CreditSend if offset == CREDIT_BYTES => {
                self.credited = false;
                if let Some(accountability) = &mut self.accountability {
                    accountability.completed(self.previous.expect("credit follows admission"));
                }
            }
            Lane::CreditReceive => {
                if self.direction == Direction::Import {
                    return Err(Error::Protocol);
                }
                if offset == CREDIT_BYTES {
                    let credit = buffer.get(..CREDIT_BYTES).ok_or(Error::Protocol)?;
                    if credit[0] != 1
                        || credit[1..] != self.sequence.to_le_bytes()
                        || self.credited
                        || !matches!(self.stage, Stage::Sending | Stage::Credit)
                    {
                        return Err(Error::Protocol);
                    }
                    self.credited = true;
                    self.offsets[lane as usize] = 0;
                    if self.stage == Stage::Credit {
                        self.stage = Stage::Source;
                        if let Some(accountability) = &mut self.accountability {
                            accountability.completed(self.sequence);
                        }
                    }
                }
            }
            Lane::CreditSend => {}
        }
        Ok(())
    }
}
