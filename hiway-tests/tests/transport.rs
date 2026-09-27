use hiway::transport::{
    encode_frame, Contract, Direction, Effect, Error, Input, IoResult, Lane, OpId, Operation,
    Phase, Protocol, HEADER_BYTES,
};
use hiway::{EventId, EventSpec, SchemaRevision, WireCodec, WireError};

struct Number;
impl EventSpec for Number {
    type Payload = u32;
    const ID: EventId = EventId::from_name("transport.number");
}
impl WireCodec for Number {
    fn encoded_len(_: &u32) -> usize {
        4
    }
    fn encode(value: &u32, output: &mut [u8]) -> Result<usize, WireError> {
        output.copy_from_slice(&value.to_le_bytes());
        Ok(4)
    }
    fn decode(bytes: &[u8], _: SchemaRevision) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(
            bytes.try_into().map_err(|_| WireError::InvalidPayload)?,
        ))
    }
}

fn machine(direction: Direction) -> Protocol {
    Protocol::new(Contract::of::<Number>(), direction, 64).unwrap()
}

fn finish(protocol: &mut Protocol, generation: &mut u32, lane: Lane, count: usize, buffer: &[u8]) {
    *generation += 1;
    let op = OpId {
        slot: lane as u32,
        generation: *generation,
    };
    protocol.input(Input::Accepted { lane, op }).unwrap();
    protocol
        .input(Input::Completed {
            op,
            result: IoResult::Bytes(count),
            buffer,
        })
        .unwrap();
}

#[test]
fn every_header_split_preserves_admission_credit() {
    let mut frame = [0; 64];
    let length = encode_frame::<Number>(19, &73, &mut frame).unwrap();
    for split in 1..HEADER_BYTES {
        let mut p = machine(Direction::Import);
        let mut generation = 0;
        let pending = p.effect(Lane::Data);
        assert_eq!(p.effect(Lane::Data), pending);
        finish(&mut p, &mut generation, Lane::Data, split, &frame);
        assert_eq!(
            p.effect(Lane::Data),
            Some(Effect::Receive {
                lane: Lane::Data,
                offset: split,
                length: HEADER_BYTES - split
            })
        );
        finish(
            &mut p,
            &mut generation,
            Lane::Data,
            HEADER_BYTES - split,
            &frame,
        );
        finish(
            &mut p,
            &mut generation,
            Lane::Data,
            length - HEADER_BYTES,
            &frame,
        );
        assert_eq!(
            p.effect(Lane::Data),
            Some(Effect::Admit {
                length: 4,
                revision: SchemaRevision(1)
            })
        );
        assert_eq!(p.effect(Lane::CreditSend), None);
        p.input(Input::Admitted).unwrap();
        assert_eq!(p.credit(), [1, 19, 0, 0, 0, 0, 0, 0, 0]);
        finish(&mut p, &mut generation, Lane::CreditSend, 3, &[]);
        assert_eq!(
            p.effect(Lane::CreditSend),
            Some(Effect::Transmit {
                lane: Lane::CreditSend,
                offset: 3,
                length: 6
            })
        );
        assert_eq!(p.effect(Lane::Data), None);
        finish(&mut p, &mut generation, Lane::CreditSend, 6, &[]);
        assert_eq!(p.effect(Lane::Data), pending);
    }
}

#[test]
fn credit_can_complete_before_send_but_not_release_an_unfinished_send() {
    let mut p = machine(Direction::Export);
    p.input(Input::Provided {
        sequence: 5,
        length: 42,
    })
    .unwrap();
    let send = OpId {
        slot: 0,
        generation: 1,
    };
    p.input(Input::Accepted {
        lane: Lane::Data,
        op: send,
    })
    .unwrap();
    assert_eq!(p.effect(Lane::Data), None);
    let mut generation = 1;
    finish(
        &mut p,
        &mut generation,
        Lane::CreditReceive,
        9,
        &[1, 5, 0, 0, 0, 0, 0, 0, 0],
    );
    assert_eq!(p.effect(Lane::Data), None);
    p.input(Input::Completed {
        op: send,
        result: IoResult::Bytes(2),
        buffer: &[],
    })
    .unwrap();
    assert_eq!(
        p.effect(Lane::Data),
        Some(Effect::Transmit {
            lane: Lane::Data,
            offset: 2,
            length: 40
        })
    );
    finish(&mut p, &mut generation, Lane::Data, 40, &[]);
    assert_eq!(p.effect(Lane::Data), Some(Effect::Provide));
    // Duplicate completion cannot advance the next frame.
    p.input(Input::Completed {
        op: send,
        result: IoResult::Bytes(42),
        buffer: &[],
    })
    .unwrap();
    assert_eq!(p.effect(Lane::Data), Some(Effect::Provide));
}

#[test]
fn cancellation_requires_both_terminal_completions_in_either_order() {
    for cancel_first in [true, false] {
        let mut operation = Operation::new(7);
        let id = operation.prepare().unwrap();
        assert_eq!(operation.phase(), Phase::Prepared);
        assert!(!operation.reclaim());
        assert!(operation.accepted(id));
        operation.cancel();
        assert_eq!(operation.cancellation(), Some(id));
        assert!(operation.cancel_accepted(id));
        if cancel_first {
            assert!(operation.cancel_completed(id));
        } else {
            assert!(operation.completed(id));
        }
        assert!(!operation.reclaim());
        assert!(operation.prepare().is_none());
        if cancel_first {
            assert!(operation.completed(id));
        } else {
            assert!(operation.cancel_completed(id));
        }
        assert!(operation.reclaim());
        let next = operation.prepare().unwrap();
        assert_ne!(next, id);
        assert!(!operation.completed(id));
        assert!(!operation.cancel_completed(id));
        assert_eq!(operation.phase(), Phase::Prepared);
    }
    let mut operation = Operation::new(0);
    let id = operation.prepare().unwrap();
    operation.accepted(id);
    operation.cancel();
    operation.completed(id);
    assert_eq!(operation.cancellation(), None);
    assert!(operation.reclaim());
    operation.prepare().unwrap();
    operation.cancel();
    assert!(operation.reclaim());
}

#[test]
fn revocation_preserves_outstanding_ids_and_retry_preserves_progress() {
    let mut p = machine(Direction::Import);
    let op = OpId {
        slot: 0,
        generation: 8,
    };
    let receive = p.effect(Lane::Data);
    p.input(Input::Accepted {
        lane: Lane::Data,
        op,
    })
    .unwrap();
    p.input(Input::Completed {
        op,
        result: IoResult::Retry,
        buffer: &[],
    })
    .unwrap();
    assert_eq!(p.effect(Lane::Data), receive);
    let op = OpId {
        generation: 9,
        ..op
    };
    p.input(Input::Accepted {
        lane: Lane::Data,
        op,
    })
    .unwrap();
    p.input(Input::Revoked).unwrap();
    assert_eq!(p.closed(), Some(Error::Revoked));
    assert_eq!(
        p.effect(Lane::Data),
        Some(Effect::Cancel {
            lane: Lane::Data,
            target: op
        })
    );
    p.input(Input::Completed {
        op,
        result: IoResult::Bytes(HEADER_BYTES),
        buffer: &[],
    })
    .unwrap();
    assert_eq!(p.effect(Lane::Data), None);
    assert_eq!(p.effect(Lane::CreditSend), None);
}

#[test]
fn malformed_headers_and_unsolicited_credit_close_without_admission() {
    let mut frame = [0; 64];
    encode_frame::<Number>(0, &1, &mut frame).unwrap();
    for byte in [0, 4, 20, 34] {
        let mut invalid = frame;
        invalid[byte] = 255;
        let mut p = machine(Direction::Import);
        let op = OpId {
            slot: 0,
            generation: 1,
        };
        p.input(Input::Accepted {
            lane: Lane::Data,
            op,
        })
        .unwrap();
        assert!(p
            .input(Input::Completed {
                op,
                result: IoResult::Bytes(HEADER_BYTES),
                buffer: &invalid
            })
            .is_err());
        assert!(p.closed().is_some());
        assert_eq!(p.effect(Lane::CreditSend), None);
    }
    let mut p = machine(Direction::Export);
    let op = OpId {
        slot: 2,
        generation: 1,
    };
    p.input(Input::Accepted {
        lane: Lane::CreditReceive,
        op,
    })
    .unwrap();
    assert_eq!(
        p.input(Input::Completed {
            op,
            result: IoResult::Bytes(9),
            buffer: &[1, 0, 0, 0, 0, 0, 0, 0, 0]
        }),
        Err(Error::Protocol)
    );
}

#[test]
fn static_endpoints_accept_a_caller_supplied_lock_family() {
    use hiway::{
        locking::{Lock, LockFamily},
        StaticFabric, StaticStream, StreamItem,
    };
    struct Local;
    struct LocalMutex<T>(core::cell::RefCell<T>);
    impl LockFamily for Local {
        type Lock<T> = LocalMutex<T>;
        fn wrap<T>(self, value: T) -> LocalMutex<T> {
            LocalMutex(core::cell::RefCell::new(value))
        }
    }
    impl<T> Lock<T> for LocalMutex<T> {
        type Guard<'a>
            = core::cell::RefMut<'a, T>
        where
            T: 'a;
        fn lock(&self) -> Self::Guard<'_> {
            self.0.borrow_mut()
        }
        fn try_lock(&self) -> Option<Self::Guard<'_>> {
            self.0.try_borrow_mut().ok()
        }
    }
    #[hiway::port(send(Number), required(Number))]
    struct Port;
    let stream: StaticStream<Number, 1, 1, 2, _> = StaticStream::with_lock(Local);
    let port = Port::bind(&StaticFabric::new(&stream)).unwrap();
    port.publish_now_number(87).unwrap();
    assert!(
        matches!(port.recv_now_number().unwrap(), Some(StreamItem::Data { value, .. }) if *value == 87)
    );
}

#[cfg(feature = "std")]
#[test]
fn grant_reservation_checks_scope_and_retains_charge_through_revocation() {
    use hiway::transport::Reservation;
    use hiway::{DynamicFabric, Limits, Permission, Rights, StreamConfig, TopicError};
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<Number>(StreamConfig::default())
        .unwrap();
    let grant = fabric
        .grant(
            &[Permission::new::<Number>(Rights::PUBLISH)],
            Limits {
                streams: 1,
                connections: 2,
                retained_items: 1,
                waiters: 1,
                bytes: 128,
                ..Limits::ZERO
            },
        )
        .unwrap();
    let reservation = grant
        .reserve_transport::<Number>(Direction::Import, 128)
        .unwrap();
    assert_eq!(grant.usage().bytes, 128);
    assert_eq!(
        reservation.check(Contract::of::<Number>(), Direction::Import, 129),
        Err(TopicError::Capacity)
    );
    assert_eq!(
        reservation.check(Contract::of::<Number>(), Direction::Export, 128),
        Err(TopicError::Denied)
    );
    let access = reservation.enter().unwrap();
    let mut revoke = pin!(grant.revoke());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(revoke.as_mut().poll(&mut cx).is_pending());
    assert!(reservation.is_revoked());
    assert_eq!(reservation.enter().err(), Some(TopicError::Revoked));
    assert_eq!(grant.usage().bytes, 128);
    drop(access);
    assert_eq!(revoke.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    assert_eq!(grant.usage().bytes, 128);
    drop(reservation);
    assert_eq!(grant.usage().bytes, 0);
}
