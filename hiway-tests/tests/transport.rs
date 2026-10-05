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

#[test]
// Keep admission and drop fixtures local to this lifecycle regression.
#[allow(clippy::too_many_lines, clippy::items_after_statements)]
fn import_drops_completed_admission_before_credit_and_close() {
    use hiway::{EventSender, SendError, StaticSender, StaticStream, SubscriptionRole};
    use std::{
        cell::Cell,
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    struct Completion<'a, F> {
        future: F,
        dropped: &'a Cell<bool>,
    }
    impl<F: Future + Unpin> Future for Completion<'_, F> {
        type Output = F::Output;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.get_mut().future).poll(cx)
        }
    }
    impl<F> Drop for Completion<'_, F> {
        fn drop(&mut self) {
            self.dropped.set(true);
        }
    }
    #[derive(Clone)]
    struct Sender<'a> {
        sender: StaticSender<'a, Number, 1>,
        dropped: &'a Cell<bool>,
    }
    impl EventSender<Number> for Sender<'_> {
        type Prepared<'a>
            = <StaticSender<'a, Number, 1> as EventSender<Number>>::Prepared<'a>
        where
            Self: 'a;
        fn prepare(&self, value: u32) -> Result<Self::Prepared<'_>, hiway::TrySendError<u32>> {
            self.sender.prepare(value)
        }
        fn send_now(&self, value: u32) -> Result<(), hiway::TrySendError<u32>> {
            self.sender.send_now(value)
        }
        fn send(&self, value: u32) -> impl Future<Output = Result<(), SendError<u32>>> {
            Completion {
                future: self.sender.send(value),
                dropped: self.dropped,
            }
        }
    }
    struct Frames<'a> {
        dropped: &'a Cell<bool>,
        admitted: &'a Cell<bool>,
        closed: &'a Cell<bool>,
        delivered: bool,
    }
    impl hiway::transport::Frames for Frames<'_> {
        fn poll_source(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
            unreachable!("import does not request a source")
        }
        fn provide<E: WireCodec>(&mut self, _: u64, _: &E::Payload) -> Result<(), Error> {
            unreachable!("import does not provide a frame")
        }
        fn poll_frame<E: WireCodec>(
            &mut self,
            _: &mut Context<'_>,
        ) -> Poll<Result<E::Payload, Error>> {
            if self.delivered {
                Poll::Ready(Err(Error::Closed))
            } else {
                self.delivered = true;
                Poll::Ready(
                    E::decode(&7u32.to_le_bytes(), Contract::of::<E>().revision)
                        .map_err(Error::Wire),
                )
            }
        }
        fn admitted(&mut self) -> Result<(), Error> {
            assert!(
                self.dropped.get(),
                "admission future still owns its resources at credit"
            );
            self.admitted.set(true);
            Ok(())
        }
        fn poll_endpoint<F: Future>(
            &mut self,
            cx: &mut Context<'_>,
            future: Pin<&mut F>,
        ) -> Poll<Result<F::Output, Error>> {
            future.poll(cx).map(Ok)
        }
        fn close(&mut self, _: Error) {
            assert!(
                self.dropped.get(),
                "admission future still owns its resources at close"
            );
            self.closed.set(true);
        }
    }
    for revoked in [false, true] {
        let stream = StaticStream::<Number, 1>::new();
        let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
        if revoked {
            stream.close(hiway::CloseReason::Revoked);
        }
        let dropped = Cell::new(false);
        let admitted = Cell::new(false);
        let closed = Cell::new(false);
        let sender = Sender {
            sender: stream.sender(),
            dropped: &dropped,
        };
        let frames = Frames {
            dropped: &dropped,
            admitted: &admitted,
            closed: &closed,
            delivered: false,
        };
        let mut import = std::pin::pin!(hiway::transport::import(sender, frames));
        let expected = if revoked {
            Error::Send(SendError::Revoked(()))
        } else {
            Error::Closed
        };
        assert_eq!(
            import
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(expected))
        );
        assert!(dropped.get());
        assert_eq!(admitted.get(), !revoked);
        assert!(closed.get());
        if !revoked {
            assert!(
                matches!(receiver.recv_now().unwrap(), Some(hiway::StreamItem::Data { sequence: 0, value }) if *value == 7)
            );
        }
    }
    use std::rc::Rc;
    struct Tracked {
        release: Option<Rc<Cell<bool>>>,
        observed: Option<Rc<Cell<Option<bool>>>>,
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            if let (Some(release), Some(observed)) = (&self.release, &self.observed) {
                observed.set(Some(release.get()));
            }
        }
    }
    struct TrackedEvent;
    impl EventSpec for TrackedEvent {
        type Payload = Tracked;
        const ID: EventId = EventId::from_u128(3);
    }
    impl WireCodec for TrackedEvent {
        fn encoded_len(_: &Tracked) -> usize {
            4
        }
        fn encode(_: &Tracked, output: &mut [u8]) -> Result<usize, WireError> {
            output.copy_from_slice(&7u32.to_le_bytes());
            Ok(4)
        }
        fn decode(_: &[u8], _: SchemaRevision) -> Result<Tracked, WireError> {
            Ok(Tracked {
                release: None,
                observed: None,
            })
        }
    }
    struct Prepared(Tracked);
    impl hiway::PreparedSend<TrackedEvent> for Prepared {
        fn try_send(self) -> Result<(), hiway::TrySendError<Self>> {
            Err(hiway::TrySendError::Revoked(self))
        }
        fn into_inner(self) -> Tracked {
            self.0
        }
    }
    #[derive(Clone)]
    struct Rejecting {
        release: Rc<Cell<bool>>,
        observed: Rc<Cell<Option<bool>>>,
    }
    impl EventSender<TrackedEvent> for Rejecting {
        type Prepared<'a>
            = Prepared
        where
            Self: 'a;
        fn prepare(&self, payload: Tracked) -> Result<Prepared, hiway::TrySendError<Tracked>> {
            Err(hiway::TrySendError::Revoked(payload))
        }
        fn send_now(&self, payload: Tracked) -> Result<(), hiway::TrySendError<Tracked>> {
            Err(hiway::TrySendError::Revoked(payload))
        }
        fn send(
            &self,
            mut payload: Tracked,
        ) -> impl Future<Output = Result<(), SendError<Tracked>>> {
            payload.release = Some(self.release.clone());
            payload.observed = Some(self.observed.clone());
            Completion {
                future: std::future::ready(Err(SendError::Revoked(payload))),
                dropped: &self.release,
            }
        }
    }
    let release = Rc::new(Cell::new(false));
    let observed = Rc::new(Cell::new(None));
    let admitted = Cell::new(false);
    let closed = Cell::new(false);
    let sender = Rejecting {
        release: release.clone(),
        observed: observed.clone(),
    };
    let frames = Frames {
        dropped: &release,
        admitted: &admitted,
        closed: &closed,
        delivered: false,
    };
    let mut import = std::pin::pin!(hiway::transport::import(sender, frames));
    assert_eq!(
        import
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(Error::Send(SendError::Revoked(()))))
    );
    assert_eq!(
        observed.get(),
        Some(true),
        "rejected payload dropped before the completed admission future"
    );
    assert!(!admitted.get());
    assert!(closed.get());
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
fn complete_frames_and_every_byte_split_preserve_credit_and_sequence() {
    let mut frame = [0; 64];
    let length = encode_frame::<Number>(19, &73, &mut frame).unwrap();
    for (capacity, split) in [length, frame.len()]
        .into_iter()
        .flat_map(|capacity| (0..length).map(move |split| (capacity, split)))
    {
        let mut p = Protocol::new(Contract::of::<Number>(), Direction::Import, capacity).unwrap();
        let mut generation = 0;
        if split != 0 {
            finish(&mut p, &mut generation, Lane::Data, split, &frame[..split]);
            assert!(
                matches!(p.effect(Lane::Data), Some(Effect::Receive { offset, .. }) if offset == split)
            );
            assert_eq!(p.effect(Lane::CreditSend), None);
        }
        finish(
            &mut p,
            &mut generation,
            Lane::Data,
            length - split,
            &frame[..length],
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
        assert_eq!(p.effect(Lane::Data), None);
        finish(&mut p, &mut generation, Lane::CreditSend, 9, &[]);
        let mut next = frame;
        encode_frame::<Number>(20, &91, &mut next).unwrap();
        finish(&mut p, &mut generation, Lane::Data, length, &next[..length]);
        assert!(matches!(
            p.effect(Lane::Data),
            Some(Effect::Admit { length: 4, .. })
        ));
    }
}

#[test]
fn header_only_frame_fits_minimum_storage_without_early_credit() {
    let mut frame = [0; 64];
    encode_frame::<Number>(0, &0, &mut frame).unwrap();
    frame[34..38].copy_from_slice(&0u32.to_le_bytes());
    let mut p = Protocol::new(Contract::of::<Number>(), Direction::Import, HEADER_BYTES).unwrap();
    finish(
        &mut p,
        &mut 0,
        Lane::Data,
        HEADER_BYTES,
        &frame[..HEADER_BYTES],
    );
    assert_eq!(
        p.effect(Lane::Data),
        Some(Effect::Admit {
            length: 0,
            revision: SchemaRevision(1)
        })
    );
    assert_eq!(p.effect(Lane::CreditSend), None);
}

#[test]
fn combined_receive_rejects_invalid_header_and_uncredited_bytes() {
    let mut valid = [0; 64];
    let length = encode_frame::<Number>(0, &73, &mut valid).unwrap();
    for corrupt in 0..5 {
        let mut frame = valid;
        let count = if corrupt == 4 { length + 1 } else { length };
        if corrupt < 4 {
            frame[[0, 4, 20, 34][corrupt]] = 255;
        }
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
                result: IoResult::Bytes(count),
                buffer: &frame[..count]
            })
            .is_err());
        assert!(p.closed().is_some());
        assert_eq!(p.effect(Lane::CreditSend), None);
        assert_eq!(p.effect(Lane::Data), None);
    }
}

#[test]
fn combined_receive_rejects_duplicate_skipped_and_wrapped_sequences() {
    for (first, next) in [(19, 19), (19, 21), (u64::MAX, 0)] {
        let mut frame = [0; 64];
        let length = encode_frame::<Number>(first, &73, &mut frame).unwrap();
        let mut p = machine(Direction::Import);
        let mut generation = 0;
        finish(&mut p, &mut generation, Lane::Data, length, &frame);
        p.input(Input::Admitted).unwrap();
        finish(&mut p, &mut generation, Lane::CreditSend, 9, &[]);
        encode_frame::<Number>(next, &91, &mut frame).unwrap();
        let op = OpId {
            slot: 0,
            generation: generation + 1,
        };
        p.input(Input::Accepted {
            lane: Lane::Data,
            op,
        })
        .unwrap();
        assert_eq!(
            p.input(Input::Completed {
                op,
                result: IoResult::Bytes(length),
                buffer: &frame
            }),
            Err(Error::Protocol)
        );
        assert_eq!(p.effect(Lane::Data), None);
        assert_eq!(p.effect(Lane::CreditSend), None);
    }
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
        assert!(
            matches!(p.effect(Lane::Data), Some(Effect::Receive { offset, length, .. })
            if offset == split && length >= HEADER_BYTES - split && length <= frame.len() - split)
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
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::{Context, Poll, Wake, Waker},
    };
    #[derive(Default)]
    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    for ancestor in [false, true] {
        let fabric = DynamicFabric::new();
        fabric
            .create_stream::<Number>(StreamConfig::default())
            .unwrap();
        let permissions = [Permission::new::<Number>(Rights::PUBLISH)];
        let limits = Limits {
            streams: 1,
            connections: 2,
            retained_items: 1,
            waiters: 1,
            bytes: 128,
            ..Limits::ZERO
        };
        let parent = fabric
            .grant(
                &permissions,
                Limits {
                    grants: 1,
                    ..limits
                },
            )
            .unwrap();
        let grant = if ancestor {
            parent.restrict(&permissions, limits).unwrap()
        } else {
            parent.clone()
        };
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
        let second_access = reservation.enter().unwrap();
        let mut revoke = pin!(parent.revoke());
        let notifications = Arc::new(Wakes::default());
        let waker = Waker::from(notifications.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(revoke.as_mut().poll(&mut cx).is_pending());
        assert!(reservation.is_revoked());
        assert_eq!(reservation.enter().err(), Some(TopicError::Revoked));
        assert_eq!(grant.usage().bytes, 128);
        drop(access);
        assert!(revoke.as_mut().poll(&mut cx).is_pending());
        notifications.0.store(0, Ordering::SeqCst);
        drop(second_access);
        assert!(notifications.0.load(Ordering::SeqCst) > 0);
        assert_eq!(revoke.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
        assert_eq!(grant.usage().bytes, 128);
        drop(reservation);
        assert_eq!(grant.usage().bytes, 0);
    }
}
