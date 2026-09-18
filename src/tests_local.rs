use super::*;
use std::{
    boxed::Box,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
    task::Wake,
};

struct Number;

impl EventSpec for Number {
    type Payload = u32;
    const ID: crate::EventId = crate::EventId::from_name("static-tests::Number");
}

#[derive(Default)]
struct Counter(AtomicUsize);

impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn data(sequence: u64, value: u32) -> StreamItem<PayloadValue<u32>> {
    StreamItem::Data {
        sequence,
        value: PayloadValue(value),
    }
}

#[test]
fn strict_static_operations_defer_callbacks_and_report_lock_contention() {
    let stream = StaticStream::<Number, 1>::new();
    let sender = stream.sender();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(counter.clone());
    let mut context = Context::from_waker(&waker);
    let mut receive = Box::pin(receiver.recv());
    assert!(receive.as_mut().poll(&mut context).is_pending());
    sender.try_send(7).unwrap();
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    stream.maintain();
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        receive.as_mut().poll(&mut context),
        Poll::Ready(Ok(data(0, 7)))
    );
    let guard = stream.state.lock();
    assert!(matches!(
        sender.try_send(8),
        Err(TrySendError::Contended(8))
    ));
    assert!(matches!(receiver.try_recv(), Err(ReceiveError::Contended)));
    drop(guard);
    sender.try_send(8).unwrap();
    assert_eq!(receiver.try_recv().unwrap(), Some(data(1, 8)));
}

#[test]
fn async_contention_parks_and_cancellation_releases_only_its_slot() {
    let stream = StaticStream::<Number, 1, 1, 2>::new();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(counter.clone());
    let guard = stream.state.lock();
    let (polled, parked) = std::sync::mpsc::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let task = scope.spawn(move || {
            let mut context = Context::from_waker(&waker);
            let mut cancelled = Box::pin(receiver.recv());
            let mut live = Box::pin(receiver.recv());
            let first = cancelled.as_mut().poll(&mut context);
            let second = live.as_mut().poll(&mut context);
            drop(cancelled);
            let mut replacement = Box::pin(receiver.recv());
            let reused = replacement.as_mut().poll(&mut context);
            let mut excess = Box::pin(receiver.recv());
            let full = excess.as_mut().poll(&mut context);
            drop(replacement);
            polled.send((first, second, reused, full)).unwrap();
            resumed.recv().unwrap();
            live.as_mut().poll(&mut context)
        });
        let result = parked.recv_timeout(std::time::Duration::from_secs(2));
        let before = counter.0.load(Ordering::SeqCst);
        drop(guard);
        stream.sender().send_now(7).unwrap();
        resume.send(()).unwrap();
        let outcome = task.join().unwrap();
        let (first, second, reused, full) =
            result.expect("poll or cancellation waited for the stream lock");
        assert!(first.is_pending() && second.is_pending() && reused.is_pending());
        assert_eq!(full, Poll::Ready(Err(ReceiveError::WaitersFull)));
        assert_eq!(before, 0);
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert_eq!(outcome, Poll::Ready(Ok(data(0, 7))));
    });
}

#[test]
fn idle_maintenance_does_not_repoll_blocked_futures() {
    let stream = StaticStream::<Number, 1, 1, 1>::new();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    let sender = stream.sender();
    sender.send_now(1).unwrap();
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(counter.clone());
    let mut context = Context::from_waker(&waker);
    let mut pending = Box::pin(sender.send(2));
    assert!(pending.as_mut().poll(&mut context).is_pending());
    for _ in 0..8 {
        stream.maintain();
    }
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    assert_eq!(receiver.try_recv().unwrap(), Some(data(0, 1)));
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    stream.maintain();
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert_eq!(pending.as_mut().poll(&mut context), Poll::Ready(Ok(())));
    stream.maintain();
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert_eq!(receiver.recv_now().unwrap(), Some(data(1, 2)));

    let eager = StaticStream::<Number, 1, 1, 0>::new();
    let receiver = eager.subscribe(SubscriptionRole::Required).unwrap();
    assert_eq!(
        Box::pin(eager.sender().send(9)).as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    );
    assert_eq!(
        Box::pin(receiver.recv()).as_mut().poll(&mut context),
        Poll::Ready(Ok(data(0, 9)))
    );
    assert_eq!(
        Box::pin(receiver.recv()).as_mut().poll(&mut context),
        Poll::Ready(Err(ReceiveError::WaitersFull))
    );
}

#[test]
fn static_events_can_borrow_non_send_non_sync_payloads() {
    struct Borrowed<'a>(PhantomData<&'a core::cell::Cell<u32>>);
    impl<'a> EventSpec for Borrowed<'a> {
        type Payload = &'a core::cell::Cell<u32>;
        const ID: crate::EventId = crate::EventId::from_name("static-tests::Borrowed");
    }
    let value = core::cell::Cell::new(41);
    let stream = StaticStream::<Borrowed<'_>, 1>::new();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    stream
        .sender()
        .publish_now(EventValue::new(&value))
        .unwrap();
    let StreamItem::Data {
        sequence,
        value: borrowed,
    } = receiver.recv_now().unwrap().unwrap()
    else {
        panic!("borrowed data was lost")
    };
    assert_eq!(sequence, 0);
    borrowed.set(42);
    assert_eq!(value.get(), 42);
}

#[test]
fn observers_report_gaps_without_retaining_publication_capacity() {
    let stream = StaticStream::<Number, 2>::new();
    let receiver = stream.subscribe(SubscriptionRole::Observer).unwrap();
    let sender = stream.sender();
    for value in 0..5 {
        sender.send_now(value).unwrap();
    }
    assert_eq!(
        receiver.recv_now().unwrap(),
        Some(StreamItem::Gap { from: 0, to: 3 })
    );
    assert_eq!(receiver.recv_now().unwrap().unwrap(), data(3, 3));
    assert_eq!(receiver.recv_now().unwrap().unwrap(), data(4, 4));
    assert_eq!(receiver.recv_now().unwrap(), None);
}

#[test]
fn all_required_cursors_must_release_the_overwritten_sequence() {
    let stream = StaticStream::<Number, 2>::new();
    let a = stream.subscribe(SubscriptionRole::Required).unwrap();
    let b = stream.subscribe(SubscriptionRole::Required).unwrap();
    let sender = stream.sender();
    sender.send_now(10).unwrap();
    sender.send_now(20).unwrap();
    assert_eq!(a.recv_now().unwrap().unwrap(), data(0, 10));
    assert!(matches!(sender.send_now(30), Err(TrySendError::Full(30))));
    assert_eq!(b.recv_now().unwrap().unwrap(), data(0, 10));
    sender.send_now(30).unwrap();
    assert_eq!(a.recv_now().unwrap().unwrap(), data(1, 20));
    assert_eq!(a.recv_now().unwrap().unwrap(), data(2, 30));
    assert_eq!(b.recv_now().unwrap().unwrap(), data(1, 20));
    assert_eq!(b.recv_now().unwrap().unwrap(), data(2, 30));
}

#[test]
fn detachment_releases_capacity_and_new_members_start_at_tail() {
    let stream = StaticStream::<Number, 1, 1>::new();
    let sender = stream.sender();
    sender.send_now(1).unwrap();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    assert_eq!(receiver.recv_now().unwrap(), None);
    sender.send_now(2).unwrap();
    assert!(matches!(sender.send_now(3), Err(TrySendError::Full(3))));
    assert!(matches!(
        stream.subscribe(SubscriptionRole::Observer),
        Err(TopicError::Capacity)
    ));
    drop(receiver);
    sender.send_now(3).unwrap();
    let replacement = stream.subscribe(SubscriptionRole::Observer).unwrap();
    assert_eq!(replacement.recv_now().unwrap(), None);
    sender.send_now(4).unwrap();
    assert_eq!(replacement.recv_now().unwrap().unwrap(), data(3, 4));
}

#[test]
fn a_blocked_stream_does_not_consume_another_streams_capacity() {
    let a = StaticStream::<Number, 1>::new();
    let b = StaticStream::<Number, 1>::new();
    let _stopped = a.subscribe(SubscriptionRole::Required).unwrap();
    let active = b.subscribe(SubscriptionRole::Required).unwrap();
    a.sender().send_now(1).unwrap();
    assert!(matches!(a.sender().send_now(2), Err(TrySendError::Full(2))));
    for value in 10..20 {
        b.sender().send_now(value).unwrap();
        assert_eq!(
            active.recv_now().unwrap().unwrap(),
            data(u64::from(value - 10), value)
        );
    }
}

#[test]
fn cancelling_one_shared_waker_send_preserves_the_other_waiter() {
    let stream = StaticStream::<Number, 1, 1, 2>::new();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    let sender = stream.sender();
    sender.send_now(1).unwrap();
    let count = Arc::new(Counter::default());
    let waker = Waker::from(count.clone());
    let mut context = Context::from_waker(&waker);
    let mut first = Box::pin(sender.send(2));
    let mut second = Box::pin(sender.send(3));
    assert!(first.as_mut().poll(&mut context).is_pending());
    assert!(second.as_mut().poll(&mut context).is_pending());
    let mut excess = Box::pin(sender.send(4));
    assert_eq!(
        excess.as_mut().poll(&mut context),
        Poll::Ready(Err(SendError::WaitersFull(4)))
    );
    drop(first);
    let mut replacement = Box::pin(sender.send(5));
    assert!(replacement.as_mut().poll(&mut context).is_pending());
    drop(replacement);
    assert_eq!(receiver.recv_now().unwrap().unwrap(), data(0, 1));
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert_eq!(second.as_mut().poll(&mut context), Poll::Ready(Ok(())));
    assert_eq!(receiver.recv_now().unwrap().unwrap(), data(1, 3));
    assert_eq!(receiver.recv_now().unwrap(), None);
}

#[test]
fn concurrent_receives_have_distinct_bounded_waiter_records() {
    let stream = StaticStream::<Number, 2, 1, 2>::new();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    let count = Arc::new(Counter::default());
    let waker = Waker::from(count.clone());
    let mut context = Context::from_waker(&waker);
    let mut first = Box::pin(receiver.recv());
    let mut second = Box::pin(receiver.recv());
    let mut excess = Box::pin(receiver.recv());
    assert!(first.as_mut().poll(&mut context).is_pending());
    assert!(second.as_mut().poll(&mut context).is_pending());
    assert_eq!(
        excess.as_mut().poll(&mut context),
        Poll::Ready(Err(ReceiveError::WaitersFull))
    );
    drop(first);
    stream.sender().send_now(7).unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        second.as_mut().poll(&mut context),
        Poll::Ready(Ok(data(0, 7)))
    );
}

#[test]
fn close_terminates_waiters_and_does_not_expose_retained_data() {
    let stream = StaticStream::<Number, 1>::new();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    let sender = stream.sender();
    sender.send_now(1).unwrap();
    let count = Arc::new(Counter::default());
    let waker = Waker::from(count.clone());
    let mut context = Context::from_waker(&waker);
    let mut pending = Box::pin(sender.send(2));
    assert!(pending.as_mut().poll(&mut context).is_pending());
    stream.close(CloseReason::Revoked);
    stream.close(CloseReason::Closed);
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        pending.as_mut().poll(&mut context),
        Poll::Ready(Err(SendError::Revoked(2)))
    );
    assert_eq!(
        receiver.recv_now(),
        Err(ReceiveError::Closed(CloseReason::Revoked))
    );
    assert!(matches!(sender.send_now(3), Err(TrySendError::Revoked(3))));
    assert!(matches!(
        stream.subscribe(SubscriptionRole::Observer),
        Err(TopicError::Revoked)
    ));

    let empty = StaticStream::<Number, 1>::new();
    let receiver = empty.subscribe(SubscriptionRole::Observer).unwrap();
    let mut receive = Box::pin(receiver.recv());
    assert!(receive.as_mut().poll(&mut context).is_pending());
    empty.close(CloseReason::Disconnected);
    assert_eq!(count.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        receive.as_mut().poll(&mut context),
        Poll::Ready(Err(ReceiveError::Closed(CloseReason::Disconnected)))
    );
}

struct ReenterOnDrop {
    stream: Arc<StaticStream<Number, 2>>,
    drops: Arc<AtomicUsize>,
}

impl Wake for ReenterOnDrop {
    fn wake(self: Arc<Self>) {
        drop(self);
    }
}

impl Drop for ReenterOnDrop {
    fn drop(&mut self) {
        self.stream.sender().send_now(77).unwrap();
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn cancellation_drops_user_wakers_outside_the_stream_lock() {
    let stream = Arc::new(StaticStream::<Number, 2>::new());
    let receiver = stream.subscribe(SubscriptionRole::Observer).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut pending = Box::pin(receiver.recv());
    {
        let waker = Waker::from(Arc::new(ReenterOnDrop {
            stream: stream.clone(),
            drops: drops.clone(),
        }));
        assert!(pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
    }
    drop(pending);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(receiver.recv_now().unwrap().unwrap(), data(0, 77));
}

#[test]
fn sequence_exhaustion_rejects_without_wrapping_or_losing_the_payload() {
    let stream = StaticStream::<Number, 2>::new();
    stream.with(|state| state.tail = u64::MAX - 1);
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    let sender = stream.sender();
    sender.send_now(8).unwrap();
    let error = sender.send_now(9).unwrap_err();
    assert!(matches!(error, TrySendError::SequenceExhausted(9)));
    assert_eq!(error.into_inner(), 9);
    let mut pending = Box::pin(sender.send(10));
    assert_eq!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(SendError::SequenceExhausted(10)))
    );
    assert_eq!(receiver.recv_now().unwrap().unwrap(), data(u64::MAX - 1, 8));
    assert_eq!(receiver.recv_now().unwrap(), None);
}

#[test]
fn all_async_publication_conveniences_return_rejected_payloads() {
    fn rejected(future: impl Future<Output = Result<(), SendError<u32>>>) -> SendError<u32> {
        let Poll::Ready(Err(error)) = Box::pin(future)
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("closed publication must reject immediately")
        };
        error
    }
    let stream = StaticStream::<Number, 1>::new();
    let sender = stream.sender();
    let topic = Topic::<Number, _>::new(sender);
    stream.close(CloseReason::Closed);
    assert_eq!(rejected(sender.send(11)), SendError::Closed(11));
    assert_eq!(rejected(topic.send(22)), SendError::Closed(22));
    assert_eq!(
        rejected(sender.publish(EventValue::new(33))),
        SendError::Closed(33)
    );
    assert_eq!(
        rejected(publish(&sender, EventValue::new(44))),
        SendError::Closed(44)
    );
}

#[test]
fn async_errors_preserve_move_only_borrowed_payloads_without_debug_bounds() {
    struct MoveOnly<'a>(&'a core::cell::Cell<u32>);
    impl Drop for MoveOnly<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    let drops = core::cell::Cell::new(0);
    for variant in 0..4 {
        let payload = MoveOnly(&drops);
        let error = match variant {
            0 => SendError::Closed(payload),
            1 => SendError::Revoked(payload),
            2 => SendError::WaitersFull(payload),
            _ => SendError::SequenceExhausted(payload),
        };
        let _display = std::format!("{error:?}: {error}");
        let mapped = error.map(|payload| (payload, 7));
        assert!(matches!(
            (&mapped, variant),
            (SendError::Closed(_), 0)
                | (SendError::Revoked(_), 1)
                | (SendError::WaitersFull(_), 2)
                | (SendError::SequenceExhausted(_), 3)
        ));
        let (payload, tag) = mapped.into_inner();
        assert_eq!(tag, 7);
        assert_eq!(drops.get(), variant);
        drop(payload);
        assert_eq!(drops.get(), variant + 1);
    }
}

#[test]
fn stale_membership_keys_cannot_read_reused_subscriber_slots() {
    let mut state = State::<u32, 1, 1>::new();
    let old = state.subscribe(SubscriptionRole::Observer).unwrap();
    state.cursors[old.index] = None;
    let current = state.subscribe(SubscriptionRole::Observer).unwrap();
    state.send_now(12).unwrap();
    assert_eq!(
        state.recv_now(old),
        Err(ReceiveError::Closed(CloseReason::Closed))
    );
    assert_eq!(state.recv_now(current).unwrap().unwrap(), data(0, 12));
}

#[test]
fn receive_and_sender_registration_race_cannot_lose_progress() {
    for _ in 0..128 {
        let stream = StaticStream::<Number, 1>::new();
        let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
        let sender = stream.sender();
        sender.send_now(1).unwrap();
        let count = Arc::new(Counter::default());
        let waker = Waker::from(count.clone());
        let mut future = Box::pin(sender.send(2));
        let barrier = Barrier::new(2);
        let first_poll = std::thread::scope(|scope| {
            let task = scope.spawn(|| {
                barrier.wait();
                future.as_mut().poll(&mut Context::from_waker(&waker))
            });
            barrier.wait();
            assert_eq!(receiver.recv_now().unwrap().unwrap(), data(0, 1));
            task.join().unwrap()
        });
        if first_poll.is_pending() {
            assert!(count.0.load(Ordering::SeqCst) > 0);
            assert_eq!(
                future.as_mut().poll(&mut Context::from_waker(&waker)),
                Poll::Ready(Ok(()))
            );
        } else {
            assert_eq!(first_poll, Poll::Ready(Ok(())));
        }
        assert_eq!(receiver.recv_now().unwrap().unwrap(), data(1, 2));
    }
}

#[test]
fn concurrent_publications_have_one_acceptance_sequence() {
    let stream = StaticStream::<Number, 80>::new();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    let barrier = Barrier::new(4);
    std::thread::scope(|scope| {
        for producer in 0..4 {
            let stream = &stream;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for index in 0..20 {
                    stream.sender().send_now(producer * 100 + index).unwrap();
                }
            });
        }
    });
    let mut next = [0; 4];
    for expected_sequence in 0..80 {
        let StreamItem::Data { sequence, value } = receiver.recv_now().unwrap().unwrap() else {
            panic!("required receiver lost data")
        };
        assert_eq!(sequence, expected_sequence);
        let producer = (*value / 100) as usize;
        assert_eq!(*value % 100, next[producer]);
        next[producer] += 1;
    }
    assert_eq!(next, [20; 4]);
    assert_eq!(receiver.recv_now().unwrap(), None);
}

#[test]
fn bounded_stream_matches_an_unbounded_history_model() {
    for mut script in 0..5usize.pow(6) {
        let stream = StaticStream::<Number, 2, 2>::new();
        let observer = stream.subscribe(SubscriptionRole::Observer).unwrap();
        let mut required = Some(stream.subscribe(SubscriptionRole::Required).unwrap());
        let mut history = std::vec::Vec::new();
        let mut observer_next = 0;
        let mut required_next = 0;
        for step in 0..6 {
            match script % 5 {
                0 => {
                    let result = stream.sender().send_now(step);
                    if required.is_some() && history.len() - required_next == 2 {
                        assert!(matches!(result, Err(TrySendError::Full(value)) if value == step));
                    } else {
                        result.unwrap();
                        history.push(step);
                    }
                }
                1 => {
                    let oldest = history.len().saturating_sub(2);
                    let expected = if observer_next < oldest {
                        let from = observer_next;
                        observer_next = oldest;
                        Some(StreamItem::Gap {
                            from: from as u64,
                            to: oldest as u64,
                        })
                    } else if observer_next < history.len() {
                        let item = data(observer_next as u64, history[observer_next]);
                        observer_next += 1;
                        Some(item)
                    } else {
                        None
                    };
                    assert_eq!(observer.recv_now().unwrap(), expected);
                }
                2 => {
                    if let Some(receiver) = required.as_ref() {
                        let expected = history.get(required_next).map(|value| StreamItem::Data {
                            sequence: required_next as u64,
                            value: PayloadValue(*value),
                        });
                        if expected.is_some() {
                            required_next += 1;
                        }
                        assert_eq!(receiver.recv_now().unwrap(), expected);
                    }
                }
                3 => {
                    required = None;
                }
                4 => {
                    if required.is_none() {
                        required = Some(stream.subscribe(SubscriptionRole::Required).unwrap());
                        required_next = history.len();
                    }
                }
                _ => unreachable!(),
            }
            script /= 5;
        }
    }
}
