#![cfg(loom)]

use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use hiway::{
    DynamicFabric, DynamicReceiver, DynamicSender, EventId, EventSpec, Grant, Limits, Permission,
    ReceiveError, Rights, StreamConfig, StreamItem, SubscriptionRole, TopicError, TrySendError,
};
use loom::thread;

struct Value;

impl EventSpec for Value {
    type Payload = u8;
    const ID: EventId = EventId::from_name("loom::value");
}

// These schedules instrument production stream/grant mutexes and atomics.
// Arc, ArcSwap, and notification internals are not part of the modeled contract.
// Two preemptions bound the explored schedules; this is not an async wake proof.
fn model(check: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(check);
}

fn setup() -> (DynamicFabric, Grant) {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<Value>(StreamConfig {
            capacity: 2,
            subscribers: 2,
            waiters: 2,
        })
        .unwrap();
    let grant = fabric
        .grant(
            &[
                Permission::new::<Value>(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED)
                    .with_limits(hiway::StreamLimits {
                        retained_items: 4,
                        subscriptions: 2,
                        waiters: 2,
                    }),
            ],
            Limits {
                streams: 2,
                grants: 2,
                subscriptions: 2,
                retained_items: 4,
                waiters: 2,
                ..Limits::ZERO
            },
        )
        .unwrap();
    (fabric, grant)
}

fn admit_prepared(sender: &DynamicSender<Value>, value: u8) -> Result<(), TrySendError<u8>> {
    sender.prepare(value).and_then(|prepared| {
        prepared
            .try_send()
            .map_err(|error| error.map(hiway::PreparedPublication::into_inner))
    })
}

fn retry_prepared_value(fabric: &DynamicFabric, sender: &DynamicSender<Value>, value: u8) {
    fabric.maintain();
    match admit_prepared(sender, value) {
        Ok(()) => {}
        Err(TrySendError::Contended(value) | TrySendError::MaintenanceRequired(value)) => {
            fabric.maintain();
            admit_prepared(sender, value).expect("same-path prepared retry after maintenance");
        }
        Err(error) => panic!("unexpected prepared retry rejection: {error:?}"),
    }
}

fn retry_prepared(
    fabric: &DynamicFabric,
    sender: &DynamicSender<Value>,
    result: Result<(), TrySendError<u8>>,
) {
    match result {
        Ok(()) => {}
        Err(TrySendError::Contended(value) | TrySendError::MaintenanceRequired(value)) => {
            retry_prepared_value(fabric, sender, value);
        }
        Err(error) => panic!("unexpected admission rejection: {error:?}"),
    }
}

fn receive(receiver: &DynamicReceiver<Value>) -> (u64, u8) {
    match receiver.recv_now().unwrap().unwrap() {
        StreamItem::Data { sequence, value } => (sequence, *value),
        StreamItem::Gap { from, to } => panic!("required receiver lost {from}..{to}"),
    }
}

struct WakeCount(loom::sync::atomic::AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, loom::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn static_registration_and_slot_reuse_raced_with_publication_preserve_progress() {
    model(|| {
        let stream = Arc::new(hiway::StaticStream::<Value, 1, 1, 2>::new());
        let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
        let mut cancelled = Box::pin(receiver.recv());
        assert!(cancelled
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        let producer = stream.clone();
        let publication = thread::spawn(move || producer.sender().send_now(7).unwrap());
        drop(cancelled);
        let counter = Arc::new(WakeCount(loom::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut context = Context::from_waker(&waker);
        let mut live = Box::pin(receiver.recv());
        let first = live.as_mut().poll(&mut context);
        publication.join().unwrap();
        let result = if first.is_pending() {
            assert!(counter.0.load(loom::sync::atomic::Ordering::SeqCst) > 0);
            live.as_mut().poll(&mut context)
        } else {
            first
        };
        assert!(
            matches!(result, Poll::Ready(Ok(StreamItem::Data { sequence: 0, value })) if *value == 7)
        );
        let mut first = Box::pin(receiver.recv());
        let mut second = Box::pin(receiver.recv());
        assert!(first.as_mut().poll(&mut context).is_pending());
        assert!(second.as_mut().poll(&mut context).is_pending());
        assert_eq!(
            Box::pin(receiver.recv()).as_mut().poll(&mut context),
            Poll::Ready(Err(ReceiveError::WaitersFull))
        );
    });
}

#[test]
fn static_required_receipt_raced_with_sender_registration_cannot_lose_wakeup() {
    model(|| {
        let stream = Arc::new(hiway::StaticStream::<Value, 1, 1, 1>::new());
        let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
        stream.sender().send_now(1).unwrap();
        let producer = stream.clone();
        let publication = thread::spawn(move || {
            loom::future::block_on(producer.sender().send(2)).unwrap();
        });
        assert!(
            matches!(receiver.recv_now(), Ok(Some(StreamItem::Data { sequence: 0, value })) if *value == 1)
        );
        publication.join().unwrap();
        assert!(
            matches!(receiver.recv_now(), Ok(Some(StreamItem::Data { sequence: 1, value })) if *value == 2)
        );
        assert_eq!(receiver.recv_now().unwrap(), None);
    });
}

#[test]
fn static_close_raced_with_receive_registration_terminates_the_future() {
    model(|| {
        let stream = Arc::new(hiway::StaticStream::<Value, 1, 1, 1>::new());
        let receiver = stream.subscribe(SubscriptionRole::Observer).unwrap();
        let owner = stream.clone();
        let close = thread::spawn(move || owner.close(hiway::CloseReason::Revoked));
        assert_eq!(
            loom::future::block_on(receiver.recv()),
            Err(ReceiveError::Closed(hiway::CloseReason::Revoked))
        );
        close.join().unwrap();
    });
}

#[test]
fn delegation_raced_with_revocation_cannot_admit_after_the_cutoff() {
    model(|| {
        let (fabric, parent) = setup();
        let observer = fabric
            .grant(
                &[
                    Permission::new::<Value>(Rights::OBSERVE).with_limits(hiway::StreamLimits {
                        subscriptions: 1,
                        ..hiway::StreamLimits::ZERO
                    }),
                ],
                Limits {
                    streams: 1,
                    subscriptions: 1,
                    ..Limits::ZERO
                },
            )
            .unwrap()
            .subscribe::<Value>(SubscriptionRole::Observer)
            .unwrap();
        let delegate = parent.clone();
        let publication = thread::spawn(move || {
            let child = match delegate.restrict(
                &[
                    Permission::new::<Value>(Rights::PUBLISH).with_limits(hiway::StreamLimits {
                        retained_items: 1,
                        ..hiway::StreamLimits::ZERO
                    }),
                ],
                Limits {
                    streams: 1,
                    retained_items: 1,
                    ..Limits::ZERO
                },
            ) {
                Ok(child) => child,
                Err(error) => {
                    assert_eq!(error, TopicError::Revoked);
                    return false;
                }
            };
            let sender = match child.sender::<Value>() {
                Ok(sender) => sender,
                Err(error) => {
                    assert_eq!(error, TopicError::Revoked);
                    return false;
                }
            };
            match sender.send_now(9) {
                Ok(()) => true,
                Err(TrySendError::Revoked(9)) => false,
                Err(error) => panic!("unexpected publication failure: {error:?}"),
            }
        });
        loom::future::block_on(parent.revoke()).unwrap();
        let at_cutoff = observer.recv_now().unwrap();
        let admitted = publication.join().unwrap();
        assert_eq!(at_cutoff.is_some(), admitted);
        if let Some(item) = at_cutoff {
            assert!(matches!(item, StreamItem::Data { sequence: 0, value } if *value == 9));
        }
        assert_eq!(observer.recv_now().unwrap(), None);
        assert_eq!(parent.usage(), Limits::ZERO);
    });
}

#[test]
fn first_publisher_binding_raced_with_revocation_cannot_strand_a_full_stream_waiter() {
    model(|| {
        let (fabric, local) = setup();
        let receiver = local
            .subscribe::<Value>(SubscriptionRole::Required)
            .unwrap();
        let sender = local.sender::<Value>().unwrap();
        sender.send_now(1).unwrap();
        sender.send_now(2).unwrap();
        let publisher = fabric
            .grant(
                &[
                    Permission::new::<Value>(Rights::PUBLISH).with_limits(hiway::StreamLimits {
                        retained_items: 1,
                        waiters: 1,
                        ..hiway::StreamLimits::ZERO
                    }),
                ],
                Limits {
                    streams: 1,
                    retained_items: 1,
                    waiters: 1,
                    ..Limits::ZERO
                },
            )
            .unwrap();
        let peer = publisher.clone();
        let publication = thread::spawn(move || match peer.sender::<Value>() {
            Ok(sender) => assert_eq!(
                loom::future::block_on(sender.send(9)),
                Err(hiway::SendError::Revoked(9))
            ),
            Err(error) => assert_eq!(error, TopicError::Revoked),
        });
        loom::future::block_on(publisher.revoke()).unwrap();
        publication.join().unwrap();
        assert_eq!(receive(&receiver), (0, 1));
        assert_eq!(receive(&receiver), (1, 2));
        assert_eq!(receiver.recv_now().unwrap(), None);
        assert_eq!(publisher.usage(), Limits::ZERO);
    });
}

#[test]
fn cancelling_a_receive_raced_with_prepared_admission_preserves_the_other_waiter() {
    model(|| {
        let (fabric, grant) = setup();
        let receiver = grant
            .subscribe::<Value>(SubscriptionRole::Required)
            .unwrap();
        let sender = grant.sender::<Value>().unwrap();
        let mut cancelled = Box::pin(receiver.recv());
        assert!(cancelled
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        let producer = sender.clone();
        let publish = thread::spawn(move || admit_prepared(&producer, 7));
        let counter = Arc::new(WakeCount(loom::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut context = Context::from_waker(&waker);
        let mut surviving = Box::pin(receiver.recv());
        let first = surviving.as_mut().poll(&mut context);
        drop(cancelled);
        retry_prepared(&fabric, &sender, publish.join().unwrap());
        fabric.maintain();
        let result = match first {
            Poll::Ready(result) => result,
            Poll::Pending => {
                assert!(counter.0.load(loom::sync::atomic::Ordering::SeqCst) > 0);
                let Poll::Ready(result) = surviving.as_mut().poll(&mut context) else {
                    panic!("surviving receive did not progress after admission");
                };
                result
            }
        };
        assert!(matches!(result, Ok(StreamItem::Data { sequence: 0, value }) if *value == 7));
        drop(surviving);
        assert_eq!(grant.usage().waiters, 0);
        assert_eq!(grant.usage().retained_items, 0);
    });
}

#[test]
fn concurrent_publishers_preserve_unique_admission_sequences() {
    model(|| {
        let (_fabric, grant) = setup();
        let sender = grant.sender::<Value>().unwrap();
        let receiver = grant
            .subscribe::<Value>(SubscriptionRole::Required)
            .unwrap();
        let first_sender = sender.clone();
        let second_sender = sender.clone();
        let first = thread::spawn(move || first_sender.send_now(1));
        let second = thread::spawn(move || second_sender.send_now(2));
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        let mut admitted = Vec::new();
        for (value, result) in [(1_u8, first), (2, second)] {
            match result {
                Ok(()) => admitted.push(value),
                Err(TrySendError::Contended(rejected)) => assert_eq!(rejected, value),
                Err(error) => panic!("unexpected concurrent send_now: {error:?}"),
            }
        }
        assert!(
            !admitted.is_empty(),
            "an empty capacity-2 stream admitted neither send_now"
        );

        let mut got = Vec::with_capacity(admitted.len());
        for _ in 0..admitted.len() {
            got.push(receive(&receiver));
        }
        assert_eq!(
            got.iter().map(|item| item.0).collect::<Vec<_>>(),
            (0..admitted.len() as u64).collect::<Vec<_>>()
        );
        let mut values: Vec<_> = got.iter().map(|item| item.1).collect();
        values.sort_unstable();
        let mut expected = admitted;
        expected.sort_unstable();
        assert_eq!(values, expected);
        assert!(receiver.recv_now().unwrap().is_none());
        assert_eq!(grant.usage().retained_items, 0);
    });
}

#[test]
fn prepared_publishers_racing_maintenance_preserve_credits_and_sequences() {
    model(|| {
        let (fabric, grant) = setup();
        let receiver = grant
            .subscribe::<Value>(SubscriptionRole::Required)
            .unwrap();
        let first = grant.sender::<Value>().unwrap();
        let second = first.clone();
        let left = thread::spawn(move || admit_prepared(&first, 1));
        let right = thread::spawn(move || admit_prepared(&second, 2));
        fabric.maintain();
        let left = left.join().unwrap();
        let right = right.join().unwrap();
        let mut admitted = Vec::new();
        let mut rejected = Vec::new();
        for (value, result) in [(1_u8, left), (2, right)] {
            match result {
                Ok(()) => admitted.push(value),
                Err(TrySendError::Contended(value) | TrySendError::MaintenanceRequired(value)) => {
                    rejected.push(value);
                }
                Err(error) => panic!("unexpected concurrent prepared admission: {error:?}"),
            }
        }
        assert_eq!(grant.usage().retained_items, admitted.len());
        let sender = grant.sender::<Value>().unwrap();
        for value in rejected {
            retry_prepared_value(&fabric, &sender, value);
        }
        assert_eq!(grant.usage().retained_items, 2);
        let unpack = |item: StreamItem<std::sync::Arc<u8>>| match item {
            StreamItem::Data { sequence, value } => (sequence, *value),
            StreamItem::Gap { .. } => panic!("required data lost"),
        };
        let first = unpack(receiver.try_recv().unwrap().unwrap());
        let second = unpack(receiver.try_recv().unwrap().unwrap());
        assert_eq!((first.0, second.0), (0, 1));
        assert!(matches!((first.1, second.1), (1, 2) | (2, 1)));
        assert_eq!(grant.usage().retained_items, 2);
        fabric.maintain();
        assert_eq!(grant.usage().retained_items, 0);
    });
}

#[test]
fn subscription_before_publication_sees_the_admitted_item() {
    model(|| {
        let (fabric, grant) = setup();
        let subscriber = grant.clone();
        let (ready, bound) = loom::sync::mpsc::channel();
        let (proceed, start) = loom::sync::mpsc::channel();
        let binding = thread::spawn(move || {
            let receiver = subscriber
                .subscribe::<Value>(SubscriptionRole::Required)
                .unwrap();
            ready.send(()).unwrap();
            start.recv().unwrap();
            receiver
        });
        bound.recv().unwrap();
        let sender = grant.sender::<Value>().unwrap();
        admit_prepared(&sender, 7).unwrap();
        proceed.send(()).unwrap();
        let receiver = binding.join().unwrap();
        assert_eq!(receive(&receiver), (0, 7));
        fabric.maintain();
        admit_prepared(&sender, 8).unwrap();
        assert_eq!(receive(&receiver), (1, 8));
        assert_eq!(grant.usage().retained_items, 0);
    });
}

#[test]
fn publication_before_subscription_starts_at_the_new_tail() {
    model(|| {
        let (fabric, grant) = setup();
        let sender = grant.sender::<Value>().unwrap();
        admit_prepared(&sender, 7).unwrap();
        let receiver = grant
            .subscribe::<Value>(SubscriptionRole::Required)
            .unwrap();
        assert_eq!(receiver.try_recv().unwrap(), None);
        fabric.maintain();
        admit_prepared(&sender, 8).unwrap();
        assert_eq!(receive(&receiver), (1, 8));
        assert_eq!(grant.usage().retained_items, 0);
    });
}

#[test]
fn subscription_raced_with_publication_starts_at_one_consistent_tail() {
    model(|| {
        let (fabric, grant) = setup();
        let subscriber = grant.clone();
        let binding =
            thread::spawn(move || subscriber.subscribe::<Value>(SubscriptionRole::Required));
        let sender = grant.sender::<Value>().unwrap();
        let result = admit_prepared(&sender, 7);
        let receiver = binding.join().unwrap().unwrap();
        match result {
            Ok(()) => match receiver.try_recv().unwrap() {
                Some(StreamItem::Data { sequence: 0, value }) => assert_eq!(*value, 7),
                None => {}
                other => panic!("inconsistent subscription boundary: {other:?}"),
            },
            Err(TrySendError::Contended(7) | TrySendError::MaintenanceRequired(7)) => {
                assert_eq!(
                    receiver.try_recv().unwrap(),
                    None,
                    "a rejected first attempt must not produce data"
                );
                retry_prepared_value(&fabric, &sender, 7);
                assert_eq!(receive(&receiver), (0, 7));
                fabric.maintain();
                admit_prepared(&sender, 8).unwrap();
                assert_eq!(receive(&receiver), (1, 8));
                assert_eq!(grant.usage().retained_items, 0);
                return;
            }
            Err(error) => panic!("unexpected raced publication: {error:?}"),
        }
        fabric.maintain();
        admit_prepared(&sender, 8).unwrap();
        assert_eq!(receive(&receiver), (1, 8));
        assert_eq!(grant.usage().retained_items, 0);
    });
}

#[test]
fn subscription_raced_with_revocation_cannot_create_a_live_late_receiver() {
    model(|| {
        let (fabric, grant) = setup();
        let subscriber = grant.clone();
        let binding =
            thread::spawn(move || subscriber.subscribe::<Value>(SubscriptionRole::Required));
        let mut revoke = Box::pin(grant.revoke());
        let mut context = Context::from_waker(Waker::noop());
        let first = revoke.as_mut().poll(&mut context);
        let receiver = binding.join().unwrap();
        fabric.maintain();
        if first.is_pending() {
            assert_eq!(revoke.as_mut().poll(&mut context), Poll::Ready(Ok(())));
        } else {
            assert_eq!(first, Poll::Ready(Ok(())));
        }
        match receiver {
            Ok(receiver) => assert!(matches!(
                receiver.try_recv(),
                Err(ReceiveError::Closed(hiway::CloseReason::Revoked))
            )),
            Err(error) => assert_eq!(error, TopicError::Revoked),
        }
        assert_eq!(grant.usage().subscriptions, 0);
    });
}

#[test]
fn detaching_the_required_receiver_raced_with_admission_conserves_prepared_credit() {
    model(|| {
        let (fabric, grant) = setup();
        let receiver = grant
            .subscribe::<Value>(SubscriptionRole::Required)
            .unwrap();
        let sender = grant.sender::<Value>().unwrap();
        let producer = sender.clone();
        let (ready, prepared) = loom::sync::mpsc::channel();
        let (proceed, start) = loom::sync::mpsc::channel();
        let publish = thread::spawn(move || {
            let value = producer.prepare(3).unwrap();
            ready.send(()).unwrap();
            start.recv().unwrap();
            value
                .try_send()
                .map_err(|error| error.map(hiway::PreparedPublication::into_inner))
        });
        prepared.recv().unwrap();
        sender.send_now(1).unwrap();
        sender.send_now(2).unwrap();
        proceed.send(()).unwrap();
        drop(receiver);
        let accepted = match publish.join().unwrap() {
            Ok(()) => true,
            Err(
                TrySendError::Full(3)
                | TrySendError::Contended(3)
                | TrySendError::MaintenanceRequired(3),
            ) => false,
            other => panic!("unexpected detach/admission outcome: {other:?}"),
        };
        fabric.maintain();
        assert_eq!(grant.usage().retained_items, 0);
        let receiver = grant
            .subscribe::<Value>(SubscriptionRole::Required)
            .unwrap();
        sender.prepare(4).unwrap().try_send().unwrap();
        assert_eq!(receive(&receiver), (if accepted { 3 } else { 2 }, 4));
        assert_eq!(grant.usage().retained_items, 0);
    });
}

#[test]
fn append_raced_with_receipt_preserves_required_data_and_cursor() {
    model(|| {
        let (_fabric, grant) = setup();
        let sender = grant.sender::<Value>().unwrap();
        let receiver = grant
            .subscribe::<Value>(SubscriptionRole::Required)
            .unwrap();
        sender.send_now(7).unwrap();
        let appender = sender.clone();
        let reader = receiver.clone();
        let append = thread::spawn(move || appender.send_now(8));
        let read = thread::spawn(move || reader.recv_now());
        let append = append.join().unwrap();
        let read = read.join().unwrap();
        let first = match read {
            Ok(Some(StreamItem::Data { sequence, value })) => (sequence, *value),
            Err(ReceiveError::Contended) => receive(&receiver),
            other => panic!("required receipt lost existing data: {other:?}"),
        };
        assert_eq!(first, (0, 7));
        match append {
            Ok(()) => {}
            Err(TrySendError::Contended(8)) => sender.send_now(8).unwrap(),
            Err(error) => panic!("unexpected concurrent append: {error:?}"),
        }
        assert_eq!(receive(&receiver), (1, 8));
        assert!(receiver.recv_now().unwrap().is_none());
        assert_eq!(grant.usage().retained_items, 0);
    });
}

#[test]
fn revocation_cutoff_raced_with_admission_rejects_late_work() {
    model(|| {
        let (fabric, parent) = setup();
        let receiver = parent
            .subscribe::<Value>(SubscriptionRole::Required)
            .unwrap();
        let publisher = parent
            .restrict(
                &[
                    Permission::new::<Value>(Rights::PUBLISH).with_limits(hiway::StreamLimits {
                        retained_items: 2,
                        subscriptions: 0,
                        waiters: 0,
                    }),
                ],
                Limits {
                    streams: 1,
                    retained_items: 2,
                    ..Limits::ZERO
                },
            )
            .unwrap();
        let sender = publisher.sender::<Value>().unwrap();
        let concurrent_sender = sender.clone();
        let admission = thread::spawn(move || admit_prepared(&concurrent_sender, 9));
        let mut revoke = Box::pin(publisher.revoke());
        let mut context = Context::from_waker(Waker::noop());
        let first_poll = revoke.as_mut().poll(&mut context);
        let at_cutoff = if first_poll.is_ready() {
            Some(receiver.recv_now())
        } else {
            None
        };
        let admitted = match admission.join().unwrap() {
            Ok(()) => true,
            Err(TrySendError::Revoked(9) | TrySendError::Contended(9)) => false,
            other => panic!("unexpected raced admission result: {other:?}"),
        };
        fabric.maintain();
        if first_poll.is_pending() {
            assert_eq!(revoke.as_mut().poll(&mut context), Poll::Ready(Ok(())));
        } else {
            assert_eq!(first_poll, Poll::Ready(Ok(())));
        }
        assert!(publisher.is_revoked());
        assert!(matches!(
            sender.send_now(10),
            Err(TrySendError::Revoked(10))
        ));

        match at_cutoff {
            Some(Ok(Some(StreamItem::Data { sequence, value }))) => {
                assert!(admitted);
                assert_eq!((sequence, *value), (0, 9));
                assert!(receiver.recv_now().unwrap().is_none());
            }
            Some(Ok(None)) => {
                assert!(!admitted, "publication appeared after a completed cutoff");
                assert!(receiver.recv_now().unwrap().is_none());
            }
            Some(Err(ReceiveError::Contended)) | None => {
                if admitted {
                    assert_eq!(receive(&receiver), (0, 9));
                }
                assert!(receiver.recv_now().unwrap().is_none());
            }
            other => panic!("unexpected required receipt at cutoff: {other:?}"),
        }
    });
}

#[test]
fn concurrent_child_reservations_and_release_conserve_parent_allowance() {
    model(|| {
        let fabric = DynamicFabric::new();
        fabric
            .create_stream::<Value>(StreamConfig {
                capacity: 1,
                subscribers: 1,
                waiters: 0,
            })
            .unwrap();
        let parent = fabric
            .grant(
                &[Permission::new::<Value>(Rights::OBSERVE)],
                Limits {
                    streams: 2,
                    grants: 1,
                    subscriptions: 1,
                    retained_items: 1,
                    waiters: 1,
                    connections: 1,
                    bytes: 1,
                },
            )
            .unwrap();
        let allowance = Limits {
            streams: 1,
            grants: 0,
            subscriptions: 1,
            retained_items: 1,
            waiters: 1,
            connections: 1,
            bytes: 1,
        };
        let first_parent = parent.clone();
        let second_parent = parent.clone();
        let first = thread::spawn(move || {
            first_parent.restrict(&[Permission::new::<Value>(Rights::OBSERVE)], allowance)
        });
        let second = thread::spawn(move || {
            second_parent.restrict(&[Permission::new::<Value>(Rights::OBSERVE)], allowance)
        });
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        let ((Ok(child), Err(TopicError::Capacity)) | (Err(TopicError::Capacity), Ok(child))) =
            (first, second)
        else {
            panic!("a single grant slot must admit exactly one live child");
        };
        let expected = Limits {
            grants: 1,
            ..allowance
        };
        assert_eq!(parent.usage(), expected);

        let contender = parent.clone();
        let release = thread::spawn(move || drop(child));
        let reserve = thread::spawn(move || {
            contender.restrict(&[Permission::new::<Value>(Rights::OBSERVE)], allowance)
        });
        let reserved = reserve.join().unwrap();
        assert!(
            parent.usage().grants <= 1,
            "drop/restrict charged more than one live child grant"
        );
        match reserved {
            Ok(replacement) => {
                release.join().unwrap();
                assert_eq!(parent.usage(), expected);
                drop(replacement);
            }
            Err(TopicError::Capacity) => {
                release.join().unwrap();
                assert_eq!(parent.usage(), Limits::ZERO);
                let replacement = parent
                    .restrict(&[Permission::new::<Value>(Rights::OBSERVE)], allowance)
                    .expect("credits must be visible after drop joins");
                assert_eq!(parent.usage(), expected);
                drop(replacement);
            }
            Err(error) => panic!("unexpected reservation failure: {error:?}"),
        }
        assert_eq!(parent.usage(), Limits::ZERO);
    });
}

#[test]
fn concurrent_delegation_reserves_one_event_without_consuming_another() {
    struct Other;
    impl EventSpec for Other {
        type Payload = u8;
        const ID: EventId = EventId::from_u128(912);
    }
    model(|| {
        let fabric = DynamicFabric::new();
        fabric
            .create_stream::<Value>(StreamConfig {
                capacity: 2,
                subscribers: 0,
                waiters: 0,
            })
            .unwrap();
        fabric
            .create_stream::<Other>(StreamConfig {
                capacity: 2,
                subscribers: 0,
                waiters: 0,
            })
            .unwrap();
        let per_event = hiway::StreamLimits {
            retained_items: 1,
            ..hiway::StreamLimits::ZERO
        };
        let permission = Permission::new::<Value>(Rights::PUBLISH).with_limits(per_event);
        let parent = fabric
            .grant(
                &[
                    permission,
                    Permission::new::<Other>(Rights::PUBLISH).with_limits(per_event),
                ],
                Limits {
                    streams: 3,
                    grants: 2,
                    retained_items: 2,
                    ..Limits::ZERO
                },
            )
            .unwrap();
        let allowance = Limits {
            streams: 1,
            retained_items: 1,
            ..Limits::ZERO
        };
        let left = parent.clone();
        let right = parent.clone();
        let first = loom::thread::spawn(move || left.restrict(&[permission], allowance));
        let second = loom::thread::spawn(move || right.restrict(&[permission], allowance));
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        for result in [&first, &second] {
            if let Err(error) = result {
                assert_eq!(*error, hiway::TopicError::Capacity);
            }
        }
        assert_eq!(parent.stream_usage::<Value>().unwrap().retained_items, 1);
        assert_eq!(parent.stream_usage::<Other>().unwrap().retained_items, 0);
        parent.sender::<Other>().unwrap().send_now(7).unwrap();
        assert_eq!(parent.usage().retained_items, 2);
        drop(first);
        drop(second);
        assert_eq!(
            parent.stream_usage::<Value>().unwrap(),
            hiway::StreamLimits::ZERO
        );
        fabric.close_stream::<Other>().unwrap();
        assert_eq!(parent.usage(), Limits::ZERO);
    });
}
