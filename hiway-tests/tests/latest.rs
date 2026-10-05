use hiway::{
    Delivery, EventSpec, PayloadValue, SchemaRevision, StaticStream, StreamItem, SubscriptionRole,
    TopicError, WireCodec, WireError,
};
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

#[hiway::events]
pub enum Updates {
    #[event(id = "latest.test.snapshot", latest)]
    Snapshot(u32),
    #[event(id = "latest.test.action")]
    Action(u32),
}

macro_rules! number_codec {
    ($event:ty) => {
        impl WireCodec for $event {
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
    };
}
number_codec!(updates::Snapshot);
number_codec!(updates::Action);

fn data(sequence: u64, value: u32) -> StreamItem<PayloadValue<u32>> {
    StreamItem::Data {
        sequence,
        value: PayloadValue(value),
    }
}

#[test]
fn static_snapshots_coalesce_and_replay_without_hiding_action_gaps() {
    let snapshots = StaticStream::<updates::Snapshot, 1>::new();
    let watcher = snapshots.subscribe(SubscriptionRole::Observer).unwrap();
    assert_eq!(watcher.recv_now().unwrap(), None);
    assert_eq!(
        snapshots.subscribe(SubscriptionRole::Required).err(),
        Some(TopicError::InvalidConfig)
    );
    for value in 0..4 {
        snapshots.sender().send_now(value).unwrap();
    }
    assert_eq!(watcher.recv_now().unwrap(), Some(data(3, 3)));
    assert_eq!(watcher.recv_now().unwrap(), None);
    let late = snapshots.subscribe(SubscriptionRole::Observer).unwrap();
    assert_eq!(late.recv_now().unwrap(), Some(data(3, 3)));
    snapshots.sender().send_now(4).unwrap();
    assert_eq!(watcher.recv_now().unwrap(), Some(data(4, 4)));
    assert_eq!(late.recv_now().unwrap(), Some(data(4, 4)));

    let actions = StaticStream::<updates::Action, 1>::new();
    let observer = actions.subscribe(SubscriptionRole::Observer).unwrap();
    actions.sender().send_now(10).unwrap();
    actions.sender().send_now(11).unwrap();
    assert_eq!(
        observer.recv_now().unwrap(),
        Some(StreamItem::Gap { from: 0, to: 1 })
    );
    assert_eq!(observer.recv_now().unwrap(), Some(data(1, 11)));
    let required = actions.subscribe(SubscriptionRole::Required).unwrap();
    actions.sender().send_now(12).unwrap();
    assert!(matches!(
        actions.sender().send_now(13),
        Err(hiway::TrySendError::Full(13))
    ));
    assert_eq!(required.recv_now().unwrap(), Some(data(2, 12)));
}

#[test]
fn invalid_snapshot_capacity_is_rejected_and_idle_watchers_wake_on_change() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct Signal(AtomicUsize);
    impl std::task::Wake for Signal {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    assert!(std::panic::catch_unwind(StaticStream::<updates::Snapshot, 2>::new).is_err());
    let snapshots = StaticStream::<updates::Snapshot, 1>::new();
    let watcher = snapshots.subscribe(SubscriptionRole::Observer).unwrap();
    let signal = Arc::new(Signal(AtomicUsize::new(0)));
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut receive = pin!(watcher.recv());
    assert!(receive.as_mut().poll(&mut cx).is_pending());
    assert_eq!(signal.0.load(Ordering::Relaxed), 0);
    snapshots.sender().send_now(7).unwrap();
    assert!(signal.0.load(Ordering::Relaxed) > 0);
    assert_eq!(receive.as_mut().poll(&mut cx), Poll::Ready(Ok(data(0, 7))));
}

fn receive_frame<E: WireCodec<Payload = u32>>(
    protocol: &mut hiway::transport::Protocol,
    sequence: u64,
    generation: u32,
) -> Result<(), hiway::transport::Error> {
    use hiway::transport::{Input, IoResult, Lane, OpId};
    let mut frame = [0; 42];
    hiway::transport::encode_frame::<E>(sequence, &7, &mut frame).unwrap();
    let op = OpId {
        slot: 0,
        generation,
    };
    protocol.input(Input::Accepted {
        lane: Lane::Data,
        op,
    })?;
    protocol.input(Input::Completed {
        op,
        result: IoResult::Bytes(frame.len()),
        buffer: &frame,
    })?;
    protocol.input(Input::Admitted)?;
    let op = OpId {
        slot: 1,
        generation,
    };
    protocol.input(Input::Accepted {
        lane: Lane::CreditSend,
        op,
    })?;
    protocol.input(Input::Completed {
        op,
        result: IoResult::Bytes(9),
        buffer: &[],
    })
}

#[test]
fn snapshot_wire_versions_may_skip_but_never_repeat_regress_or_wrap() {
    use hiway::transport::{Contract, Direction, Error, Protocol};
    for (first, next, latest, ordered) in [
        (0, 1, true, true),
        (0, 4, true, false),
        (4, 4, false, false),
        (4, 3, false, false),
        (u64::MAX - 1, u64::MAX, true, true),
        (u64::MAX, 0, false, false),
    ] {
        let mut snapshot =
            Protocol::new(Contract::of::<updates::Snapshot>(), Direction::Import, 64).unwrap();
        let mut action =
            Protocol::new(Contract::of::<updates::Action>(), Direction::Import, 64).unwrap();
        receive_frame::<updates::Snapshot>(&mut snapshot, first, 1).unwrap();
        receive_frame::<updates::Action>(&mut action, first, 1).unwrap();
        assert_eq!(
            receive_frame::<updates::Snapshot>(&mut snapshot, next, 2),
            if latest { Ok(()) } else { Err(Error::Protocol) }
        );
        assert_eq!(
            receive_frame::<updates::Action>(&mut action, next, 2),
            if ordered {
                Ok(())
            } else {
                Err(Error::Protocol)
            }
        );
    }
}

#[cfg(feature = "std")]
mod dynamic {
    use super::*;
    use hiway::{
        DynamicFabric, Grant, Limits, Permission, Rights, StreamConfig, StreamLimits, TrySendError,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn scope<E: EventSpec + 'static>(items: usize) -> (DynamicFabric, Grant)
    where
        E::Payload: Send + Sync + 'static,
    {
        let fabric = DynamicFabric::new();
        fabric
            .create_stream::<E>(StreamConfig {
                capacity: 1,
                subscribers: 3,
                waiters: 3,
            })
            .unwrap();
        let grant = fabric
            .grant(
                &[
                    Permission::new::<E>(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED)
                        .with_limits(StreamLimits {
                            retained_items: items,
                            subscriptions: 3,
                            waiters: 3,
                        }),
                ],
                Limits {
                    streams: 1,
                    retained_items: items + 1,
                    subscriptions: 3,
                    waiters: 5,
                    connections: 2,
                    bytes: 4096,
                    ..Limits::ZERO
                },
            )
            .unwrap();
        (fabric, grant)
    }

    #[test]
    fn current_snapshot_survives_consumption_maintenance_and_late_subscription() {
        struct OrderedAlias;
        impl EventSpec for OrderedAlias {
            type Payload = u32;
            const ID: hiway::EventId = updates::Snapshot::ID;
        }

        let (fabric, grant) = scope::<updates::Snapshot>(2);
        let sender = grant.sender::<updates::Snapshot>().unwrap();
        let watcher = grant
            .subscribe::<updates::Snapshot>(SubscriptionRole::Observer)
            .unwrap();
        assert_eq!(
            grant
                .subscribe::<updates::Snapshot>(SubscriptionRole::Required)
                .err(),
            Some(TopicError::InvalidConfig)
        );
        for value in 0..4 {
            sender.send_now(value).unwrap();
        }
        let Some(StreamItem::Data { sequence, value }) = watcher.recv_now().unwrap() else {
            panic!("snapshot missing");
        };
        assert_eq!((sequence, *value), (3, 3));
        fabric.maintain();
        assert_eq!(watcher.recv_now().unwrap(), None);
        let late = grant
            .subscribe::<updates::Snapshot>(SubscriptionRole::Observer)
            .unwrap();
        let Some(StreamItem::Data { sequence, value }) = late.recv_now().unwrap() else {
            panic!("current state was retired");
        };
        assert_eq!((sequence, *value), (3, 3));

        assert_eq!(
            fabric.create_stream::<OrderedAlias>(StreamConfig {
                capacity: 1,
                subscribers: 3,
                waiters: 3
            }),
            Err(TopicError::InvalidConfig)
        );
        assert_eq!(
            grant.sender::<OrderedAlias>().err(),
            Some(TopicError::InvalidConfig)
        );
        assert_eq!(
            grant
                .subscribe::<OrderedAlias>(SubscriptionRole::Observer)
                .err(),
            Some(TopicError::InvalidConfig)
        );
        assert_eq!(
            fabric.create_stream::<updates::Snapshot>(StreamConfig {
                capacity: 2,
                subscribers: 3,
                waiters: 3
            }),
            Err(TopicError::InvalidConfig)
        );
        let (_, small) = scope::<updates::Snapshot>(1);
        assert_eq!(
            small.sender::<updates::Snapshot>().err(),
            Some(TopicError::Capacity)
        );
        fabric.close_stream::<updates::Snapshot>().unwrap();
        assert!(watcher.recv_now().is_err());
        assert_eq!(
            grant
                .stream_usage::<updates::Snapshot>()
                .unwrap()
                .retained_items,
            0
        );
    }

    struct Tracked {
        id: u32,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    struct Snapshot;
    impl EventSpec for Snapshot {
        type Payload = Tracked;
        const ID: hiway::EventId = hiway::EventId::from_name("latest.test.tracked");
        const DELIVERY: Delivery = Delivery::Latest;
    }

    #[test]
    fn strict_replacement_defers_retirement_and_preserves_current_on_rejection() {
        struct Signal(AtomicUsize);
        impl std::task::Wake for Signal {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let (fabric, grant) = scope::<Snapshot>(3);
        let sender = grant.sender::<Snapshot>().unwrap();
        let watcher = grant
            .subscribe::<Snapshot>(SubscriptionRole::Observer)
            .unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let payload = |id| Tracked {
            id,
            drops: drops.clone(),
        };
        sender.send_now(payload(0)).unwrap();
        let Some(StreamItem::Data { value: old, .. }) = watcher.recv_now().unwrap() else {
            panic!("no initial state");
        };
        let signal = Arc::new(Signal(AtomicUsize::new(0)));
        let waker = Waker::from(signal.clone());
        let mut cx = Context::from_waker(&waker);
        let mut waiting = pin!(watcher.recv());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        let first = sender.prepare(payload(1)).unwrap();
        let second = sender.prepare(payload(2)).unwrap();
        let wake_count_before = signal.0.load(Ordering::Relaxed);
        assert!(first.try_send().is_ok());
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        assert_eq!(signal.0.load(Ordering::Relaxed), wake_count_before);
        let Err(TrySendError::MaintenanceRequired(second)) = second.try_send() else {
            panic!("retirement storage was not bounded");
        };
        let Some(StreamItem::Data { value: current, .. }) = watcher.try_recv().unwrap() else {
            panic!("failed replacement lost state");
        };
        assert_eq!(current.id, 1);
        drop(old);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        fabric.maintain();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(signal.0.load(Ordering::Relaxed) > wake_count_before);
        assert!(second.try_send().is_ok());
        fabric.maintain();
        assert_eq!(
            drops.load(Ordering::Relaxed),
            1,
            "held snapshot retired early"
        );
        drop(current);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        let Some(StreamItem::Data { value, .. }) = watcher.recv_now().unwrap() else {
            panic!("no replacement state");
        };
        assert_eq!(value.id, 2);
        drop(value);
        fabric.close_stream::<Snapshot>().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 3);
        assert_eq!(grant.stream_usage::<Snapshot>().unwrap().retained_items, 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn uring_replays_current_state_and_coalesces_updates_across_credit_waits() {
        use hiway_uring::Driver;
        use std::{
            os::unix::net::UnixStream,
            time::{Duration, Instant},
        };
        let source = StaticStream::<updates::Snapshot, 1>::new();
        let destination = StaticStream::<updates::Snapshot, 1>::new();
        source.sender().send_now(0).unwrap();
        let watcher = destination.subscribe(SubscriptionRole::Observer).unwrap();
        let (outgoing, incoming) = UnixStream::pair().unwrap();
        let (credit_out, credit_in) = UnixStream::pair().unwrap();
        let mut driver = Driver::<(), 2, 64>::new(4).unwrap();
        let mut export = pin!(driver
            .export(
                source.subscribe(SubscriptionRole::Observer).unwrap(),
                outgoing,
                credit_out,
                ()
            )
            .unwrap());
        let mut import = pin!(driver
            .import(destination.sender(), incoming, credit_in, ())
            .unwrap());
        let mut cx = Context::from_waker(Waker::noop());
        let deadline = Instant::now() + Duration::from_secs(5);
        for expected in [0, 10] {
            if expected == 10 {
                for value in 1..=10 {
                    source.sender().send_now(value).unwrap();
                }
            }
            loop {
                assert!(export.as_mut().poll(&mut cx).is_pending());
                assert!(import.as_mut().poll(&mut cx).is_pending());
                driver.advance_io().unwrap();
                driver.dispatch();
                if let Some(StreamItem::Data { value, .. }) = watcher.recv_now().unwrap() {
                    assert_eq!(*value, expected);
                    break;
                }
                assert!(Instant::now() < deadline, "snapshot link stalled");
                std::thread::yield_now();
            }
        }
        driver.cancel_all();
        while !driver.shutdown_complete() {
            driver.advance_io().unwrap();
            driver.dispatch();
            assert!(Instant::now() < deadline, "snapshot shutdown stalled");
            std::thread::yield_now();
        }
    }

    #[cfg(all(feature = "tokio-io", unix))]
    #[tokio::test]
    async fn unix_replays_snapshots_and_accepts_forward_version_jumps() {
        use hiway::UnixLink;
        use tokio::{
            net::UnixStream,
            time::{timeout, Duration},
        };
        let (source_fabric, source) = scope::<updates::Snapshot>(2);
        let (destination_fabric, destination) = scope::<updates::Snapshot>(2);
        let sender = source.sender::<updates::Snapshot>().unwrap();
        sender.send_now(0).unwrap();
        let watcher = destination
            .subscribe::<updates::Snapshot>(SubscriptionRole::Observer)
            .unwrap();
        let (outgoing, incoming) = UnixStream::pair().unwrap();
        let (credit_out, credit_in) = UnixStream::pair().unwrap();
        let export = UnixLink::export::<updates::Snapshot>(
            source.clone(),
            outgoing,
            credit_out,
            SubscriptionRole::Observer,
            4,
        )
        .unwrap();
        let import =
            UnixLink::import::<updates::Snapshot>(destination.clone(), incoming, credit_in, 4)
                .unwrap();
        let export = tokio::spawn(export);
        let import = tokio::spawn(import);
        for expected in [0, 10] {
            if expected == 10 {
                for value in 1..=10 {
                    sender.send_now(value).unwrap();
                }
            }
            let StreamItem::Data { value, .. } = timeout(Duration::from_secs(5), watcher.recv())
                .await
                .unwrap()
                .unwrap()
            else {
                panic!("snapshot gap");
            };
            assert_eq!(*value, expected);
        }
        source.revoke().await.unwrap();
        destination.revoke().await.unwrap();
        assert!(timeout(Duration::from_secs(5), export)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert!(timeout(Duration::from_secs(5), import)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        source_fabric.close_stream::<updates::Snapshot>().unwrap();
        destination_fabric
            .close_stream::<updates::Snapshot>()
            .unwrap();
    }
}
