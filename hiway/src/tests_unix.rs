use std::{
    future::Future,
    io::{ErrorKind, Read, Write},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::{
    DynamicFabric, EventId, EventSpec, Grant, Limits, Permission, Rights, SchemaRevision,
    StreamConfig, StreamItem, SubscriptionRole, TopicError, WireCodec, WireError,
};

use super::{IpcError, UnixLink};

#[path = "tests_unix_model.rs"]
mod model;

#[path = "tests_unix_schedule.rs"]
mod schedule;

struct Number;

impl EventSpec for Number {
    type Payload = u32;
    const ID: EventId = EventId::from_u128(5);
}

impl WireCodec for Number {
    fn encoded_len(_: &u32) -> usize {
        4
    }

    fn encode(value: &u32, output: &mut [u8]) -> Result<usize, WireError> {
        output
            .get_mut(..4)
            .ok_or(WireError::BufferTooSmall)?
            .copy_from_slice(&value.to_le_bytes());
        Ok(4)
    }

    fn decode(bytes: &[u8], revision: SchemaRevision) -> Result<u32, WireError> {
        if revision != SchemaRevision(1) {
            return Err(WireError::UnsupportedRevision);
        }
        Ok(u32::from_le_bytes(
            bytes.try_into().map_err(|_| WireError::InvalidPayload)?,
        ))
    }
}

fn allowance() -> Limits {
    Limits {
        streams: 4,
        grants: 4,
        subscriptions: 8,
        retained_items: 16,
        waiters: 16,
        connections: 8,
        bytes: 1024,
    }
}

#[tokio::test]
async fn ipc_cannot_borrow_event_reservations_and_full_transport_pool_cannot_block_local_progress()
{
    for generic_items in 0..=1 {
        let fabric = DynamicFabric::new();
        fabric
            .create_stream::<Number>(StreamConfig {
                capacity: 1,
                subscribers: 1,
                waiters: 1,
            })
            .unwrap();
        let event_limits = crate::StreamLimits {
            retained_items: 1,
            subscriptions: 1,
            waiters: 1,
        };
        let grant = fabric
            .grant(
                &[
                    Permission::new::<Number>(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED)
                        .with_limits(event_limits),
                ],
                Limits {
                    streams: 1,
                    retained_items: 1 + generic_items,
                    subscriptions: 1,
                    waiters: 3,
                    connections: 2,
                    bytes: 60,
                    ..Limits::ZERO
                },
            )
            .unwrap();
        let sender = grant.sender::<Number>().unwrap();
        let receiver = grant
            .subscribe::<Number>(SubscriptionRole::Required)
            .unwrap();
        let before = grant.usage();
        let (data, _peer_data) = UnixStream::pair().unwrap();
        let (control, _peer_control) = UnixStream::pair().unwrap();
        let link = UnixLink::import::<Number>(grant.clone(), data, control, 4);
        if generic_items == 0 {
            assert!(matches!(link, Err(IpcError::Topic(TopicError::Capacity))));
            assert_eq!(grant.usage(), before);
            sender.send_now(7).unwrap();
            assert!(
                matches!(receiver.recv_now().unwrap(), Some(StreamItem::Data { value, .. }) if *value == 7)
            );
        } else {
            let mut link = link.unwrap();
            sender.send_now(7).unwrap();
            let mut context = Context::from_waker(Waker::noop());
            let mut pending = std::pin::pin!(sender.send(8));
            assert!(pending.as_mut().poll(&mut context).is_pending());
            assert_eq!(grant.usage().waiters, 3);
            assert_eq!(grant.stream_usage::<Number>().unwrap(), event_limits);
            assert!(
                matches!(receiver.recv_now().unwrap(), Some(StreamItem::Data { value, .. }) if *value == 7)
            );
            assert!(matches!(
                pending.as_mut().poll(&mut context),
                Poll::Ready(Ok(()))
            ));
            grant.revoke().await.unwrap();
            assert!(matches!(
                Pin::new(&mut link).poll(&mut context),
                Poll::Ready(Err(IpcError::Revoked))
            ));
            assert_eq!(grant.usage().connections, 0);
            assert_eq!(grant.usage().bytes, 0);
            fabric.close_stream::<Number>().unwrap();
            assert_eq!(grant.usage().retained_items, 0);
        }
    }
}

fn permission(rights: Rights) -> [Permission; 1] {
    [
        Permission::new::<Number>(rights).with_limits(crate::StreamLimits {
            retained_items: 8,
            subscriptions: 4,
            waiters: 8,
        }),
    ]
}

fn setup(capacity: usize) -> (DynamicFabric, Grant) {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<Number>(StreamConfig {
            capacity,
            subscribers: 8,
            waiters: 16,
        })
        .unwrap();
    let grant = fabric
        .grant(
            &permission(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED),
            allowance(),
        )
        .unwrap();
    (fabric, grant)
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("IPC operation failed to make progress")
}

#[tokio::test]
async fn relay_retains_source_charges_until_revoked_under_downstream_backpressure() {
    bounded(async {
        let (fabric, parent) = setup(1);
        let source = parent
            .restrict(
                &permission(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED),
                Limits {
                    grants: 0,
                    ..allowance()
                },
            )
            .unwrap();
        let reservation = parent.usage();
        let (input, mut upstream) = UnixStream::pair().unwrap();
        let (input_control, mut upstream_control) = UnixStream::pair().unwrap();
        let (output, mut downstream) = UnixStream::pair().unwrap();
        let (output_control, downstream_control) = UnixStream::pair().unwrap();
        let relay = UnixLink::relay::<Number>(
            source.clone(),
            (input, input_control),
            (output, output_control),
            SubscriptionRole::Required,
            4,
        )
        .unwrap();
        assert_eq!(source.usage().connections, 4);
        assert_eq!(source.usage().bytes, 120);
        assert_eq!(source.usage().retained_items, 2);
        assert_eq!(source.usage().waiters, 4);
        let peer = async {
            upstream.write_all(&frame(10, 40)).await.unwrap();
            let mut credit = [0; 9];
            upstream_control.read_exact(&mut credit).await.unwrap();
            assert_eq!(credit, [1, 10, 0, 0, 0, 0, 0, 0, 0]);
            let mut bytes = [0; 42];
            downstream.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes.as_slice(), frame(0, 40));
            // Withhold downstream credit: one frame stays in flight, one stays local.
            upstream.write_all(&frame(11, 41)).await.unwrap();
            upstream_control.read_exact(&mut credit).await.unwrap();
            assert_eq!(credit, [1, 11, 0, 0, 0, 0, 0, 0, 0]);
            upstream.write_all(&frame(12, 42)).await.unwrap();
            while source.stream_usage::<Number>().unwrap().waiters == 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(source.usage().connections, 4);
            assert_eq!(source.usage().bytes, 120);
            assert_eq!(source.usage().retained_items, 3);
            assert_eq!(source.usage().waiters, 5);
            assert_eq!(parent.usage(), reservation);
            parent.revoke().await.unwrap();
            (upstream, upstream_control, downstream, downstream_control)
        };
        let (result, _sockets) = tokio::join!(relay, peer);
        assert!(matches!(result, Err(IpcError::Revoked)));
        fabric.maintain();
        assert_eq!(source.usage(), Limits::ZERO);
        assert_eq!(parent.usage(), reservation);
        drop(source);
        assert_eq!(parent.usage(), Limits::ZERO);
    })
    .await;
}

#[tokio::test]
async fn relay_constructor_rolls_back_when_only_one_leg_fits() {
    let (fabric, _) = setup(1);
    let source = fabric
        .grant(
            &permission(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED),
            Limits {
                connections: 2,
                ..allowance()
            },
        )
        .unwrap();
    let (input, _upstream) = UnixStream::pair().unwrap();
    let (input_control, _upstream_control) = UnixStream::pair().unwrap();
    let (output, _downstream) = UnixStream::pair().unwrap();
    let (output_control, _downstream_control) = UnixStream::pair().unwrap();
    let result = UnixLink::relay::<Number>(
        source.clone(),
        (input, input_control),
        (output, output_control),
        SubscriptionRole::Required,
        4,
    );
    assert!(matches!(result, Err(IpcError::Topic(TopicError::Capacity))));
    assert_eq!(source.usage(), Limits::ZERO);
}

// The peer fixture builds the published wire layout without calling the decoder
// or encoder under test: magic, event, major, revision, sequence, payload length.
fn frame(sequence: u64, value: u32) -> Vec<u8> {
    let mut bytes = b"HWY1".to_vec();
    bytes.extend_from_slice(&5_u128.to_le_bytes());
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&sequence.to_le_bytes());
    bytes.extend_from_slice(&4_u32.to_le_bytes());
    bytes.extend_from_slice(&value.to_le_bytes());
    bytes
}

#[tokio::test]
async fn local_quota_activity_does_not_wake_an_idle_ipc_link() {
    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (_fabric, grant) = setup(1);
    let sender = grant.sender::<Number>().unwrap();
    let receiver = grant
        .subscribe::<Number>(SubscriptionRole::Required)
        .unwrap();
    let (data, _peer_data) = UnixStream::pair().unwrap();
    let (control, _peer_control) = UnixStream::pair().unwrap();
    let mut import = UnixLink::import::<Number>(grant.clone(), data, control, 4).unwrap();
    let wake_count = Arc::new(WakeCount::default());
    let waker = Waker::from(wake_count.clone());
    let mut context = Context::from_waker(&waker);
    assert!(Pin::new(&mut import).poll(&mut context).is_pending());

    for expected in 0..1024 {
        sender.send_now(expected).unwrap();
        match receiver.recv_now().unwrap().unwrap() {
            StreamItem::Data { value, .. } => assert_eq!(*value, expected),
            item @ StreamItem::Gap { .. } => panic!("required receiver lost local data: {item:?}"),
        }
    }
    assert_eq!(wake_count.0.load(Ordering::Relaxed), 0);
    grant.revoke().await.unwrap();
    assert!(wake_count.0.load(Ordering::Relaxed) > 0);
    assert!(matches!(
        Pin::new(&mut import).poll(&mut context),
        Poll::Ready(Err(IpcError::Revoked))
    ));
}

#[tokio::test]
async fn fragmented_peer_reads_with_immediate_credits_preserve_export_order() {
    bounded(async {
        let (_fabric, grant) = setup(2);
        let sender = grant.sender::<Number>().unwrap();
        let (data, mut peer_data) = UnixStream::pair().unwrap();
        let (control, mut peer_control) = UnixStream::pair().unwrap();
        let export =
            UnixLink::export::<Number>(grant.clone(), data, control, SubscriptionRole::Required, 4)
                .unwrap();
        let publish = async {
            for value in 0..256 {
                sender.send(value).await.unwrap();
            }
        };
        let peer = async {
            let mut previous = None;
            for expected in 0..256 {
                let mut bytes = [0; 42];
                for part in bytes.chunks_mut(3) {
                    peer_data.read_exact(part).await.unwrap();
                }
                let sequence = u64::from_le_bytes(bytes[26..34].try_into().unwrap());
                let mut credit = [1; 9];
                credit[1..].copy_from_slice(&sequence.to_le_bytes());
                peer_control.write_all(&credit).await.unwrap();
                assert_eq!(
                    u32::from_le_bytes(bytes[38..42].try_into().unwrap()),
                    expected
                );
                if let Some(last) = previous {
                    assert_eq!(sequence, last + 1);
                }
                previous = Some(sequence);
            }
            grant.revoke().await.unwrap();
            (peer_data, peer_control)
        };
        let (result, (), _sockets) = tokio::join!(export, publish, peer);
        assert!(matches!(result, Err(IpcError::Revoked)));
    })
    .await;
}

#[tokio::test]
async fn authorized_links_transfer_between_independent_fabrics() {
    bounded(async {
        let (_source, source) = setup(2);
        let (_destination, destination) = setup(2);
        let sender = source.sender::<Number>().unwrap();
        let receiver = destination
            .subscribe::<Number>(SubscriptionRole::Required)
            .unwrap();
        let (source_data, destination_data) = UnixStream::pair().unwrap();
        let (source_control, destination_control) = UnixStream::pair().unwrap();
        let export = UnixLink::export::<Number>(
            source.clone(),
            source_data,
            source_control,
            SubscriptionRole::Required,
            4,
        )
        .unwrap();
        let import = UnixLink::import::<Number>(
            destination.clone(),
            destination_data,
            destination_control,
            4,
        )
        .unwrap();
        let consume = async {
            sender.send(17).await.unwrap();
            sender.send(29).await.unwrap();
            for expected in [17, 29] {
                match receiver.recv().await.unwrap() {
                    StreamItem::Data { value, .. } => assert_eq!(*value, expected),
                    item @ StreamItem::Gap { .. } => {
                        panic!("required receiver lost data: {item:?}")
                    }
                }
            }
            source.revoke().await.unwrap();
            destination.revoke().await.unwrap();
        };
        let (exported, imported, ()) = tokio::join!(export, import, consume);
        assert!(matches!(exported, Err(IpcError::Revoked)));
        assert!(matches!(imported, Err(IpcError::Revoked)));
        assert_eq!(source.usage().connections, 0);
        assert_eq!(destination.usage().bytes, 0);
    })
    .await;
}

#[tokio::test]
async fn binding_cannot_add_rights_or_exceed_byte_allowance() {
    let (fabric, _grant) = setup(1);
    let observe = fabric
        .grant(&permission(Rights::OBSERVE), allowance())
        .unwrap();
    let (data, _peer_data) = UnixStream::pair().unwrap();
    let (control, _peer_control) = UnixStream::pair().unwrap();
    assert!(matches!(
        UnixLink::import::<Number>(observe.clone(), data, control, 4),
        Err(IpcError::Topic(TopicError::Denied))
    ));
    let (data, _peer_data) = UnixStream::pair().unwrap();
    let (control, _peer_control) = UnixStream::pair().unwrap();
    assert!(matches!(
        UnixLink::export::<Number>(
            observe.clone(),
            data,
            control,
            SubscriptionRole::Required,
            4
        ),
        Err(IpcError::Topic(TopicError::Denied))
    ));
    assert_eq!(observe.usage().connections, 0);
    assert_eq!(observe.usage().bytes, 0);

    let limited = fabric
        .grant(
            &permission(Rights::PUBLISH),
            Limits {
                bytes: 3,
                ..allowance()
            },
        )
        .unwrap();
    let (data, _peer_data) = UnixStream::pair().unwrap();
    let (control, _peer_control) = UnixStream::pair().unwrap();
    assert!(matches!(
        UnixLink::import::<Number>(limited.clone(), data, control, 4),
        Err(IpcError::Topic(TopicError::Capacity))
    ));
    assert_eq!(limited.usage().connections, 0);
    assert_eq!(limited.usage().retained_items, 0);
}

#[tokio::test]
async fn invalid_headers_are_rejected_before_waiting_for_payload() {
    bounded(async {
        for field in [0, 4, 20, 34] {
            let (_fabric, grant) = setup(1);
            let receiver = grant
                .subscribe::<Number>(SubscriptionRole::Observer)
                .unwrap();
            let (data, mut peer_data) = UnixStream::pair().unwrap();
            let (control, _peer_control) = UnixStream::pair().unwrap();
            let import = UnixLink::import::<Number>(grant.clone(), data, control, 4).unwrap();
            let mut bytes = frame(7, 41);
            bytes[field] ^= 1;
            if field == 34 {
                bytes[34] = 5;
            }
            peer_data.write_all(&bytes[..38]).await.unwrap();
            let result = import.await;
            if field == 34 {
                assert!(matches!(
                    result,
                    Err(IpcError::Oversized {
                        length: 5,
                        limit: 4
                    })
                ));
            } else {
                assert!(matches!(result, Err(IpcError::Protocol)));
            }
            assert!(receiver.recv_now().unwrap().is_none());
            assert_eq!(grant.usage().bytes, 0);
        }
    })
    .await;
}

#[tokio::test]
async fn fragmented_frame_and_duplicate_sequence_preserve_single_admission() {
    bounded(async {
        let (_fabric, grant) = setup(2);
        let receiver = grant
            .subscribe::<Number>(SubscriptionRole::Required)
            .unwrap();
        let (data, mut peer_data) = UnixStream::pair().unwrap();
        let (control, mut peer_control) = UnixStream::pair().unwrap();
        let import = UnixLink::import::<Number>(grant, data, control, 4).unwrap();
        let peer = async {
            let bytes = frame(7, 41);
            for part in bytes.chunks(3) {
                peer_data.write_all(part).await.unwrap();
                tokio::task::yield_now().await;
            }
            let mut credit = [0; 9];
            peer_control.read_exact(&mut credit).await.unwrap();
            assert_eq!(credit, [1, 7, 0, 0, 0, 0, 0, 0, 0]);
            peer_data.write_all(&bytes).await.unwrap();
            match receiver.recv().await.unwrap() {
                StreamItem::Data { value, .. } => assert_eq!(*value, 41),
                item @ StreamItem::Gap { .. } => panic!("unexpected receive: {item:?}"),
            }
            (peer_data, peer_control)
        };
        let (result, _sockets) = tokio::join!(import, peer);
        assert!(matches!(result, Err(IpcError::Protocol)));
        assert!(receiver.recv_now().unwrap().is_none());
    })
    .await;
}

#[tokio::test]
async fn blocked_local_admission_cannot_hold_revocation_or_leak_credit() {
    bounded(async {
        let (fabric, local) = setup(1);
        let receiver = local.subscribe::<Number>(SubscriptionRole::Required).unwrap();
        local.sender::<Number>().unwrap().send_now(3).unwrap();
        let grant = fabric.grant(&permission(Rights::PUBLISH), allowance()).unwrap();
        let (data, mut peer_data) = UnixStream::pair().unwrap();
        let (control, peer_control) = UnixStream::pair().unwrap();
        let import = UnixLink::import::<Number>(grant.clone(), data, control, 4).unwrap();
        let peer = async {
            peer_data.write_all(&frame(7, 41)).await.unwrap();
            while grant.usage().waiters <= 2 {
                tokio::task::yield_now().await;
            }
            let mut credit = [0; 9];
            let mut peer_control = peer_control.into_std().unwrap();
            assert!(matches!(peer_control.read(&mut credit), Err(error) if error.kind() == ErrorKind::WouldBlock));
            grant.revoke().await.unwrap();
            (peer_data, peer_control)
        };
        let (result, _sockets) = tokio::join!(import, peer);
        assert!(matches!(result, Err(IpcError::Revoked)));
        match receiver.recv().await.unwrap() {
            StreamItem::Data { value, .. } => assert_eq!(*value, 3),
            item @ StreamItem::Gap { .. } => panic!("pre-existing admission changed: {item:?}"),
        }
        assert!(receiver.recv_now().unwrap().is_none());
        assert_eq!(grant.usage().waiters, 0);
        assert_eq!(grant.usage().connections, 0);
        assert_eq!(grant.usage().bytes, 0);
    })
    .await;
}

#[tokio::test]
async fn full_credit_socket_cannot_prevent_local_revocation() {
    bounded(async {
        let (fabric, local) = setup(1);
        let receiver = local
            .subscribe::<Number>(SubscriptionRole::Required)
            .unwrap();
        let grant = fabric
            .grant(&permission(Rights::PUBLISH), allowance())
            .unwrap();
        let (data, mut peer_data) = UnixStream::pair().unwrap();
        let (control, _peer_control) = UnixStream::pair().unwrap();
        let mut raw_control = control.into_std().unwrap();
        loop {
            match raw_control.write(&[0; 4096]) {
                Ok(written) => assert!(written > 0),
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => panic!("socket setup failed: {error}"),
            }
        }
        let import = UnixLink::import::<Number>(
            grant.clone(),
            data,
            UnixStream::from_std(raw_control).unwrap(),
            4,
        )
        .unwrap();
        let peer = async {
            peer_data.write_all(&frame(7, 41)).await.unwrap();
            match receiver.recv().await.unwrap() {
                StreamItem::Data { value, .. } => assert_eq!(*value, 41),
                item @ StreamItem::Gap { .. } => panic!("unexpected receive: {item:?}"),
            }
            grant.revoke().await.unwrap();
        };
        let (result, ()) = tokio::join!(import, peer);
        assert!(matches!(result, Err(IpcError::Revoked)));
        assert_eq!(grant.usage().connections, 0);
        assert_eq!(grant.usage().bytes, 0);
    })
    .await;
}

#[tokio::test]
async fn full_data_socket_cannot_block_control_termination_or_leak_reservations() {
    enum Finish {
        Cancel,
        Revoke,
        Disconnect,
        InvalidCredit,
        DuplicateEarlyCredit,
    }
    bounded(async {
        for finish in [Finish::Cancel, Finish::Revoke, Finish::Disconnect, Finish::InvalidCredit, Finish::DuplicateEarlyCredit] {
            let (fabric, local) = setup(1);
            let grant = fabric.grant(&permission(Rights::OBSERVE | Rights::REQUIRED), allowance()).unwrap();
            let (data, _stopped_peer) = UnixStream::pair().unwrap();
            let mut full_data = data.into_std().unwrap();
            loop {
                match full_data.write(&[0; 4096]) {
                    Ok(written) => assert!(written > 0),
                    Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                    Err(error) => panic!("socket setup failed: {error}"),
                }
            }
            let (control, mut peer_control) = UnixStream::pair().unwrap();
            let mut export = UnixLink::export::<Number>(
                grant.clone(), UnixStream::from_std(full_data).unwrap(), control,
                SubscriptionRole::Required, 4,
            ).unwrap();
            local.sender::<Number>().unwrap().send_now(41).unwrap();
            assert!(Pin::new(&mut export).poll(&mut Context::from_waker(Waker::noop())).is_pending());
            assert_eq!(local.usage().retained_items, 0);
            assert_eq!(grant.usage(), Limits {
                subscriptions: 1, retained_items: 1, waiters: 2, connections: 2, bytes: 60,
                ..Limits::ZERO
            });
            match finish {
                Finish::Cancel => drop(export),
                Finish::Revoke => {
                    grant.revoke().await.unwrap();
                    assert!(matches!(export.await, Err(IpcError::Revoked)));
                }
                Finish::Disconnect => {
                    peer_control.shutdown().await.unwrap();
                    assert!(matches!(export.await, Err(IpcError::Io(error)) if error.kind() == ErrorKind::UnexpectedEof));
                }
                Finish::InvalidCredit | Finish::DuplicateEarlyCredit => {
                    if matches!(finish, Finish::DuplicateEarlyCredit) {
                        peer_control.write_all(&[1, 0, 0, 0, 0, 0, 0, 0, 0]).await.unwrap();
                        peer_control.write_all(&[1, 0, 0, 0, 0, 0, 0, 0, 0]).await.unwrap();
                    } else {
                        peer_control.write_all(&[0; 9]).await.unwrap();
                    }
                    assert!(matches!(export.await, Err(IpcError::Protocol)));
                }
            }
            assert_eq!(grant.usage(), Limits::ZERO);
            assert_eq!(local.usage(), Limits::ZERO);
            assert_eq!(peer_control.read(&mut [0]).await.unwrap(), 0);
        }
    }).await;
}

#[tokio::test]
async fn unsolicited_credit_flood_terminates_without_growing_buffers() {
    bounded(async {
        let (_fabric, grant) = setup(1);
        let (data, _peer_data) = UnixStream::pair().unwrap();
        let (control, mut peer_control) = UnixStream::pair().unwrap();
        let export =
            UnixLink::export::<Number>(grant.clone(), data, control, SubscriptionRole::Observer, 4)
                .unwrap();
        peer_control.write_all(&[1; 900]).await.unwrap();
        assert!(matches!(export.await, Err(IpcError::Protocol)));
        assert_eq!(grant.usage().connections, 0);
        assert_eq!(grant.usage().bytes, 0);
        assert_eq!(grant.usage().subscriptions, 0);
    })
    .await;
}

#[tokio::test]
async fn invalid_credits_after_valid_progress_cannot_authorize_another_frame() {
    bounded(async {
        for (tag, sequence) in [(0, 1_u64), (1, 0), (1, 2)] {
            let (_fabric, grant) = setup(2);
            let (data, mut peer_data) = UnixStream::pair().unwrap();
            let (control, mut peer_control) = UnixStream::pair().unwrap();
            let export = UnixLink::export::<Number>(
                grant.clone(),
                data,
                control,
                SubscriptionRole::Required,
                4,
            )
            .unwrap();
            let sender = grant.sender::<Number>().unwrap();
            sender.send_now(41).unwrap();
            sender.send_now(42).unwrap();
            let peer = async {
                let mut bytes = [0; 42];
                peer_data.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes.as_slice(), frame(0, 41));
                peer_control
                    .write_all(&[1, 0, 0, 0, 0, 0, 0, 0, 0])
                    .await
                    .unwrap();
                peer_data.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes.as_slice(), frame(1, 42));
                let mut credit = [tag; 9];
                credit[1..].copy_from_slice(&sequence.to_le_bytes());
                for byte in credit {
                    peer_control.write_all(&[byte]).await.unwrap();
                    tokio::task::yield_now().await;
                }
                (peer_data, peer_control)
            };
            let (result, (mut peer_data, _peer_control)) = tokio::join!(export, peer);
            assert!(matches!(result, Err(IpcError::Protocol)));
            assert_eq!(peer_data.read(&mut [0]).await.unwrap(), 0);
            assert_eq!(grant.usage(), Limits::ZERO);
        }
    })
    .await;
}

#[tokio::test]
async fn every_truncated_credit_terminates_and_releases_the_export_reservation() {
    bounded(async {
        for prefix in 0..9 {
            let (_fabric, grant) = setup(1);
            let (data, mut peer_data) = UnixStream::pair().unwrap();
            let (control, mut peer_control) = UnixStream::pair().unwrap();
            let export = UnixLink::export::<Number>(
                grant.clone(), data, control, SubscriptionRole::Required, 4,
            ).unwrap();
            grant.sender::<Number>().unwrap().send_now(41).unwrap();
            let peer = async {
                let mut bytes = [0; 42];
                peer_data.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes.as_slice(), frame(0, 41));
                peer_control.write_all(&[1, 0, 0, 0, 0, 0, 0, 0, 0][..prefix]).await.unwrap();
                peer_control.shutdown().await.unwrap();
                (peer_data, peer_control)
            };
            let (result, _sockets) = tokio::join!(export, peer);
            assert!(matches!(result, Err(IpcError::Io(error)) if error.kind() == ErrorKind::UnexpectedEof));
            assert_eq!(grant.usage(), Limits::ZERO);
        }
    }).await;
}

#[tokio::test]
async fn every_truncated_frame_rejects_admission_and_releases_reservations() {
    bounded(async {
        let bytes = frame(7, 41);
        for prefix in 0..bytes.len() {
            let (_fabric, grant) = setup(1);
            let receiver = grant.subscribe::<Number>(SubscriptionRole::Observer).unwrap();
            let baseline = grant.usage();
            let (data, mut peer_data) = UnixStream::pair().unwrap();
            let (control, _peer_control) = UnixStream::pair().unwrap();
            let import = UnixLink::import::<Number>(grant.clone(), data, control, 4).unwrap();
            peer_data.write_all(&bytes[..prefix]).await.unwrap();
            peer_data.shutdown().await.unwrap();
            assert!(matches!(import.await, Err(IpcError::Io(error)) if error.kind() == ErrorKind::UnexpectedEof));
            assert_eq!(receiver.recv_now().unwrap(), None);
            assert_eq!(grant.usage(), baseline);
        }
    })
    .await;
}

#[tokio::test]
async fn cancelling_a_partial_read_closes_sockets_and_releases_its_allowance() {
    bounded(async {
        let (_fabric, grant) = setup(1);
        let (data, mut peer_data) = UnixStream::pair().unwrap();
        let (control, mut peer_control) = UnixStream::pair().unwrap();
        let mut import = UnixLink::import::<Number>(grant.clone(), data, control, 4).unwrap();
        peer_data.write_all(&frame(7, 41)[..20]).await.unwrap();
        tokio::select! {
            biased;
            result = &mut import => panic!("partial header unexpectedly completed: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        drop(import);
        let mut byte = [0];
        assert_eq!(peer_control.read(&mut byte).await.unwrap(), 0);
        assert_eq!(grant.usage().connections, 0);
        assert_eq!(grant.usage().bytes, 0);
        assert_eq!(grant.usage().waiters, 0);
        assert_eq!(grant.usage().retained_items, 0);
    })
    .await;
}

#[tokio::test]
async fn observer_gap_is_reported_instead_of_silently_reordering_export() {
    bounded(async {
        let (_fabric, grant) = setup(1);
        let (data, _peer_data) = UnixStream::pair().unwrap();
        let (control, _peer_control) = UnixStream::pair().unwrap();
        let export =
            UnixLink::export::<Number>(grant.clone(), data, control, SubscriptionRole::Observer, 4)
                .unwrap();
        let sender = grant.sender::<Number>().unwrap();
        sender.send_now(11).unwrap();
        sender.send_now(12).unwrap();
        assert!(matches!(export.await, Err(IpcError::Gap { from, to }) if to - from == 1));
    })
    .await;
}
