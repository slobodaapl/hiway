use hiway::transport::{
    encode_frame, Connection, Contract, Direction, Error, Input, IoResult, Lane, OpId, Outcome,
    Protocol, Record,
};
use hiway::{EventId, EventSpec, SchemaRevision, WireCodec, WireError};

struct Number;
impl EventSpec for Number {
    type Payload = u32;
    const ID: EventId = EventId::from_name("accountability.number");
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

const CONNECTION: Connection = Connection {
    id: 72,
    generation: 9,
};

fn machine(direction: Direction) -> Protocol {
    Protocol::with_connection(Contract::of::<Number>(), direction, 64, CONNECTION).unwrap()
}

fn complete(protocol: &mut Protocol, lane: Lane, count: usize, buffer: &[u8]) -> Result<(), Error> {
    let op = OpId {
        slot: lane as u32,
        generation: 1,
    };
    protocol.input(Input::Accepted { lane, op }).unwrap();
    protocol.input(Input::Completed {
        op,
        result: IoResult::Bytes(count),
        buffer,
    })
}

fn record(protocol: &mut Protocol, direction: Direction, sequence: Option<u64>, outcome: Outcome) {
    assert_eq!(
        protocol.pop_record(),
        Some(Record {
            connection: CONNECTION,
            contract: Contract::of::<Number>(),
            direction,
            sequence,
            outcome,
        })
    );
}

#[test]
fn export_completion_requires_both_full_send_and_matching_credit() {
    for credit_first in [false, true] {
        let mut protocol = machine(Direction::Export);
        protocol
            .input(Input::Provided {
                sequence: 41,
                length: 42,
            })
            .unwrap();
        record(
            &mut protocol,
            Direction::Export,
            Some(41),
            Outcome::Admitted,
        );
        let mut credit = [1; 9];
        credit[1..].copy_from_slice(&41u64.to_le_bytes());
        let lanes = if credit_first {
            [Lane::CreditReceive, Lane::Data]
        } else {
            [Lane::Data, Lane::CreditReceive]
        };
        for (index, lane) in lanes.into_iter().enumerate() {
            complete(
                &mut protocol,
                lane,
                if lane == Lane::Data { 42 } else { 9 },
                &credit,
            )
            .unwrap();
            if index == 0 {
                assert!(protocol.pop_record().is_none());
            }
        }
        record(
            &mut protocol,
            Direction::Export,
            Some(41),
            Outcome::Completed,
        );
        assert!(protocol.pop_record().is_none());
        assert_eq!(protocol.lost_records(), 0);
    }
}

#[test]
fn import_records_admission_before_credit_and_attributes_rejection_to_host() {
    let mut protocol = machine(Direction::Import);
    let mut frame = [0; 64];
    let length = encode_frame::<Number>(42, &17, &mut frame).unwrap();
    complete(&mut protocol, Lane::Data, length, &frame).unwrap();
    assert!(protocol.pop_record().is_none());
    protocol.input(Input::Admitted).unwrap();
    let admission = protocol.pop_record().unwrap();
    assert_eq!(admission.connection, CONNECTION);
    assert_eq!(admission.direction, Direction::Import);
    assert_eq!(admission.sequence, Some(42));
    assert_eq!(admission.outcome, Outcome::Admitted);
    assert!(protocol.pop_record().is_none());
    complete(&mut protocol, Lane::CreditSend, 9, &[]).unwrap();
    let completion = protocol.pop_record().unwrap();
    assert_eq!(completion.outcome, Outcome::Completed);
    assert_eq!(completion.sequence, Some(42));

    encode_frame::<Number>(900, &18, &mut frame).unwrap();
    frame[4..20].copy_from_slice(
        &EventId::from_name("peer.claimed.identity")
            .as_u128()
            .to_le_bytes(),
    );
    assert_eq!(
        complete(&mut protocol, Lane::Data, length, &frame),
        Err(Error::Protocol)
    );
    record(
        &mut protocol,
        Direction::Import,
        Some(900),
        Outcome::Rejected(Error::Protocol),
    );
    protocol.input(Input::Closed(Error::Closed)).unwrap();
    assert!(protocol.pop_record().is_none());
}

#[test]
fn cancellation_is_once_and_never_fabricates_completion() {
    for revoke in [false, true] {
        let mut protocol = machine(Direction::Import);
        let op = OpId {
            slot: 0,
            generation: 7,
        };
        protocol
            .input(Input::Accepted {
                lane: Lane::Data,
                op,
            })
            .unwrap();
        protocol
            .input(if revoke {
                Input::Revoked
            } else {
                Input::Closed(Error::Closed)
            })
            .unwrap();
        record(&mut protocol, Direction::Import, None, Outcome::Cancelled);
        protocol
            .input(Input::Completed {
                op,
                result: IoResult::Bytes(42),
                buffer: &[0; 64],
            })
            .unwrap();
        protocol.input(Input::Closed(Error::Closed)).unwrap();
        assert!(protocol.pop_record().is_none());
    }
    let mut protocol = machine(Direction::Import);
    assert_eq!(
        complete(&mut protocol, Lane::Data, 0, &[]),
        Err(Error::Closed)
    );
    record(
        &mut protocol,
        Direction::Import,
        None,
        Outcome::Rejected(Error::Closed),
    );
}

#[test]
fn bounded_history_reports_loss_and_retains_newest_outcomes() {
    let mut protocol = machine(Direction::Export);
    for sequence in 0..50u64 {
        protocol
            .input(Input::Provided {
                sequence,
                length: 42,
            })
            .unwrap();
        complete(&mut protocol, Lane::Data, 42, &[]).unwrap();
        let mut credit = [1; 9];
        credit[1..].copy_from_slice(&sequence.to_le_bytes());
        complete(&mut protocol, Lane::CreditReceive, 9, &credit).unwrap();
    }
    let mut drained = Vec::new();
    while let Some(record) = protocol.pop_record() {
        drained.push(record);
    }
    assert!(drained.len() <= 8);
    assert!(protocol.lost_records() > 0);
    assert_eq!(
        u64::try_from(drained.len()).unwrap() + protocol.lost_records(),
        100
    );
    assert_eq!(drained.last().unwrap().sequence, Some(49));
    assert_eq!(drained.last().unwrap().outcome, Outcome::Completed);
    assert!(drained
        .windows(2)
        .all(|pair| pair[0].sequence <= pair[1].sequence));
}

#[cfg(all(feature = "std", target_os = "linux"))]
#[test]
fn driver_archives_dropped_links_and_changes_generation_on_slot_reuse() {
    use hiway::{StaticStream, SubscriptionRole};
    use hiway_uring::Driver;
    use std::os::unix::net::UnixStream;
    let mut driver = Driver::<(), 1, 64>::new(2).unwrap();
    let source = StaticStream::<Number, 1>::new();
    let mut identities = Vec::new();
    for _ in 0..80 {
        let (_data_peer, data) = UnixStream::pair().unwrap();
        let (_control_peer, control) = UnixStream::pair().unwrap();
        let (connection, link) = driver
            .export_tracked(
                source.subscribe(SubscriptionRole::Observer).unwrap(),
                data,
                control,
                (),
            )
            .unwrap();
        identities.push(connection);
        drop(link);
        driver.advance_io().unwrap();
        driver.dispatch();
    }
    assert!(identities
        .windows(2)
        .all(|pair| pair[0].id == pair[1].id && pair[1].generation == pair[0].generation + 1));
    let mut records = Vec::new();
    while let Some(record) = driver.pop_record() {
        records.push(record);
    }
    let lost = usize::try_from(driver.lost_records()).unwrap();
    assert!(lost > 0);
    assert!(records.len() <= 64);
    assert_eq!(records.len() + lost, identities.len());
    for (record, connection) in records.iter().zip(identities.iter().skip(lost)) {
        assert_eq!(record.connection, *connection);
        assert_eq!(record.outcome, Outcome::Cancelled);
        assert_eq!(record.sequence, None);
    }
    driver.cancel_all();
    assert!(driver.shutdown_complete());
}

#[cfg(all(feature = "std", target_os = "linux"))]
#[test]
fn driver_records_live_transfer_through_strict_io_and_endpoint_admission() {
    use hiway::{StaticStream, StreamItem, SubscriptionRole};
    use hiway_uring::Driver;
    use std::{
        future::Future,
        os::unix::net::UnixStream,
        task::{Context, Waker},
        time::{Duration, Instant},
    };
    let mut driver = Driver::<(), 2, 64>::new(4).unwrap();
    let source = StaticStream::<Number, 1>::new();
    let destination = StaticStream::<Number, 1>::new();
    let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
    let (outgoing, incoming) = UnixStream::pair().unwrap();
    let (credit_out, credit_in) = UnixStream::pair().unwrap();
    let (export_id, export) = driver
        .export_tracked(
            source.subscribe(SubscriptionRole::Required).unwrap(),
            outgoing,
            credit_out,
            (),
        )
        .unwrap();
    let (import_id, import) = driver
        .import_tracked(destination.sender(), incoming, credit_in, ())
        .unwrap();
    assert_ne!(export_id, import_id);
    let mut export = Box::pin(export);
    let mut import = Box::pin(import);
    source.sender().send_now(91).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut cx = Context::from_waker(Waker::noop());
    let mut records = Vec::new();
    while records
        .iter()
        .filter(|record: &&Record| record.outcome == Outcome::Completed)
        .count()
        < 2
    {
        assert!(export.as_mut().poll(&mut cx).is_pending());
        assert!(import.as_mut().poll(&mut cx).is_pending());
        driver.advance_io().unwrap();
        driver.dispatch();
        while let Some(record) = driver.pop_record() {
            records.push(record);
        }
        assert!(Instant::now() < deadline, "credit completion stalled");
        std::thread::yield_now();
    }
    assert!(
        matches!(receiver.recv_now().unwrap(), Some(StreamItem::Data { sequence: 0, value }) if *value == 91)
    );
    driver.cancel_all();
    drop(export);
    drop(import);
    while !driver.shutdown_complete() {
        driver.advance_io().unwrap();
        driver.dispatch();
        assert!(Instant::now() < deadline, "shutdown stalled");
        std::thread::yield_now();
    }
    while let Some(record) = driver.pop_record() {
        records.push(record);
    }
    for (connection, direction) in [
        (export_id, Direction::Export),
        (import_id, Direction::Import),
    ] {
        let history: Vec<_> = records
            .iter()
            .filter(|record| record.connection == connection)
            .collect();
        assert_eq!(
            history
                .iter()
                .map(|record| record.outcome)
                .collect::<Vec<_>>(),
            [Outcome::Admitted, Outcome::Completed, Outcome::Cancelled]
        );
        assert!(history.iter().all(|record| record.sequence == Some(0)
            && record.contract == Contract::of::<Number>()
            && record.direction == direction));
    }
    assert_eq!(driver.lost_records(), 0);
}

#[cfg(all(feature = "tokio-io", unix))]
mod unix {
    use super::*;
    use hiway::{
        DynamicFabric, Grant, Limits, Permission, Rights, StreamConfig, StreamLimits, UnixLink,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixStream,
        time::{timeout, Duration},
    };

    fn scope() -> Grant {
        let fabric = DynamicFabric::new();
        fabric
            .create_stream::<Number>(StreamConfig {
                capacity: 1,
                ..StreamConfig::default()
            })
            .unwrap();
        fabric
            .grant(
                &[
                    Permission::new::<Number>(Rights::PUBLISH | Rights::OBSERVE).with_limits(
                        StreamLimits {
                            retained_items: 2,
                            subscriptions: 1,
                            waiters: 2,
                        },
                    ),
                ],
                Limits {
                    streams: 1,
                    retained_items: 3,
                    connections: 2,
                    waiters: 4,
                    subscriptions: 1,
                    bytes: 1024,
                    ..Limits::ZERO
                },
            )
            .unwrap()
    }

    #[tokio::test]
    async fn tracked_export_completes_after_matching_credit_and_reports_revocation() {
        use hiway::SubscriptionRole;
        let source = scope();
        let destination = scope();
        let (outgoing, incoming) = UnixStream::pair().unwrap();
        let (credit_out, credit_in) = UnixStream::pair().unwrap();
        let (export, history) = UnixLink::export_tracked::<Number>(
            source.clone(),
            outgoing,
            credit_out,
            SubscriptionRole::Observer,
            4,
            CONNECTION,
        )
        .unwrap();
        let import =
            UnixLink::import::<Number>(destination.clone(), incoming, credit_in, 4).unwrap();
        let export = tokio::spawn(export);
        let import = tokio::spawn(import);
        source.sender::<Number>().unwrap().send_now(38).unwrap();
        let mut records = Vec::new();
        timeout(Duration::from_secs(5), async {
            loop {
                if let Some(record) = history.pop_record() {
                    let complete = record.outcome == Outcome::Completed;
                    records.push(record);
                    if complete {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.outcome)
                .collect::<Vec<_>>(),
            [Outcome::Admitted, Outcome::Completed]
        );
        assert!(records.iter().all(|record| record.connection == CONNECTION
            && record.sequence == Some(0)
            && record.direction == Direction::Export));
        source.revoke().await.unwrap();
        assert!(timeout(Duration::from_secs(5), export)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert_eq!(history.pop_record().unwrap().outcome, Outcome::Cancelled);
        assert!(history.pop_record().is_none());
        destination.revoke().await.unwrap();
        assert!(timeout(Duration::from_secs(5), import)
            .await
            .unwrap()
            .unwrap()
            .is_err());
    }

    #[tokio::test]
    async fn imported_frame_reports_admission_completion_and_drop_cancellation() {
        let (mut peer, data) = UnixStream::pair().unwrap();
        let (mut credit_peer, control) = UnixStream::pair().unwrap();
        let (link, history) =
            UnixLink::import_tracked::<Number>(scope(), data, control, 4, CONNECTION).unwrap();
        let task = tokio::spawn(link);
        let mut frame = [0; 42];
        encode_frame::<Number>(81, &23, &mut frame).unwrap();
        peer.write_all(&frame).await.unwrap();
        let mut credit = [0; 9];
        timeout(Duration::from_secs(5), credit_peer.read_exact(&mut credit))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&credit[1..], &81u64.to_le_bytes());
        let admission = history.pop_record().unwrap();
        assert_eq!(admission.connection, CONNECTION);
        assert_eq!(admission.contract, Contract::of::<Number>());
        assert_eq!(admission.sequence, Some(81));
        assert_eq!(admission.outcome, Outcome::Admitted);
        assert_eq!(history.pop_record().unwrap().outcome, Outcome::Completed);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let cancellation = history.pop_record().unwrap();
        assert_eq!(cancellation.sequence, Some(81));
        assert_eq!(cancellation.outcome, Outcome::Cancelled);
        assert!(history.pop_record().is_none());
        assert_eq!(history.lost_records(), 0);
    }

    #[tokio::test]
    async fn codec_rejection_and_unpolled_cancellation_survive_link_destruction() {
        let (mut peer, data) = UnixStream::pair().unwrap();
        let (_credit_peer, control) = UnixStream::pair().unwrap();
        let (link, history) =
            UnixLink::import_tracked::<Number>(scope(), data, control, 4, CONNECTION).unwrap();
        let task = tokio::spawn(link);
        let mut frame = [0; 42];
        encode_frame::<Number>(13, &23, &mut frame).unwrap();
        frame[34..38].copy_from_slice(&3u32.to_le_bytes());
        peer.write_all(&frame[..41]).await.unwrap();
        assert!(timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        let rejection = history.pop_record().unwrap();
        assert_eq!(rejection.connection, CONNECTION);
        assert_eq!(rejection.sequence, Some(13));
        assert_eq!(
            rejection.outcome,
            Outcome::Rejected(Error::Wire(WireError::InvalidPayload))
        );
        assert!(history.pop_record().is_none());

        let (_peer, data) = UnixStream::pair().unwrap();
        let (_credit_peer, control) = UnixStream::pair().unwrap();
        let (link, history) = UnixLink::import_tracked::<Number>(
            scope(),
            data,
            control,
            4,
            Connection {
                generation: 10,
                ..CONNECTION
            },
        )
        .unwrap();
        drop(link);
        let cancellation = history.pop_record().unwrap();
        assert_eq!(cancellation.connection.generation, 10);
        assert_eq!(cancellation.sequence, None);
        assert_eq!(cancellation.outcome, Outcome::Cancelled);
        assert!(history.pop_record().is_none());
    }
}
