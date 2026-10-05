#[cfg(target_os = "linux")]
fn run<const BYTES: usize>(entries: u32) {
    const COUNT: u32 = 10_000;
    use hiway::{
        EventId, EventSpec, SchemaRevision, StaticStream, StreamItem, SubscriptionRole, WireCodec,
        WireError,
    };
    use hiway_uring::Driver;
    use std::{
        future::Future,
        os::unix::net::UnixStream,
        pin::pin,
        task::{Context, Waker},
        time::Instant,
    };

    struct Number;
    impl EventSpec for Number {
        type Payload = u64;
        const ID: EventId = EventId::from_name("uring.benchmark.number");
    }
    impl WireCodec for Number {
        fn encoded_len(_: &u64) -> usize {
            8
        }
        fn encode(value: &u64, output: &mut [u8]) -> Result<usize, WireError> {
            output.copy_from_slice(&value.to_le_bytes());
            Ok(8)
        }
        fn decode(bytes: &[u8], _: SchemaRevision) -> Result<u64, WireError> {
            Ok(u64::from_le_bytes(
                bytes.try_into().map_err(|_| WireError::InvalidPayload)?,
            ))
        }
    }

    let source_a = StaticStream::<Number, 1>::new();
    let source_b = StaticStream::<Number, 1>::new();
    let destination_a = StaticStream::<Number, 1>::new();
    let destination_b = StaticStream::<Number, 1>::new();
    let received_a = destination_a.subscribe(SubscriptionRole::Required).unwrap();
    let received_b = destination_b.subscribe(SubscriptionRole::Required).unwrap();
    let mut driver = Driver::<(), 4, BYTES>::new(entries).unwrap();
    let (out_a, in_a) = UnixStream::pair().unwrap();
    let (ack_in_a, ack_out_a) = UnixStream::pair().unwrap();
    let (out_b, in_b) = UnixStream::pair().unwrap();
    let (ack_in_b, ack_out_b) = UnixStream::pair().unwrap();
    let mut export_a = pin!(driver
        .export(
            source_a.subscribe(SubscriptionRole::Required).unwrap(),
            out_a,
            ack_out_a,
            ()
        )
        .unwrap());
    let mut import_a = pin!(driver
        .import(destination_a.sender(), in_a, ack_in_a, ())
        .unwrap());
    let mut export_b = pin!(driver
        .export(
            source_b.subscribe(SubscriptionRole::Required).unwrap(),
            out_b,
            ack_out_b,
            ()
        )
        .unwrap());
    let mut import_b = pin!(driver
        .import(destination_b.sender(), in_b, ack_in_b, ())
        .unwrap());
    let mut cx = Context::from_waker(Waker::noop());
    let mut delivered = [0_u64; 2];
    let mut sent = [0_u64; 2];
    let start = Instant::now();
    while delivered != [u64::from(COUNT); 2] {
        for (index, source) in [&source_a, &source_b].into_iter().enumerate() {
            if sent[index] < u64::from(COUNT) && source.sender().send_now(sent[index]).is_ok() {
                sent[index] += 1;
            }
        }
        assert!(export_a.as_mut().poll(&mut cx).is_pending());
        assert!(import_a.as_mut().poll(&mut cx).is_pending());
        assert!(export_b.as_mut().poll(&mut cx).is_pending());
        assert!(import_b.as_mut().poll(&mut cx).is_pending());
        driver.advance().unwrap();
        for (index, receiver) in [&received_a, &received_b].into_iter().enumerate() {
            if let Some(StreamItem::Data { value, .. }) = receiver.recv_now().unwrap() {
                assert_eq!(*value, delivered[index]);
                delivered[index] += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    println!("entries={entries} frame_bytes={BYTES} links=4 frames={} elapsed_us={} frames_per_second={:.0}",
        COUNT * 2, elapsed.as_micros(), f64::from(COUNT * 2) / elapsed.as_secs_f64());
}

fn main() {
    #[cfg(target_os = "linux")]
    for entries in [2, 8, 32] {
        run::<64>(entries);
        run::<4096>(entries);
    }
}
