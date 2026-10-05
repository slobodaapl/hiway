use super::Driver;
use hiway::{
    EventId, EventSpec, SchemaRevision, StaticStream, StreamItem, SubscriptionRole, WireCodec,
    WireError,
};
use std::{
    future::Future,
    io::Write,
    os::unix::net::UnixStream,
    task::{Context, Waker},
    time::{Duration, Instant},
};

struct Number;
impl EventSpec for Number {
    type Payload = u64;
    const ID: EventId = EventId::from_name("uring.receive-hint.number");
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

#[test]
fn unsupported_receive_hint_recovers_all_accepted_receives() {
    let mut driver = Driver::<(), 2, 64>::new(8).unwrap();
    // An unknown receive bit makes this kernel reject the optional hint.
    driver.receive_ioprio = 0x8001;
    let streams: [StaticStream<Number, 1>; 2] = std::array::from_fn(|_| StaticStream::new());
    let receivers = streams
        .each_ref()
        .map(|stream| stream.subscribe(SubscriptionRole::Required).unwrap());
    let mut peers = Vec::new();
    let mut imports = Vec::new();
    let mut cx = Context::from_waker(Waker::noop());
    for stream in &streams {
        let (peer, data) = UnixStream::pair().unwrap();
        let (credit, control) = UnixStream::pair().unwrap();
        let mut import = Box::pin(driver.import(stream.sender(), data, control, ()).unwrap());
        assert!(import.as_mut().poll(&mut cx).is_pending());
        peers.push((peer, credit));
        imports.push(import);
    }
    // Submit every initial receive before consuming its rejection CQE.
    driver.advance_io().unwrap();
    for (index, (peer, _)) in peers.iter_mut().enumerate() {
        let mut frame = [0; 46];
        hiway::transport::encode_frame::<Number>(0, &(index as u64 + 10), &mut frame).unwrap();
        peer.write_all(&frame).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut delivered = [false; 2];
    while delivered != [true; 2] {
        driver.advance_io().unwrap();
        driver.dispatch();
        for (index, import) in imports.iter_mut().enumerate() {
            assert!(import.as_mut().poll(&mut cx).is_pending());
            if let Some(StreamItem::Data { value, .. }) = receivers[index].recv_now().unwrap() {
                assert_eq!(*value, index as u64 + 10);
                delivered[index] = true;
            }
        }
        assert!(
            Instant::now() < deadline,
            "rejected hint did not recover: {delivered:?}"
        );
        if delivered != [true; 2] {
            driver.wait().unwrap();
        }
    }
}
