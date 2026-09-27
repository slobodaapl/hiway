#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use hiway::{
        events, EventSpec, SchemaRevision, StaticStream, StreamItem, SubscriptionRole, WireCodec,
        WireError,
    };
    use hiway_uring::Driver;
    use std::{
        future::Future,
        os::unix::net::UnixStream,
        pin::pin,
        task::{Context, Waker},
    };

    #[events]
    enum Events {
        Number(u64),
    }
    impl WireCodec for events::Number {
        fn encoded_len(_: &u64) -> usize {
            8
        }
        fn encode(value: &u64, output: &mut [u8]) -> Result<usize, WireError> {
            output.copy_from_slice(&value.to_le_bytes());
            Ok(8)
        }
        fn decode(bytes: &[u8], revision: SchemaRevision) -> Result<u64, WireError> {
            if revision != Self::SCHEMA_REVISION {
                return Err(WireError::UnsupportedRevision);
            }
            Ok(u64::from_le_bytes(
                bytes.try_into().map_err(|_| WireError::InvalidPayload)?,
            ))
        }
    }

    let source = StaticStream::<events::Number, 2>::new();
    let destination = StaticStream::<events::Number, 2>::new();
    let received = destination
        .subscribe(SubscriptionRole::Required)
        .map_err(hiway::transport::Error::Topic)?;
    let mut driver = Driver::<(), 2, 64>::new(8)?;
    let (data_out, data_in) = UnixStream::pair()?;
    let (credit_in, credit_out) = UnixStream::pair()?;
    let export = driver.export(
        source
            .subscribe(SubscriptionRole::Required)
            .map_err(hiway::transport::Error::Topic)?,
        data_out,
        credit_out,
        (),
    )?;
    let import = driver.import(destination.sender(), data_in, credit_in, ())?;
    let mut export = pin!(export);
    let mut import = pin!(import);
    let mut cx = Context::from_waker(Waker::noop());
    source.sender().send_now(42).expect("empty source stream");
    loop {
        if let std::task::Poll::Ready(result) = export.as_mut().poll(&mut cx) {
            result?;
        }
        if let std::task::Poll::Ready(result) = import.as_mut().poll(&mut cx) {
            result?;
        }
        if let Some(StreamItem::Data { value, .. }) = received
            .recv_now()
            .map_err(hiway::transport::Error::Receive)?
        {
            println!("received {}", *value);
            return Ok(());
        }
        // All application work for this example is ready before the host wait.
        // A larger host waits on both local wakes and ring completion readiness.
        if driver.advance()? == 0 {
            driver.wait()?;
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn main() {}
