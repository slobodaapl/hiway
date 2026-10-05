#![cfg(all(target_os = "linux", feature = "tokio-io"))]

use hiway::{
    DynamicFabric, EventId, EventReceiver, EventSpec, Grant, Limits, Permission, Rights,
    SchemaRevision, StaticStream, StreamConfig, StreamItem, StreamLimits, SubscriptionRole,
    UnixLink, WireCodec, WireError,
};
use hiway_uring::Driver;
use std::{
    cell::Cell,
    future::{poll_fn, Future},
    io::Write,
    os::unix::net::UnixStream,
    pin::pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

#[derive(Clone)]
struct RevokingSender<'a> {
    sender: hiway::StaticSender<'a, Number, 1>,
    grant: Grant,
    polled: &'a Cell<bool>,
    revoke_inside: bool,
}

impl hiway::EventSender<Number> for RevokingSender<'_> {
    type Prepared<'a>
        = <hiway::StaticSender<'a, Number, 1> as hiway::EventSender<Number>>::Prepared<'a>
    where
        Self: 'a;

    fn prepare(&self, value: u32) -> Result<Self::Prepared<'_>, hiway::TrySendError<u32>> {
        self.sender.prepare(value)
    }
    fn send_now(&self, value: u32) -> Result<(), hiway::TrySendError<u32>> {
        self.sender.send_now(value)
    }
    fn send(&self, value: u32) -> impl Future<Output = Result<(), hiway::SendError<u32>>> {
        if self.revoke_inside {
            assert!(
                pin!(self.grant.revoke())
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending(),
                "revocation completed during unguarded admission construction"
            );
        }
        async move {
            let mut send = pin!(self.sender.send(value));
            poll_fn(|cx| {
                self.polled.set(true);
                if self.revoke_inside {
                    assert!(
                        pin!(self.grant.revoke()).as_mut().poll(cx).is_pending(),
                        "revocation completed before the entered admission finished"
                    );
                }
                send.as_mut().poll(cx)
            })
            .await
        }
    }
}

#[test]
fn revocation_brackets_admission_but_not_parking() {
    for parked in [false, true] {
        let grant = grant();
        let mut driver = Driver::<hiway::transport::GrantReservation, 1, 64>::new(4).unwrap();
        let destination = StaticStream::<Number, 1>::new();
        let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
        if parked {
            destination.sender().send_now(99).unwrap();
        }
        let polled = Cell::new(false);
        let sender = RevokingSender {
            sender: destination.sender(),
            grant: grant.clone(),
            polled: &polled,
            revoke_inside: !parked,
        };
        let reservation = grant
            .reserve_transport::<Number>(
                hiway::transport::Direction::Import,
                Driver::<hiway::transport::GrantReservation, 1, 64>::reservation_bytes(),
            )
            .unwrap();
        let (mut peer, data) = UnixStream::pair().unwrap();
        let (_peer_control, control) = UnixStream::pair().unwrap();
        let mut frame = [0; 42];
        hiway::transport::encode_frame::<Number>(0, &73, &mut frame).unwrap();
        peer.write_all(&frame).unwrap();
        let mut import = pin!(driver.import(sender, data, control, reservation).unwrap());
        let mut cx = Context::from_waker(Waker::noop());
        let deadline = Instant::now() + Duration::from_secs(5);
        while !polled.get() {
            driver.advance().unwrap();
            let result = import.as_mut().poll(&mut cx);
            if !parked && polled.get() {
                assert_eq!(result, Poll::Ready(Err(hiway::transport::Error::Revoked)));
            } else {
                assert!(result.is_pending());
            }
            assert!(Instant::now() < deadline);
        }
        assert_eq!(
            pin!(grant.revoke()).as_mut().poll(&mut cx),
            Poll::Ready(Ok(()))
        );
        let expected = if parked { 99 } else { 73 };
        assert!(
            matches!(receiver.recv_now().unwrap(), Some(StreamItem::Data { value, .. }) if *value == expected)
        );
        if parked {
            assert_eq!(
                import.as_mut().poll(&mut cx),
                Poll::Ready(Err(hiway::transport::Error::Revoked))
            );
        }
        assert!(receiver.recv_now().unwrap().is_none());
    }
}

struct Number;

struct RevokingReceiver<'a> {
    receiver: hiway::StaticReceiver<'a, Number, 1>,
    grant: Grant,
}
impl hiway::Port for RevokingReceiver<'_> {}
impl<'a> EventReceiver<Number> for RevokingReceiver<'a> {
    type Value = <hiway::StaticReceiver<'a, Number, 1> as EventReceiver<Number>>::Value;
    fn event_try_recv(&self) -> Result<Option<StreamItem<Self::Value>>, hiway::ReceiveError> {
        self.receiver.event_try_recv()
    }
    fn event_recv_now(&self) -> Result<Option<StreamItem<Self::Value>>, hiway::ReceiveError> {
        self.receiver.event_recv_now()
    }
    fn event_recv(
        &self,
    ) -> impl Future<Output = Result<StreamItem<Self::Value>, hiway::ReceiveError>> {
        assert!(
            pin!(self.grant.revoke())
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "revocation completed during unguarded receive construction"
        );
        async move {
            let mut receive = pin!(self.receiver.event_recv());
            poll_fn(|cx| {
                assert!(
                    pin!(self.grant.revoke()).as_mut().poll(cx).is_pending(),
                    "revocation completed before the entered receive finished"
                );
                receive.as_mut().poll(cx)
            })
            .await
        }
    }
}

#[test]
fn revocation_brackets_export_consumption() {
    let grant = grant();
    let driver = Driver::<hiway::transport::GrantReservation, 1, 64>::new(4).unwrap();
    let source = StaticStream::<Number, 1>::new();
    let receiver = RevokingReceiver {
        receiver: source.subscribe(SubscriptionRole::Required).unwrap(),
        grant: grant.clone(),
    };
    let reservation = grant
        .reserve_transport::<Number>(
            hiway::transport::Direction::Export,
            Driver::<hiway::transport::GrantReservation, 1, 64>::reservation_bytes(),
        )
        .unwrap();
    let (_peer, data) = UnixStream::pair().unwrap();
    let (_peer_control, control) = UnixStream::pair().unwrap();
    source.sender().send_now(73).unwrap();
    let mut export = pin!(driver.export(receiver, data, control, reservation).unwrap());
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        export.as_mut().poll(&mut cx),
        Poll::Ready(Err(hiway::transport::Error::Revoked))
    );
    assert_eq!(
        pin!(grant.revoke()).as_mut().poll(&mut cx),
        Poll::Ready(Ok(()))
    );
}

impl EventSpec for Number {
    type Payload = u32;
    const ID: EventId = EventId::from_name("interop.number");
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

fn grant() -> Grant {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<Number>(StreamConfig::default())
        .unwrap();
    fabric
        .grant(
            &[
                Permission::new::<Number>(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED)
                    .with_limits(StreamLimits {
                        retained_items: 2,
                        subscriptions: 2,
                        waiters: 2,
                    }),
            ],
            Limits {
                streams: 1,
                subscriptions: 2,
                retained_items: 4,
                waiters: 4,
                connections: 2,
                bytes: Driver::<hiway::transport::GrantReservation, 1, 64>::reservation_bytes(),
                ..Limits::ZERO
            },
        )
        .unwrap()
}

fn sockets() -> (UnixStream, tokio::net::UnixStream) {
    let (ring, legacy) = UnixStream::pair().unwrap();
    legacy.set_nonblocking(true).unwrap();
    (ring, tokio::net::UnixStream::from_std(legacy).unwrap())
}

async fn transfer<F, R>(driver: &mut Driver<(), 1, 64>, ring: F, legacy: UnixLink, receiver: R)
where
    F: Future<Output = Result<(), hiway::transport::Error>>,
    R: EventReceiver<Number>,
{
    let mut ring = pin!(ring);
    let mut legacy = pin!(legacy);
    let deadline = Instant::now() + Duration::from_secs(5);
    poll_fn(|cx| {
        assert!(
            Instant::now() < deadline,
            "transport interoperability timed out"
        );
        assert!(legacy.as_mut().poll(cx).is_pending());
        assert!(ring.as_mut().poll(cx).is_pending());
        driver.advance().unwrap();
        if let Some(StreamItem::Data { value, .. }) = receiver.event_recv_now().unwrap() {
            assert_eq!(*value, 73);
            return Poll::Ready(());
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_export_to_uring_import() {
    let grant = grant();
    let destination = StaticStream::<Number, 1>::new();
    let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
    let (data, legacy_data) = sockets();
    let (control, legacy_control) = sockets();
    let legacy = UnixLink::export::<Number>(
        grant.clone(),
        legacy_data,
        legacy_control,
        SubscriptionRole::Required,
        4,
    )
    .unwrap();
    let mut driver = Driver::<(), 1, 64>::new(4).unwrap();
    let ring = driver
        .import(destination.sender(), data, control, ())
        .unwrap();
    grant.sender::<Number>().unwrap().send_now(73).unwrap();
    transfer(&mut driver, ring, legacy, receiver).await;
}

#[tokio::test(flavor = "current_thread")]
async fn uring_export_to_tokio_import() {
    let grant = grant();
    let source = StaticStream::<Number, 1>::new();
    let receiver = grant
        .subscribe::<Number>(SubscriptionRole::Required)
        .unwrap();
    let (data, legacy_data) = sockets();
    let (control, legacy_control) = sockets();
    let legacy = UnixLink::import::<Number>(grant.clone(), legacy_data, legacy_control, 4).unwrap();
    let mut driver = Driver::<(), 1, 64>::new(4).unwrap();
    let ring = driver
        .export(
            source.subscribe(SubscriptionRole::Required).unwrap(),
            data,
            control,
            (),
        )
        .unwrap();
    source.sender().send_now(73).unwrap();
    transfer(&mut driver, ring, legacy, receiver).await;
}
