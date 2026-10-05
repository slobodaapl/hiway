#![cfg(all(target_os = "linux", feature = "std"))]

use hiway::transport::{Direction, GrantReservation, Reservation, StrictReservation};
use hiway::{
    DynamicFabric, EventId, EventSpec, Grant, Limits, Permission, Rights, SchemaRevision,
    StreamConfig, StreamItem, StreamLimits, SubscriptionRole, WireCodec, WireError,
};
use hiway_uring::{Driver, Progress};
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::{
    alloc::System,
    future::Future,
    io::{Read, Write},
    os::unix::net::UnixStream,
    pin::pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
static IN_STRICT: AtomicBool = AtomicBool::new(false);
static FORBIDDEN: AtomicUsize = AtomicUsize::new(0);
static WAKER_OPS: AtomicUsize = AtomicUsize::new(0);
static DECODES: AtomicUsize = AtomicUsize::new(0);

fn callback() {
    if IN_STRICT.load(Ordering::Relaxed) {
        FORBIDDEN.fetch_add(1, Ordering::Relaxed);
    }
}

fn waker_operation(_: *const ()) {
    callback();
    WAKER_OPS.fetch_add(1, Ordering::Relaxed);
}

fn clone_waker(data: *const ()) -> RawWaker {
    waker_operation(data);
    raw_waker()
}

fn raw_waker() -> RawWaker {
    static VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_waker,
        waker_operation,
        waker_operation,
        waker_operation,
    );
    RawWaker::new(std::ptr::null(), &VTABLE)
}

fn waker() -> Waker {
    // SAFETY: the vtable owns no pointer or resource; every operation accesses
    // only static atomics, including clones and drops on other threads.
    unsafe { Waker::from_raw(raw_waker()) }
}

#[derive(Debug)]
struct Payload(u32);

impl Drop for Payload {
    fn drop(&mut self) {
        callback();
    }
}

struct Number;
impl EventSpec for Number {
    type Payload = Payload;
    const ID: EventId = EventId::from_name("strict.number");
}
impl WireCodec for Number {
    fn encoded_len(_: &Payload) -> usize {
        callback();
        4
    }
    fn encode(value: &Payload, output: &mut [u8]) -> Result<usize, WireError> {
        callback();
        output.copy_from_slice(&value.0.to_le_bytes());
        Ok(4)
    }
    fn decode(bytes: &[u8], _: SchemaRevision) -> Result<Payload, WireError> {
        callback();
        DECODES.fetch_add(1, Ordering::Relaxed);
        Ok(Payload(u32::from_le_bytes(
            bytes.try_into().map_err(|_| WireError::InvalidPayload)?,
        )))
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
                bytes: 4096,
                ..Limits::ZERO
            },
        )
        .unwrap()
}

fn step(driver: &mut Driver<GrantReservation, 1, 64>) -> Progress<1> {
    let region = Region::new(ALLOCATOR);
    IN_STRICT.store(true, Ordering::Relaxed);
    let result = driver.advance_io();
    let pending = driver.pending_work();
    IN_STRICT.store(false, Ordering::Relaxed);
    let changes = region.change();
    assert_eq!(FORBIDDEN.load(Ordering::Relaxed), 0);
    assert_eq!(changes.allocations, 0);
    assert_eq!(changes.reallocations, 0);
    assert_eq!(changes.deallocations, 0);
    let progress = result.unwrap();
    assert_eq!(progress.work, pending);
    progress
}

// Keep allocation measurements isolated from other tests in this binary.
#[test]
fn strict_io_defers_callbacks_and_preserves_authority() {
    deferred_access_preserves_the_revocation_cutoff();
    receive_and_reclaim_require_explicit_dispatch();
    revoked_prepared_export_never_reaches_the_peer();
}

fn deferred_access_preserves_the_revocation_cutoff() {
    let grant = grant();
    let reservation = grant
        .reserve_transport::<Number>(Direction::Import, 64)
        .unwrap();
    let access = reservation.enter_deferred().unwrap();
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    let mut revoke = pin!(grant.revoke());
    assert!(revoke.as_mut().poll(&mut cx).is_pending());
    let before = WAKER_OPS.load(Ordering::Relaxed);
    IN_STRICT.store(true, Ordering::Relaxed);
    drop(access);
    IN_STRICT.store(false, Ordering::Relaxed);
    assert_eq!(WAKER_OPS.load(Ordering::Relaxed), before);
    assert_eq!(FORBIDDEN.load(Ordering::Relaxed), 0);
    reservation.maintain();
    assert!(WAKER_OPS.load(Ordering::Relaxed) > before);
    assert_eq!(revoke.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    assert_eq!(
        reservation.enter_deferred().err(),
        Some(hiway::TopicError::Revoked)
    );
}

fn receive_and_reclaim_require_explicit_dispatch() {
    let destination = grant();
    let receiver = destination
        .subscribe::<Number>(SubscriptionRole::Required)
        .unwrap();
    let grant = grant();
    let mut driver = Driver::<GrantReservation, 1, 64>::new(4).unwrap();
    let reservation = grant
        .reserve_transport::<Number>(
            Direction::Import,
            Driver::<GrantReservation, 1, 64>::reservation_bytes(),
        )
        .unwrap();
    let charged = grant.usage();
    let (mut peer, data) = UnixStream::pair().unwrap();
    let (mut credit, control) = UnixStream::pair().unwrap();
    credit.set_nonblocking(true).unwrap();
    let mut import = Box::pin(
        driver
            .import(
                destination.sender::<Number>().unwrap(),
                data,
                control,
                reservation,
            )
            .unwrap(),
    );
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    assert!(import.as_mut().poll(&mut cx).is_pending());
    let mut frame = [0; 42];
    hiway::transport::encode_frame::<Number>(0, &Payload(73), &mut frame).unwrap();
    peer.write_all(&frame).unwrap();
    let before = WAKER_OPS.load(Ordering::Relaxed);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !step(&mut driver).work[0].wake {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    for _ in 0..8 {
        assert!(step(&mut driver).work[0].wake);
    }
    assert_eq!(WAKER_OPS.load(Ordering::Relaxed), before);
    assert_eq!(DECODES.load(Ordering::Relaxed), 0);
    assert!(receiver.recv_now().unwrap().is_none());
    driver.dispatch();
    assert!(WAKER_OPS.load(Ordering::Relaxed) > before);
    assert!(driver.pending_work()[0].is_empty());
    assert!(import.as_mut().poll(&mut cx).is_pending());
    assert_eq!(DECODES.load(Ordering::Relaxed), 1);
    assert!(
        matches!(receiver.recv_now().unwrap(), Some(StreamItem::Data { value, .. }) if value.0 == 73)
    );
    let mut ack = [0; 9];
    let mut credit_bytes = 0;
    while credit_bytes < ack.len() {
        step(&mut driver);
        match credit.read(&mut ack[credit_bytes..]) {
            Ok(0) => panic!("credit socket closed before acknowledging admission"),
            Ok(count) => credit_bytes += count,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) => {}
            Err(error) => panic!("{error}"),
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(ack, [1, 0, 0, 0, 0, 0, 0, 0, 0]);
    drop(import);
    while !step(&mut driver).work[0].reclaim {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(grant.usage(), charged);
    assert_eq!(driver.active_links(), 1);
    driver.dispatch();
    assert_eq!(grant.usage(), Limits::ZERO);
    assert_eq!(driver.active_links(), 0);
    assert!(driver.pending_work()[0].is_empty());
}

fn revoked_prepared_export_never_reaches_the_peer() {
    let source = grant();
    let grant = grant();
    let mut driver = Driver::<GrantReservation, 1, 64>::new(4).unwrap();
    let reservation = grant
        .reserve_transport::<Number>(
            Direction::Export,
            Driver::<GrantReservation, 1, 64>::reservation_bytes(),
        )
        .unwrap();
    let (mut peer, data) = UnixStream::pair().unwrap();
    peer.set_nonblocking(true).unwrap();
    let (_credit, control) = UnixStream::pair().unwrap();
    let mut export = Box::pin(
        driver
            .export(
                source
                    .subscribe::<Number>(SubscriptionRole::Required)
                    .unwrap(),
                data,
                control,
                reservation,
            )
            .unwrap(),
    );
    source
        .sender::<Number>()
        .unwrap()
        .send_now(Payload(99))
        .unwrap();
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    assert!(export.as_mut().poll(&mut cx).is_pending());
    assert_eq!(
        pin!(grant.revoke()).as_mut().poll(&mut cx),
        Poll::Ready(Ok(()))
    );
    assert!(step(&mut driver).work[0].reclaim);
    let mut byte = [0];
    assert_eq!(
        peer.read(&mut byte).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        export.as_mut().poll(&mut cx),
        Poll::Ready(Err(hiway::transport::Error::Revoked))
    );
    drop(export);
    driver.dispatch();
    assert_eq!(peer.read(&mut byte).unwrap(), 0);
    assert_eq!(grant.usage(), Limits::ZERO);
}
