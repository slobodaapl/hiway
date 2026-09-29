#![cfg(target_os = "linux")]

use hiway::transport::{Contract, Direction, Reservation};
use hiway::{
    EventId, EventSpec, SchemaRevision, StaticStream, StreamItem, SubscriptionRole, WireCodec,
    WireError,
};
use hiway_uring::Driver;
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::{
    alloc::System,
    cell::Cell,
    future::{poll_fn, Future},
    io::{Read, Write},
    os::unix::net::UnixStream,
    pin::pin,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

struct Number;
impl EventSpec for Number {
    type Payload = u64;
    const ID: EventId = EventId::from_name("uring.test.number");
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

struct Charge(Rc<Cell<usize>>);
impl Drop for Charge {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}
impl Reservation for Charge {
    type Access<'a> = ();
    fn check(&self, _: Contract, _: Direction, _: usize) -> Result<(), hiway::TopicError> {
        Ok(())
    }
    fn enter(&self) -> Result<(), hiway::TopicError> {
        Ok(())
    }
    fn is_revoked(&self) -> bool {
        false
    }
}

#[test]
fn sockets_admit_and_cancel_without_transport_allocations() {
    let mut driver =
        Driver::<Charge, 2, 64>::new(2).expect("io_uring must be available for this test");
    assert_eq!(driver.wait().unwrap(), 0);
    let source = StaticStream::<Number, 2>::new();
    let destination = StaticStream::<Number, 1>::new();
    let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
    let source_receiver = source.subscribe(SubscriptionRole::Required).unwrap();
    let (data_out, data_in) = UnixStream::pair().unwrap();
    let (credit_in, credit_out) = UnixStream::pair().unwrap();
    let charges = Rc::new(Cell::new(2));
    let export_guard = Charge(charges.clone());
    let import_guard = Charge(charges.clone());
    let deadline = Instant::now() + Duration::from_secs(5);
    let region = Region::new(ALLOCATOR);
    {
        let export = driver
            .export(source_receiver, data_out, credit_out, export_guard)
            .unwrap();
        let import = driver
            .import(destination.sender(), data_in, credit_in, import_guard)
            .unwrap();
        let mut export = pin!(export);
        let mut import = pin!(import);
        let mut cx = Context::from_waker(Waker::noop());
        source.sender().send_now(73).unwrap();
        loop {
            assert!(export.as_mut().poll(&mut cx).is_pending());
            assert!(import.as_mut().poll(&mut cx).is_pending());
            driver.advance().unwrap();
            if let Some(StreamItem::Data { value, .. }) = receiver.recv_now().unwrap() {
                assert_eq!(*value, 73);
                break;
            }
            assert!(Instant::now() < deadline, "no delivery");
            std::thread::yield_now();
        }
        assert_eq!(charges.get(), 2);
    }
    // Link drop requests closure; outstanding kernel ownership retains charges.
    assert_eq!(charges.get(), 2);
    while driver.active_links() != 0 {
        driver.advance().unwrap();
        assert!(
            Instant::now() < deadline,
            "cancellation did not reclaim links"
        );
        std::thread::yield_now();
    }
    assert_eq!(charges.get(), 0);
    assert_eq!(driver.wait().unwrap(), 0);
    let allocations = region.change();
    assert_eq!(allocations.allocations, 0);
    assert_eq!(allocations.reallocations, 0);
    admission_credit_waits_for_destination();
    saturated_idle_receives_do_not_strand_submission();
    dropping_driver_finishes_kernel_ownership();
    revocation_retains_outstanding_charges();
    forgetting_a_future_does_not_abandon_kernel_storage();
    failed_links_reclaim_and_reuse_the_slot();
    callback_panic_retains_kernel_storage();
}

fn saturated_idle_receives_do_not_strand_submission() {
    let mut driver = Driver::<(), 8, 64>::new(2).unwrap();
    use std::os::fd::{AsFd, FromRawFd};
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    assert!(fd >= 0);
    let mut completions = unsafe { std::fs::File::from_raw_fd(fd) };
    driver.register_eventfd(completions.as_fd()).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
    let signal = Arc::new(Signal(AtomicBool::new(false)));
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut peers = Vec::new();
    let mut imports = Vec::new();
    for _ in 0..8 {
        let (peer, data) = UnixStream::pair().unwrap();
        let (control_peer, control) = UnixStream::pair().unwrap();
        peers.push((peer, control_peer));
        imports.push(Box::pin(
            driver
                .import(destination.sender(), data, control, ())
                .unwrap(),
        ));
    }
    for import in &mut imports {
        assert!(import.as_mut().poll(&mut cx).is_pending());
    }
    let mut frame = [0; 46];
    hiway::transport::encode_frame::<Number>(0, &73, &mut frame).unwrap();
    peers.last_mut().unwrap().0.write_all(&frame).unwrap();
    driver.advance().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let completed = match completions.read(&mut [0; 8]) {
            Ok(8) => true,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
            other => panic!("completion notification: {other:?}"),
        };
        if signal.0.swap(false, Ordering::SeqCst) || completed {
            for import in &mut imports {
                assert!(import.as_mut().poll(&mut cx).is_pending());
            }
            driver.advance().unwrap();
        }
        if let Some(StreamItem::Data { value, .. }) = receiver.recv_now().unwrap() {
            assert_eq!(*value, 73);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "SQ saturation stranded the active link without a wake"
        );
        std::thread::yield_now();
    }
}

fn failed_links_reclaim_and_reuse_the_slot() {
    let mut driver = Driver::<Charge, 1, 64>::new(2).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
    let charges = Rc::new(Cell::new(0));
    for length in [0, 12, 40, 46] {
        let (mut peer, data) = UnixStream::pair().unwrap();
        let (_peer_control, control) = UnixStream::pair().unwrap();
        let mut frame = [0; 46];
        hiway::transport::encode_frame::<Number>(0, &73, &mut frame).unwrap();
        if length == 46 {
            frame[0] = 0;
        }
        peer.write_all(&frame[..length]).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();
        charges.set(1);
        {
            let mut import = pin!(driver
                .import(destination.sender(), data, control, Charge(charges.clone()))
                .unwrap());
            let mut cx = Context::from_waker(Waker::noop());
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                driver.advance().unwrap();
                if let Poll::Ready(result) = import.as_mut().poll(&mut cx) {
                    assert_eq!(
                        result,
                        Err(if length == 46 {
                            hiway::transport::Error::Protocol
                        } else {
                            hiway::transport::Error::Closed
                        })
                    );
                    break;
                }
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while driver.active_links() != 0 {
            driver.advance().unwrap();
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(charges.get(), 0);
        assert_eq!(driver.wait().unwrap(), 0);
        assert!(receiver.recv_now().unwrap().is_none());
    }
    let source = StaticStream::<Number, 1>::new();
    let (peer, data) = UnixStream::pair().unwrap();
    let (_peer_control, control) = UnixStream::pair().unwrap();
    peer.shutdown(std::net::Shutdown::Read).unwrap();
    charges.set(1);
    {
        let mut export = pin!(driver
            .export(
                source.subscribe(SubscriptionRole::Required).unwrap(),
                data,
                control,
                Charge(charges.clone())
            )
            .unwrap());
        source.sender().send_now(81).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            driver.advance().unwrap();
            if let Poll::Ready(result) = export.as_mut().poll(&mut cx) {
                assert_eq!(result, Err(hiway::transport::Error::Io(32)));
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
    }
    drop(driver);
    assert_eq!(charges.get(), 0);
}

fn callback_panic_retains_kernel_storage() {
    struct PanicWake;
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("host wake failed");
        }
    }
    let mut driver = Driver::<Charge, 1, 64>::new(4).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let (_peer, data) = UnixStream::pair().unwrap();
    let (_peer_control, control) = UnixStream::pair().unwrap();
    let charges = Rc::new(Cell::new(1));
    let mut import = pin!(driver
        .import(destination.sender(), data, control, Charge(charges.clone()))
        .unwrap());
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(import
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    driver.advance().unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(driver))).is_err());
    assert_eq!(charges.get(), 1);
}

fn admission_credit_waits_for_destination() {
    let mut driver = Driver::<(), 1, 64>::new(4).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
    destination.sender().send_now(99).unwrap();
    let (mut peer, data) = UnixStream::pair().unwrap();
    let (mut credit, control) = UnixStream::pair().unwrap();
    let attempts = Cell::new(0);
    let mut import = pin!(driver
        .import(
            ObservedSender {
                sender: destination.sender(),
                attempts: &attempts
            },
            data,
            control,
            ()
        )
        .unwrap());
    let signal = Arc::new(Signal(AtomicBool::new(false)));
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(import.as_mut().poll(&mut cx).is_pending());
    driver.advance().unwrap();
    let mut frame = [0; 46];
    hiway::transport::encode_frame::<Number>(0, &73, &mut frame).unwrap();
    peer.write_all(&frame).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while attempts.get() == 0 {
        driver.advance().unwrap();
        if signal.0.swap(false, Ordering::SeqCst) {
            assert!(import.as_mut().poll(&mut cx).is_pending());
        }
        assert!(
            Instant::now() < deadline,
            "receive completion did not wake admission"
        );
        std::thread::yield_now();
    }
    driver.advance().unwrap();
    credit
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let mut ack = [0; 9];
    let error = credit
        .read(&mut ack)
        .expect_err("credit preceded local admission");
    assert!(matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));
    signal.0.store(false, Ordering::SeqCst);
    assert!(
        matches!(receiver.recv_now().unwrap(), Some(StreamItem::Data { value, .. }) if *value == 99)
    );
    destination.maintain();
    assert!(
        signal.0.swap(false, Ordering::SeqCst),
        "freed local capacity did not wake admission"
    );
    assert!(import.as_mut().poll(&mut cx).is_pending());
    assert!(
        matches!(receiver.recv_now().unwrap(), Some(StreamItem::Data { value, .. }) if *value == 73)
    );
    credit.set_nonblocking(true).unwrap();
    let mut received = 0;
    while received != ack.len() {
        driver.advance().unwrap();
        if signal.0.swap(false, Ordering::SeqCst) {
            assert!(import.as_mut().poll(&mut cx).is_pending());
        }
        match credit.read(&mut ack[received..]) {
            Ok(0) => panic!("control socket closed"),
            Ok(count) => received += count,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("{error}"),
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(ack, [1, 0, 0, 0, 0, 0, 0, 0, 0]);
}

struct Signal(AtomicBool);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct ObservedSender<'a> {
    sender: hiway::StaticSender<'a, Number, 1>,
    attempts: &'a Cell<usize>,
}
impl hiway::EventSender<Number> for ObservedSender<'_> {
    type Prepared<'a>
        = <hiway::StaticSender<'a, Number, 1> as hiway::EventSender<Number>>::Prepared<'a>
    where
        Self: 'a;
    fn prepare(&self, value: u64) -> Result<Self::Prepared<'_>, hiway::TrySendError<u64>> {
        self.sender.prepare(value)
    }
    fn send_now(&self, value: u64) -> Result<(), hiway::TrySendError<u64>> {
        self.sender.send_now(value)
    }
    async fn send(&self, value: u64) -> Result<(), hiway::SendError<u64>> {
        let mut send = pin!(self.sender.send(value));
        poll_fn(|cx| {
            self.attempts.set(self.attempts.get() + 1);
            send.as_mut().poll(cx)
        })
        .await
    }
}

fn dropping_driver_finishes_kernel_ownership() {
    let mut driver = Driver::<Charge, 1, 64>::new(2).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let (peer_data, data) = UnixStream::pair().unwrap();
    let (peer_control, control) = UnixStream::pair().unwrap();
    let charges = Rc::new(Cell::new(1));
    let future = driver
        .import(destination.sender(), data, control, Charge(charges.clone()))
        .unwrap();
    let mut future = pin!(future);
    driver.advance().unwrap();
    assert_eq!(charges.get(), 1);
    drop(driver);
    assert_eq!(charges.get(), 0);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut cx),
        std::task::Poll::Ready(Err(hiway::transport::Error::Closed))
    ));
    drop((peer_data, peer_control));
}

struct Revocable {
    _charge: Charge,
    revoked: Rc<Cell<bool>>,
}
impl Reservation for Revocable {
    type Access<'a> = ();
    fn check(&self, _: Contract, _: Direction, _: usize) -> Result<(), hiway::TopicError> {
        Ok(())
    }
    fn enter(&self) -> Result<(), hiway::TopicError> {
        if self.revoked.get() {
            Err(hiway::TopicError::Revoked)
        } else {
            Ok(())
        }
    }
    fn is_revoked(&self) -> bool {
        self.revoked.get()
    }
}

fn revocation_retains_outstanding_charges() {
    let mut driver = Driver::<Revocable, 1, 64>::new(2).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let (_peer_data, data) = UnixStream::pair().unwrap();
    let (_peer_control, control) = UnixStream::pair().unwrap();
    let charges = Rc::new(Cell::new(1));
    let revoked = Rc::new(Cell::new(false));
    let guard = Revocable {
        _charge: Charge(charges.clone()),
        revoked: revoked.clone(),
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    let region = Region::new(ALLOCATOR);
    {
        let future = driver
            .import(destination.sender(), data, control, guard)
            .unwrap();
        let mut future = pin!(future);
        driver.advance().unwrap();
        driver.advance().unwrap();
        revoked.set(true);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            future.as_mut().poll(&mut cx),
            std::task::Poll::Ready(Err(hiway::transport::Error::Revoked))
        ));
        assert_eq!(charges.get(), 1);
    }
    while driver.active_links() != 0 {
        driver.advance().unwrap();
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(charges.get(), 0);
    let allocations = region.change();
    assert_eq!(allocations.allocations, 0);
    assert_eq!(allocations.reallocations, 0);
}

fn forgetting_a_future_does_not_abandon_kernel_storage() {
    let mut driver = Driver::<Charge, 1, 64>::new(2).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let (_peer_data, data) = UnixStream::pair().unwrap();
    let (_peer_control, control) = UnixStream::pair().unwrap();
    let charges = Rc::new(Cell::new(1));
    let future = driver
        .import(destination.sender(), data, control, Charge(charges.clone()))
        .unwrap();
    driver.advance().unwrap();
    std::mem::forget(future);
    assert_eq!(charges.get(), 1);
    drop(driver);
    assert_eq!(charges.get(), 0);
}
