#![cfg(target_os = "linux")]

use hiway::transport::{Contract, Direction, Error, Reservation};
use hiway::{
    EventId, EventSpec, SchemaRevision, StaticStream, StreamItem, SubscriptionRole, TopicError,
    WireCodec, WireError,
};
use hiway_uring::{Budget, Driver};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    io::{Read, Write},
    os::unix::net::UnixStream,
    pin::pin,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

struct Number;
impl EventSpec for Number {
    type Payload = u64;
    const ID: EventId = EventId::from_name("uring.shutdown.number");
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

#[derive(Default)]
struct Signal(AtomicUsize);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn cutoff_rejects_new_links_and_preserves_existing_traffic() {
    let mut driver = Driver::<(), 2, 64>::new(4).unwrap();
    assert!(!driver.shutdown_complete());
    let source = StaticStream::<Number, 1>::new();
    let destination = StaticStream::<Number, 1>::new();
    let received = destination.subscribe(SubscriptionRole::Required).unwrap();
    let (outgoing, incoming) = UnixStream::pair().unwrap();
    let (credit_out, credit_in) = UnixStream::pair().unwrap();
    let mut export = pin!(driver
        .export(
            source.subscribe(SubscriptionRole::Required).unwrap(),
            outgoing,
            credit_out,
            ()
        )
        .unwrap());
    let mut import = pin!(driver
        .import(destination.sender(), incoming, credit_in, ())
        .unwrap());
    driver.stop_admission();
    driver.stop_admission();
    for direction in [Direction::Import, Direction::Export] {
        let (_data_peer, data) = UnixStream::pair().unwrap();
        let (_control_peer, control) = UnixStream::pair().unwrap();
        let error = match direction {
            Direction::Import => driver.import(destination.sender(), data, control, ()).err(),
            Direction::Export => driver
                .export(
                    source.subscribe(SubscriptionRole::Observer).unwrap(),
                    data,
                    control,
                    (),
                )
                .err(),
        };
        assert_eq!(error, Some(Error::Closed));
    }
    source.sender().send_now(73).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        assert!(export.as_mut().poll(&mut cx).is_pending());
        assert!(import.as_mut().poll(&mut cx).is_pending());
        driver.advance_io().unwrap();
        driver.dispatch();
        if let Some(StreamItem::Data { value, .. }) = received.recv_now().unwrap() {
            assert_eq!(*value, 73);
            break;
        }
        assert!(Instant::now() < deadline, "cutoff stopped existing traffic");
        std::thread::yield_now();
    }
    assert!(!driver.shutdown_complete());
    driver.cancel_all();
    while !driver.shutdown_complete() {
        driver.advance_io().unwrap();
        driver.dispatch_with_budget(1);
        assert!(Instant::now() < deadline, "shutdown did not finish");
        std::thread::yield_now();
    }
    drop(driver);
    assert_eq!(
        export.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::Closed))
    );
    assert_eq!(
        import.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::Closed))
    );
}

struct Cutoff {
    callback: RefCell<Option<Box<dyn FnOnce()>>>,
    during_check: bool,
}
impl Cutoff {
    fn run(&self) {
        if let Some(callback) = self.callback.borrow_mut().take() {
            callback();
        }
    }
}
impl Reservation for Cutoff {
    type Access<'a> = ();
    fn check(&self, _: Contract, _: Direction, _: usize) -> Result<(), TopicError> {
        if self.during_check {
            self.run();
        }
        Ok(())
    }
    fn enter(&self) -> Result<(), TopicError> {
        if !self.during_check {
            self.run();
        }
        Ok(())
    }
    fn is_revoked(&self) -> bool {
        false
    }
}

#[test]
fn reservation_callbacks_cannot_attach_after_cutoff() {
    for during_check in [false, true] {
        let driver = Rc::new(Driver::<Cutoff, 1, 64>::new(2).unwrap());
        let cutoff = driver.clone();
        let reservation = Cutoff {
            callback: RefCell::new(Some(Box::new(move || cutoff.stop_admission()))),
            during_check,
        };
        let destination = StaticStream::<Number, 1>::new();
        let (_data_peer, data) = UnixStream::pair().unwrap();
        let (_control_peer, control) = UnixStream::pair().unwrap();
        assert_eq!(
            driver
                .import(destination.sender(), data, control, reservation)
                .err(),
            Some(Error::Closed)
        );
        assert_eq!(driver.active_links(), 0);
        assert!(driver.shutdown_complete());
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
    fn check(&self, _: Contract, _: Direction, _: usize) -> Result<(), TopicError> {
        Ok(())
    }
    fn enter(&self) -> Result<(), TopicError> {
        Ok(())
    }
    fn is_revoked(&self) -> bool {
        false
    }
}

#[test]
fn cancellation_defers_wakes_and_reclaims_one_slot_per_dispatch() {
    let mut driver = Driver::<Charge, 3, 64>::new(2).unwrap();
    let streams: [StaticStream<Number, 1>; 3] = std::array::from_fn(|_| StaticStream::new());
    let charges = Rc::new(Cell::new(3));
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut imports = Vec::new();
    let mut peers = Vec::new();
    for stream in &streams {
        let (peer, data) = UnixStream::pair().unwrap();
        let (credit, control) = UnixStream::pair().unwrap();
        let mut import = Box::pin(
            driver
                .import(stream.sender(), data, control, Charge(charges.clone()))
                .unwrap(),
        );
        assert!(import.as_mut().poll(&mut cx).is_pending());
        imports.push(import);
        peers.push((peer, credit));
    }
    driver.cancel_all();
    driver.cancel_all();
    assert_eq!(charges.get(), 3);
    assert_eq!(signal.0.load(Ordering::Relaxed), 0);
    assert_eq!(driver.dispatch_with_budget(0), 0);
    assert!(!driver.shutdown_complete());
    for remaining in (0..3).rev() {
        assert_eq!(driver.dispatch_with_budget(1), 1);
        assert_eq!(charges.get(), remaining);
        assert_eq!(signal.0.load(Ordering::Relaxed), 3 - remaining);
        assert_eq!(driver.shutdown_complete(), remaining == 0);
    }
    driver.cancel_all();
    assert!(driver.shutdown_complete());
    assert_eq!(driver.dispatch_with_budget(usize::MAX), 0);
    assert_eq!(driver.wait().unwrap(), 0);
    drop(driver);
    for import in &mut imports {
        assert_eq!(
            import.as_mut().poll(&mut cx),
            Poll::Ready(Err(Error::Closed))
        );
    }
    drop(peers);
}

#[test]
fn stalled_partial_receive_retires_before_socket_reclamation() {
    let mut driver = Driver::<(), 1, 64>::new(2).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let (mut peer, data) = UnixStream::pair().unwrap();
    peer.set_nonblocking(true).unwrap();
    let (_credit_peer, control) = UnixStream::pair().unwrap();
    let mut import = pin!(driver
        .import(destination.sender(), data, control, ())
        .unwrap());
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    assert!(import
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    peer.write_all(b"HWY1").unwrap();
    driver.advance_io().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let progress = driver.advance_io().unwrap();
        if progress.completions != 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "partial receive did not complete"
        );
        std::thread::yield_now();
    }
    driver.cancel_all();
    let budget = Budget {
        completions: 1,
        submissions: 1,
        bytes: 1,
    };
    loop {
        let progress = driver.advance_io_with_budget(budget).unwrap();
        assert!(progress.completions <= 1);
        assert!(progress.submissions <= 1);
        assert_eq!(progress.bytes, 0);
        assert!(!driver.shutdown_complete());
        assert_eq!(signal.0.load(Ordering::Relaxed), 0);
        assert_eq!(
            peer.read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        if progress.work[0].reclaim {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cancellation stranded a partial frame"
        );
        std::thread::yield_now();
    }
    assert_eq!(driver.dispatch_with_budget(1), 1);
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    assert_eq!(signal.0.load(Ordering::Relaxed), 1);
    assert!(driver.shutdown_complete());
    drop(driver);
    assert_eq!(
        import.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Err(Error::Closed))
    );
}
