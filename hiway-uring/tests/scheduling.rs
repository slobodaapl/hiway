#![cfg(target_os = "linux")]

use hiway::transport::{Contract, Direction, Reservation};
use hiway::{
    EventId, EventSpec, SchemaRevision, StaticStream, StreamItem, SubscriptionRole, TopicError,
    WireCodec, WireError,
};
use hiway_uring::{Budget, Driver, Schedule};
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::{
    alloc::System,
    cell::{Cell, RefCell},
    future::Future,
    io::{Read, Write},
    os::unix::net::UnixStream,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Wake, Waker},
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
const TINY: Budget = Budget {
    completions: 1,
    submissions: 1,
    bytes: 1,
};

struct Number;
impl EventSpec for Number {
    type Payload = u64;
    const ID: EventId = EventId::from_name("uring.scheduling.number");
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
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct Authority {
    id: usize,
    trace: Rc<RefCell<Vec<usize>>>,
    contended: Rc<Cell<bool>>,
    live: Rc<Cell<usize>>,
}
impl Reservation for Authority {
    type Access<'a> = ();
    fn check(&self, _: Contract, _: Direction, _: usize) -> Result<(), TopicError> {
        Ok(())
    }
    fn enter(&self) -> Result<(), TopicError> {
        self.trace.borrow_mut().push(self.id);
        if self.contended.get() {
            Err(TopicError::Contended)
        } else {
            Ok(())
        }
    }
    fn is_revoked(&self) -> bool {
        false
    }
}
impl Drop for Authority {
    fn drop(&mut self) {
        self.live.set(self.live.get() - 1);
    }
}

// Allocation observations are isolated from other tests in this binary.
#[test]
fn scheduling_bounds_fairness_and_parking() {
    tiny_budgets_deliver_beside_a_flood(false);
    tiny_budgets_deliver_beside_a_flood(true);
    submissions_rotate_and_cancellation_finishes(2, TINY);
    submissions_rotate_and_cancellation_finishes(
        8,
        Budget {
            submissions: 16,
            ..TINY
        },
    );
    submissions_rotate_and_cancellation_finishes(
        2,
        Budget {
            submissions: 16,
            bytes: 64,
            ..TINY
        },
    );
    contention_requests_retry_without_waking();
    zero_limits_reject_without_consuming_work();
}

// Keep the competing links and their borrowed endpoints in one test scope.
#[allow(clippy::too_many_lines)]
fn tiny_budgets_deliver_beside_a_flood(nonblocking: bool) {
    let mut driver = Driver::<(), 3, 64>::new(2).unwrap();
    let source = StaticStream::<Number, 1>::new();
    let source_receiver = source.subscribe(SubscriptionRole::Required).unwrap();
    let (_stopped_reader, mut blocked_data) = UnixStream::pair().unwrap();
    blocked_data.set_nonblocking(true).unwrap();
    loop {
        match blocked_data.write(&[0; 4096]) {
            Ok(0) => panic!("socket closed while filling its send buffer"),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => panic!("{error}"),
        }
    }
    blocked_data.set_nonblocking(nonblocking).unwrap();
    let (_stopped_credit, blocked_control) = UnixStream::pair().unwrap();
    let mut blocked_export = Box::pin(
        driver
            .export(source_receiver, blocked_data, blocked_control, ())
            .unwrap(),
    );
    let blocked_signal = Arc::new(Signal::default());
    let blocked_waker = Waker::from(blocked_signal.clone());
    source.sender().send_now(99).unwrap();
    assert!(blocked_export
        .as_mut()
        .poll(&mut Context::from_waker(&blocked_waker))
        .is_pending());
    let destinations: [StaticStream<Number, 1>; 2] = std::array::from_fn(|_| StaticStream::new());
    let receivers = destinations
        .each_ref()
        .map(|stream| stream.subscribe(SubscriptionRole::Required).unwrap());
    let signals = [Arc::new(Signal::default()), Arc::new(Signal::default())];
    let wakers = signals.each_ref().map(|signal| Waker::from(signal.clone()));
    let mut imports = Vec::new();
    let mut peers = Vec::new();
    let mut credits = Vec::new();
    for (index, destination) in destinations.iter().enumerate() {
        let (mut peer, data) = UnixStream::pair().unwrap();
        let (credit, control) = UnixStream::pair().unwrap();
        credit.set_nonblocking(true).unwrap();
        let mut import = Box::pin(
            driver
                .import(destination.sender(), data, control, ())
                .unwrap(),
        );
        assert!(import
            .as_mut()
            .poll(&mut Context::from_waker(&wakers[index]))
            .is_pending());
        for sequence in 0..if index == 0 { 32 } else { 1 } {
            let mut frame = [0; 46];
            hiway::transport::encode_frame::<Number>(sequence, &(sequence + 100), &mut frame)
                .unwrap();
            peer.write_all(&frame).unwrap();
        }
        imports.push(import);
        peers.push(peer);
        credits.push(credit);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut received = [0_u64; 2];
    let mut healthy_at = None;
    while received != [32, 1] {
        let region = Region::new(ALLOCATOR);
        let progress = driver.advance_io_with_budget(TINY).unwrap();
        let change = region.change();
        assert_eq!(
            (
                change.allocations,
                change.deallocations,
                change.reallocations
            ),
            (0, 0, 0)
        );
        assert!(progress.completions <= 1 && progress.submissions <= 1 && progress.bytes <= 1);
        if progress.schedule == Schedule::Continue {
            assert_eq!(
                driver.wait().unwrap(),
                0,
                "wait parked with local work remaining"
            );
        }
        driver.dispatch();
        for index in 0..2 {
            if signals[index].0.swap(0, Ordering::Relaxed) != 0 {
                assert!(imports[index]
                    .as_mut()
                    .poll(&mut Context::from_waker(&wakers[index]))
                    .is_pending());
            }
            if let Some(StreamItem::Data { value, .. }) = receivers[index].recv_now().unwrap() {
                assert_eq!(*value, received[index] + 100);
                received[index] += 1;
                if index == 1 {
                    healthy_at = Some(received[0]);
                }
            }
            destinations[index].maintain();
            match credits[index].read(&mut [0; 64]) {
                Ok(0) => panic!("credit socket closed"),
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) => {}
                Err(error) => panic!("{error}"),
            }
        }
        assert!(
            Instant::now() < deadline,
            "healthy traffic or the flood stopped progressing: nonblocking={nonblocking}, received={received:?}, progress={progress:?}, work={:?}",
            driver.pending_work()
        );
        std::thread::yield_now();
    }
    assert!(
        healthy_at.unwrap() < 32,
        "healthy traffic waited for the flood to finish"
    );
    assert_eq!(
        blocked_signal.0.load(Ordering::Relaxed),
        0,
        "blocked send caused a retry wake"
    );
    assert!(blocked_export
        .as_mut()
        .poll(&mut Context::from_waker(&blocked_waker))
        .is_pending());
    loop {
        let progress = driver.advance_io_with_budget(TINY).unwrap();
        driver.dispatch();
        if progress.schedule == Schedule::Wait {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "idle I/O did not park (nonblocking={nonblocking})"
        );
        std::thread::yield_now();
    }
    drop(peers);
}

fn submissions_rotate_and_cancellation_finishes(entries: u32, budget: Budget) {
    let mut driver = Driver::<Authority, 3, 64>::new(entries).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let trace = Rc::new(RefCell::new(Vec::new()));
    let live = Rc::new(Cell::new(3));
    let mut imports = Vec::new();
    let mut peers = Vec::new();
    for id in 0..3 {
        let (peer, data) = UnixStream::pair().unwrap();
        let (credit, control) = UnixStream::pair().unwrap();
        imports.push(Box::pin(
            driver
                .import(
                    destination.sender(),
                    data,
                    control,
                    Authority {
                        id,
                        trace: trace.clone(),
                        contended: Rc::new(Cell::new(false)),
                        live: live.clone(),
                    },
                )
                .unwrap(),
        ));
        peers.push((peer, credit));
    }
    trace.borrow_mut().clear();
    for turn in 0..6 {
        let progress = driver.advance_with_budget(budget).unwrap();
        assert_eq!(progress.submissions, 1);
        assert_eq!(
            progress.bytes,
            if turn < 3 { 64.min(budget.bytes) } else { 1 }
        );
        assert_eq!(progress.completions, 0);
    }
    assert_eq!(&*trace.borrow(), &[0, 1, 2, 0, 1, 2]);
    assert_eq!(driver.schedule(), Schedule::Wait);
    drop(imports.remove(0));
    drop(imports.remove(0));
    assert_eq!(live.get(), 3);
    let deadline = Instant::now() + Duration::from_secs(5);
    while driver.active_links() != 1 {
        let progress = driver.advance_with_budget(TINY).unwrap();
        assert!(progress.completions <= 1 && progress.submissions <= 1);
        assert_eq!(progress.bytes, 0, "cancellation consumed a byte budget");
        assert!(
            Instant::now() < deadline,
            "tiny budgets stranded cancellation"
        );
        std::thread::yield_now();
    }
    assert_eq!(live.get(), 1);
    drop(driver);
    assert_eq!(live.get(), 0);
}

fn contention_requests_retry_without_waking() {
    let mut driver = Driver::<Authority, 1, 64>::new(4).unwrap();
    let destination = StaticStream::<Number, 1>::new();
    let trace = Rc::new(RefCell::new(Vec::new()));
    let contended = Rc::new(Cell::new(false));
    let (_peer, data) = UnixStream::pair().unwrap();
    let (_credit, control) = UnixStream::pair().unwrap();
    let mut import = Box::pin(
        driver
            .import(
                destination.sender(),
                data,
                control,
                Authority {
                    id: 0,
                    trace: trace.clone(),
                    contended: contended.clone(),
                    live: Rc::new(Cell::new(1)),
                },
            )
            .unwrap(),
    );
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    assert!(import
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    contended.set(true);
    trace.borrow_mut().clear();
    for attempt in 1..=16 {
        let progress = driver.advance_with_budget(TINY).unwrap();
        assert_eq!(progress.schedule, Schedule::Retry);
        assert_eq!(
            (progress.completions, progress.submissions, progress.bytes),
            (0, 0, 0)
        );
        assert_eq!(
            trace.borrow().len(),
            attempt,
            "admission retried inside a pass"
        );
        assert_eq!(signal.0.load(Ordering::Relaxed), 0);
    }
    contended.set(false);
    let progress = driver
        .advance_with_budget(Budget {
            completions: 4,
            submissions: 4,
            bytes: 64,
        })
        .unwrap();
    assert_eq!((progress.submissions, progress.bytes), (1, 64));
    assert_eq!(progress.schedule, Schedule::Continue);
    let progress = driver.advance_with_budget(TINY).unwrap();
    assert_eq!(
        (progress.completions, progress.submissions, progress.bytes),
        (0, 1, 1)
    );
    assert_eq!(progress.schedule, Schedule::Wait);
    for _ in 0..16 {
        let progress = driver.advance_with_budget(TINY).unwrap();
        assert_eq!(
            (progress.completions, progress.submissions, progress.bytes),
            (0, 0, 0)
        );
        assert_eq!(progress.schedule, Schedule::Wait);
        assert_eq!(signal.0.load(Ordering::Relaxed), 0);
    }
}

fn zero_limits_reject_without_consuming_work() {
    let mut driver = Driver::<(), 1, 64>::new(2).unwrap();
    for budget in [
        Budget {
            completions: 0,
            ..TINY
        },
        Budget {
            submissions: 0,
            ..TINY
        },
        Budget { bytes: 0, ..TINY },
    ] {
        assert_eq!(
            driver.advance_io_with_budget(budget).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(driver.schedule(), Schedule::Wait);
    }
}
