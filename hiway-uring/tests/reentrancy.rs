#![cfg(target_os = "linux")]

use hiway::transport::{Contract, Direction, Error, Reservation};
use hiway::{
    EventId, EventReceiver, EventSpec, Port, ReceiveError, SchemaRevision, StaticStream,
    StreamItem, SubscriptionRole, TopicError, WireCodec, WireError,
};
use hiway_uring::Driver;
use std::{
    cell::{Cell, RefCell},
    future::{pending, Future},
    io::{Read, Write},
    os::unix::net::UnixStream,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Site {
    Check,
    Enter,
    AccessDrop,
    ReservationDrop,
    Length,
    DecodeContext,
    Encode,
    Decode,
    Endpoint,
    Clone,
    Drop,
    Wake,
}
type Hook = (Site, Box<dyn FnOnce()>);
thread_local! { static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) }; }

fn fire(site: Site) {
    let hook = HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|(target, _)| *target == site) {
            slot.take()
        } else {
            None
        }
    });
    if let Some((_, action)) = hook {
        action();
    }
}

fn arm(site: Site, action: impl FnOnce() + 'static) {
    HOOK.with(|slot| {
        assert!(slot
            .borrow_mut()
            .replace((site, Box::new(action)))
            .is_none());
    });
}

struct Number;
impl EventSpec for Number {
    type Payload = u64;
    const ID: EventId = EventId::from_name("uring.reentrant.number");
}
impl WireCodec for Number {
    fn encoded_len(_: &u64) -> usize {
        fire(Site::Length);
        8
    }
    fn encode(value: &u64, output: &mut [u8]) -> Result<usize, WireError> {
        fire(Site::Encode);
        output.copy_from_slice(&value.to_le_bytes());
        Ok(8)
    }
    fn decode(bytes: &[u8], _: SchemaRevision) -> Result<u64, WireError> {
        fire(Site::Decode);
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_| WireError::InvalidPayload)?,
        ))
    }
}

#[derive(Default)]
struct Authority(Rc<Cell<bool>>);
struct Access;
impl Drop for Access {
    fn drop(&mut self) {
        fire(Site::AccessDrop);
    }
}
impl Drop for Authority {
    fn drop(&mut self) {
        fire(Site::ReservationDrop);
    }
}
impl Reservation for Authority {
    type Access<'a> = Access;
    fn check(&self, _: Contract, _: Direction, _: usize) -> Result<(), TopicError> {
        Ok(())
    }
    fn enter(&self) -> Result<Access, TopicError> {
        fire(Site::Enter);
        if self.0.get() {
            Err(TopicError::Revoked)
        } else {
            Ok(Access)
        }
    }
    fn is_revoked(&self) -> bool {
        fire(Site::Check);
        self.0.get()
    }

    fn decode_context(&self) -> impl hiway::DecodeContext + 'static {
        fire(Site::DecodeContext);
    }
}

struct Source(Cell<Option<u64>>);
impl Port for Source {}
impl EventReceiver<Number> for Source {
    type Value = Box<u64>;
    fn event_try_recv(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError> {
        Ok(self.0.take().map(|value| StreamItem::Data {
            sequence: 0,
            value: Box::new(value),
        }))
    }
    fn event_recv_now(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError> {
        self.event_try_recv()
    }
    async fn event_recv(&self) -> Result<StreamItem<Self::Value>, ReceiveError> {
        fire(Site::Endpoint);
        match self.event_try_recv()? {
            Some(item) => Ok(item),
            None => pending().await,
        }
    }
}

fn raw_waker() -> RawWaker {
    fn clone(_: *const ()) -> RawWaker {
        fire(Site::Clone);
        raw_waker()
    }
    fn drop(_: *const ()) {
        fire(Site::Drop);
    }
    fn wake(_: *const ()) {
        fire(Site::Wake);
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake, drop);
    RawWaker::new(std::ptr::null(), &VTABLE)
}

fn waker() -> Waker {
    // SAFETY: no pointer is dereferenced or owned. Each operation uses only the
    // executing thread's hook, so cloning, waking and dropping are thread-safe.
    unsafe { Waker::from_raw(raw_waker()) }
}

type Link = Pin<Box<dyn Future<Output = Result<(), Error>>>>;
type Side = Rc<RefCell<Option<Link>>>;
type TestDriver = Driver<Authority, 3, 64>;

struct TrackedAuthority {
    inner: Authority,
    dropped: Rc<Cell<bool>>,
}

impl Drop for TrackedAuthority {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

impl Reservation for TrackedAuthority {
    type Access<'a> = Access;

    fn check(
        &self,
        contract: Contract,
        direction: Direction,
        bytes: usize,
    ) -> Result<(), TopicError> {
        self.inner.check(contract, direction, bytes)
    }

    fn enter(&self) -> Result<Access, TopicError> {
        self.inner.enter()
    }

    fn is_revoked(&self) -> bool {
        self.inner.is_revoked()
    }

    fn decode_context(&self) -> impl hiway::DecodeContext + 'static {
        self.inner.decode_context()
    }
}

type TrackedDriver = Driver<TrackedAuthority, 3, 64>;

fn tracked_authority(dropped: Rc<Cell<bool>>) -> TrackedAuthority {
    TrackedAuthority {
        inner: Authority::default(),
        dropped,
    }
}

fn tracked_export(
    driver: &TrackedDriver,
    value: Option<u64>,
    dropped: Rc<Cell<bool>>,
    data: UnixStream,
    control: UnixStream,
) -> Link {
    Box::pin(
        driver
            .export(
                Source(Cell::new(value)),
                data,
                control,
                tracked_authority(dropped),
            )
            .unwrap(),
    )
}

fn side(driver: &TestDriver) -> (Side, UnixStream, UnixStream) {
    let (peer, data) = UnixStream::pair().unwrap();
    let (credit, control) = UnixStream::pair().unwrap();
    let future = driver
        .export(Source(Cell::new(None)), data, control, Authority::default())
        .unwrap();
    (Rc::new(RefCell::new(Some(Box::pin(future)))), peer, credit)
}

fn reenter(site: Site, other: &Side, victim: &Side) -> Rc<Cell<bool>> {
    let other = other.clone();
    let victim = victim.clone();
    let called = Rc::new(Cell::new(false));
    let result = called.clone();
    arm(site, move || {
        assert!(other
            .borrow_mut()
            .as_mut()
            .unwrap()
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(victim.borrow_mut().take());
        called.set(true);
    });
    result
}

#[test]
fn callbacks_can_poll_and_drop_other_links() {
    for site in [
        Site::Check,
        Site::Enter,
        Site::AccessDrop,
        Site::ReservationDrop,
        Site::Length,
        Site::Encode,
        Site::Endpoint,
        Site::Clone,
        Site::Drop,
        Site::Wake,
    ] {
        let mut driver = TestDriver::new(8).unwrap();
        let (other, _other_data, _other_credit) = side(&driver);
        let (victim, _victim_data, _victim_credit) = side(&driver);
        let (mut peer, data) = UnixStream::pair().unwrap();
        peer.set_nonblocking(true).unwrap();
        let (mut credit, control) = UnixStream::pair().unwrap();
        let mut export = Box::pin(
            driver
                .export(
                    Source(Cell::new(Some(73))),
                    data,
                    control,
                    Authority::default(),
                )
                .unwrap(),
        );
        let waker = waker();
        let called = reenter(site, &other, &victim);
        assert!(export
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        // Replacing a registered waker must release it outside the table borrow.
        if site == Site::Drop {
            assert!(export
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut frame = [0; 46];
        let mut received = 0;
        while received < frame.len() {
            driver.advance().unwrap();
            match peer.read(&mut frame[received..]) {
                Ok(0) => panic!("export closed before completing its frame"),
                Ok(count) => received += count,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) => {}
                Err(error) => panic!("{error}"),
            }
            assert!(Instant::now() < deadline, "{site:?}");
            std::thread::yield_now();
        }
        assert_eq!(&frame[..4], b"HWY1");
        assert_eq!(&frame[38..], &73_u64.to_le_bytes());
        // The export can receive its next item only after peer admission credit.
        if site == Site::Wake {
            credit.write_all(&[1, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        }
        while site == Site::Wake && !called.get() {
            driver.advance().unwrap();
            assert!(
                Instant::now() < deadline,
                "completion did not wake the export"
            );
            std::thread::yield_now();
        }
        drop(export);
        while !called.get() {
            driver.advance().unwrap();
            assert!(Instant::now() < deadline, "callback never ran: {site:?}");
            std::thread::yield_now();
        }
        assert!(victim.borrow().is_none());
    }
}

#[test]
fn decoder_can_poll_and_drop_other_links() {
    let mut driver = TestDriver::new(8).unwrap();
    let (other, _other_data, _other_credit) = side(&driver);
    let (victim, _victim_data, _victim_credit) = side(&driver);
    let destination = StaticStream::<Number, 1>::new();
    let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
    let (mut peer, data) = UnixStream::pair().unwrap();
    let (_credit, control) = UnixStream::pair().unwrap();
    let mut import = Box::pin(
        driver
            .import(destination.sender(), data, control, Authority::default())
            .unwrap(),
    );
    let mut frame = [0; 46];
    hiway::transport::encode_frame::<Number>(0, &73, &mut frame).unwrap();
    peer.write_all(&frame).unwrap();
    let called = reenter(Site::Decode, &other, &victim);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        driver.advance().unwrap();
        assert!(import
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        if let Some(StreamItem::Data { value, .. }) = receiver.recv_now().unwrap() {
            assert_eq!(*value, 73);
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(called.get());
    assert!(victim.borrow().is_none());
}

#[test]
fn codec_results_respect_reentrant_revocation_and_driver_close() {
    for site in [Site::Length, Site::Encode, Site::Decode] {
        for close in [false, true] {
            let driver = Rc::new(RefCell::new(Some(TestDriver::new(8).unwrap())));
            let revoked = Rc::new(Cell::new(false));
            let authority = Authority(revoked.clone());
            let (mut peer, data) = UnixStream::pair().unwrap();
            let (_credit, control) = UnixStream::pair().unwrap();
            let destination = StaticStream::<Number, 1>::new();
            let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
            let mut link: Pin<Box<dyn Future<Output = Result<(), Error>> + '_>> =
                if site == Site::Decode {
                    let mut frame = [0; 46];
                    hiway::transport::encode_frame::<Number>(0, &73, &mut frame).unwrap();
                    peer.write_all(&frame).unwrap();
                    Box::pin(
                        driver
                            .borrow()
                            .as_ref()
                            .unwrap()
                            .import(destination.sender(), data, control, authority)
                            .unwrap(),
                    )
                } else {
                    Box::pin(
                        driver
                            .borrow()
                            .as_ref()
                            .unwrap()
                            .export(Source(Cell::new(Some(73))), data, control, authority)
                            .unwrap(),
                    )
                };
            let owner = driver.clone();
            arm(site, move || {
                if close {
                    drop(owner.borrow_mut().take());
                } else {
                    revoked.set(true);
                }
            });
            let deadline = Instant::now() + Duration::from_secs(5);
            let error = loop {
                // Encoding starts in the source phase, before any submission.
                if site == Site::Decode {
                    driver.borrow_mut().as_mut().unwrap().advance().unwrap();
                }
                if let Poll::Ready(result) =
                    link.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                {
                    break result.unwrap_err();
                }
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            };
            assert_eq!(error, if close { Error::Closed } else { Error::Revoked });
            assert!(receiver.recv_now().unwrap().is_none());
            drop(link);
            drop(driver.borrow_mut().take());
            if site != Site::Decode {
                assert_eq!(
                    peer.read(&mut [0; 1]).unwrap(),
                    0,
                    "revoked encoding reached the peer"
                );
            }
        }
    }
}

#[test]
fn waker_callbacks_cannot_strand_ready_frames() {
    for site in [Site::Clone, Site::Drop] {
        let driver = Rc::new(RefCell::new(Driver::<(), 1, 64>::new(4).unwrap()));
        let destination = StaticStream::<Number, 1>::new();
        let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
        let (mut peer, data) = UnixStream::pair().unwrap();
        let (_credit, control) = UnixStream::pair().unwrap();
        let mut import = Box::pin(
            driver
                .borrow()
                .import(destination.sender(), data, control, ())
                .unwrap(),
        );
        let waker = waker();
        if site == Site::Drop {
            assert!(import
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
        }
        let mut frame = [0; 46];
        hiway::transport::encode_frame::<Number>(0, &73, &mut frame).unwrap();
        peer.write_all(&frame).unwrap();
        let owner = driver.clone();
        arm(site, move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let mut driver = owner.borrow_mut();
                let ready = driver.advance_io().unwrap().work[0].wake;
                driver.dispatch();
                if ready {
                    break;
                }
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
        });
        let next = if site == Site::Clone {
            &waker
        } else {
            Waker::noop()
        };
        assert!(import
            .as_mut()
            .poll(&mut Context::from_waker(next))
            .is_pending());
        assert!(
            matches!(receiver.recv_now().unwrap(), Some(StreamItem::Data { value, .. }) if *value == 73)
        );
        assert!(HOOK.with(|slot| slot.borrow().is_none()));
    }
}

fn assert_closed(result: Poll<Result<(), Error>>) {
    assert!(matches!(result, Poll::Ready(Err(Error::Closed))));
}

#[test]
fn endpoint_callback_can_drop_driver_with_receive_in_flight() {
    let owner = Rc::new(RefCell::new(Some(TestDriver::new(8).unwrap())));
    let destination = StaticStream::<Number, 1>::new();
    let (receive_peer, receive_data) = UnixStream::pair().unwrap();
    let (_receive_credit, receive_control) = UnixStream::pair().unwrap();
    let mut receive: Pin<Box<dyn Future<Output = Result<(), Error>> + '_>> = Box::pin(
        owner
            .borrow()
            .as_ref()
            .unwrap()
            .import(
                destination.sender(),
                receive_data,
                receive_control,
                Authority::default(),
            )
            .unwrap(),
    );
    assert!(receive
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    owner.borrow_mut().as_mut().unwrap().advance().unwrap();

    let (_peer, data) = UnixStream::pair().unwrap();
    let (_credit, control) = UnixStream::pair().unwrap();
    let mut trigger: Link = Box::pin(
        owner
            .borrow()
            .as_ref()
            .unwrap()
            .export(Source(Cell::new(None)), data, control, Authority::default())
            .unwrap(),
    );
    let callback_owner = owner.clone();
    let dropped = Rc::new(Cell::new(false));
    let callback_ran = dropped.clone();
    arm(Site::Endpoint, move || {
        drop(callback_owner.borrow_mut().take());
        callback_ran.set(true);
    });

    let first = trigger
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    assert!(dropped.get());
    assert!(owner.borrow().is_none());
    assert_closed(first);
    assert_closed(
        receive
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
    );
    drop(trigger);
    drop(receive);
    drop(receive_peer);
}

#[test]
fn reservation_and_access_drop_callbacks_can_drop_driver() {
    for site in [Site::Check, Site::Enter, Site::AccessDrop] {
        let owner = Rc::new(RefCell::new(Some(TrackedDriver::new(8).unwrap())));
        let sibling_dropped = Rc::new(Cell::new(false));
        let (sibling_peer, sibling_data) = UnixStream::pair().unwrap();
        let (_sibling_credit, sibling_control) = UnixStream::pair().unwrap();
        let mut sibling: Option<Link> = Some(Box::pin(
            owner
                .borrow()
                .as_ref()
                .unwrap()
                .export(
                    Source(Cell::new(None)),
                    sibling_data,
                    sibling_control,
                    tracked_authority(sibling_dropped.clone()),
                )
                .unwrap(),
        ));
        assert!(sibling
            .as_mut()
            .unwrap()
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());

        let (_peer, data) = UnixStream::pair().unwrap();
        let (_credit, control) = UnixStream::pair().unwrap();
        let target_dropped = Rc::new(Cell::new(false));
        let mut target: Link = Box::pin(
            owner
                .borrow()
                .as_ref()
                .unwrap()
                .export(
                    Source(Cell::new(Some(73))),
                    data,
                    control,
                    tracked_authority(target_dropped.clone()),
                )
                .unwrap(),
        );
        let callback_owner = owner.clone();
        let callback_ran = Rc::new(Cell::new(false));
        let callback_ran_in_hook = callback_ran.clone();
        let target_alive_in_hook = target_dropped.clone();
        arm(site, move || {
            drop(callback_owner.borrow_mut().take());
            assert!(
                !target_alive_in_hook.get(),
                "{site:?} callback destroyed its reservation before returning"
            );
            callback_ran_in_hook.set(true);
        });

        let first = target
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        assert!(callback_ran.get(), "{site:?}");
        assert_closed(first);
        drop(target);
        assert!(
            target_dropped.get(),
            "{site:?} reservation retained by sibling"
        );
        assert!(sibling.is_some(), "sibling future dropped during {site:?}");
        drop(sibling.take());
        drop(sibling_peer);
    }
}

#[test]
fn decode_context_callback_can_drop_driver_and_release_reservation() {
    let owner = Rc::new(RefCell::new(Some(TrackedDriver::new(8).unwrap())));
    let sibling_dropped = Rc::new(Cell::new(false));
    let (sibling_peer, sibling_data) = UnixStream::pair().unwrap();
    let (_sibling_credit, sibling_control) = UnixStream::pair().unwrap();
    let mut sibling: Option<Link> = Some(Box::pin(
        owner
            .borrow()
            .as_ref()
            .unwrap()
            .export(
                Source(Cell::new(None)),
                sibling_data,
                sibling_control,
                tracked_authority(sibling_dropped),
            )
            .unwrap(),
    ));
    assert!(sibling
        .as_mut()
        .unwrap()
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());

    let destination = StaticStream::<Number, 1>::new();
    let receiver = destination.subscribe(SubscriptionRole::Required).unwrap();
    let (mut peer, data) = UnixStream::pair().unwrap();
    let (_credit, control) = UnixStream::pair().unwrap();
    let mut frame = [0; 46];
    hiway::transport::encode_frame::<Number>(0, &73, &mut frame).unwrap();
    peer.write_all(&frame).unwrap();
    let target_dropped = Rc::new(Cell::new(false));
    let mut import: Pin<Box<dyn Future<Output = Result<(), Error>> + '_>> = Box::pin(
        owner
            .borrow()
            .as_ref()
            .unwrap()
            .import(
                destination.sender(),
                data,
                control,
                tracked_authority(target_dropped.clone()),
            )
            .unwrap(),
    );
    let callback_owner = owner.clone();
    let callback_ran = Rc::new(Cell::new(false));
    let target_alive_in_callback = target_dropped.clone();
    let callback_ran_in_context = callback_ran.clone();
    arm(Site::DecodeContext, move || {
        drop(callback_owner.borrow_mut().take());
        assert!(!target_alive_in_callback.get());
        callback_ran_in_context.set(true);
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    let first = loop {
        owner.borrow_mut().as_mut().unwrap().advance().unwrap();
        let result = import
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        if callback_ran.get() {
            break result;
        }
        assert!(
            result.is_pending(),
            "decode context did not run before completion"
        );
        assert!(
            Instant::now() < deadline,
            "decode context callback did not run"
        );
        std::thread::yield_now();
    };

    assert_closed(first);
    assert!(owner.borrow().is_none());
    assert!(receiver.recv_now().unwrap().is_none());
    drop(import);
    assert!(target_dropped.get(), "reservation retained by sibling link");
    assert!(sibling.is_some());
    drop(sibling.take());
    drop(sibling_peer);
}

#[test]
fn nested_enter_callbacks_close_both_links_with_two_reservations_borrowed() {
    let owner = Rc::new(RefCell::new(Some(TrackedDriver::new(8).unwrap())));
    let mut context = Context::from_waker(Waker::noop());
    let (sibling_peer, sibling_data) = UnixStream::pair().unwrap();
    let (_sibling_credit, sibling_control) = UnixStream::pair().unwrap();
    let sibling_future = tracked_export(
        owner.borrow().as_ref().unwrap(),
        None,
        Rc::new(Cell::new(false)),
        sibling_data,
        sibling_control,
    );
    let mut sibling = Some(sibling_future);
    assert!(sibling
        .as_mut()
        .unwrap()
        .as_mut()
        .poll(&mut context)
        .is_pending());

    let inner_dropped = Rc::new(Cell::new(false));
    let (_inner_peer, inner_data) = UnixStream::pair().unwrap();
    let (_inner_credit, inner_control) = UnixStream::pair().unwrap();
    let inner_future = tracked_export(
        owner.borrow().as_ref().unwrap(),
        Some(41),
        inner_dropped.clone(),
        inner_data,
        inner_control,
    );
    let inner: Side = Rc::new(RefCell::new(Some(inner_future)));

    let outer_dropped = Rc::new(Cell::new(false));
    let (_outer_peer, outer_data) = UnixStream::pair().unwrap();
    let (_outer_credit, outer_control) = UnixStream::pair().unwrap();
    let mut outer = tracked_export(
        owner.borrow().as_ref().unwrap(),
        Some(73),
        outer_dropped.clone(),
        outer_data,
        outer_control,
    );
    let nested_called = Rc::new(Cell::new(false));
    let callback_owner = owner.clone();
    let inner_for_callback = inner.clone();
    let inner_dropped_in_callback = inner_dropped.clone();
    let outer_dropped_in_callback = outer_dropped.clone();
    let nested_called_in_callback = nested_called.clone();
    arm(Site::Enter, move || {
        let nested_owner = callback_owner.clone();
        let inner_dropped = inner_dropped_in_callback.clone();
        let outer_dropped = outer_dropped_in_callback.clone();
        let nested_called = nested_called_in_callback.clone();
        arm(Site::Enter, move || {
            drop(nested_owner.borrow_mut().take());
            assert!(!inner_dropped.get());
            assert!(!outer_dropped.get());
            nested_called.set(true);
        });
        let result = inner_for_callback
            .borrow_mut()
            .as_mut()
            .unwrap()
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        assert_closed(result);
        assert!(nested_called_in_callback.get());
        assert!(callback_owner.borrow().is_none());
        assert!(!outer_dropped_in_callback.get());
    });

    let first = outer.as_mut().poll(&mut context);
    assert!(nested_called.get());
    assert_closed(first);
    assert!(owner.borrow().is_none());

    drop(inner.borrow_mut().take());
    assert!(
        inner_dropped.get(),
        "inner reservation retained by sibling link"
    );
    drop(outer);
    assert!(
        outer_dropped.get(),
        "outer reservation retained by sibling link"
    );
    assert!(sibling.is_some());
    drop(sibling.take());
    drop(sibling_peer);
}

#[test]
fn access_drop_cancellation_keeps_cleanup_observable_until_dispatch() {
    let owner = Rc::new(RefCell::new(Some(TrackedDriver::new(8).unwrap())));
    let destination = StaticStream::<Number, 1>::new();
    let (receive_peer, receive_data) = UnixStream::pair().unwrap();
    let (_receive_credit, receive_control) = UnixStream::pair().unwrap();
    let mut receive: Pin<Box<dyn Future<Output = Result<(), Error>> + '_>> = Box::pin(
        owner
            .borrow()
            .as_ref()
            .unwrap()
            .import(
                destination.sender(),
                receive_data,
                receive_control,
                tracked_authority(Rc::new(Cell::new(false))),
            )
            .unwrap(),
    );
    assert!(receive
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    owner.borrow_mut().as_mut().unwrap().advance().unwrap();

    let (_peer, data) = UnixStream::pair().unwrap();
    let (_credit, control) = UnixStream::pair().unwrap();
    let target_dropped = Rc::new(Cell::new(false));
    let mut export: Link = Box::pin(
        owner
            .borrow()
            .as_ref()
            .unwrap()
            .export(
                Source(Cell::new(Some(73))),
                data,
                control,
                tracked_authority(target_dropped.clone()),
            )
            .unwrap(),
    );
    let callback_owner = owner.clone();
    let callback_ran = Rc::new(Cell::new(false));
    let target_alive_in_drop = target_dropped.clone();
    let callback_ran_in_access = callback_ran.clone();
    arm(Site::AccessDrop, move || {
        callback_owner.borrow().as_ref().unwrap().cancel_all();
        assert!(!target_alive_in_drop.get());
        callback_ran_in_access.set(true);
    });

    let first = export
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    assert!(callback_ran.get());
    assert_closed(first);
    {
        let driver = owner.borrow();
        let driver = driver.as_ref().unwrap();
        assert!(!driver.shutdown_complete());
        assert!(driver
            .pending_work()
            .iter()
            .any(|work| work.wake || work.reclaim || work.notify_authority));
    }

    drop(export);
    drop(receive);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let complete = owner.borrow().as_ref().unwrap().shutdown_complete();
        if complete {
            break;
        }
        {
            let mut driver = owner.borrow_mut();
            let driver = driver.as_mut().unwrap();
            driver.dispatch();
            driver.advance().unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "shutdown cleanup did not complete"
        );
        std::thread::yield_now();
    }
    assert!(target_dropped.get());
    drop(receive_peer);
}
