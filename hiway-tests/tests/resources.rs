#![cfg(feature = "std")]

use hiway::{
    DecodeContext, DecodeError, DynamicFabric, EventId, EventSpec, Grant, Limits, Permission,
    Resource, Rights, SchemaRevision, StreamConfig, StreamItem, StreamLimits, SubscriptionRole,
    TopicError, WireCodec, WireError,
};
use std::{
    cell::{Cell, RefCell},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

fn resource_grant(items: usize, bytes: usize, grants: usize) -> Grant {
    DynamicFabric::new()
        .grant(
            &[],
            Limits {
                retained_items: items,
                bytes,
                grants,
                ..Limits::ZERO
            },
        )
        .unwrap()
}

#[test]
fn resource_reservations_are_atomic_and_cannot_borrow_event_allowances() {
    let grant = resource_grant(2, 8, 0);
    assert_eq!(
        grant.reserve_resources(1, 9).err(),
        Some(TopicError::Capacity)
    );
    assert_eq!(grant.usage(), Limits::ZERO);
    let reservation = grant.reserve_resources(2, 8).unwrap();
    assert_eq!(grant.usage().retained_items, 2);
    assert_eq!(grant.usage().bytes, 8);
    assert_eq!(
        grant.reserve_resources(1, 0).err(),
        Some(TopicError::Capacity)
    );
    assert_eq!(
        grant.reserve_resources(0, 1).err(),
        Some(TopicError::Capacity)
    );
    drop(reservation);
    assert_eq!(grant.usage(), Limits::ZERO);

    let (_, event_grant) = stream_grant(0, 0, 0);
    assert_eq!(
        event_grant.stream_limits::<Blob>().unwrap().retained_items,
        1
    );
    assert_eq!(
        event_grant.reserve_resources(1, 0).err(),
        Some(TopicError::Capacity)
    );
    assert_eq!(event_grant.usage(), Limits::ZERO);
}

struct Retired {
    grant: Grant,
    still_charged: Arc<AtomicBool>,
}
impl Drop for Retired {
    fn drop(&mut self) {
        self.still_charged.store(
            self.grant.usage().bytes == 8 && self.grant.usage().retained_items == 1,
            Ordering::Relaxed,
        );
    }
}

#[tokio::test]
async fn shared_resource_retires_after_last_owner_even_after_revocation() {
    let grant = resource_grant(1, 8, 0);
    let still_charged = Arc::new(AtomicBool::new(false));
    let resource = Arc::new(grant.reserve_resources(1, 8).unwrap().attach(Retired {
        grant: grant.clone(),
        still_charged: still_charged.clone(),
    }));
    let retained = resource.clone();
    drop(resource);
    grant.revoke().await.unwrap();
    assert_eq!(
        grant.reserve_resources(0, 0).err(),
        Some(TopicError::Revoked)
    );
    assert_eq!(grant.usage().bytes, 8);
    std::thread::spawn(move || drop(retained)).join().unwrap();
    assert!(
        still_charged.load(Ordering::Relaxed),
        "quota returned before resource destructor"
    );
    assert_eq!(grant.usage(), Limits::ZERO);
}

#[tokio::test]
async fn child_allowance_stays_reserved_until_resource_retirement() {
    let parent = resource_grant(1, 8, 1);
    let child = parent
        .restrict(
            &[],
            Limits {
                retained_items: 1,
                bytes: 8,
                ..Limits::ZERO
            },
        )
        .unwrap();
    let resource = child.reserve_resources(1, 8).unwrap().attach([0u8; 8]);
    drop(child);
    parent.revoke().await.unwrap();
    assert_eq!(parent.usage().bytes, 8);
    assert_eq!(parent.usage().grants, 1);
    drop(resource);
    assert_eq!(parent.usage(), Limits::ZERO);
}

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static DECODE_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

struct Blob;
impl EventSpec for Blob {
    type Payload = Resource<Vec<u8>>;
    const ID: EventId = EventId::from_name("resources.uniform-blob");
}
impl WireCodec for Blob {
    fn encoded_len(_: &Self::Payload) -> usize {
        5
    }
    fn encode(payload: &Self::Payload, output: &mut [u8]) -> Result<usize, WireError> {
        if output.len() < 5 {
            return Err(WireError::BufferTooSmall);
        }
        let length = u32::try_from(payload.len()).map_err(|_| WireError::InvalidPayload)?;
        output[..4].copy_from_slice(&length.to_le_bytes());
        output[4] = payload.first().copied().unwrap_or(0);
        Ok(5)
    }
    fn decode(_: &[u8], _: SchemaRevision) -> Result<Self::Payload, WireError> {
        Err(WireError::InvalidPayload)
    }
    fn decode_with_resources(
        bytes: &[u8],
        _: SchemaRevision,
        resources: &Grant,
    ) -> Result<Self::Payload, DecodeError> {
        if bytes.len() != 5 {
            return Err(WireError::InvalidPayload.into());
        }
        let length = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let reservation = resources.reserve_resources(1, length)?;
        if bytes[4] == 255 {
            return Err(WireError::InvalidPayload.into());
        }
        let hook = DECODE_HOOK.with(|hook| hook.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        Ok(reservation.attach(vec![bytes[4]; length]))
    }
}

#[test]
fn decode_reserves_expanded_storage_before_allocating_and_rolls_back_errors() {
    let grant = resource_grant(1, 16, 0);
    assert_eq!(
        grant
            .decode::<Blob>(&[17, 0, 0, 0, 7], SchemaRevision(0))
            .err(),
        Some(DecodeError::Resources(TopicError::Capacity))
    );
    assert_eq!(ALLOCATIONS.get(), 0);
    assert_eq!(grant.usage(), Limits::ZERO);
    assert_eq!(
        grant
            .decode::<Blob>(&[16, 0, 0, 0, 255], SchemaRevision(0))
            .err(),
        Some(DecodeError::Wire(WireError::InvalidPayload))
    );
    assert_eq!(grant.usage(), Limits::ZERO);
    let resource = grant
        .decode::<Blob>(&[16, 0, 0, 0, 7], SchemaRevision(0))
        .unwrap();
    assert_eq!(resource.as_slice(), &[7; 16]);
    assert_eq!(grant.usage().bytes, 16);
    assert_eq!(ALLOCATIONS.get(), 1);
    drop(resource);
    assert_eq!(grant.usage(), Limits::ZERO);
}

fn stream_grant(
    transport_bytes: usize,
    resource_bytes: usize,
    generic_items: usize,
) -> (DynamicFabric, Grant) {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<Blob>(StreamConfig {
            capacity: 1,
            subscribers: 1,
            waiters: 1,
        })
        .unwrap();
    let grant = fabric
        .grant(
            &[
                Permission::new::<Blob>(Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED)
                    .with_limits(StreamLimits {
                        retained_items: 1,
                        subscriptions: 1,
                        waiters: 1,
                    }),
            ],
            Limits {
                streams: 1,
                subscriptions: 1,
                retained_items: 1 + generic_items,
                waiters: 3,
                connections: 2,
                bytes: transport_bytes + resource_bytes,
                ..Limits::ZERO
            },
        )
        .unwrap();
    (fabric, grant)
}

fn frame(sequence: u64) -> [u8; 43] {
    let payload = resource_grant(1, 16, 0)
        .reserve_resources(1, 16)
        .unwrap()
        .attach(vec![7; 16]);
    let mut frame = [0; 43];
    hiway::transport::encode_frame::<Blob>(sequence, &payload, &mut frame).unwrap();
    frame
}

#[cfg(target_os = "linux")]
#[test]
fn uring_credit_and_link_shutdown_do_not_retire_retained_payloads() {
    use hiway::transport::{Direction, Error, GrantReservation};
    use hiway_uring::{Budget, Driver};
    use std::{
        future::Future,
        io::{Read, Write},
        os::unix::net::UnixStream,
        pin::pin,
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };

    type Domain = Driver<GrantReservation, 1, 64>;
    let arena = Domain::reservation_bytes();
    let (_fabric, grant) = stream_grant(arena, 16, 2);
    let receiver = grant.subscribe::<Blob>(SubscriptionRole::Required).unwrap();
    let mut driver = Domain::new(2).unwrap();
    let (mut peer, data) = UnixStream::pair().unwrap();
    let (mut credits, control) = UnixStream::pair().unwrap();
    credits.set_nonblocking(true).unwrap();
    let reservation = grant
        .reserve_transport::<Blob>(Direction::Import, arena)
        .unwrap();
    let mut import = pin!(driver
        .import(grant.sender::<Blob>().unwrap(), data, control, reservation)
        .unwrap());
    peer.write_all(&frame(0)).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let deadline = Instant::now() + Duration::from_secs(5);
    let held = loop {
        driver.advance_io().unwrap();
        driver.dispatch_with_budget(1);
        assert!(import.as_mut().poll(&mut cx).is_pending());
        if let Some(StreamItem::Data { value, .. }) = receiver.recv_now().unwrap() {
            break value;
        }
        assert!(Instant::now() < deadline, "no resource delivery");
        std::thread::yield_now();
    };
    assert_eq!(held.as_slice(), &[7; 16]);
    let retained = held.clone();
    drop(held);
    let mut credit = [0; 9];
    let mut read = 0;
    while read != credit.len() {
        driver.advance_io().unwrap();
        driver.dispatch();
        match credits.read(&mut credit[read..]) {
            Ok(0) => panic!("credit socket closed"),
            Ok(count) => read += count,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("{error}"),
        }
        assert!(Instant::now() < deadline, "admission credit not returned");
        std::thread::yield_now();
    }
    assert_eq!(credit, [1, 0, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(grant.usage().bytes, arena + 16);
    let allocations = ALLOCATIONS.get();
    peer.write_all(&frame(1)).unwrap();
    loop {
        driver.advance_io().unwrap();
        driver.dispatch();
        if let Poll::Ready(result) = import.as_mut().poll(&mut cx) {
            assert_eq!(result, Err(Error::Topic(TopicError::Capacity)));
            break;
        }
        assert!(
            Instant::now() < deadline,
            "retained resource quota was bypassed"
        );
        std::thread::yield_now();
    }
    assert_eq!(ALLOCATIONS.get(), allocations);
    driver.cancel_all();
    while !driver.shutdown_complete() {
        driver
            .advance_io_with_budget(Budget {
                completions: 1,
                submissions: 1,
                bytes: 1,
            })
            .unwrap();
        driver.dispatch_with_budget(1);
        assert!(
            Instant::now() < deadline,
            "shutdown did not retire transport storage"
        );
        std::thread::yield_now();
    }
    drop(driver);
    let mut revoke = pin!(grant.revoke());
    assert_eq!(revoke.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    assert_eq!(grant.usage().bytes, 16);
    assert_eq!(grant.usage().retained_items, 1);
    drop(retained);
    assert_eq!(grant.usage().bytes, 0);
    assert_eq!(grant.usage().retained_items, 0);
}

#[cfg(target_os = "linux")]
#[test]
fn decode_can_drop_driver_without_borrowing_its_reservation() {
    use hiway::transport::{Direction, Error, GrantReservation};
    use hiway_uring::Driver;
    use std::{
        future::Future,
        io::Write,
        os::unix::net::UnixStream,
        pin::pin,
        rc::Rc,
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };

    type Domain = Driver<GrantReservation, 1, 64>;
    let arena = Domain::reservation_bytes();
    let (_fabric, grant) = stream_grant(arena, 16, 2);
    let driver = Rc::new(RefCell::new(Some(Domain::new(2).unwrap())));
    let (mut peer, data) = UnixStream::pair().unwrap();
    let (_credits, control) = UnixStream::pair().unwrap();
    let reservation = grant
        .reserve_transport::<Blob>(Direction::Import, arena)
        .unwrap();
    let mut import = pin!(driver
        .borrow()
        .as_ref()
        .unwrap()
        .import(grant.sender::<Blob>().unwrap(), data, control, reservation)
        .unwrap());
    let owner = driver.clone();
    DECODE_HOOK
        .with(|hook| *hook.borrow_mut() = Some(Box::new(move || drop(owner.borrow_mut().take()))));
    peer.write_all(&frame(0)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        driver.borrow_mut().as_mut().unwrap().advance_io().unwrap();
        if let Poll::Ready(result) = import.as_mut().poll(&mut cx) {
            assert_eq!(result, Err(Error::Closed));
            break;
        }
        assert!(Instant::now() < deadline, "codec not reached");
        std::thread::yield_now();
    }
    assert!(driver.borrow().is_none());
    assert_eq!(grant.usage().bytes, 0);
    assert_eq!(grant.usage().retained_items, 0);
}

#[cfg(all(feature = "tokio-io", unix))]
#[tokio::test]
async fn unix_link_uses_resource_aware_decode_and_preserves_retained_charge() {
    use hiway::{IpcError, UnixLink};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixStream,
        time::{timeout, Duration},
    };

    let arena = 5 + 38 + 2 * 9;
    let (_fabric, grant) = stream_grant(arena, 16, 2);
    let receiver = grant.subscribe::<Blob>(SubscriptionRole::Required).unwrap();
    let (data, mut peer) = UnixStream::pair().unwrap();
    let (control, mut credits) = UnixStream::pair().unwrap();
    let mut import = Box::pin(UnixLink::import::<Blob>(grant.clone(), data, control, 5).unwrap());
    peer.write_all(&frame(0)).await.unwrap();
    let mut credit = [0; 9];
    timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut import => panic!("early import termination: {result:?}"),
            result = credits.read_exact(&mut credit) => { result.unwrap(); },
        }
    })
    .await
    .unwrap();
    assert_eq!(credit, [1, 0, 0, 0, 0, 0, 0, 0, 0]);
    let Some(StreamItem::Data {
        value: retained, ..
    }) = receiver.recv_now().unwrap()
    else {
        panic!("credit preceded payload admission");
    };
    assert_eq!(retained.as_slice(), &[7; 16]);
    assert_eq!(grant.usage().bytes, arena + 16);
    let allocations = ALLOCATIONS.get();
    peer.write_all(&frame(1)).await.unwrap();
    let result = timeout(Duration::from_secs(5), import.as_mut())
        .await
        .unwrap();
    assert!(matches!(result, Err(IpcError::Topic(TopicError::Capacity))));
    assert_eq!(ALLOCATIONS.get(), allocations);
    drop(import);
    grant.revoke().await.unwrap();
    assert_eq!(grant.usage().bytes, 16);
    drop(retained);
    assert_eq!(grant.usage().bytes, 0);
}
