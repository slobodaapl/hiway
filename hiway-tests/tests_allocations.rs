#![cfg(not(loom))]

use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

use hiway::{
    events, DynamicFabric, Grant, Limits, Permission, Rights, StreamConfig, StreamItem,
    SubscriptionRole, TrySendError,
};
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[events]
enum AllocationEvents {
    Value(u64),
}

fn finite_limits() -> Limits {
    Limits {
        streams: 4,
        grants: 4,
        subscriptions: 8,
        retained_items: 32,
        waiters: 8,
        connections: 4,
        bytes: 16_384,
    }
}

fn setup_value(capacity: usize) -> (DynamicFabric, Grant) {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<allocation_events::Value>(StreamConfig {
            capacity,
            subscribers: 4,
            waiters: 4,
        })
        .is_ok());
    let grant = bus
        .grant(
            &[Permission::new::<allocation_events::Value>(
                Rights::PUBLISH
                    .union(Rights::OBSERVE)
                    .union(Rights::REQUIRED),
            )
            .with_limits(hiway::StreamLimits {
                retained_items: 32,
                subscriptions: 8,
                waiters: 8,
            })],
            finite_limits(),
        )
        .expect("value grant");
    (bus, grant)
}

fn assert_no_allocations(stats: stats_alloc::Stats) {
    assert_eq!(stats.allocations, 0);
    assert_eq!(stats.reallocations, 0);
}

fn rejected_and_pending_sends_do_not_allocate_unreserved_transport_storage() {
    let (_bus, grant) = setup_value(1);
    let sender = grant.sender::<allocation_events::Value>().expect("sender");
    let _receiver = grant
        .subscribe::<allocation_events::Value>(SubscriptionRole::Required)
        .expect("required receiver");
    assert!(sender.send_now(1).is_ok());

    let full_stats = {
        let region = Region::new(GLOBAL);
        let Err(error) = sender.send_now(2) else {
            panic!("required stream accepted beyond capacity")
        };
        assert!(matches!(error, TrySendError::Full(2)));
        assert_eq!(error.into_inner(), 2);
        region.change()
    };
    assert_no_allocations(full_stats);

    let wake = Waker::noop();
    let mut context = Context::from_waker(wake);
    let baseline_waiters = grant.usage().waiters;
    let pending_stats = {
        let region = Region::new(GLOBAL);
        let mut send = std::pin::pin!(sender.send(3));
        assert!(matches!(send.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(grant.usage().waiters, baseline_waiters + 1);
        region.change()
    };
    assert_no_allocations(pending_stats);
    assert_eq!(grant.usage().waiters, baseline_waiters);
}

fn successful_admission_uses_one_payload_allocation_after_reservation() {
    let (_bus, grant) = setup_value(1);
    let sender = grant.sender::<allocation_events::Value>().expect("sender");
    let receiver = grant
        .subscribe::<allocation_events::Value>(SubscriptionRole::Required)
        .expect("required receiver");
    assert!(sender.send_now(1).is_ok());
    assert!(matches!(
        receiver.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 0,
            value
        })) if *value == 1
    ));

    let stats = {
        let region = Region::new(GLOBAL);
        assert!(sender.send_now(2).is_ok());
        region.change()
    };
    assert_eq!(stats.allocations, 1);
    assert_eq!(stats.reallocations, 0);
    let _ = receiver.recv_now();
}

async fn closed_and_revoked_sends_return_payloads_without_allocating() {
    let (bus, grant) = setup_value(1);
    let sender = grant.sender::<allocation_events::Value>().expect("sender");
    let _receiver = grant
        .subscribe::<allocation_events::Value>(SubscriptionRole::Observer)
        .expect("observer receiver");
    assert!(bus.close_stream::<allocation_events::Value>().is_ok());

    let closed_stats = {
        let region = Region::new(GLOBAL);
        let Err(error) = sender.send_now(4) else {
            panic!("closed stream accepted a publication")
        };
        assert!(matches!(error, TrySendError::Closed(4)));
        assert_eq!(error.into_inner(), 4);
        region.change()
    };
    assert_no_allocations(closed_stats);

    let (_bus, grant) = setup_value(1);
    let sender = grant.sender::<allocation_events::Value>().expect("sender");
    let _receiver = grant
        .subscribe::<allocation_events::Value>(SubscriptionRole::Observer)
        .expect("observer receiver");
    grant.revoke().await.expect("revoke");

    let revoked_stats = {
        let region = Region::new(GLOBAL);
        let Err(error) = sender.send_now(5) else {
            panic!("revoked stream accepted a publication")
        };
        assert!(matches!(error, TrySendError::Revoked(5)));
        assert_eq!(error.into_inner(), 5);
        region.change()
    };
    assert_no_allocations(revoked_stats);
}

#[test]
fn transport_allocation_contracts() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        rejected_and_pending_sends_do_not_allocate_unreserved_transport_storage();
        successful_admission_uses_one_payload_allocation_after_reservation();
        closed_and_revoked_sends_return_payloads_without_allocating().await;
        prepared_admission_and_receive_do_not_touch_the_allocator();
        static_waiter_registration_and_cancellation_do_not_allocate();
    });
}

fn static_waiter_registration_and_cancellation_do_not_allocate() {
    let region = Region::new(GLOBAL);
    let stream = hiway::StaticStream::<allocation_events::Value, 1, 1, 1>::new();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();
    let sender = stream.sender();
    sender.try_send(1).unwrap();
    let mut context = Context::from_waker(Waker::noop());
    {
        let mut cancelled = std::pin::pin!(sender.send(2));
        assert!(cancelled.as_mut().poll(&mut context).is_pending());
    }
    let mut live = std::pin::pin!(sender.send(3));
    assert!(live.as_mut().poll(&mut context).is_pending());
    assert!(
        matches!(receiver.try_recv().unwrap(), Some(StreamItem::Data { sequence: 0, value }) if *value == 1)
    );
    stream.maintain();
    assert_eq!(live.as_mut().poll(&mut context), Poll::Ready(Ok(())));
    assert!(
        matches!(receiver.try_recv().unwrap(), Some(StreamItem::Data { sequence: 1, value }) if *value == 3)
    );
    let stats = region.change();
    assert_no_allocations(stats);
    assert_eq!(stats.deallocations, 0);
}

fn prepared_admission_and_receive_do_not_touch_the_allocator() {
    let (bus, grant) = setup_value(1);
    let sender = grant.sender::<allocation_events::Value>().unwrap();
    let observer = grant
        .subscribe::<allocation_events::Value>(SubscriptionRole::Observer)
        .unwrap();
    let first = sender.prepare(7).unwrap();
    let second = sender.prepare(8).unwrap();
    let region = Region::new(GLOBAL);
    first.try_send().unwrap();
    let second = second
        .try_send()
        .expect_err("full storage needs reclamation");
    assert!(matches!(second, TrySendError::MaintenanceRequired(_)));
    let received = observer.try_recv().unwrap().unwrap();
    let stats = region.change();
    assert_no_allocations(stats);
    assert_eq!(stats.deallocations, 0);
    assert!(matches!(received, StreamItem::Data { sequence: 0, ref value } if **value == 7));
    drop(received);
    bus.maintain();
    let region = Region::new(GLOBAL);
    second.into_inner().try_send().unwrap();
    let stats = region.change();
    assert_no_allocations(stats);
    assert_eq!(stats.deallocations, 0);
}
