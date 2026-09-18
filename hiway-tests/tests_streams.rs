#![cfg(not(loom))]

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use hiway::{
    events, CloseReason, DynamicFabric, Grant, Limits, Permission, ReceiveError, Rights, SendError,
    StreamConfig, StreamItem, SubscriptionRole, TopicError, TrySendError,
};

#[derive(Debug)]
struct MoveOnly(u32);

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReentrantPayload {
    on_drop: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl ReentrantPayload {
    fn plain(_value: u32) -> Self {
        Self { on_drop: None }
    }

    fn with_callback(_value: u32, on_drop: Arc<dyn Fn() + Send + Sync>) -> Self {
        Self {
            on_drop: Some(on_drop),
        }
    }
}

impl Drop for ReentrantPayload {
    fn drop(&mut self) {
        if let Some(on_drop) = self.on_drop.take() {
            on_drop();
        }
    }
}

#[events]
enum StreamEvents {
    Value(u32),
    Other(u32),
    MoveOnly(MoveOnly),
    Resource(DropProbe),
    Reentrant(ReentrantPayload),
}

const DATA_LIMITS: hiway::StreamLimits = hiway::StreamLimits {
    retained_items: 64,
    subscriptions: 8,
    waiters: 8,
};

fn all_rights() -> Rights {
    Rights::PUBLISH
        .union(Rights::OBSERVE)
        .union(Rights::REQUIRED)
}

fn finite_limits() -> Limits {
    Limits {
        streams: 8,
        grants: 8,
        subscriptions: 16,
        retained_items: 128,
        waiters: 16,
        connections: 8,
        bytes: 65_536,
    }
}

#[test]
fn rights_and_unassigned_capacity_do_not_implicitly_create_event_reservations() {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<stream_events::Value>(stream_config(2))
        .unwrap();
    let grant = fabric
        .grant(
            &[Permission::new::<stream_events::Value>(all_rights())],
            finite_limits(),
        )
        .unwrap();
    assert_eq!(
        grant.stream_limits::<stream_events::Value>().unwrap(),
        hiway::StreamLimits::ZERO
    );
    assert!(matches!(
        grant.sender::<stream_events::Value>(),
        Err(TopicError::Capacity)
    ));
    assert!(matches!(
        grant.subscribe::<stream_events::Value>(SubscriptionRole::Observer),
        Err(TopicError::Capacity)
    ));
    let reserved =
        Permission::new::<stream_events::Value>(Rights::PUBLISH).with_limits(hiway::StreamLimits {
            retained_items: 1,
            ..hiway::StreamLimits::ZERO
        });
    assert!(matches!(
        grant.restrict(
            &[reserved],
            Limits {
                streams: 1,
                retained_items: 1,
                ..Limits::ZERO
            }
        ),
        Err(TopicError::Capacity)
    ));
    assert_eq!(grant.usage(), Limits::ZERO);
}

#[test]
fn one_grants_event_reservations_isolate_retention_membership_and_waiters() {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<stream_events::Value>(stream_config(8))
        .unwrap();
    fabric
        .create_stream::<stream_events::Other>(stream_config(8))
        .unwrap();
    let per_event = hiway::StreamLimits {
        retained_items: 1,
        subscriptions: 1,
        waiters: 1,
    };
    let grant = fabric
        .grant(
            &[
                Permission::new::<stream_events::Value>(all_rights()).with_limits(per_event),
                Permission::new::<stream_events::Other>(all_rights()).with_limits(per_event),
            ],
            Limits {
                streams: 2,
                retained_items: 2,
                subscriptions: 2,
                waiters: 2,
                ..Limits::ZERO
            },
        )
        .unwrap();
    let first = grant.sender::<stream_events::Value>().unwrap();
    let second = grant.sender::<stream_events::Other>().unwrap();
    let stopped = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .unwrap();
    assert!(matches!(
        grant.subscribe::<stream_events::Value>(SubscriptionRole::Observer),
        Err(TopicError::Capacity)
    ));
    let active = grant
        .subscribe::<stream_events::Other>(SubscriptionRole::Required)
        .unwrap();
    first.send_now(1).unwrap();
    let wake = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    {
        let mut blocked = std::pin::pin!(first.send(2));
        assert!(poll_with(blocked.as_mut(), &waker).is_pending());
        let mut excess = std::pin::pin!(first.send(3));
        assert_eq!(
            poll_with(excess.as_mut(), &waker),
            Poll::Ready(Err(SendError::WaitersFull(3)))
        );
        assert_eq!(
            grant.stream_usage::<stream_events::Value>().unwrap(),
            per_event
        );
        let before = wake.calls.load(Ordering::SeqCst);
        for sequence in 0..8 {
            let mut received = std::pin::pin!(active.recv());
            assert!(poll_with(received.as_mut(), Waker::noop()).is_pending());
            assert_eq!(grant.usage().waiters, 2);
            second.send_now(sequence).unwrap();
            assert!(matches!(poll_with(received.as_mut(), Waker::noop()),
                Poll::Ready(Ok(StreamItem::Data { sequence: actual, value }))
                if actual == u64::from(sequence) && *value == sequence));
        }
        assert_eq!(wake.calls.load(Ordering::SeqCst), before);
        assert_eq!(
            grant
                .stream_usage::<stream_events::Other>()
                .unwrap()
                .retained_items,
            0
        );
    }
    assert_eq!(grant.usage().waiters, 0);
    assert_eq!(
        grant.stream_limits::<stream_events::Value>().unwrap(),
        per_event
    );
    drop(stopped);
    assert_eq!(grant.usage().retained_items, 0);
}

#[test]
fn delegation_cannot_convert_event_capacity_or_mint_it_through_clones() {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<stream_events::Value>(stream_config(8))
        .unwrap();
    fabric
        .create_stream::<stream_events::Other>(stream_config(8))
        .unwrap();
    let per_event = hiway::StreamLimits {
        retained_items: 2,
        subscriptions: 1,
        waiters: 1,
    };
    let total = Limits {
        streams: 6,
        grants: 3,
        retained_items: 4,
        subscriptions: 2,
        waiters: 2,
        ..Limits::ZERO
    };
    let parent = fabric
        .grant(
            &[
                Permission::new::<stream_events::Value>(all_rights()).with_limits(per_event),
                Permission::new::<stream_events::Other>(all_rights()).with_limits(per_event),
            ],
            total,
        )
        .unwrap();
    let a = Permission::new::<stream_events::Value>(all_rights()).with_limits(per_event);
    let child_total = Limits {
        streams: 1,
        retained_items: 2,
        subscriptions: 1,
        waiters: 1,
        ..Limits::ZERO
    };
    let child = parent.restrict(&[a], child_total).unwrap();
    let before = parent.usage();
    assert!(matches!(
        parent.clone().restrict(&[a], child_total),
        Err(TopicError::Capacity)
    ));
    assert_eq!(parent.usage(), before);
    assert_eq!(
        parent.stream_usage::<stream_events::Value>().unwrap(),
        per_event
    );
    let b = Permission::new::<stream_events::Other>(all_rights()).with_limits(per_event);
    let other = parent.restrict(&[b], child_total).unwrap();
    assert_eq!(parent.usage().retained_items, 4);
    drop(other);
    let before = parent.usage();
    // No generic remainder: unused B capacity cannot become IPC frame capacity.
    assert!(matches!(
        parent.restrict(
            &[],
            Limits {
                retained_items: 1,
                ..Limits::ZERO
            }
        ),
        Err(TopicError::Capacity)
    ));
    // Reclaiming A does not authorize a larger B reservation.
    drop(child);
    let too_large = b.with_limits(hiway::StreamLimits {
        retained_items: 3,
        ..per_event
    });
    assert!(matches!(
        parent.restrict(
            &[too_large],
            Limits {
                retained_items: 3,
                ..child_total
            }
        ),
        Err(TopicError::Capacity)
    ));
    assert_eq!(parent.usage(), Limits::ZERO);
    assert_eq!(before.retained_items, 2);
    let child = parent.restrict(&[a], child_total).unwrap();
    let clone = child.clone();
    drop(child);
    assert_eq!(parent.usage().retained_items, 2);
    drop(clone);
    assert_eq!(parent.usage(), Limits::ZERO);
}

fn stream_config(capacity: usize) -> StreamConfig {
    StreamConfig {
        capacity,
        subscribers: 8,
        waiters: 8,
    }
}

fn setup_value(capacity: usize) -> (DynamicFabric, Grant) {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::Value>(stream_config(capacity))
        .is_ok());
    let grant = bus
        .grant(
            &[Permission::new::<stream_events::Value>(all_rights()).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("value grant");
    (bus, grant)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Observer,
    Required,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Slot {
    Observer,
    Required,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Publish(u32),
    Receive(Slot),
    Subscribe(Role),
    Drop(Slot),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expected {
    Published,
    Full,
    Empty,
    Data { sequence: u64, value: u32 },
    Gap { from: u64, to: u64 },
    Subscribed,
    Dropped,
}

#[derive(Clone, Copy)]
struct ModelSubscription {
    cursor: u64,
    role: Role,
}

#[derive(Clone)]
struct StreamModel {
    capacity: usize,
    base: u64,
    next: u64,
    history: Vec<(u64, u32)>,
    observer: Option<ModelSubscription>,
    required: Option<ModelSubscription>,
}

impl StreamModel {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            base: 0,
            next: 0,
            history: Vec::new(),
            observer: None,
            required: None,
        }
    }

    fn subscription(&self, slot: Slot) -> Option<ModelSubscription> {
        match slot {
            Slot::Observer => self.observer,
            Slot::Required => self.required,
        }
    }

    fn subscription_mut(&mut self, slot: Slot) -> &mut Option<ModelSubscription> {
        match slot {
            Slot::Observer => &mut self.observer,
            Slot::Required => &mut self.required,
        }
    }

    fn legal_actions(&self) -> Vec<Action> {
        let mut actions = vec![Action::Publish(10), Action::Publish(20)];

        if let Some(subscription) = self.observer {
            debug_assert_eq!(subscription.role, Role::Observer);
            actions.push(Action::Receive(Slot::Observer));
            actions.push(Action::Drop(Slot::Observer));
        } else {
            actions.push(Action::Subscribe(Role::Observer));
        }

        if let Some(subscription) = self.required {
            debug_assert_eq!(subscription.role, Role::Required);
            actions.push(Action::Receive(Slot::Required));
            actions.push(Action::Drop(Slot::Required));
        } else {
            actions.push(Action::Subscribe(Role::Required));
        }

        actions
    }

    fn apply(&mut self, action: Action) -> Expected {
        match action {
            Action::Publish(value) => {
                let required_blocks = self.history.len() == self.capacity
                    && self
                        .required
                        .is_some_and(|subscription| subscription.cursor <= self.base);
                if required_blocks {
                    return Expected::Full;
                }

                if self.history.len() == self.capacity {
                    self.history.remove(0);
                    self.base += 1;
                }
                self.history.push((self.next, value));
                self.next += 1;
                Expected::Published
            }
            Action::Receive(slot) => {
                let base = self.base;
                let next = self.next;
                let cursor = self
                    .subscription(slot)
                    .expect("generated operation must be legal")
                    .cursor;

                if cursor < base {
                    let gap = Expected::Gap {
                        from: cursor,
                        to: base,
                    };
                    self.subscription_mut(slot)
                        .as_mut()
                        .expect("generated operation must be legal")
                        .cursor = base;
                    return gap;
                }

                if cursor == next {
                    return Expected::Empty;
                }

                let index = usize::try_from(cursor - base).expect("model cursor fits usize");
                let (sequence, value) = self.history[index];
                self.subscription_mut(slot)
                    .as_mut()
                    .expect("generated operation must be legal")
                    .cursor += 1;
                Expected::Data { sequence, value }
            }
            Action::Subscribe(role) => {
                let slot = match role {
                    Role::Observer => Slot::Observer,
                    Role::Required => Slot::Required,
                };
                let cursor = self.next;
                let subscription = self.subscription_mut(slot);
                assert!(subscription.is_none(), "generated operation must be legal");
                *subscription = Some(ModelSubscription { cursor, role });
                Expected::Subscribed
            }
            Action::Drop(slot) => {
                self.subscription_mut(slot)
                    .take()
                    .expect("generated operation must be legal");
                Expected::Dropped
            }
        }
    }
}

fn enumerate_sequences(
    remaining: usize,
    model: &StreamModel,
    path: &mut Vec<Action>,
    sequences: &mut Vec<Vec<Action>>,
) {
    if remaining == 0 {
        sequences.push(path.clone());
        return;
    }

    for action in model.legal_actions() {
        let mut next = model.clone();
        next.apply(action);
        path.push(action);
        enumerate_sequences(remaining - 1, &next, path, sequences);
        path.pop();
    }
}

macro_rules! actual_receive {
    ($receiver:expr) => {{
        match $receiver.recv_now() {
            Ok(None) => Expected::Empty,
            Ok(Some(StreamItem::Data { sequence, value })) => Expected::Data {
                sequence,
                value: *value,
            },
            Ok(Some(StreamItem::Gap { from, to })) => Expected::Gap { from, to },
            Err(ReceiveError::Contended | ReceiveError::WaitersFull) => {
                panic!("unexpected receive contention in sequential model case")
            }
            Err(ReceiveError::Closed(reason)) => {
                panic!("unexpected receive closure in sequential model case: {reason:?}")
            }
            Err(_) => panic!("unexpected receive error in sequential model case"),
        }
    }};
}

macro_rules! actual_step {
    ($action:expr, $sender:expr, $observer:expr, $required:expr, $grant:expr) => {{
        match $action {
            Action::Publish(value) => match $sender.send_now(value) {
                Ok(()) => Expected::Published,
                Err(TrySendError::Full(payload)) => {
                    assert_eq!(payload, value);
                    Expected::Full
                }
                Err(
                    TrySendError::Contended(payload)
                    | TrySendError::Closed(payload)
                    | TrySendError::MaintenanceRequired(payload)
                    | TrySendError::Revoked(payload)
                    | TrySendError::WaitersFull(payload)
                    | TrySendError::SequenceExhausted(payload),
                ) => {
                    panic!("unexpected rejection for sequential model case: {payload}")
                }
                Err(_) => panic!("unexpected unknown rejection for sequential model case"),
            },
            Action::Receive(Slot::Observer) => {
                actual_receive!($observer.as_mut().expect("observer"))
            }
            Action::Receive(Slot::Required) => {
                actual_receive!($required.as_mut().expect("required"))
            }
            Action::Subscribe(Role::Observer) => {
                $observer = Some(
                    $grant
                        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
                        .expect("observer subscription"),
                );
                Expected::Subscribed
            }
            Action::Subscribe(Role::Required) => {
                $required = Some(
                    $grant
                        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
                        .expect("required subscription"),
                );
                Expected::Subscribed
            }
            Action::Drop(Slot::Observer) => {
                drop($observer.take());
                Expected::Dropped
            }
            Action::Drop(Slot::Required) => {
                drop($required.take());
                Expected::Dropped
            }
        }
    }};
}

#[test]
fn short_operation_sequences_match_an_independent_stream_model() {
    let mut sequences = Vec::new();
    enumerate_sequences(5, &StreamModel::new(2), &mut Vec::new(), &mut sequences);
    assert!(sequences.len() > 100);

    for actions in sequences {
        let (_bus, grant) = setup_value(2);
        let sender = grant.sender::<stream_events::Value>().expect("sender");
        let mut observer = Some(
            grant
                .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
                .expect("observer subscription type"),
        );
        drop(observer.take());
        let mut required = Some(
            grant
                .subscribe::<stream_events::Value>(SubscriptionRole::Required)
                .expect("required subscription type"),
        );
        drop(required.take());
        let mut model = StreamModel::new(2);

        for action in actions {
            let expected = model.apply(action);
            let actual = actual_step!(action, sender, observer, required, grant);
            assert_eq!(actual, expected, "action sequence diverged at {action:?}");
        }
    }
}

#[test]
fn identical_event_contracts_remain_isolated_by_fabric_scope() {
    let (_left_bus, left_grant) = setup_value(2);
    let (_right_bus, right_grant) = setup_value(2);
    let sender = left_grant
        .sender::<stream_events::Value>()
        .expect("left sender");
    let receiver = right_grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
        .expect("right receiver");

    assert!(sender.send_now(41).is_ok());
    assert!(receiver.recv_now().expect("right receive").is_none());
}

#[test]
fn permissions_are_directional_and_required_membership_is_explicit() {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::Value>(stream_config(2))
        .is_ok());

    let publish_only = bus
        .grant(
            &[Permission::new::<stream_events::Value>(Rights::PUBLISH).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("publish grant");
    let observe_only = bus
        .grant(
            &[Permission::new::<stream_events::Value>(Rights::OBSERVE).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("observe grant");
    let observe_without_required = bus
        .grant(
            &[
                Permission::new::<stream_events::Value>(Rights::PUBLISH.union(Rights::OBSERVE))
                    .with_limits(DATA_LIMITS),
            ],
            finite_limits(),
        )
        .expect("observer grant");

    assert!(matches!(
        publish_only.subscribe::<stream_events::Value>(SubscriptionRole::Observer),
        Err(TopicError::Denied)
    ));
    assert!(matches!(
        observe_only.sender::<stream_events::Value>(),
        Err(TopicError::Denied)
    ));
    assert!(matches!(
        observe_without_required.subscribe::<stream_events::Value>(SubscriptionRole::Required),
        Err(TopicError::Denied)
    ));
}

#[test]
fn child_quota_is_conserved_when_the_parent_grant_is_cloned() {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::Value>(stream_config(2))
        .is_ok());

    let mut parent_limits = finite_limits();
    parent_limits.grants = 1;
    let parent = bus
        .grant(
            &[Permission::new::<stream_events::Value>(all_rights()).with_limits(DATA_LIMITS)],
            parent_limits,
        )
        .expect("parent grant");

    let mut child_limits = Limits::ZERO;
    child_limits.streams = 1;
    child_limits.subscriptions = 1;
    child_limits.retained_items = 2;
    child_limits.waiters = 1;
    child_limits.connections = 1;
    child_limits.bytes = 1_024;
    let child = parent
        .restrict(
            &[
                Permission::new::<stream_events::Value>(Rights::OBSERVE).with_limits(
                    hiway::StreamLimits {
                        retained_items: 0,
                        subscriptions: 1,
                        waiters: 1,
                    },
                ),
            ],
            child_limits,
        )
        .expect("first child grant");
    assert!(child
        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
        .is_ok());

    let cloned_parent = parent.clone();
    assert_eq!(parent.usage().grants, cloned_parent.usage().grants);
    assert_eq!(
        parent.usage().subscriptions,
        cloned_parent.usage().subscriptions
    );
    assert!(matches!(
        cloned_parent.restrict(
            &[
                Permission::new::<stream_events::Value>(Rights::OBSERVE).with_limits(
                    hiway::StreamLimits {
                        retained_items: 0,
                        subscriptions: 1,
                        waiters: 1
                    }
                )
            ],
            child_limits,
        ),
        Err(TopicError::Capacity)
    ));
}

#[test]
fn exhausted_publisher_quota_cannot_evict_another_publishers_value() {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::Value>(stream_config(2))
        .is_ok());

    let mut exhausted_limits = finite_limits();
    exhausted_limits.retained_items = 1;
    let exhausted = bus
        .grant(
            &[
                Permission::new::<stream_events::Value>(Rights::PUBLISH).with_limits(
                    hiway::StreamLimits {
                        retained_items: 1,
                        subscriptions: 8,
                        waiters: 8,
                    },
                ),
            ],
            exhausted_limits,
        )
        .expect("exhausted publisher grant");
    let _reservation = exhausted
        .restrict(
            &[
                Permission::new::<stream_events::Value>(Rights::PUBLISH).with_limits(
                    hiway::StreamLimits {
                        retained_items: 1,
                        ..hiway::StreamLimits::ZERO
                    },
                ),
            ],
            Limits {
                streams: 1,
                retained_items: 1,
                ..Limits::ZERO
            },
        )
        .unwrap();
    let producer = bus
        .grant(
            &[Permission::new::<stream_events::Value>(Rights::PUBLISH).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("producer grant");
    let observer = bus
        .grant(
            &[Permission::new::<stream_events::Value>(Rights::OBSERVE).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("observer grant");
    let observer_receiver = observer
        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
        .expect("observer receiver");
    let producer_sender = producer
        .sender::<stream_events::Value>()
        .expect("producer sender");
    let exhausted_sender = exhausted
        .sender::<stream_events::Value>()
        .expect("exhausted sender");

    assert!(producer_sender.send_now(7).is_ok());
    assert!(matches!(
        exhausted_sender.send_now(8),
        Err(TrySendError::Full(8))
    ));
    assert!(matches!(
        observer_receiver.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 0,
            value
        })) if *value == 7
    ));
}

#[test]
fn one_grants_quota_cannot_evict_another_events_observer_value() {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::Value>(stream_config(2))
        .is_ok());
    assert!(bus
        .create_stream::<stream_events::Other>(stream_config(2))
        .is_ok());

    let mut shared_limits = finite_limits();
    shared_limits.retained_items = 2;
    let shared = bus
        .grant(
            &[
                Permission::new::<stream_events::Value>(all_rights()).with_limits(
                    hiway::StreamLimits {
                        retained_items: 1,
                        subscriptions: 8,
                        waiters: 8,
                    },
                ),
                Permission::new::<stream_events::Other>(all_rights()).with_limits(
                    hiway::StreamLimits {
                        retained_items: 1,
                        subscriptions: 8,
                        waiters: 8,
                    },
                ),
            ],
            shared_limits,
        )
        .expect("shared multi-event grant");
    let _reservation = shared
        .restrict(
            &[
                Permission::new::<stream_events::Other>(Rights::PUBLISH).with_limits(
                    hiway::StreamLimits {
                        retained_items: 1,
                        ..hiway::StreamLimits::ZERO
                    },
                ),
            ],
            Limits {
                streams: 1,
                retained_items: 1,
                ..Limits::ZERO
            },
        )
        .unwrap();
    let other_producer = bus
        .grant(
            &[Permission::new::<stream_events::Other>(Rights::PUBLISH).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("other producer grant");
    let other_observer = bus
        .grant(
            &[Permission::new::<stream_events::Other>(Rights::OBSERVE).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("other observer grant");

    let required_a = shared
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .expect("required event A receiver");
    let sender_a = shared
        .sender::<stream_events::Value>()
        .expect("event A sender");
    let observer_b = other_observer
        .subscribe::<stream_events::Other>(SubscriptionRole::Observer)
        .expect("event B observer");
    let producer_b = other_producer
        .sender::<stream_events::Other>()
        .expect("event B producer");
    let shared_sender_b = shared
        .sender::<stream_events::Other>()
        .expect("shared event B sender");

    assert!(sender_a.send_now(1).is_ok());
    assert!(producer_b.send_now(42).is_ok());
    assert!(matches!(
        shared_sender_b.send_now(43),
        Err(TrySendError::Full(43))
    ));
    assert!(matches!(
        observer_b.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 0,
            value
        })) if *value == 42
    ));
    drop(required_a);
}

#[test]
fn rejection_errors_preserve_their_payloads() {
    assert_eq!(TrySendError::Full(1_u32).into_inner(), 1);
    assert_eq!(TrySendError::Contended(2_u32).into_inner(), 2);
    assert_eq!(TrySendError::Closed(3_u32).into_inner(), 3);
    assert_eq!(TrySendError::Revoked(4_u32).into_inner(), 4);
    assert_eq!(TrySendError::WaitersFull(5_u32).into_inner(), 5);
    assert_eq!(TrySendError::SequenceExhausted(6_u32).into_inner(), 6);

    let (_bus, grant) = setup_value(1);
    let sender = grant.sender::<stream_events::Value>().expect("sender");
    let _receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .expect("required receiver");
    assert!(sender.send_now(10).is_ok());
    let error = sender.send_now(11).expect_err("required stream is full");
    assert_eq!(error.into_inner(), 11);
}

#[test]
fn observer_overwrite_reports_a_gap_without_stalling_publication() {
    let (_bus, grant) = setup_value(2);
    let sender = grant.sender::<stream_events::Value>().expect("sender");
    let receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
        .expect("observer receiver");

    assert!(sender.send_now(0).is_ok());
    assert!(sender.send_now(1).is_ok());
    assert!(sender.send_now(2).is_ok());

    assert!(matches!(
        receiver.recv_now(),
        Ok(Some(StreamItem::Gap { from: 0, to: 1 }))
    ));
    assert!(matches!(
        receiver.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 1,
            value
        })) if *value == 1
    ));
    assert!(matches!(
        receiver.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 2,
            value
        })) if *value == 2
    ));
}

#[test]
fn dropping_a_required_receiver_releases_its_stream_capacity() {
    let (_bus, grant) = setup_value(1);
    let sender = grant.sender::<stream_events::Value>().expect("sender");
    let receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .expect("required receiver");

    assert!(sender.send_now(1).is_ok());
    let error = sender
        .send_now(2)
        .expect_err("required receiver is stalled");
    assert_eq!(error.into_inner(), 2);
    drop(receiver);
    assert!(sender.send_now(2).is_ok());
}

#[test]
fn required_backpressure_does_not_cross_into_an_unrelated_stream() {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::Value>(stream_config(1))
        .is_ok());
    assert!(bus
        .create_stream::<stream_events::Other>(stream_config(1))
        .is_ok());

    let grant = bus
        .grant(
            &[
                Permission::new::<stream_events::Value>(all_rights()).with_limits(DATA_LIMITS),
                Permission::new::<stream_events::Other>(all_rights()).with_limits(DATA_LIMITS),
            ],
            finite_limits(),
        )
        .expect("grant");
    let value_sender = grant
        .sender::<stream_events::Value>()
        .expect("value sender");
    let other_sender = grant
        .sender::<stream_events::Other>()
        .expect("other sender");
    let _value_receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .expect("value receiver");
    let other_receiver = grant
        .subscribe::<stream_events::Other>(SubscriptionRole::Required)
        .expect("other receiver");

    assert!(value_sender.send_now(1).is_ok());
    assert!(matches!(
        value_sender.send_now(2),
        Err(TrySendError::Full(2))
    ));
    assert!(other_sender.send_now(99).is_ok());
    assert!(matches!(
        other_receiver.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 0,
            value
        })) if *value == 99
    ));
}

struct WakeCounter {
    calls: AtomicUsize,
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_with<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    let mut context = Context::from_waker(waker);
    future.poll(&mut context)
}

#[test]
fn strict_operations_defer_wakes_and_cleanup_to_the_owner() {
    let (bus, grant) = setup_value(1);
    let sender = grant.sender::<stream_events::Value>().unwrap();
    let receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .unwrap();
    let prepared = sender.prepare(7).unwrap();
    let counter = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(counter.clone());
    let mut receive = Box::pin(receiver.recv());
    assert!(poll_with(receive.as_mut(), &waker).is_pending());
    counter.calls.store(0, Ordering::SeqCst);
    prepared.try_send().unwrap();
    assert_eq!(counter.calls.load(Ordering::SeqCst), 0);
    bus.maintain();
    assert!(counter.calls.load(Ordering::SeqCst) > 0);
    assert!(
        matches!(poll_with(receive.as_mut(), &waker), Poll::Ready(Ok(StreamItem::Data { sequence: 0, value })) if *value == 7)
    );
    drop(receive);

    sender.send_now(8).unwrap();
    let mut pending = Box::pin(sender.send(9));
    assert!(poll_with(pending.as_mut(), &waker).is_pending());
    counter.calls.store(0, Ordering::SeqCst);
    let item = receiver.try_recv().unwrap().unwrap();
    assert_eq!(counter.calls.load(Ordering::SeqCst), 0);
    assert_eq!(grant.usage().retained_items, 1);
    drop(item);
    bus.maintain();
    assert!(counter.calls.load(Ordering::SeqCst) > 0);
    assert_eq!(grant.usage().retained_items, 0);
    assert!(matches!(
        poll_with(pending.as_mut(), &waker),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn prepared_rejections_preserve_payloads_and_do_not_hold_revocation_open() {
    let (bus, grant) = setup_value(1);
    let sender = grant.sender::<stream_events::Value>().unwrap();
    let observer = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
        .unwrap();
    let first = sender.prepare(7).unwrap();
    let second = sender.prepare(8).unwrap();
    first.try_send().unwrap();
    let error = second.try_send().expect_err("physical storage is full");
    assert!(matches!(error, TrySendError::MaintenanceRequired(_)));
    assert_eq!(grant.usage().retained_items, 2);
    bus.maintain();
    assert_eq!(grant.usage().retained_items, 1);
    error.into_inner().try_send().unwrap();
    assert!(matches!(
        observer.try_recv(),
        Ok(Some(StreamItem::Gap { from: 0, to: 1 }))
    ));
    assert!(
        matches!(observer.try_recv(), Ok(Some(StreamItem::Data { sequence: 1, value })) if *value == 8)
    );
    bus.maintain();

    let prepared = sender.prepare(9).unwrap();
    let mut revoke = Box::pin(grant.revoke());
    assert_eq!(
        poll_with(revoke.as_mut(), Waker::noop()),
        Poll::Ready(Ok(()))
    );
    let error = prepared
        .try_send()
        .expect_err("prepared payload cannot bypass revocation");
    assert!(matches!(error, TrySendError::Revoked(_)));
    assert_eq!(error.into_inner().into_inner(), 9);
    assert_eq!(grant.usage().retained_items, 0);
}

#[test]
fn preparation_reclamation_does_not_hold_its_cutoff_gate_during_payload_drop() {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<stream_events::Reentrant>(stream_config(1))
        .unwrap();
    let grant = fabric
        .grant(
            &[
                Permission::new::<stream_events::Reentrant>(all_rights()).with_limits(
                    hiway::StreamLimits {
                        retained_items: 1,
                        subscriptions: 1,
                        waiters: 0,
                    },
                ),
            ],
            finite_limits(),
        )
        .unwrap();
    let _observer = grant
        .subscribe::<stream_events::Reentrant>(SubscriptionRole::Observer)
        .unwrap();
    let sender = grant.sender::<stream_events::Reentrant>().unwrap();
    let revoked = Arc::new(AtomicUsize::new(0));
    let observed = revoked.clone();
    let owner = grant.clone();
    let callback = Arc::new(move || {
        let mut revoke = std::pin::pin!(owner.revoke());
        assert_eq!(
            revoke
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(()))
        );
        observed.fetch_add(1, Ordering::Relaxed);
    });
    sender
        .send_now(ReentrantPayload::with_callback(1, callback))
        .unwrap();
    assert!(matches!(
        sender.prepare(ReentrantPayload::plain(2)),
        Err(TrySendError::Revoked(_))
    ));
    assert_eq!(revoked.load(Ordering::Relaxed), 1);
    assert_eq!(grant.usage().retained_items, 0);
}

#[test]
fn strict_receive_does_not_run_a_payload_destructor() {
    let bus = DynamicFabric::new();
    bus.create_stream::<stream_events::Resource>(stream_config(1))
        .unwrap();
    let grant = bus
        .grant(
            &[Permission::new::<stream_events::Resource>(all_rights()).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .unwrap();
    let sender = grant.sender::<stream_events::Resource>().unwrap();
    let receiver = grant
        .subscribe::<stream_events::Resource>(SubscriptionRole::Required)
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    sender
        .prepare(DropProbe(drops.clone()))
        .unwrap()
        .try_send()
        .unwrap();
    let item = receiver.try_recv().unwrap().unwrap();
    drop(item);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    bus.maintain();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn cancelling_one_waiter_leaves_another_waiter_independent() {
    let (_bus, grant) = setup_value(1);
    let sender = grant.sender::<stream_events::Value>().expect("sender");
    let receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .expect("required receiver");
    assert!(sender.send_now(1).is_ok());

    let wake_counter = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&wake_counter));
    let mut cancelled = Box::pin(sender.send(2));
    let mut surviving = Box::pin(sender.send(3));
    assert!(matches!(
        poll_with(cancelled.as_mut(), &waker),
        Poll::Pending
    ));
    assert!(matches!(
        poll_with(surviving.as_mut(), &waker),
        Poll::Pending
    ));
    drop(cancelled);

    assert!(matches!(
        receiver.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 0,
            value
        })) if *value == 1
    ));
    assert!(wake_counter.calls.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poll_with(surviving.as_mut(), &waker),
        Poll::Ready(Ok(()))
    ));
    assert!(matches!(
        receiver.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 1,
            value
        })) if *value == 3
    ));
}

#[test]
fn empty_receive_and_full_send_register_without_self_wake_loops() {
    let (_bus, grant) = setup_value(1);
    let sender = grant.sender::<stream_events::Value>().expect("sender");
    let receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
        .expect("observer receiver");
    let receive_wakes = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let receive_waker_handle = Waker::from(Arc::clone(&receive_wakes));
    let mut receive = Box::pin(receiver.recv());

    assert!(matches!(
        poll_with(receive.as_mut(), &receive_waker_handle),
        Poll::Pending
    ));
    assert_eq!(receive_wakes.calls.load(Ordering::SeqCst), 0);
    assert!(sender.send_now(7).is_ok());
    assert_eq!(receive_wakes.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        poll_with(receive.as_mut(), &receive_waker_handle),
        Poll::Ready(Ok(StreamItem::Data {
            sequence: 0,
            value
        })) if *value == 7
    ));
    assert_eq!(receive_wakes.calls.load(Ordering::SeqCst), 1);

    let (_bus, grant) = setup_value(1);
    let sender = grant.sender::<stream_events::Value>().expect("sender");
    let receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .expect("required receiver");
    assert!(sender.send_now(8).is_ok());
    let send_wakes = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let send_waker_handle = Waker::from(Arc::clone(&send_wakes));
    let mut send = Box::pin(sender.send(9));

    assert!(matches!(
        poll_with(send.as_mut(), &send_waker_handle),
        Poll::Pending
    ));
    assert_eq!(send_wakes.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        receiver.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 0,
            value
        })) if *value == 8
    ));
    assert_eq!(send_wakes.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        poll_with(send.as_mut(), &send_waker_handle),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(send_wakes.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn revocation_closes_subscriptions_and_rejects_new_publications() {
    let (_bus, grant) = setup_value(1);
    let receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
        .expect("observer receiver");
    let sender = grant.sender::<stream_events::Value>().expect("sender");

    grant.revoke().await.expect("revoke");
    assert!(matches!(
        receiver.recv_now(),
        Err(ReceiveError::Closed(CloseReason::Revoked))
    ));
    assert!(matches!(sender.send_now(1), Err(TrySendError::Revoked(1))));
}

#[test]
fn stalled_observer_does_not_consume_a_tiny_sender_retention_quota() {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::Value>(stream_config(4))
        .is_ok());
    let mut limits = finite_limits();
    limits.retained_items = 1;
    let grant = bus
        .grant(
            &[
                Permission::new::<stream_events::Value>(all_rights()).with_limits(
                    hiway::StreamLimits {
                        retained_items: 1,
                        subscriptions: 8,
                        waiters: 8,
                    },
                ),
            ],
            limits,
        )
        .expect("value grant");
    let sender = grant.sender::<stream_events::Value>().expect("sender");
    let receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
        .expect("observer receiver");

    for value in 0..8 {
        assert!(sender.send_now(value).is_ok());
    }
    let first = receiver.recv_now().expect("observer receive");
    assert!(
        matches!(first, Some(StreamItem::Gap { from: 0, to: 7 })),
        "unexpected first item: {first:?}"
    );
    assert!(matches!(
        receiver.recv_now(),
        Ok(Some(StreamItem::Data {
            sequence: 7,
            value
        })) if *value == 7
    ));
    assert!(receiver.recv_now().expect("observer receive").is_none());
}

#[test]
fn closing_a_stream_terminates_old_handles_and_releases_queued_payloads() {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::Value>(stream_config(1))
        .is_ok());
    let grant = bus
        .grant(
            &[Permission::new::<stream_events::Value>(all_rights()).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("value grant");
    let sender = grant.sender::<stream_events::Value>().expect("sender");
    let required = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .expect("required receiver");
    assert!(sender.send_now(1).is_ok());
    let observer = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Observer)
        .expect("tail observer");

    let close_wakes = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let close_waker_handle = Waker::from(Arc::clone(&close_wakes));
    let mut pending_send = Box::pin(sender.send(2));
    let mut pending_receive = Box::pin(observer.recv());
    assert!(matches!(
        poll_with(pending_send.as_mut(), &close_waker_handle),
        Poll::Pending
    ));
    assert!(matches!(
        poll_with(pending_receive.as_mut(), &close_waker_handle),
        Poll::Pending
    ));

    assert!(bus.close_stream::<stream_events::Value>().is_ok());
    assert!(close_wakes.calls.load(Ordering::SeqCst) >= 2);
    assert!(matches!(
        poll_with(pending_send.as_mut(), &close_waker_handle),
        Poll::Ready(Err(SendError::Closed(2)))
    ));
    assert!(matches!(
        poll_with(pending_receive.as_mut(), &close_waker_handle),
        Poll::Ready(Err(ReceiveError::Closed(CloseReason::Closed)))
    ));
    assert!(matches!(sender.send_now(3), Err(TrySendError::Closed(3))));
    assert!(matches!(
        observer.recv_now(),
        Err(ReceiveError::Closed(CloseReason::Closed))
    ));
    drop(required);
    drop(grant);

    assert!(bus
        .create_stream::<stream_events::Value>(stream_config(1))
        .is_err());
    assert!(matches!(sender.send_now(5), Err(TrySendError::Closed(5))));

    let drops = Arc::new(AtomicUsize::new(0));
    assert!(bus
        .create_stream::<stream_events::Resource>(stream_config(1))
        .is_ok());
    let resource_grant = bus
        .grant(
            &[Permission::new::<stream_events::Resource>(all_rights()).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("resource grant");
    let resource_sender = resource_grant
        .sender::<stream_events::Resource>()
        .expect("resource sender");
    let _resource_receiver = resource_grant
        .subscribe::<stream_events::Resource>(SubscriptionRole::Observer)
        .expect("resource receiver");
    assert!(resource_sender
        .send_now(DropProbe(Arc::clone(&drops)))
        .is_ok());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(bus.close_stream::<stream_events::Resource>().is_ok());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn payload_drop_can_publish_on_another_handle_of_the_same_stream() {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::Reentrant>(stream_config(1))
        .is_ok());
    let grant = bus
        .grant(
            &[Permission::new::<stream_events::Reentrant>(all_rights()).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("reentrant grant");
    let callback_sender = grant
        .sender::<stream_events::Reentrant>()
        .expect("callback sender");
    let callback_result = Arc::new(AtomicUsize::new(0));
    let callback_result_for_drop = Arc::clone(&callback_result);
    let callback = Arc::new(move || {
        let result = callback_sender.send_now(ReentrantPayload::plain(99));
        let code = match result {
            Ok(()) => 1,
            Err(error) => {
                if matches!(&error, TrySendError::Full(_)) {
                    2
                } else if matches!(&error, TrySendError::Contended(_)) {
                    3
                } else {
                    4
                }
            }
        };
        callback_result_for_drop.store(code, Ordering::SeqCst);
    });
    let sender = grant.sender::<stream_events::Reentrant>().expect("sender");
    let receiver = grant
        .subscribe::<stream_events::Reentrant>(SubscriptionRole::Observer)
        .expect("observer receiver");

    assert!(sender
        .send_now(ReentrantPayload::with_callback(1, callback))
        .is_ok());
    let (done, completed) = std::sync::mpsc::channel();
    let publisher = thread::spawn(move || {
        let accepted = sender.send_now(ReentrantPayload::plain(2)).is_ok();
        done.send(accepted).expect("reentrant publisher result");
    });
    assert!(completed
        .recv_timeout(Duration::from_secs(1))
        .expect("payload eviction deadlocked the publisher"));
    publisher.join().expect("reentrant publisher panicked");
    assert!(matches!(callback_result.load(Ordering::SeqCst), 1 | 2));
    while let Ok(Some(_)) = receiver.recv_now() {}
}

#[test]
fn concurrent_publishers_admit_each_value_once_in_stream_order() {
    let (_bus, grant) = setup_value(128);
    let left = grant.sender::<stream_events::Value>().expect("left sender");
    let right = grant
        .sender::<stream_events::Value>()
        .expect("right sender");
    let receiver = grant
        .subscribe::<stream_events::Value>(SubscriptionRole::Required)
        .expect("required receiver");

    thread::scope(|scope| {
        scope.spawn(move || {
            for value in 0..32 {
                let mut payload = value;
                loop {
                    match left.send_now(payload) {
                        Ok(()) => break,
                        Err(TrySendError::Contended(value)) => payload = value,
                        Err(TrySendError::Full(value)) => {
                            panic!("unexpected full stream: {value}")
                        }
                        Err(
                            TrySendError::Closed(value)
                            | TrySendError::MaintenanceRequired(value)
                            | TrySendError::Revoked(value)
                            | TrySendError::WaitersFull(value)
                            | TrySendError::SequenceExhausted(value),
                        ) => {
                            panic!("unexpected publish rejection: {value}")
                        }
                        Err(error) => panic!("unexpected publish rejection: {error}"),
                    }
                }
            }
        });
        scope.spawn(move || {
            for value in 100..132 {
                let mut payload = value;
                loop {
                    match right.send_now(payload) {
                        Ok(()) => break,
                        Err(TrySendError::Contended(value)) => payload = value,
                        Err(TrySendError::Full(value)) => {
                            panic!("unexpected full stream: {value}")
                        }
                        Err(
                            TrySendError::Closed(value)
                            | TrySendError::MaintenanceRequired(value)
                            | TrySendError::Revoked(value)
                            | TrySendError::WaitersFull(value)
                            | TrySendError::SequenceExhausted(value),
                        ) => {
                            panic!("unexpected publish rejection: {value}")
                        }
                        Err(error) => panic!("unexpected publish rejection: {error}"),
                    }
                }
            }
        });
    });

    let mut seen = Vec::new();
    for sequence in 0..64 {
        match receiver.recv_now().expect("receive") {
            Some(StreamItem::Data {
                sequence: actual_sequence,
                value,
            }) => {
                assert_eq!(actual_sequence, sequence);
                seen.push(*value);
            }
            Some(StreamItem::Gap { from, to }) => {
                panic!("required stream lost data in range {from}..{to}")
            }
            None => panic!("required stream ended before sequence {sequence}"),
        }
    }
    seen.sort_unstable();
    assert_eq!(seen, (0..32).chain(100..132).collect::<Vec<_>>());
}

#[test]
fn payloads_need_not_be_clone_and_received_arcs_are_caller_owned() {
    let bus = DynamicFabric::new();
    assert!(bus
        .create_stream::<stream_events::MoveOnly>(stream_config(1))
        .is_ok());
    let grant = bus
        .grant(
            &[Permission::new::<stream_events::MoveOnly>(all_rights()).with_limits(DATA_LIMITS)],
            finite_limits(),
        )
        .expect("move-only grant");
    let sender = grant
        .sender::<stream_events::MoveOnly>()
        .expect("move-only sender");
    let receiver = grant
        .subscribe::<stream_events::MoveOnly>(SubscriptionRole::Observer)
        .expect("move-only receiver");

    assert!(sender.send_now(MoveOnly(7)).is_ok());
    let item = receiver.recv_now().expect("move-only receive");
    match item {
        Some(StreamItem::Data { value, .. }) => assert_eq!(value.0, 7),
        Some(StreamItem::Gap { .. }) | None => panic!("move-only value missing"),
    }
}
