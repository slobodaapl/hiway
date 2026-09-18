#![cfg(not(loom))]

use hiway::{
    events, graph, port, publish, validate_evolution, validate_schema, DynamicFabric,
    EnvelopeHeader, EventId, EventMetadata, EventPort, EventSpec, EventValue, FieldKind,
    FieldPresence, FieldSpec, Grant, Limits, Permission, PortError, PortExt, Rights, SendError,
    StaticStream, StreamConfig, StreamItem, SubscriptionRole, TopicError, Transform, TransformOp,
    TrySendError, WireEnvelope, WireError, WireMajor,
};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProgressData(u32);

#[events]
enum Pipewise {
    Started,
    Stopped,
    Progress(ProgressData),
    AlternativeProgress(ProgressData),
    Unrelated(ProgressData),
}

#[events(wire_major = 2, schema_revision = 3)]
enum Versioned {
    Value(u32),
}

#[port(
    factory = Grant,
    send(pipewise::Started),
    recv(pipewise::Progress, pipewise::AlternativeProgress)
)]
struct WorkerPort;

#[port(factory = Grant, recv(pipewise::Progress))]
struct ObserverPort;

#[port(factory = Grant, required(pipewise::Progress))]
struct RequiredPort;

#[port(
    factory = Grant,
    send(pipewise::Progress),
    recv(pipewise::Progress)
)]
struct CounterPort;

#[port(factory = Grant, recv(pipewise::Started, pipewise::Stopped))]
struct SignalPort;

#[port(
    factory = Grant,
    recv(pipewise::Progress, pipewise::AlternativeProgress)
)]
struct PairPort;

#[graph(
    started = (pipewise::Started, 1),
    progress = (pipewise::Progress, 2),
    alternative = (pipewise::AlternativeProgress, 2),
)]
struct TestGraph;

struct Counter<P> {
    port: P,
    value: u32,
}

impl<P> Counter<P> {
    fn new(port: P, value: u32) -> Self {
        Self { port, value }
    }

    fn increment(&mut self) {
        self.value += 1;
    }
}

impl<P: EventPort<pipewise::Progress>> Counter<P> {
    fn publish_value(&self) -> Result<(), TrySendError<ProgressData>> {
        self.port
            .publish_now(pipewise::Progress(ProgressData(self.value)))
    }

    async fn publish_value_async(&self) -> Result<(), SendError<ProgressData>> {
        self.port
            .publish(pipewise::Progress(ProgressData(self.value)))
            .await
    }
}

struct ConflictingU8;
struct ConflictingU16;

impl EventSpec for ConflictingU8 {
    type Payload = u8;
    const ID: EventId = EventId::from_name("collision");
}

impl EventSpec for ConflictingU16 {
    type Payload = u16;
    const ID: EventId = EventId::from_name("collision");
}

struct AddSeven;

impl TransformOp<ProgressData> for AddSeven {
    type Output = ProgressData;

    async fn apply(&self, input: ProgressData) -> Self::Output {
        ProgressData(input.0 + 7)
    }
}

struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

const PUBLISH_LIMITS: hiway::StreamLimits = hiway::StreamLimits {
    retained_items: 8,
    subscriptions: 0,
    waiters: 1,
};
const OBSERVE_LIMITS: hiway::StreamLimits = hiway::StreamLimits {
    retained_items: 0,
    subscriptions: 1,
    waiters: 1,
};
const DUPLEX_LIMITS: hiway::StreamLimits = hiway::StreamLimits {
    retained_items: 8,
    subscriptions: 1,
    waiters: 1,
};

fn config(capacity: usize) -> StreamConfig {
    StreamConfig {
        capacity,
        subscribers: 8,
        waiters: 8,
    }
}

fn component_grant(
    fabric: &DynamicFabric,
    permissions: &[Permission],
    subscriptions: usize,
) -> Grant {
    fabric
        .grant(
            permissions,
            Limits {
                streams: 8,
                subscriptions,
                retained_items: permissions
                    .iter()
                    .map(|permission| permission.limits.retained_items)
                    .sum(),
                waiters: 8,
                ..Limits::ZERO
            },
        )
        .unwrap()
}

#[test]
fn event_identity_and_tagged_conversions_preserve_the_variant() {
    assert_ne!(pipewise::Started::ID, pipewise::Progress::ID);
    assert_ne!(pipewise::Started::ID, pipewise::Stopped::ID);
    assert_ne!(pipewise::Progress::ID, pipewise::AlternativeProgress::ID);
    assert_eq!(
        versioned::Value::ID,
        EventId::from_name(concat!(module_path!(), "::Versioned::Value"))
    );
    let combined: Pipewise = pipewise::Progress(ProgressData(5)).into();
    let tagged = EventValue::<pipewise::Progress>::try_from(combined)
        .ok()
        .unwrap();
    assert_eq!(tagged.into_inner(), ProgressData(5));
    let wrong_tag =
        EventValue::<pipewise::AlternativeProgress>::try_from(Pipewise::Progress(ProgressData(6)));
    assert!(matches!(
        wrong_tag,
        Err(Pipewise::Progress(ProgressData(6)))
    ));
}

fn typed_port_channels() -> (WorkerPort, SignalPort, Grant) {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<pipewise::Started>(config(4))
        .unwrap();
    fabric
        .create_stream::<pipewise::Stopped>(config(1))
        .unwrap();
    fabric
        .create_stream::<pipewise::Progress>(config(2))
        .unwrap();
    fabric
        .create_stream::<pipewise::AlternativeProgress>(config(2))
        .unwrap();
    fabric
        .create_stream::<pipewise::Unrelated>(config(1))
        .unwrap();
    let worker_grant = component_grant(
        &fabric,
        &[
            Permission::new::<pipewise::Started>(Rights::PUBLISH).with_limits(PUBLISH_LIMITS),
            Permission::new::<pipewise::Progress>(Rights::OBSERVE).with_limits(OBSERVE_LIMITS),
            Permission::new::<pipewise::AlternativeProgress>(Rights::OBSERVE)
                .with_limits(OBSERVE_LIMITS),
        ],
        2,
    );
    let signal_grant = component_grant(
        &fabric,
        &[
            Permission::new::<pipewise::Started>(Rights::OBSERVE).with_limits(OBSERVE_LIMITS),
            Permission::new::<pipewise::Stopped>(Rights::OBSERVE).with_limits(OBSERVE_LIMITS),
        ],
        2,
    );
    let producer = component_grant(
        &fabric,
        &[
            Permission::new::<pipewise::Stopped>(Rights::PUBLISH).with_limits(PUBLISH_LIMITS),
            Permission::new::<pipewise::Progress>(Rights::PUBLISH).with_limits(PUBLISH_LIMITS),
            Permission::new::<pipewise::AlternativeProgress>(Rights::PUBLISH)
                .with_limits(PUBLISH_LIMITS),
            Permission::new::<pipewise::Unrelated>(Rights::PUBLISH).with_limits(PUBLISH_LIMITS),
        ],
        0,
    );
    let worker = WorkerPort::bind(&worker_grant).unwrap();
    let signals = SignalPort::bind(&signal_grant).unwrap();
    (worker, signals, producer)
}

#[tokio::test]
async fn distinct_specs_use_independent_typed_port_channels() {
    let (worker, signals, producer) = typed_port_channels();
    worker
        .prepare(pipewise::Started)
        .unwrap()
        .try_send()
        .unwrap();
    worker.publish_now_started(()).unwrap();
    worker.publish_started(()).await.unwrap();
    publish(&worker, pipewise::Started).await.unwrap();
    for sequence in 0..4 {
        assert_eq!(
            signals.recv_started().await.unwrap().map(|value| *value),
            StreamItem::Data {
                sequence,
                value: ()
            }
        );
    }
    producer
        .sender::<pipewise::Stopped>()
        .unwrap()
        .send(())
        .await
        .unwrap();
    assert_eq!(
        signals
            .recv_now::<pipewise::Stopped>()
            .unwrap()
            .map(|item| item.map(|value| *value)),
        Some(StreamItem::Data {
            sequence: 0,
            value: ()
        })
    );
    producer
        .sender::<pipewise::Unrelated>()
        .unwrap()
        .send_now(ProgressData(99))
        .unwrap();
    assert!(worker.recv_now_progress().unwrap().is_none());
    assert!(worker.recv_now_alternative_progress().unwrap().is_none());
    producer
        .sender::<pipewise::Progress>()
        .unwrap()
        .send(ProgressData(7))
        .await
        .unwrap();
    producer
        .sender::<pipewise::AlternativeProgress>()
        .unwrap()
        .send(ProgressData(8))
        .await
        .unwrap();
    assert_eq!(
        worker
            .recv::<pipewise::Progress>()
            .await
            .unwrap()
            .map(|value| *value),
        StreamItem::Data {
            sequence: 0,
            value: ProgressData(7)
        }
    );
    assert_eq!(
        worker
            .recv_now_alternative_progress()
            .unwrap()
            .map(|item| item.map(|value| *value)),
        Some(StreamItem::Data {
            sequence: 0,
            value: ProgressData(8)
        })
    );
    assert!(worker.recv_now_progress().unwrap().is_none());
}

#[tokio::test]
async fn developer_constructor_owns_state_and_accepts_an_injected_port() {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<pipewise::Progress>(config(1))
        .unwrap();
    let grant = component_grant(
        &fabric,
        &[
            Permission::new::<pipewise::Progress>(Rights::PUBLISH | Rights::OBSERVE)
                .with_limits(DUPLEX_LIMITS),
        ],
        1,
    );
    let mut counter = Counter::new(CounterPort::bind(&grant).unwrap(), 7);
    counter.increment();
    counter.publish_value().unwrap();
    assert_eq!(
        counter
            .port
            .recv_now_progress()
            .unwrap()
            .map(|item| item.map(|value| *value)),
        Some(StreamItem::Data {
            sequence: 0,
            value: ProgressData(8)
        })
    );
    counter.publish_value_async().await.unwrap();
    assert_eq!(
        counter
            .port
            .recv_progress()
            .await
            .unwrap()
            .map(|value| *value),
        StreamItem::Data {
            sequence: 1,
            value: ProgressData(8)
        }
    );
}

#[test]
fn generated_observer_reports_gaps_without_imposing_backpressure() {
    let (sender, observer) = {
        let fabric = DynamicFabric::new();
        fabric
            .create_stream::<pipewise::Progress>(config(1))
            .unwrap();
        let grant = component_grant(
            &fabric,
            &[
                Permission::new::<pipewise::Progress>(Rights::PUBLISH | Rights::OBSERVE)
                    .with_limits(DUPLEX_LIMITS),
            ],
            1,
        );
        (
            grant.sender::<pipewise::Progress>().unwrap(),
            ObserverPort::bind(&grant).unwrap(),
        )
    };
    sender.send_now(ProgressData(1)).unwrap();
    sender.send_now(ProgressData(2)).unwrap();
    assert_eq!(
        observer.recv_now_progress().unwrap(),
        Some(StreamItem::Gap { from: 0, to: 1 })
    );
    assert_eq!(
        observer
            .recv_now_progress()
            .unwrap()
            .map(|item| item.map(|value| *value)),
        Some(StreamItem::Data {
            sequence: 1,
            value: ProgressData(2)
        })
    );
}

#[test]
fn dropping_generated_required_port_releases_pending_publication() {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<pipewise::Progress>(config(1))
        .unwrap();
    let grant = component_grant(
        &fabric,
        &[Permission::new::<pipewise::Progress>(
            Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED,
        )
        .with_limits(DUPLEX_LIMITS)],
        1,
    );
    let receiver = RequiredPort::bind(&grant).unwrap();
    let sender = grant.sender::<pipewise::Progress>().unwrap();
    sender.send_now(ProgressData(1)).unwrap();
    assert!(matches!(
        sender.send_now(ProgressData(2)),
        Err(TrySendError::Full(ProgressData(2)))
    ));
    let calls = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = Waker::from(calls.clone());
    let mut context = Context::from_waker(&waker);
    let mut sending = std::pin::pin!(sender.send(ProgressData(2)));
    assert!(sending.as_mut().poll(&mut context).is_pending());
    drop(receiver);
    assert!(calls.0.load(Ordering::Relaxed) > 0);
    assert!(matches!(
        sending.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(grant.usage().subscriptions, 0);
}

#[test]
fn generated_bind_rejects_unauthorized_operations_and_releases_partial_membership() {
    let fabric = DynamicFabric::new();
    fabric
        .create_stream::<pipewise::Progress>(StreamConfig {
            subscribers: 1,
            ..config(1)
        })
        .unwrap();
    fabric
        .create_stream::<pipewise::AlternativeProgress>(StreamConfig {
            subscribers: 1,
            ..config(1)
        })
        .unwrap();
    let observer_only = component_grant(
        &fabric,
        &[Permission::new::<pipewise::Progress>(Rights::OBSERVE).with_limits(OBSERVE_LIMITS)],
        1,
    );
    assert!(matches!(
        RequiredPort::bind(&observer_only),
        Err(PortError::Topic(TopicError::Denied))
    ));
    assert!(matches!(
        CounterPort::bind(&observer_only),
        Err(PortError::Topic(TopicError::Denied))
    ));
    assert!(matches!(
        PairPort::bind(&observer_only),
        Err(PortError::Topic(TopicError::Denied))
    ));
    assert_eq!(observer_only.usage().subscriptions, 0);
    let observer = ObserverPort::bind(&observer_only).unwrap();
    assert_eq!(observer_only.usage().subscriptions, 1);
    drop(observer);

    let one_slot = component_grant(
        &fabric,
        &[
            Permission::new::<pipewise::Progress>(Rights::OBSERVE).with_limits(OBSERVE_LIMITS),
            Permission::new::<pipewise::AlternativeProgress>(Rights::OBSERVE).with_limits(
                hiway::StreamLimits {
                    retained_items: 0,
                    subscriptions: 0,
                    waiters: 1,
                },
            ),
        ],
        1,
    );
    assert!(matches!(
        PairPort::bind(&one_slot),
        Err(PortError::Topic(TopicError::Capacity))
    ));
    assert_eq!(one_slot.usage().subscriptions, 0);
    let observer = ObserverPort::bind(&one_slot).unwrap();
    assert!(observer.recv_now_progress().unwrap().is_none());
}

#[test]
fn static_graph_composes_multiple_streams_with_runtime_membership() {
    let started = StaticStream::<pipewise::Started, 1>::new();
    let progress = StaticStream::<pipewise::Progress, 2>::new();
    let alternative = StaticStream::<pipewise::AlternativeProgress, 2>::new();
    let graph = TestGraph::new(&started, &progress, &alternative);
    let progress_sender = graph.sender::<pipewise::Progress>().unwrap();
    progress_sender.send_now(ProgressData(99)).unwrap();
    let received = graph
        .subscribe::<pipewise::Progress>(SubscriptionRole::Required)
        .unwrap();
    let other = graph
        .subscribe::<pipewise::AlternativeProgress>(SubscriptionRole::Observer)
        .unwrap();
    assert!(received.recv_now().unwrap().is_none());
    progress_sender.send_now(ProgressData(7)).unwrap();
    graph
        .sender::<pipewise::AlternativeProgress>()
        .unwrap()
        .send_now(ProgressData(8))
        .unwrap();
    assert_eq!(
        received
            .recv_now()
            .unwrap()
            .map(|item| item.map(|value| *value)),
        Some(StreamItem::Data {
            sequence: 1,
            value: ProgressData(7)
        })
    );
    assert_eq!(
        other
            .recv_now()
            .unwrap()
            .map(|item| item.map(|value| *value)),
        Some(StreamItem::Data {
            sequence: 0,
            value: ProgressData(8)
        })
    );
    progress_sender.send_now(ProgressData(9)).unwrap();
    progress_sender.send_now(ProgressData(10)).unwrap();
    assert!(matches!(
        progress_sender.send_now(ProgressData(11)),
        Err(TrySendError::Full(ProgressData(11)))
    ));
    drop(received);
    progress_sender.send_now(ProgressData(11)).unwrap();
    let late = graph
        .subscribe::<pipewise::Progress>(SubscriptionRole::Observer)
        .unwrap();
    assert!(late.recv_now().unwrap().is_none());
    progress_sender.send_now(ProgressData(12)).unwrap();
    assert_eq!(
        late.recv_now()
            .unwrap()
            .map(|item| item.map(|value| *value)),
        Some(StreamItem::Data {
            sequence: 5,
            value: ProgressData(12)
        })
    );
}

#[tokio::test]
async fn subscriber_transforms_compose_synchronous_and_async_operations_in_order() {
    let transform = Transform::new(|ProgressData(value)| ProgressData(value + 1))
        .then(AddSeven)
        .then(Transform::new_async(async |ProgressData(value)| {
            ProgressData(value * 2)
        }));
    assert_eq!(transform.apply(ProgressData(3)).await, ProgressData(22));
    assert_eq!(transform.apply(ProgressData(0)).await, ProgressData(16));
}

#[test]
fn transforms_defer_execution_and_can_return_borrowed_values() {
    let text = String::from("borrowed output");
    let calls = std::cell::Cell::new(0);
    let sync = Transform::new(|()| {
        calls.set(calls.get() + 1);
        text.as_str()
    });
    let asynchronous = Transform::new_async(async |()| {
        calls.set(calls.get() + 1);
        text.as_str()
    });
    let mut sync_future = std::pin::pin!(sync.apply(()));
    let mut async_future = std::pin::pin!(asynchronous.apply(()));
    assert_eq!(calls.get(), 0);
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(
        sync_future.as_mut().poll(&mut context),
        Poll::Ready(text.as_str())
    );
    assert_eq!(calls.get(), 1);
    assert_eq!(
        async_future.as_mut().poll(&mut context),
        Poll::Ready(text.as_str())
    );
    assert_eq!(calls.get(), 2);
}

#[test]
fn registry_rejects_one_identity_with_two_payload_types() {
    let fabric = DynamicFabric::new();
    fabric.create_stream::<ConflictingU8>(config(1)).unwrap();
    assert!(matches!(
        fabric.create_stream::<ConflictingU16>(config(1)),
        Err(TopicError::TypeMismatch(hiway::TopicTypeMismatch { id })) if id == ConflictingU8::ID
    ));
    let grant = component_grant(
        &fabric,
        &[
            Permission::new::<ConflictingU8>(Rights::PUBLISH | Rights::OBSERVE)
                .with_limits(DUPLEX_LIMITS),
        ],
        1,
    );
    assert!(matches!(
        grant.sender::<ConflictingU16>(),
        Err(TopicError::TypeMismatch(_))
    ));
    let receiver = grant
        .subscribe::<ConflictingU8>(SubscriptionRole::Observer)
        .unwrap();
    grant
        .sender::<ConflictingU8>()
        .unwrap()
        .send_now(5)
        .unwrap();
    assert_eq!(
        receiver
            .recv_now()
            .unwrap()
            .map(|item| item.map(|value| *value)),
        Some(StreamItem::Data {
            sequence: 0,
            value: 5
        })
    );
}

#[test]
fn schema_metadata_and_evolution_checks_are_wire_boundaries() {
    assert_eq!(versioned::Value::WIRE_MAJOR, WireMajor(2));
    assert_eq!(versioned::Value::SCHEMA_REVISION, hiway::SchemaRevision(3));

    let previous_fields = [FieldSpec {
        tag: 1,
        kind: FieldKind::Unsigned,
        presence: FieldPresence::Required,
    }];
    let next_fields = [
        previous_fields[0],
        FieldSpec {
            tag: 2,
            kind: FieldKind::Text,
            presence: FieldPresence::Optional,
        },
    ];
    let previous = hiway::Schema {
        event: EventId::from_name("schema::value"),
        wire_major: WireMajor(1),
        revision: hiway::SchemaRevision(1),
        fields: &previous_fields,
        reserved_tags: &[],
    };
    let next = hiway::Schema {
        event: previous.event,
        wire_major: previous.wire_major,
        revision: hiway::SchemaRevision(2),
        fields: &next_fields,
        reserved_tags: &[],
    };
    validate_schema(&next).unwrap();
    validate_evolution(&previous, &next).unwrap();

    let incompatible_fields = [FieldSpec {
        tag: 1,
        kind: FieldKind::Text,
        presence: FieldPresence::Required,
    }];
    let incompatible = hiway::Schema {
        fields: &incompatible_fields,
        ..next
    };
    assert_eq!(
        validate_evolution(&previous, &incompatible),
        Err(hiway::SchemaError::FieldKindChanged(1))
    );
    let made_optional = [FieldSpec {
        tag: 1,
        kind: FieldKind::Unsigned,
        presence: FieldPresence::Optional,
    }];
    assert_eq!(
        validate_evolution(
            &previous,
            &hiway::Schema {
                fields: &made_optional,
                ..next
            }
        ),
        Err(hiway::SchemaError::RequiredFieldMadeOptional(1))
    );

    let mut metadata = EventMetadata::new(hiway::OriginId::ZERO, 9, 11);
    metadata.ttl = Some(1);
    assert!(metadata.consume_hop());
    assert!(!metadata.consume_hop());
}

#[test]
fn schema_reservations_remain_unreusable_across_revisions() {
    let previous_fields = [FieldSpec {
        tag: 1,
        kind: FieldKind::Unsigned,
        presence: FieldPresence::Required,
    }];
    let next_fields = [
        previous_fields[0],
        FieldSpec {
            tag: 7,
            kind: FieldKind::Bytes,
            presence: FieldPresence::Optional,
        },
    ];
    let previous = hiway::Schema {
        event: EventId::from_name("schema::reserved"),
        wire_major: WireMajor(1),
        revision: hiway::SchemaRevision(1),
        fields: &previous_fields,
        reserved_tags: &[7],
    };
    let next = hiway::Schema {
        fields: &next_fields,
        reserved_tags: &[],
        ..previous
    };
    assert_eq!(
        validate_evolution(&previous, &next),
        Err(hiway::SchemaError::ReservedTagReused(7))
    );
}

#[test]
fn envelope_header_round_trips_without_allocating_payload_storage() {
    let envelope = WireEnvelope::new(
        EventId::from_name("pipewise::Progress"),
        WireMajor(3),
        hiway::SchemaRevision(9),
        EventMetadata::new(hiway::OriginId::new([8; 16]), 12, 99),
        &[4, 5, 6],
    );
    let header = EnvelopeHeader::from_envelope(&envelope).unwrap();
    let mut bytes = [0; hiway::ENVELOPE_HEADER_BYTES];
    header.encode(&mut bytes).unwrap();
    assert_eq!(EnvelopeHeader::decode(&bytes), Ok(header));
    assert_eq!(
        EnvelopeHeader::decode(&bytes[..3]),
        Err(WireError::Truncated)
    );
    assert_eq!(
        EnvelopeHeader::for_event::<pipewise::Progress>(EventMetadata::default(), 3)
            .unwrap()
            .event,
        pipewise::Progress::ID
    );
}

#[test]
fn envelope_header_rejects_values_reserved_for_absent_metadata() {
    let ttl = EventMetadata {
        ttl: Some(u32::MAX),
        ..EventMetadata::default()
    };
    let ttl_header = EnvelopeHeader::for_event::<pipewise::Progress>(ttl, 0).unwrap();
    let mut bytes = [0; hiway::ENVELOPE_HEADER_BYTES];
    assert_eq!(ttl_header.encode(&mut bytes), Err(WireError::InvalidHeader));

    let sequence = EventMetadata {
        fabric_sequence: Some(u64::MAX),
        ..EventMetadata::default()
    };
    let sequence_header = EnvelopeHeader::for_event::<pipewise::Progress>(sequence, 0).unwrap();
    assert_eq!(
        sequence_header.encode(&mut bytes),
        Err(WireError::InvalidHeader)
    );
}
