#![cfg(not(loom))]

use std::{
    cell::Cell,
    future::Future,
    sync::mpsc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use hiway::{
    events, graph, port, publish, validate_evolution, validate_schema, AllocTransformSubscriber,
    Broker, ClientId, DeliveryPolicy, DirectRoute, DynamicSender, EnvelopeHeader, EventId,
    EventMetadata, EventPort, EventReceiver, EventSpec, EventValue, FieldKind, FieldPresence,
    FieldSpec, HeaplessRoute, Hiway, Inbox, MappedTarget, OwnedEndpoint, OwnedPortBinding,
    OwnedStorage, PortBinding, PortError, PortExt, PushResult, Receiver, SendError, SharedInbox,
    Target, Transform, TransformOp, TrySendError, WireEnvelope, WireError, WireMajor,
};
#[cfg(unix)]
use hiway::{RouteKey, UnixFrame, UnixLink};

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProgressData(u32);

#[events]
enum Pipewise {
    Started,
    Stopped,
    Progress(ProgressData),
    AlternativeProgress(ProgressData),
    Unrelated(ProgressData),
}

type TestBus = hiway::DynamicFabric;

struct WrongCapacityBus(hiway::DynamicFabric);

impl OwnedStorage<ProgressData, 1> for WrongCapacityBus {
    type Receiver = SharedInbox<ProgressData, 2>;

    fn new_receiver() -> Self::Receiver {
        SharedInbox::new()
    }
}

impl OwnedPortBinding<pipewise::Progress> for WrongCapacityBus {
    type Sender = DynamicSender<pipewise::Progress>;
    type Subscription = ();

    fn sender_owned(&self) -> Result<Self::Sender, hiway::TopicError> {
        self.0.sender()
    }

    fn subscribe_owned<T, H>(
        &self,
        _target: &MappedTarget<ProgressData, T, H>,
        _policy: DeliveryPolicy,
    ) -> Result<Self::Subscription, hiway::TopicError>
    where
        T: Send + 'static,
        H: Target<T> + Clone + Send + Sync + 'static,
    {
        Ok(())
    }
}

#[port(
    factory = TestBus,
    send(pipewise::Started),
    recv(pipewise::Progress, pipewise::AlternativeProgress)
)]
pub struct WorkerPort;

#[port(factory = TestBus, capacity = 1usize, recv(pipewise::Progress))]
pub struct SmallPort;

#[port(
    factory = TestBus,
    capacity = 1,
    send(pipewise::Progress),
    recv(pipewise::Progress)
)]
struct CounterPort;

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

impl<P> Counter<P>
where
    P: EventPort<pipewise::Progress> + EventReceiver<pipewise::Progress>,
{
    fn publish_value(&self) -> Result<(), TrySendError<ProgressData>> {
        self.port
            .try_publish(pipewise::Progress(ProgressData(self.value)))
    }

    async fn publish_value_async(&self) -> Result<(), SendError> {
        self.port
            .publish(pipewise::Progress(ProgressData(self.value)))
            .await
    }

    fn try_recv(&self) -> Option<ProgressData> {
        self.port.try_recv::<pipewise::Progress>()
    }
}

#[port(
    factory = TestBus,
    recv(pipewise::Started, pipewise::Stopped)
)]
pub struct SignalPort;

#[graph(
    started = (pipewise::Started, 1),
    progress = (pipewise::Progress, 2),
    alternative = (pipewise::AlternativeProgress, 2),
)]
pub struct TestGraph;

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

struct CountedEvent;

#[derive(Clone)]
struct NonSync(Cell<u32>);

struct NonSyncEvent;

impl EventSpec for NonSyncEvent {
    type Payload = NonSync;
    const ID: EventId = EventId::from_name("test::non_sync");
}

struct AddSeven;

impl TransformOp<ProgressData> for AddSeven {
    type Output = ProgressData;

    async fn apply(&self, input: ProgressData) -> Self::Output {
        ProgressData(input.0 + 7)
    }
}

struct Counted {
    value: u8,
    clones: Arc<AtomicUsize>,
}

struct CountingWake {
    calls: Arc<AtomicUsize>,
}

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

struct ReentrantWake {
    inbox: SharedInbox<ProgressData, 1>,
    calls: Arc<AtomicUsize>,
}

impl Wake for ReentrantWake {
    fn wake(self: Arc<Self>) {
        let _ = self.inbox.len();
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

struct RejectTarget;

impl Target<ProgressData> for RejectTarget {
    fn can_accept(&self, _policy: DeliveryPolicy) -> bool {
        true
    }

    fn push(&self, _payload: ProgressData, _policy: DeliveryPolicy) -> PushResult {
        PushResult::Full
    }

    fn register_waker(&self, _waker: &Waker) -> bool {
        true
    }

    fn wake_waiters(&self) {}
}

impl Clone for Counted {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::Relaxed);
        Self {
            value: self.value,
            clones: Arc::clone(&self.clones),
        }
    }
}

impl EventSpec for CountedEvent {
    type Payload = Counted;
    const ID: EventId = EventId::from_name("test::counted");
}

#[events(wire_major = 2, schema_revision = 3)]
enum Versioned {
    Value(u32),
}

#[tokio::test]
async fn distinct_specs_use_independent_typed_port_channels() {
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

    let hiway = Hiway::new();
    let worker = WorkerPort::bind(hiway.fabric()).unwrap();
    let signals = SignalPort::bind(hiway.fabric()).unwrap();

    publish(&worker, pipewise::Started).await.unwrap();
    hiway
        .alloc_sender::<pipewise::Stopped>()
        .unwrap()
        .send(())
        .await
        .unwrap();
    assert_eq!(signals.try_recv::<pipewise::Started>(), Some(()));
    assert_eq!(signals.try_recv_stopped(), Some(()));

    hiway
        .alloc_sender::<pipewise::Unrelated>()
        .unwrap()
        .try_send(ProgressData(99))
        .unwrap();
    assert!(worker.try_recv::<pipewise::Progress>().is_none());

    hiway
        .alloc_sender::<pipewise::Progress>()
        .unwrap()
        .send(ProgressData(7))
        .await
        .unwrap();
    hiway
        .alloc_sender::<pipewise::AlternativeProgress>()
        .unwrap()
        .send(ProgressData(8))
        .await
        .unwrap();

    assert_eq!(
        worker.recv::<pipewise::Progress>().await,
        Some(ProgressData(7))
    );
    assert_eq!(
        worker.try_recv_alternative_progress(),
        Some(ProgressData(8))
    );
    assert!(worker.try_recv_progress().is_none());
}

#[tokio::test]
async fn injected_port_uses_typed_event_values_and_drops_subscriptions() {
    let hiway = Hiway::new();
    let first = SmallPort::bind(hiway.fabric()).unwrap();
    let second = SmallPort::bind(hiway.fabric()).unwrap();
    let sender = hiway.alloc_sender::<pipewise::Progress>().unwrap();

    sender.try_send(ProgressData(1)).unwrap();
    assert_eq!(first.try_recv_progress(), Some(ProgressData(1)));
    assert_eq!(second.try_recv_progress(), Some(ProgressData(1)));

    sender.try_send(ProgressData(2)).unwrap();

    let worker = WorkerPort::bind(hiway.fabric()).unwrap();
    worker.try_publish(pipewise::Started).unwrap();
    worker.try_publish_started(()).unwrap();
    worker.publish(pipewise::Started).await.unwrap();
    publish(&worker, pipewise::Started).await.unwrap();
    drop(first);

    assert_eq!(second.try_recv_progress(), Some(ProgressData(2)));
    sender.try_send(ProgressData(3)).unwrap();
    assert_eq!(second.try_recv_progress(), Some(ProgressData(3)));

    let (detached_sender, detached_port) = {
        let fabric = hiway::DynamicFabric::new();
        let sender = fabric.sender::<pipewise::Progress>().unwrap();
        let port = SmallPort::bind(&fabric).unwrap();
        (sender, port)
    };
    detached_sender.try_send(ProgressData(4)).unwrap();
    assert_eq!(detached_port.try_recv_progress(), Some(ProgressData(4)));
}

#[tokio::test]
async fn developer_constructor_owns_state_and_accepts_an_injected_port() {
    let hiway = Hiway::new();
    let mut counter = Counter::new(CounterPort::bind(hiway.fabric()).unwrap(), 7);

    counter.increment();
    counter.publish_value().unwrap();
    assert_eq!(counter.try_recv(), Some(ProgressData(8)));
    counter.publish_value_async().await.unwrap();
    assert_eq!(counter.try_recv(), Some(ProgressData(8)));
}

#[test]
fn static_graph_composes_one_heapless_fabric_per_event() {
    let inbox = Inbox::<ProgressData, 4, 2>::new();
    let target = MappedTarget::new(inbox.handle(), |value| value);
    let started = HeaplessRoute::<pipewise::Started, 1>::new();
    let progress = HeaplessRoute::<pipewise::Progress, 2>::new();
    let alternative = HeaplessRoute::<pipewise::AlternativeProgress, 2>::new();
    let graph = TestGraph::new(&started, &progress, &alternative);
    let _subscription =
        <TestGraph<'_, '_> as PortBinding<'_, pipewise::Progress>>::subscribe_mapped(
            &graph,
            &target,
            DeliveryPolicy::Reliable,
        )
        .unwrap();

    graph
        .sender::<pipewise::Progress>()
        .unwrap()
        .try_send(ProgressData(7))
        .unwrap();
    assert_eq!(inbox.try_recv(), Some(ProgressData(7)));
}

#[tokio::test]
async fn subscriber_transforms_are_composable_and_input_scoped() {
    let hiway = Hiway::new();
    let input = SharedInbox::<ProgressData>::new();
    let storage = hiway::TransformStorage::new(input.clone());
    let output = SharedInbox::<ProgressData, 1>::new();
    let output_target = MappedTarget::new(output.target(), |value: ProgressData| value);
    let _output_subscription = hiway
        .alloc_subscribe_mapped::<pipewise::AlternativeProgress, _, _>(
            &output_target,
            DeliveryPolicy::Reliable,
        )
        .unwrap();

    let transform = Transform::new(|ProgressData(value)| ProgressData(value + 1))
        .then(AddSeven)
        .then(Transform::new_async(|ProgressData(value)| async move {
            ProgressData(value * 2)
        }));
    let runner = AllocTransformSubscriber::<
        _,
        pipewise::Progress,
        pipewise::AlternativeProgress,
        _,
        _,
    >::new(hiway.fabric(), &storage, transform)
    .unwrap();
    let exercise = async {
        hiway
            .alloc_sender::<pipewise::Unrelated>()
            .unwrap()
            .try_send(ProgressData(99))
            .unwrap();
        assert!(output.try_recv().is_none());

        hiway
            .alloc_sender::<pipewise::Progress>()
            .unwrap()
            .send(ProgressData(3))
            .await
            .unwrap();
        let received = output.recv().await;
        input.close();
        received
    };
    let (run_result, received) = tokio::join!(runner.run(), exercise);
    run_result.unwrap();
    assert_eq!(received, Some(ProgressData(22)));
    assert!(output.try_recv().is_none());
}

#[test]
fn sender_handles_do_not_need_registry_access_after_resolution() {
    let hiway = Hiway::new();
    let inbox = SharedInbox::<(), 1>::new();
    let target = MappedTarget::new(inbox.target(), |value: ()| value);
    let _subscription = hiway
        .alloc_subscribe_mapped::<pipewise::Started, _, _>(&target, DeliveryPolicy::Reliable)
        .unwrap();
    let sender = hiway.alloc_sender::<pipewise::Started>().unwrap();
    drop(hiway);
    assert_eq!(sender.id(), pipewise::Started::ID);
    sender.try_send(()).unwrap();
    assert_eq!(inbox.try_recv(), Some(()));
}

#[test]
fn generated_port_capacity_is_bounded() {
    let hiway = Hiway::new();
    let small = SmallPort::bind(hiway.fabric()).unwrap();
    let sender = hiway.alloc_sender::<pipewise::Progress>().unwrap();
    sender.try_send(ProgressData(1)).unwrap();
    assert!(matches!(
        sender.try_send(ProgressData(2)),
        Err(TrySendError::Full(ProgressData(2)))
    ));
    assert_eq!(small.try_recv_progress(), Some(ProgressData(1)));
}

#[test]
fn generated_port_capacity_is_checked_against_fixed_receivers() {
    let factory = WrongCapacityBus(hiway::DynamicFabric::new());
    let error = OwnedEndpoint::<WrongCapacityBus, pipewise::Progress, 1>::bind(&factory)
        .err()
        .unwrap();
    assert_eq!(
        error,
        PortError::ReceiverCapacity {
            expected: 1,
            actual: 2,
        }
    );
}

#[test]
fn fanout_clones_only_for_nonfinal_subscribers() {
    let hiway = Hiway::new();
    let first = SharedInbox::<Counted, 1>::new();
    let second = SharedInbox::<Counted, 1>::new();
    let third = SharedInbox::<Counted, 1>::new();
    let first_target = MappedTarget::new(first.target(), |value: Counted| value);
    let second_target = MappedTarget::new(second.target(), |value: Counted| value);
    let third_target = MappedTarget::new(third.target(), |value: Counted| value);
    let _first_subscription = hiway
        .alloc_subscribe_mapped::<CountedEvent, _, _>(&first_target, DeliveryPolicy::DropNewest)
        .unwrap();
    let _second_subscription = hiway
        .alloc_subscribe_mapped::<CountedEvent, _, _>(&second_target, DeliveryPolicy::DropNewest)
        .unwrap();
    let _third_subscription = hiway
        .alloc_subscribe_mapped::<CountedEvent, _, _>(&third_target, DeliveryPolicy::DropNewest)
        .unwrap();
    let clones = Arc::new(AtomicUsize::new(0));

    hiway
        .alloc_sender::<CountedEvent>()
        .unwrap()
        .try_send(Counted {
            value: 4,
            clones: Arc::clone(&clones),
        })
        .unwrap();

    assert_eq!(clones.load(Ordering::Relaxed), 2);
    assert_eq!(first.try_recv().map(|event| event.value), Some(4));
    assert_eq!(second.try_recv().map(|event| event.value), Some(4));
    assert_eq!(third.try_recv().map(|event| event.value), Some(4));
}

#[test]
fn allocating_backend_only_requires_send_payloads() {
    let hiway = Hiway::new();
    let inbox = SharedInbox::<NonSync, 1>::new();
    let target = MappedTarget::new(inbox.target(), |value: NonSync| value);
    let _subscription = hiway
        .alloc_subscribe_mapped::<NonSyncEvent, _, _>(&target, DeliveryPolicy::Reliable)
        .unwrap();

    hiway
        .alloc_sender::<NonSyncEvent>()
        .unwrap()
        .try_send(NonSync(Cell::new(9)))
        .unwrap();
    assert_eq!(inbox.try_recv().map(|value| value.0.get()), Some(9));
}

#[test]
fn concurrent_subscription_updates_keep_both_targets() {
    let fabric = Arc::new(hiway::DynamicFabric::new());
    let first = SharedInbox::<ProgressData, 1>::new();
    let second = SharedInbox::<ProgressData, 1>::new();
    let first_fabric = Arc::clone(&fabric);
    let first_inbox = first.clone();
    let first_subscription = std::thread::spawn(move || {
        let target = MappedTarget::new(first_inbox.target(), |value: ProgressData| value);
        first_fabric
            .subscribe_mapped::<pipewise::Progress, _, _>(&target, DeliveryPolicy::DropNewest)
            .unwrap()
    });
    let second_fabric = Arc::clone(&fabric);
    let second_inbox = second.clone();
    let second_subscription = std::thread::spawn(move || {
        let target = MappedTarget::new(second_inbox.target(), |value: ProgressData| value);
        second_fabric
            .subscribe_mapped::<pipewise::Progress, _, _>(&target, DeliveryPolicy::DropNewest)
            .unwrap()
    });
    let first_subscription = first_subscription.join().unwrap();
    let second_subscription = second_subscription.join().unwrap();

    fabric
        .sender::<pipewise::Progress>()
        .unwrap()
        .try_send(ProgressData(5))
        .unwrap();
    assert!(first.try_recv().is_some());
    assert!(second.try_recv().is_some());
    drop((first_subscription, second_subscription));
}

#[test]
fn registry_rejects_one_id_with_two_rust_event_specs() {
    let hiway = Hiway::new();
    hiway.alloc_topic::<ConflictingU8>().unwrap();
    assert!(matches!(
        hiway.alloc_topic::<ConflictingU16>(),
        Err(hiway::TopicError::TypeMismatch(_))
    ));
}

#[tokio::test]
async fn reliable_capacity_is_owned_by_the_subscribed_inbox() {
    let hiway = Hiway::new();
    let inbox = SharedInbox::<ProgressData, 1>::new();
    let target = MappedTarget::new(inbox.target(), |value: ProgressData| value);
    let _subscription = hiway
        .alloc_subscribe_mapped::<pipewise::Progress, _, _>(&target, DeliveryPolicy::Reliable)
        .unwrap();
    let sender = hiway.alloc_sender::<pipewise::Progress>().unwrap();

    sender.try_send(ProgressData(1)).unwrap();
    let error = sender.try_send(ProgressData(2)).unwrap_err();
    assert!(matches!(error, TrySendError::Full(ProgressData(2))));

    let sending = sender.send(ProgressData(2));
    tokio::pin!(sending);
    tokio::select! {
        result = &mut sending => panic!("send completed while the reliable inbox was full: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(inbox.try_recv(), Some(ProgressData(1)));
    sending.await.unwrap();
    assert_eq!(inbox.try_recv(), Some(ProgressData(2)));
}

#[tokio::test]
async fn removing_a_full_reliable_subscription_releases_pending_senders() {
    let hiway = Hiway::new();
    let inbox = SharedInbox::<ProgressData, 1>::new();
    let target = MappedTarget::new(inbox.target(), |value: ProgressData| value);
    let subscription = hiway
        .alloc_subscribe_mapped::<pipewise::Progress, _, _>(&target, DeliveryPolicy::Reliable)
        .unwrap();
    let sender = hiway.alloc_sender::<pipewise::Progress>().unwrap();
    sender.try_send(ProgressData(1)).unwrap();

    let sending = sender.send(ProgressData(2));
    tokio::pin!(sending);
    tokio::select! {
        result = &mut sending => panic!("send completed while its only reliable inbox was full: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    drop(subscription);
    sending.await.unwrap();
}

#[test]
fn bounded_waiters_report_exhaustion_and_cancelled_senders_release_slots() {
    let inbox = Inbox::<ProgressData, 1, 1>::new();
    let route = DirectRoute::<pipewise::Progress, _>::new(inbox.handle(), DeliveryPolicy::Reliable);
    let sender = route.sender();
    sender.try_send(ProgressData(1)).unwrap();

    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let first_waker = Waker::from(Arc::new(CountingWake {
        calls: Arc::clone(&first_calls),
    }));
    let second_waker = Waker::from(Arc::new(CountingWake {
        calls: Arc::clone(&second_calls),
    }));
    let mut first = sender.send(ProgressData(2));
    let mut second = sender.send(ProgressData(3));
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);

    assert!(matches!(
        Future::poll(std::pin::Pin::new(&mut first), &mut first_context),
        Poll::Pending
    ));
    assert!(matches!(
        Future::poll(std::pin::Pin::new(&mut second), &mut second_context),
        Poll::Ready(Err(SendError::WaitersFull))
    ));

    drop(first);
    let mut third = sender.send(ProgressData(4));
    assert!(matches!(
        Future::poll(std::pin::Pin::new(&mut third), &mut first_context),
        Poll::Pending
    ));
    assert_eq!(first_calls.load(Ordering::Relaxed), 0);
    let _ = inbox.try_recv();
    assert_eq!(first_calls.load(Ordering::Relaxed), 1);
    assert!(matches!(
        Future::poll(std::pin::Pin::new(&mut third), &mut first_context),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn dynamic_waiter_wakeup_does_not_hold_the_inbox_mutex() {
    let inbox = SharedInbox::<ProgressData, 1>::new();
    let route = DirectRoute::<pipewise::Progress, _>::new(inbox.target(), DeliveryPolicy::Reliable);
    let sender = route.sender();
    sender.try_send(ProgressData(1)).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(ReentrantWake {
        inbox: inbox.clone(),
        calls: Arc::clone(&calls),
    }));
    let mut sending = sender.send(ProgressData(2));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Future::poll(std::pin::Pin::new(&mut sending), &mut context),
        Poll::Pending
    ));

    let (done_sender, done_receiver) = mpsc::channel();
    let pop_inbox = inbox.clone();
    let pop_thread = thread::spawn(move || {
        let _ = pop_inbox.try_recv();
        done_sender.send(()).unwrap();
    });
    assert!(done_receiver.recv_timeout(Duration::from_secs(1)).is_ok());
    pop_thread.join().unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn target_push_failures_are_not_reported_as_success() {
    let target = RejectTarget;
    let target: &(dyn Target<ProgressData> + Sync) = &target;
    let route = DirectRoute::<pipewise::Progress, _>::new(target, DeliveryPolicy::Reliable);
    let sender = route.sender();
    assert!(matches!(
        sender.try_send(ProgressData(1)),
        Err(TrySendError::PushFailed)
    ));
}

#[test]
fn drop_policies_are_local_to_each_inbox() {
    let hiway = Hiway::new();
    let newest = SharedInbox::<ProgressData, 1>::new();
    let oldest = SharedInbox::<ProgressData, 1>::new();
    let latest = SharedInbox::<ProgressData, 1>::new();
    let newest_target = MappedTarget::new(newest.target(), |value: ProgressData| value);
    let oldest_target = MappedTarget::new(oldest.target(), |value: ProgressData| value);
    let latest_target = MappedTarget::new(latest.target(), |value: ProgressData| value);
    let _newest_subscription = hiway
        .alloc_subscribe_mapped::<pipewise::Progress, _, _>(
            &newest_target,
            DeliveryPolicy::DropNewest,
        )
        .unwrap();
    let _oldest_subscription = hiway
        .alloc_subscribe_mapped::<pipewise::Progress, _, _>(
            &oldest_target,
            DeliveryPolicy::DropOldest,
        )
        .unwrap();
    let _latest_subscription = hiway
        .alloc_subscribe_mapped::<pipewise::Progress, _, _>(&latest_target, DeliveryPolicy::Latest)
        .unwrap();
    let sender = hiway.alloc_sender::<pipewise::Progress>().unwrap();

    sender.try_send(ProgressData(1)).unwrap();
    sender.try_send(ProgressData(2)).unwrap();

    assert_eq!(newest.try_recv(), Some(ProgressData(1)));
    assert_eq!(oldest.try_recv(), Some(ProgressData(2)));
    assert_eq!(latest.try_recv(), Some(ProgressData(2)));
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
fn opaque_broker_routes_once_per_client_and_deduplicates_origin_sequences() {
    let route = RouteKey::for_event::<pipewise::Progress>();
    let mut broker = Broker::new();
    broker.subscribe(route, ClientId(1));
    broker.subscribe(route, ClientId(2));
    let metadata = EventMetadata::new(hiway::OriginId::new([3; 16]), 7, 42);
    let payload = [1, 2, 3];
    let envelope = WireEnvelope::new(
        route.event,
        route.wire_major,
        hiway::SchemaRevision(1),
        metadata,
        &payload,
    );

    let mut deliveries = Vec::new();
    assert_eq!(
        broker.route(envelope, |client, routed| {
            assert!(std::ptr::eq(routed.payload.as_ptr(), payload.as_ptr()));
            assert_eq!(routed.metadata.fabric_sequence, Some(1));
            deliveries.push(client);
        }),
        Ok(2)
    );
    assert_eq!(deliveries, [ClientId(1), ClientId(2)]);
    assert_eq!(broker.route(envelope, |_, _| {}), Ok(0));

    let mut expired = envelope;
    expired.metadata.ttl = Some(0);
    assert_eq!(
        broker.route(expired, |_, _| {}),
        Err(hiway::BrokerError::Expired)
    );
}

#[test]
fn broker_origin_marks_are_bounded_and_explicitly_reclaimable() {
    let route = RouteKey::for_event::<pipewise::Progress>();
    let mut broker = Broker::with_seen_origin_capacity(1);
    let first_origin = hiway::OriginId::new([1; 16]);
    let second_origin = hiway::OriginId::new([2; 16]);
    let payload = [0];
    let first = WireEnvelope::new(
        route.event,
        route.wire_major,
        hiway::SchemaRevision(1),
        EventMetadata::new(first_origin, 1, 0),
        &payload,
    );
    let second = WireEnvelope::new(
        route.event,
        route.wire_major,
        hiway::SchemaRevision(1),
        EventMetadata::new(second_origin, 1, 0),
        &payload,
    );
    broker.route(first, |_, _| {}).unwrap();
    assert_eq!(broker.seen_origin_count(), 1);
    assert_eq!(
        broker.route(second, |_, _| {}),
        Err(hiway::BrokerError::OriginCapacity)
    );
    assert!(broker.forget_origin(first_origin));
    broker.route(second, |_, _| {}).unwrap();
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

#[cfg(unix)]
#[test]
fn unix_frames_keep_routing_opaque_and_round_trip_payload_bytes() {
    let route = RouteKey {
        event: EventId::from_name("remote::value"),
        wire_major: WireMajor(4),
    };
    let header = EnvelopeHeader {
        event: route.event,
        wire_major: route.wire_major,
        schema_revision: hiway::SchemaRevision(2),
        metadata: EventMetadata::new(hiway::OriginId::ZERO, 1, 2),
        payload_len: 3,
    };
    let frame = UnixFrame::Publish {
        header,
        payload: vec![9, 8, 7],
    };
    let encoded = frame.encode().unwrap();
    assert_eq!(UnixFrame::decode(&encoded), Ok(frame));

    let (left, right) = std::os::unix::net::UnixStream::pair().unwrap();
    let mut sender = UnixLink::from_stream(left).unwrap();
    let mut receiver = UnixLink::from_stream(right).unwrap();
    sender.queue(&UnixFrame::Subscribe(route)).unwrap();
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let _ = hiway::Link::poll(&mut sender, &mut context);
    let _ = hiway::Link::poll(&mut receiver, &mut context);
    assert_eq!(receiver.take_received(), Some(UnixFrame::Subscribe(route)));
}
