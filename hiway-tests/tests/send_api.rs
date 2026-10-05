use std::{
    cell::RefCell,
    future::Future,
    ops::Deref,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use hiway::*;

#[hiway::events]
enum Events {
    Job(u32),
    Local(Rc<RefCell<u32>>),
}

#[hiway::graph(name = (events::Job, 1, 1, 1))]
struct JobGraph;

#[hiway::port(send(events::Job), required(events::Job))]
struct JobPort;

fn assert_send<T: Send>(_: T) {}

fn assert_sender_future_send<E, S>(sender: &S, payload: E::Payload)
where
    E: EventSpec,
    S: SendEventSender<E>,
{
    assert_send(sender.send_future(payload));
}

fn assert_receiver_future_send<E, R>(receiver: &R)
where
    E: EventSpec,
    R: SendEventReceiver<E>,
{
    assert_send(receiver.event_recv_future());
}

fn assert_port_futures_send<'a, E, P>(port: &'a P, payload: E::Payload)
where
    E: EventSpec,
    P: EventPort<E> + SendEventReceiver<E>,
    P::Sender: SendEventSender<E> + 'a,
{
    assert_send(port.publish_send::<E>(EventValue::new(payload)));
    assert_send(port.recv_send::<E>());
}

fn assert_data_value<V>(item: StreamItem<V>, expected: u32)
where
    V: Deref<Target = u32>,
{
    match item {
        StreamItem::Data { value, .. } => assert_eq!(*value, expected),
        StreamItem::Gap { .. } => panic!("required receiver returned a gap"),
    }
}

#[test]
fn send_future_waits_for_required_reader_capacity() {
    let stream = StaticStream::<events::Job, 1, 1, 1>::new();
    let sender = stream.sender();
    let receiver = stream.subscribe(SubscriptionRole::Required).unwrap();

    assert_sender_future_send::<events::Job, _>(&sender, 2);
    assert_receiver_future_send::<events::Job, _>(&receiver);

    sender.send_now(1).unwrap();
    let mut send = std::pin::pin!(sender.send_future(2));
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(send.as_mut().poll(&mut context), Poll::Pending));
    assert_data_value(receiver.event_try_recv().unwrap().unwrap(), 1);
    assert!(matches!(
        send.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    assert_data_value(receiver.event_try_recv().unwrap().unwrap(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn generated_port_send_and_receive_keep_single_event_inference() {
    let stream = StaticStream::<events::Job, 1, 1, 1>::new();
    let graph = JobGraph::new(&stream);
    let port = JobPort::bind(&graph).unwrap();

    assert_port_futures_send::<events::Job, _>(&port, 7);
    assert!(port.publish_send(events::Job(7)).await.is_ok());
    assert_data_value(port.recv_send().await.unwrap(), 7);
}

struct LocalQueue<E: EventSpec> {
    next_sequence: u64,
    item: Option<(u64, Rc<E::Payload>)>,
}

struct LocalSender<E: EventSpec> {
    queue: Rc<RefCell<LocalQueue<E>>>,
}

impl<E: EventSpec> Clone for LocalSender<E> {
    fn clone(&self) -> Self {
        Self {
            queue: Rc::clone(&self.queue),
        }
    }
}

struct LocalReceiver<E: EventSpec> {
    queue: Rc<RefCell<LocalQueue<E>>>,
}

struct LocalPrepared<E: EventSpec> {
    queue: Rc<RefCell<LocalQueue<E>>>,
    payload: Option<E::Payload>,
}

fn store_local<E: EventSpec>(queue: &Rc<RefCell<LocalQueue<E>>>, payload: E::Payload) {
    let mut queue = queue.borrow_mut();
    let sequence = queue.next_sequence;
    queue.next_sequence += 1;
    queue.item = Some((sequence, Rc::new(payload)));
}

impl<E: EventSpec> PreparedSend<E> for LocalPrepared<E> {
    fn try_send(self) -> Result<(), TrySendError<Self>> {
        store_local(
            &self.queue,
            self.payload.expect("prepared payload is present"),
        );
        Ok(())
    }

    fn into_inner(mut self) -> E::Payload {
        self.payload.take().expect("prepared payload is present")
    }
}

impl<E: EventSpec> EventSender<E> for LocalSender<E> {
    type Prepared<'a>
        = LocalPrepared<E>
    where
        Self: 'a;

    fn prepare(&self, payload: E::Payload) -> Result<Self::Prepared<'_>, TrySendError<E::Payload>> {
        Ok(LocalPrepared {
            queue: Rc::clone(&self.queue),
            payload: Some(payload),
        })
    }

    fn send_now(&self, payload: E::Payload) -> Result<(), TrySendError<E::Payload>> {
        store_local(&self.queue, payload);
        Ok(())
    }

    fn send(
        &self,
        payload: E::Payload,
    ) -> impl std::future::Future<Output = Result<(), SendError<E::Payload>>> {
        let queue = Rc::clone(&self.queue);
        async move {
            store_local(&queue, payload);
            Ok(())
        }
    }
}

impl<E: EventSpec> Port for LocalReceiver<E> {}

impl<E: EventSpec> EventReceiver<E> for LocalReceiver<E> {
    type Value = Rc<E::Payload>;

    fn event_try_recv(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError> {
        Ok(self
            .queue
            .borrow_mut()
            .item
            .take()
            .map(|(sequence, value)| StreamItem::Data { sequence, value }))
    }

    fn event_recv_now(&self) -> Result<Option<StreamItem<Self::Value>>, ReceiveError> {
        self.event_try_recv()
    }

    async fn event_recv(&self) -> Result<StreamItem<Self::Value>, ReceiveError> {
        Ok(self
            .event_recv_now()?
            .expect("local fixture receives after publication"))
    }
}

struct LocalFactory {
    job_queue: Rc<RefCell<LocalQueue<events::Job>>>,
    local_queue: Rc<RefCell<LocalQueue<events::Local>>>,
}

impl LocalFactory {
    fn new() -> Self {
        Self {
            job_queue: Rc::new(RefCell::new(LocalQueue {
                next_sequence: 0,
                item: None,
            })),
            local_queue: Rc::new(RefCell::new(LocalQueue {
                next_sequence: 0,
                item: None,
            })),
        }
    }
}

impl OwnedPortBinding<events::Job> for LocalFactory {
    type Sender = LocalSender<events::Job>;
    type Receiver = LocalReceiver<events::Job>;

    fn sender_owned(&self) -> Result<Self::Sender, TopicError> {
        Ok(LocalSender {
            queue: Rc::clone(&self.job_queue),
        })
    }

    fn subscribe_owned(&self, _role: SubscriptionRole) -> Result<Self::Receiver, TopicError> {
        Ok(LocalReceiver {
            queue: Rc::clone(&self.job_queue),
        })
    }
}

impl OwnedPortBinding<events::Local> for LocalFactory {
    type Sender = LocalSender<events::Local>;
    type Receiver = LocalReceiver<events::Local>;

    fn sender_owned(&self) -> Result<Self::Sender, TopicError> {
        Ok(LocalSender {
            queue: Rc::clone(&self.local_queue),
        })
    }

    fn subscribe_owned(&self, _role: SubscriptionRole) -> Result<Self::Receiver, TopicError> {
        Ok(LocalReceiver {
            queue: Rc::clone(&self.local_queue),
        })
    }
}

#[hiway::port(
    factory = LocalFactory,
    send(events::Job),
    required(events::Job)
)]
struct LocalPort;

#[hiway::port(
    factory = LocalFactory,
    send(events::Local),
    required(events::Local)
)]
struct LocalPayloadPort;

#[tokio::test(flavor = "current_thread")]
async fn ordinary_local_api_accepts_non_send_endpoints_and_payloads() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let factory = LocalFactory::new();
            let port = LocalPort::bind(&factory).unwrap();

            assert!(port.publish(events::Job(31)).await.is_ok());
            assert_data_value(port.recv().await.unwrap(), 31);

            let local_port = LocalPayloadPort::bind(&factory).unwrap();
            let payload = Rc::new(RefCell::new(31));

            assert!(local_port
                .publish(events::Local(Rc::clone(&payload)))
                .await
                .is_ok());
            match local_port.recv().await.unwrap() {
                StreamItem::Data { value, .. } => {
                    let value = &*value;
                    assert!(Rc::ptr_eq(&payload, value));
                    *value.borrow_mut() += 1;
                }
                StreamItem::Gap { .. } => panic!("required receiver returned a gap"),
            }
            assert_eq!(*payload.borrow(), 32);
        })
        .await;
}

#[cfg(feature = "std")]
mod dynamic {
    use super::*;

    #[hiway::port(factory = Grant, send(events::Job), required(events::Job))]
    struct GrantedPort;

    fn spawn_send<E, S>(
        sender: S,
        payload: E::Payload,
    ) -> tokio::task::JoinHandle<Result<(), SendError<E::Payload>>>
    where
        E: EventSpec,
        E::Payload: Send + 'static,
        S: SendEventSender<E> + Send + Sync + 'static,
    {
        tokio::spawn(async move { sender.send_future(payload).await })
    }

    fn spawn_receive<R>(receiver: R) -> tokio::task::JoinHandle<Result<u32, ReceiveError>>
    where
        R: SendEventReceiver<events::Job> + Send + Sync + 'static,
    {
        tokio::spawn(async move {
            match receiver.event_recv_future().await? {
                StreamItem::Data { value, .. } => Ok(*value),
                StreamItem::Gap { .. } => panic!("required receiver returned a gap"),
            }
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn owned_dynamic_endpoints_spawn_generically_and_reject_after_revoke() {
        let fabric = DynamicFabric::new();
        fabric
            .create_stream::<events::Job>(StreamConfig {
                capacity: 1,
                subscribers: 1,
                waiters: 1,
            })
            .unwrap();

        let grant = fabric
            .grant(
                &[Permission::new::<events::Job>(
                    Rights::PUBLISH | Rights::OBSERVE | Rights::REQUIRED,
                )
                .with_limits(StreamLimits {
                    retained_items: 1,
                    subscriptions: 1,
                    waiters: 1,
                })],
                Limits {
                    streams: 1,
                    retained_items: 1,
                    subscriptions: 1,
                    waiters: 1,
                    ..Limits::ZERO
                },
            )
            .unwrap();
        let sender = grant.sender::<events::Job>().unwrap();
        let receiver = grant
            .subscribe::<events::Job>(SubscriptionRole::Required)
            .unwrap();

        let send = spawn_send::<events::Job, _>(sender.clone(), 44);
        let receive = spawn_receive(receiver);
        assert!(send.await.unwrap().is_ok());
        assert_eq!(receive.await.unwrap().unwrap(), 44);

        let port = GrantedPort::bind(&grant).unwrap();
        assert_port_futures_send::<events::Job, _>(&port, 46);
        assert!(port.publish_send(events::Job(46)).await.is_ok());
        assert_data_value(port.recv_send().await.unwrap(), 46);

        grant.revoke().await.unwrap();
        assert!(port.publish_send(events::Job(45)).await.is_err());
        assert!(sender.send_future(45).await.is_err());
    }
}
