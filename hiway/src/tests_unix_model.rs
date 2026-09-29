use super::*;
use crate::{DynamicReceiver, StreamLimits, TrySendError};

#[derive(Clone, Copy, Debug)]
enum Finish {
    Cancel,
    Disconnect,
    Revoke,
    Drain,
}

struct Model {
    history: Vec<u32>,
    retained: bool,
    observer: usize,
    required: Option<usize>,
    pending: Option<u32>,
    connected: bool,
}

impl Model {
    fn new(required: bool) -> Self {
        Self {
            history: Vec::new(),
            retained: false,
            observer: 0,
            required: required.then_some(0),
            pending: None,
            connected: true,
        }
    }

    fn forward(&mut self, value: u32) {
        assert!(self.connected && self.pending.is_none());
        if self
            .required
            .is_some_and(|cursor| cursor < self.history.len())
        {
            self.pending = Some(value);
        } else {
            self.accept(value);
        }
    }

    fn accept(&mut self, value: u32) {
        self.history.push(value);
        self.retained = true;
    }

    fn receive(&mut self, required: bool) -> Option<StreamItem<u32>> {
        let end = self.history.len();
        let first = end - usize::from(self.retained);
        let cursor = if required {
            self.required.as_mut().unwrap()
        } else {
            &mut self.observer
        };
        let item = if *cursor < first {
            let from = *cursor;
            *cursor = first;
            Some(StreamItem::Gap {
                from: from as u64,
                to: first as u64,
            })
        } else if *cursor < end {
            let sequence = *cursor;
            *cursor += 1;
            Some(StreamItem::Data {
                sequence: sequence as u64,
                value: self.history[sequence],
            })
        } else {
            None
        };
        if self.observer == end && self.required.is_none_or(|cursor| cursor == end) {
            self.retained = false;
        }
        item
    }

    fn terminate(&mut self) {
        self.connected = false;
        self.pending = None;
    }

    fn usage(&self) -> Limits {
        Limits {
            retained_items: usize::from(self.retained) + usize::from(self.connected),
            waiters: 2 * usize::from(self.connected) + usize::from(self.pending.is_some()),
            connections: 2 * usize::from(self.connected),
            bytes: 60 * usize::from(self.connected),
            ..Limits::ZERO
        }
    }
}

struct Session {
    parent: Grant,
    child: Grant,
    local: Grant,
    observer: DynamicReceiver<Number>,
    required: Option<DynamicReceiver<Number>>,
    link: Option<UnixLink>,
    data: UnixStream,
    control: Option<UnixStream>,
    wire_sequence: u64,
}

fn session_limits() -> Limits {
    Limits {
        streams: 1,
        retained_items: 2,
        waiters: 3,
        connections: 2,
        bytes: 60,
        ..Limits::ZERO
    }
}

impl Session {
    fn new(required: bool) -> Self {
        let (fabric, local) = setup(1);
        let permissions = [
            Permission::new::<Number>(Rights::PUBLISH).with_limits(StreamLimits {
                retained_items: 1,
                subscriptions: 0,
                waiters: 1,
            }),
        ];
        let parent = fabric
            .grant(
                &permissions,
                Limits {
                    grants: 1,
                    ..session_limits()
                },
            )
            .unwrap();
        let child = parent.restrict(&permissions, session_limits()).unwrap();
        let cloned = child.clone();
        drop(child);
        let observer = local
            .subscribe::<Number>(SubscriptionRole::Observer)
            .unwrap();
        let required = required.then(|| {
            local
                .subscribe::<Number>(SubscriptionRole::Required)
                .unwrap()
        });
        let (data, peer_data) = UnixStream::pair().unwrap();
        let (control, peer_control) = UnixStream::pair().unwrap();
        let link = UnixLink::import::<Number>(cloned.clone(), data, control, 4).unwrap();
        Self {
            parent,
            child: cloned,
            local,
            observer,
            required,
            link: Some(link),
            data: peer_data,
            control: Some(peer_control),
            wire_sequence: 0,
        }
    }

    fn check(&self, model: &Model) {
        assert_eq!(self.child.usage(), model.usage());
        assert_eq!(
            self.parent.usage(),
            Limits {
                grants: 1,
                ..session_limits()
            }
        );
        assert_eq!(
            self.child.stream_usage::<Number>().unwrap(),
            StreamLimits {
                retained_items: usize::from(model.retained),
                subscriptions: 0,
                waiters: usize::from(model.pending.is_some()),
            }
        );
    }

    async fn credit(&mut self, sequence: u64) {
        let mut credit = [0; 9];
        driving(self.link.as_mut().unwrap(), async {
            self.control
                .as_mut()
                .unwrap()
                .read_exact(&mut credit)
                .await
                .unwrap();
        })
        .await;
        assert_eq!(credit[0], 1);
        assert_eq!(
            u64::from_le_bytes(credit[1..].try_into().unwrap()),
            sequence
        );
    }

    async fn forward(&mut self, model: &mut Model, value: u32) {
        model.forward(value);
        self.data
            .write_all(&frame(self.wire_sequence, value))
            .await
            .unwrap();
        if model.pending.is_some() {
            driving(self.link.as_mut().unwrap(), async {
                while self.child.stream_usage::<Number>().unwrap().waiters == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            let mut byte = [0];
            assert!(matches!(self.control.as_ref().unwrap().try_read(&mut byte),
                Err(error) if error.kind() == ErrorKind::WouldBlock));
        } else {
            self.credit(self.wire_sequence).await;
        }
        self.wire_sequence += 1;
        self.check(model);
    }

    fn receive(&self, model: &mut Model, required: bool) -> bool {
        let expected = model.receive(required);
        let receiver = if required {
            self.required.as_ref().unwrap()
        } else {
            &self.observer
        };
        assert_eq!(
            receiver
                .recv_now()
                .unwrap()
                .map(|item| item.map(|value| *value)),
            expected
        );
        self.check(model);
        expected.is_some()
    }

    fn cancel_local_waiter(&self, model: &Model) {
        let sender = self.local.sender::<Number>().unwrap();
        let mut context = Context::from_waker(Waker::noop());
        {
            let mut pending = std::pin::pin!(sender.send(99));
            assert!(pending.as_mut().poll(&mut context).is_pending());
            assert_eq!(self.local.stream_usage::<Number>().unwrap().waiters, 1);
            self.check(model);
        }
        assert_eq!(self.local.stream_usage::<Number>().unwrap().waiters, 0);
        self.check(model);
    }

    async fn finish(&mut self, model: &mut Model, finish: Finish) {
        if matches!(finish, Finish::Drain) {
            if let Some(value) = model.pending {
                assert!(self.receive(model, true));
                self.credit(self.wire_sequence - 1).await;
                model.pending = None;
                model.accept(value);
            }
            self.check(model);
        }
        match finish {
            Finish::Cancel => {
                drop(self.link.take());
            }
            Finish::Disconnect | Finish::Drain => {
                drop(self.control.take());
                assert!(matches!(self.link.take().unwrap().await,
                    Err(IpcError::Io(error)) if error.kind() == ErrorKind::UnexpectedEof));
            }
            Finish::Revoke => {
                let stale = self.child.sender::<Number>().unwrap();
                self.parent.revoke().await.unwrap();
                assert!(matches!(stale.send_now(90), Err(TrySendError::Revoked(90))));
                assert!(matches!(
                    self.link.take().unwrap().await,
                    Err(IpcError::Revoked)
                ));
            }
        }
        model.terminate();
        self.check(model);
    }
}

async fn driving<T>(link: &mut UnixLink, action: impl Future<Output = T>) -> T {
    tokio::select! {
        biased;
        result = link => panic!("session terminated during an admitted action: {result:?}"),
        result = action => result,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lifecycle_model_composes_grants_streams_waits_and_unix_termination() {
    bounded(async {
        for required in [false, true] {
            for consume_first in [false, true] {
                for finish in [
                    Finish::Cancel,
                    Finish::Disconnect,
                    Finish::Revoke,
                    Finish::Drain,
                ] {
                    let mut session = Session::new(required);
                    let mut model = Model::new(required);
                    session.check(&model);
                    session.forward(&mut model, 10).await;
                    if consume_first {
                        assert!(session.receive(&mut model, false));
                    }
                    session.forward(&mut model, 20).await;
                    if model.pending.is_some() {
                        session.cancel_local_waiter(&model);
                    }
                    session.finish(&mut model, finish).await;
                    while session.receive(&mut model, false) {}
                    if required {
                        while session.receive(&mut model, true) {}
                    }
                    assert_eq!(model.usage(), Limits::ZERO);
                    let parent = session.parent.clone();
                    drop(session);
                    assert_eq!(parent.usage(), Limits::ZERO);
                }
            }
        }
    })
    .await;
}
