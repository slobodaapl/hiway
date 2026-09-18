use super::*;
use std::{collections::VecDeque, sync::Mutex};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
struct IoState {
    input: VecDeque<u8>,
    output: Vec<u8>,
    read_bytes: usize,
    writable: usize,
    eof: bool,
    reader: Option<Waker>,
    writer: Option<Waker>,
}

#[derive(Clone, Default)]
struct Io(Arc<Mutex<IoState>>);

impl Io {
    fn feed(&self, bytes: &[u8], eof: bool) {
        let wake = {
            let mut state = self.0.lock().unwrap();
            state.input.extend(bytes);
            state.eof = eof;
            state.reader.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }

    fn permit_write(&self, bytes: usize) {
        let wake = {
            let mut state = self.0.lock().unwrap();
            state.writable += bytes;
            state.writer.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }

    fn counts(&self) -> (usize, usize) {
        let state = self.0.lock().unwrap();
        (state.read_bytes, state.output.len())
    }
}

impl AsyncRead for Io {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut state = self.0.lock().unwrap();
        if state.input.is_empty() && !state.eof && buffer.remaining() > 0 {
            state.reader = Some(context.waker().clone());
            return Poll::Pending;
        }
        while buffer.remaining() > 0 {
            let Some(byte) = state.input.pop_front() else {
                break;
            };
            buffer.put_slice(&[byte]);
            state.read_bytes += 1;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Io {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut state = self.0.lock().unwrap();
        if state.writable == 0 && !bytes.is_empty() {
            state.writer = Some(context.waker().clone());
            return Poll::Pending;
        }
        let length = bytes.len().min(state.writable);
        state.writable -= length;
        state.output.extend_from_slice(&bytes[..length]);
        Poll::Ready(Ok(length))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct Ready(AtomicUsize);

impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(1, Ordering::Release);
    }
}

struct Driver {
    link: Option<UnixLink>,
    ready: Arc<Ready>,
    result: Option<Result<(), IpcError>>,
}

impl Driver {
    fn new(link: UnixLink) -> Self {
        Self {
            link: Some(link),
            ready: Arc::new(Ready(AtomicUsize::new(1))),
            result: None,
        }
    }

    fn drive(&mut self, data: &Io, control: &Io) {
        let waker = Waker::from(self.ready.clone());
        let mut polls = 0;
        while self.link.is_some() && self.ready.0.swap(0, Ordering::AcqRel) != 0 {
            polls += 1;
            assert!(polls <= 8, "idle driver repeatedly woke itself");
            let before = (data.counts(), control.counts());
            let result =
                Pin::new(self.link.as_mut().unwrap()).poll(&mut Context::from_waker(&waker));
            let after = (data.counts(), control.counts());
            assert!(after.0 .0 - before.0 .0 <= 42);
            assert!(after.0 .1 - before.0 .1 <= 42);
            assert!(after.1 .0 - before.1 .0 <= 9);
            assert!(after.1 .1 - before.1 .1 <= 9);
            if let Poll::Ready(result) = result {
                self.result = Some(result);
                drop(self.link.take());
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Finish {
    Cancel,
    Revoke,
    Disconnect,
    InvalidControl,
}

fn terminate(driver: &mut Driver, grant: &Grant, data: &Io, control: &Io, finish: Finish) {
    match finish {
        Finish::Cancel => drop(driver.link.take()),
        Finish::Revoke => {
            let mut revoke = std::pin::pin!(grant.revoke());
            assert_eq!(
                revoke
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            );
        }
        Finish::Disconnect => control.feed(&[], true),
        Finish::InvalidControl => control.feed(&[0; 18], false),
    }
    driver.drive(data, control);
    match finish {
        Finish::Cancel => assert!(driver.result.is_none()),
        Finish::Revoke => assert!(matches!(driver.result, Some(Err(IpcError::Revoked)))),
        Finish::Disconnect => assert!(matches!(&driver.result,
            Some(Err(IpcError::Io(error))) if error.kind() == ErrorKind::UnexpectedEof)),
        Finish::InvalidControl => {
            assert!(matches!(driver.result, Some(Err(IpcError::Protocol))));
        }
    }
    assert!(driver.link.is_none());
}

fn import_schedule(cut: usize, credit: usize, actions: &[usize], eager: bool, finish: Finish) {
    let (fabric, local) = setup(1);
    let grant = fabric
        .grant(&permission(Rights::PUBLISH), allowance())
        .unwrap();
    let required = local
        .subscribe::<Number>(SubscriptionRole::Required)
        .unwrap();
    local.sender::<Number>().unwrap().send_now(10).unwrap();
    let data = Io::default();
    let control = Io::default();
    let read = control.clone();
    let write = control.clone();
    let mut driver = Driver::new(
        UnixLink::import_io::<Number, _, _>(grant.clone(), data.clone(), move || (read, write), 4)
            .unwrap(),
    );
    driver.drive(&data, &control);
    let mut fed = 0;
    let mut received = false;
    let mut accepted = false;
    let mut writable = 0;
    let input = frame(7, 20);
    let expected_credit = [1, 7, 0, 0, 0, 0, 0, 0, 0];
    for &action in actions {
        match action {
            0 => {
                data.feed(&input[..cut], false);
                fed += cut;
            }
            1 => {
                data.feed(&input[cut..], false);
                fed += input.len() - cut;
            }
            2 => {
                control.permit_write(credit);
                writable = credit;
            }
            3 => {
                assert!(matches!(required.recv_now().unwrap(),
                    Some(StreamItem::Data { sequence: 0, value }) if *value == 10));
                received = true;
            }
            _ => unreachable!(),
        }
        if eager {
            driver.drive(&data, &control);
            accepted |= fed == input.len() && received;
            assert!(driver.result.is_none());
            assert_eq!(
                control.0.lock().unwrap().output,
                expected_credit[..if accepted { writable } else { 0 }]
            );
            assert_eq!(
                grant.stream_usage::<Number>().unwrap(),
                crate::StreamLimits {
                    retained_items: usize::from(accepted),
                    waiters: usize::from(fed == input.len() && !received),
                    subscriptions: 0,
                }
            );
        }
        assert_eq!(grant.usage().connections, 2);
        assert_eq!(grant.usage().bytes, 60);
    }
    let output = control.0.lock().unwrap().output.clone();
    terminate(&mut driver, &grant, &data, &control, finish);
    data.feed(&input, false);
    control.permit_write(9);
    driver.drive(&data, &control);
    assert_eq!(control.0.lock().unwrap().output, output);
    if !received {
        assert!(matches!(required.recv_now().unwrap(),
            Some(StreamItem::Data { value, .. }) if *value == 10));
    }
    let tail = required
        .recv_now()
        .unwrap()
        .map(|item| item.map(|value| *value));
    assert_eq!(
        tail,
        accepted.then_some(StreamItem::Data {
            sequence: 1,
            value: 20
        })
    );
    assert_eq!(required.recv_now().unwrap(), None);
    assert_eq!(grant.usage(), Limits::ZERO);
    assert_eq!(Arc::strong_count(&data.0), 1);
    assert_eq!(Arc::strong_count(&control.0), 1);
}

#[test]
fn import_readiness_schedules_preserve_acceptance_credit_and_cutoff() {
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    let order = [a, b, c, d];
                    if (0..4).any(|i| order[..i].contains(&order[i]))
                        || order.iter().position(|&x| x == 0) > order.iter().position(|&x| x == 1)
                    {
                        continue;
                    }
                    for cut in [1, 38, 41] {
                        for credit in [0, 1, 8, 9] {
                            for length in 0..=4 {
                                for eager in [false, true] {
                                    for finish in [
                                        Finish::Cancel,
                                        Finish::Revoke,
                                        Finish::Disconnect,
                                        Finish::InvalidControl,
                                    ] {
                                        import_schedule(
                                            cut,
                                            credit,
                                            &order[..length],
                                            eager,
                                            finish,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn export_schedule(cut: usize, credit_cut: usize, actions: &[usize], eager: bool, finish: Finish) {
    let (fabric, local) = setup(2);
    let grant = fabric
        .grant(&permission(Rights::OBSERVE | Rights::REQUIRED), allowance())
        .unwrap();
    let data = Io::default();
    let control = Io::default();
    let mut driver = Driver::new(
        UnixLink::export_io::<Number>(
            grant.clone(),
            data.clone(),
            control.clone(),
            SubscriptionRole::Required,
            4,
        )
        .unwrap(),
    );
    let sender = local.sender::<Number>().unwrap();
    sender.send_now(10).unwrap();
    sender.send_now(20).unwrap();
    driver.drive(&data, &control);
    let mut expected = frame(0, 10);
    expected.extend(frame(1, 20));
    let credit = [1, 0, 0, 0, 0, 0, 0, 0, 0];
    let mut credited = 0;
    let mut writable = 0;
    for &action in actions {
        match action {
            0 => {
                data.permit_write(cut);
                writable += cut;
            }
            1 => {
                control.feed(&credit[..credit_cut], false);
                credited += credit_cut;
            }
            2 => {
                control.feed(&credit[credit_cut..], false);
                credited += credit.len() - credit_cut;
            }
            3 => {
                data.permit_write(84 - cut);
                writable += 84 - cut;
            }
            _ => unreachable!(),
        }
        if eager {
            driver.drive(&data, &control);
            assert!(driver.result.is_none());
            let written = writable.min(if credited == credit.len() { 84 } else { 42 });
            assert_eq!(data.0.lock().unwrap().output, expected[..written]);
            assert_eq!(
                local.usage().retained_items,
                usize::from(credited < credit.len() || writable < 42)
            );
        }
        assert_eq!(
            grant.usage(),
            Limits {
                subscriptions: 1,
                retained_items: 1,
                waiters: 2,
                connections: 2,
                bytes: 60,
                ..Limits::ZERO
            }
        );
    }
    let before = data.0.lock().unwrap().output.clone();
    terminate(&mut driver, &grant, &data, &control, finish);
    let after = data.0.lock().unwrap().output.clone();
    assert_eq!(after, expected[..after.len()]);
    if matches!(finish, Finish::Cancel | Finish::Revoke) {
        assert_eq!(after, before);
    }
    data.permit_write(84);
    control.feed(&credit, false);
    driver.drive(&data, &control);
    assert_eq!(data.0.lock().unwrap().output, after);
    assert_eq!(grant.usage(), Limits::ZERO);
    assert_eq!(local.usage(), Limits::ZERO);
    assert_eq!(Arc::strong_count(&data.0), 1);
    assert_eq!(Arc::strong_count(&control.0), 1);
}

#[test]
fn export_readiness_schedules_preserve_frames_credits_and_cutoff() {
    for order in [
        [0, 3, 1, 2],
        [0, 1, 3, 2],
        [0, 1, 2, 3],
        [1, 2, 0, 3],
        [1, 0, 2, 3],
        [1, 0, 3, 2],
    ] {
        for cut in [0, 1, 37, 38, 41, 42] {
            for credit_cut in [1, 8] {
                for length in 0..=4 {
                    for eager in [false, true] {
                        for finish in [
                            Finish::Cancel,
                            Finish::Revoke,
                            Finish::Disconnect,
                            Finish::InvalidControl,
                        ] {
                            export_schedule(cut, credit_cut, &order[..length], eager, finish);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn always_ready_import_yields_between_frames() {
    let (fabric, _) = setup(3);
    let grant = fabric
        .grant(&permission(Rights::PUBLISH | Rights::OBSERVE), allowance())
        .unwrap();
    let observer = grant
        .subscribe::<Number>(SubscriptionRole::Observer)
        .unwrap();
    let data = Io::default();
    let control = Io::default();
    for value in 0..3 {
        data.feed(&frame(u64::from(value), value), false);
    }
    control.permit_write(27);
    let read = control.clone();
    let write = control.clone();
    let mut driver = Driver::new(
        UnixLink::import_io::<Number, _, _>(grant.clone(), data.clone(), move || (read, write), 4)
            .unwrap(),
    );
    driver.drive(&data, &control);
    for value in 0..3 {
        assert_eq!(
            observer
                .recv_now()
                .unwrap()
                .map(|item| item.map(|value| *value)),
            Some(StreamItem::Data {
                sequence: u64::from(value),
                value
            })
        );
    }
    assert_eq!(control.counts().1, 27);
    assert!(driver.result.is_none());
    terminate(&mut driver, &grant, &data, &control, Finish::Revoke);
    drop(observer);
    assert_eq!(grant.usage(), Limits::ZERO);
}
