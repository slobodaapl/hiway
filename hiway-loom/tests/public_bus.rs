#![cfg(loom)]

use std::{
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc as StdArc,
    task::{Context, Poll, Wake, Waker},
};

use hiway::{Bus, HiwayEvent, PublishError};
use loom::{
    future::block_on,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
};

// These models use the public Bus. cfg(loom) changes its state lock, not its
// storage or protocol code.

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Value(u8);

impl HiwayEvent for Value {}

type ValueBus = Bus<Value, 1, 2, 2, 2>;

fn value_bus() -> Arc<ValueBus> {
    Arc::new(ValueBus::new())
}

#[test]
fn concurrent_publishers_and_overlapping_tick_deliver_each_value_once() {
    loom::model(|| {
        let bus = value_bus();
        let mut subscriber = bus.subscribe().unwrap();

        let first_bus = bus.clone();
        let first = thread::spawn(move || block_on(first_bus.publish(Value(1))).unwrap());
        let second_bus = bus.clone();
        let second = thread::spawn(move || block_on(second_bus.publish(Value(2))).unwrap());
        let ticker_bus = bus.clone();
        let ticker = thread::spawn(move || ticker_bus.tick());

        first.join().unwrap();
        second.join().unwrap();
        ticker.join().unwrap();
        let _ = bus.tick();
        let _ = bus.tick();
        assert!(!bus.tick());

        let mut values = [
            block_on(subscriber.recv()).unwrap().0,
            block_on(subscriber.recv()).unwrap().0,
        ];
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
    });
}

#[test]
fn recv_tick_race_observes_or_wakes_for_a_committed_event() {
    loom::model(|| {
        let bus = value_bus();
        let mut subscriber = bus.subscribe().unwrap();

        let publisher_bus = bus.clone();
        let publisher = thread::spawn(move || {
            block_on(publisher_bus.publish(Value(7))).unwrap();
            assert!(publisher_bus.tick());
        });

        assert_eq!(block_on(subscriber.recv()).unwrap(), Value(7));
        publisher.join().unwrap();
    });
}

#[test]
fn concurrent_ticks_commit_one_ready_frame_once() {
    loom::model(|| {
        let bus = value_bus();
        let mut subscriber = bus.subscribe().unwrap();
        block_on(bus.publish(Value(3))).unwrap();

        let first_bus = bus.clone();
        let first = thread::spawn(move || first_bus.tick());
        let second_bus = bus.clone();
        let second = thread::spawn(move || second_bus.tick());
        let results = [first.join().unwrap(), second.join().unwrap()];

        assert_eq!(
            results.into_iter().filter(|committed| *committed).count(),
            1
        );
        assert_eq!(block_on(subscriber.recv()).unwrap(), Value(3));
        assert!(!bus.tick());
    });
}

struct CloneGate {
    state: Mutex<CloneGateState>,
    changed: Condvar,
}

struct CloneGateState {
    entered: bool,
    release: bool,
}

impl CloneGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(CloneGateState {
                entered: false,
                release: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn block_clone(&self) {
        let mut state = self.state.lock().unwrap();
        if state.entered {
            return;
        }
        state.entered = true;
        self.changed.notify_all();
        while !state.release {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.release = true;
        self.changed.notify_all();
    }
}

struct CloneEvent {
    value: u8,
    gate: Option<Arc<CloneGate>>,
}

impl Clone for CloneEvent {
    fn clone(&self) -> Self {
        if let Some(gate) = &self.gate {
            gate.block_clone();
        }
        Self {
            value: self.value,
            gate: self.gate.clone(),
        }
    }
}

impl HiwayEvent for CloneEvent {}

type CloneBus = Bus<CloneEvent, 1, 2, 2, 2>;

type SaturatedCloneBus = Bus<CloneEvent, 1, 1, 2, 2>;

#[test]
fn active_preparation_saturates_ready_capacity_and_retry_preserves_batch() {
    loom::model(|| {
        let gate = Arc::new(CloneGate::new());
        let bus = Arc::new(SaturatedCloneBus::new());
        let mut first = bus.subscribe().unwrap();
        let _second = bus.subscribe().unwrap();

        let publisher_bus = bus.clone();
        let publisher = thread::spawn({
            let gate = gate.clone();
            move || {
                block_on(publisher_bus.publish(CloneEvent {
                    value: 1,
                    gate: Some(gate),
                }))
                .unwrap();
            }
        });
        gate.wait_until_entered();

        let Err(PublishError::ReadyFull(full)) = block_on(bus.publish(CloneEvent {
            value: 2,
            gate: None,
        })) else {
            panic!("active preparation must occupy ready capacity");
        };
        let retry = full.into_batch();

        gate.release();
        publisher.join().unwrap();
        assert!(bus.tick());
        assert_eq!(block_on(first.recv()).unwrap().value, 1);

        bus.try_submit(retry).unwrap();
        assert!(bus.tick());
        assert_eq!(block_on(first.recv()).unwrap().value, 2);
    });
}

#[test]
fn subscription_generation_isolated_and_event_clone_runs_outside_state_lock() {
    loom::model(|| {
        let gate = Arc::new(CloneGate::new());
        let bus = Arc::new(CloneBus::new());
        let old = bus.subscribe().unwrap();
        let mut keeper = bus.subscribe().unwrap();

        let publisher_bus = bus.clone();
        let publisher = thread::spawn({
            let gate = gate.clone();
            move || {
                block_on(publisher_bus.publish(CloneEvent {
                    value: 1,
                    gate: Some(gate),
                }))
                .unwrap();
            }
        });
        gate.wait_until_entered();
        assert!(!bus.tick());

        drop(old);
        let mut replacement = bus.subscribe().unwrap();
        gate.release();

        publisher.join().unwrap();
        assert!(bus.tick());
        assert_eq!(block_on(keeper.recv()).unwrap().value, 1);

        block_on(bus.publish(CloneEvent {
            value: 2,
            gate: None,
        }))
        .unwrap();
        assert!(bus.tick());
        assert_eq!(block_on(replacement.recv()).unwrap().value, 2);
    });
}

#[derive(Debug)]
struct MaybePanicClone {
    value: u8,
    panic: bool,
}

impl Clone for MaybePanicClone {
    fn clone(&self) -> Self {
        assert!(!self.panic, "test clone panic");
        Self {
            value: self.value,
            panic: self.panic,
        }
    }
}

#[derive(Clone, Debug, HiwayEvent)]
enum ClonePanicEvent {
    Value(MaybePanicClone),
}

type ClonePanicBus = Bus<ClonePanicEvent, 1, 1, 1, 2>;

#[test]
fn clone_panic_releases_preparation_reservation() {
    loom::model(|| {
        let bus = Arc::new(ClonePanicBus::new());
        let mut first = bus.subscribe().unwrap();
        let _second = bus.subscribe().unwrap();

        let result = catch_unwind(AssertUnwindSafe({
            let bus = bus.clone();
            move || {
                block_on(bus.publish(MaybePanicClone {
                    value: 1,
                    panic: true,
                }))
            }
        }));
        assert!(result.is_err());

        block_on(bus.publish(MaybePanicClone {
            value: 2,
            panic: false,
        }))
        .unwrap();
        assert!(bus.tick());
        assert!(matches!(
            block_on(first.recv()).unwrap(),
            ClonePanicEvent::Value(MaybePanicClone { value: 2, .. })
        ));
    });
}

#[derive(Clone, Debug)]
struct TagValue {
    value: u8,
    panic: bool,
}

#[derive(Clone, Debug)]
enum TagEvent {
    Value(TagValue),
}

impl From<TagValue> for TagEvent {
    fn from(value: TagValue) -> Self {
        Self::Value(value)
    }
}

impl HiwayEvent for TagEvent {
    fn __hiway_tag(&self) -> usize {
        match self {
            Self::Value(value) if value.panic => panic!("test tag panic"),
            Self::Value(_) => 0,
        }
    }
}

impl TryFrom<TagEvent> for TagValue {
    type Error = TagEvent;

    fn try_from(value: TagEvent) -> Result<Self, Self::Error> {
        match value {
            TagEvent::Value(value) => Ok(value),
        }
    }
}

impl hiway::__private::EventTag<TagEvent> for TagValue {
    fn __hiway_tag() -> usize {
        0
    }
}

type TagPanicBus = Bus<TagEvent, 1, 1, 1, 1>;

#[test]
fn tag_panic_releases_preparation_reservation() {
    loom::model(|| {
        let bus = Arc::new(TagPanicBus::new());
        let mut values = bus.consume::<TagValue>().unwrap();

        let result = catch_unwind(AssertUnwindSafe({
            let bus = bus.clone();
            move || {
                block_on(bus.publish(TagValue {
                    value: 1,
                    panic: true,
                }))
            }
        }));
        assert!(result.is_err());

        block_on(bus.publish(TagValue {
            value: 2,
            panic: false,
        }))
        .unwrap();
        assert!(bus.tick());
        assert_eq!(block_on(values.recv()).unwrap().value, 2);
    });
}

struct DropGate {
    state: Mutex<DropGateState>,
    changed: Condvar,
}

struct DropGateState {
    entered: bool,
    release: bool,
}

impl DropGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(DropGateState {
                entered: false,
                release: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn block_drop(&self) {
        let mut state = self.state.lock().unwrap();
        if state.entered {
            return;
        }
        state.entered = true;
        self.changed.notify_all();
        while !state.release {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.release = true;
        self.changed.notify_all();
    }
}

struct DropEvent {
    value: u8,
    gate: Option<Arc<DropGate>>,
}

impl Clone for DropEvent {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            gate: self.gate.clone(),
        }
    }
}

impl Drop for DropEvent {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.block_drop();
        }
    }
}

impl HiwayEvent for DropEvent {}

type DropBus = Bus<DropEvent, 1, 1, 1, 1>;

#[test]
fn rejected_ready_frame_drops_after_releasing_bus_state() {
    loom::model(|| {
        let bus = Arc::new(DropBus::new());
        block_on(bus.publish(DropEvent {
            value: 1,
            gate: None,
        }))
        .unwrap();

        let gate = Arc::new(DropGate::new());
        let publisher_bus = bus.clone();
        let publisher = thread::spawn({
            let gate = gate.clone();
            move || {
                let result = block_on(publisher_bus.publish(DropEvent {
                    value: 2,
                    gate: Some(gate),
                }));
                assert!(matches!(result, Err(PublishError::ReadyFull(_))));
            }
        });

        gate.wait_until_entered();
        let subscription = bus.subscribe().unwrap();
        drop(subscription);
        gate.release();

        publisher.join().unwrap();
    });
}

#[test]
fn tick_evicted_event_drops_after_releasing_bus_state() {
    loom::model(|| {
        let bus = Arc::new(DropBus::new());
        let subscriber = bus.subscribe().unwrap();
        let gate = Arc::new(DropGate::new());

        block_on(bus.publish(DropEvent {
            value: 1,
            gate: Some(gate.clone()),
        }))
        .unwrap();
        assert!(bus.tick());
        block_on(bus.publish(DropEvent {
            value: 2,
            gate: None,
        }))
        .unwrap();

        let ticker_bus = bus.clone();
        let ticker = thread::spawn(move || assert!(ticker_bus.tick()));
        gate.wait_until_entered();
        drop(subscriber);
        gate.release();
        ticker.join().unwrap();
    });
}

struct WakeProbe {
    bus: Arc<ValueBus>,
    called: AtomicBool,
}

impl WakeProbe {
    fn reenter(&self) {
        self.called.store(true, Ordering::SeqCst);
        let subscription = self.bus.subscribe().unwrap();
        drop(subscription);
    }
}

impl Wake for WakeProbe {
    fn wake(self: StdArc<Self>) {
        self.reenter();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        self.reenter();
    }
}

#[test]
fn subscriber_waker_reenters_bus_after_tick_releases_state() {
    loom::model(|| {
        let bus = value_bus();
        let mut subscriber = bus.subscribe().unwrap();
        let probe = StdArc::new(WakeProbe {
            bus: bus.clone(),
            called: AtomicBool::new(false),
        });
        let waker = Waker::from(probe.clone());
        let mut context = Context::from_waker(&waker);
        let mut receive = Box::pin(subscriber.recv());

        assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));
        block_on(bus.publish(Value(5))).unwrap();
        assert!(bus.tick());
        assert!(probe.called.load(Ordering::SeqCst));
        assert_eq!(block_on(receive).unwrap(), Value(5));
    });
}
