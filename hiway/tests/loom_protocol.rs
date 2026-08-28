use loom::{
    sync::{Arc, Condvar, Mutex, RwLock},
    thread,
};

type Batch = [u8; 2];
type Frame = Arc<Vec<Batch>>;

fn batch(first: u8, second: u8) -> Batch {
    [first, second]
}

#[derive(Clone)]
struct ModelBus {
    transforms: Arc<RwLock<Vec<u8>>>,
    commit: Arc<Mutex<()>>,
    ready: Arc<Mutex<Vec<Batch>>>,
    // Tokio owns broadcast fan-out, capacity, and lag. One append represents one frame send.
    committed: Arc<Mutex<Vec<Frame>>>,
}

impl ModelBus {
    fn new() -> Self {
        Self {
            transforms: Arc::new(RwLock::new(vec![1])),
            commit: Arc::new(Mutex::new(())),
            ready: Arc::new(Mutex::new(Vec::new())),
            committed: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn prepare(&self, batch: Batch) {
        self.ready.lock().unwrap().push(batch);
    }

    fn tick(&self) -> bool {
        self.tick_before_commit(|| {})
    }

    fn tick_before_commit<F>(&self, before_commit: F) -> bool
    where
        F: FnOnce(),
    {
        let Ok(commit) = self.commit.try_lock() else {
            return false;
        };
        let frame = {
            let Ok(mut ready) = self.ready.try_lock() else {
                return false;
            };
            if ready.is_empty() {
                return false;
            }
            Arc::new(std::mem::take(&mut *ready))
        };
        before_commit();
        self.committed.lock().unwrap().push(frame);
        drop(commit);
        true
    }

    fn committed(&self) -> Vec<Frame> {
        self.committed.lock().unwrap().clone()
    }

    fn subscribe(&self) -> ModelSubscription {
        ModelSubscription {
            committed: self.committed.clone(),
            next_frame: 0,
            frame: None,
            batch_index: 0,
            event_index: 0,
        }
    }
}

struct ModelSubscription {
    committed: Arc<Mutex<Vec<Frame>>>,
    next_frame: usize,
    frame: Option<Frame>,
    batch_index: usize,
    event_index: usize,
}

impl ModelSubscription {
    fn recv(&mut self) -> Option<u8> {
        loop {
            if let Some(frame) = &self.frame {
                if let Some(batch) = frame.get(self.batch_index) {
                    if let Some(token) = batch.get(self.event_index) {
                        self.event_index += 1;
                        return Some(*token);
                    }
                    self.batch_index += 1;
                    self.event_index = 0;
                    continue;
                }
            }

            self.frame = None;
            self.batch_index = 0;
            self.event_index = 0;
            let frame = self.committed.lock().unwrap().get(self.next_frame).cloned();
            let frame = frame?;
            self.next_frame += 1;
            self.frame = Some(frame);
        }
    }

    fn drain(&mut self) -> Vec<u8> {
        let mut tokens = Vec::new();
        while let Some(token) = self.recv() {
            tokens.push(token);
        }
        tokens
    }
}

#[test]
fn snapshot_releases_registry_before_user_work() {
    loom::model(|| {
        let bus = ModelBus::new();
        let snapshot_ready = Arc::new((Mutex::new(false), Condvar::new()));
        let user_gate = Arc::new((Mutex::new(false), Condvar::new()));

        let publisher_bus = bus.clone();
        let publisher_ready = snapshot_ready.clone();
        let publisher_gate = user_gate.clone();
        let publisher = thread::spawn(move || {
            let snapshot = {
                let stages = publisher_bus.transforms.read().unwrap();
                stages.clone()
            };

            let (ready, changed) = &*publisher_ready;
            let mut ready = ready.lock().unwrap();
            *ready = true;
            changed.notify_one();
            drop(ready);

            let (gate, changed) = &*publisher_gate;
            let mut gate = gate.lock().unwrap();
            while !*gate {
                gate = changed.wait(gate).unwrap();
            }
            snapshot
        });

        let registrar_bus = bus.clone();
        let registrar_ready = snapshot_ready.clone();
        let registrar_gate = user_gate.clone();
        let registrar = thread::spawn(move || {
            let (ready, changed) = &*registrar_ready;
            let mut ready = ready.lock().unwrap();
            while !*ready {
                ready = changed.wait(ready).unwrap();
            }
            drop(ready);

            registrar_bus.transforms.write().unwrap().push(2);

            let (gate, changed) = &*registrar_gate;
            let mut gate = gate.lock().unwrap();
            *gate = true;
            changed.notify_one();
        });

        let snapshot = publisher.join().unwrap();
        registrar.join().unwrap();

        assert_eq!(snapshot, vec![1]);
        assert_eq!(&*bus.transforms.read().unwrap(), &vec![1, 2]);
    });
}

#[test]
fn ready_frame_uses_preparation_completion_order() {
    loom::model(|| {
        let bus = ModelBus::new();
        let slow_gate = Arc::new((Mutex::new((false, false)), Condvar::new()));

        let first_bus = bus.clone();
        let first_gate = slow_gate.clone();
        let slow = thread::spawn(move || {
            let (state, changed) = &*first_gate;
            let mut state = state.lock().unwrap();
            state.0 = true;
            changed.notify_one();
            while !state.1 {
                state = changed.wait(state).unwrap();
            }
            drop(state);
            first_bus.prepare(batch(1, 11));
        });

        let (state, changed) = &*slow_gate;
        let mut state = state.lock().unwrap();
        while !state.0 {
            state = changed.wait(state).unwrap();
        }
        drop(state);

        let fast_bus = bus.clone();
        let fast_gate = slow_gate.clone();
        let fast = thread::spawn(move || {
            fast_bus.prepare(batch(2, 22));
            let (state, changed) = &*fast_gate;
            let mut state = state.lock().unwrap();
            state.1 = true;
            changed.notify_one();
        });

        fast.join().unwrap();
        slow.join().unwrap();

        assert!(bus.tick());
        assert!(!bus.tick());
        let committed = bus.committed();
        assert_eq!(committed.len(), 1);
        assert_eq!(
            committed[0]
                .iter()
                .map(|batch| batch[0])
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
    });
}

#[test]
fn concurrent_tickers_commit_the_complete_frame_once() {
    loom::model(|| {
        let bus = ModelBus::new();
        bus.prepare(batch(1, 11));
        bus.prepare(batch(2, 22));

        let first_bus = bus.clone();
        let first = thread::spawn(move || first_bus.tick());
        let second_bus = bus.clone();
        let second = thread::spawn(move || second_bus.tick());
        let first_ticked = first.join().unwrap();
        let second_ticked = second.join().unwrap();
        assert_eq!(u8::from(first_ticked) + u8::from(second_ticked), 1);
        assert!(!bus.tick());

        let committed = bus.committed();
        assert_eq!(committed.len(), 1);
        let mut seen_first = 0;
        let mut seen_second = 0;
        for batch in &*committed[0] {
            match batch.as_ref() {
                [1, 11] => seen_first += 1,
                [2, 22] => seen_second += 1,
                other => panic!("interleaved or unknown batch: {other:?}"),
            }
        }
        assert_eq!((seen_first, seen_second), (1, 1));
    });
}

#[test]
fn in_flight_tick_cannot_be_overtaken_by_a_later_frame() {
    loom::model(|| {
        let bus = ModelBus::new();
        bus.prepare(batch(1, 11));
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));

        let first_bus = bus.clone();
        let first_gate = gate.clone();
        let first_tick = thread::spawn(move || {
            first_bus.tick_before_commit(|| {
                let (state, changed) = &*first_gate;
                let mut state = state.lock().unwrap();
                state.0 = true;
                changed.notify_one();
                while !state.1 {
                    state = changed.wait(state).unwrap();
                }
            })
        });

        let (gate_state, changed) = &*gate;
        let mut state = gate_state.lock().unwrap();
        while !state.0 {
            state = changed.wait(state).unwrap();
        }
        drop(state);

        bus.prepare(batch(2, 22));
        assert!(!bus.tick());

        let mut state = gate_state.lock().unwrap();
        state.1 = true;
        changed.notify_one();
        drop(state);

        assert!(first_tick.join().unwrap());
        assert!(bus.tick());
        assert!(!bus.tick());

        let committed = bus.committed();
        assert_eq!(committed.len(), 2);
        let batches = committed
            .iter()
            .flat_map(|frame| frame.iter())
            .map(std::convert::AsRef::as_ref)
            .collect::<Vec<_>>();
        assert_eq!(batches, vec![[1, 11].as_slice(), [2, 22].as_slice()]);
    });
}

#[test]
fn tick_is_nonblocking_and_commits_one_complete_frame() {
    loom::model(|| {
        let bus = ModelBus::new();
        assert!(!bus.tick());

        let held = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let holder_bus = bus.clone();
        let holder_held = held.clone();
        let holder_release = release.clone();
        let holder = thread::spawn(move || {
            let ready = holder_bus.ready.lock().unwrap();

            let (started, changed) = &*holder_held;
            let mut started = started.lock().unwrap();
            *started = true;
            changed.notify_one();
            drop(started);

            let (released, changed) = &*holder_release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
            drop(released);
            drop(ready);
        });

        let (started, changed) = &*held;
        let mut started = started.lock().unwrap();
        while !*started {
            started = changed.wait(started).unwrap();
        }
        drop(started);

        assert!(!bus.tick());

        let (released, changed) = &*release;
        let mut released = released.lock().unwrap();
        *released = true;
        changed.notify_one();
        drop(released);
        holder.join().unwrap();

        bus.prepare(batch(1, 11));
        bus.prepare(batch(2, 22));
        assert!(bus.tick());
        assert_eq!(bus.committed().len(), 1);
        assert!(!bus.tick());
        assert_eq!(bus.committed()[0].len(), 2);
    });
}

#[test]
fn subscriber_drains_each_batch_before_the_next_batch() {
    loom::model(|| {
        let bus = ModelBus::new();
        let mut subscriber = bus.subscribe();
        bus.prepare(batch(1, 11));
        assert!(bus.tick());
        assert_eq!(subscriber.recv(), Some(1));

        bus.prepare(batch(2, 22));
        assert!(bus.tick());
        assert_eq!(subscriber.drain(), vec![11, 2, 22]);
    });
}

#[test]
fn subscribers_have_identical_order_and_independent_cursors() {
    loom::model(|| {
        let bus = ModelBus::new();
        let mut first = bus.subscribe();
        let mut second = bus.subscribe();
        bus.prepare(batch(1, 11));
        bus.prepare(batch(2, 22));
        assert!(bus.tick());
        assert!(!bus.tick());

        assert_eq!(first.recv(), Some(1));
        assert_eq!(second.recv(), Some(1));
        assert_eq!(second.drain(), vec![11, 2, 22]);
        assert_eq!(first.drain(), vec![11, 2, 22]);
    });
}
