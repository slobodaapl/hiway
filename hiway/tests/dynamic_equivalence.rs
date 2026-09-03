#![cfg(feature = "dynamic")]

use std::sync::{Arc, Mutex};

use hiway::{stage, Bus, DynamicBus, HiwayEvent, PipelineExt, PublishError};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Input(u8);

#[derive(Clone, Debug, PartialEq, Eq)]
struct First(u8);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Second {
    value: u8,
    keep: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Final(u8);

#[derive(Clone, Debug, PartialEq, Eq, HiwayEvent)]
enum Event {
    Input(Input),
    First(First),
    Second(Second),
    Final(Final),
}

struct TracedIter {
    events: std::vec::IntoIter<Event>,
    trace: Arc<Mutex<Vec<&'static str>>>,
    label: &'static str,
}

impl TracedIter {
    fn new(
        events: impl IntoIterator<Item = Event>,
        trace: Arc<Mutex<Vec<&'static str>>>,
        label: &'static str,
    ) -> Self {
        Self {
            events: events.into_iter().collect::<Vec<_>>().into_iter(),
            trace,
            label,
        }
    }
}

impl Iterator for TracedIter {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        self.trace.lock().unwrap().push(self.label);
        self.events.next()
    }
}

async fn run_static() -> (Vec<Event>, Vec<&'static str>) {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let expand_trace = trace.clone();
    let branch_trace = trace.clone();
    let reduce_trace = trace.clone();
    let pipeline = stage(move |input: Input| {
        expand_trace.lock().unwrap().push("expand");
        let trace = expand_trace.clone();
        async move {
            TracedIter::new(
                [
                    Event::First(First(input.0)),
                    Event::First(First(input.0 + 1)),
                    Event::First(First(input.0 + 2)),
                ],
                trace,
                "expand.next",
            )
        }
    })
    .then(stage(move |first: First| {
        branch_trace.lock().unwrap().push("branch");
        let trace = branch_trace.clone();
        async move {
            TracedIter::new(
                [
                    Event::Second(Second {
                        value: first.0,
                        keep: first.0 != 8,
                    }),
                    Event::Second(Second {
                        value: first.0,
                        keep: false,
                    }),
                ],
                trace,
                "branch.next",
            )
        }
    }))
    .then(stage(move |second: Second| {
        reduce_trace.lock().unwrap().push("reduce");
        let trace = reduce_trace.clone();
        async move {
            TracedIter::new(
                core::iter::once(Event::Final(Final(second.value))).take(usize::from(second.keep)),
                trace,
                "reduce.next",
            )
        }
    }));
    let bus: Bus<Event, 2, 2, 2, 1, _> = Bus::with_pipeline(pipeline);
    let mut events = bus.subscribe().unwrap();

    bus.publish(Input(7)).await.unwrap();
    assert!(bus.tick());
    let events = vec![events.recv().await.unwrap(), events.recv().await.unwrap()];
    let trace = trace.lock().unwrap().clone();
    (events, trace)
}

async fn run_dynamic() -> (Vec<Event>, Vec<&'static str>) {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let bus: DynamicBus<Event, 2, 2, 2, 1> = DynamicBus::new();

    let expand_trace = trace.clone();
    bus.transform(move |input: Input| {
        expand_trace.lock().unwrap().push("expand");
        let trace = expand_trace.clone();
        async move {
            TracedIter::new(
                [
                    Event::First(First(input.0)),
                    Event::First(First(input.0 + 1)),
                    Event::First(First(input.0 + 2)),
                ],
                trace,
                "expand.next",
            )
        }
    });
    let branch_trace = trace.clone();
    bus.transform(move |first: First| {
        branch_trace.lock().unwrap().push("branch");
        let trace = branch_trace.clone();
        async move {
            TracedIter::new(
                [
                    Event::Second(Second {
                        value: first.0,
                        keep: first.0 != 8,
                    }),
                    Event::Second(Second {
                        value: first.0,
                        keep: false,
                    }),
                ],
                trace,
                "branch.next",
            )
        }
    });
    let reduce_trace = trace.clone();
    bus.transform(move |second: Second| {
        reduce_trace.lock().unwrap().push("reduce");
        let trace = reduce_trace.clone();
        async move {
            TracedIter::new(
                core::iter::once(Event::Final(Final(second.value))).take(usize::from(second.keep)),
                trace,
                "reduce.next",
            )
        }
    });

    let mut events = bus.subscribe().unwrap();
    bus.publish(Input(7)).await.unwrap();
    assert!(bus.tick());
    let events = vec![events.recv().await.unwrap(), events.recv().await.unwrap()];
    let trace = trace.lock().unwrap().clone();
    (events, trace)
}

#[tokio::test]
async fn dynamic_matches_static_depth_first_outputs_and_effects() {
    let (static_events, static_trace) = run_static().await;
    let (dynamic_events, dynamic_trace) = run_dynamic().await;
    let expected_events = vec![Event::Final(Final(7)), Event::Final(Final(9))];
    let expected_trace = vec![
        "expand",
        "expand.next",
        "branch",
        "branch.next",
        "reduce",
        "reduce.next",
        "reduce.next",
        "branch.next",
        "reduce",
        "reduce.next",
        "branch.next",
        "expand.next",
        "branch",
        "branch.next",
        "reduce",
        "reduce.next",
        "branch.next",
        "reduce",
        "reduce.next",
        "branch.next",
        "expand.next",
        "branch",
        "branch.next",
        "reduce",
        "reduce.next",
        "reduce.next",
        "branch.next",
        "reduce",
        "reduce.next",
        "branch.next",
        "expand.next",
    ];

    assert_eq!(static_events, expected_events);
    assert_eq!(static_trace, expected_trace);
    assert_eq!(dynamic_events, expected_events);
    assert_eq!(dynamic_trace, expected_trace);
    assert_eq!(dynamic_events, static_events);
    assert_eq!(dynamic_trace, static_trace);
}

#[tokio::test]
async fn dynamic_matches_static_passthrough_drop_and_overflow() {
    let static_bus = Bus::default().transform(|input: Input| async move {
        let count = if input.0 == 0 { 0 } else { 5 };
        core::iter::repeat_n(Event::Final(Final(input.0)), count)
    });
    let mut static_events = static_bus.subscribe().unwrap();

    static_bus.publish(Final(9)).await.unwrap();
    assert!(static_bus.tick());
    assert_eq!(static_events.recv().await.unwrap(), Event::Final(Final(9)));
    static_bus.publish(Input(0)).await.unwrap();
    assert!(!static_bus.tick());
    assert!(matches!(
        static_bus.publish(Input(1)).await,
        Err(PublishError::OutputFull)
    ));
    assert!(!static_bus.tick());

    let dynamic_bus = DynamicBus::<Event>::new();
    dynamic_bus.transform(|input: Input| async move {
        let count = if input.0 == 0 { 0 } else { 5 };
        core::iter::repeat_n(Event::Final(Final(input.0)), count)
    });
    let mut dynamic_events = dynamic_bus.subscribe().unwrap();

    dynamic_bus.publish(Final(9)).await.unwrap();
    assert!(dynamic_bus.tick());
    assert_eq!(dynamic_events.recv().await.unwrap(), Event::Final(Final(9)));
    dynamic_bus.publish(Input(0)).await.unwrap();
    assert!(!dynamic_bus.tick());
    assert!(matches!(
        dynamic_bus.publish(Input(1)).await,
        Err(PublishError::OutputFull)
    ));
    assert!(!dynamic_bus.tick());
}
