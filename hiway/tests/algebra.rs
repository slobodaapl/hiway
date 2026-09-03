use std::sync::{Arc, Mutex};

use hiway::{stage, Batch, HiwayEvent, Identity, OutputFull, Pipeline, PipelineExt};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Input(u8);

#[derive(Clone, Debug, PartialEq, Eq)]
struct First(u8);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Second {
    value: u8,
    discarded: bool,
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

fn traced_first(
    trace: Arc<Mutex<Vec<&'static str>>>,
) -> impl Fn(Input) -> std::future::Ready<TracedIter> + Sync {
    move |input| {
        trace.lock().unwrap().push("a.call");
        std::future::ready(TracedIter::new(
            [
                Event::First(First(input.0)),
                Event::First(First(input.0 + 1)),
            ],
            trace.clone(),
            "a.next",
        ))
    }
}

fn traced_second(
    trace: Arc<Mutex<Vec<&'static str>>>,
) -> impl Fn(First) -> std::future::Ready<TracedIter> + Sync {
    move |first| {
        let (call, next) = if first.0 == 0 {
            ("b0.call", "b0.next")
        } else {
            ("b1.call", "b1.next")
        };
        trace.lock().unwrap().push(call);
        std::future::ready(TracedIter::new(
            [Event::Second(Second {
                value: first.0,
                discarded: false,
            })],
            trace.clone(),
            next,
        ))
    }
}

fn traced_third(
    trace: Arc<Mutex<Vec<&'static str>>>,
) -> impl Fn(Second) -> std::future::Ready<TracedIter> + Sync {
    move |second| {
        let (call, next) = if second.value == 0 {
            ("c0.call", "c0.next")
        } else {
            ("c1.call", "c1.next")
        };
        trace.lock().unwrap().push(call);
        std::future::ready(TracedIter::new(
            [Event::Final(Final(second.value))],
            trace.clone(),
            next,
        ))
    }
}

async fn apply<P>(pipeline: P, event: Event) -> Result<Batch<Event, 2>, OutputFull>
where
    P: Pipeline<Event, 2>,
{
    pipeline.apply(event).await
}

#[tokio::test]
async fn identity_is_left_and_right_identity() {
    let left = Identity.then(stage(|input: Input| async move {
        [Event::Final(Final(input.0 + 1))]
    }));
    let right =
        stage(|input: Input| async move { [Event::Final(Final(input.0 + 1))] }).then(Identity);
    let expected = Batch::try_from_iter([Event::Final(Final(8))]).unwrap();

    assert_eq!(
        apply(left, Event::Input(Input(7))).await,
        Ok(expected.clone())
    );
    assert_eq!(apply(right, Event::Input(Input(7))).await, Ok(expected));
}

#[tokio::test]
async fn associativity_preserves_expansion_and_reduction_order() {
    let left = stage(|input: Input| async move {
        [
            Event::First(First(input.0)),
            Event::First(First(input.0 + 1)),
        ]
    })
    .then(stage(|first: First| async move {
        [
            Event::Second(Second {
                value: first.0,
                discarded: false,
            }),
            Event::Second(Second {
                value: first.0,
                discarded: true,
            }),
        ]
    }))
    .then(stage(|second: Second| async move {
        core::iter::once(Event::Final(Final(second.value))).take(usize::from(!second.discarded))
    }));

    let right = stage(|input: Input| async move {
        [
            Event::First(First(input.0)),
            Event::First(First(input.0 + 1)),
        ]
    })
    .then(
        stage(|first: First| async move {
            [
                Event::Second(Second {
                    value: first.0,
                    discarded: false,
                }),
                Event::Second(Second {
                    value: first.0,
                    discarded: true,
                }),
            ]
        })
        .then(stage(|second: Second| async move {
            core::iter::once(Event::Final(Final(second.value))).take(usize::from(!second.discarded))
        })),
    );
    let expected = Batch::try_from_iter([Event::Final(Final(0)), Event::Final(Final(1))]).unwrap();

    let left_result = apply(left, Event::Input(Input(0))).await;
    let right_result = apply(right, Event::Input(Input(0))).await;

    assert_eq!(left_result, Ok(expected.clone()));
    assert_eq!(right_result, Ok(expected));
    assert_eq!(left_result, right_result);
}

#[tokio::test]
async fn grouping_preserves_lazy_depth_first_iterator_effects() {
    let left_trace = Arc::new(Mutex::new(Vec::new()));
    let left = stage(traced_first(left_trace.clone()))
        .then(stage(traced_second(left_trace.clone())))
        .then(stage(traced_third(left_trace.clone())));
    let right_trace = Arc::new(Mutex::new(Vec::new()));
    let right = stage(traced_first(right_trace.clone())).then(
        stage(traced_second(right_trace.clone())).then(stage(traced_third(right_trace.clone()))),
    );
    let expected_batch =
        Batch::try_from_iter([Event::Final(Final(0)), Event::Final(Final(1))]).unwrap();
    let expected_trace = [
        "a.call", "a.next", "b0.call", "b0.next", "c0.call", "c0.next", "c0.next", "b0.next",
        "a.next", "b1.call", "b1.next", "c1.call", "c1.next", "c1.next", "b1.next", "a.next",
    ];

    assert_eq!(
        apply(left, Event::Input(Input(0))).await,
        Ok(expected_batch.clone())
    );
    assert_eq!(
        apply(right, Event::Input(Input(0))).await,
        Ok(expected_batch)
    );
    assert_eq!(left_trace.lock().unwrap().as_slice(), expected_trace);
    assert_eq!(right_trace.lock().unwrap().as_slice(), expected_trace);
}

#[tokio::test]
async fn associativity_preserves_user_transform_trace() {
    let left_trace = Arc::new(Mutex::new(Vec::new()));
    let left_expand_trace = left_trace.clone();
    let left_first_trace = left_trace.clone();
    let left_second_trace = left_trace.clone();
    let left = stage(move |input: Input| {
        left_expand_trace.lock().unwrap().push("expand");
        async move {
            [
                Event::First(First(input.0)),
                Event::First(First(input.0 + 1)),
            ]
        }
    })
    .then(stage(move |first: First| {
        left_first_trace.lock().unwrap().push("branch");
        async move {
            [
                Event::Second(Second {
                    value: first.0,
                    discarded: false,
                }),
                Event::Second(Second {
                    value: first.0,
                    discarded: true,
                }),
            ]
        }
    }))
    .then(stage(move |second: Second| {
        left_second_trace.lock().unwrap().push("reduce");
        async move {
            core::iter::once(Event::Final(Final(second.value))).take(usize::from(!second.discarded))
        }
    }));

    let right_trace = Arc::new(Mutex::new(Vec::new()));
    let right_expand_trace = right_trace.clone();
    let right_first_trace = right_trace.clone();
    let right_second_trace = right_trace.clone();
    let right = stage(move |input: Input| {
        right_expand_trace.lock().unwrap().push("expand");
        async move {
            [
                Event::First(First(input.0)),
                Event::First(First(input.0 + 1)),
            ]
        }
    })
    .then(
        stage(move |first: First| {
            right_first_trace.lock().unwrap().push("branch");
            async move {
                [
                    Event::Second(Second {
                        value: first.0,
                        discarded: false,
                    }),
                    Event::Second(Second {
                        value: first.0,
                        discarded: true,
                    }),
                ]
            }
        })
        .then(stage(move |second: Second| {
            right_second_trace.lock().unwrap().push("reduce");
            async move {
                core::iter::once(Event::Final(Final(second.value)))
                    .take(usize::from(!second.discarded))
            }
        })),
    );

    let left_result = apply(left, Event::Input(Input(0))).await;
    let right_result = apply(right, Event::Input(Input(0))).await;
    let expected_trace = [
        "expand", "branch", "reduce", "reduce", "branch", "reduce", "reduce",
    ];

    assert_eq!(left_result, right_result);
    assert_eq!(
        left_trace.lock().unwrap().as_slice(),
        expected_trace.as_slice()
    );
    assert_eq!(
        right_trace.lock().unwrap().as_slice(),
        expected_trace.as_slice()
    );
}

#[tokio::test]
async fn intermediate_expansion_can_exceed_final_capacity() {
    let pipeline = stage(|input: Input| async move {
        [
            Event::First(First(input.0)),
            Event::First(First(input.0 + 1)),
            Event::First(First(input.0 + 2)),
        ]
    })
    .then(stage(|first: First| async move {
        core::iter::once(Event::Final(Final(first.0))).take(usize::from(first.0 == 10))
    }));

    let result = apply(pipeline, Event::Input(Input(10))).await;

    assert_eq!(
        result,
        Ok(Batch::try_from_iter([Event::Final(Final(10))]).unwrap())
    );
}

#[tokio::test]
async fn final_output_overflow_returns_only_output_full() {
    let pipeline = stage(|input: Input| async move {
        [
            Event::Final(Final(input.0)),
            Event::Final(Final(input.0 + 1)),
            Event::Final(Final(input.0 + 2)),
        ]
    });

    assert_eq!(
        apply(pipeline, Event::Input(Input(10))).await,
        Err(OutputFull)
    );
}

#[tokio::test]
async fn associativity_preserves_failure_and_effect_trace() {
    let left_trace = Arc::new(Mutex::new(Vec::new()));
    let left_expand = left_trace.clone();
    let left_branch = left_trace.clone();
    let left_emit = left_trace.clone();
    let left = stage(move |input: Input| {
        left_expand.lock().unwrap().push(('a', input.0));
        async move {
            [
                Event::First(First(input.0)),
                Event::First(First(input.0 + 1)),
            ]
        }
    })
    .then(stage(move |first: First| {
        left_branch.lock().unwrap().push(('b', first.0));
        async move {
            [
                Event::Second(Second {
                    value: first.0,
                    discarded: false,
                }),
                Event::Second(Second {
                    value: first.0 + 10,
                    discarded: false,
                }),
            ]
        }
    }))
    .then(stage(move |second: Second| {
        left_emit.lock().unwrap().push(('c', second.value));
        async move { [Event::Final(Final(second.value))] }
    }));

    let right_trace = Arc::new(Mutex::new(Vec::new()));
    let right_expand = right_trace.clone();
    let right_branch = right_trace.clone();
    let right_emit = right_trace.clone();
    let right = stage(move |input: Input| {
        right_expand.lock().unwrap().push(('a', input.0));
        async move {
            [
                Event::First(First(input.0)),
                Event::First(First(input.0 + 1)),
            ]
        }
    })
    .then(
        stage(move |first: First| {
            right_branch.lock().unwrap().push(('b', first.0));
            async move {
                [
                    Event::Second(Second {
                        value: first.0,
                        discarded: false,
                    }),
                    Event::Second(Second {
                        value: first.0 + 10,
                        discarded: false,
                    }),
                ]
            }
        })
        .then(stage(move |second: Second| {
            right_emit.lock().unwrap().push(('c', second.value));
            async move { [Event::Final(Final(second.value))] }
        })),
    );
    let expected = [('a', 0), ('b', 0), ('c', 0), ('c', 10), ('b', 1), ('c', 1)];

    assert_eq!(apply(left, Event::Input(Input(0))).await, Err(OutputFull));
    assert_eq!(apply(right, Event::Input(Input(0))).await, Err(OutputFull));
    assert_eq!(left_trace.lock().unwrap().as_slice(), expected.as_slice());
    assert_eq!(right_trace.lock().unwrap().as_slice(), expected.as_slice());
}
