use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use hiway::{Bus, Hiway, HiwayEvent, RecvError};
use tokio::{
    sync::Notify,
    time::{timeout, Duration},
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct UiEvent {
    text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GameEvent {
    text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogEvent {
    text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Signal;

#[derive(Clone, Debug, PartialEq, Eq, HiwayEvent)]
enum Event {
    Ui(UiEvent),
    Game(GameEvent),
    Log(LogEvent),
    Signal(Signal),
}

#[derive(Clone, Debug, PartialEq, Eq, HiwayEvent)]
enum OtherEvent {
    Value(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GenericPayload<T>(T);

#[derive(Clone, Debug, PartialEq, Eq, HiwayEvent)]
enum GenericEvent<T>
where
    T: std::fmt::Display,
{
    Value(GenericPayload<T>),
}

#[test]
fn derive_provides_variant_conversions() {
    let ui = UiEvent {
        text: "click".into(),
    };
    let event: Event = ui.clone().into();

    assert_eq!(event, Event::Ui(ui.clone()));
    assert_eq!(UiEvent::try_from(event), Ok(ui));
    assert_eq!(
        UiEvent::try_from(Event::Game(GameEvent {
            text: "move".into()
        })),
        Err(Event::Game(GameEvent {
            text: "move".into()
        }))
    );
}

#[tokio::test]
async fn generic_derive_preserves_source_where_clauses() {
    let bus = Bus::<GenericEvent<String>>::new();
    let mut values = bus.consume::<GenericPayload<String>>();
    let value = GenericPayload("value".to_owned());

    bus.publish(value.clone()).await;
    assert!(bus.tick());

    assert_eq!(values.recv().await.unwrap(), value);
    assert_eq!(
        GenericPayload::<String>::try_from(GenericEvent::<String>::Value(value.clone())),
        Ok(value),
    );
}

#[tokio::test]
async fn direct_payload_publish_reaches_independent_subscribers() {
    let bus = Bus::<Event>::new();
    let mut raw = bus.subscribe();
    let mut ui = bus.consume::<UiEvent>();
    let mut game = bus.consume::<GameEvent>();

    bus.publish(UiEvent {
        text: "click".into(),
    })
    .await;
    assert!(bus.tick());
    bus.publish(GameEvent {
        text: "move".into(),
    })
    .await;
    assert!(bus.tick());

    assert_eq!(
        raw.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "click".into()
        })
    );
    assert_eq!(
        raw.recv().await.unwrap(),
        Event::Game(GameEvent {
            text: "move".into()
        })
    );
    assert_eq!(
        ui.recv().await.unwrap(),
        UiEvent {
            text: "click".into()
        }
    );
    assert_eq!(
        game.recv().await.unwrap(),
        GameEvent {
            text: "move".into()
        }
    );
}

#[tokio::test]
async fn one_global_transform_preserves_original_and_emits_log_once() {
    let bus = Bus::<Event>::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    bus.transform(move |event: Event| {
        counted.fetch_add(1, Ordering::SeqCst);
        let derived = match &event {
            Event::Ui(ui) => Some(LogEvent {
                text: format!("ui:{}", ui.text),
            }),
            Event::Game(game) => Some(LogEvent {
                text: format!("game:{}", game.text),
            }),
            Event::Log(_) | Event::Signal(_) => None,
        };
        let mut events = vec![event];
        if let Some(derived) = derived {
            events.push(derived.into());
        }
        events
    });

    let mut raw = bus.subscribe();
    let mut ui = bus.consume::<UiEvent>();
    let mut game = bus.consume::<GameEvent>();
    let mut logs = bus.consume::<LogEvent>();

    bus.publish(UiEvent {
        text: "click".into(),
    })
    .await;
    assert!(bus.tick());
    bus.publish(GameEvent {
        text: "move".into(),
    })
    .await;
    assert!(bus.tick());

    assert_eq!(
        raw.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "click".into()
        })
    );
    assert_eq!(
        raw.recv().await.unwrap(),
        Event::Log(LogEvent {
            text: "ui:click".into()
        })
    );
    assert_eq!(
        raw.recv().await.unwrap(),
        Event::Game(GameEvent {
            text: "move".into()
        })
    );
    assert_eq!(
        raw.recv().await.unwrap(),
        Event::Log(LogEvent {
            text: "game:move".into()
        })
    );
    assert_eq!(
        ui.recv().await.unwrap(),
        UiEvent {
            text: "click".into()
        }
    );
    assert_eq!(
        game.recv().await.unwrap(),
        GameEvent {
            text: "move".into()
        }
    );
    assert_eq!(
        logs.recv().await.unwrap(),
        LogEvent {
            text: "ui:click".into()
        }
    );
    assert_eq!(
        logs.recv().await.unwrap(),
        LogEvent {
            text: "game:move".into()
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn publish_prepares_without_delivery_until_tick() {
    let bus = Bus::<Event>::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    bus.transform(move |event: Event| {
        counted.fetch_add(1, Ordering::SeqCst);
        vec![event]
    });
    let mut events = bus.subscribe();

    bus.publish(UiEvent {
        text: "waiting".into(),
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    assert!(timeout(Duration::from_millis(10), events.recv())
        .await
        .is_err());
    assert!(bus.tick());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "waiting".into()
        })
    );
    assert!(!bus.tick());
}

#[tokio::test]
async fn one_tick_commits_every_ready_publication_as_one_frame() {
    let bus = Bus::<Event>::new();
    bus.transform(|ui: UiEvent| {
        vec![
            ui.into(),
            LogEvent {
                text: "derived".into(),
            }
            .into(),
        ]
    });

    let mut events = bus.subscribe();
    bus.publish(UiEvent {
        text: "first".into(),
    })
    .await;
    bus.publish(UiEvent {
        text: "second".into(),
    })
    .await;

    assert!(bus.tick());
    assert!(!bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "first".into()
        })
    );
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Log(LogEvent {
            text: "derived".into()
        })
    );
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "second".into()
        })
    );
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Log(LogEvent {
            text: "derived".into()
        })
    );
}

#[tokio::test]
async fn emitted_events_visit_later_stages_in_order() {
    let bus = Bus::<Event>::new();
    let first_calls = Arc::new(AtomicUsize::new(0));
    let later_calls = Arc::new(AtomicUsize::new(0));

    let first_counted = first_calls.clone();
    bus.transform(move |event: Event| {
        first_counted.fetch_add(1, Ordering::SeqCst);
        match event {
            Event::Ui(ui) => vec![
                Event::Ui(ui),
                LogEvent {
                    text: "scrubbed".into(),
                }
                .into(),
            ],
            other => vec![other],
        }
    });

    let later_counted = later_calls.clone();
    bus.transform(move |mut event: Event| {
        later_counted.fetch_add(1, Ordering::SeqCst);
        match &mut event {
            Event::Ui(ui) => ui.text.push_str("|later"),
            Event::Log(log) => log.text.push_str("|later"),
            Event::Game(game) => game.text.push_str("|later"),
            Event::Signal(_) => {}
        }
        vec![event]
    });

    let mut raw = bus.subscribe();
    bus.publish(UiEvent {
        text: "input".into(),
    })
    .await;
    assert!(bus.tick());

    assert_eq!(
        raw.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "input|later".into()
        })
    );
    assert_eq!(
        raw.recv().await.unwrap(),
        Event::Log(LogEvent {
            text: "scrubbed|later".into()
        })
    );
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(later_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn variant_transforms_target_payloads_and_preserve_other_variants() {
    let bus = Bus::<Event>::new();
    bus.transform(|mut ui: UiEvent| {
        ui.text.push_str("|mapped");
        vec![ui.into()]
    });
    bus.transform(|ui: UiEvent| {
        vec![
            ui.clone().into(),
            LogEvent {
                text: format!("ui:{}", ui.text),
            }
            .into(),
        ]
    });
    bus.transform(|game: GameEvent| {
        if game.text == "drop" {
            Vec::new()
        } else {
            vec![game.into()]
        }
    });

    let mut events = bus.subscribe();
    bus.publish(UiEvent {
        text: "input".into(),
    })
    .await;
    assert!(bus.tick());
    bus.publish(GameEvent {
        text: "drop".into(),
    })
    .await;
    assert!(!bus.tick());

    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "input|mapped".into()
        })
    );
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Log(LogEvent {
            text: "ui:input|mapped".into()
        })
    );
}

#[tokio::test]
async fn transform_infers_bus_input_and_mixed_outputs() {
    let bus = Bus::new();
    bus.transform(|mut ui: UiEvent| {
        ui.text.push_str("|mutated");
        let log = LogEvent {
            text: format!("ui:{}", ui.text),
        };
        vec![ui.into(), log.into()]
    });

    let mut events = bus.subscribe();
    bus.publish(UiEvent {
        text: "input".into(),
    })
    .await;
    assert!(bus.tick());
    bus.publish(Signal).await;
    assert!(bus.tick());

    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "input|mutated".into()
        })
    );
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Log(LogEvent {
            text: "ui:input|mutated".into()
        })
    );
    assert_eq!(events.recv().await.unwrap(), Event::Signal(Signal));
}

#[tokio::test]
async fn async_transforms_preserve_explicit_original_events() {
    let bus = Bus::<Event>::new();
    bus.transform_async(|event: Event| async move {
        match event {
            Event::Ui(ui) => {
                let log = LogEvent {
                    text: format!("async:{}", ui.text),
                };
                vec![Event::Ui(ui), log.into()]
            }
            other => vec![other],
        }
    });
    bus.transform_async(|ui: UiEvent| async move {
        let log = LogEvent {
            text: format!("variant:{}", ui.text),
        };
        vec![ui.into(), log.into()]
    });

    let mut events = bus.subscribe();
    bus.publish(UiEvent {
        text: "input".into(),
    })
    .await;
    assert!(bus.tick());

    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "input".into()
        })
    );
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Log(LogEvent {
            text: "variant:input".into()
        })
    );
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Log(LogEvent {
            text: "async:input".into()
        })
    );
}

#[tokio::test]
async fn global_filter_drops_events_for_every_subscriber() {
    let bus = Bus::<Event>::new();
    bus.transform(|event: Event| match event {
        Event::Game(_) => Vec::new(),
        other => vec![other],
    });

    let mut first = bus.subscribe();
    let mut second = bus.subscribe();
    bus.publish(GameEvent {
        text: "hidden".into(),
    })
    .await;
    assert!(!bus.tick());
    bus.publish(UiEvent {
        text: "visible".into(),
    })
    .await;
    assert!(bus.tick());

    let expected = Event::Ui(UiEvent {
        text: "visible".into(),
    });
    assert_eq!(first.recv().await.unwrap(), expected);
    assert_eq!(second.recv().await.unwrap(), expected);
}

#[tokio::test]
async fn async_filter_waits_and_controls_delivery() {
    let bus = Bus::<Event>::new();
    bus.transform_async(|event: Event| async move {
        match event {
            Event::Ui(ui) if ui.text == "keep" => vec![Event::Ui(ui)],
            _ => Vec::new(),
        }
    });

    let mut events = bus.subscribe();
    bus.publish(UiEvent {
        text: "drop".into(),
    })
    .await;
    assert!(!bus.tick());
    bus.publish(UiEvent {
        text: "keep".into(),
    })
    .await;
    assert!(bus.tick());

    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "keep".into()
        })
    );
}

#[tokio::test]
async fn blocked_transform_does_not_block_another_publication_or_tick() {
    let bus = Bus::<Event>::new();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let transform_entered = entered.clone();
    let transform_release = release.clone();
    bus.transform_async(move |event: Event| {
        let entered = transform_entered.clone();
        let release = transform_release.clone();
        async move {
            match event {
                Event::Ui(ui) if ui.text == "slow" => {
                    entered.notify_one();
                    release.notified().await;
                    vec![Event::Ui(ui)]
                }
                other => vec![other],
            }
        }
    });

    let mut events = bus.subscribe();
    let slow_bus = bus.clone();
    let slow_publish = tokio::spawn(async move {
        slow_bus
            .publish(UiEvent {
                text: "slow".into(),
            })
            .await;
    });
    entered.notified().await;

    let fast_bus = bus.clone();
    let mut fast_publish = tokio::spawn(async move {
        fast_bus
            .publish(UiEvent {
                text: "fast".into(),
            })
            .await;
    });
    match timeout(Duration::from_secs(1), &mut fast_publish).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            release.notify_one();
            slow_publish.await.unwrap();
            panic!("fast publication task failed: {error}");
        }
        Err(_) => {
            release.notify_one();
            slow_publish.await.unwrap();
            let _ = fast_publish.await;
            panic!("fast publication remained blocked by slow transform");
        }
    }

    let tick_bus = bus.clone();
    let mut ticking = tokio::task::spawn_blocking(move || tick_bus.tick());
    let ticked = match timeout(Duration::from_secs(1), &mut ticking).await {
        Ok(Ok(ticked)) => ticked,
        Ok(Err(error)) => {
            release.notify_one();
            slow_publish.await.unwrap();
            panic!("tick task failed: {error}");
        }
        Err(_) => {
            release.notify_one();
            slow_publish.await.unwrap();
            let _ = ticking.await;
            panic!("tick remained blocked while slow transform was pending");
        }
    };
    if !ticked {
        release.notify_one();
        slow_publish.await.unwrap();
        panic!("tick did not commit the prepared fast publication");
    }

    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "fast".into()
        })
    );

    release.notify_one();
    slow_publish.await.unwrap();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "slow".into()
        })
    );
}

#[tokio::test]
async fn transform_append_is_snapshotted_per_publication() {
    let bus = Bus::<Event>::new();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let transform_entered = entered.clone();
    let transform_release = release.clone();
    let transform_invocations = invocations.clone();

    bus.transform_async(move |mut event: Event| {
        let entered = transform_entered.clone();
        let release = transform_release.clone();
        let invocations = transform_invocations.clone();
        async move {
            if invocations.fetch_add(1, Ordering::SeqCst) == 0 {
                entered.notify_one();
                release.notified().await;
            }
            if let Event::Ui(ui) = &mut event {
                ui.text.push_str("|first");
            }
            vec![event]
        }
    });

    let mut events = bus.subscribe();
    let publishing = bus.clone();
    let first_publication = tokio::spawn(async move {
        publishing.publish(UiEvent { text: "one".into() }).await;
    });

    entered.notified().await;
    bus.transform(|mut event: Event| {
        if let Event::Ui(ui) = &mut event {
            ui.text.push_str("|second");
        }
        vec![event]
    });
    release.notify_one();
    first_publication.await.unwrap();
    assert!(bus.tick());

    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "one|first".into()
        })
    );

    bus.publish(UiEvent { text: "two".into() }).await;
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "two|first|second".into()
        })
    );
}

#[tokio::test]
async fn registry_keeps_event_types_independent() {
    let hiway = Hiway::new();
    let events: Bus<Event> = hiway.bus();
    let other: Bus<OtherEvent> = hiway.bus();
    let mut event_subscriber = events.subscribe();
    let mut other_subscriber = other.subscribe();

    events.publish(Signal).await;
    assert!(events.tick());
    other.publish(OtherEvent::Value("other".into())).await;
    assert!(other.tick());

    assert_eq!(
        event_subscriber.recv().await.unwrap(),
        Event::Signal(Signal)
    );
    assert_eq!(
        other_subscriber.recv().await.unwrap(),
        OtherEvent::Value("other".into())
    );
}

#[tokio::test]
async fn registry_reuses_the_typed_bus_and_transform_configuration() {
    let hiway = Hiway::new();
    let first = hiway.bus();
    let second: Bus<Event> = hiway.bus();
    first.transform(|mut event: Event| {
        if let Event::Ui(ui) = &mut event {
            ui.text.push_str("|shared");
        }
        vec![event]
    });

    let mut events = second.subscribe();
    second
        .publish(UiEvent {
            text: "input".into(),
        })
        .await;
    assert!(second.tick());

    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "input|shared".into()
        })
    );
}

#[tokio::test]
async fn lag_is_reported_by_raw_subscriptions() {
    let bus = Bus::<Event>::with_capacity(1);
    bus.transform(|ui: UiEvent| {
        vec![
            ui.into(),
            LogEvent {
                text: "derived".into(),
            }
            .into(),
        ]
    });
    let mut events = bus.subscribe();
    bus.publish(UiEvent { text: "one".into() }).await;
    bus.publish(UiEvent {
        text: "one-more".into(),
    })
    .await;
    assert!(bus.tick());
    bus.publish(UiEvent { text: "two".into() }).await;
    assert!(bus.tick());

    assert_eq!(events.recv().await, Err(RecvError::Lagged(1)));
}
