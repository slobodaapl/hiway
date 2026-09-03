use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};

#[cfg(feature = "dynamic")]
use hiway::DynamicBus;
use hiway::{stage, Batch, Bus, HiwayEvent, PipelineExt, PublishError, RecvError, SubscribersFull};
use tokio::sync::Notify;

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

#[derive(Clone, Debug, PartialEq, Eq)]
struct Wanted(u8);

#[derive(Debug)]
struct CountedUnrelated {
    value: u8,
    clones: Arc<AtomicUsize>,
}

impl Clone for CountedUnrelated {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            value: self.value,
            clones: self.clones.clone(),
        }
    }
}

#[derive(Clone, Debug, HiwayEvent)]
enum RoutedEvent {
    Unrelated(CountedUnrelated),
    Wanted(Wanted),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Borrowed<'a>(&'a str);

#[derive(Clone, Debug, PartialEq, Eq, HiwayEvent)]
enum BorrowedEvent<'a> {
    Text(Borrowed<'a>),
}

struct WakeCounter {
    calls: AtomicUsize,
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, HiwayEvent)]
enum Event {
    Ui(UiEvent),
    Game(GameEvent),
    Log(LogEvent),
    Signal(Signal),
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum ManualEvent {
    Signal(Signal),
}

impl HiwayEvent for ManualEvent {}

#[test]
fn derive_provides_typed_variant_conversions() {
    let ui = UiEvent {
        text: "click".into(),
    };
    let event: Event = ui.clone().into();

    assert_eq!(event, Event::Ui(ui.clone()));
    assert_eq!(UiEvent::try_from(event), Ok(ui));
    assert!(UiEvent::try_from(Event::Signal(Signal)).is_err());
}

#[test]
fn default_bus_is_const_constructible() {
    let bus: Bus<Event> = const { Bus::new() };
    assert!(bus.subscribe().is_ok());
}

#[tokio::test]
async fn generic_derive_preserves_source_where_clauses() {
    let bus = Bus::<GenericEvent<String>>::new();
    let mut values = bus.consume::<GenericPayload<String>>().unwrap();
    let value = GenericPayload("value".to_owned());

    bus.publish(value.clone()).await.unwrap();
    assert!(bus.tick());
    assert_eq!(values.recv().await.unwrap(), value);
}

#[tokio::test]
async fn default_bus_infers_event_type_and_supports_nonblocking_receive() {
    let bus = Bus::default().transform(async |mut ui: UiEvent| {
        ui.text.push_str("|transformed");
        [ui.into()]
    });
    let mut raw = bus.subscribe().unwrap();
    let mut ui = bus.consume::<UiEvent>().unwrap();

    assert_eq!(raw.try_recv(), Ok(None));
    assert_eq!(ui.try_recv(), Ok(None));

    bus.publish(UiEvent {
        text: "input".into(),
    })
    .await
    .unwrap();
    assert!(bus.tick());
    assert_eq!(
        raw.try_recv(),
        Ok(Some(Event::Ui(UiEvent {
            text: "input|transformed".into()
        })))
    );
    assert_eq!(
        ui.try_recv(),
        Ok(Some(UiEvent {
            text: "input|transformed".into()
        }))
    );
    assert_eq!(raw.try_recv(), Ok(None));
    assert_eq!(ui.try_recv(), Ok(None));
}

#[tokio::test]
async fn manual_event_supports_inferred_whole_event_transform() {
    let bus = Bus::default().transform(async |event: ManualEvent| match event {
        ManualEvent::Signal(_) => [ManualEvent::Signal(Signal)],
    });
    let mut events = bus.subscribe().unwrap();

    bus.publish(ManualEvent::Signal(Signal)).await.unwrap();
    assert!(bus.tick());
    assert_eq!(events.try_recv(), Ok(Some(ManualEvent::Signal(Signal))));
}

#[tokio::test]
async fn publication_waits_for_tick_and_wakes_the_receiver() {
    let bus = Bus::<Event>::new();
    let mut events = bus.subscribe().unwrap();

    bus.publish(UiEvent {
        text: "waiting".into(),
    })
    .await
    .unwrap();

    let wake = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    let mut receiving = Box::pin(events.recv());
    assert!(matches!(
        receiving.as_mut().poll(&mut context),
        Poll::Pending
    ));
    assert!(bus.tick());
    assert_eq!(wake.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        receiving.await.unwrap(),
        Event::Ui(UiEvent {
            text: "waiting".into()
        })
    );
    assert!(!bus.tick());
}

#[test]
fn try_recv_clears_waker_retained_by_cancelled_receive() {
    let bus = Bus::<Event>::new();
    let mut events = bus.subscribe().unwrap();
    let wake = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    let mut receiving = Box::pin(events.recv());

    assert!(matches!(
        receiving.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(receiving);
    drop(waker);
    assert_eq!(Arc::strong_count(&wake), 2);

    assert_eq!(events.try_recv(), Ok(None));
    assert_eq!(Arc::strong_count(&wake), 1);
}

#[tokio::test]
async fn one_tick_commits_every_ready_publication_in_order() {
    let bus = Bus::<Event>::new();
    let mut events = bus.subscribe().unwrap();

    bus.publish(UiEvent {
        text: "first".into(),
    })
    .await
    .unwrap();
    bus.publish(GameEvent {
        text: "second".into(),
    })
    .await
    .unwrap();
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
        Event::Game(GameEvent {
            text: "second".into()
        })
    );
}

#[tokio::test]
async fn typed_stages_expand_then_transform_in_stage_order() {
    let expand = stage(|ui: UiEvent| async move {
        let log = LogEvent {
            text: format!("log:{}", ui.text),
        };
        [Event::Ui(ui), log.into()]
    });
    let suffix = stage(|mut event: Event| async move {
        match &mut event {
            Event::Ui(ui) => ui.text.push_str("|later"),
            Event::Game(game) => game.text.push_str("|later"),
            Event::Log(log) => log.text.push_str("|later"),
            Event::Signal(_) => {}
        }
        [event]
    });
    let bus = Bus::<Event>::with_pipeline(expand.then(suffix));
    let mut raw = bus.subscribe().unwrap();
    let mut logs = bus.consume::<LogEvent>().unwrap();

    bus.publish(UiEvent {
        text: "input".into(),
    })
    .await
    .unwrap();
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
            text: "log:input|later".into()
        })
    );
    assert_eq!(
        logs.recv().await.unwrap(),
        LogEvent {
            text: "log:input|later".into()
        }
    );
}

#[tokio::test]
async fn subscribers_keep_independent_cursors() {
    let pipeline = stage(|ui: UiEvent| async move {
        [
            Event::Ui(ui),
            LogEvent {
                text: "derived".into(),
            }
            .into(),
        ]
    });
    let bus: Bus<Event, 2, 2, 2, 2, _> = Bus::with_pipeline(pipeline);
    let mut first = bus.subscribe().unwrap();
    let mut second = bus.subscribe().unwrap();

    bus.publish(UiEvent { text: "one".into() }).await.unwrap();
    assert!(bus.tick());

    assert!(matches!(first.recv().await.unwrap(), Event::Ui(_)));
    assert!(matches!(second.recv().await.unwrap(), Event::Ui(_)));
    assert!(matches!(second.recv().await.unwrap(), Event::Log(_)));
    assert!(matches!(first.recv().await.unwrap(), Event::Log(_)));
}

#[tokio::test]
async fn completed_publication_is_rejected_whole_when_ready_is_full() {
    let bus = Bus::<Event, 2, 1, 2, 1>::new();
    let mut events = bus.subscribe().unwrap();

    bus.publish(UiEvent { text: "one".into() }).await.unwrap();
    let Err(PublishError::ReadyFull(full)) = bus.publish(UiEvent { text: "two".into() }).await
    else {
        panic!("the second publication must return its prepared batch");
    };
    let retry = full.into_batch();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent { text: "one".into() })
    );

    bus.try_submit(retry).unwrap();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent { text: "two".into() })
    );
}

#[tokio::test]
async fn retrying_ready_full_batch_does_not_replay_transform() {
    let calls = Arc::new(AtomicUsize::new(0));
    let transform_calls = calls.clone();
    let pipeline = stage(move |ui: UiEvent| {
        let call = transform_calls.fetch_add(1, Ordering::SeqCst) + 1;
        async move {
            [Event::Ui(UiEvent {
                text: format!("{}-{call}", ui.text),
            })]
        }
    });
    let bus: Bus<Event, 1, 1, 2, 1, _> = Bus::with_pipeline(pipeline);
    let mut events = bus.subscribe().unwrap();

    bus.publish(UiEvent { text: "one".into() }).await.unwrap();
    let Err(PublishError::ReadyFull(full)) = bus.publish(UiEvent { text: "two".into() }).await
    else {
        panic!("the second publication must return its prepared batch");
    };
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    assert!(bus.tick());
    bus.try_submit(full.into_batch()).unwrap();
    assert!(bus.tick());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "one-1".into()
        })
    );
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "two-2".into()
        })
    );
}

#[cfg(feature = "dynamic")]
#[tokio::test]
async fn dynamic_retry_does_not_replay_runtime_transform() {
    let calls = Arc::new(AtomicUsize::new(0));
    let transform_calls = calls.clone();
    let bus: DynamicBus<Event, 1, 1, 2, 1> = DynamicBus::new();
    bus.transform(move |ui: UiEvent| {
        let call = transform_calls.fetch_add(1, Ordering::SeqCst) + 1;
        async move {
            [Event::Ui(UiEvent {
                text: format!("{}-{call}", ui.text),
            })]
        }
    });
    let mut events = bus.subscribe().unwrap();

    bus.publish(UiEvent { text: "one".into() }).await.unwrap();
    let Err(PublishError::ReadyFull(full)) = bus.publish(UiEvent { text: "two".into() }).await
    else {
        panic!("the second publication must return its prepared batch");
    };
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    assert!(bus.tick());
    bus.try_submit(full.into_batch()).unwrap();
    assert!(bus.tick());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        events.try_recv(),
        Ok(Some(Event::Ui(UiEvent {
            text: "one-1".into()
        })))
    );
    assert_eq!(
        events.try_recv(),
        Ok(Some(Event::Ui(UiEvent {
            text: "two-2".into()
        })))
    );
}

#[tokio::test]
async fn typed_routes_skip_unrelated_events_before_clone_storage_and_lag() {
    let clones = Arc::new(AtomicUsize::new(0));
    let bus = Bus::<RoutedEvent, 1, 4, 1, 2>::new();
    let mut raw = bus.subscribe().unwrap();
    let mut wanted = bus.consume::<Wanted>().unwrap();
    let wake = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    let mut receiving = Box::pin(wanted.recv());
    assert!(matches!(
        receiving.as_mut().poll(&mut context),
        Poll::Pending
    ));

    for value in [1, 2] {
        bus.publish(CountedUnrelated {
            value,
            clones: clones.clone(),
        })
        .await
        .unwrap();
        assert!(bus.tick());
        assert!(matches!(
            raw.recv().await.unwrap(),
            RoutedEvent::Unrelated(_)
        ));
    }
    assert_eq!(clones.load(Ordering::SeqCst), 0);
    assert_eq!(wake.calls.load(Ordering::SeqCst), 0);

    bus.publish(Wanted(7)).await.unwrap();
    assert!(bus.tick());
    assert!(matches!(
        raw.recv().await.unwrap(),
        RoutedEvent::Wanted(Wanted(7))
    ));
    assert_eq!(
        receiving.as_mut().poll(&mut context),
        Poll::Ready(Ok(Wanted(7)))
    );
}

#[tokio::test]
async fn fanout_clones_each_event_once_per_additional_matching_subscriber() {
    let clones = Arc::new(AtomicUsize::new(0));
    let bus = Bus::<RoutedEvent, 1, 1, 1, 3>::new();
    let mut first = bus.subscribe().unwrap();
    let mut second = bus.subscribe().unwrap();
    let mut third = bus.subscribe().unwrap();

    bus.publish(CountedUnrelated {
        value: 7,
        clones: clones.clone(),
    })
    .await
    .unwrap();
    assert_eq!(clones.load(Ordering::SeqCst), 2);
    assert!(bus.tick());
    assert!(matches!(
        first.try_recv(),
        Ok(Some(RoutedEvent::Unrelated(_)))
    ));
    assert!(matches!(
        second.try_recv(),
        Ok(Some(RoutedEvent::Unrelated(_)))
    ));
    assert!(matches!(
        third.try_recv(),
        Ok(Some(RoutedEvent::Unrelated(_)))
    ));
}

#[tokio::test]
async fn subscriber_created_after_preparation_misses_publication() {
    let bus = Bus::<Event, 1, 2, 1, 1>::new();
    bus.publish(UiEvent { text: "old".into() }).await.unwrap();
    let mut events = bus.subscribe().unwrap();
    assert!(bus.tick());

    let wake = Arc::new(WakeCounter {
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake);
    let mut context = Context::from_waker(&waker);
    let mut receiving = Box::pin(events.recv());
    assert!(matches!(
        receiving.as_mut().poll(&mut context),
        Poll::Pending
    ));

    bus.publish(UiEvent { text: "new".into() }).await.unwrap();
    assert!(bus.tick());
    assert_eq!(
        receiving.as_mut().poll(&mut context),
        Poll::Ready(Ok(Event::Ui(UiEvent { text: "new".into() })))
    );
}

#[tokio::test]
async fn borrowed_events_do_not_require_static_lifetime() {
    let text = String::from("borrowed");
    let bus = Bus::<BorrowedEvent<'_>>::new();
    let mut events = bus.consume::<Borrowed<'_>>().unwrap();

    bus.publish(Borrowed(text.as_str())).await.unwrap();
    assert!(bus.tick());
    assert_eq!(events.recv().await.unwrap(), Borrowed("borrowed"));
}

#[tokio::test]
async fn zero_subscriber_capacity_still_accepts_and_ticks_publications() {
    let bus = Bus::<Event, 1, 1, 1, 0>::new();
    assert!(matches!(bus.subscribe(), Err(SubscribersFull)));
    assert!(bus.publish(Signal).await.is_ok());
    assert!(bus.tick());
}

#[tokio::test]
async fn transform_output_overflow_publishes_nothing() {
    let pipeline = stage(|ui: UiEvent| async move {
        [
            Event::Ui(ui),
            LogEvent {
                text: "too many".into(),
            }
            .into(),
        ]
    });
    let bus: Bus<Event, 1, 2, 1, 1, _> = Bus::with_pipeline(pipeline);

    assert!(matches!(
        bus.publish(UiEvent {
            text: "input".into()
        })
        .await,
        Err(PublishError::OutputFull)
    ));
    assert!(!bus.tick());
}

#[tokio::test]
async fn subscriber_slots_are_bounded_and_reusable() {
    let bus = Bus::<Event, 1, 1, 1, 1>::new();
    let first = bus.subscribe().unwrap();
    assert!(matches!(bus.subscribe(), Err(SubscribersFull)));
    drop(first);
    assert!(bus.subscribe().is_ok());
}

#[tokio::test]
async fn lag_reports_overwritten_tick_frames() {
    let bus = Bus::<Event, 1, 1, 1, 1>::new();
    let mut events = bus.subscribe().unwrap();

    bus.publish(UiEvent { text: "one".into() }).await.unwrap();
    assert!(bus.tick());
    bus.publish(UiEvent { text: "two".into() }).await.unwrap();
    assert!(bus.tick());

    assert_eq!(events.recv().await, Err(RecvError::Lagged(1)));
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent { text: "two".into() })
    );
}

#[tokio::test]
async fn frame_ring_wraparound_reports_every_overwritten_frame() {
    let bus = Bus::<Event, 1, 1, 2, 1>::new();
    let mut events = bus.subscribe().unwrap();

    for text in ["one", "two", "three", "four"] {
        bus.publish(UiEvent { text: text.into() }).await.unwrap();
        assert!(bus.tick());
    }

    assert_eq!(events.try_recv(), Err(RecvError::Lagged(2)));
    assert_eq!(
        events.try_recv(),
        Ok(Some(Event::Ui(UiEvent {
            text: "three".into()
        })))
    );
    assert_eq!(
        events.try_recv(),
        Ok(Some(Event::Ui(UiEvent {
            text: "four".into()
        })))
    );
    assert_eq!(events.try_recv(), Ok(None));
}

#[tokio::test]
async fn consumed_frames_do_not_count_as_lag() {
    let bus = Bus::<Event, 1, 1, 1, 1>::new();
    let mut events = bus.subscribe().unwrap();

    bus.publish(UiEvent { text: "one".into() }).await.unwrap();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent { text: "one".into() })
    );

    bus.publish(UiEvent { text: "two".into() }).await.unwrap();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent { text: "two".into() })
    );
}

#[tokio::test]
async fn publishing_without_subscribers_is_valid_and_not_replayed() {
    let bus = Bus::<Event>::new();
    bus.publish(Signal).await.unwrap();
    assert!(bus.tick());

    let mut events = bus.subscribe().unwrap();
    bus.publish(UiEvent { text: "new".into() }).await.unwrap();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent { text: "new".into() })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suspended_transform_does_not_block_other_publishers_or_tick() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let transform_entered = entered.clone();
    let transform_release = release.clone();
    let pipeline = stage(move |event: Event| {
        let entered = transform_entered.clone();
        let release = transform_release.clone();
        async move {
            match &event {
                Event::Ui(ui) if ui.text == "slow" => {
                    entered.notify_one();
                    release.notified().await;
                }
                _ => {}
            }
            [event]
        }
    });
    let bus = Arc::new(Bus::<Event, 1, 2, 2, 1, _>::with_pipeline(pipeline));
    let mut events = bus.subscribe().unwrap();

    let slow_bus = bus.clone();
    let slow = tokio::spawn(async move {
        slow_bus
            .publish(UiEvent {
                text: "slow".into(),
            })
            .await
    });
    entered.notified().await;

    let fast_bus = bus.clone();
    let fast_done = Arc::new(Notify::new());
    let fast_finished = fast_done.clone();
    let fast = tokio::spawn(async move {
        let result = fast_bus
            .publish(UiEvent {
                text: "fast".into(),
            })
            .await;
        fast_finished.notify_one();
        result
    });
    fast_done.notified().await;
    fast.await.unwrap().unwrap();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "fast".into()
        })
    );

    release.notify_one();
    slow.await.unwrap().unwrap();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "slow".into()
        })
    );
}

#[cfg(feature = "dynamic")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_pipeline_snapshots_runtime_stages_before_awaiting() {
    let bus = Arc::new(DynamicBus::<Event>::new());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let transform_entered = entered.clone();
    let transform_release = release.clone();

    bus.transform(move |mut event: Event| {
        let entered = transform_entered.clone();
        let release = transform_release.clone();
        async move {
            entered.notify_one();
            release.notified().await;
            if let Event::Ui(ui) = &mut event {
                ui.text.push_str("|first");
            }
            [event]
        }
    });

    let mut events = bus.subscribe().unwrap();
    let publishing = bus.clone();
    let first =
        tokio::spawn(async move { publishing.publish(UiEvent { text: "one".into() }).await });
    entered.notified().await;

    bus.transform(|mut event: Event| async move {
        if let Event::Ui(ui) = &mut event {
            ui.text.push_str("|second");
        }
        [event]
    });
    release.notify_one();
    first.await.unwrap().unwrap();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "one|first".into()
        })
    );

    release.notify_one();
    bus.publish(UiEvent { text: "two".into() }).await.unwrap();
    assert!(bus.tick());
    assert_eq!(
        events.recv().await.unwrap(),
        Event::Ui(UiEvent {
            text: "two|first|second".into()
        })
    );
}

#[test]
fn batch_collection_never_truncates() {
    assert_eq!(Batch::<u8, 2>::try_from_iter([1, 2]).unwrap().len(), 2);
    assert!(Batch::<u8, 1>::try_from_iter([1, 2]).is_err());
}
