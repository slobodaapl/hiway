#![cfg(feature = "dispatch")]

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use hiway::{dptree, Bus, Dispatcher, HiwayEvent};

#[derive(Clone, Debug, PartialEq, Eq)]
struct NumberEvent {
    value: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TextEvent {
    text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, HiwayEvent)]
enum Event {
    Number(NumberEvent),
    Text(TextEvent),
}

#[tokio::test]
async fn typed_handlers_extract_payloads_and_keep_first_match() {
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    let first_sink = first.clone();
    let second_sink = second.clone();

    let dispatcher = Dispatcher::<Event>::new()
        .on(move |number: NumberEvent| {
            let sink = first_sink.clone();
            async move {
                sink.fetch_add(number.value, Ordering::SeqCst);
            }
        })
        .on(move |number: NumberEvent| {
            let sink = second_sink.clone();
            async move {
                sink.fetch_add(number.value, Ordering::SeqCst);
            }
        });

    assert!(dispatcher.dispatch(NumberEvent { value: 7 }.into()).await);
    assert!(
        !dispatcher
            .dispatch(
                TextEvent {
                    text: "ignored".into()
                }
                .into()
            )
            .await
    );
    assert_eq!(first.load(Ordering::SeqCst), 7);
    assert_eq!(second.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn raw_dptree_branch_supports_case_filter_map_and_dependency() {
    let total = Arc::new(AtomicUsize::new(0));
    let branch = dptree::case![Event::Number(number)]
        .filter(|number: NumberEvent| number.value > 0)
        .map(|number: NumberEvent| number.value)
        .endpoint(|value: usize, sink: Arc<AtomicUsize>| async move {
            sink.fetch_add(value, Ordering::SeqCst);
        });
    let dispatcher = Dispatcher::<Event>::new()
        .with_dependency(total.clone())
        .branch(branch);

    assert!(dispatcher.dispatch(NumberEvent { value: 5 }.into()).await);
    assert!(!dispatcher.dispatch(NumberEvent { value: 0 }.into()).await);
    assert!(
        !dispatcher
            .dispatch(
                TextEvent {
                    text: "ignored".into()
                }
                .into()
            )
            .await
    );
    assert_eq!(total.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn listener_runs_typed_handlers_from_bus_publications() {
    let total = Arc::new(AtomicUsize::new(0));
    let sink = total.clone();
    let bus = Bus::<Event>::new();
    let mut listener = bus.listen().on(move |number: NumberEvent| {
        let sink = sink.clone();
        async move {
            sink.fetch_add(number.value, Ordering::SeqCst);
        }
    });

    bus.publish(NumberEvent { value: 5 }).await;
    assert!(bus.tick());

    assert!(listener.next().await.unwrap());
    assert_eq!(total.load(Ordering::SeqCst), 5);
    let _ = listener.subscription();
    let _ = listener.subscription_mut();
}
