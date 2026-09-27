#![no_std]

use hiway::{events, EventSpec, EventValue};

#[derive(Clone)]
struct Reading(u16);

#[events(wire_major = 2, schema_revision = 4)]
enum EmbeddedEvents {
    Ready,
    Reading(Reading),
    PreviousReading(Reading),
}

const _: hiway::EventId = embedded_events::Reading::ID;

#[test]
fn explicit_enum_adapter_round_trips_payload() {
    let event: EmbeddedEvents = embedded_events::Reading(Reading(7)).into();
    let tagged = EventValue::<embedded_events::Reading>::try_from(event)
        .ok()
        .unwrap();
    assert_eq!(tagged.into_inner().0, 7);
}

#[test]
fn multi_event_graph_uses_caller_owned_storage() {
    assert_eq!(hiway_tests::exercise_static_graph(), Some((42, 90)));
}

mod inferred_ports {
    use hiway::{
        events, graph, port, PortError, StaticStream, StreamItem, TopicError, TrySendError,
    };

    #[events]
    enum Events {
        First(u16),
        Second(u16),
    }

    #[graph(first = (events::First, 1, 1, 1), second = (events::Second, 1, 1, 1))]
    struct Graph;

    #[port(send(events::First), required(events::First))]
    struct Duplex;
    #[port(send(events::First))]
    struct Publisher;
    #[port(recv(events::First, events::Second))]
    struct Observer;
    #[port(required(events::Second))]
    struct Second;
    #[port]
    struct Empty;

    #[test]
    fn inferred_port_outlives_graph_and_preserves_required_backpressure() {
        let first = StaticStream::new();
        let second = StaticStream::new();
        let port = Duplex::bind(&Graph::new(&first, &second)).unwrap();
        port.publish_now(events::First(17)).unwrap();
        assert!(matches!(
            port.publish_now_first(18),
            Err(TrySendError::Full(18))
        ));
        assert!(
            matches!(port.recv_now_first().unwrap(), Some(StreamItem::Data { value, .. }) if *value == 17)
        );
        port.publish_now_first(19).unwrap();
        assert!(
            matches!(port.recv_now::<events::First>().unwrap(), Some(StreamItem::Data { value, .. }) if *value == 19)
        );
        let _empty = Empty::bind(&()).unwrap();
    }

    #[test]
    fn failed_bind_releases_prior_subscription_and_observers_allow_overwrite() {
        let first = StaticStream::new();
        let second = StaticStream::new();
        let graph = Graph::new(&first, &second);
        let occupied = Second::bind(&graph).unwrap();
        assert!(matches!(
            Observer::bind(&graph),
            Err(PortError::Topic(TopicError::Capacity))
        ));
        let recovered = Duplex::bind(&graph).unwrap();
        drop(recovered);
        drop(occupied);
        let observer = Observer::bind(&graph).unwrap();
        let publisher = Publisher::bind(&graph).unwrap();
        publisher.publish_now_first(1).unwrap();
        publisher.publish_now_first(2).unwrap();
        assert!(matches!(
            observer.recv_now_first().unwrap(),
            Some(StreamItem::Gap { .. })
        ));
        assert!(
            matches!(observer.recv_now_first().unwrap(), Some(StreamItem::Data { value, .. }) if *value == 2)
        );
    }
}
