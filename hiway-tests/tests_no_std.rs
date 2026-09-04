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
