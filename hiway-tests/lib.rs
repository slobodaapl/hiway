#![no_std]

use hiway::{events, graph, EventReceiver, StaticStream, StreamItem, SubscriptionRole};

#[events]
enum EmbeddedEvents {
    Position(u16),
    Health(u8),
}

#[graph(
    position = (embedded_events::Position, 2),
    health = (embedded_events::Health, 2),
)]
struct EmbeddedGraph;

/// Compiled for a target with neither a system allocator nor std.
#[must_use]
pub fn exercise_static_graph() -> Option<(u16, u8)> {
    let positions = StaticStream::<embedded_events::Position, 2>::new();
    let health = StaticStream::<embedded_events::Health, 2>::new();
    let graph = EmbeddedGraph::new(&positions, &health);
    let position = graph
        .subscribe::<embedded_events::Position>(SubscriptionRole::Required)
        .ok()?;
    let health = graph
        .subscribe::<embedded_events::Health>(SubscriptionRole::Observer)
        .ok()?;
    graph
        .sender::<embedded_events::Position>()
        .ok()?
        .send_now(42)
        .ok()?;
    graph
        .sender::<embedded_events::Health>()
        .ok()?
        .send_now(90)
        .ok()?;
    match (
        position.event_recv_now().ok()??,
        health.event_recv_now().ok()??,
    ) {
        (
            StreamItem::Data {
                value: position, ..
            },
            StreamItem::Data { value: health, .. },
        ) => Some((*position, *health)),
        _ => None,
    }
}
