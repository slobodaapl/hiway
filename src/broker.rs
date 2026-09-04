use std::{collections::HashMap, sync::Arc};

use crate::{EventId, EventSpec, OriginId, WireEnvelope, WireMajor};

/// Default bound for origin high-water marks retained by one broker.
pub const DEFAULT_SEEN_ORIGINS: usize = 4096;

/// Opaque subscriber identity used by a transport broker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ClientId(pub u64);

/// Immutable subscriber snapshot captured for one routed envelope.
pub type ClientSnapshot = Arc<[ClientId]>;

/// Routed envelope plus its immutable client snapshot.
pub type RoutedEnvelope<'a> = (WireEnvelope<'a>, Option<ClientSnapshot>);

/// Dynamic wire route key. Payload types are intentionally absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RouteKey {
    /// Event identity.
    pub event: EventId,
    /// Breaking wire generation.
    pub wire_major: WireMajor,
}

impl RouteKey {
    /// Builds the wire route for one statically declared event.
    #[must_use]
    pub const fn for_event<S: EventSpec>() -> Self {
        Self {
            event: S::ID,
            wire_major: S::WIRE_MAJOR,
        }
    }
}

/// Failure while admitting an opaque wire envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrokerError {
    /// The envelope's TTL has expired.
    Expired,
    /// The bounded origin table has no slot for a new origin.
    OriginCapacity,
}

/// Minimal opaque broker core for `hiwayd` or another link driver.
///
/// The deduplication table stores one high-water sequence per origin.
pub struct Broker {
    routes: HashMap<RouteKey, Arc<[ClientId]>>,
    seen_sequences: HashMap<OriginId, u64>,
    max_seen_origins: usize,
    next_commit: u64,
}

impl Broker {
    /// Creates an empty broker.
    #[must_use]
    pub fn new() -> Self {
        Self::with_seen_origin_capacity(DEFAULT_SEEN_ORIGINS)
    }

    /// Creates an empty broker with an explicit deduplication bound.
    #[must_use]
    pub fn with_seen_origin_capacity(max_seen_origins: usize) -> Self {
        assert!(
            max_seen_origins > 0,
            "origin capacity must be greater than zero"
        );
        Self {
            routes: HashMap::new(),
            seen_sequences: HashMap::new(),
            max_seen_origins,
            next_commit: 0,
        }
    }

    /// Returns the number of retained origin high-water marks.
    #[must_use]
    pub fn seen_origin_count(&self) -> usize {
        self.seen_sequences.len()
    }

    /// Forgets one origin after its producer is known to be gone.
    pub fn forget_origin(&mut self, origin: OriginId) -> bool {
        self.seen_sequences.remove(&origin).is_some()
    }

    /// Adds one client interest declaration.
    pub fn subscribe(&mut self, route: RouteKey, client: ClientId) {
        let clients = self.routes.get(&route).cloned().unwrap_or_default();
        if clients.contains(&client) {
            return;
        }
        let mut next = Vec::with_capacity(clients.len() + 1);
        next.extend(clients.iter().copied());
        next.push(client);
        self.routes.insert(route, Arc::from(next));
    }

    /// Removes one client interest declaration.
    pub fn unsubscribe(&mut self, route: RouteKey, client: ClientId) {
        let Some(clients) = self.routes.get(&route).cloned() else {
            return;
        };
        let mut next = Vec::with_capacity(clients.len());
        next.extend(
            clients
                .iter()
                .copied()
                .filter(|candidate| *candidate != client),
        );
        if next.is_empty() {
            self.routes.remove(&route);
        } else if next.len() != clients.len() {
            self.routes.insert(route, Arc::from(next));
        }
    }

    /// Returns whether any client currently wants this route.
    #[must_use]
    pub fn has_interest(&self, route: RouteKey) -> bool {
        self.routes
            .get(&route)
            .is_some_and(|clients| !clients.is_empty())
    }

    /// Removes every route owned by one disconnected client.
    pub fn remove_client(&mut self, client: ClientId) {
        let routes = self.routes.keys().copied().collect::<Vec<_>>();
        for route in routes {
            self.unsubscribe(route, client);
        }
    }

    /// Admits one envelope and returns one immutable client snapshot.
    ///
    /// The returned envelope borrows the input payload. This split form lets a
    /// daemon encode the routed envelope once and write the same bytes to all
    /// clients in the snapshot.
    pub fn route_snapshot<'a>(
        &mut self,
        envelope: WireEnvelope<'a>,
    ) -> Result<RoutedEnvelope<'a>, BrokerError> {
        let mut metadata = envelope.metadata;
        if !metadata.consume_hop() {
            return Err(BrokerError::Expired);
        }
        if let Some(last_sequence) = self.seen_sequences.get(&metadata.origin) {
            if metadata.sequence <= *last_sequence {
                return Ok((envelope, None));
            }
        } else if self.seen_sequences.len() >= self.max_seen_origins {
            return Err(BrokerError::OriginCapacity);
        }
        self.seen_sequences
            .insert(metadata.origin, metadata.sequence);

        self.next_commit = self.next_commit.wrapping_add(1);
        metadata.fabric_sequence = Some(self.next_commit);
        let routed = WireEnvelope {
            event: envelope.event,
            wire_major: envelope.wire_major,
            schema_revision: envelope.schema_revision,
            metadata,
            payload: envelope.payload,
        };
        let route = RouteKey {
            event: routed.event,
            wire_major: routed.wire_major,
        };
        Ok((routed, self.routes.get(&route).cloned()))
    }

    /// Routes one envelope to every interested client, invoking `deliver`
    /// once per client with the same encoded payload bytes.
    pub fn route(
        &mut self,
        envelope: WireEnvelope<'_>,
        mut deliver: impl FnMut(ClientId, WireEnvelope<'_>),
    ) -> Result<usize, BrokerError> {
        let (routed, Some(clients)) = self.route_snapshot(envelope)? else {
            return Ok(0);
        };
        for client in clients.iter().copied() {
            deliver(client, routed);
        }
        Ok(clients.len())
    }
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}
