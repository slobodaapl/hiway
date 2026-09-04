# Hiway

Hiway is a typed local event router. Event declarations define stable contracts;
the selected fabric decides storage, allocation, and live membership.

## Event contracts

`#[events]` generates one zero-sized marker per enum variant:

```rust
use hiway::events;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Progress {
    pub completed: u32,
}

#[events(wire_major = 2, schema_revision = 3)]
pub enum PipelineEvents {
    Started,
    Progress(Progress),
    AlternativeProgress(Progress),
}
```

This declaration creates `pipeline_events::Started`,
`pipeline_events::Progress`, and `pipeline_events::AlternativeProgress`.
Each marker implements `EventSpec`; its payload is the corresponding variant
payload. The ID comes from:

```text
module_path!() :: PipelineEvents :: Variant
```

Moving or renaming the declaration changes its identity. Payload types do not
identify routes, so two events can safely carry the same Rust type.

The generated enum is an explicit fan-in adapter for wire multiplexing,
recording, replay, or logging. Generated constructors retain the event tag:

```rust
let combined: PipelineEvents = pipeline_events::Progress(progress).into();
```

There is deliberately no `From<Progress> for PipelineEvents`: the payload
alone cannot distinguish `Progress` from `AlternativeProgress`. Normal
local routing stores the payload directly and never constructs the enum.

## No-`std`, no-`alloc` core

Hiway has an empty default feature set:

```toml
hiway = { version = "0.1", default-features = false }
```

`HeaplessRoute`, `DirectRoute`, `Inbox`, typed send/receive futures,
`PortBinding`, and `#[graph]` use caller-owned bounded storage. They require
neither `std` nor `alloc`.

```rust
use hiway::{DeliveryPolicy, HeaplessRoute, Inbox};

let inbox = Inbox::<Progress, 8>::new();
let route = HeaplessRoute::<pipeline_events::Progress, 4>::new();
let subscription = route.subscribe(&inbox, DeliveryPolicy::Reliable)?;

route.sender().send(progress).await?;
let received = inbox.recv().await;
```

The route stores subscriber references in inline `heapless` storage. A send
does not allocate, box a future, consult a global registry, or create a combined
event enum. `DirectRoute` removes the subscriber table when one fixed target
is sufficient.

`StaticFabric` adapts one route to `PortBinding<E>`. `#[graph]` composes
several static routes behind one compile-time graph:

```rust
use hiway::graph;

#[graph(
    started = (pipeline_events::Started, 2),
    progress = (pipeline_events::Progress, 4),
)]
pub struct PipelineGraph;

let graph = PipelineGraph::new(&started_route, &progress_route);
let sender = graph.sender::<pipeline_events::Progress>()?;
```

The graph fixes the event set and storage capacities. Subscription membership
can still change at runtime.

## Injected ports

Enable `std` for `DynamicFabric`, its runtime registry, and thread-safe
allocating inboxes:

```toml
hiway = { version = "0.1", features = ["std"] }
```

`#[port]` creates one owning endpoint for a declared capability set. Application
state remains an ordinary downstream type and receives that endpoint through
its own constructor:

```rust
use hiway::{events, port, EventPort, EventReceiver, PortExt};

#[derive(Clone, Copy)]
struct Position {
    x: i32,
    y: i32,
}

#[events]
enum GameEvents {
    PlayerMoved(Position),
    DamageTaken(u32),
}

type GameBus = hiway::DynamicFabric;

#[port(
    factory = GameBus,
    send(game_events::PlayerMoved),
    recv(game_events::DamageTaken),
)]
struct PlayerPort;

struct Player<P> {
    port: P,
    health: u32,
    position: Position,
}

impl<P> Player<P> {
    fn new(port: P, health: u32, position: Position) -> Self {
        Self {
            port,
            health,
            position,
        }
    }
}

impl<P> Player<P>
where
    P: EventPort<game_events::PlayerMoved>
        + EventReceiver<game_events::DamageTaken>,
{
    async fn move_to(
        &mut self,
        position: Position,
    ) -> Result<(), hiway::SendError> {
        self.position = position;
        self.port
            .publish(game_events::PlayerMoved(position))
            .await
    }

    fn apply_damage(&mut self) {
        while let Some(damage) = self.port.try_recv::<game_events::DamageTaken>() {
            self.health = self.health.saturating_sub(damage);
        }
    }
}

let game_bus = GameBus::new();
let player = Player::new(
    PlayerPort::bind(&game_bus)?,
    100,
    Position { x: 0, y: 0 },
);
```

`Player::new` belongs to the application. `PlayerPort` owns one sender per
declared publication, one receiver per declared subscription, and every
subscription guard. Dropping `Player<P>` drops `P` and unsubscribes it.

Generic application code uses `PortExt`:

```rust
player.port.try_publish(game_events::PlayerMoved(position))?;
player.port.publish(game_events::PlayerMoved(position)).await?;
let damage = player.port.try_recv::<game_events::DamageTaken>();
let damage = player.port.recv::<game_events::DamageTaken>().await;
```

Named aliases such as `publish_player_moved`,
`try_publish_player_moved`, `recv_damage_taken`, and
`try_recv_damage_taken` remain available directly on `PlayerPort`. The macro only
implements `EventPort<E>` and `EventReceiver<E>` for events declared in the
corresponding `send(...)` and `recv(...)` lists. Undeclared capabilities
fail at compile time.

`factory = GameBus` is a concrete downstream choice, not a macro alias for
`DynamicFabric`. Another owning backend can implement `OwnedStorage` and
`OwnedPortBinding<E>`. Borrowed static routes remain available directly
through `PortBinding<E>`; Hiway does not hide allocation inside that path.

Live ports are process-local infrastructure. Persist application fields or an
application snapshot, then bind a fresh endpoint after deserialization. Sender
handles, queued messages, wakers, and subscription guards are not serialized;
Hiway does not add a Serde dependency.

## Generic systems and role boundaries

`EventPort<E>` and `EventReceiver<E>` let higher-level systems require only
the events they use:

```rust
async fn announce_move<P>(
    port: &P,
    position: Position,
) -> Result<(), hiway::SendError>
where
    P: hiway::EventPort<game_events::PlayerMoved>,
{
    hiway::publish(port, game_events::PlayerMoved(position)).await
}
```

Server and client crates can define different role traits by combining these
capabilities. Network codecs belong on the network backend; `EventSpec` does
not require allocation or networking.

## Delivery behavior

Inbox delivery is bounded and FIFO. `Reliable` waits for capacity;
`try_send()` and `try_publish()` return the original payload when a reliable
target is full. `DropNewest`, `DropOldest`, and `Latest` are local
subscription policies.

Resolved dynamic senders retain their typed route handle. Publication does not
repeat a registry lookup. Fanout clones only for non-final subscribers and
moves the original payload to the final target.

## Subscriber transforms and links

Transforms are ordinary typed subscriber components:

```rust
use hiway::{Transform, TransformSubscriber};

let transform = Transform::new(|value: u32| value + 1)
    .then(Transform::new_async(|value: u32| async move { value * 2 }));
let runner = TransformSubscriber::new(&fabric, &storage, transform)?;
executor.spawn(runner.run());
```

`Link` is an executor-neutral polling trait. The application owns the driver.
Unix transport and the broker operate on opaque `(EventId, WireMajor)` routes,
envelope metadata, and borrowed payload bytes. `Schema`, `FieldSpec`,
`validate_schema()`, and `validate_evolution()` provide tagged
wire-compatibility checks.

## Examples

```text
cargo run --example basic --features std
cargo run --example terminal --features std
cargo run --example many_components --features std -- 50000
cargo run --example game --features std
```

The game example uses one injected `GameBus` and four ordinary stateful
objects: UI, player, world, and monster. Events carry movement, attacks,
spawns, defeats, and weather without exposing generated runtime types.

## Checks

```text
cargo test --workspace --all-features
RUSTFLAGS="--cfg loom" cargo test -p hiway-loom --test public_bus
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo doc --workspace --all-features --no-deps
```
