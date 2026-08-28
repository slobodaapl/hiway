# hiway

`hiway` is a typed in-process event bus for Rust. Publishers send ordinary payload structs. Subscribers consume either the complete event enum or one payload type. A shared transform chain may modify, drop, or expand each publication. A central tick commits every finished publication currently waiting as one frame.

```text
payload -> event enum -> transform 1 -> transform 2 -> ready publications
                                                                |
                                                              tick
                                                                v
                                                   Tokio broadcast frame
                                                    |-> subscriber A
                                                    |-> subscriber B
                                                    `-> subscriber C
```

Subscribers are independent. They do not register with each other, know which other subscribers exist, or occupy positions in a routing graph.

## Events

Define payloads as normal Rust structs, then collect them in one newtype enum:

```rust
#[derive(Clone, Debug)]
struct UiEvent {
    control: String,
}

#[derive(Clone, Debug)]
struct GameEvent {
    action: String,
}

#[derive(Clone, Debug)]
struct LogEvent {
    source: &'static str,
    message: String,
}

#[derive(Clone, Debug, hiway::HiwayEvent)]
enum Events {
    Ui(UiEvent),
    Game(GameEvent),
    Log(LogEvent),
}
```

`HiwayEvent` generates the conversions used by typed publishing, consumption, and transforms. Every enum variant must wrap exactly one payload type, and each payload type may appear only once. Represent a signal without data by wrapping a zero-sized struct.

There is no separate kind enum, kind field, optional-field envelope, or manual trait implementation.

## Buses

Create a standalone bus when its owner can pass clones directly to components:

```rust
use hiway::Bus;

let events = Bus::new();
```

Standalone buses have no name. Rust infers the event enum from later transforms, publishers, or consumers.

`Bus::new()` retains up to 256 committed tick frames for slow subscribers. Use `Bus::with_capacity(n)` when that lag window needs an explicit size.

Unrelated components holding the same `Hiway` instance or clone can retrieve one shared bus per event enum type:

```rust
let hiway = hiway::Hiway::new();
let events = hiway.bus();
```

The event type is inferred from how `events` is used. Repeated lookups for that type return the same bus and share its transform configuration. The first lookup determines its broadcast capacity.

## Publishing and consuming

The generated conversion lets a publisher send a payload directly:

```rust
events.publish(UiEvent {
    control: "inventory".into(),
}).await;

assert!(events.tick());
```

`publish().await` runs the snapshotted transform chain and queues its complete output as one publication batch. It does not broadcast. `tick()` performs no transforms and waits for nothing; it commits every publication batch already waiting as one frame and reports whether the frame contained anything. Real applications normally call it from one central update loop.

`subscribe` receives the complete enum:

```rust
let mut all_events = events.subscribe();

match all_events.recv().await? {
    Events::Ui(ui) => println!("ui: {}", ui.control),
    Events::Game(game) => println!("game: {}", game.action),
    Events::Log(log) => println!("{}: {}", log.source, log.message),
}
```

`consume` receives one payload type and skips unrelated variants locally:

```rust
let mut logs = events.consume::<LogEvent>();
let log = logs.recv().await?;
```

Skipping a variant in one typed consumer does not hide it from any other subscriber. Each subscription has its own Tokio broadcast receiver.

## Global transforms

Transforms belong to the bus, not to subscribers. Every publication crosses the same ordered chain once before entering the ready accumulator.

```rust
events.transform(|mut ui: UiEvent| {
    ui.control.make_ascii_lowercase();
    let log = LogEvent {
        source: "ui",
        message: ui.control.clone(),
    };
    vec![ui.into(), log.into()]
});
```

The closure input selects one payload type. Other variants pass through unchanged. Its returned vector defines the complete output:

- `vec![]` drops the matching event.
- One value preserves or replaces it.
- Several values expand it in order.

Use the complete enum when one stage handles several input variants. The output type remains inferred from the bus:

```rust
events.transform(|event: Events| match event {
    Events::Ui(ui) => {
        let log = LogEvent {
            source: "ui",
            message: scrub_ui(&ui),
        };
        vec![Events::Ui(ui), log.into()]
    }
    Events::Game(game) if game.action == "internal-probe" => vec![],
    other => vec![other],
});
```

Subscribers receive the returned events in vector order. The asynchronous form is `transform_async`; it follows the same contract and returns `Vec<Events>` from its future.

Each stage processes every output from the previous stage. Events emitted by a stage continue through later stages only. They never restart the chain, so an emission cannot accidentally recurse through the transform that created it.

A transform is infallible at the bus boundary. Represent a failure as an event or deliberately drop the publication.

## Runtime configuration and ordering

`transform` and `transform_async` append stages. They do not remove or reorder existing stages.

Each `publish` call snapshots the current chain before awaiting a transform. A stage appended during an in-flight publication affects later snapshots, not that publication.

Sequential awaited publishes enter the ready accumulator in order. Concurrent publishers run their transform chains concurrently and enter it when they finish. `hiway` does not impose a global transform lock, so one slow asynchronous transform does not block unrelated publishers or the central tick.

One tick swaps out the complete ready accumulator in constant time, then commits it with one broadcast send. A publication that finishes after that swap waits for the next tick. The tick never runs user transforms, iterates publications or subscribers, or awaits work. Subscribers unpack the frame locally. Outputs from each publication remain contiguous and cannot interleave with another publication.

## Concurrency safety

`publish()` holds the transform registry only long enough to clone the current stages. It releases that lock before awaiting or invoking transform work. Completed publications briefly lock the ready accumulator to append one batch. `tick()` takes a non-blocking commit guard, swaps out the ready accumulator, releases the ready guard, performs one Tokio broadcast send, then releases the commit guard. This preserves frame order across concurrent tick calls without holding the ready lock during transport work. Hiway invokes no transform code and crosses no `.await` while holding these locks. Its lock order has no reverse path.

The dev-only Loom suite exhaustively checks bounded schedules for transform snapshot/registration, concurrent preparation/ticking, atomic frame delivery, and independent subscriber cursors. A separate public API test runs the production Tokio and parking_lot path with one transform deliberately suspended while another publication and tick complete:

```bash
cargo test -p hiway --test loom_protocol
cargo test -p hiway --test semantics blocked_transform_does_not_block_another_publication_or_tick
```

This covers Hiway's synchronization protocol. It relies on Tokio and parking_lot's own synchronization guarantees. A transform that never returns, an application that never ticks, or components that cyclically wait for events can still starve application work; none creates an internal Hiway lock cycle.

## dptree listeners

The default `dispatch` feature provides a `dptree`-backed router for components that handle several payload types through one subscription:

```rust
events
    .listen()
    .on(|ui: UiEvent| async move {
        println!("ui: {}", ui.control);
    })
    .on(|log: LogEvent| async move {
        println!("{}: {}", log.source, log.message);
    })
    .run()
    .await?;
```

Typed `on` branches cover the common case. Advanced callers can attach raw dptree branches and dependencies for `case!`, filtering, mapping, and dependency injection. This routing is local to one listener and happens after the bus's global transforms. Independent subscribers remain the fan-out mechanism.

Disable default features when handler routing is unnecessary:

```toml
hiway = { version = "0.1", default-features = false }
```

## Delivery semantics

The transport is Tokio `broadcast`; its capacity is measured in committed tick frames. A receiver that falls behind gets `RecvError::Lagged(n)`, where `n` is the number of missed frames. The loss is reported rather than hidden.

Prepared publication batches wait in an unbounded accumulator until ticked. This keeps the central tick non-blocking; applications must tick regularly enough to prevent unbounded growth. Committing with no active receivers is valid, and that frame is not retained. A subscriber created after `publish()` but before `tick()` receives the frame.

## Terminal example

The adjacent example crate is inert unless its `example` feature is enabled:

```bash
cargo run -p hiway-example --features example --bin hiway-demo
```

The demo draws one broadcast bus. Three independent objects attach to it; the diamond is inline middleware, not an endpoint or another bus:

```text
┌─ CHARACTER PRODUCER ──────┐              ┌─ TERMINAL UI ─────────────┐
│ publishes CharacterEvent  │              │ billboard + live counts   │
└─────────────┬─────────────┘              └─────────────┬─────────────┘
              │             INLINE TRANSFORM             │
──────────────┴──────────────────◆───────────────────────┴────────────── one Tokio broadcast bus
                                 │
                                 │
                       ┌─────────┴────────────┐
                       │ COUNT KEEPER         │
                       │ cumulative totals    │
                       └──────────────────────┘
```

The producer, count keeper, terminal UI, and central ticker run as independent asynchronous tasks. The producer publishes characters without knowing who consumes them. The inline transform colors each `CharacterEvent`, preserves it, and emits a one-hot `RgbaCountEvent`. It owns no cumulative state.

The count keeper consumes only `RgbaCountEvent`, owns the cumulative totals, then publishes `UiCountUpdateEvent`. The terminal UI consumes transformed characters and count updates, and owns all drawing. None of these objects needs to know that either of the other objects exists.

The colorizer alternates between pass-through, red, green, and blue, turning `H` into `H`, `r!H`, `g!H`, or `b!H`. `A-AS-IS` is the pass-through counter, not an alpha channel. The terminal redraws one alternate-screen frame, so the animation does not fill scrollback. Press `Ctrl-C` to stop it and restore the previous screen.
