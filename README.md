# hiway

`hiway` is a bounded typed event bus for Rust. Publishers run asynchronous
transforms and prepare routed subscriber batches. A synchronous tick moves every
completed publication into subscriber frame queues. Subscribers then receive
events through independent cursors.

The static core supports `no_std`, performs no internal heap allocation, and
depends on no async runtime. It returns ordinary Rust futures and never spawns
them. User transforms, conversions, tags, clones, destructors, and wakers remain
user code and may allocate or panic.

```text
event -> async pipeline -> final bounded Batch
                              |
                       reserve + route + clone
                              |
                    subscriber pending frames
                              |
                            tick
                              v
                    subscriber frame queues
```

Publishing and ticking remain valid with no subscribers. The next tick still
observes a completed nonempty publication, but a later subscriber receives no
history.

## Event algebra

Let `E` be the application event enum, `Seq<E>` an ordered sequence, and `F<X>`
an asynchronous computation producing `X`. One built-in transform stage is:

```text
E -> F<Seq<E>>
```

The sequence defines the transform:

- Empty drops the input.
- One event preserves or replaces it.
- Several events expand it in order.
- Sequential stages feed each output depth-first into the remaining stages.

`OUTPUTS` bounds only the final `Batch`. An intermediate stage may emit more
than `OUTPUTS` when later stages reduce the sequence. Hiway never truncates the
final output or returns a partial batch.

Built-in `Identity`, `Stage`, and `Then` use one continuation interpreter. For
finite stage iterators that are fully polled to normal completion, left/right
grouping produces the same output, error, transform order, and user-effect
order. This law does not cover cancellation, unwinding, destructor effects, or
an arbitrary direct `Pipeline` implementation. `OutputFull` prevents bus
publication, but it cannot undo user effects already performed before the
excess final event.

Only final output is bounded. A large intermediate iterator may consume one
poll before yielding control, and an infinite iterator can monopolize its
publishing task when downstream stages keep completing immediately.

## Events and typed routing

Define ordinary payloads, then collect them in one newtype enum:

```rust
#[derive(Clone, Debug)]
struct UiEvent {
    control: &'static str,
}

#[derive(Clone, Debug)]
struct LogEvent {
    message: &'static str,
}

#[derive(Clone, Debug, hiway::HiwayEvent)]
enum Events {
    Ui(UiEvent),
    Log(LogEvent),
}
```

The derive generates `From`/`TryFrom` conversions and a hidden ordinal tag for
each variant. Each variant must wrap exactly one unique payload type. There is
no user-visible kind enum or kind field.

For generic enums, payload types must remain distinct for every possible
instantiation. Use newtype payloads when two variants would otherwise share the
same generic type.

`subscribe()` receives the complete enum. `consume::<Payload>()` records one
route when the subscription is created. Unrelated variants are excluded before
subscriber storage, cloning, waking, and lag accounting.

## Static transforms

The default path starts with the default-capacity identity bus, then consumes
and returns it with one new statically typed transform. Its input type selects
one payload; other variants pass through unchanged.

```rust
use hiway::Bus;

let bus = Bus::default()
    .transform(async |mut ui: UiEvent| {
        ui.control = "inventory";
        [ui.into()]
    })
    .transform(async |ui: UiEvent| {
        [
            Events::Ui(ui),
            Events::Log(LogEvent {
                message: "opened inventory",
            }),
        ]
    });
```

`Bus::default()` fixes capacities at `4 / 8 / 4 / 4`; the first transform and
later use infer the event enum. Each `transform` call changes the concrete bus
type, so finish this chain before borrowing the bus for subscriptions or shared
tasks. The operation remains allocator-free and works without `std`.

Appending a transform moves the existing pipeline and inline bus state into the
new concrete type. It does not rerun the new transform over batches already
prepared for a later tick.

`stage(...).then(...)` remains useful for reusable standalone pipelines.
`with_pipeline` installs one such pipeline or a named whole-pipeline
implementation:

```rust
use hiway::{stage, PipelineExt};

let pipeline = stage(async |ui: UiEvent| [Events::Ui(ui)])
    .then(stage(async |event: Events| [event]));
```

Pass that value to `Bus::with_pipeline(pipeline)`; publishing or subscribing
then infers `Events` without a turbofish.

Direct `Pipeline` implementations are complete terminal pipelines. Only
Hiway's built-in `Identity`, `Stage`, and `Then` implement `PipelineExt` and can
use `.then(...)`.

Explicit capacity bounds remain available:

```rust
let bus = hiway::Bus::<Events, 8, 32, 8, 6>::new()
    .transform(async |ui: UiEvent| [Events::Ui(ui)]);
```

Static transforms use no boxed futures or runtime transform list. The
application awaits or spawns `publish()` using its executor.

## Publishing, ticking, and consuming

```rust
let mut logs = bus.consume::<LogEvent>()?;

bus.publish(UiEvent {
    control: "Inventory",
}).await?;

assert!(bus.tick());
let log = logs.recv().await?;
```

Manual loops can receive without registering a waker:

```rust
match logs.try_recv()? {
    Some(log) => consume(log),
    None => {}
}
```

For a nonempty transformed batch, `publish().await` atomically reserves one
`READY` slot and snapshots active subscriber generations/routes. It then filters
variants and performs the minimum required event clones in the publishing task,
outside synchronization. If `N` subscribers receive an event, Hiway makes
`N - 1` clones and moves the original into the final delivery.

The producer commits every prepared subscriber batch in one critical section.
That commit order defines concurrent publication order. A tag or clone panic
releases the preparation reservation and commits nothing.

`tick()` drains only completed publications. It never waits for producers,
runs transforms, inspects tags, or clones events. It moves pending frames,
evicts old frames, takes wakers, then destroys evicted events and invokes wakers
after synchronization. A preparation completing after the drain waits for the
next tick.

A subscribe/publish race linearizes at the producer snapshot: the new
subscriber may receive or miss that publication. A subscriber created after
`publish().await` returns always misses it.

## Backpressure and retry

`READY` counts completed publications plus active preparation reservations.
`PublishError::ReadyFull` preserves the exact transformed batch:

```rust
use hiway::PublishError;

if let Err(PublishError::ReadyFull(full)) = bus.publish(event).await {
    let batch = full.into_batch();
    assert!(bus.tick());
    bus.try_submit(batch)?;
}
```

`try_submit` repeats preparation only. It never reruns the transform pipeline.
If retry still finds `READY` full, its `ReadyFull` again owns the untouched
batch.

Failures remain explicit:

- `PublishError::OutputFull`: final pipeline output exceeded `OUTPUTS`.
- `PublishError::ReadyFull(full)`: preparation could not reserve `READY`.
- `SubscribersFull`: no bounded subscription slot remains.
- `RecvError::Lagged(n)`: `n` unread tick frames were overwritten.

## Bounds and memory

The complete type is:

```text
Bus<Event, OUTPUTS, READY, FRAMES, SUBSCRIBERS, Pipeline>
```

- `OUTPUTS`: maximum events in the final transformed publication.
- `READY`: completed publications plus active preparation reservations.
- `FRAMES`: committed tick frames retained by each subscriber.
- `SUBSCRIBERS`: simultaneous subscription slots.

The defaults are `4 / 8 / 4 / 4`. Subscriber inline event storage scales
approximately as:

```text
SUBSCRIBERS * (FRAMES + 1) * READY * OUTPUTS * size_of::<Event>()
```

At the defaults, those subscriber queues reserve capacity for about 640
`Event` values. Zero subscribers reserve no frame storage, but `FRAMES` must
still be positive.

The extra frame is each subscriber's producer-prepared pending frame. Producer
scratch per active `publish` future contains the transformed source batch plus
at most one delivery batch per subscriber. Concurrent publication multiplies
that scratch by the number of active futures. Tick scratch contains at most one
discarded frame and one waker per subscriber. Add queue metadata, alignment,
and padding. The static core uses bounded inline storage and performs no
internal heap allocation. Payload clones and transforms may allocate because
their implementations belong to the application.

Tick frame moves/evictions/wakes are bounded by active subscribers; the
implementation also scans the fixed `SUBSCRIBERS` slots. Tick is not a strict
`O(1)` operation. Inline byte movement also scales with configured capacities
and `size_of::<Event>()`; prefer compact events or handles for large payloads.

## Features and dynamic transforms

Default features are empty. Embedded applications use the allocator-free
`no_std` core and must supply the target platform's `critical-section`
implementation:

```toml
hiway = { version = "0.1" }
```

Desktop applications should enable `std`, which gives each bus its own mutex:

```toml
hiway = { version = "0.1", features = ["std"] }
```

The derive macro runs on the build host and does not add `std` to the target.
Correctness of the no-`std` synchronization path depends on the platform's
`critical-section` implementation.

Runtime-injectable transforms are opt-in:

```toml
hiway = { version = "0.1", features = ["dynamic"] }
```

```rust
let bus = hiway::DynamicBus::<Events>::new();

bus.transform(|mut ui: UiEvent| async move {
    ui.control = "inventory";
    [Events::Ui(ui)]
});
```

`DynamicBus` uses `std` trait objects, `Vec`, `Arc`, boxed futures, and boxed
iterators. Transform registration publishes a new `Arc<[Stage]>` snapshot. A
publication clones that outer `Arc` once before awaiting user work; the stage
list lock is never held across `.await`.

The distinction is deliberate: `Bus::transform(self, ...)` builds an immutable
static type before sharing, while `DynamicBus::transform(&self, ...)` mutates a
runtime stage registry after construction.

An explicit iterator stack preserves the same depth-first, final-bound order as
the static interpreter. A stage added during an in-flight publication affects
later publications only. `DynamicBus::try_submit` retries a preserved batch
without rerunning runtime stages.

## Concurrency and panics

Concurrent publishers run independently. Their preparation commit order is the
publication order. A suspended transform or in-progress clone does not hold bus
state, block tick, or prevent another producer from completing.

With `std`, each bus owns one `std::sync::Mutex`. Without `std`, state access
uses the platform `critical-section`; Loom substitutes one `loom::sync::Mutex`
per bus. Event destruction and waker invocation occur after state
synchronization. Receive clears a registered waker when it returns an event or
lag result. Cancelling a pending receive may retain one waker until another
receive, a matching delivery tick, or subscription drop.

User transforms, conversions, tags, clones, destructors, and wakers may panic.
Hiway does not convert those panics into bus errors. Transform panics occur
before reservation. Tag/clone panics release their reservation and commit
nothing. Destructor/waker panics occur outside synchronization, after any state
transition that selected them.

## Loom model

`hiway-loom` exercises the exact public `Bus`; only its lock backend changes:

```bash
RUSTFLAGS="--cfg loom" cargo test -p hiway-loom
```

The models cover concurrent producer completion/ticks, receive registration,
in-progress preparation, ready-capacity saturation/retry, subscriber generation
reuse, clone/tag panic cleanup, atomic publication, destruction outside state
synchronization, and reentrant waking. This is evidence for the modeled
protocol. It does not prove a platform's `critical-section` implementation or
arbitrary user transform code.

## Terminal example

```bash
cargo run -p hiway --example basic --features std
cargo run -p hiway --example terminal --features std
```

The demo uses Tokio as the executable's current-thread executor. Hiway does not
depend on Tokio. Its producer, count keeper, terminal UI, and ticker are
independent borrowed futures connected through one bus.

## Release checks

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo test --workspace --all-features
cargo check -p hiway --no-default-features
cargo test -p hiway --no-default-features
cargo test -p hiway --doc
cargo test -p hiway --no-default-features --doc
cargo run -p hiway --example basic --features std
cargo run -p hiway --example terminal --features std
cargo clippy --workspace --all-targets --all-features -- -D warnings -W clippy::pedantic
cargo clippy -p hiway --no-default-features --all-targets -- -D warnings -W clippy::pedantic
RUSTFLAGS="--cfg loom" cargo clippy -p hiway-loom --all-targets -- -D warnings -W clippy::pedantic
RUSTFLAGS="--cfg loom" cargo test -p hiway-loom
cargo package -p hiway-macros
cargo package -p hiway
```
