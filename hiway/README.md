# Hiway

Typed event streams with scoped authority, bounded retention, and explicit
backpressure. Event contracts are static; membership and links are dynamic.

## Overview

Hiway routes typed events through local streams. Event declarations define the
contracts. Ports define what each component may publish, observe, or require.
Futures stay with the caller, and each `DynamicFabric` is independent.

- `#[events]` declares event markers and their payloads.
- `#[port]` generates typed senders and receivers for a component.
- `DynamicFabric` and `Grant` provide scoped runtime routing and authority.
- `StaticStream` and `#[graph]` provide bounded caller-owned storage without
  `std` or `alloc`.
- `UnixLink` connects already-authorized local endpoints through Unix sockets.

The default feature set requires neither `std` nor `alloc`. Enable `std` for
the allocating backend. Enable `tokio-io` for Unix-socket links on Unix.

## Example

Declare events and the authority a component needs:

```rust
# #[cfg(feature = "std")]
# {
use hiway::{events, port, Grant};

#[events]
enum Events {
    Job(u32),
    Completed,
}

#[port(
    factory = Grant,
    send(events::Completed),
    required(events::Job),
)]
struct WorkerPort;
# }
```

`examples/basic.rs` contains a complete worker with two streams.
`examples/game.rs` builds several components on one fabric.
`examples/many_components.rs` shows one port type shared by many instances.
`examples/terminal.rs` shows an observer-only component.

Local endpoints may use non-`Send` payloads and futures. Owned generated
ports backed by thread-safe streams also work with `tokio::spawn` through the
ordinary publication and receive methods. Generic spawning helpers can use
`SendEventSender` and `SendEventReceiver` bounds with `PortExt::publish_send`
and `recv_send` to require movable futures. These optional contracts do not
change local implementations or ordinary event-type inference.

## Latest-state snapshots

Events are ordered by default. Mark a complete, independently usable snapshot
with `#[event(latest)]`, or set `EventSpec::DELIVERY` to `Delivery::Latest` in a
manual declaration. Actions and dependent updates keep ordered delivery.

Latest-state streams have capacity one and accept only observers. A new
subscription receives the current snapshot; unread updates coalesce to the
newest version. Reading or maintaining the stream does not discard that state.

Dynamic publishers need an event-item allowance of at least two: the current
snapshot and its prepared replacement. That allowance tracks stream entries,
not consumer-held `Arc` clones. Use resource reservations and `Resource<T>`
for storage that must stay charged until its last owner drops. Strict prepared
replacement defers one retired snapshot to `DynamicFabric::maintain`; another
strict replacement returns `MaintenanceRequired` until that slot is reclaimed.

Unix and `io_uring` links accept forward sequence jumps for latest-state events,
but reject repeated or backward versions. Ordered links retain contiguous
sequence checks and terminate on observer gaps. Delivery mode is part of the
provisioned event contract; changing it requires a new event ID or wire major.

## Connection accountability

Transport records identify the provisioned connection, its generation, event
contract, direction and most recently observed frame sequence. Peer-supplied
event names or payload identity cannot change that attribution.

`hiway-uring::Driver` records every attached link. Its `import_tracked` and
`export_tracked` methods also return the assigned connection identity, so the
host can associate records with an endpoint. IDs are slots within that driver;
successful slot reuse increments the generation without wrapping. Drain with
`pop_record`. Each connection retains eight records, and retired connections share
a 64-record archive. Ordering is per connection.

`UnixLink::import_tracked` and `export_tracked` take a host-assigned
`transport::Connection` and return a `UnixAccountability` handle. The host must
keep ID/generation pairs unique within its accountability domain. The handle
retains eight records and survives link completion or cancellation. Existing Unix
constructors remain untracked. Standalone protocol users can select
`Protocol::with_connection` and drain its records directly.

Admission means local destination acceptance on import, or acceptance of an
encoded frame for I/O on export. Completion means credit sent on import, or
full frame transmission plus matching credit received on export. It does not
acknowledge application processing. Terminal framing, codec, endpoint and I/O
failures produce rejection records; local closure, revocation and dropped link
futures produce cancellation records. Cancellation does not replace terminal
kernel completions or release their buffers early.
Terminal records retain the last observed sequence even if that frame completed.

Overflow discards the oldest records. `lost_records` reports a saturating count
of overwritten records, including retired `io_uring` generations. The host drains
and persists records outside the strict I/O path; recording performs no file
I/O or application callbacks. Construction failures remain returned errors,
before a link has been attached.

## Components across backends

`examples/backends.rs` runs the same `Worker<P>` with borrowed static
endpoints and owned dynamic endpoints. Its receive/process/send logic uses
`EventReceiver` and `EventPort`; both setups bind the same port declaration:

```rust
use hiway::{events, graph, port, StaticStream};

#[events]
enum Events { Job(u16), Completed(u32) }

#[port(send(events::Completed), required(events::Job))]
struct WorkerPort;

#[graph(jobs = (events::Job, 2), completed = (events::Completed, 2))]
struct Graph;

let jobs = StaticStream::new();
let completed = StaticStream::new();
let port = WorkerPort::bind(&Graph::new(&jobs, &completed))?;
# Ok::<(), hiway::PortError>(())
```

`bind` infers the endpoint types from `PortBinding`. The static streams must
outlive the port; the graph wrapper need not. With a dynamic `Grant`,
`WorkerPort::bind(&grant)` returns owned endpoints that retain their grant.
Dropping the original grant handle does not revoke them; explicit revocation
still applies.

The existing `#[port(factory = Grant, ...)]` form produces a concrete port
through `OwnedPortBinding`. Omit `factory` to let the binding supply the
endpoint types. Components storing that port can use a type parameter, as
`Worker<P>` does in the example.

```sh
cargo run -p hiway --example backends --features std
```

## Persisting application state

`examples/persistence.rs` separates serializable `WorkerState` from a live
`WorkerPort`. It processes one job, serializes the state to JSON, drops the
runtime, then deserializes the snapshot and processes another job.

Each run creates its fabric and grants at the composition root and calls
`WorkerPort::bind` with an authorized grant. The restored worker keeps its
application state but receives a fresh subscription. The snapshot contains
no grant, port or subscription cursor; loading it does not restore authority
or replay position.

```sh
cargo run -p hiway --example persistence --features std
```

Serde and JSON are choices made by this example. The Hiway library does not
serialize application state or runtime endpoints.

## Event identities

By default, `#[events]` derives each event ID from its Rust module path, enum
name and variant name. Moving or renaming a declaration changes that ID.
Use this default for ordinary declarations. Once published, the declaration's
name and module path are part of its protocol identity.

Use `#[event(id = "...")]` when a rename or move must preserve a published
identity, or when matching an externally defined protocol name. For example,
a renamed declaration can retain the ID of `jobs_api::jobs::Jobs::Submitted`:

```rust
use hiway::events;

#[events(wire_major = 1, schema_revision = 1)]
enum JobEvents {
    #[event(id = "jobs_api::jobs::Jobs::Submitted")]
    Queued(u32),
}
```

The full string is hashed with `EventId::from_name`; Rust names and payload
types are excluded. Keep the string unchanged when refactoring the source.
There is no need to repeat a declaration's name in a string unless this
compatibility requirement exists. An explicit ID does not make incompatible
payload changes compatible: `wire_major` and
`schema_revision` still describe the wire format's evolution.

Adding an explicit identity to an existing event changes its ID unless the
string matches its previous fully qualified Rust name. Use that old name to
preserve an existing route, or coordinate the identity change with its peers.

## Static graph storage

`#[graph]` borrows streams owned by the caller. Each entry can specify its
payload capacity, subscriber slots and waiter slots:

```rust
use hiway::{events, graph, StaticStream};

#[events]
enum Events {
    Position(u16),
}

#[graph(position = (events::Position, 4, 2, 3))]
struct Graph;

let positions = StaticStream::<events::Position, 4, 2, 3>::new();
let graph = Graph::new(&positions);
```

The shorter `(event, capacity)` form keeps the defaults of eight subscriber
slots and sixteen waiter slots. Each stream may use different dimensions;
the supplied `StaticStream` must match its graph entry.

`StaticStream::with_lock(policy)` accepts a `locking::LockFamily`. The default
`StaticStream::new()` keeps the existing spin lock.

## io-uring transport

`hiway-uring` supplies a Linux transport using `io-uring` directly. One
`Driver` owns a ring shared by its links. The caller chooses the link count,
frame-buffer size and submission capacity at construction. `export` and
`import` accept ordinary `EventReceiver`/`EventSender` endpoints and return
concrete futures; no executor or background thread is installed.

The host polls those futures and calls `advance` for bounded, nonwaiting I/O
progress. `wait` is a separate blocking host operation. Hosts combining socket
and local endpoint readiness can register an eventfd for completion notification
and include local wakers in their own wait mechanism. Revocation is checked on
poll and driver progress, and authorization is checked before each submission.

Hosts needing callback isolation use `advance_io()` followed by `dispatch()`
outside the strict path. `advance_io()` handles I/O and returns a fixed-size
`Progress` report: I/O counts, scheduling advice, and per-slot wake, reclamation
and authority notification flags. It runs no application codec, endpoint future, destructor
or waker callback. Authorization still brackets submission. This path accepts
`()` or `GrantReservation`; custom reservations use `advance()`.

Pending work stays in the driver until dispatched, including after submission
errors; `pending_work()` exposes it without consuming it. Ignoring a report
does not lose work. Call `dispatch()` before parking, and poll link futures
separately for decoding and endpoint operations. Dispatch may execute callbacks
and destructors. Neither syscall latency nor callback execution time has a hard
deadline; premature driver drop still drains outstanding I/O.

`advance_io_with_budget(Budget { completions, submissions, bytes })` sets limits
for one strict pass; `advance_with_budget` also dispatches callbacks. All three
limits must be nonzero. Counts include control traffic and cancellation SQEs;
cancellation consumes no byte allowance. The byte limit caps newly requested
send/receive lengths, splitting frames and credit messages when needed. It does
not cap bytes completing from earlier passes. The convenience methods use the
ring capacities for CQEs/SQEs and no additional byte limit. Each pass still
checks authority for every configured slot; these are I/O limits, not deadlines.

Links take turns submitting one request per round, with rotating lanes and at
most three rounds per pass. A link denied byte or SQ capacity retains its next
turn. Cancellation shares the submission limit and can use the reserved SQ
entry. Link wakes report local frame readiness or closure, not submission
backlog or admission contention.

`Progress::schedule` distinguishes `Continue` (advance again with a replenished
budget), `Retry` (admission contention requiring a host-scheduled retry), and
`Wait` (await external readiness). Contention is attempted once per link per
pass and generates no retry wake. After dispatching work and polling ready
futures, refresh this advice with `driver.schedule()`: callbacks may have added
work. Host readiness includes ring, endpoint and authority changes. `wait()`
returns immediately when local I/O, retry or dispatch work remains; otherwise
it waits only for ring completion, so combined readiness belongs to the host.

Link callbacks run outside the shared slot-table borrow, allowing them to poll
or drop other link futures synchronously. Codec results are checked against
closure and revocation before being committed. Waker replacement rechecks
readiness after clone/drop callbacks, so progress during registration is not lost.

The arena never grows after construction. Buffers, operation records and waiter
slots are reused; transport operations and link removal allocate nothing.
`hiway_uring::Pool::try_new()` allocates the buffers separately;
`Driver::with_pool(entries, pool)` takes ownership without reallocating them.
`Driver::new(entries)` creates its own pool and delegates to `with_pool`.
Application codecs, endpoint implementations, reservation callbacks, supplied
wakers and the kernel retain their own allocation behavior. Submitted buffers
and reservation guards remain owned until terminal completion. Link drop marks
its record closing. For shutdown, `stop_admission()` rejects new link attachments
while existing links continue; `cancel_all()` stops admission and closes all
links without callbacks or waiting. Advance cancellation with
`advance_io_with_budget`, then dispatch at most a chosen number of slot records
with `dispatch_with_budget(slots)`. Dispatch rotates between slots; its limit
bounds records, not callback execution time. A zero limit does no work.

Keep the driver owned across host turns until `shutdown_complete()` reports
terminal completions and dispatched reclamation. This works even while link
futures remain alive; they observe closure when polled and retain the buffer
pool until dropped. Dispatch wakes and destructors outside the critical path.
Driver drop after completion runs no draining loop. Dropping an unfinished
driver remains a blocking cancellation-and-drain fallback. If teardown
cannot establish completion, it retains the domain's storage rather than free
buffers still accessible to the kernel.

The protocol lives in `hiway::transport`, with no allocator or executor. Its
pending effects require explicit acceptance and completion, and receive
completion grants no admission credit. It uses the existing HWY1 data and
credit format. Grant-backed hosts use `Grant::reserve_transport` with the
driver's `reservation_bytes()`; static endpoints can use `()`.

Decoded storage and retained application resources use
`Grant::reserve_resources(items, bytes)`. Reserve a declared upper bound before
allocation, then `reservation.attach(value)` transfers the charge into
`Resource<T>`. Moving that owner or retaining it through `Arc` keeps the charge;
the value is destroyed before quota returns. Hold it through the last external
use, including GPU completion. A separately cloned allocation needs its own
reservation. Resource costs use the generic allowance, without borrowing an
event's reserved data capacity.

Grant-backed imports in both transport backends call
`WireCodec::decode_with_resources` with the host's grant. Override that method
to reserve before decoding and carry the reservation in the payload or its
resources. The default preserves legacy decoding without resource accounting.
Native codecs and their declared costs remain trusted. Admission credit and
transport shutdown do not retire a retained resource; its last owner does.

```sh
cargo run -p hiway-uring --example round_trip
cargo test -p hiway-uring
cargo bench -p hiway-uring --bench throughput
```

## Features

The default feature set is empty. The main feature combinations are:

- `std`: `DynamicFabric`, `Grant`, and allocating local streams.
- `tokio-io`: `std` plus `UnixLink` on Unix.
- no features: `StaticStream`, `StaticFabric`, and graph bindings using
  caller-owned bounded storage.

The full API documentation describes admission, retention, grants, revocation,
gaps, cancellation, schema evolution, and IPC boundaries.

## Development

```text
cargo test --workspace --all-features --locked
cargo test --workspace --no-default-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all -- --check
RUSTFLAGS="--cfg loom" cargo test -p hiway-loom --test public_bus --locked
cargo check -p hiway-tests --lib --no-default-features --target thumbv7em-none-eabi --locked
cargo build -p hiway-tests --bin embedded --no-default-features --target thumbv7em-none-eabi --locked
```

## License

Hiway is licensed under the MIT License. See `LICENSE-MIT`.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Hiway by you shall be licensed under the MIT License, without
additional terms or conditions.
