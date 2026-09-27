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

```rust,ignore
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
```

`examples/basic.rs` contains a complete worker with two streams.
`examples/game.rs` builds several components on one fabric.
`examples/many_components.rs` shows one port type shared by many instances.
`examples/terminal.rs` shows an observer-only component.

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
cargo run --example backends --features std
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
cargo run --example persistence --features std
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
