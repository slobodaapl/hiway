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

## Stable event identities

By default, `#[events]` derives each event ID from its Rust module path, enum
name and variant name. Moving or renaming a declaration changes that ID.
For protocols shared across processes or releases, give each variant an
explicit identity:

```rust
use hiway::events;

#[events(wire_major = 1, schema_revision = 1)]
enum JobEvents {
    #[event(id = "com.example.jobs.submitted")]
    Submitted(u32),
    #[event(id = "com.example.jobs.completed")]
    Completed,
}
```

The full string is hashed with `EventId::from_name`; Rust names and payload
types are excluded. Keep the string unchanged when refactoring the source.
Variants without `#[event(id = "...")]` retain the default identity scheme.
Use distinct names for distinct protocol events. An explicit ID does not
make incompatible payload changes compatible: `wire_major` and
`schema_revision` still describe the wire format's evolution.

Adding an explicit identity to an existing event changes its ID unless the
string matches its previous fully qualified Rust name. Use that old name to
preserve an existing route, or coordinate the identity change with its peers.

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
