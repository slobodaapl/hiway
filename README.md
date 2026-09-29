# Hiway

Typed event streams with scoped authority, bounded retention, and explicit
backpressure. Event contracts are static; membership and links are dynamic.

See the [Hiway crate README](hiway/README.md) for usage, examples, features,
and development commands.

The root manifest defines the workspace. Each crate has its own directory:

- `hiway/`: the event bus, library tests, and runnable examples.
- `hiway-macros/`: attribute macros for events and ports.
- `hiway-uring/`: the Linux io-uring transport.
- `hiway-tests/`: integration tests and embedded checks.
- `hiway-loom/`: concurrency model checks.

Hiway is licensed under the MIT License. See [LICENSE-MIT](LICENSE-MIT).
