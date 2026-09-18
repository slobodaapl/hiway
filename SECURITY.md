# Security

## Supported versions

Security fixes target the latest released version. The default branch is development code and may change without compatibility guarantees before a release.

## Reporting a vulnerability

Use **Report a vulnerability** in the repository Security tab. Include the affected version, impact, reproduction steps, and any suggested mitigation.

Do not disclose exploit details in a public issue. If private vulnerability reporting is unavailable, open a public issue containing no sensitive details and request a private contact channel.

## Scope

The `hiway` crate forbids unsafe code; dependencies may use it.

An application owns each fabric and issues scoped grants. Generated port types
describe requested operations; runtime binding checks actual permissions and
capacity. Event IDs and socket paths are not capabilities. Hiway provides no
unauthenticated broker/listener.

Unix links require host-provisioned grants and protected, already-connected
data/control sockets. The host must prevent a plugin from accessing other
bootstrap channels, host memory, filesystem resources, or unrelated sockets.
Separate processes under the same user do not provide a complete sandbox. Hiway
does not create that OS boundary or enforce CPU scheduling.

Transport resources have finite allowances. Local payload accounting bounds
retained items, not arbitrary heaps reachable from a payload. IPC accounting
bounds Hiway's encoded buffers, not kernel socket buffers or allocations made
by application codecs. Application codecs run as trusted host code; validate
and constrain them.

Each event has reserved item, subscription, and waiter capacity within its
grant. Child grants reserve matching parent event pools; cloning shares those
counters. Neither delegation nor IPC attachment can turn one event's allowance
into another's. IPC frames and driver waits consume the separate generic
remainder. Capacity and authority are both required for binding.

Observer subscriptions cannot impose required-receiver backpressure. Authorized
required receivers may stall their stream. Independent streams share hardware,
allocators, and the application's executor. Hiway provides no hard-latency or
information-flow noninterference guarantee.

Revocation stops new admissions after its local cutoff. It cannot retract
received data, roll back application effects, or reclaim handles kept alive by
trusted in-process application code. Previously admitted records can retain
charges until consumption, eviction, or explicit stream closure.
