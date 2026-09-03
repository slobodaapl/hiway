# Security and assurance

## Supported versions

Security fixes target the latest released version. The default branch is development code and may change without compatibility guarantees before a release.

## Reporting a vulnerability

Use **Report a vulnerability** in the repository Security tab. Include the affected version, impact, reproduction steps, and any suggested mitigation.

Do not disclose exploit details in a public issue. If private vulnerability reporting is unavailable, open a public issue containing no sensitive details and request a private contact channel.

## Security boundary

`hiway` forbids unsafe code. This guarantee does not extend to dependencies.

CI checks the default `no_std` configuration against `thumbv7em-none-eabi`.

## Supply chain

Repository and release builds use the committed `Cargo.lock` with `--locked`. CI checks formatting, tests, `no_std`, Clippy, Loom models, package construction, dependency advisories, licenses, and sources.

Tagged releases include a CycloneDX SBOM and GitHub provenance and SBOM attestations. Workflow actions are pinned to immutable commits.
