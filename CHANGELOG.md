# Changelog

All notable changes to `svc-threads` (the service, its image and its chart) are
documented in this file. The published language has its own changelog,
[`crates/contract-threads/CHANGELOG.md`](crates/contract-threads/CHANGELOG.md).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z`: CD publishes a version only once its
heading exists.

## Unreleased

### Added

- **0.1.0 scaffold** (fatty manner on `br-service-engine` v0.4.1, soft EDA,
  multi-pod). A bootable, migrated, compiling skeleton the e2e oracle and the
  implementation build on:
  - Cargo workspace: `crates/contract-threads` (the published language) and
    `crates/svc-threads` (the service), MSRV 1.89, the engine's
    `br-rust-common` v1.3.0 pin, `br-llm-messages` v0.3.0.
  - Engine boot kit (`migrate | serve | schema`); the `threads` schema set in
    band `9_800_000_001..=9_800_999_999`, with the tables of the specification:
    `known_users`, `thread`, `thread_member`, `thread_resource`, `title_job`,
    `message`, `message_item`, `message_block`, `artifact`, `read_mark`,
    `fact`.
  - The scope `threads:runner` declared through the engine's handshake
    (readiness held until Identity accepts); the directory mirror of
    Identity's users into `threads.known_users`, a retracted user kept out of
    the roster with their last name; the principal, its roster standing and its
    resolver; the two admission rules (human surface: an active user;
    runner surface: a service account holding `threads:runner`).
  - Thread and message read paths: stores (set-based reads), membership
    visibility, the threads view (a member's list, most recently active first;
    one thread) and the messages view (a page of a branch), the
    `threads.members.{thread_id}` KV offer, the `message_content` accumulator
    bound to `STREAMING_threads`, the artifact blob kind (object storage
    required at boot).
  - The GraphQL surface under `threads…`: `threadsThreads`, `threadsThread`,
    `threadsMessages`, `threadsArtifactDownload`, `threadsContext`,
    `threadsSetReadMark`, `threadsRequestArtifactUpload`, `threadsSetTitle`,
    `threadsFailTitleJob`, `threadsThreadsDeltas`, `threadsThreadDeltas`.
    Write gestures and runner reads answer `NOT_IMPLEMENTED` until the
    implementation lands; the stores' write paths refuse.
  - e2e harness skeleton (real Postgres, NATS, MinIO, the spawned binary) and
    `scaffold_boot` scenarios: migrate, scope handshake, readiness, members
    manifest, roster projection and retraction, both admissions, the live list
    over SSE and WebSocket, the SDL.
  - Packaging: runtime-only `Dockerfile`, `scripts/publish.sh` (static-musl
    multi-arch image + chart), the thin chart `charts/br-svc-threads` over
    `br-service-engine-chart` 2.0.0; CI arms the Rust jobs and runs the e2e
    suite on real infrastructure; CD reads the service version from its crate
    and tags `contract-threads/v*`.
- Repository bootstrap: governance files (LICENSE, CONTRIBUTING, SECURITY, SUPPORT) ahead of the service scaffold.
- CI/CD workflows mirroring the sibling services, pre-scaffold-safe: a `scaffold probe` job skips the Rust checks until `Cargo.toml` lands; CD's detect-bump no-ops without a Cargo workspace. Branch protection script (`scripts/setup-branch-protection.sh`) as the declarative required-checks source of truth.

### Changed

- Repository renamed from `svc-chat` to `svc-threads`; the README, CI/CD workflows (release tag prefix `svc-threads/v*`) and branch-protection script use the new name.
