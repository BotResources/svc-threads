# svc-threads

Threads is the shared, passive conversation store of the BotResources platform.
It keeps conversations between people and AIs — threads, their members and
resources, messages in the conversation model of
[`br-llm-messages`](https://github.com/BotResources/br-llm-messages-rs)
(2.1, schema `br-llm-messages/5`: each message is a turn), the artifacts runners attach to them, and each
member's read mark — and serves them, live, to their members.

- Threads never decides who may write: every write reaches it as a command from
  a trusted service that already took the decision. It only keeps the
  conversation coherent.
- Threads decides who may read: the members of a thread, and nobody else.
- Threads publishes the members of each thread (`threads.members.{thread_id}`),
  so that other services decide locally.
- Threads runs no AI logic; to name a new conversation it asks a runner through
  Jobs.

**Status: 0.6.0.** See [CHANGELOG.md](CHANGELOG.md).

## Layout

| Crate | What it is |
|---|---|
| [`crates/contract-threads`](crates/contract-threads) | The published language: the commands Threads accepts, the one answer it publishes per command, the members KV value, the content model and the chunk format of its message stream, the title Job configuration. What a commanding service imports. |
| [`crates/svc-threads`](crates/svc-threads) | The service, on [`br-service-engine`](https://github.com/BotResources/br-service-engine) (fatty manner, soft EDA, multi-pod), organized by capability under `src/slices/` (`thread`, `message`, `artifact`, `runner`). |

## Running

`svc-threads [migrate|serve|schema]` — the engine boot kit's three entry points
(the engine README's *Ops contract v1*):

- `migrate`: `DATABASE_URL_OWNER`, `APP_ROLE`;
- `serve`: converts, once and before the pod is ready, the messages an
  earlier Threads stored (under a Postgres advisory lock: one pod converts,
  the others wait); then serves. `DATABASE_URL`, `APP_ROLE`, `NATS_URL`, `ENGINE_CHANNEL`, `HOSTNAME`,
  `PORT`, and the object-storage group `S3_ENDPOINT`, `S3_BUCKET`,
  `S3_ACCESS_KEY`, `S3_SECRET_KEY` (`S3_REGION`, `S3_PUBLIC_ENDPOINT`
  optional) — Threads refuses to serve without object storage;
- `schema`: prints the SDL, touches no infrastructure.

Threads' own settings, read at `serve`, each with its default:

| Variable | Default | What it sets |
|---|---|---|
| `THREADS_TITLE_RUNNER_TYPE` | `thread_title` | the runner type of the title Job Threads asks Jobs to run |
| `THREADS_STREAM_IDLE_MS` | `600000` (10 min) | how long a writer may push nothing before its `STREAMING` message is failed |
| `THREADS_UPLOAD_WINDOW_MS` | `900000` (15 min) | how long an upload access stays valid, and a `PENDING` artifact waits for its message |

Declared infrastructure, bound and never created (a missing piece keeps the pod
not ready): PostgreSQL in session mode (one owner role with `BYPASSRLS`, one app
role), the NATS streams `INTEGRATION_CMD`, `INTEGRATION_EVT` (bounded
`max_age`) and `STREAMING_threads` (subjects `stream.threads.>`, `max_age`
within 24 h), the `PUBLISHED_LANGUAGE` KV bucket, and an S3-compatible bucket
honouring POST-policy checksum conditions.

## Deploy: 0.4 → 0.5

The first 0.5 `serve` converts every message an earlier Threads stored to the
`br-llm-messages` 2.0 model before the pod becomes ready
(`src/slices/message/upgrade.rs`): it judges them all first — each converted
in memory, every id checked once — and writes nothing if one cannot be
converted (the pod then never becomes ready and logs the message ids); then it
writes one transaction per batch of 256 messages. A crash resumes where it
stopped; a pod that boots once the conversion is done finds nothing to do.

- **The rollout is `Recreate`.** 0.4 pods cannot read what 0.5 writes, and two
  versions must never share the store: the engine's chart renders
  `strategy: Recreate` (ops contract v1), which the 0.5 rollout needs. Do not
  override it, and do not run a 0.4 pod beside a 0.5 one.
- **Raise the startup probe.** The conversion runs inside the boot, before
  `serve` binds its listener: `/livez` and `/readyz` both fail until it ends
  (see the engine gap below). The chart's default startup budget is 150 s
  (`probes.startup`: `periodSeconds` 5 × `failureThreshold` 30); a pod killed
  by the probe mid-conversion only restarts it — every converted batch is kept
  — but never becomes ready while the budget is shorter than the conversion.
  Set `probes.startup.failureThreshold` so that the budget covers the
  messages to convert.
- **How long it takes.** Measured on a release build against a local
  PostgreSQL 14 (loopback, no network round trip), messages of 1 to 4 blocks:
  50 000 messages in 28 s, about 0.6 ms of work per message (≈ 1 800
  messages/s); a boot with nothing left to convert takes milliseconds. The
  reads are batched (three statements per 256 messages, plus one identity
  check); the writes are about six statements per message, so on a real
  network budget roughly `N × (0.6 ms + 6 × RTT)`: with a 0.5 ms round trip,
  ≈ 36 s per 10 000 messages, ≈ 6 min per 100 000. Count the messages to
  convert with `SELECT count(*) FROM threads.message` before the rollout.
- **Rollback to 0.4** is an emergency path, not a downgrade: `migrate` keeps
  `threads.message.content_schema`'s default `br-llm-messages/3`, so a 0.4 pod
  brought back keeps storing and answering commands, and the messages it
  stores are converted at the next 0.5 boot. What 0.4 cannot do is read what
  0.5 wrote or converted: an AI message in the 2.0 form has no 0.4 author id
  (a thread holding one fails to read in 0.4), and a user message written by
  0.5 has no 0.4 blocks. An edit or a deletion 0.4 makes to a message already in
  the 2.0 form does not reach its 2.0 records: re-apply it after rolling
  forward.

ENGINE GAP (br-service-engine v0.4.2): the boot kit offers no asynchronous
step between the migration check and readiness, and no service input to
readiness. The conversion therefore runs inside the synchronous `register`
(`block_in_place`, which needs the multi-threaded runtime `serve` runs on),
before the listener is bound. With such a step the engine could keep `/livez`
up and `/readyz` down while it runs, and the startup probe would not have to
cover it.

## Tests

```bash
cargo test --workspace                       # unit tests; the e2e scenarios are #[ignore]d

# the e2e suite: real PostgreSQL, a spawned nats-server, the nats CLI and minio on PATH
E2E_PG_ADMIN_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  cargo test -p svc-threads --tests -- --include-ignored --test-threads=1
```

`cargo fmt` does not reach the slice modules `compose_service!` declares; check
them with `rustfmt --edition 2024 --check crates/svc-threads/src/slices/*/mod.rs`.

## CI/CD

CI (`.github/workflows/ci.yml`) gates pull requests; CD (`cd.yml`) owns pushes
to `main`: image-first, tag-after — a `svc-threads/v*` tag is a receipt that the
image and the chart shipped, and `contract-threads/v*` releases the published
language (the tag is the release).

| Thing | Why it is the way it is |
|---|---|
| `scaffold probe` job | Kept as a required check from the bootstrap: it arms the Rust jobs once a Cargo workspace exists. |
| `scripts/setup-branch-protection.sh` | Declarative source of truth for the required checks; each entry must match a `ci.yml` job `name:` verbatim or PRs block forever waiting for a check that never reports. |
| `integration (e2e)` builds MinIO and natscli from source | MinIO publishes no binary any more and an artifact upload needs its POST-policy checksum conditions; the `nats` CLI declares the streams the harness cannot, and publishes the message stream's frames. Both are pinned and cached. |
| The chart is thin | `charts/br-svc-threads` depends on the engine's library chart `br-service-engine-chart` and supplies values only; `ci/lint-values.yaml` sets the deployment-owned keys for the render. |
| Per-crate changelogs | `CHANGELOG.md` is the service's (its `## x.y.z` heading publishes), `crates/contract-threads/CHANGELOG.md` the contract's (its heading tags). |

## License

Apache-2.0 — see [LICENSE](LICENSE). This repository is published read-only and
does not accept external contributions; see
[CONTRIBUTING.md](CONTRIBUTING.md) and [SUPPORT.md](SUPPORT.md).
