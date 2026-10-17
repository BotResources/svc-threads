# Changelog

All notable changes to `svc-threads` (the service, its image and its chart) are
documented in this file. The published language has its own changelog,
[`crates/contract-threads/CHANGELOG.md`](crates/contract-threads/CHANGELOG.md).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z`: CD publishes a version only once its
heading exists.

## Unreleased

### Security

- `Cargo.lock` only: `ctutils` 0.4.3 → 0.4.6 (RUSTSEC-2026-0385, non-constant-time
  mask generation and `u128` comparisons), which failed `cargo audit` and
  `cargo deny` on every PR. No code, contract or version change: the fix ships
  with the next release.

### Added

- Release pipeline integration with the production Services registry:
  `scripts/registry-gate.sh` (sealed-and-not-implemented gate at PR time,
  pre-build, and pre-push), `scripts/registry-docs.sh` (procedural SDL + DB
  schema posed from the built artifacts before the image push), and
  `scripts/registry-implement.sh` (image record + advisory implemented-flip
  probe after the push). CI/CD only — no service behavior changes.

## 0.4.0 - 2026-10-20

Threads 0.4 (registry patch 0.4.0): a user message is edited in place by its
author, and a thread is resolved and reopened. `contract-threads` 0.4.0 carries
the new commands and reason codes (see its changelog).

### Added

- **`message.edit`** (`integration.cmd.threads.message.edit.v1`): a `COMPLETED`
  user message that is not deleted, rewritten in place by its author from its
  current version. Its blocks are replaced whole and judged as a stored user
  message's (at least one text, no artifact); a block id of the edited message
  may be kept, any other held id is `KEY_REUSED`. The message keeps its id, its
  place in the tree and its descendants; `edited_at` is set and `version`
  raised. Refusals in order: `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`,
  `THREAD_RESOLVED`, `MESSAGE_NOT_FOUND`, `MESSAGE_NOT_EDITABLE`,
  `NOT_THE_AUTHOR`, `VERSION_CONFLICT` (naming `current_version`), then the
  content (`ID_NOT_UUIDV7`, `KEY_REUSED`, `MESSAGE_INVALID`). An `Edited` fact
  records the new blocks' kinds and positions, never their text; followers
  receive the updated message live.
- **`thread.resolve` / `thread.reopen`**: a resolved thread records
  `resolved_by` / `resolved_at` and stays `OPEN`. While resolved, storing,
  editing and deleting a message are refused `THREAD_RESOLVED`; a `STREAMING`
  message still completes, fails or cancels; title, members, resources, read
  marks, closing and deletion still apply, and closing keeps the resolution.
  Repeats are refused `THREAD_ALREADY_RESOLVED` / `THREAD_NOT_RESOLVED`; a
  closed thread is neither resolved nor reopened (`THREAD_NOT_OPEN`).
  `Resolved` / `Reopened` facts; followers receive the thread live.
- GraphQL: `MessageView.editedAt`, `ThreadView.resolvedBy`,
  `ThreadView.resolvedAt`.
- Migration `9800000004_edit_and_resolve`: `threads.message.edited_at`,
  `threads.thread.resolved_by` / `resolved_at` (set together).

### Changed

- `message.store` and `message.delete` are refused `THREAD_RESOLVED` on a
  resolved thread.

### Compatibility

- `contract-threads` 0.4.0's `ReasonCode` is closed: consumers of Threads'
  refusals must move to 0.4.0 (see its changelog).
- **Rolling upgrade.** While 0.3.1 and 0.4 pods run side by side, a 0.3.1 pod
  knows no resolution: it may accept a `message.store` or `message.delete` on a
  thread a 0.4 pod resolved. Nothing is corrupted (the resolution columns are
  untouched and the message is stored or deleted as on an open thread); the
  window closes when the last 0.3.1 pod is gone.
- **NATS permissions.** The commanding services' publish permissions must allow
  the three new command subjects: `integration.cmd.threads.message.edit.v1`,
  `integration.cmd.threads.thread.resolve.v1` and
  `integration.cmd.threads.thread.reopen.v1`.

### Tests

- e2e scenarios T19 (editing), T20 (two edits from one version), T21 (an edit
  is live), T22 (resolving and reopening), T24 (a command received again: the
  stored answer replayed, then — once the harness expires it as the engine's
  retention sweep does — the command judged anew under its own guards). T09's
  and T10's transcripts follow their current gestures (T10: "a retry of M1").

## 0.3.1 - 2026-10-17

Dependency maintenance release: Threads moves from `br-service-engine` 0.4.1 to
0.4.2. No wire, schema, migration or published-language change; `contract-threads`
0.3.1 only moves its `br-core-integration` pin (see its changelog).

### Changed

- **`br-service-engine` v0.4.1 -> `br-service-engine/v0.4.2`.** No source change
  in Threads. The chart pairing is unchanged (`br-service-engine-chart` 2.0.0
  serves 0.4.2).
- **`br-rust-common` moves from the legacy workspace tag `v1.3.0` to the
  per-crate tags** `{crate}/v0.1.0`: `br-core-auth`, `br-core-directory`,
  `br-core-integration`, `br-core-scope`, `br-scope-declaration-contract`,
  `br-util-nats-fabric`; the engine brings `br-util-postgres` 0.1.1. The service
  graph resolves one copy of each `br-*` crate.
- **Jobs' published language** `contract-jobs` v0.5.0 -> `contract-jobs/v0.5.1`
  (re-pin only; its wire is byte-identical to 0.5.0), so `CommandCoords` and
  `EventCoords` are those of the per-crate `br-core-integration`.
- **The e2e harness** moves to the per-crate tags `br-test-harness/v0.1.0`,
  `conformance-scope/v0.1.0` and `conformance-directory/v0.1.0`. The
  conformance crates pin `br-test-harness` 0.1.0 and name its types, so the
  harness stays on 0.1.0: the suite holds one copy of it. Only the test graph
  holds two copies of `br-util-postgres` (0.1.0 through
  `conformance-directory` and `br-util-directory`, 0.1.1 through the engine);
  the service binary's graph holds one of each `br-*` crate.

### Fixed (through the engine)

- A database URL is no longer accepted without TLS on a remote host by reading
  its host after the first `@` of the whole URL; every entry point reads the
  host from the URL authority.
- `Engine::run` no longer fails at boot with `SchemaParse "stack limit
  reached"` on a static musl binary.
- An undecodable value under a consumed KV prefix (the directory mirror) is
  dead-lettered instead of restarting the mirror in a loop.
- A led mirror no longer drops a change on a multi-pod deployment.
- A pod that loses the schema-version claim stops (readiness lowered, workers
  stopped, `EngineError::SchemaClaimLost`) instead of only lowering readiness.
- `run_service` handles `SIGTERM` and `SIGINT` as a graceful shutdown.
- A `/graphql/ws` operation re-resolves its principal and its facts, so a member
  removed or deactivated while a socket is open loses access at the next
  operation instead of keeping the upgrade-time principal.
- `POST /graphql` and the `/graphql/ws` upgrade answer a refused passport with
  `401` and a GraphQL error envelope (`UNAUTHENTICATED`), not plain text.
- `rustls` 0.23.46 clears RUSTSEC-2026-0346 to -0349.

### Behavior changes (operators)

- **Stricter database URL.** `DATABASE_URL` and `DATABASE_URL_OWNER` must name
  their host explicitly in the authority: a URL with no scheme, a foreign
  scheme, no host (`postgres:///db`) or a `host=` / `hostaddr=` override is
  refused at boot, even on loopback or with `sslmode=require`.
- **`migrate` under replicas.** The engine's `migrate` grants app access in one
  transaction under an advisory lock with a bounded retry, so replicas that
  migrate together no longer fail with `tuple concurrently updated`.
- **Duration and blob-TTL checks at boot.** Every engine duration must be at
  least one microsecond; the blob upload window (`THREADS_UPLOAD_WINDOW_MS`)
  must lie between 1 millisecond and 7 days and the download TTL between 1
  second and 7 days. A value outside is refused at boot instead of booting a
  broken reaper or signing expired URLs.
- **`/graphql/ws` needs a principal.** Every socket operation resolves the
  principal again through Threads' registered `PrincipalResolver`; a principal
  that resolves to nothing is refused `UNAUTHENTICATED` for that operation.

## 0.3.0

### Breaking (consumers)

- **A runner reading `threadsContext` — the title runner included — loads a
  `br-llm-messages/3` conversation**: it needs `br-llm-messages` 0.5 or later
  (through `contract-threads` 0.3.0); 0.3 and 0.4 refuse the schema. Every
  entry now bears an id, its message's.
- **A writer's content meets the library's stricter rules**: a tool name
  matches `^[A-Za-z_][A-Za-z0-9_-]{0,63}$`; a tool call id is unique in its
  turn and on every branch of the thread through the message (a fork may
  reuse it); a tool result names its call's tool; a tool results item holds a
  result. Content that broke them in 0.2 is refused `MESSAGE_INVALID` (a whole
  message) or fails its stream.

### Changed

- **The conversation model is `br-llm-messages` v0.5.0, schema
  `br-llm-messages/3`** (was v0.3.0, `br-llm-messages/2`), through
  `contract-threads` 0.3.0. A runner's context names `br-llm-messages/3` in its
  conversation's `schema`.
- **A message's id is the id of its input or its turn.** A user message is
  checked and read back as a user input whose id is the message's; an AI
  message's turn already bore it. Every entry of a runner's conversation, whole
  or lightened, bears its message's id.
- **The library's stricter rules hold on every message**, refused
  `MESSAGE_INVALID` with the library's code in `error`: a tool name of 1 to 64
  ASCII letters, digits, `_` or `-` (`invalid_tool_name`); a tool call id used
  once in its turn (`duplicate_tool_call_id_in_turn`); a tool result naming the
  tool of the call it answers (`tool_name_mismatch`); a tool results item
  holding at least one result — Threads blocks alone are none
  (`empty_tool_results`). In a stream, the chunk the library refuses fails the
  message under the same code.
- **A tool call id is used once in the conversation, on every branch.** The
  library judges one conversation; Threads holds the rule for every branch
  through a message. A whole AI turn reusing a tool call id of a message before
  it on its branch is refused `MESSAGE_INVALID`
  (`duplicate_tool_call_id_in_conversation`, with `tool_call_id`); a fork is
  another branch and may reuse it, and a cancelled message is no
  conversation's. A stream is judged when it closes, against the messages
  before it and after it — one stored under it while it streamed included:
  completed or failed, it keeps its items up to the step that reuses an id; a
  cancel is sealed as received. Every message command locks the thread first,
  so of two messages on one branch the second written sees the first.
- **A failure names the cut its close made.** A message failed by its writer
  or by the stream watch whose content was cut short of what its stream held —
  a step reusing a tool call id of its branch, a step the library cannot
  finish (`unfinishable_step`), content dropped whole (`block_id_reused`) — has
  its `Failed` fact name that cut when the stream gave no reason of its own; a
  silent writer's stays `stream_idle`.
- **A value the library refuses names the library's code.** A library block,
  tool result or stream event whose JSON the library cannot load is refused
  with the code of the value it refuses (`invalid_tool_name`, `blank`, …),
  found with the library's own constructors for the shape expected there;
  `block_content_invalid` (`chunk_unreadable` for a stream event) remains for
  JSON that is not the library's shape. A blank user text is now refused
  `blank` (0.2: `block_content_invalid`). The refusal's `params` carry the
  detail the library names: `field` (`blank`, `invalid_json`), `tool_call_id`,
  `entry_id`.
- **The `error` codes of `MESSAGE_INVALID` are published**:
  `contract_threads::MessageInvalidError`, every one of the library's 38
  refusals and Threads' own. A library refusal of a later version is
  `library_refusal` until it is named.
- **A streamed step is stored as the library finished it.** The library drops
  a step's blank text blocks when it finishes it; Threads no longer stores
  them, so the stored blocks are the step's. A step still streaming shows them.
  A finished step whose blocks would not line up with what streamed fails the
  stream (`step_misaligned`) instead of being stored.
- A stream event this library does not know does not load: its chunk is
  refused `chunk_unreadable`. `event_not_supported` is kept for an event a
  later library loads that Threads does not fold yet.
- GraphQL: `ThreadContext.conversation`'s description names
  `br-llm-messages/3`; no type or field changes.

### Fixed

- A completion whose content was dropped whole between its precheck and its
  seal (a block id another message holds) was stored `COMPLETED` without
  items, and the thread's context then failed to load: it is now `FAILED`
  (`block_id_reused`).

### Performance

- Migration `9800000003_message_block_by_message`: an index on
  `message_block (message_id)`. A message's blocks were read, removed and
  cascaded by scanning every block; the close of a stream calling tools reads
  its branch's calls through it, once per close, and not at all when the
  stream calls no tool.

### Migration

- `9800000003_message_block_by_message` (schema: one index). No data
  migration: no Threads deployment holds data written by 0.1 or 0.2, so no
  stored conversation predates the stricter rules or the `br-llm-messages/3`
  schema; a stored message that broke them would no longer load in a runner's
  conversation.

## 0.2.0

### Changed

- **Command admission belongs to the NATS fabric.** Threads admits every
  decodable command whatever its envelope's actor kind: the actor is the
  facts' attribution (`actor_id`, `actor_kind`), never an authorization, and
  the payload's `acting_user_id` stays the user whose gesture led to the
  command. A command a commanding service stages from a GraphQL mutation —
  which `br-service-engine` envelopes under the human caller — is now answered
  like any other; in 0.1 it was dead-lettered unanswered. An envelope without
  metadata (no actor at all) is still dead-lettered.
- **A title holds 1 to 200 characters.** An empty title is refused
  `TITLE_EMPTY` (params `min_chars`) on `thread.create`, `thread.set_title` and
  the title Job runner's `threadsSetTitle`, where 0.1 stored it. Built on
  `contract-threads` 0.2.0.

### Fixed

- Registry breach `01a13f5b-bdfd-77a8-8f04-e67bdba00f09` (an empty title was
  accepted).

### Migration

- `9800000002_empty_title_is_no_title`: a thread stored with an empty title by
  0.1 becomes untitled (data only, no schema change), so that every stored
  title is a valid one when read back.

## 0.1.0

### Added

- **Threads 0.1.0** (fatty manner on `br-service-engine` v0.4.1, soft EDA,
  multi-pod):
  - Thread commands (`integration.cmd.threads.thread.{create, add_members,
    remove_members, add_resource_ids, remove_resource_ids, set_title, close,
    delete}.v1`) and message commands (`integration.cmd.threads.message.{store,
    complete, fail, cancel, delete}.v1`), accepted from service accounts only,
    each answered once (`thread|message.{confirmed|refused}.v1`) in the
    transaction of its change, a redelivered command replaying its answer; a
    guarded transition per command, its facts in `threads.fact` (never a
    message's content nor a display name).
  - Members published as `threads.members.{thread_id}` at every membership
    change, retracted at deletion; members' windows follow a membership change
    at once (principal-facts impacts).
  - Messages in the `br-llm-messages/2` model: a user input or an AI turn given
    whole, judged by the library (`MESSAGE_INVALID` with the library's error
    code); an AI turn streamed on `stream.threads.{message_id}`, each step's
    events judged as they fold, shown live to the members, sealed by
    `message.complete` (`{last_seq, hash}`, `STREAM_MISMATCH`), `fail` or
    `cancel`; a closed stream keeps every complete item, an unfinished step
    when the library can finish it, and answers every open tool call with an
    interrupted error result.
  - The stream watch: a writer silent for the idle window, or a stream the
    library refused, has its message failed; deleting a thread seals the
    streams of its streaming messages.
  - Live views: a stream's folds re-render the streaming message only
    (`message_stream` noun) and a read mark the reader's thread only
    (`thread_reader` noun); list and feed heads are repopulated by a thread's or
    a message's own change. A member revoked mid-session has their feed ended.
  - Caller-minted thread, message, block and artifact ids must be UUIDv7
    (`ID_NOT_UUIDV7`, `ARTIFACT_ID_NOT_UUIDV7`).
  - Forks and branches: the active branch ends at the most recent message;
    any branch is read page by page; a deleted message stays in the tree
    without content; only its user author deletes it.
  - The title Job, with Jobs as its owner: created at a thread's first user
    message, its runner sets the title or declares its failure (`JOB_NOT_ACTIVE`
    for anything but the active title Job), Threads finishes, fails or cancels
    it; a title set by command, or the thread's deletion, cancels it.
  - Artifacts: an upload access pinned to the declared size and digest, attached
    by a message of the same thread once its bytes are verified
    (`ARTIFACT_NOT_READY`), a member's read access, removed with its message,
    its thread, or at the end of the upload window.
  - The runner's context: the active branch as a conversation, whole or
    lightened for a named AI, every message carrying its author.
  - Read marks; child threads; an owner who leaves the roster takes the
    threads they own with them.
  - Threads' settings `THREADS_TITLE_RUNNER_TYPE`, `THREADS_STREAM_IDLE_MS`,
    `THREADS_UPLOAD_WINDOW_MS`.
  - The e2e oracle of the specification's scenarios (T01–T18) and the directory
    conformance battery.
- **0.1.0 scaffold**: the Cargo workspace (`contract-threads`, `svc-threads`),
  the engine boot kit, the `threads` schema, the scope `threads:runner` and its
  handshake, the directory mirror of Identity's users, the two admission rules,
  the read path, the GraphQL surface under `threads…`, the packaging (image,
  thin chart over `br-service-engine-chart` 2.0.0) and CI/CD.
- Repository bootstrap: governance files (LICENSE, CONTRIBUTING, SECURITY, SUPPORT) ahead of the service scaffold.
- CI/CD workflows mirroring the sibling services, pre-scaffold-safe: a `scaffold probe` job skips the Rust checks until `Cargo.toml` lands; CD's detect-bump no-ops without a Cargo workspace. Branch protection script (`scripts/setup-branch-protection.sh`) as the declarative required-checks source of truth.

### Changed

- Repository renamed from `svc-chat` to `svc-threads`; the README, CI/CD workflows (release tag prefix `svc-threads/v*`) and branch-protection script use the new name.
