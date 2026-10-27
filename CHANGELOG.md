# Changelog

All notable changes to `svc-threads` (the service, its image and its chart) are
documented in this file. The published language has its own changelog,
[`crates/contract-threads/CHANGELOG.md`](crates/contract-threads/CHANGELOG.md).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z`: CD publishes a version only once its
heading exists.

## Unreleased

## 0.5.0 - 2026-10-27

Threads 0.5 (registry patch 0.5.0): a message is a turn of the
`br-llm-messages` 2.1 model (schema `br-llm-messages/5`), stored as the
library's records (`br-llm-messages-records/1`) with Threads' own annotations;
a runner receives the agent context Threads sorts for the agent it answers for.
`contract-threads` 0.5.0 carries the new published language (see its
changelog).

### Added

- **Built on `br-llm-messages` 2.1.0** (tag `br-llm-messages/v2.1.0`; the
  schemas are unchanged). A 2.0 reader refuses what a 2.1 writer may write, so
  Threads, the reader, reads 2.1 before any runner writes it: a
  `runtime_notice` entry is stored and shown, in an input turn and in an AI
  turn (an agent context keeps another agent's like its other incoming
  entries); entries are taken in every order the library accepts — an incoming
  entry or a model response at any point of an open turn — on `message.store`
  v2 and on the stream; a tool name is up to 256 characters; a whole AI turn
  may hold, after its closure, the results of a call the closure left
  outstanding. Threads' own stream rules stay: one response is produced at a
  time (`response_still_producing`), the closure is Threads'
  (`closure_by_writer`), and a closed message takes no more chunks — a result
  arriving after Threads closed a stream does not join it. The
  `threads.message_entry.kind` CHECK of migration `9800000005` admits
  `runtime_notice`.
- **`message.store` v2** (`integration.cmd.threads.message.store.v2`): a user
  message is the input turn, under the message's id, holding one human message
  of a member (the library's plain blocks: text, image, structured content);
  an AI message is a whole `/5` turn — an AI turn of its agent (the library's
  agent id, a UUIDv7, and its name), stored as the library accepts it: its
  writer's closure gives the message's status (`completed`, `budget_exhausted`
  → `COMPLETED`; `cancelled` → `CANCELLED`; `failed`, `interrupted` →
  `FAILED`), and Threads closes it completed when it holds none — or an input
  turn of incoming entries (requests, mail, task notices; a human message is
  refused `MESSAGE_INVALID`, `human_message_in_ai_message`: it travels in a user
  message only) — or an AI turn opened `STREAMING`. Every entry the library
  accepts is stored and shown, with no behaviour of Threads'. Annotations
  (`ARTIFACT`, `THREAD_REFERENCE`, `JOB_REFERENCE`, `ERROR`, `WARNING`) follow
  an entry of an AI message; a user message holds none. What the library
  refuses is `MESSAGE_INVALID` naming the library's error code (`object_id`,
  `location`), but an id the caller supplies for a new object — an entry's, a
  block's, a citation's, an annotation's — that is no UUIDv7 is
  `ID_NOT_UUIDV7` (a reference that is none — an agent, a human, the call a
  result answers — stays `MESSAGE_INVALID`); every id of a message, its
  entries, blocks, citations and annotations is used once (`KEY_REUSED`,
  through `threads.message_identity`). A string holding U+0000, which the
  library accepts and Postgres cannot store, is refused `MESSAGE_INVALID`
  (`nul_character`) — in a stored message, an edit, or a chunk (which fails its
  stream).
- **The message stream folds the library's response deltas** with the
  library's `ResponseDraft` (`DELTA` chunks), whole entries (`ENTRY`) and
  annotations (`ANNOTATION`) into the library's `AITurn`, on the accumulator
  `turn_content`. Members see a response being produced live, as the library's
  draft snapshot. Completing, failing or cancelling closes the turn with the
  library's closure (under a UUIDv7 Threads mints); a response still produced is
  kept interrupted (members see it, a runner never); a tool call without its
  result stays unanswered; a completion the library cannot close (a response
  still produced, a call unanswered) fails the message; a silent writer closes
  it interrupted. A frame under the 0.4 accumulator name (`message_content`)
  is refused: its message fails, closed failed. A completion is refused, in
  order, `MESSAGE_NOT_FOUND`, `MESSAGE_NOT_STREAMING`, `ARTIFACT_NOT_READY`
  (judged on the stream before its seal) and `STREAM_MISMATCH`; it never drops
  an `ARTIFACT` annotation — one its seal reaches that the stream did not hold
  yet when it was judged rolls the completion back, seal included, and its
  redelivery judges it again. A fail, a cancel or the watch, which nobody can
  refuse, drops an `ARTIFACT` annotation whose artifact is not ready. Members
  and runners see a stream judged as its close judges it, the ids other
  messages hold included.
- **The agent context**: `threadsContext(threadId, agentId)` answers, for the
  agent named (a UUIDv7, else `AI_PERSPECTIVE_INVALID`), the library's
  projected thread (`projectedThread`, `br-llm-messages-projected-thread/1`)
  built by `RecordSet::into_projected_thread_pending` from one statement over
  the active branch — the input turns and the agent's own turns whole, another
  agent's turns reduced to their responses' root text, image and structured
  blocks (payload not read) and their incoming entries, another agent's
  response still produced given as pending — and, naming none, the whole branch
  as a `br-llm-messages/5` conversation (`conversation`).
- **The upgrade**: the messages Threads 0.4 or earlier stored are converted
  once at boot, before the service is ready, under a Postgres advisory lock
  (multi-pod): judged all first — every message converted in memory, every id
  checked once across them and against the messages already in the 2.0 form —
  then one transaction per batch of 256, resumable
  (`threads.message.content_schema`); the reads are batched (three statements
  per batch). Measured: 50 000 messages in 28 s on a release build against a
  local PostgreSQL; see the README's *Deploy* section for the startup probe it
  needs. A store fault while writing is logged with its SQLSTATE and
  constraint. Message ids (= turn ids) and block ids are
  kept; entry and closure ids are derived (`derive(source, role)`: the
  source's 48-bit timestamp, `SHA-256(source ‖ role)`); a step becomes a model
  response (stop reason → ending, usage under the 2.0 names, a thinking's
  signature → replay material `anthropic.signature`, a redacted thinking →
  `anthropic.redacted_thinking`, a tool call → a runtime tool call whose 0.4 id
  is its external id), a tool result answers its call's block, a status becomes
  the turn's closure, Threads blocks become annotations; a `STREAMING` message
  becomes `FAILED`, closed interrupted. No fact is added, no version raised. A
  message that cannot be converted (an AI author id that is no UUIDv7, a 0.4
  `CITATION` block, …) is logged by its id and keeps the service from becoming
  ready; nothing is converted then.
- Migration `9800000005_llm_messages_2_records`: `threads.message_entry`,
  `threads.entry_block` (the library's records), `threads.message_annotation`,
  `threads.message_identity`; `threads.message` gains `content_schema`,
  `turn_kind`, `author_agent_id`, `observed_at`. The 0.4 tables
  (`message_item`, `message_block`) are kept as they are, read by the upgrade
  only; deleting or editing a message removes its rows there too.

### Changed

- **`message.store` v1 is deprecated**: still accepted, converted to the 2.0
  model and answered on the same events — with the answers 0.4 gave: a user
  author outside the thread is `NOT_A_MEMBER` before anything of the content,
  and the block ids are judged as 0.4 judged them before the conversion (each a
  UUIDv7, else `ID_NOT_UUIDV7`; each named once, else `KEY_REUSED`). A v1 AI
  message opened `STREAMING` is refused `MESSAGE_INVALID` (`streaming_v1`), and
  its author id must be a UUIDv7 (`MESSAGE_INVALID`).
- **`message.edit`** carries `content`, the human message's new plain blocks;
  the 0.4 `blocks` are still accepted, judged as 0.4 judged their ids, and
  converted. A block id that is not a UUIDv7 is `ID_NOT_UUIDV7`.
- A command that loses a race for a key (two stores of one message, two
  contents declaring one id: the database's unique violation) is retried
  instead of dead-lettered, so that its redelivery is answered `KEY_REUSED`.
- GraphQL: `MessageView` loses `blocks` and `items`, gains `turnKind` and
  `entries { id index kind state content annotations { id index kind content } }`
  — a complete entry's `content` is the library's entry JSON, a response being
  produced or interrupted the library's draft snapshot; `AuthorView.aiId` is
  the agent id. `threadsContext` takes `agentId: String` instead of `ai:
  AiPerspective` and answers `conversation` or `projectedThread`. The
  `ItemView`, `ItemKind`, `BlockView` types and the 0.4 `BlockKind` enum are
  gone; `EntryState`, `TurnKind`, `AnnotationKind`, `EntryView`,
  `AnnotationView` are new.
- The message facts are `Created` (author, parent, job, status, turn kind, and
  the kinds and positions of the entries of a message given whole, their blocks
  and annotations — never their content), `EntryAdded` (one per entry a stream
  adds, but the closure), `Completed` / `Failed` (its `reason`, the library's
  code for a refused stream) / `Cancelled`, `Edited`, `Deleted`.

### Security

- `Cargo.lock` only: `ctutils` 0.4.3 → 0.4.6 (RUSTSEC-2026-0385, non-constant-time
  mask generation and `u128` comparisons), which failed `cargo audit` and
  `cargo deny` on every PR.

### Compatibility

- **Not a rolling upgrade.** 0.4 pods cannot read what 0.5 writes: stop them
  before 0.5 serves — the chart's `Recreate` strategy, which the rollout needs.
  `migrate` only adds tables and columns; the conversion runs at the first 0.5
  `serve`, before the listener is bound: raise the startup probe to cover it
  (README, *Deploy*).
- **Rollback.** `threads.message.content_schema` keeps its default
  `br-llm-messages/3`: a 0.4 pod brought back keeps storing, and its messages
  are converted at the next 0.5 boot. 0.4 cannot read the AI messages 0.5 wrote
  or converted, and its edits and deletions of messages already in the 2.0 form
  do not reach their 2.0 records (README, *Deploy*).
- **NATS permissions.** The commanding services' publish permissions must allow
  `integration.cmd.threads.message.store.v2`.
- A 0.4 writer's chunks (`message_content`, the 0.4 chunk format) fail their
  message: writers move to the `turn_content` chunks of `contract-threads` 0.5.

### Engine gaps (br-service-engine v0.4.2)

- No asynchronous boot step between the migration check and readiness, nor a
  service readiness input: the upgrade runs inside the synchronous `register`
  (`block_in_place`), with `/livez` and `/readyz` both down meanwhile. A boot
  step could keep `/livez` up and `/readyz` down instead.
- `ChunkReader` reads one stream per call: the members' view and the agent
  context read the streams of their `STREAMING` messages concurrently, one
  read each; a batched `ChunkReader::states(&[key])` would make it one.

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
