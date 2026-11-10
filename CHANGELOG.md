# Changelog

All notable changes to `svc-threads` (the service, its image and its chart) are
documented in this file. The published language has its own changelog,
[`crates/contract-threads/CHANGELOG.md`](crates/contract-threads/CHANGELOG.md).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z`: CD publishes a version only once its
heading exists.

## Unreleased

## 0.9.0 - 2026-11-10

Threads 0.9 (registry patch 0.9.0): a runner reads back the files of earlier
turns. With a thread's context it receives the `ATTACHED` artifacts of the
messages of the conversation it receives, and a time-bounded read access to
each it asks for. Additive: no command, event, KV entry, table or migration
changes; `contract-threads` is unchanged (0.8.0).

### Added

- **`ThreadContext.artifacts: [ContextArtifact!]!`** on `threadsContext`,
  whole or as an agent context: the `ATTACHED` artifacts of the messages of
  the conversation the runner receives (the active branch without its deleted
  messages, its cancelled messages and its messages failed before their first
  entry), beside the conversation — never inside it. In the order of their
  messages on the branch, then of the `ARTIFACT` annotation naming each in its
  message (its entry's position, then its own after that entry). A `PENDING`
  artifact is never given, nor an artifact of a message on another branch.
  Read in one statement for the whole conversation.
- **`type ContextArtifact { id: UUID!, name: String!, mediaType: String!,
  size: Int!, sha256: String!, messageId: UUID!, downloadUrl: String }`**:
  `size` in bytes (`Int`, as the upload request's), `sha256` the lowercase
  hexadecimal digest the upload was pinned to, `messageId` the message that
  attached it. `downloadUrl` is a time-bounded read access to its bytes, the
  engine's presigned GET, resolved only when selected: a context read that
  does not select it signs nothing. It is `null` once the artifact is gone.
- **The runner's gate on artifacts** (`RunnerArtifactsView`, visibility
  `RunnerReach`): the engine signs a runner's read access behind a view over
  the same artifact store, visible to a service account holding
  `threads:runner` for an `ATTACHED` artifact of any thread, and to nobody
  else. The admission of the context read is unchanged: a caller without
  `threads:runner` is refused `RUNNER_SCOPE_REQUIRED` and never reaches an
  artifact. The members' read access (`threadsArtifactDownload`) is unchanged;
  both paths sign through one function, which waits alike for the engine's
  promotion of verified bytes.
- e2e T36 (`t36_a_runner_reads_back_the_artifacts_of_the_conversation`): the
  artifacts of the active branch with their declaration and message, neither
  a `PENDING` one nor one of another branch, the same for an agent context,
  no read access unless asked for, the bytes read back through the one asked
  for, the refusal without `threads:runner`; and the order of a message's
  artifacts by their annotations.

- **A failed signing nulls that access only.** When the read access of one
  artifact cannot be signed, its `downloadUrl` is `null` and the failure is
  reported as one GraphQL error at that field's path
  (`["threadsContext","artifacts",i,"downloadUrl"]`); the context — its
  conversation, members, resources and the other accesses — is answered
  whole. `threadsContext: null` still means only that the thread does not
  exist.

### Cost

- Selecting `downloadUrl` signs every artifact of the context. The engine
  signs one reference at a time, so each costs three statements (its gated
  load, two, then its blob read; more only while the engine has not yet
  promoted verified bytes). A context read signs **at most 4 artifacts at
  once** (`SIGNING_CONCURRENCY`), whatever their number, so it never holds
  more than 4 of the pool's 20 connections for signing; the others wait
  their turn, none is dropped. A runner that does not need the bytes leaves
  `downloadUrl` unselected.
- A read access no longer waits for the engine's promotion of an artifact
  whose row is gone (deleted since the context was read): it answers `null`
  at once, for members and runners alike.

## 0.8.0 - 2026-11-05

Threads 0.8: Threads publishes the summary of each thread, so that a service
lists conversations without asking it (Front Agents' list of a user's agent
conversations is the first consumer). `contract-threads` 0.8.0 adds its
published language (see its changelog). Additive: no command, event, root or
existing KV entry changes.

### Added

- **The `thread_summary` KV offer**: one `threads.thread_summary.{thread_id}`
  entry of the `PUBLISHED_LANGUAGE` bucket per thread —
  `{ thread_id, title?, parent_thread_id?, origin_block_id?, status,
  resolved_at?, created_at, last_activity_at }` — and its manifest
  `threads.thread_summary_manifest` = `{"prefix": "threads.thread_summary.",
  "version": 1}`. Published by the engine's offer machinery like the members
  entry: staged in the transaction of every save or deletion of a thread,
  drained by the offer's leader, reconciled whole at boot and periodically
  (so the threads that exist before the upgrade are published by the first
  pod that leads the offer), retracted when the thread is deleted (on
  command, or because its owner left the roster).
- **When the entry moves.** `last_activity_at` is the thread's last
  activity, the order of a member's list: it moves when a message is stored —
  a complete message, or a streamed message when its stream opens — and when
  the writer of a streamed message closes it (see *Changed*). A chunk of a
  stream, an edit or a deletion of a message never save the thread, so a
  streamed answer rewrites the entry twice (opening and close), never per
  chunk. A save that leaves the summary as it was (members, resources,
  read marks) writes nothing: the relay finds the value already held. Staged
  keys coalesce per thread until drained; no timer coalescing is added.
- e2e T34 (`t34_the_summary_of_each_thread_is_published`): creation, title,
  a complete message moving the activity, a streamed message moving it at its
  opening (its chunks leave the entry's revision unchanged) and once more at
  its completion,
  resolution, a child thread's parent and origin, deletion retracting the
  key.

### Changed

- **A streamed message closed by its writer moves the thread's last
  activity.** `message.complete` (v1, v2), `message.fail` (v1, v2) and
  `message.cancel` of a `STREAMING` message now load the thread (row lock,
  thread before message, the order every message command takes), move its
  `last_activity_at` to the close and save it — so a conversation rises when
  an agent finishes answering, not only when it starts. This **changes the
  order of `threadsThreads` / `threadsThreadsDeltas`**: a thread whose answer
  just completed moves to the top of its members' lists (an `Upsert` with the
  new cause `MESSAGE_CLOSED`, and the open head repopulates), and its
  `lastActivityAt` reads the close's date. A stream failed by the idle watch
  (`stream_idle`) is not a writer's close and moves nothing. No fact, no
  version bump: activity is not a change of the thread.

### Unchanged

- `threads.members.{thread_id}` and its manifest (version 1): same key, same
  payload, same triggers.

## 0.7.0 - 2026-11-03

Threads 0.7 (registry patch 0.7.0): Threads no longer names a conversation. A
title is set by command (`thread.set_title`, unchanged) or by a runner,
directly (`threadsSetTitle(threadId, title)`, without any Job); naming a thread
is the commanding services' and the runners' concern (the agents service names
a thread after its first message). Threads no longer talks to Jobs. A member
lists the child threads of a thread. `contract-threads` 0.7.0 removes the title
Job's published language (see its changelog).

### Added

- **`threadsChildThreads(threadId: UUID!, size: Int!, after: UUID):
  [ThreadView!]!`**: one page of the child threads of a thread that the caller
  is a member of, the oldest first (creation, then id), with the view of the
  thread list (`threadsThreads`) — `parentThreadId` and `originBlockId`
  included — and the same admission (an active user of the roster,
  `ACTIVE_USER_REQUIRED` otherwise). A child the caller is not a member of is
  left out without error. The list still works once the parent is deleted; it
  is empty for a thread that does not exist or has no child the caller sees.
  `after` names the last child of the previous page; an `after` that is not a
  child of that thread the caller is a member of (a child they do not see,
  another thread, an id nobody created) gives an empty page, as an unknown
  `before` does on the thread list. `size` is bounded as on the thread list
  (from 1, `WINDOW_SIZE_INVALID`; a page larger than the engine's window
  capacity is refused `WINDOW_TOO_LARGE` as there). A query only: no
  subscription, no command, no fact. One statement selects the page; the
  children are then read and projected as the thread list's are.
- **Migration `9800000007`**: the index `thread_child_idx` on
  `threads.thread (parent_thread_id, created_at, id)`, partial (`WHERE
  parent_thread_id IS NOT NULL`). A plain `CREATE INDEX` in the migration's
  transaction, not `CONCURRENTLY`: `migrate` runs while no pod serves.

### Removed

- **The title Job.** A thread's first `COMPLETED` message no longer creates a
  Job; a title set by command and a thread's deletion (on command, or because
  its owner left the roster) no longer cancel one.
- **`threadsFailTitleJob(threadId, jobId, reasonCode, message)`**, with the
  refusals `JOB_NOT_ACTIVE` and `FAILURE_REASON_INVALID`: there is no title Job
  to declare failed. The SDL loses this mutation root.
- **Every Jobs consumption**: Threads no longer publishes
  `integration.cmd.jobs.job.create.v1`, `job.finish.v2`, `job.fail.v2` or
  `job.cancel.v2`, and no longer consumes `integration.evt.jobs.job.
  creation_rejected.v1`, `job.failed.v1` or `job.cancelled.v1`: the durables
  `threads-title-job-rejected`, `threads-title-job-failed` and
  `threads-title-job-cancelled` are no longer registered. The `contract-jobs`
  dependency is gone.
- **`THREADS_TITLE_RUNNER_TYPE`** is no longer read.
- The delta cause `TITLE_JOB_ENDED` is no longer emitted.

### Changed

- **Breaking for runner clients: `threadsSetTitle(threadId: UUID!, title:
  String!): MutationAck!`** — the `jobId` argument is gone, and with it the
  title-Job gate. Any service account holding `threads:runner` sets the title
  of any thread (a default title, the title of a thread it created), exactly
  as `thread.set_title` does: the same transition (1 to 200 characters,
  `TITLE_EMPTY` / `TITLE_TOO_LONG`; the same title is acknowledged and changes
  nothing; a resolved or closed thread's title can still change), the same
  save and the same live delta to the thread's members. A caller without the
  scope is refused `RUNNER_SCOPE_REQUIRED`, a thread that does not exist
  `THREAD_NOT_FOUND`. A runner client that still sends `jobId` (the runner
  kit's `ThreadClient::set_title` of the title-Job era) is refused by GraphQL
  validation (unknown argument) and must drop it. `threads:runner` now allows
  reading a thread's context, requesting an artifact upload and setting a
  thread's title.
- **`TitleChanged` says who set the title**: `by: COMMAND` for
  `thread.set_title`, `by: RUNNER` for `threadsSetTitle` (the fact's actor is
  then the runner's service account, with no acting user).
- **Migration `9800000006`** drops `threads.title_job`. A title Job still
  `RUNNING` when it runs is forgotten: Threads neither finishes nor cancels it
  at Jobs (README, *Deploy: 0.6 → 0.7*); its runner's `threadsSetTitle` with a
  `jobId` is refused as above. Past facts are not rewritten: a `TitleChanged`
  fact a title Job recorded keeps `by: TITLE_JOB`.

### Tests

- T12 (the title Job) is deleted with the harness's Jobs stand-in. T14 and T17
  lose their title-Job clauses: neither expects a `job.cancel`, and T17 no
  longer publishes Jobs' `job.cancelled`. T27 proves that Threads commands
  nobody on the whole command stream instead of on Jobs' subjects, on an
  untitled thread and counting from before its first message — the gesture
  that started a title Job up to 0.6.
- T33: listing the child threads of a thread — a member lists the children
  they are a member of, oldest first, each with its origin block and the view
  of the thread list, by pages after a child; a child they are not a member of
  is left out; the list survives the parent's deletion; a thread without child
  and an unknown id give an empty list. A second test pins what the
  specification leaves open: an `after` that is not a child the caller sees
  gives an empty page, a size out of bounds is refused `WINDOW_SIZE_INVALID`
  and a service account `ACTIVE_USER_REQUIRED`.
- T32 (proposed, not yet in the registry): a runner sets a thread's title —
  acknowledged, read by the members, live on a member's thread feed and on the
  owner's list, recorded `by: RUNNER` with the runner as actor; the same title
  changes nothing; an empty or 201-character title, a member, a service
  account without `threads:runner` and an unknown thread are each refused with
  their code and change nothing; a resolved, then closed, thread's title can
  still be set.

## 0.6.0 - 2026-10-29

Threads 0.6 (registry patch 0.6.0): a writer keeps a waiting stream alive and
names the closure of the stream it ends; a restarted runner can close the turn
its predecessor left open; the title Job starts at the first completed message;
an agent context keeps less of what another agent received.
`contract-threads` 0.6.0 carries the new published language (see its
changelog).

### Added

- **Keepalive chunk** on `stream.threads.{message_id}` (same subject and
  frame): it folds to nothing, takes a sequence number, enters the seal digest,
  and counts as activity for the silence rule. It adds no entry, no fact and no
  version change, may come inside a response still producing, works on a
  resolved thread, and changes nothing for a message that is not `STREAMING`.
- **`message.complete.v2` and `message.fail.v2`**: the writer may name the
  closure reason among the library's five (completed, cancelled, failed,
  interrupted, budget exhausted), with its detail. The status follows the
  reason (completed and budget exhausted `COMPLETED`, cancelled `CANCELLED`,
  failed and interrupted `FAILED`). A closure the library cannot read, such as
  failed without a detail, is refused `MESSAGE_INVALID` and the message stays
  `STREAMING`. A chunk the library refused fails the message whatever reason is
  named. Completed, named or not, still needs a whole turn: a response still
  producing or a call left without a result fails the message (`failed`);
  budget exhausted and every other reason leave them as the library allows. The
  version 1 commands behave as in 0.5.
- The closing fact (`Completed`, `Failed`, `Cancelled`) carries the `closure`
  kind of the turn; the detail of a closure is in no fact.

### Changed

- **The title Job starts at the first message to be `COMPLETED`**, whatever its
  kind (a user message, an AI message, a request stored as an input turn, a
  stream completed by `complete` or by `fail.v2` naming a completed closure),
  in the transaction that completes it; `triggered_by` is the user author of the
  stored message when a user wrote it, otherwise the acting user of the command
  (the acting user alone when a stream is closed), when there is one. A message
  that ends `CANCELLED` or `FAILED` starts none. In 0.5 only a user message did.
- **A stored AI turn is checked against its closure when it loads**: a turn
  whose status differs from the status its closure gives, or whose closure
  payload cannot be read, fails to load as corrupt.
- **Agent context**: another agent's turn keeps only its model responses; what
  that agent received (notices, tasks, mails, requests, human messages) is no
  longer in it.
- Comments removed from the files this release touches. The message close
  matrix moved out of `draft.rs` into `close.rs`, `closure.rs` and `closers.rs`.

### Checked

- `br-agentic-contract` 0.1.0 (tag `br-agentic-contract/v0.1.0`): nothing in
  `contract-threads` or `svc-threads` duplicates its types. Threads owns no
  runner-side port or wire type of the agentic contract; its agent ids and
  closure reasons are the `br-llm-messages` ones. No dependency is added.

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
