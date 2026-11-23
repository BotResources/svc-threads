# The surface the 0.5.0 oracle is written against

The registry's sealed patch 0.5.0 is the canon: its offers fix the wire of
`message.store` v2, `message.edit`, `message.complete` and the message stream
word for word. This file names the Rust symbols and the GraphQL fields the
suite compiles and asserts against where the registry names none. The suite
does not compile until `contract-threads` 0.5.0 exposes them. A rename here is
an alignment of the suite, never a change of what it asserts. The type names
follow the implementation in progress where its wire agrees with the offers;
where the two differ, the suite follows the offer: the `USER` form carries
`entry`, annotations travel as `{ after_entry_id, annotation }` inside the
`AI` form and in the `ANNOTATION` chunk, and an `Annotation` is `{ id, kind,
content }`.

## `contract-threads` 0.5.0

- `pub use br_llm_messages as llm;` re-exports **br-llm-messages 2.1.0**. The
  suite builds every library value with the library's own constructors and
  serializes it with the library's `Serialize`.
- `LibraryJson`, the library's JSON carried as is (`#[serde(transparent)]`
  over a `serde_json::Value`), with `from_json(Value)` and `as_json()`.
  Threads judges it; decoding a command never fails on it.
- `ThreadsCommand` gains `const VERSION: u8` (default `1`). `coords()` and the
  envelope's schema version use it. `StoreMessageV2::VERSION == 2`.
- `StoreMessageV2 { message_id, thread_id, parent_message_id: Option<Uuid>,
  job_id: Option<Uuid>, acting_user_id: Option<Uuid>, impersonator_id:
  Option<Uuid>, message: MessageTurn }`. It is `ThreadsCommand` with aggregate
  `message`, verb `store`, version 2, so it travels on
  `integration.cmd.threads.message.store.v2` with `command_type`
  `message.store`.
- `MessageTurn` is tagged by `"kind"`:
  - `User(UserTurn)` is `"USER"`, with `UserTurn { entry: LibraryJson,
    annotations: Vec<PlacedAnnotation> }`. `entry` is the library's tagged
    turn-entry JSON (`{"type":"human_message", ...}`). `annotations` is
    `#[serde(default, skip_serializing_if = "Vec::is_empty")]`. It exists only
    to be refused `MESSAGE_INVALID` / `annotation_in_user_message` (T05).
  - `Ai(AiTurn)` is `"AI"`, with `AiTurn { agent_id: Uuid,
    author_name: String, #[serde(flatten)] delivery: AiDelivery }`.
- `AiDelivery` is tagged by `"status"`:
  - `Completed { turn: LibraryJson, annotations: Vec<PlacedAnnotation> }`
    is `"COMPLETED"`. `turn` is the library's `/5` turn JSON. A whole AI turn
    carries its own closure (T27). `annotations` defaults to empty.
  - `Streaming` is `"STREAMING"`.
- `PlacedAnnotation { after_entry_id: Uuid, annotation: Annotation }`.
- `Annotation { id: Uuid, #[serde(flatten)] body: AnnotationBody }`.
  `AnnotationBody` is `#[serde(tag = "kind", content = "content")]`, with
  variants `Artifact(ArtifactRef)` `"ARTIFACT"`, `ThreadReference(ThreadRef)`
  `"THREAD_REFERENCE"`, `JobReference(JobRef)` `"JOB_REFERENCE"`,
  `Error(Notice)` `"ERROR"` and `Warning(Notice)` `"WARNING"`. `ArtifactRef {
  artifact_id }`, `ThreadRef { thread_id }`, `JobRef { job_id }` and `Notice {
  code, text: Option<String> }` keep their 0.4 shapes.
- `Chunk` is tagged by `"kind"`, with variants `Delta { delta: LibraryJson }`
  `"DELTA"` (a library `ResponseDelta`), `Entry { entry: LibraryJson }`
  `"ENTRY"` (a library turn entry) and `Annotation { after_entry_id: Uuid,
  annotation: Annotation }` `"ANNOTATION"`.
- `ChunkFrame { accumulator, key, seq, chunk }`. `ChunkFrame::new(message_id,
  seq, chunk)` sets `accumulator` to `TURN_CONTENT_ACCUMULATOR`
  (`"turn_content"`). `subject()` and `msg_id()` are as in 0.4. `seal_hash(&[Chunk])`
  hashes as in 0.4.
- `EditMessage { message_id, thread_id, acting_user_id, expected_version,
  content: Vec<LibraryJson> }`. Each item of `content` is the library's
  plain-block JSON.
- Unchanged from 0.4: `CompleteMessage`, `FailMessage`, `CancelMessage`,
  `DeleteMessage`, `StreamSeal`, every thread command, the four answer events,
  `ReasonCode`, `MessageStatus`, `ClosingStatus`, `PublishedThreadMembers`,
  `members::members_key`, `MEMBERS_MANIFEST_KEY`. (`TitleJobConfig` was
  part of this list until `contract-threads` 0.7.0 removed it with the title
  Job.)
- The suite never uses the 0.4 content types (`Block`, `Item`, `ChunkPart`,
  `LibraryEvent`, ...). It sends `store.v1` through `contract-threads` 0.4.0 by
  its git tag (dev-dependency `contract-threads-v0-4`), as a consumer still on
  v1 does.

## Refusals and facts

- `MESSAGE_INVALID` carries `params.error`. For the library's refusals, this is
  the library's `ErrorCode::as_str()`, which the suite computes with the library
  on the same input. Otherwise it is Threads' own code from the store.v2 offer:
  `annotation_in_user_message`, `turn_id_mismatch`, `author_mismatch`,
  `block_kind_not_allowed`, ...
- Message facts are `Created`, `EntryAdded`, `Completed`, `Failed`, `Cancelled`,
  `Edited` and `Deleted`. The closure is the end fact (`Completed` / `Failed` /
  `Cancelled`), never an `EntryAdded`. A stream the library refuses records a
  `Failed` fact whose `payload.reason` is the library's code; a stream refused
  on Threads' own rules (`response_in_entry_chunk`, `response_still_producing`,
  `closure_by_writer`) records Threads' code there (T07).
- An entry, block or annotation id that is not a UUIDv7 is refused
  `ID_NOT_UUIDV7`, an id another message holds `KEY_REUSED` — judged before
  the library is asked. A v1 store and an edit in the 0.4 form get 0.4's
  answers: `ID_NOT_UUIDV7`, and `KEY_REUSED` for a block id used twice or held
  by another message (T25, T28).
- A whole AI turn closed otherwise than `completed` is accepted, its status
  given by its closure: `completed` / `budget_exhausted` → `COMPLETED`,
  `cancelled` → `CANCELLED`, `failed` / `interrupted` → `FAILED`. An AI
  message whose turn is an input turn holding a `human_message` is refused
  `MESSAGE_INVALID` / `human_message_in_ai_message` (T27).
- A completion whose stream holds an `ANNOTATION` naming an artifact whose
  bytes have not landed is refused `ARTIFACT_NOT_READY`, however close behind
  the chunk it comes (T13).
- The closure Threads appends on `complete`, `fail`, `cancel` or a silent writer
  has an id Threads mints: a UUIDv7, unique in the thread.
- A conversation's and a projected thread's `ConversationId` is the thread's id.

## GraphQL (0.5.0 SDL, as the suite reads it)

```graphql
type MessageView {            # blocks and items are gone; parentMessageId too (0.12.0)
  id threadId jobId status deleted createdAt editedAt version affordances
  author: AuthorView!         # aiId: the agent id, a UUIDv7 string
  turnKind: TurnKind!         # INPUT_TURN | AI_TURN
  entries: [EntryView!]!      # in order; [] for a deleted message
}
type EntryView {
  id: UUID!
  index: Int!                 # position in the turn, from 0
  kind: String!               # the library's entry kind: human_message, model_response, ...
  state: EntryState!          # PRODUCING | COMPLETE | INTERRUPTED (the library's record states)
  content: JSON!              # COMPLETE: the library's turn-entry JSON;
                              # PRODUCING / INTERRUPTED: the library's DraftSnapshot (br-llm-messages-draft/1)
  annotations: [AnnotationView!]!   # the annotations placed after this entry, in order
}
type AnnotationView { id: UUID! index: Int! kind: AnnotationKind! content: JSON! }
enum AnnotationKind { ARTIFACT THREAD_REFERENCE JOB_REFERENCE ERROR WARNING }

# `ai: AiPerspective` is replaced by the agent id alone:
threadsContext(threadId: UUID!, agentId: String): ThreadContext
type ThreadContext {
  threadId: UUID!
  conversation: JSON          # no agent named: the thread in order (0.12.0), br-llm-messages/5; null otherwise
  projectedThread: JSON       # an agent named: br-llm-messages-projected-thread/1; null otherwise
  members: [MemberView!]!
  resources: [ResourceView!]!
}
```

An agent id that is not a UUIDv7 is refused with the error extension code
`AI_PERSPECTIVE_INVALID`.

## The upgrade (T26)

T26 runs the previous release, `svc-threads` 0.4.0, which it builds once with
`cargo install --git <this repository> --tag svc-threads/v0.4.0`. Set
`THREADS_PREVIOUS_BIN` to skip the build. T26 commands that release through its
own commands, stops it, and starts this version on the same database with
`migrate`, then `serve`. A message that cannot be converted makes `migrate`
fail or keeps `serve` from becoming ready. Either way, the message id is named
in that output.

## 0.6.0 additions

- `Chunk::Keepalive` (wire `{"kind":"KEEPALIVE"}`), built by `Chunk::keepalive()`.
- `CompleteMessageV2 { message_id, thread_id, seal: StreamSeal, closure:
  Option<LibraryJson>, acting_user_id }` and `FailMessageV2 { message_id,
  thread_id, seal: Option<StreamSeal>, closure: Option<LibraryJson>,
  acting_user_id }`: `ThreadsCommand`, aggregate `message`, verbs `complete`
  and `fail`, version 2. `closure` is the library's closure reason JSON.
- The closing fact's payload carries `closure`, one of `completed`,
  `budget_exhausted`, `cancelled`, `interrupted`, `failed`.

## 0.7.0 changes

- The title Job is gone: no `job.*` command, no Jobs stand-in in the harness,
  no `threadsFailTitleJob` root (T12 deleted).
- The runner's title operation stays, without its `jobId` (breaking for runner
  clients such as the runner kit's `ThreadClient::set_title`):

  ```graphql
  threadsSetTitle(threadId: UUID!, title: String!): MutationAck!
  ```

  Open to a service account holding `threads:runner`, for any thread
  (`RUNNER_SCOPE_REQUIRED` otherwise, a human caller included); the same
  decision as `thread.set_title` (`TITLE_EMPTY`, `TITLE_TOO_LONG`, the same
  title acknowledged and changing nothing, a resolved or closed thread's title
  still settable); `THREAD_NOT_FOUND` for a thread that does not exist. Its
  `TitleChanged` fact's payload is `{ title, by: "RUNNER" }`, its actor the
  runner's service account (`actor_kind` `service`), no acting user; a command's
  says `by: "COMMAND"` (T32, proposed by the oracle).

- The children of a thread, a query of the human surface (T33):

  ```graphql
  threadsChildThreads(threadId: UUID!, size: Int!, after: UUID): [ThreadView!]!
  ```

  The children of `threadId` the caller is a member of, the oldest first
  (`createdAt`, then `id`), each the very `ThreadView` `threadsThread` answers
  for it; `after` is the last child of the previous page. An `after` that is
  not a child of `threadId` the caller is a member of gives an empty page; a
  thread that does not exist, or whose children the caller sees none of, an
  empty list. `size` out of bounds is refused `WINDOW_SIZE_INVALID`, a caller
  the human surface does not admit `ACTIVE_USER_REQUIRED`. The suite reads it
  with `Q_CHILD_THREADS` (`common::constants`), which selects every field
  `Q_THREAD` selects.

## 0.7.0 removals

- `contract-threads` 0.7.0 removes the `title_job` module: `TitleJobConfig`
  (and its re-export at the crate root) and the root-name constants
  `CONTEXT_ROOT`, `TITLE_ROOT` and `FAILURE_ROOT`. The suite names roots in
  `common::constants` only.
- The title roots of the title-Job era: `threadsFailTitleJob(threadId, jobId,
  reasonCode, message)` is gone, with `JOB_NOT_ACTIVE` and
  `FAILURE_REASON_INVALID`; `threadsSetTitle(threadId, jobId, title)` is gone
  in that form and returns as a generic runner operation without `jobId`
  (above, *0.7.0 changes*; T32).
- The harness's Jobs stand-in (`common::jobs`: the capture of the `job.*`
  commands Threads sent and the `job.*` events the scenarios answered with),
  `THREADS_TITLE_RUNNER_TYPE` and `TITLE_RUNNER_TYPE`. In their place,
  `TestContext::commands_not_sent_by_the_scenario()` counts the integration
  commands on the bus that the scenario did not send through
  `flows::command`: read before and after a gesture, the difference is what
  Threads commanded meanwhile (T27 proves it nothing, from before an untitled
  thread's first message).

## 0.12.0 changes — one list

Threads drops the message tree: a thread's messages are one list, ordered by
`created_at`, then `id`. No parent, no branch, no fork.

- **Commands.** `message.store` v3 is `message.store` v2 without
  `parent_message_id`; v1 and v2 are refused `COMMAND_VERSION_RETIRED` in a
  `message.refused` answer and store nothing (T40). `message.erase` v1
  (`integration.cmd.threads.message.erase.v1`) carries `thread_id`,
  `message_id`, `including: bool` and an optional `acting_user_id`: it removes
  every message stored after the named one, and the named one when
  `including`, whoever wrote them and whatever their status, with their
  entries, blocks, annotations, artifacts (bytes included) and the read marks
  set on them. Refusals, in that order: `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`,
  `THREAD_RESOLVED`, `MESSAGE_NOT_FOUND`; nothing to remove is confirmed and
  changes nothing; a late chunk or close of an erased `STREAMING` message finds
  no message (`MESSAGE_NOT_FOUND`). Answered by `message.confirmed` naming the
  command, the thread and the message named; the suite does not assert the
  answer's `status`, which the registry leaves open (T39).
- **Where the suite builds them.** `contract-threads` 0.9.0 carries neither, so
  the suite builds both on the offers' wire in `common::wire`
  (`StoreMessageV3`, `EraseMessages`, each a `contract_threads::ThreadsCommand`:
  aggregate `message`, verbs `store` v3 and `erase` v1). The contract crate is
  expected to expose its own types for them (and `ReasonCode::
  CommandVersionRetired`); the suite then switches to those and `common::wire`
  goes. Until the closed `ReasonCode` knows `COMMAND_VERSION_RETIRED`, an
  answer the contract cannot decode is kept raw (`commands::Answer::Undecoded`)
  and `flows::message_refused_code` reads its code from the JSON.
- **The retired versions on the bus.** `store.v1` travels through
  `contract-threads` 0.4.0 (dev-dependency `contract-threads-v0-4`), `store.v2`
  through `contract-threads` 0.9.0 by its git tag (dev-dependency
  `contract-threads-v0-9`), as consumers still on them send them
  (`commands::send_v2`, `previous::command_v2`).
- **The fact.** The thread fact `MessagesErased` is read under the payload keys
  `message_id`, `including` and `count` (an assumption of the oracle: the
  registry names the three values, not their keys); it never carries content.
  An erased message's artifact ends on its `Removed` fact.
- **GraphQL.** `MessageView.parentMessageId` is gone, and so is `branchTip`
  from `threadsMessages`, `threadsThreadDeltas` and `threadsContext` (T40
  checks the SDL holds neither). `threadsMessages(threadId, size, before)`
  still answers a page the newest first, `before` naming the oldest message of
  the previous page; `threadsThreadDeltas(threadId, size)` follows the head
  page of the list. A runner names the message its context stops at — the
  messages up to that one included, whole or as an agent's context, with the
  `ATTACHED` artifacts of those messages — with an argument the registry does
  not name: the suite assumes **`upTo: UUID`**, defined once in
  `common::constants` (`context_cut_arg!`, used by `Q_CONTEXT_UP_TO`,
  `Q_CONTEXT_ARTIFACTS`, `Q_CONTEXT_ARTIFACT_ACCESS`); a rename is that one
  line. A named message that is not one of the thread's gives the thread's
  context with an empty conversation and no artifact.
- **The suite's reads.** `flows::page` / `page_answer` read one page;
  `flows::messages` / `message_ids` read the whole list, oldest first;
  `flows::message_view` finds one message in it; `flows::context_up_to` reads
  the cut. `branch`, `branch_answer`, `active_branch`, `context_on`,
  `feed::thread_feed_on` and `Q_CONTEXT_ON` are gone, and every message
  builder of `common::flows` lost its parent.
- **The upgrade from branches (T40).** `svc-threads` 0.11.0, the last release
  with branches, is built once from its tag (`previous::branches_release_binary`,
  `TestContext::setup_branches_release`; `THREADS_BRANCHES_RELEASE_BIN` skips
  the build) and commanded with its own `store.v2`; it is read before the
  upgrade with its own SDL (`previous::Q_MESSAGES_0_11`,
  `message_view_0_11`). The active branch (the parent chain ending at the most
  recent message) stays with, for each message of it, the latest AI answer of
  each agent whose answer is not on it; every other message goes, found
  nowhere in the store but the fact log's history. T26, which upgrades
  straight from 0.4.0, meets the same rule: its "second answer to the same
  message" is another agent's, so it stays.
- **Scenarios.** New: T39 (`t39_erasing_the_end_of_a_thread.rs`) and T40
  (`t40_one_list_order_pages_cut_retired_store_versions_upgrade.rs`, two
  tests). Rewritten on their 0.12.0 gestures: T05, T06, T08, T09, T11, T19,
  T26, T31 (one test now, the silence case on thread V within it). Removed:
  T10 (forks and the active branch) and T25 (store.v1 converted), deleted from
  the registry; `t37_a_runner_reads_the_context_of_a_named_branch.rs`,
  superseded by T40's cut; `t38_following_a_named_branch_live.rs`, a branch
  feed that no longer exists. T36 keeps its scenario, the artifact "of another
  branch" read as the artifact of a message stored after the message the
  runner names. The registry gestures of T36 and T37 still speak of branches:
  to be corrected by the operator.

## 0.11.0 additions

- `contract-threads` 0.9.0, module `resources`: `PublishedThreadResources {
  resources: BTreeMap<String, Vec<String>> }` (an empty object for a thread
  without resources), `RESOURCES_PREFIX` (`threads.resources.`),
  `RESOURCES_MANIFEST_KEY` (`threads.resources_manifest`),
  `RESOURCES_VERSION` (1) and `resources_key(thread_id)`.
- `TestContext::resources_entry(thread)` reads the entry with its KV revision
  (an unchanged revision proves nothing rewrote it),
  `TestContext::resources_until(thread, what, wanted)` waits for it,
  `TestContext::resources_settled(thread)` reads it after the quiet window,
  and `TestContext::resources_manifest()` reads the raw manifest.
- Registry T03 is now *Resources: named id lists, published per thread*: the
  file is `t03_resources_named_id_lists_published_per_thread.rs`, test
  `t03_resources_are_named_id_lists_shown_to_members_and_runners_and_published_per_thread`
  (formerly `t03_resources_named_id_lists_never_published.rs`). Each change
  rewrites the entry (a new revision), each no-op leaves its revision; the
  command's answer is still the one event, and the members entry is left as
  it was.
- Registry T38 (*Thread resources: published, rewritten on change, removed on
  deletion*) is `t38_the_resources_of_each_thread_are_published.rs`, written
  step by step on the registry gesture, quoted verbatim in its header. Tests:
  `t38_each_thread_publishes_its_resources_from_its_creation_to_its_deletion`
  (T and U, from their creation to their deletion, U through its owner's
  departure) and
  `t38_the_threads_of_the_previous_version_get_their_entry_and_keep_it_across_a_restart`
  (the upgrade, then one restart). "The previous version" is the last
  release, `svc-threads` 0.10.1, built once from its tag like 0.4.0 for T26
  (`previous::last_release_binary`, `TestContext::setup_last_release`;
  `THREADS_LAST_RELEASE_BIN` skips the build). "After any restart the entries
  match the threads" is read after one restart: a probe thread created with
  resources after the restart is published (the restarted pod leads the
  offer, and its boot reconciliation runs before its first drain; awaited
  for 60 s, past the killed pod's lease, 30 s by default), then the
  entries of the threads the previous version stored hold the same values at
  the same revisions. "Leaves the entry as it was" is value and revision; a
  title change and a stored message are compared once the summary entry shows
  them (a positive sign the relays drained), a no-op command (which stages
  nothing) after the quiet window.
- Numbering: the file `t38_following_a_named_branch_live.rs` carries the
  number the oracle proposed in 0.10.1, before the registry gave T38 to the
  resources; the files are told apart by their titles, as the summary's
  `t34_…` file is the registry's T35.

## 0.10.1 additions (removed in 0.12.0)

- `threadsThreadDeltas(threadId: UUID!, size: Int!, branchTip: UUID)`. With
  `branchTip`, the head page followed is that of the branch ending at that
  message (it and its ancestors) instead of the active branch — the window
  `threadsMessages` reads with the same `branchTip`, live. The tip is looked
  for among the thread's own messages: a message no thread holds, or a
  message of another thread, ends no branch, and the feed opens on the thread
  and no message (no refusal); a tip named before its message is stored
  enters the feed once it is. The suite opens it with
  `feed::thread_feed_on`; T38 (number proposed by the oracle) asserts it.

## 0.10.0 additions (removed in 0.12.0: the cut replaces `branchTip`)

- `threadsContext(threadId: UUID!, agentId: String, branchTip: UUID):
  ThreadContext`. With `branchTip`, the conversation — whole or the agent
  context — and the `ATTACHED` artifacts beside it are those of the branch
  ending at that message (it and its ancestors) instead of the active
  branch. The tip is looked for among the thread's own messages, as
  `threadsMessages` looks for it: a message no thread holds, or a message of
  another thread, ends no branch, and the context of the thread is answered
  with an empty conversation and no artifact (no refusal; `null` still means
  only that the thread does not exist). A deleted tip — or a cancelled one,
  or one failed before its first entry — still ends its branch and is left
  out of the conversation, as such a message is anywhere on a branch; the
  active tip reads exactly as no tip. The suite reads it with `Q_CONTEXT_ON`
  (`common::constants`, `flows::context_on`); T37 (number proposed by the
  oracle, to be declared in the registry) asserts every case.

## 0.9.1 changes

- The `content` of an `ARTIFACT` annotation in a member's message view
  (`threadsMessages`, `threadsThreadDeltas`) is `{ artifact_id, name,
  media_type, size }`, read from the artifact attached to that message;
  `{ artifact_id }` alone when the artifact is not attached to it (a stream's
  annotation before its close). T06, T11, T25 and T26 assert the new content
  where they read an `ARTIFACT` annotation; T13's
  third scenario asserts it in the paged read, live and in a `Reset`, and
  that a runner's conversation still carries no annotation.

## 0.9.0 additions

- `ThreadContext.artifacts: [ContextArtifact!]!` and `ContextArtifact { id:
  UUID!, name: String!, mediaType: String!, size: Int!, sha256: String!,
  messageId: UUID!, downloadUrl: String }`, the names a runner library binds
  to. The suite reads them with `Q_CONTEXT_ARTIFACTS` (no `downloadUrl`) and
  `Q_CONTEXT_ARTIFACT_ACCESS` (with it), whole or for an agent
  (`common::constants`). T36 asserts the list whole against the declarations
  of the files the runner uploaded, the absence of any URL when
  `downloadUrl` is not selected, the bytes behind the one selected, and the
  order of a message's artifacts by their annotations (proposed by the
  oracle from the contract's ordering rule; the registry gesture names the
  order of the messages only).

## 0.8.0 additions

- `contract-threads` 0.8.0, module `summary`: `PublishedThreadSummary {
  thread_id, title: Option<String>, parent_thread_id: Option<Uuid>,
  origin_block_id: Option<Uuid>, status: ThreadStatus, resolved_at:
  Option<DateTime<Utc>>, created_at, last_activity_at }`, `SUMMARY_PREFIX`
  (`threads.thread_summary.`), `SUMMARY_MANIFEST_KEY`
  (`threads.thread_summary_manifest`), `SUMMARY_VERSION` (1) and
  `summary_key(thread_id)`.
- `TestContext::summary_entry(thread)` reads the entry with its KV revision
  (an unchanged revision proves nothing rewrote it), and
  `TestContext::summary_manifest()` the raw manifest. T34 asserts the entry
  against the thread as its members read it (`createdAt`, `lastActivityAt`,
  `resolvedAt`, `parentThreadId`, `originBlockId`).
