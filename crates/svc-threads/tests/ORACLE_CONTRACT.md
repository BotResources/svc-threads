# The surface the 0.5.0 oracle is written against

The registry's sealed patch 0.5.0 is the canon: its offers fix the wire of
`message.store` v2, `message.edit`, `message.complete` and the message stream
word for word. This file names the Rust symbols and the GraphQL fields the
suite compiles and asserts against where the registry names none. The suite
does not compile until `contract-threads` 0.5.0 exposes them. A rename here is
an alignment of the suite, never a change of what it asserts.

## `contract-threads` 0.5.0

- `pub use br_llm_messages as llm;` re-exports **br-llm-messages 2.0.0**. The
  suite builds every library value with the library's own constructors and
  serializes it with the library's `Serialize`.
- `ThreadsCommand` gains `const VERSION: u8` (default `1`). `coords()` and the
  envelope's schema version use it. `StoreMessageV2::VERSION == 2`.
- `StoreMessageV2 { message_id, thread_id, parent_message_id: Option<Uuid>,
  job_id: Option<Uuid>, acting_user_id: Option<Uuid>, impersonator_id:
  Option<Uuid>, message: NewTurn }`. It is `ThreadsCommand` with aggregate
  `message`, verb `store`, version 2, so it travels on
  `integration.cmd.threads.message.store.v2` with `command_type`
  `message.store`.
- `NewTurn` is tagged by `"kind"`:
  - `User(UserTurn)` is `"USER"`, with `UserTurn { entry: serde_json::Value,
    annotations: Vec<PlacedAnnotation> }`. `entry` is the library's tagged
    turn-entry JSON (`{"type":"human_message", ...}`). `annotations` is
    `#[serde(default, skip_serializing_if = "Vec::is_empty")]`. It exists only
    to be refused `MESSAGE_INVALID` / `annotation_in_user_message` (T05).
  - `Ai(AiTurnMessage)` is `"AI"`, with `AiTurnMessage { agent_id: Uuid,
    author_name: String, #[serde(flatten)] delivery: TurnDelivery }`.
- `TurnDelivery` is tagged by `"status"`:
  - `Completed { turn: serde_json::Value, annotations: Vec<PlacedAnnotation> }`
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
- `Chunk` is tagged by `"kind"`, with variants `Delta { delta:
  serde_json::Value }` `"DELTA"` (a library `ResponseDelta`), `Entry { entry:
  serde_json::Value }` `"ENTRY"` (a library turn entry) and `Annotation {
  after_entry_id: Uuid, annotation: Annotation }` `"ANNOTATION"`.
- `ChunkFrame { accumulator, key, seq, chunk }`. `ChunkFrame::new(message_id,
  seq, chunk)` sets `accumulator` to `TURN_CONTENT_ACCUMULATOR`
  (`"turn_content"`). `subject()` and `msg_id()` are as in 0.4. `seal_hash(&[Chunk])`
  hashes as in 0.4.
- `EditMessage { message_id, thread_id, acting_user_id, expected_version,
  content: Vec<serde_json::Value> }`. Each item of `content` is the library's
  plain-block JSON.
- Unchanged from 0.4: `CompleteMessage`, `FailMessage`, `CancelMessage`,
  `DeleteMessage`, `StreamSeal`, every thread command, the four answer events,
  `ReasonCode`, `MessageStatus`, `ClosingStatus`, `PublishedThreadMembers`,
  `members::members_key`, `MEMBERS_MANIFEST_KEY`, `TitleJobConfig`.
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
  `Failed` fact whose `payload.reason` is the library's code.
- The closure Threads appends on `complete`, `fail`, `cancel` or a silent writer
  has an id Threads mints: a UUIDv7, unique in the thread.
- A conversation's and a projected thread's `ConversationId` is the thread's id.

## GraphQL (0.5.0 SDL, as the suite reads it)

```graphql
type MessageView {            # blocks and items are gone
  id threadId parentMessageId jobId status deleted createdAt editedAt version affordances
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
  conversation: JSON          # no agent named: the active branch, br-llm-messages/5; null otherwise
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
