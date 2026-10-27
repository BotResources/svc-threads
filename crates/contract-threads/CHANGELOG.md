# Changelog

All notable changes to `contract-threads`, the published language of
`svc-threads`, are documented in this file. The crate is released by its tag
`contract-threads/v{version}` alone; consumers depend on it by git tag.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z`: CD tags a version only once its heading
exists.

## 0.5.0 - 2026-10-27

The published language of Threads 0.5 (registry patch 0.5.0): a message is a
turn of the `br-llm-messages` 2.1 model (schema `br-llm-messages/5`).

### Added

- `llm` re-exports **br-llm-messages 2.1.0** (was 0.5.0): commanders build
  turns, entries, blocks and response deltas with the exact library version
  Threads judges them by. 2.1 is additive over 2.0, but a 2.0 reader refuses
  what only 2.1 accepts (a `runtime_notice` entry, an order of entries 2.0
  refused, results after a closure for a call it left outstanding, a tool name
  over 64 characters): a commander writes those once the Threads it talks to
  reads 2.1. The refusal vocabulary is unchanged: 2.1 adds no error code.
- `LibraryJson`: the library's JSON carried as is and judged by Threads
  (`to_turn`, `to_entry`, `to_plain_block`, `to_delta`): a command or a chunk
  whose library part the library refuses still decodes, and is answered.
- `StoreMessageV2` (`integration.cmd.threads.message.store.v2`, `command_type`
  `message.store`, schema version 2): `message` is a `MessageTurn` —
  `USER` (`UserTurn { entry, annotations }`: the library's human message, its
  human the author; `annotations` exists to be refused
  `annotation_in_user_message`) or `AI` (`AiTurn { agent_id, author_name,
  delivery }`, `AiDelivery::Completed { turn, annotations }` — the library's
  `/5` turn, an AI turn of the agent or an input turn of incoming entries — or
  `AiDelivery::Streaming`).
- `Annotation { id, kind, content }` (`AnnotationBody`: `ARTIFACT`,
  `THREAD_REFERENCE`, `JOB_REFERENCE`, `ERROR`, `WARNING`, their 0.4 content
  shapes), `AnnotationKind`, `PlacedAnnotation { after_entry_id, annotation }`.
- `ThreadsCommand::VERSION` (default 1): the subject's `v{N}` and the
  envelope's schema version; `coords::versioned_command_coords`.
- `TURN_CONTENT_ACCUMULATOR` (`turn_content`).
- `v1`: the 0.4 forms of a message — `v1::StoreMessage` (`message.store` v1,
  deprecated, converted by Threads), `v1::Block`, `v1::BlockBody`, `v1::Item`,
  `v1::StepItem`, `v1::BlockKind`, … — the 0.4 library's content as raw JSON.

### Changed

- **Breaking:** `Chunk` is `DELTA { delta }` (a library `ResponseDelta`),
  `ENTRY { entry }` (a whole entry other than a model response) or
  `ANNOTATION { after_entry_id, annotation }`; `ChunkFrame::new` names the
  accumulator `turn_content`. `seal_hash` hashes as before.
- **Breaking:** `EditMessage` carries `content`, the human message's new plain
  blocks (the library's JSON), instead of `blocks`. (Threads still accepts the
  0.4 `blocks` on the same offer.)
- **Breaking:** `MessageInvalidError` is the 2.0 vocabulary: the 19 codes of
  the library's `ErrorCode` under their own names (`MessageInvalidError::
  of_library`), `library_refusal`, and Threads' own codes (`turn_id_mismatch`,
  `not_a_user_turn`, `annotation_in_user_message`, `annotation_entry_unknown`,
  `author_mismatch`, `human_message_in_ai_message`, `nul_character`,
  `parent_not_in_thread`, `chunk_unreadable`, `response_in_entry_chunk`,
  `response_still_producing`, `closure_by_writer`, `streaming_v1`,
  `v1_not_convertible`).
- `StoreMessageV2`: a whole AI turn closed by its writer is stored as the
  library accepts it, its closure giving the message's status (`completed`,
  `budget_exhausted` → `COMPLETED`; `cancelled` → `CANCELLED`; `failed`,
  `interrupted` → `FAILED`). An id of an entry, a block or a citation that is
  not a UUIDv7 is `ID_NOT_UUIDV7`; a reference that is none stays
  `MESSAGE_INVALID`. The documented refusal orders of `CompleteMessage`,
  `FailMessage` and `CancelMessage` name `ARTIFACT_NOT_READY` before
  `STREAM_MISMATCH`.

### Removed

- **Breaking:** `StoreMessage`, `NewMessage`, `UserMessage`, `AiMessage`,
  `AiAuthor`, `Block`, `BlockBody`, `LibraryBlock`, `Item`, `StepItem`,
  `ToolResultsItem`, `Citation`, `ChunkPart`, `LibraryEvent`, `BlockKind`,
  `LibraryValueError` and `MESSAGE_CONTENT_ACCUMULATOR` at the crate root
  (the 0.4 store forms live on under `v1`).

### Unchanged

- Thread commands, `CompleteMessage`, `FailMessage`, `CancelMessage`,
  `DeleteMessage`, `StreamSeal`, the four answer events, `ReasonCode`,
  `MessageStatus`, the members KV value, `TitleJobConfig`.

## 0.4.0 - 2026-10-20

### Added

- `EditMessage` (`message.edit`): `message_id`, `thread_id`, a required
  `acting_user_id`, `expected_version` and the new `blocks`.
- `ResolveThread` (`thread.resolve`, with a required `acting_user_id`, the
  resolver) and `ReopenThread` (`thread.reopen`).
- `ReasonCode` variants `THREAD_RESOLVED`, `THREAD_ALREADY_RESOLVED`,
  `THREAD_NOT_RESOLVED`, `MESSAGE_NOT_EDITABLE`, `VERSION_CONFLICT` (its
  refusal names `current_version` and `expected_version` in `params`).

### Compatibility

- `ReasonCode` is a closed enum: a consumer built on 0.3.x fails to decode a
  `thread.refused` / `message.refused` carrying one of the new codes. Every
  service that reads Threads' refusals must move to 0.4.0 before it sends the
  new commands — and before Threads 0.4 can refuse its `message.store` or
  `message.delete` with `THREAD_RESOLVED`.
- **Rolling upgrade.** While 0.3.1 and 0.4 pods run side by side, a 0.3.1 pod
  knows no resolution: it may accept a `message.store` or `message.delete` on a
  thread a 0.4 pod resolved. Nothing is corrupted (the resolution columns are
  untouched and the message is stored or deleted as on an open thread); the
  window closes when the last 0.3.1 pod is gone.
- **NATS permissions.** The commanding services' publish permissions must allow
  the three new command subjects: `integration.cmd.threads.message.edit.v1`,
  `integration.cmd.threads.thread.resolve.v1` and
  `integration.cmd.threads.thread.reopen.v1`.

### Changed

- `ORIGIN_BLOCK_REQUIRED`, `ID_NOT_UUIDV7` and `RESOURCE_KEY_INVALID` are named
  by the specification: their documentation loses the *proposed* mark. No
  wire change.

## 0.3.1 - 2026-10-17

- Dependency re-pin only. The contract is unchanged: coordinates, payload
  shapes, the chunk format and the error vocabulary are byte-identical to 0.3.0.
- `CommandCoords`, `EventCoords` and the other `br-core-integration` types the
  crate names are those of `br-core-integration/v0.1.0`, pinned at the per-crate
  tag instead of the legacy workspace tag `v1.3.0`. A platform consumer on
  `br-service-engine` 0.4.2 pins `contract-threads/v0.3.1` together with the
  per-crate tags and resolves one copy of each type; a consumer still on
  `v1.3.0` keeps `contract-threads/v0.3.0`.

## 0.3.0

### Added

- `LibraryEvent`: one stream event of the library carried as the library's
  JSON, as `LibraryBlock` carries a block — `of(&StreamEvent)` writes it (the
  same JSON value `br-llm-messages` 0.3 serialized, pinned per event),
  `from_json` takes it raw, `to_event()` reads it by the library's rules
  without copying, `into_event()` consuming it.
- `MessageInvalidError` (`invalid`): the vocabulary of a `MESSAGE_INVALID`
  refusal's `error` param, `#[non_exhaustive]`, with its wire literals
  (`as_str`, `parse`, serde) — the code of each of the library's 38 refusals
  (`of_library`, `library_refusal` for one of a later version) and of
  Threads' own rules.
- `LibraryValueError`: why the contract cannot write a library value
  (`UnsupportedAssistantBlock`, `UnsupportedStreamEvent`, `Encode`).

### Changed

- **Breaking:** `contract_threads::llm` re-exports `br-llm-messages` v0.5.0
  (was v0.3.0); the stored schema is `br-llm-messages/3`. Its breaking changes
  reach every consumer naming the library through the contract: types are
  reached from the crate root only; `MessageError`, `StreamEvent`, `BlockKind`
  and `AssistantBlock` are `#[non_exhaustive]`; `UserInput::new` takes an
  `InputId` first; tool names, tool call ids and tool results follow stricter
  rules; `StreamEvent` is no longer `Serialize`. See the library's changelog
  for 0.4.0 and 0.5.0.
- **Breaking:** `ChunkPart::StepEvent::event` is a `LibraryEvent` instead of a
  `StreamEvent`, the library no longer serializing stream events. The wire is
  unchanged: the event is the same JSON under `event`, and `seal_hash` gives
  the same digest. A chunk whose event the library refuses (an invalid tool
  name, say) now decodes, for Threads to fail its message with the library's
  reason.
- **Breaking:** `BlockBody::assistant` returns a `LibraryValueError`, and
  refuses an `AssistantBlock` a later library adds
  (`UnsupportedAssistantBlock`) until the contract names its kind.
- `LibraryBlock::to_user`, `to_assistant` and `to_tool_result` read the JSON
  without copying it.
- The documentation of the content and the chunk format names
  `br-llm-messages/3`.

## 0.2.0

### Added

- `ReasonCode::TitleEmpty` (`TITLE_EMPTY`): a title of no character.
- `ThreadTitleError` (`Empty`, `TooLong(TitleTooLong)`), displayed as its
  reason code, with `reason_code()`; `ThreadTitle::MIN_CHARS` (1).

### Changed

- **Breaking:** `ThreadTitle::parse` refuses an empty title and returns
  `Result<ThreadTitle, ThreadTitleError>` instead of
  `Result<ThreadTitle, TitleTooLong>`. `TitleTooLong` is unchanged and rides in
  `ThreadTitleError::TooLong`; a caller matching it moves to that variant (or to
  `reason_code()`). A caller matching `ReasonCode` exhaustively gains the
  `TitleEmpty` arm.
- The `thread.create` and `thread.set_title` documentation names `TITLE_EMPTY`
  among their refusals.

## 0.1.0

### Added

- The published language of Threads 0.1:
  - thread commands (`integration.cmd.threads.thread.{create, add_members,
    remove_members, add_resource_ids, remove_resource_ids, set_title, close,
    delete}.v1`) and message commands (`integration.cmd.threads.message.{store,
    complete, fail, cancel, delete}.v1`), with their coordinates and envelope
    `command_type`;
  - the one answer per command: `integration.evt.threads.{thread|message}.
    {confirmed|refused}.v1`, a refusal carrying a `ReasonCode` and structured
    `params`;
  - the `threads.members.{thread_id}` KV value (`PublishedThreadMembers`), its
    prefix, manifest key and wire version;
  - the content model: blocks (`Block`: the library's JSON for the
    `br-llm-messages/2` kinds, typed content for Threads' `ARTIFACT`,
    `THREAD_REFERENCE`, `JOB_REFERENCE`, `CITATION`, `ERROR`, `WARNING`) and
    items (`STEP`, `TOOL_RESULTS`);
  - the message stream: `Chunk` (`STEP_EVENT`, `TOOL_RESULTS`, `BLOCK`, each
    naming its item), the lane-A `ChunkFrame`, its subject and `Nats-Msg-Id`,
    and `seal_hash`, the digest a seal declares (keys ordered as the engine
    reads a chunk back);
  - `TitleJobConfig`, the configuration of the title Job and the names of the
    runner operations;
  - value objects: `ThreadTitle` (at most 200 characters), `SealDigest`;
  - the reason codes `RESOURCE_KEY_INVALID` (proposed: a blank resource key) and
    `ID_NOT_UUIDV7` (proposed: a caller-minted thread, message or block id that
    is not a UUIDv7);
  - `message.store` names its refusals: `MESSAGE_NOT_FOUND` for a parent nobody
    stored, `MESSAGE_INVALID` for a parent of another thread;
    statuses and kinds with a total wire mapping;
  - `br-llm-messages` v0.3.0 re-exported as `contract_threads::llm`.
