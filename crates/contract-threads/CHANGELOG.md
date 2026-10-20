# Changelog

All notable changes to `contract-threads`, the published language of
`svc-threads`, are documented in this file. The crate is released by its tag
`contract-threads/v{version}` alone; consumers depend on it by git tag.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z`: CD tags a version only once its heading
exists.

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
