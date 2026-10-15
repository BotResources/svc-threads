# Changelog

All notable changes to `contract-threads`, the published language of
`svc-threads`, are documented in this file. The crate is released by its tag
`contract-threads/v{version}` alone; consumers depend on it by git tag.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z`: CD tags a version only once its heading
exists.

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
