# Changelog

All notable changes to `svc-threads` (the service, its image and its chart) are
documented in this file. The published language has its own changelog,
[`crates/contract-threads/CHANGELOG.md`](crates/contract-threads/CHANGELOG.md).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z`: CD publishes a version only once its
heading exists.

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
