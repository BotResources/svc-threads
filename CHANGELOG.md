# Changelog

All notable changes to `svc-threads` (the service, its image and its chart) are
documented in this file. The published language has its own changelog,
[`crates/contract-threads/CHANGELOG.md`](crates/contract-threads/CHANGELOG.md).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z`: CD publishes a version only once its
heading exists.

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
