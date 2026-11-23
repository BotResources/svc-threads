//! The vocabulary of the oracle, in one place: the binary's environment contract
//! (the engine's ops contract plus the object-storage group), the scope Threads
//! declares, the reason codes, the GraphQL documents the scenarios send, and the
//! timeouts.
//!
//! Every root, argument and field below is the SDL the suite reads: the last
//! implemented SDL the registry carries (0.11.0), with the 0.12.0 changes
//! `tests/ORACLE_CONTRACT.md` names — no `parentMessageId`, no `branchTip`,
//! and the runner's cut of a context (`context_cut_arg!`). A rename is a
//! naming alignment in this one file, never a change of what is asserted.
//! `affordances` is the engine's JSON scalar `{ action: { allowed, reason? } }`.

use std::time::Duration;

// ── the binary's environment contract ───────────────────────────────────────

pub const ENV_DATABASE_URL: &str = "DATABASE_URL";
pub const ENV_DATABASE_URL_OWNER: &str = "DATABASE_URL_OWNER";
pub const ENV_APP_ROLE: &str = "APP_ROLE";
pub const ENV_NATS_URL: &str = "NATS_URL";
pub const ENV_ENGINE_CHANNEL: &str = "ENGINE_CHANNEL";
pub const ENV_HOSTNAME: &str = "HOSTNAME";
pub const ENV_PORT: &str = "PORT";
pub const ENV_HOST: &str = "HOST";
pub const ENV_TRUSTED_NETWORK_HOSTS: &str = "TRUSTED_NETWORK_HOSTS";
pub const ENV_RUST_LOG: &str = "RUST_LOG";
/// The engine's beat — the relays' and the offer drain's cadence among others.
pub const ENV_ENGINE_BEAT_MS: &str = "ENGINE_BEAT_MS";
pub const ENV_S3_ENDPOINT: &str = "S3_ENDPOINT";
pub const ENV_S3_BUCKET: &str = "S3_BUCKET";
pub const ENV_S3_ACCESS_KEY: &str = "S3_ACCESS_KEY";
pub const ENV_S3_SECRET_KEY: &str = "S3_SECRET_KEY";
pub const ENV_S3_REGION: &str = "S3_REGION";

// ── Threads' own configuration (proposed by the oracle) ────────────────────
//
// The specification fixes two values the suite cannot wait out: a writer
// silent for 10 minutes has its message failed, and a PENDING artifact is
// removed after the upload window. The 0.1.0 scaffold hard-coded both
// (`STREAM_IDLE_WINDOW`, `UPLOAD_WINDOW`), so the oracle proposes these two
// keys, read at `serve`, defaulting to the specified values. Real
// configuration, like `ENGINE_BEAT_MS` — never a test seam.

/// Milliseconds a writer may stay silent before its `STREAMING` message is
/// failed (specified: 10 minutes).
pub const ENV_STREAM_IDLE_MS: &str = "THREADS_STREAM_IDLE_MS";
/// Milliseconds a `PENDING` artifact waits for its message before it is
/// removed.
pub const ENV_UPLOAD_WINDOW_MS: &str = "THREADS_UPLOAD_WINDOW_MS";

pub const MIGRATE_ARG: &str = "migrate";
pub const SERVE_ARG: &str = "serve";
pub const SCHEMA_ARG: &str = "schema";

pub const READYZ_PATH: &str = "/readyz";

/// The service key: the manifest key of its scope declaration, its root prefix
/// and the name of its stream.
pub const SERVICE: &str = "threads";

// ── scopes (registry permissions § Scopes) ──────────────────────────────────

pub const SCOPE_RUNNER: &str = "threads:runner";
pub const DECLARED_SCOPES: [&str; 1] = [SCOPE_RUNNER];

// ── reason codes ────────────────────────────────────────────────────────────

/// Named by the registry.
pub const ACTIVE_USER_REQUIRED: &str = "ACTIVE_USER_REQUIRED";
pub const THREAD_NOT_FOUND: &str = "THREAD_NOT_FOUND";
/// A page size out of its bounds (from 1), as the engine refuses it.
pub const WINDOW_SIZE_INVALID: &str = "WINDOW_SIZE_INVALID";
pub const MESSAGE_NOT_FOUND: &str = "MESSAGE_NOT_FOUND";
pub const KEY_REUSED: &str = "KEY_REUSED";
pub const TITLE_EMPTY: &str = "TITLE_EMPTY";
pub const TITLE_TOO_LONG: &str = "TITLE_TOO_LONG";
/// Not named by the registry (it describes the refusal): the scaffold's, to
/// ratify.
pub const RUNNER_SCOPE_REQUIRED: &str = "RUNNER_SCOPE_REQUIRED";
/// Named by the registry: an agent context named by an agent id that is not a
/// UUIDv7.
pub const AI_PERSPECTIVE_INVALID: &str = "AI_PERSPECTIVE_INVALID";
/// Named by the registry (0.12.0): a `message.store` v1 or v2, which named a
/// parent, refused in a `message.refused` answer.
pub const COMMAND_VERSION_RETIRED: &str = "COMMAND_VERSION_RETIRED";

/// The stream window a scenario that waits it out configures
/// ([`ENV_STREAM_IDLE_MS`]); every other scenario runs on the service's own
/// default, the specified 10 minutes.
pub const SHORT_STREAM_IDLE: Duration = Duration::from_secs(4);
/// The upload window a scenario that waits it out configures
/// ([`ENV_UPLOAD_WINDOW_MS`]); every other scenario runs on the default.
pub const SHORT_UPLOAD_WINDOW: Duration = Duration::from_secs(4);

// ── GraphQL documents ───────────────────────────────────────────────────────

pub const Q_THREADS: &str = "query($size: Int!, $before: UUID) { \
    threadsThreads(size: $size, before: $before) { \
        id ownerId title status lastActivityAt version readMark affordances \
        members { userId displayName inRoster } resources { key ids } } }";

pub const Q_THREAD: &str = "query($id: UUID!) { threadsThread(id: $id) { \
    id ownerId title parentThreadId originBlockId status resolvedBy resolvedAt createdAt \
    lastActivityAt version readMark affordances members { userId displayName inRoster } \
    resources { key ids } } }";

/// The children of a thread, each with every field `Q_THREAD` reads: one
/// child's view compares whole with the thread's.
pub const Q_CHILD_THREADS: &str = "query($thread: UUID!, $size: Int!, $after: UUID) { \
    threadsChildThreads(threadId: $thread, size: $size, after: $after) { \
    id ownerId title parentThreadId originBlockId status resolvedBy resolvedAt createdAt \
    lastActivityAt version readMark affordances members { userId displayName inRoster } \
    resources { key ids } } }";

/// One page of a thread's messages, the newest first; `before` names the
/// oldest message of the previous page (0.12.0: one list, no branch).
pub const Q_MESSAGES: &str = "query($thread: UUID!, $size: Int!, $before: UUID) { \
    threadsMessages(threadId: $thread, size: $size, before: $before) { \
        id threadId jobId status deleted createdAt editedAt version affordances \
        author { kind userId aiId name } turnKind \
        entries { id index kind state content annotations { id index kind content } } } }";

/// The context of a thread, whole: no agent named.
pub const Q_CONTEXT: &str = "query($thread: UUID!) { threadsContext(threadId: $thread) { \
    threadId conversation projectedThread members { userId displayName } resources { key ids } } }";

/// The context, whole (`agent` null) or as the agent context of the agent id
/// named.
pub const Q_CONTEXT_FOR: &str = "query($thread: UUID!, $agent: String) { \
    threadsContext(threadId: $thread, agentId: $agent) { \
    threadId conversation projectedThread members { userId displayName } resources { key ids } } }";

/// The argument of `threadsContext` by which a runner names a message, to
/// receive the thread's conversation up to that message included (0.12.0).
/// The registry fixes the behaviour, not the name: this is the oracle's
/// assumption, to align on the implementation's name here and only here.
macro_rules! context_cut_arg {
    () => {
        "upTo"
    };
}

/// The name of the cut argument, for the messages a scenario prints.
pub const CONTEXT_CUT_ARG: &str = context_cut_arg!();

/// The context of a thread, whole or for an agent, up to the message `upTo`
/// names — the whole thread with `upTo` null — with its attached artifacts
/// (0.12.0).
pub const Q_CONTEXT_UP_TO: &str = concat!(
    "query($thread: UUID!, $agent: String, $upTo: UUID) { ",
    "threadsContext(threadId: $thread, agentId: $agent, ",
    context_cut_arg!(),
    ": $upTo) { ",
    "threadId conversation projectedThread members { userId displayName } resources { key ids } ",
    "artifacts { id name mediaType size sha256 messageId } } }"
);

/// The attached artifacts a runner receives with a thread's context, whole or
/// for an agent, up to the message `upTo` names or whole, without asking for
/// any read access (0.9.0; the cut, 0.12.0).
pub const Q_CONTEXT_ARTIFACTS: &str = concat!(
    "query($thread: UUID!, $agent: String, $upTo: UUID) { ",
    "threadsContext(threadId: $thread, agentId: $agent, ",
    context_cut_arg!(),
    ": $upTo) { ",
    "threadId artifacts { id name mediaType size sha256 messageId } } }"
);

/// The same, asking for a read access to each (0.9.0; the cut, 0.12.0).
pub const Q_CONTEXT_ARTIFACT_ACCESS: &str = concat!(
    "query($thread: UUID!, $agent: String, $upTo: UUID) { ",
    "threadsContext(threadId: $thread, agentId: $agent, ",
    context_cut_arg!(),
    ": $upTo) { ",
    "threadId artifacts { id name mediaType size sha256 messageId downloadUrl } } }"
);

pub const Q_ARTIFACT_DOWNLOAD: &str = "query($thread: UUID!, $artifact: UUID!) { \
    threadsArtifactDownload(threadId: $thread, artifactId: $artifact) }";

pub const M_SET_READ_MARK: &str = "mutation($thread: UUID!, $message: UUID!) { \
    threadsSetReadMark(threadId: $thread, messageId: $message) { success } }";

pub const M_REQUEST_UPLOAD: &str = "mutation($thread: UUID!, $artifact: UUID!, $name: String!, \
    $mediaType: String!, $size: Int!, $sha256: String!) { \
    threadsRequestArtifactUpload(threadId: $thread, artifactId: $artifact, name: $name, \
    mediaType: $mediaType, size: $size, sha256: $sha256) { url fields } }";

/// A runner sets a thread's title (0.7.0: no Job named).
pub const M_SET_TITLE: &str = "mutation($thread: UUID!, $title: String!) { \
    threadsSetTitle(threadId: $thread, title: $title) { success } }";

pub const S_THREADS: &str = "subscription($size: Int!) { threadsThreadsDeltas(size: $size) { \
    __typename \
    ... on ThreadsReset { revision views { ... on ThreadView { id title version } } } \
    ... on ThreadsUpsert { revision view { ... on ThreadView { id title version } } } \
    ... on ThreadsRemove { revision key } } }";

/// The fields of a message the suite reads, on every channel.
pub const MESSAGE_FIELDS: &str = "id threadId jobId status deleted createdAt \
    editedAt version affordances author { kind userId aiId name } turnKind \
    entries { id index kind state content annotations { id index kind content } }";

/// The fields of a thread the suite reads on its live channels.
pub const THREAD_FIELDS: &str = "id ownerId title parentThreadId originBlockId status \
    resolvedBy resolvedAt lastActivityAt version readMark affordances members { userId displayName inRoster } \
    resources { key ids }";

// ── timeouts ────────────────────────────────────────────────────────────────

/// Boot to readiness, a migration, MinIO's start.
pub const READY_TIMEOUT: Duration = Duration::from_secs(60);
/// A change reaching its observer: a projection, a published entry, an answer.
pub const PUSH_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a scenario watches to prove nothing happens.
pub const QUIET_WINDOW: Duration = Duration::from_millis(1500);
/// The engine beat the suite runs the binary with: a real configuration value,
/// not a backdoor — only how soon a relay or the offer drain comes back.
pub const ENGINE_BEAT: Duration = Duration::from_millis(250);
