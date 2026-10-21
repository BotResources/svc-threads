//! The vocabulary of the oracle, in one place: the binary's environment contract
//! (the engine's ops contract plus the object-storage group), the scope Threads
//! declares, the reason codes, the GraphQL documents the scenarios send, and the
//! timeouts.
//!
//! Every root, argument and field below is the scaffold's SDL (`svc-threads
//! schema`): the registry carries no SDL for 0.1.0 yet. A rename is a naming
//! alignment in this one file, never a change of what is asserted.
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
// The specification fixes three values the suite cannot wait out or must name:
// a writer silent for 10 minutes has its message failed, a PENDING artifact is
// removed after the upload window, and the title Job runs on "the title runner
// type". The 0.1.0 scaffold hard-codes the first two (`STREAM_IDLE_WINDOW`,
// `UPLOAD_WINDOW`) and names no runner type, so the oracle proposes these three
// keys, read at `serve`, defaulting to the specified values. Real
// configuration, like `ENGINE_BEAT_MS` — never a test seam.

/// Milliseconds a writer may stay silent before its `STREAMING` message is
/// failed (specified: 10 minutes).
pub const ENV_STREAM_IDLE_MS: &str = "THREADS_STREAM_IDLE_MS";
/// Milliseconds a `PENDING` artifact waits for its message before it is
/// removed.
pub const ENV_UPLOAD_WINDOW_MS: &str = "THREADS_UPLOAD_WINDOW_MS";
/// The runner type of the title Job.
pub const ENV_TITLE_RUNNER_TYPE: &str = "THREADS_TITLE_RUNNER_TYPE";

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
pub const MESSAGE_NOT_FOUND: &str = "MESSAGE_NOT_FOUND";
pub const KEY_REUSED: &str = "KEY_REUSED";
pub const TITLE_EMPTY: &str = "TITLE_EMPTY";
pub const TITLE_TOO_LONG: &str = "TITLE_TOO_LONG";
/// Not named by the registry (it describes the refusal): the scaffold's, to
/// ratify.
pub const RUNNER_SCOPE_REQUIRED: &str = "RUNNER_SCOPE_REQUIRED";
/// Not named by the functional specification ("accepted only for that
/// thread's active title Job"): the draft registry tests' (T12), to ratify.
pub const JOB_NOT_ACTIVE: &str = "JOB_NOT_ACTIVE";

/// The title runner type this suite configures ([`ENV_TITLE_RUNNER_TYPE`]).
pub const TITLE_RUNNER_TYPE: &str = "thread-title-e2e";
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

pub const Q_MESSAGES: &str = "query($thread: UUID!, $size: Int!, $before: UUID, $tip: UUID) { \
    threadsMessages(threadId: $thread, size: $size, before: $before, branchTip: $tip) { \
        id threadId parentMessageId jobId status deleted createdAt editedAt version affordances \
        author { kind userId aiId name } \
        blocks { id index kind content } \
        items { index kind status stopReason usage model blocks { id index kind content } } } }";

pub const Q_CONTEXT: &str = "query($thread: UUID!) { threadsContext(threadId: $thread) { \
    threadId conversation members { userId displayName } resources { key ids } } }";

/// The context, whole (`ai` null) or lightened for the AI named.
pub const Q_CONTEXT_FOR: &str = "query($thread: UUID!, $ai: AiPerspective) { \
    threadsContext(threadId: $thread, ai: $ai) { \
    threadId conversation members { userId displayName } resources { key ids } } }";

pub const Q_ARTIFACT_DOWNLOAD: &str = "query($thread: UUID!, $artifact: UUID!) { \
    threadsArtifactDownload(threadId: $thread, artifactId: $artifact) }";

pub const M_SET_READ_MARK: &str = "mutation($thread: UUID!, $message: UUID!) { \
    threadsSetReadMark(threadId: $thread, messageId: $message) { success } }";

pub const M_REQUEST_UPLOAD: &str = "mutation($thread: UUID!, $artifact: UUID!, $name: String!, \
    $mediaType: String!, $size: Int!, $sha256: String!) { \
    threadsRequestArtifactUpload(threadId: $thread, artifactId: $artifact, name: $name, \
    mediaType: $mediaType, size: $size, sha256: $sha256) { url fields } }";

pub const M_SET_TITLE: &str = "mutation($thread: UUID!, $job: UUID!, $title: String!) { \
    threadsSetTitle(threadId: $thread, jobId: $job, title: $title) { success } }";

pub const M_FAIL_TITLE_JOB: &str = "mutation($thread: UUID!, $job: UUID!, $reason: String!, \
    $message: String) { threadsFailTitleJob(threadId: $thread, jobId: $job, \
    reasonCode: $reason, message: $message) { success } }";

pub const S_THREADS: &str = "subscription($size: Int!) { threadsThreadsDeltas(size: $size) { \
    __typename \
    ... on ThreadsReset { revision views { ... on ThreadView { id title version } } } \
    ... on ThreadsUpsert { revision view { ... on ThreadView { id title version } } } \
    ... on ThreadsRemove { revision key } } }";

/// The fields of a message the suite reads, on every channel.
pub const MESSAGE_FIELDS: &str = "id threadId parentMessageId jobId status deleted createdAt \
    editedAt version affordances author { kind userId aiId name } \
    blocks { id index kind content } \
    items { index kind status stopReason usage model blocks { id index kind content } }";

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
