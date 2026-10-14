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
/// Not named by the registry (it describes the refusal): the scaffold's, to
/// ratify.
pub const RUNNER_SCOPE_REQUIRED: &str = "RUNNER_SCOPE_REQUIRED";

// ── GraphQL documents ───────────────────────────────────────────────────────

pub const Q_THREADS: &str = "query($size: Int!, $before: UUID) { \
    threadsThreads(size: $size, before: $before) { \
        id ownerId title status lastActivityAt version readMark affordances \
        members { userId displayName inRoster } resources { key ids } } }";

pub const Q_THREAD: &str = "query($id: UUID!) { threadsThread(id: $id) { \
    id ownerId title parentThreadId originBlockId status createdAt lastActivityAt version \
    readMark affordances members { userId displayName inRoster } resources { key ids } } }";

pub const Q_MESSAGES: &str = "query($thread: UUID!, $size: Int!, $before: UUID, $tip: UUID) { \
    threadsMessages(threadId: $thread, size: $size, before: $before, branchTip: $tip) { \
        id threadId parentMessageId jobId status deleted createdAt version affordances \
        author { kind userId aiId name } \
        blocks { id index kind content } \
        items { index kind status stopReason usage model blocks { id index kind content } } } }";

pub const Q_CONTEXT: &str = "query($thread: UUID!) { threadsContext(threadId: $thread) { \
    threadId conversation members { userId displayName } resources { key ids } } }";

pub const S_THREADS: &str = "subscription($size: Int!) { threadsThreadsDeltas(size: $size) { \
    __typename \
    ... on ThreadsReset { revision views { ... on ThreadView { id title version } } } \
    ... on ThreadsUpsert { revision view { ... on ThreadView { id title version } } } \
    ... on ThreadsRemove { revision key } } }";

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
