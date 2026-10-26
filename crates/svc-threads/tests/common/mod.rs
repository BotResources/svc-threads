//! The e2e oracle harness for `svc-threads` — black-box, real Postgres, real
//! NATS JetStream + KV, a real MinIO, the real spawned binary, no mocks
//! anywhere.
//!
//! A [`TestContext`] provisions the declared infra (an ephemeral database with
//! the CNPG-shaped owner **and** the runtime role; the two fixed Fabric streams
//! with a bounded retention and the `STREAMING_threads` stream; the
//! `PUBLISHED_LANGUAGE` bucket; a MinIO bucket), runs `svc-threads migrate` as
//! the owner, spawns `svc-threads serve` on the app role, plays Identity's side
//! of the scope declaration and waits for readiness. It then exposes the four
//! observation channels every scenario asserts across:
//!
//! 1. **mutation ack / refusal** and 2. **query + affordances** — [`TestContext::run`]
//!    and [`gql`];
//! 3. **subscriptions** — [`feed`], over `br_test_harness::{SseSubscription, WsSubscription}`
//!    on [`TestContext::base_url`]: SSE (the gateway's leg) and
//!    `graphql-transport-ws` (the direct leg), both declared by the engine's ops
//!    contract; every stream opens on a `Reset`;
//! 4. **the integration side** — the commands a commanding service sends and
//!    their one answer each ([`commands`]), the message stream a writer pushes
//!    ([`stream`]), Identity's roster and scope handshake ([`identity`]), the
//!    `threads.members.{thread_id}` entries Threads publishes
//!    ([`TestContext::published_members`]), Jobs' side of the title Job
//!    ([`jobs`]), the object store ([`artifacts`]) and the fact log
//!    `threads.fact` ([`facts`], read with `SELECT` on the app role).
//!
//! [`content`] builds the conversation a scenario stores, through the
//! contract's and the library's own types; [`flows`] are the gestures every
//! scenario repeats (a thread created and confirmed, a message stored and
//! confirmed), each asserting its one answer.
//!
//! The deployment is the unit, not the process: a context can stop its pod and
//! boot it again on the same database and bus ([`TestContext::stop`] /
//! [`TestContext::start_again`]), and start further pods of the same deployment
//! beside it ([`TestContext::spawn_pod`]) — the service is declared multi-pod.
//!
//! State is built **only** through the service's own surfaces and the published
//! language it consumes: the suite never writes a row of the service. Its SQL
//! is on the app role, and `SELECT` — with one exception, the engine's own
//! housekeeping: [`TestContext::expire_answers`] removes the stored answers of
//! commands exactly as the engine's retention sweep does, which no scenario
//! could otherwise wait for (the sweep keeps a claim at least as long as the
//! command stream retains its messages).

#![allow(dead_code)] // each scenario file uses an overlapping, not identical, subset.

pub mod artifacts;
pub mod catalog;
pub mod commands;
pub mod constants;
pub mod content;
pub mod facts;
pub mod feed;
pub mod flows;
pub mod gql;
pub mod identity;
pub mod infra;
pub mod jobs;
pub mod port;
pub mod stream;

use std::future::Future;
use std::time::Duration;

use br_core_auth::{AuthMethod, Passport, PassportClaims, SCOPES_CLAIM_KEY};
use br_test_harness::{
    E2eDatabase, FabricTestNats, GraphqlClient, SpawnedProcess, run_once, wait_until,
};
use br_util_nats_fabric::KvKey;
use contract_threads::PublishedThreadMembers;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

pub use constants::*;
pub use port::free_port;

pub const APP_ROLE: &str = "threads_app";
pub const APP_PASSWORD: &str = "threads_app_e2e_pw";

/// How the scenario plays Identity's side of the scope handshake at boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handshake {
    /// Accept the declaration and wait for readiness — every functional scenario.
    Accept,
    /// Leave it unanswered: the scenario plays the verdict itself.
    Withhold,
}

/// The windows a scenario shortens to wait them out; `None` leaves the
/// service on its specified default.
#[derive(Debug, Clone, Copy, Default)]
pub struct Windows {
    pub stream_idle: Option<Duration>,
    pub upload: Option<Duration>,
}

/// A person of the scenario: the roster entry Identity published for them, and
/// the Passport they call with.
#[derive(Clone)]
pub struct Person {
    pub id: Uuid,
    pub label: String,
    pub passport: Passport,
}

/// A service account: a runner (holding `threads:runner`) or not.
#[derive(Clone)]
pub struct ServiceAccount {
    pub id: Uuid,
    pub passport: Passport,
}

/// The Passport the gateway resolves for a person, super user or not. Threads
/// reads no scope of a person.
pub fn person_passport(user: Uuid, super_user: bool) -> Passport {
    Passport::human(
        user,
        super_user,
        true,
        AuthMethod::Jwt,
        None,
        PassportClaims::new(),
    )
}

/// The Passport of a service account holding `scopes`.
pub fn service_passport(account: Uuid, scopes: &[&str]) -> Passport {
    let mut claims = serde_json::Map::new();
    claims.insert(SCOPES_CLAIM_KEY.to_string(), serde_json::json!(scopes));
    Passport::service(account, PassportClaims::from_map(claims))
}

pub struct TestContext {
    pub base_url: String,
    pub gql_client: GraphqlClient,
    pub nats: FabricTestNats,
    pub scopes: identity::ScopeHandshake,
    /// Every answer Threads publishes, captured from before the first command.
    pub answers: commands::Answers,
    /// Jobs' side of the title Job: every command Threads sends Jobs, captured
    /// from before the boot, and the facts the scenario answers with.
    pub jobs: jobs::Jobs,
    /// The app role's pool: `SELECT`, and the engine's claim sweep
    /// ([`TestContext::expire_answers`]).
    pub pool: PgPool,
    pub minio: infra::Minio,
    db: E2eDatabase,
    service: Option<SpawnedProcess>,
    /// The further pods [`TestContext::spawn_pod`] started beside the first.
    pods: Vec<SpawnedProcess>,
    /// How many processes this context spawned: each one's `HOSTNAME`.
    pod: u32,
    windows: Windows,
    /// Every command a scenario sent through [`flows::command`], and those it
    /// delivered again on purpose: at teardown, each sent command was answered
    /// exactly once, each redelivered one alike every time.
    pub(crate) sent: std::sync::Mutex<Vec<Uuid>>,
    pub(crate) redelivered: std::sync::Mutex<Vec<Uuid>>,
}

/// Another pod of the same deployment: its own process and port, the same
/// database, bus, bucket and `ENGINE_CHANNEL` as the first.
pub struct Pod {
    pub base_url: String,
    pub gql_client: GraphqlClient,
}

impl Pod {
    pub async fn run(&self, passport: &Passport, document: &str, variables: Value) -> Value {
        self.gql_client.query(passport, document, variables).await
    }
}

impl TestContext {
    /// The nominal boot: migrated, declared, accepted, ready.
    pub async fn setup() -> Self {
        Self::boot(Handshake::Accept).await
    }

    /// The nominal boot, with the windows a scenario waits out shortened.
    pub async fn setup_with(windows: Windows) -> Self {
        Self::boot_with(Handshake::Accept, windows).await
    }

    pub async fn boot(handshake: Handshake) -> Self {
        Self::boot_with(handshake, Windows::default()).await
    }

    pub async fn boot_with(handshake: Handshake, windows: Windows) -> Self {
        let _ = dotenvy::dotenv();

        let suffix = Uuid::now_v7().simple().to_string();
        let db = catalog::serialized(E2eDatabase::create_with_app_role(
            &format!("threads_owner_{suffix}"),
            &format!("threads_e2e_{suffix}"),
            APP_ROLE,
            APP_PASSWORD,
            true,
            &[],
        ))
        .await;

        let nats = FabricTestNats::start()
            .await
            .with_published_language()
            .await;
        infra::declare_streams(&nats.url()).await;

        // Bound before the binary exists: the captures replay from the stream's
        // start, so nothing the boot publishes escapes them.
        let scopes = identity::ScopeHandshake::bind(&nats).await;
        let answers = commands::Answers::bind(&nats).await;
        let jobs = jobs::Jobs::bind(&nats).await;

        let minio = infra::Minio::start().await;

        let migrated = run_once(
            env!("CARGO_BIN_EXE_svc-threads"),
            &[MIGRATE_ARG],
            &[
                (ENV_DATABASE_URL_OWNER, db.owner_migration_url().as_str()),
                (ENV_APP_ROLE, APP_ROLE),
                (ENV_TRUSTED_NETWORK_HOSTS, "127.0.0.1,localhost"),
            ],
            READY_TIMEOUT,
        )
        .await
        .expect("svc-threads migrate runs to completion");
        assert!(
            migrated.status.success(),
            "svc-threads migrate failed:\n{}\n{}",
            String::from_utf8_lossy(&migrated.stdout),
            String::from_utf8_lossy(&migrated.stderr)
        );

        let pool = PgPool::connect(&db.app_url())
            .await
            .expect("connect the app role's observation pool");

        let mut ctx = Self {
            base_url: String::new(),
            gql_client: GraphqlClient::new("http://127.0.0.1:1"),
            nats,
            scopes,
            answers,
            jobs,
            pool,
            minio,
            db,
            service: None,
            pods: Vec::new(),
            pod: 0,
            windows,
            sent: std::sync::Mutex::new(Vec::new()),
            redelivered: std::sync::Mutex::new(Vec::new()),
        };
        ctx.spawn(handshake).await;
        ctx
    }

    fn service_env(&self, port: u16) -> Vec<(&'static str, String)> {
        let mut env = vec![
            (ENV_DATABASE_URL, self.db.app_url()),
            (ENV_APP_ROLE, APP_ROLE.to_string()),
            (ENV_NATS_URL, self.nats.url()),
            (ENV_ENGINE_CHANNEL, SERVICE.to_string()),
            (ENV_HOSTNAME, format!("threads-e2e-{}", self.pod)),
            (ENV_PORT, port.to_string()),
            (ENV_HOST, "127.0.0.1".to_string()),
            (ENV_TRUSTED_NETWORK_HOSTS, "127.0.0.1,localhost".to_string()),
            (ENV_RUST_LOG, "info".to_string()),
            (ENV_ENGINE_BEAT_MS, ENGINE_BEAT.as_millis().to_string()),
            (ENV_S3_ENDPOINT, self.minio.endpoint()),
            (ENV_S3_BUCKET, self.minio.bucket.clone()),
            (ENV_S3_ACCESS_KEY, self.minio.access_key().to_string()),
            (ENV_S3_SECRET_KEY, self.minio.secret_key().to_string()),
            (ENV_S3_REGION, infra::S3_REGION.to_string()),
            (ENV_TITLE_RUNNER_TYPE, TITLE_RUNNER_TYPE.to_string()),
        ];
        if let Some(idle) = self.windows.stream_idle {
            env.push((ENV_STREAM_IDLE_MS, idle.as_millis().to_string()));
        }
        if let Some(upload) = self.windows.upload {
            env.push((ENV_UPLOAD_WINDOW_MS, upload.as_millis().to_string()));
        }
        env
    }

    /// Spawn `serve` against this context's infra as its first pod; with
    /// [`Handshake::Accept`], answer the declaration this boot publishes and wait
    /// for readiness.
    async fn spawn(&mut self, handshake: Handshake) {
        let (service, base_url) = self.launch(handshake).await;
        self.gql_client = GraphqlClient::new(&base_url);
        self.base_url = base_url;
        self.service = Some(service);
    }

    /// A further pod of the same deployment — same database, bus and bucket,
    /// same `ENGINE_CHANNEL`, its own `HOSTNAME` and port — declared, accepted
    /// and ready.
    pub async fn spawn_pod(&mut self) -> Pod {
        let (service, base_url) = self.launch(Handshake::Accept).await;
        self.pods.push(service);
        Pod {
            gql_client: GraphqlClient::new(&base_url),
            base_url,
        }
    }

    /// A port is only reserved until released: a boot that exits because another
    /// process took it is retried on a fresh one.
    async fn launch(&mut self, handshake: Handshake) -> (SpawnedProcess, String) {
        for _ in 0..3 {
            if let Some(launched) = self.try_launch(handshake).await {
                return launched;
            }
        }
        panic!("svc-threads never bound a free port in three attempts");
    }

    async fn try_launch(&mut self, handshake: Handshake) -> Option<(SpawnedProcess, String)> {
        self.pod += 1;
        let port = free_port();
        let base_url = format!("http://127.0.0.1:{port}");
        let declared_before = self.scopes.count();
        let env = self.service_env(port);
        let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let mut service =
            SpawnedProcess::spawn(env!("CARGO_BIN_EXE_svc-threads"), &[SERVE_ARG], &env);

        let port_taken =
            |service: &SpawnedProcess| service.logs().contains("Address already in use");
        // The declaration, or the process leaving because its port was taken
        // meanwhile — whichever comes first.
        let settled = wait_until(READY_TIMEOUT, || async {
            self.scopes.count() > declared_before || port_taken(&service)
        })
        .await;
        assert!(
            settled,
            "svc-threads neither declared its scope nor exited:\n{}",
            service.logs()
        );
        if port_taken(&service) {
            service.shutdown().await;
            return None;
        }
        if handshake == Handshake::Accept {
            let correlation = self
                .scopes
                .await_declare(declared_before + 1, "the boot's scope declaration")
                .await;
            self.scopes.accept(&self.nats, correlation).await;
            let boot = service
                .await_boot(&format!("{base_url}{READYZ_PATH}"), READY_TIMEOUT)
                .await;
            if !boot.is_ready() && port_taken(&service) {
                service.shutdown().await;
                return None;
            }
            assert!(
                boot.is_ready(),
                "svc-threads did not reach {READYZ_PATH}: {:?}\n{}",
                boot.exit_status(),
                service.logs()
            );
        }
        Some((service, base_url))
    }

    /// Genuinely stop the first pod: the process is killed and reaped.
    pub async fn stop(&mut self) {
        if let Some(service) = self.service.take() {
            service.shutdown().await;
        }
    }

    /// Boot a fresh first pod against the SAME database, bus and bucket.
    pub async fn start_again(&mut self, handshake: Handshake) {
        assert!(self.service.is_none(), "stop the running instance first");
        self.spawn(handshake).await;
    }

    pub async fn teardown(mut self) {
        self.assert_each_command_answered_once().await;
        self.stop().await;
        for pod in self.pods.drain(..) {
            pod.shutdown().await;
        }
        self.pool.close().await;
        self.scopes.stop().await;
        self.answers.stop().await;
        self.jobs.stop().await;
        self.minio.shutdown().await;
        self.nats.shutdown().await;
        catalog::serialized(self.db.cleanup()).await;
    }

    /// The first pod's log, then every further pod's, each under its own header.
    pub fn logs(&self) -> String {
        let mut logs = self
            .service
            .as_ref()
            .map(SpawnedProcess::logs)
            .unwrap_or_default();
        for (n, pod) in self.pods.iter().enumerate() {
            logs.push_str(&format!(
                "\n──── further pod {} ────\n{}",
                n + 1,
                pod.logs()
            ));
        }
        logs
    }

    /// Every command sent through [`flows::command`] was answered exactly
    /// once — two pods consuming one command would answer it twice — and every
    /// command delivered again was answered alike each time.
    async fn assert_each_command_answered_once(&self) {
        tokio::time::sleep(QUIET_WINDOW).await;
        let redelivered = self.redelivered.lock().expect("the ledger").clone();
        for command in self.sent.lock().expect("the ledger").iter() {
            let answers = self.answers.for_command(*command);
            if redelivered.contains(command) {
                assert!(
                    answers.windows(2).all(|pair| pair[0] == pair[1]),
                    "a redelivered command is answered alike every time: {answers:?}"
                );
            } else {
                assert_eq!(
                    answers.len(),
                    1,
                    "command {command} is answered exactly once: {answers:?}"
                );
            }
        }
    }

    /// The stored answers of `commands` expire: their idempotency claims are
    /// deleted with the statement of the engine's retention sweep
    /// (`HostedOutboxRelay` → `sweep_claims`, `DELETE FROM
    /// service_engine.message_claim`), on the role it runs on — the service's
    /// runtime pool, the app role — narrowed to the commands named instead of
    /// to claims older than the retention. A command received again afterwards
    /// is judged anew.
    pub async fn expire_answers(&self, commands: &[Uuid]) {
        let swept =
            sqlx::query("DELETE FROM service_engine.message_claim WHERE message_id = ANY($1)")
                .bind(commands)
                .execute(&self.pool)
                .await
                .expect("sweep the commands' claims, as the app role");
        assert!(
            swept.rows_affected() >= commands.len() as u64,
            "every command named had a stored answer to expire"
        );
    }

    /// The duplicate window of `INTEGRATION_EVT` shortened to `window`: an
    /// answer the engine publishes again under its first event id is then
    /// stored again by the broker once the window has passed, and a capture
    /// sees it — within the window the broker drops it as a duplicate.
    pub async fn shorten_event_duplicate_window(&self, window: Duration) {
        infra::nats_cli(
            &self.nats.url(),
            &[
                "stream",
                "edit",
                br_util_nats_fabric::INTEGRATION_EVT,
                "--dupe-window",
                &format!("{}s", window.as_secs().max(1)),
                "--force",
            ],
        )
        .await;
    }

    /// The first pod's readiness endpoint, for the conformance battery.
    pub fn readyz_url(&self) -> String {
        format!("{}{READYZ_PATH}", self.base_url)
    }

    /// The readiness verdict as the kubelet's probe reads it: status and body,
    /// or `(0, why)` when nothing answers.
    pub async fn readyz(&self) -> (u16, String) {
        match reqwest::Client::new()
            .get(format!("{}{READYZ_PATH}", self.base_url))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status().as_u16();
                (status, response.text().await.unwrap_or_default())
            }
            Err(e) => (0, format!("unreachable: {e}")),
        }
    }

    // ── channels 1 + 2 ──────────────────────────────────────────────────────

    pub async fn run(&self, passport: &Passport, document: &str, variables: Value) -> Value {
        self.gql_client.query(passport, document, variables).await
    }

    /// The same document with no Passport at all: `(status, body)`.
    pub async fn run_anonymous(&self, document: &str, variables: Value) -> (u16, Value) {
        let (status, body) = self
            .gql_client
            .query_unauthenticated(document, variables)
            .await;
        (status.as_u16(), body)
    }

    // ── the people (channel 4: Identity's roster) ───────────────────────────

    /// Identity publishes a new active user and Threads' copy holds them.
    pub async fn enrol(&self, label: &str) -> Person {
        let id = Uuid::now_v7();
        identity::publish_user(&self.nats, id, &identity::roster_entry(label, None)).await;
        self.await_known_user(id, label).await;
        Person {
            id,
            label: label.to_string(),
            passport: person_passport(id, false),
        }
    }

    /// Identity publishes a new active user whose Passport carries super-user
    /// status — which gives nothing in Threads.
    pub async fn enrol_super_user(&self, label: &str) -> Person {
        let person = self.enrol(label).await;
        Person {
            passport: person_passport(person.id, true),
            ..person
        }
    }

    /// Identity republishes `person` under `new_label`'s display name; waits
    /// until Threads' copy holds it.
    pub async fn rename(&self, person: &Person, new_label: &str) {
        identity::publish_user(
            &self.nats,
            person.id,
            &identity::roster_entry(new_label, None),
        )
        .await;
        self.await_known_user(person.id, new_label).await;
    }

    /// Identity deactivates `person` (their entry carries `disabled_at`); waits
    /// until Threads' copy holds it.
    pub async fn deactivate(&self, person: &Person) {
        identity::publish_user(
            &self.nats,
            person.id,
            &identity::roster_entry(&person.label, Some(chrono::Utc::now())),
        )
        .await;
        eventually(
            "the roster copy holds the deactivation",
            PUSH_TIMEOUT,
            || async {
                let disabled: Option<bool> = sqlx::query_scalar(
                    "SELECT disabled_at IS NOT NULL FROM threads.known_users WHERE user_id = $1",
                )
                .bind(person.id)
                .fetch_optional(&self.pool)
                .await
                .expect("read the roster copy");
                disabled == Some(true)
            },
        )
        .await;
    }

    /// Identity republishes `person` active, under their own name.
    pub async fn reactivate(&self, person: &Person) {
        identity::publish_user(
            &self.nats,
            person.id,
            &identity::roster_entry(&person.label, None),
        )
        .await;
        eventually(
            "the roster copy holds the reactivation",
            PUSH_TIMEOUT,
            || async {
                let active: Option<bool> = sqlx::query_scalar(
                    "SELECT disabled_at IS NULL AND in_roster FROM threads.known_users \
                 WHERE user_id = $1",
                )
                .bind(person.id)
                .fetch_optional(&self.pool)
                .await
                .expect("read the roster copy");
                active == Some(true)
            },
        )
        .await;
    }

    /// Identity retracts `person` from the roster; waits until Threads' copy
    /// keeps them, out of the roster.
    pub async fn retract(&self, person: &Person) {
        identity::retract_user(&self.nats, person.id).await;
        self.await_out_of_roster(person.id).await;
    }

    /// Waits until Threads' copy holds `user` out of the roster, name kept.
    pub async fn await_out_of_roster(&self, user: Uuid) {
        eventually(
            "the roster copy keeps the user, out of the roster",
            PUSH_TIMEOUT,
            || async {
                let in_roster: Option<bool> = sqlx::query_scalar(
                    "SELECT in_roster FROM threads.known_users WHERE user_id = $1",
                )
                .bind(user)
                .fetch_optional(&self.pool)
                .await
                .expect("read the roster copy");
                in_roster == Some(false)
            },
        )
        .await;
    }

    /// Waits until Threads' roster copy holds `user` in the roster under the
    /// name Identity published for `label`.
    pub async fn await_known_user(&self, user: Uuid, label: &str) {
        let expected = identity::display_name(label);
        eventually("the roster copy holds the user", PUSH_TIMEOUT, || async {
            let row: Option<(String, bool)> = sqlx::query_as(
                "SELECT display_name, in_roster FROM threads.known_users WHERE user_id = $1",
            )
            .bind(user)
            .fetch_optional(&self.pool)
            .await
            .expect("read the roster copy");
            row == Some((expected.clone(), true))
        })
        .await;
    }

    /// A service account, holding `threads:runner` or not.
    pub fn service_account(&self, runner: bool) -> ServiceAccount {
        let id = Uuid::now_v7();
        let scopes: &[&str] = if runner { &[SCOPE_RUNNER] } else { &[] };
        ServiceAccount {
            id,
            passport: service_passport(id, scopes),
        }
    }

    // ── channel 4: the members Threads publishes ────────────────────────────

    /// The `threads.members.{thread_id}` entry, if published.
    pub async fn published_members(&self, thread: Uuid) -> Option<PublishedThreadMembers> {
        let key = KvKey::new(contract_threads::members::members_key(thread))
            .expect("a members key is a valid KV key");
        self.nats
            .pl_reader::<PublishedThreadMembers>()
            .await
            .get(&key)
            .await
            .expect("read the published language")
    }

    /// The `threads.members.{thread_id}` entry and its revision, if published:
    /// an unchanged revision proves a no-op rewrote nothing.
    pub async fn members_entry(
        &self,
        thread: Uuid,
    ) -> Option<(PublishedThreadMembers, br_util_nats_fabric::Revision)> {
        let key = KvKey::new(contract_threads::members::members_key(thread))
            .expect("a members key is a valid KV key");
        self.nats
            .pl_reader::<PublishedThreadMembers>()
            .await
            .get_with_revision(&key)
            .await
            .expect("read the published language")
    }

    /// The raw manifest of the members offer, if published.
    pub async fn members_manifest(&self) -> Option<Value> {
        let key = KvKey::new(contract_threads::MEMBERS_MANIFEST_KEY)
            .expect("the manifest key is a valid KV key");
        self.nats
            .pl_get_raw(&key)
            .await
            .map(|bytes| serde_json::from_slice(&bytes).expect("a JSON manifest"))
    }
}

/// Polls `check` until it holds, failing the scenario with `what` past
/// `timeout`.
pub async fn eventually<F, Fut>(what: &str, timeout: Duration, check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    assert!(wait_until(timeout, check).await, "{what}: never held");
}

/// `svc-threads schema`: the SDL, from the binary alone.
pub async fn schema_sdl() -> String {
    let output = run_once(
        env!("CARGO_BIN_EXE_svc-threads"),
        &[SCHEMA_ARG],
        &[],
        READY_TIMEOUT,
    )
    .await
    .expect("svc-threads schema runs");
    assert!(output.status.success(), "svc-threads schema failed");
    String::from_utf8(output.stdout).expect("a UTF-8 SDL")
}
