//! The declared infrastructure the harness does not provision on its own: the
//! object store, the retention bound of the two fixed Fabric streams, and
//! Threads' `STREAMING_threads` stream.
//!
//! ## Streams — harness gaps, bridged out of band
//!
//! The engine refuses to serve over an integration stream with an unlimited
//! `max_age` (a durable replays from the start of its stream, so an unbounded
//! stream could redeliver past any claim retention), and it binds — never
//! creates — the `STREAMING_threads` stream of its accumulated lane, refusing to
//! come up unless its seal retention covers that stream's `max_age`.
//! `FabricTestNats` get-or-creates `INTEGRATION_CMD` / `INTEGRATION_EVT` with the
//! JetStream defaults (unlimited) and knows no `STREAMING_*` stream; the raw
//! handles that could do either were removed in harness v1.0.0, and `async_nats`
//! is banned from a suite. So both are declared the way a deployment declares
//! them, **out of band**, with the `nats` CLI, before the service binds them.
//!
//! Delete [`declare_streams`]' CLI calls the day the harness can declare a fixed
//! stream with a bounded `max_age` and a lane-A stream.
//!
//! ## Object storage — a real MinIO, one per scenario
//!
//! An artifact is uploaded through a presigned POST pinned to its exact size and
//! SHA-256 by a POST-policy checksum condition, so the store must be a real
//! S3-compatible server honouring it (MinIO ≥ `RELEASE.2024-12-13`). This module
//! spawns the `minio` binary found on `PATH` and creates the bucket the service
//! is configured with, which is what a deployment does.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use tokio::process::{Child, Command};
use uuid::Uuid;

/// The retention a deployment gives the two fixed streams, for this suite.
pub const INTEGRATION_MAX_AGE: &str = "30m";
/// The retention of `STREAMING_threads`: within the engine's default seal
/// retention (24 h), as the engine requires.
pub const STREAMING_MAX_AGE: &str = "1h";

const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
pub const S3_REGION: &str = "us-east-1";
const MINIO_BOOT: Duration = Duration::from_secs(30);

/// Bound `INTEGRATION_CMD` and `INTEGRATION_EVT`, and add `STREAMING_threads`,
/// on the broker at `nats_url`.
pub async fn declare_streams(nats_url: &str) {
    for stream in [
        br_util_nats_fabric::INTEGRATION_CMD,
        br_util_nats_fabric::INTEGRATION_EVT,
    ] {
        nats_cli(
            nats_url,
            &[
                "stream",
                "edit",
                stream,
                "--max-age",
                INTEGRATION_MAX_AGE,
                "--force",
            ],
        )
        .await;
    }
    let subjects = format!("stream.{}.>", super::SERVICE);
    nats_cli(
        nats_url,
        &[
            "stream",
            "add",
            contract_threads::STREAMING_STREAM,
            "--subjects",
            &subjects,
            "--storage",
            "file",
            "--retention",
            "limits",
            "--max-age",
            STREAMING_MAX_AGE,
            "--dupe-window",
            "2m",
            "--defaults",
        ],
    )
    .await;
}

/// Runs one `nats` CLI command against `nats_url`, failing the scenario loudly.
pub async fn nats_cli(nats_url: &str, args: &[&str]) -> String {
    let output = Command::new("nats")
        .arg("--server")
        .arg(nats_url)
        .args(args)
        .output()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "the `nats` CLI must be on PATH to declare the streams Threads binds \
                 (a harness gap, see infra.rs): {e}"
            )
        });
    assert!(
        output.status.success(),
        "`nats {}` failed: {}\n{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// A MinIO server of this scenario's own, and the one bucket the service uses.
pub struct Minio {
    child: Child,
    port: u16,
    store: PathBuf,
    pub bucket: String,
}

impl Minio {
    /// A free port is only reserved until it is released: another process may
    /// take it before MinIO binds. A server that exits before it is ready is
    /// retried on fresh ports.
    pub async fn start() -> Self {
        for _ in 0..5 {
            if let Some(minio) = Self::try_start().await {
                minio.create_bucket().await;
                return minio;
            }
        }
        panic!("minio never came up on five pairs of fresh ports");
    }

    async fn try_start() -> Option<Self> {
        let store = std::env::temp_dir().join(format!("threads-minio-{}", Uuid::now_v7().simple()));
        std::fs::create_dir_all(&store).expect("create the ephemeral object store");
        let port = super::free_port();
        let console = super::free_port();
        let child = Command::new("minio")
            .args([
                "server",
                store.to_str().expect("a utf-8 store path"),
                "--address",
                &format!("127.0.0.1:{port}"),
                "--console-address",
                &format!("127.0.0.1:{console}"),
            ])
            .env("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
            .env("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("`minio` (>= RELEASE.2024-12-13) must be on PATH for the threads e2e suite");
        let mut minio = Self {
            child,
            port,
            store,
            bucket: format!("threads-{}", Uuid::now_v7().simple()),
        };
        minio.await_ready().await.then_some(minio)
    }

    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn access_key(&self) -> &'static str {
        MINIO_ACCESS_KEY
    }

    pub fn secret_key(&self) -> &'static str {
        MINIO_SECRET_KEY
    }

    /// `true` once MinIO answers ready; `false` if the process exited first.
    async fn await_ready(&mut self) -> bool {
        let health = format!("{}/minio/health/ready", self.endpoint());
        let client = reqwest::Client::new();
        let deadline = Instant::now() + MINIO_BOOT;
        loop {
            if self.child.try_wait().ok().flatten().is_some() {
                return false;
            }
            if let Ok(response) = client.get(&health).send().await
                && response.status().is_success()
            {
                return true;
            }
            assert!(
                Instant::now() < deadline,
                "minio never became ready on {health}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn create_bucket(&self) {
        let bucket = Bucket::new(
            self.endpoint().parse().expect("a valid endpoint url"),
            UrlStyle::Path,
            self.bucket.clone(),
            S3_REGION.to_string(),
        )
        .expect("a valid bucket");
        let credentials = Credentials::new(MINIO_ACCESS_KEY, MINIO_SECRET_KEY);
        let client = reqwest::Client::new();
        // `/minio/health/ready` can answer before every MinIO sub-system is
        // initialised: a write in that window answers 503. Retry that one
        // status, bounded by the boot budget; any other refusal fails at once.
        let deadline = Instant::now() + MINIO_BOOT;
        loop {
            let url = bucket
                .create_bucket(&credentials)
                .sign(Duration::from_secs(60));
            let status = client
                .put(url)
                .send()
                .await
                .expect("reach minio to create the bucket")
                .status();
            if status.is_success() {
                return;
            }
            assert!(
                status == reqwest::StatusCode::SERVICE_UNAVAILABLE && Instant::now() < deadline,
                "creating the bucket {} answered {status}",
                self.bucket
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn shutdown(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
        let _ = std::fs::remove_dir_all(&self.store);
    }
}

impl Drop for Minio {
    /// A scenario that fails mid-way never reaches `shutdown`: the process is
    /// killed on drop (`kill_on_drop`), and its store must not be left behind.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.store);
    }
}
