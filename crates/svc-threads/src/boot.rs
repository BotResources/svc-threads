//! The process entry: the engine boot kit, fed with Threads' migration set, its
//! object storage and its registrations.

use std::ops::RangeInclusive;

use service_engine::config::EngineConfig;
use service_engine::error::EngineError;
use service_engine::{BlobConfig, BootPlan, Engine, LibraryMigrations, run_service};
use sqlx::migrate::Migrator;

use crate::principal::ThreadsPrincipal;
use crate::slices::{MutationRoot, QueryRoot, SubscriptionRoot};

/// Threads' own Postgres schema.
pub const SCHEMA: &str = "threads";

/// The migration versions of the `threads` schema, disjoint from the engine's
/// reserved range (`9_113_…`). Threads owns its database; the band only has to
/// stay clear of the engine's and of any library a later version embeds.
pub const BAND: RangeInclusive<i64> = 9_800_000_001..=9_800_999_999;

/// The object-storage group of the ops contract (`S3_*`). The engine reads none
/// of it; the service hands it to `with_blob_storage`.
const S3_REQUIRED: [&str; 4] = ["S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY"];
const S3_REGION: &str = "S3_REGION";
const S3_PUBLIC_ENDPOINT: &str = "S3_PUBLIC_ENDPOINT";
const DEFAULT_S3_REGION: &str = "us-east-1";

pub async fn run() -> Result<(), EngineError> {
    let threads = crate::config::ThreadsConfig::from_env()?;
    let blob = object_storage_from_env()?.map(|blob| blob.with_upload_ttl(threads.upload_window));
    crate::config::install(threads);
    let config = engine_config(EngineConfig::from_env()?, blob);

    run_service(BootPlan {
        component: "svc-threads",
        libraries: migration_sets(),
        // Threads' own migrations run as the `threads` schema's set in
        // `libraries` (see `migration_sets`), so the service set is empty.
        service_migrator: Migrator::DEFAULT,
        config,
        query: QueryRoot::default(),
        mutation: MutationRoot::default(),
        subscription: SubscriptionRoot::default(),
        declare_scopes: true,
        register: |engine: &mut Engine<ThreadsPrincipal>| {
            upgrade_before_ready()?;
            crate::register::all(engine)
        },
    })
    .await
}

/// Converts the messages an earlier Threads stored to the library's 2.0 model
/// before the service is ready ([`crate::slices::message::upgrade`]): `serve`
/// runs it once the migrations are applied and before anything is served, so
/// `/readyz` answers only once every message is converted, and a message that
/// cannot be converted ends the boot — the pod never becomes ready — after its
/// id was logged.
///
/// ENGINE GAP (br-service-engine v0.4.2): the boot kit offers a service no
/// asynchronous step between its migration check and readiness, and no
/// readiness gate of its own (the engine's assembly recomputes readiness from
/// its own inputs on every beat). The upgrade therefore runs inside the
/// synchronous `register`, on the boot task (`block_in_place`), on its own
/// pool of the app role. The engine would need a `BootPlan` step the boot awaits
/// before it serves, or a service readiness input.
///
/// While it runs, nothing listens: `/livez` and `/readyz` both fail, so the
/// rollout's startup probe must cover the upgrade (README, *Deploy*). With an
/// asynchronous boot step the engine could keep `/livez` up and `/readyz` down
/// for the duration instead.
///
/// `block_in_place` needs the multi-threaded Tokio runtime (`#[tokio::main]`'s
/// default, which `main` uses): on a current-thread runtime it panics. The
/// flavour is checked first, so that a runtime built otherwise fails the boot
/// with a configuration error instead.
fn upgrade_before_ready() -> Result<(), EngineError> {
    let url = std::env::var(DATABASE_URL)
        .map_err(|_| EngineError::Config(format!("{DATABASE_URL} must be set for serve")))?;
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| EngineError::Config("serve runs on a Tokio runtime".into()))?;
    if runtime.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return Err(EngineError::Config(
            "serve needs the multi-threaded Tokio runtime: the upgrade blocks the boot task \
             in place"
                .into(),
        ));
    }
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let pool = service_engine::connect_pool(&url).await?;
            let upgraded = crate::slices::message::upgrade::run(&pool).await;
            pool.close().await;
            upgraded.map(drop)
        })
    })
}

/// The app role's database URL, as the engine's ops contract names it.
const DATABASE_URL: &str = "DATABASE_URL";

/// The engine configuration Threads runs on: the engine's own from the
/// environment, with the service's identity and its object storage.
///
/// ENGINE GAP (br-service-engine v0.4.1): the engine presigns no read access to
/// a verified upload before its reaper verified the bytes, and offers no way to
/// promote one Threads already verified (when a message attaches it). The
/// reaper therefore sweeps on every beat: a member's read access to an artifact
/// is available within one beat of its upload. A sweep only HEADs the uploads
/// still pending.
pub fn engine_config(base: EngineConfig, blob: Option<BlobConfig>) -> EngineConfig {
    let beat = base.beat;
    let config = base
        .with_service(crate::SERVICE)
        .with_service_version(env!("CARGO_PKG_VERSION"));
    match blob {
        Some(blob) => config
            .with_blob_storage(blob)
            .with_blob_reaper_interval(beat),
        None => config,
    }
}

/// The one migration set `migrate` applies after the engine's: the `threads`
/// schema.
///
/// Declared as a schema set rather than as the service set on purpose: engine
/// v0.4.1's `migrate` grants the app role the engine's schema, every declared
/// set's schema and `public` — and nothing else — so a service table outside
/// `public` would be unreadable at `serve`. Declaring the set also buys the
/// engine's check that it creates nothing outside `threads`. The day the engine
/// grants a service's own schema, this set moves to `service_migrator`
/// unchanged: the versions stay on the one shared ledger.
pub fn migration_sets() -> Vec<LibraryMigrations> {
    vec![LibraryMigrations {
        name: crate::SERVICE,
        schema: SCHEMA,
        band: BAND,
        migrator: sqlx::migrate!("./migrations"),
    }]
}

/// The object storage artifacts live in. Absent entirely, `schema` and
/// `migrate` do not need it and `serve` fails loud at registration (an
/// artifact store is declared infrastructure); present in part, it is a
/// misconfiguration named here.
fn object_storage_from_env() -> Result<Option<BlobConfig>, EngineError> {
    match S3_REQUIRED.map(|key| std::env::var(key).ok()) {
        [None, None, None, None] => Ok(None),
        [Some(endpoint), Some(bucket), Some(access), Some(secret)] => {
            let region = std::env::var(S3_REGION).unwrap_or_else(|_| DEFAULT_S3_REGION.to_string());
            let mut blob = BlobConfig::new(endpoint, region, bucket, access, secret);
            if let Ok(public) = std::env::var(S3_PUBLIC_ENDPOINT) {
                blob = blob.with_public_endpoint(public);
            }
            Ok(Some(blob))
        }
        _ => {
            let missing: Vec<&str> = S3_REQUIRED
                .into_iter()
                .filter(|key| std::env::var(key).is_err())
                .collect();
            Err(EngineError::Config(format!(
                "object storage is configured in part: {} must be set too",
                missing.join(", ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threads_migrations_sit_inside_the_threads_band() {
        let sets = migration_sets();
        let own = sets
            .iter()
            .find(|set| set.schema == SCHEMA)
            .expect("the threads schema declares its set");
        let versions: Vec<i64> = own.migrator.iter().map(|m| m.version).collect();
        assert!(!versions.is_empty(), "the threads schema has migrations");
        assert!(versions.iter().all(|version| BAND.contains(version)));
    }

    #[test]
    fn the_threads_band_is_disjoint_from_the_engine_band() {
        let engine = service_engine::schema::RESERVED_VERSION_MIN
            ..=service_engine::schema::RESERVED_VERSION_MAX;
        assert!(BAND.start() > engine.end() || BAND.end() < engine.start());
    }
}
