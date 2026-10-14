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

/// How often the engine's reaper looks at pending uploads. A member's read
/// access to an artifact waits for the reaper to verify its bytes, so Threads
/// keeps it short.
const BLOB_REAPER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

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
        register: |engine: &mut Engine<ThreadsPrincipal>| crate::register::all(engine),
    })
    .await
}

/// The engine configuration Threads runs on: the engine's own from the
/// environment, with the service's identity and its object storage.
pub fn engine_config(base: EngineConfig, blob: Option<BlobConfig>) -> EngineConfig {
    let config = base
        .with_service(crate::SERVICE)
        .with_service_version(env!("CARGO_PKG_VERSION"));
    match blob {
        Some(blob) => config
            .with_blob_storage(blob)
            .with_blob_reaper_interval(BLOB_REAPER_INTERVAL),
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
