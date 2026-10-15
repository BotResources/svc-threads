//! `CREATE DATABASE` and `DROP DATABASE`, one at a time across the parallel scenarios.
//!
//! Scenarios run in parallel under cargo-nextest, and each one creates its own database
//! at setup and drops it at teardown — against ONE shared Postgres cluster. Since
//! PostgreSQL 15, `DROP DATABASE` makes every backend of the cluster release its file
//! handles (a `ProcSignalBarrier`, delivered as a signal), and a backend caught inside
//! `open()` by it can fail with `could not open file …: Interrupted system call` on the
//! CI runners' storage. The backend most exposed is a sibling's `CREATE DATABASE`, which
//! opens every file of the template one after the other: that is the failure a first
//! parallel CI run met (`failed to create e2e database: … base/1/3605: Interrupted system
//! call`).
//!
//! So the two catalog-wide gestures of the harness take one cluster-wide advisory lock:
//! a database is never being created while another is being dropped. Each is well under
//! a second, so the scenarios hardly wait on one another; everything else they do still
//! runs side by side. A scenario that panics before its teardown drops its database from
//! `E2eDatabase`'s `Drop`, outside this lock — the one path left uncovered, and only on a
//! run that is already red.
//!
//! This file is copied verbatim into every service's `tests/common/`, like `port.rs`.

use std::future::Future;

use sqlx::{Connection, PgConnection};

/// The advisory-lock key every suite of this repository takes for its catalog gestures
/// (an arbitrary constant; `pg_advisory_lock` keys are cluster-wide).
const CATALOG_LOCK: i64 = 0x6272_655f_6532_6564; // "bre_e2ed"

/// Run `gesture` — an `E2eDatabase` creation or cleanup — while holding the cluster-wide
/// catalog lock. The lock lives on a connection of its own, so a gesture that panics
/// releases it with that connection.
pub async fn serialized<T>(gesture: impl Future<Output = T>) -> T {
    dotenvy::dotenv().ok();
    let admin_url = std::env::var("E2E_PG_ADMIN_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("E2E_PG_ADMIN_URL (or DATABASE_URL) names the e2e Postgres cluster");
    let mut lock = PgConnection::connect(&admin_url)
        .await
        .expect("connect to the e2e Postgres cluster to take the catalog lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(CATALOG_LOCK)
        .execute(&mut lock)
        .await
        .expect("take the catalog lock");
    let outcome = gesture.await;
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(CATALOG_LOCK)
        .execute(&mut lock)
        .await
        .expect("release the catalog lock");
    lock.close().await.ok();
    outcome
}
