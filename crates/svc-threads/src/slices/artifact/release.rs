//! The bytes of artifacts a migration removed. SQL reaches the artifact rows
//! but not object storage: the upgrade to one list of messages (migration
//! `9800000008`) removes the artifacts of the messages it drops, and queues
//! their blob references in `threads.released_blob`, in its own transaction.
//! This cron job drains the queue once Threads serves: it purges each
//! reference's bytes through the engine's blob reader, then forgets it.
//!
//! Durable and idempotent: the queue is the only record of the purge to do. A
//! pod that dies between the purge and the forgetting leaves the reference
//! queued, and purging it again changes nothing. A reference some artifact
//! holds is never purged ([`super::lifecycle::purge_unheld`]). The engine's
//! cron fires the job on one pod at a time. The engine binds the blob store
//! before the beat runs, so the reader is bound when the job fires.

use futures_util::future::BoxFuture;
use service_engine::cron::{CronJob, Schedule};
use service_engine::error::CronError;
use service_engine::name::JobName;
use sqlx::PgPool;
use uuid::Uuid;

use super::lifecycle::purge_unheld;

/// How many references one firing purges at most; the next takes the rest.
const BATCH: i64 = 256;

pub struct ReleasedBytes;

impl CronJob for ReleasedBytes {
    fn name(&self) -> JobName {
        JobName::from_static("released_bytes")
    }

    fn schedule(&self) -> Schedule {
        Schedule::EveryBeats(5)
    }

    fn run<'a>(&'a self, pg: &'a PgPool) -> BoxFuture<'a, Result<(), CronError>> {
        Box::pin(async move {
            let queued: Vec<Uuid> = sqlx::query_scalar(
                "SELECT blob_ref FROM threads.released_blob ORDER BY released_at, blob_ref \
                 LIMIT $1",
            )
            .bind(BATCH)
            .fetch_all(pg)
            .await?;
            if queued.is_empty() {
                return Ok(());
            }
            let mut conn = pg.acquire().await?;
            purge_unheld(&mut conn, &queued)
                .await
                .map_err(|fault| CronError::Job(Box::new(fault)))?;
            sqlx::query("DELETE FROM threads.released_blob WHERE blob_ref = ANY($1)")
                .bind(&queued)
                .execute(&mut *conn)
                .await?;
            tracing::info!(
                purged = queued.len(),
                "the bytes of artifacts removed at the upgrade to one list of messages are purged"
            );
            Ok(())
        })
    }
}
