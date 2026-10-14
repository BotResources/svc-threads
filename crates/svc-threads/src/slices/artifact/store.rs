//! `threads.artifact` through the engine's `Persistence` seam, soft EDA. A
//! batch read is two statements: the artifacts, and the members of their
//! threads. The aggregate lists its blob reference, so that the pipeline
//! releases the bytes when the artifact is deleted (`cx.delete`).

use std::collections::{BTreeSet, HashMap};

use futures_util::future::BoxFuture;
use service_engine::BlobRef;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle, RowBatch};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::state::{ArtifactState, ArtifactStatus};
use crate::facts::{self, Fact, FactNoun};
use crate::fault::corrupt;

pub const TABLE: &str = "threads.artifact";

pub struct ArtifactStore;

impl Persistence for ArtifactStore {
    type Aggregate = ArtifactState;
    type Key = Uuid;
    type Event = Fact;

    const STYLE: PersistenceStyle = PersistenceStyle::SoftEda;

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, TABLE, key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> RowBatch<'a, Uuid, ArtifactState> {
        Box::pin(async move { read_many(conn, keys).await })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        artifact: &'a ArtifactState,
        facts: &'a [Fact],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "UPDATE {TABLE} SET message_id = $2, status = $3, version = $4 WHERE id = $1"
            ))
            .bind(artifact.id)
            .bind(artifact.message_id)
            .bind(artifact.status.as_db())
            .bind(artifact.version)
            .execute(&mut *conn)
            .await?;
            facts::append(
                conn,
                FactNoun::Artifact,
                artifact.id,
                artifact.version,
                facts,
            )
            .await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        artifact: &'a ArtifactState,
        facts: &'a [Fact],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO {TABLE} (id, thread_id, message_id, name, media_type, size, sha256, \
                     blob_ref, status, created_at, version) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
            ))
            .bind(artifact.id)
            .bind(artifact.thread_id)
            .bind(artifact.message_id)
            .bind(&artifact.name)
            .bind(&artifact.media_type)
            .bind(artifact.size)
            .bind(&artifact.sha256)
            .bind(artifact.blob_ref)
            .bind(artifact.status.as_db())
            .bind(artifact.created_at)
            .bind(artifact.version)
            .execute(&mut *conn)
            .await?;
            facts::append(
                conn,
                FactNoun::Artifact,
                artifact.id,
                artifact.version,
                facts,
            )
            .await
        })
    }

    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!("DELETE FROM {TABLE} WHERE id = $1"))
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl Aggregate for ArtifactState {
    type Store = ArtifactStore;

    fn key(&self) -> Uuid {
        self.id
    }

    fn pending_events(&self) -> &[Fact] {
        &self.pending
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        vec![BlobRef(self.blob_ref)]
    }
}

async fn read_many(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<(Uuid, ArtifactState)>, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT id, thread_id, message_id, name, media_type, size, sha256, blob_ref, status, \
                created_at, version \
         FROM {TABLE} WHERE id = ANY($1)"
    ))
    .bind(keys)
    .fetch_all(&mut *conn)
    .await?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut artifacts = Vec::with_capacity(rows.len());
    for row in &rows {
        let status: String = row.try_get("status")?;
        artifacts.push(ArtifactState {
            id: row.try_get("id")?,
            thread_id: row.try_get("thread_id")?,
            message_id: row.try_get("message_id")?,
            name: row.try_get("name")?,
            media_type: row.try_get("media_type")?,
            size: row.try_get("size")?,
            sha256: row.try_get("sha256")?,
            blob_ref: row.try_get("blob_ref")?,
            status: ArtifactStatus::from_db(&status).map_err(corrupt)?,
            created_at: row.try_get("created_at")?,
            version: row.try_get("version")?,
            members: BTreeSet::new(),
            pending: Vec::new(),
        });
    }
    let threads: Vec<Uuid> = artifacts
        .iter()
        .map(|artifact| artifact.thread_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let members: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT thread_id, user_id FROM threads.thread_member WHERE thread_id = ANY($1)",
    )
    .bind(&threads)
    .fetch_all(conn)
    .await?;
    let mut by_thread: HashMap<Uuid, BTreeSet<Uuid>> = HashMap::new();
    for (thread, user) in members {
        by_thread.entry(thread).or_default().insert(user);
    }
    Ok(artifacts
        .into_iter()
        .map(|mut artifact| {
            artifact.members = by_thread
                .get(&artifact.thread_id)
                .cloned()
                .unwrap_or_default();
            (artifact.id, artifact)
        })
        .collect())
}

/// The artifacts of a thread, or of one of its messages.
pub async fn ids_of(
    conn: &mut PgConnection,
    thread: Uuid,
    message: Option<Uuid>,
) -> Result<Vec<Uuid>, EngineError> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT id FROM {TABLE} WHERE thread_id = $1 AND ($2::uuid IS NULL OR message_id = $2) \
         ORDER BY id"
    ))
    .bind(thread)
    .bind(message)
    .fetch_all(conn)
    .await?)
}
