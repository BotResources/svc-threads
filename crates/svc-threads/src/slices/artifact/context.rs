//! What a runner receives of artifacts with a thread's context: the `ATTACHED`
//! artifacts of the messages of the conversation it receives, beside that
//! conversation — never inside it — and, when it asks for one, a time-bounded
//! read access to it.
//!
//! The list is read in one statement for the whole conversation. The read
//! access is a field resolved only when selected: a context read that does not
//! select it signs nothing.

use async_graphql::{Context, Object};
use contract_threads::AnnotationKind;
use serde::{Deserialize, Serialize};
use service_engine::blobs::Disposition;
use service_engine::error::EngineError;
use service_engine::{BlobRef, Query};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::read_access::{self, SigningPermits};
use super::state::ArtifactStatus;
use super::view::RunnerArtifactsView;
use crate::principal::ThreadsPrincipal;

/// An `ATTACHED` artifact of a message of the conversation a runner receives,
/// as the context holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachedArtifact {
    pub id: Uuid,
    pub name: String,
    pub media_type: String,
    pub size: i64,
    pub sha256: String,
    pub message_id: Uuid,
    /// The engine's reference of its bytes: what the read access signs, never
    /// shown.
    pub blob_ref: Uuid,
}

/// An `ATTACHED` artifact of a message of the conversation a runner receives,
/// as the runner reads it: with the signing permits of its context read.
pub struct ContextArtifact {
    artifact: AttachedArtifact,
    permits: SigningPermits,
}

impl ContextArtifact {
    /// The artifacts of one context read, all signing under its `permits`.
    pub fn of_read(artifacts: &[AttachedArtifact], permits: &SigningPermits) -> Vec<Self> {
        artifacts
            .iter()
            .map(|artifact| Self {
                artifact: artifact.clone(),
                permits: permits.clone(),
            })
            .collect()
    }

    /// The permits it signs under.
    pub fn permits(&self) -> &SigningPermits {
        &self.permits
    }
}

#[Object]
impl ContextArtifact {
    async fn id(&self) -> Uuid {
        self.artifact.id
    }

    async fn name(&self) -> &str {
        &self.artifact.name
    }

    async fn media_type(&self) -> &str {
        &self.artifact.media_type
    }

    /// In bytes.
    async fn size(&self) -> i64 {
        self.artifact.size
    }

    /// The lowercase hexadecimal SHA-256 of its bytes.
    async fn sha256(&self) -> &str {
        &self.artifact.sha256
    }

    /// The message that attached it.
    async fn message_id(&self) -> Uuid {
        self.artifact.message_id
    }

    /// A time-bounded read access to the artifact's bytes, signed only when
    /// asked for; `null` once the artifact is gone (its message or its thread
    /// deleted since the context was read), or when signing it failed — the
    /// failure is then reported as an error of this field.
    async fn download_url(&self, ctx: &Context<'_>) -> Option<String> {
        // A failed signing nulls this access only. Raised as the field's
        // error, it would null the non-null list and the context up to
        // `threadsContext`, whose `null` says the thread does not exist: the
        // runner would lose the conversation and every other access.
        match self.sign(ctx).await {
            Ok(url) => url,
            Err(error) => {
                tracing::warn!(
                    artifact_id = %self.artifact.id,
                    error = %error.message,
                    "a runner's read access to an artifact failed to sign"
                );
                ctx.add_error(ctx.set_error_path(error.into_server_error(ctx.item.pos)));
                None
            }
        }
    }
}

impl ContextArtifact {
    async fn sign(&self, ctx: &Context<'_>) -> async_graphql::Result<Option<String>> {
        let query = Query::<ThreadsPrincipal>::new(ctx)?;
        read_access::sign::<RunnerArtifactsView>(
            &query,
            &self.permits,
            self.artifact.id,
            BlobRef(self.artifact.blob_ref),
            Disposition::Attachment,
        )
        .await
    }
}

/// The `ATTACHED` artifacts of `messages`, the conversation's messages in their
/// order: by that order, then by the place of the `ARTIFACT` annotation naming
/// each in its message (its entry's position, then its own after that entry).
pub async fn of_messages(
    conn: &mut PgConnection,
    messages: &[Uuid],
) -> Result<Vec<AttachedArtifact>, EngineError> {
    if messages.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(ATTACHED_TO_MESSAGES)
        .bind(messages)
        .bind(ArtifactStatus::Attached.as_db())
        .bind(AnnotationKind::Artifact.as_str())
        .fetch_all(conn)
        .await?;
    rows.iter()
        .map(|row| {
            Ok(AttachedArtifact {
                id: row.try_get("id")?,
                name: row.try_get("name")?,
                media_type: row.try_get("media_type")?,
                size: row.try_get("size")?,
                sha256: row.try_get("sha256")?,
                message_id: row.try_get("message_id")?,
                blob_ref: row.try_get("blob_ref")?,
            })
        })
        .collect()
}

/// An artifact is attached in the transaction that writes its message's
/// annotations, so an `ATTACHED` artifact always has one naming it; the
/// lateral join takes the first, should a message name it twice.
const ATTACHED_TO_MESSAGES: &str = "\
    SELECT art.id, art.name, art.media_type, art.size, art.sha256, art.message_id, art.blob_ref \
    FROM unnest($1::uuid[]) WITH ORDINALITY AS turn(message_id, place) \
    JOIN threads.artifact art ON art.message_id = turn.message_id AND art.status = $2 \
    LEFT JOIN LATERAL ( \
        SELECT e.position AS entry_position, a.position \
        FROM threads.message_annotation a \
        JOIN threads.message_entry e ON e.id = a.after_entry_id \
        WHERE a.message_id = art.message_id AND a.kind = $3 \
          AND a.content ->> 'artifact_id' = art.id::text \
        ORDER BY e.position, a.position \
        LIMIT 1 \
    ) named ON true \
    ORDER BY turn.place, named.entry_position, named.position, art.id";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_artifacts_of_a_conversation_are_one_statement_in_its_order() {
        let query = ATTACHED_TO_MESSAGES;
        assert_eq!(query.matches("SELECT").count(), 2, "one lateral, no loop");
        assert!(query.contains("WITH ORDINALITY"));
        assert!(
            query
                .trim_end()
                .ends_with("ORDER BY turn.place, named.entry_position, named.position, art.id"),
            "by message, then by annotation"
        );
        assert!(query.contains("art.status = $2") && !query.contains("$4"));
    }

    #[test]
    fn the_engine_reference_is_never_in_the_schema() {
        let sdl = async_graphql::Schema::build(
            Probe,
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        )
        .finish()
        .sdl();
        assert!(sdl.contains("downloadUrl: String\n"), "{sdl}");
        assert!(!sdl.contains("blobRef"), "{sdl}");
    }

    struct Probe;

    #[async_graphql::Object]
    impl Probe {
        async fn artifact(&self) -> Option<ContextArtifact> {
            None
        }
    }
}
