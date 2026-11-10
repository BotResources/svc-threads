//! What a runner receives of artifacts with a thread's context: the `ATTACHED`
//! artifacts of the messages of the conversation it receives, beside that
//! conversation — never inside it — and, when it asks for one, a time-bounded
//! read access to it.
//!
//! The list is read in one statement for the whole conversation. The read
//! access is a field resolved only when selected: a context read that does not
//! select it signs nothing.

use async_graphql::{ComplexObject, Context, Result, SimpleObject};
use contract_threads::AnnotationKind;
use serde::{Deserialize, Serialize};
use service_engine::blobs::Disposition;
use service_engine::error::EngineError;
use service_engine::{BlobRef, Query};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::read_access;
use super::state::ArtifactStatus;
use super::view::RunnerArtifactsView;
use crate::principal::ThreadsPrincipal;

/// An `ATTACHED` artifact of a message of the conversation a runner receives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SimpleObject)]
#[graphql(complex)]
pub struct ContextArtifact {
    pub id: Uuid,
    pub name: String,
    pub media_type: String,
    /// In bytes.
    pub size: i64,
    /// The lowercase hexadecimal SHA-256 of its bytes.
    pub sha256: String,
    /// The message that attached it.
    pub message_id: Uuid,
    /// The engine's reference of its bytes: what the read access signs, never
    /// shown.
    #[graphql(skip)]
    pub blob_ref: Uuid,
}

#[ComplexObject]
impl ContextArtifact {
    /// A time-bounded read access to the artifact's bytes, signed only when
    /// asked for; `null` once the artifact is gone (its message or its thread
    /// deleted since the context was read).
    async fn download_url(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let query = Query::<ThreadsPrincipal>::new(ctx)?;
        read_access::sign::<RunnerArtifactsView>(
            &query,
            self.id,
            BlobRef(self.blob_ref),
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
) -> Result<Vec<ContextArtifact>, EngineError> {
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
            Ok(ContextArtifact {
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
