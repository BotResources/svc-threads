//! What a member reads of an artifact beside the `ARTIFACT` annotation naming
//! it: its name, media type and size, joined at read time from its row. The
//! stored annotation keeps naming the artifact by id alone.
//!
//! A label is given only for an artifact `ATTACHED` to the very message whose
//! annotation names it: a member reads it where they read that message, and an
//! annotation naming any other id — a stream's, not judged yet — reads its id
//! alone, so that no id can be probed through it.

use std::collections::HashMap;

use serde_json::{Map, Value};
use service_engine::error::EngineError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::state::ArtifactStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Label {
    message_id: Uuid,
    name: String,
    media_type: String,
    size: i64,
}

/// The labels of the artifacts one read names, by artifact id.
#[derive(Debug, Clone, Default)]
pub struct ArtifactLabels(HashMap<Uuid, Label>);

impl ArtifactLabels {
    /// The labels of `artifacts` that are attached: one statement, none for no
    /// artifact.
    pub async fn read(conn: &mut PgConnection, artifacts: &[Uuid]) -> Result<Self, EngineError> {
        if artifacts.is_empty() {
            return Ok(Self::default());
        }
        let rows = sqlx::query(
            "SELECT id, message_id, name, media_type, size FROM threads.artifact \
             WHERE id = ANY($1) AND status = $2",
        )
        .bind(artifacts)
        .bind(ArtifactStatus::Attached.as_db())
        .fetch_all(conn)
        .await?;
        let mut labels = HashMap::with_capacity(rows.len());
        for row in &rows {
            labels.insert(
                row.try_get("id")?,
                Label {
                    message_id: row.try_get("message_id")?,
                    name: row.try_get("name")?,
                    media_type: row.try_get("media_type")?,
                    size: row.try_get("size")?,
                },
            );
        }
        Ok(Self(labels))
    }

    /// The label of `artifact`, attached to `message`.
    #[cfg(test)]
    pub fn of(artifact: Uuid, message: Uuid, name: &str, media_type: &str, size: i64) -> Self {
        Self(HashMap::from([(
            artifact,
            Label {
                message_id: message,
                name: name.into(),
                media_type: media_type.into(),
                size,
            },
        )]))
    }

    /// The content of an `ARTIFACT` annotation of `message`, as members read
    /// it: `artifact_id`, then `name`, `media_type` and `size` when the
    /// artifact is attached to that message; `artifact_id` alone otherwise.
    pub fn label(&self, message: Uuid, content: &mut Map<String, Value>) {
        let Some(label) = content
            .get("artifact_id")
            .and_then(Value::as_str)
            .and_then(|id| Uuid::parse_str(id).ok())
            .and_then(|id| self.0.get(&id))
            .filter(|label| label.message_id == message)
        else {
            return;
        };
        content.insert("name".into(), Value::from(label.name.clone()));
        content.insert("media_type".into(), Value::from(label.media_type.clone()));
        content.insert("size".into(), Value::from(label.size));
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn labels(artifact: Uuid, message: Uuid) -> ArtifactLabels {
        ArtifactLabels(HashMap::from([(
            artifact,
            Label {
                message_id: message,
                name: "report.csv".into(),
                media_type: "text/csv".into(),
                size: 22,
            },
        )]))
    }

    fn read(labels: &ArtifactLabels, message: Uuid, artifact: Uuid) -> Value {
        let mut content = json!({ "artifact_id": artifact });
        labels.label(
            message,
            content.as_object_mut().expect("an annotation's content"),
        );
        content
    }

    #[test]
    fn an_artifact_attached_to_the_message_reads_its_name_media_type_and_size() {
        let (artifact, message) = (Uuid::now_v7(), Uuid::now_v7());
        assert_eq!(
            read(&labels(artifact, message), message, artifact),
            json!({
                "artifact_id": artifact,
                "name": "report.csv",
                "media_type": "text/csv",
                "size": 22,
            })
        );
    }

    #[test]
    fn an_artifact_without_its_row_reads_its_id_alone() {
        let (artifact, message) = (Uuid::now_v7(), Uuid::now_v7());
        assert_eq!(
            read(&ArtifactLabels::default(), message, artifact),
            json!({ "artifact_id": artifact }),
            "its row gone: the read does not fail"
        );
    }

    #[test]
    fn an_artifact_attached_to_another_message_reads_its_id_alone() {
        let (artifact, message) = (Uuid::now_v7(), Uuid::now_v7());
        assert_eq!(
            read(&labels(artifact, Uuid::now_v7()), message, artifact),
            json!({ "artifact_id": artifact }),
            "no label leaks through another message's annotation"
        );
    }

    #[test]
    fn a_content_without_an_artifact_id_is_left_as_it_is() {
        let labels = labels(Uuid::now_v7(), Uuid::now_v7());
        let mut content = json!({ "artifact_id": "not-a-uuid" });
        labels.label(Uuid::now_v7(), content.as_object_mut().unwrap());
        assert_eq!(content, json!({ "artifact_id": "not-a-uuid" }));
    }
}
