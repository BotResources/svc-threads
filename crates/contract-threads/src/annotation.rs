//! Annotations: Threads blocks placed after an entry of a message, to show what
//! happens around the AI. An annotation is not part of the library's
//! conversation: a runner never receives it, and a user message holds none.
//!
//! An annotation is `{ "id", "kind", "content" }`, its content typed by its
//! kind; it travels placed after an entry of its message —
//! `{ "after_entry_id", "annotation" }` ([`PlacedAnnotation`]) in a
//! `message.store`, and so in an `ANNOTATION` chunk. Its position is its place
//! among the annotations after that entry, from 0.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::wire::wire_enum;

/// One annotation, under the caller-minted UUIDv7 it keeps for its life
/// (`ID_NOT_UUIDV7` otherwise).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Annotation {
    pub id: Uuid,
    #[serde(flatten)]
    pub body: AnnotationBody,
}

impl Annotation {
    pub fn new(id: Uuid, body: AnnotationBody) -> Self {
        Self { id, body }
    }

    pub fn kind(&self) -> AnnotationKind {
        self.body.kind()
    }
}

/// An annotation placed after an entry of its message: the entry must be one
/// of the message's (`MESSAGE_INVALID`, `annotation_entry_unknown`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacedAnnotation {
    pub after_entry_id: Uuid,
    pub annotation: Annotation,
}

impl PlacedAnnotation {
    pub fn new(after_entry_id: Uuid, annotation: Annotation) -> Self {
        Self {
            after_entry_id,
            annotation,
        }
    }
}

/// An annotation's kind and its typed content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "content")]
pub enum AnnotationBody {
    /// An artifact of the same thread, produced by a runner: naming it attaches
    /// it once its bytes are present with the declared digest
    /// (`ARTIFACT_NOT_READY`).
    #[serde(rename = "ARTIFACT")]
    Artifact(ArtifactRef),
    /// A thread, typically a child started from this annotation.
    #[serde(rename = "THREAD_REFERENCE")]
    ThreadReference(ThreadRef),
    /// A Job of the Jobs service.
    #[serde(rename = "JOB_REFERENCE")]
    JobReference(JobRef),
    #[serde(rename = "ERROR")]
    Error(Notice),
    #[serde(rename = "WARNING")]
    Warning(Notice),
}

impl AnnotationBody {
    pub fn kind(&self) -> AnnotationKind {
        match self {
            Self::Artifact(_) => AnnotationKind::Artifact,
            Self::ThreadReference(_) => AnnotationKind::ThreadReference,
            Self::JobReference(_) => AnnotationKind::JobReference,
            Self::Error(_) => AnnotationKind::Error,
            Self::Warning(_) => AnnotationKind::Warning,
        }
    }

    /// The artifact an `ARTIFACT` annotation names.
    pub fn artifact(&self) -> Option<Uuid> {
        match self {
            Self::Artifact(artifact) => Some(artifact.artifact_id),
            _ => None,
        }
    }
}

wire_enum! {
    /// The kind of an annotation.
    pub enum AnnotationKind {
        Artifact => "ARTIFACT",
        ThreadReference => "THREAD_REFERENCE",
        JobReference => "JOB_REFERENCE",
        Error => "ERROR",
        Warning => "WARNING",
    }
}

/// `ARTIFACT`: an artifact of the same thread. No foreign key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: Uuid,
}

/// `THREAD_REFERENCE`: a thread. No foreign key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadRef {
    pub thread_id: Uuid,
}

/// `JOB_REFERENCE`: a Job of the Jobs service. No foreign key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRef {
    pub job_id: Uuid,
}

/// `ERROR` or `WARNING`: a code, and an optional text for the people reading
/// the thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notice {
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn an_annotation_carries_typed_content_and_travels_placed_after_its_entry() {
        let (id, entry, artifact) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let wire = json!({
            "after_entry_id": entry,
            "annotation": { "id": id, "kind": "ARTIFACT", "content": { "artifact_id": artifact } }
        });
        let placed: PlacedAnnotation = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(placed.after_entry_id, entry);
        assert_eq!(placed.annotation.kind(), AnnotationKind::Artifact);
        assert_eq!(placed.annotation.body.artifact(), Some(artifact));
        assert_eq!(serde_json::to_value(&placed).unwrap(), wire);
    }

    #[test]
    fn a_notice_carries_a_code_and_an_optional_text() {
        let warning: Annotation = serde_json::from_value(json!({
            "id": Uuid::now_v7(),
            "kind": "WARNING",
            "content": { "code": "PARTIAL_ANSWER" }
        }))
        .unwrap();
        assert_eq!(
            warning.body,
            AnnotationBody::Warning(Notice {
                code: "PARTIAL_ANSWER".into(),
                text: None
            })
        );
    }

    #[test]
    fn a_kind_the_contract_does_not_name_does_not_decode() {
        assert!(
            serde_json::from_value::<Annotation>(json!({
                "id": Uuid::now_v7(),
                "kind": "CITATION",
                "content": { "text": "a quote" }
            }))
            .is_err()
        );
        for kind in AnnotationKind::ALL {
            assert_eq!(AnnotationKind::parse(kind.as_str()), Ok(*kind));
        }
    }
}
