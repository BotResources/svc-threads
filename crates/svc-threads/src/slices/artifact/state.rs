//! An artifact's canonical state: a file a runner produced for a thread, its
//! bytes in Threads' object storage behind the engine's blob reference, and the
//! members of its thread, who may read it.
//!
//! `PENDING` at the upload request; `ATTACHED` once a message of the same
//! thread names it in an `ARTIFACT` annotation with its bytes present at the
//! declared digest. It lives as long as its message — or, never attached, as
//! long as the upload window.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::Serialize;
use service_engine::error::EngineError;
use service_engine::name::NounName;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use service_engine::{Cohort, Deps};
use uuid::Uuid;

use crate::facts::{Fact, FactMeta};
use crate::literal::literal_enum;
use crate::principal::{MEMBERSHIP_DEPS, ROSTER_DEPS, ThreadsPrincipal};
use crate::slices::thread::state::{MEMBER_DIM, UnknownStoredValue};

/// The artifact noun.
pub struct Artifact;

impl Noun for Artifact {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("artifact");
}

/// An artifact is read by the members of its thread only, while they are
/// active users of the roster copy.
impl Visibility for Artifact {
    type Row = ArtifactState;
    type Principal = ThreadsPrincipal;

    const DEPS: Deps = ROSTER_DEPS.union(MEMBERSHIP_DEPS);

    fn cohorts(row: &ArtifactState) -> Cohorts {
        row.members
            .iter()
            .map(|user| Cohort::uuid(MEMBER_DIM, *user))
            .collect()
    }

    fn memberships(principal: &ThreadsPrincipal) -> Cohorts {
        principal
            .active_user()
            .map(|user| Cohort::uuid(MEMBER_DIM, user))
            .into_iter()
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactStatus {
    Pending,
    Attached,
}

impl ArtifactStatus {
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Attached => "ATTACHED",
        }
    }

    pub fn from_db(stored: &str) -> Result<Self, UnknownStoredValue> {
        match stored {
            "PENDING" => Ok(Self::Pending),
            "ATTACHED" => Ok(Self::Attached),
            other => Err(UnknownStoredValue {
                column: "artifact.status",
                value: other.to_owned(),
            }),
        }
    }
}

literal_enum! {
    /// Why an artifact was removed, as its `Removed` fact says. A thread's
    /// deletion records none: the thread's `Deleted` fact covers its artifacts.
    pub enum RemovalReason {
        /// The upload window ended with no message naming it.
        NeverAttached => "NEVER_ATTACHED",
        /// Its message was deleted.
        MessageDeleted => "MESSAGE_DELETED",
    }
}

// The fact types of the noun `artifact`.
pub const REQUESTED: &str = "Requested";
pub const ATTACHED: &str = "Attached";
pub const REMOVED: &str = "Removed";

#[derive(Serialize)]
struct RequestedFact<'a> {
    thread_id: Uuid,
    name: &'a str,
    media_type: &'a str,
    size: i64,
    sha256: &'a str,
}

#[derive(Serialize)]
struct AttachedFact {
    message_id: Uuid,
}

#[derive(Serialize)]
struct RemovedFact {
    reason: RemovalReason,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactState {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub message_id: Option<Uuid>,
    pub name: String,
    pub media_type: String,
    pub size: i64,
    /// Lowercase hex SHA-256 the upload is pinned to.
    pub sha256: String,
    /// The engine's blob reference of its bytes.
    pub blob_ref: Uuid,
    pub status: ArtifactStatus,
    pub created_at: DateTime<Utc>,
    pub version: i64,
    /// The members of its thread.
    pub members: BTreeSet<Uuid>,
    /// Unsaved facts.
    pub pending: Vec<Fact>,
}

/// What a runner declares of the file it is about to upload.
pub struct Declared {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub name: String,
    pub media_type: String,
    pub size: i64,
    pub sha256: String,
    pub blob_ref: Uuid,
}

impl ArtifactState {
    /// A `PENDING` artifact at version 1.
    pub fn requested(
        declared: Declared,
        now: DateTime<Utc>,
        meta: &FactMeta,
    ) -> Result<Self, EngineError> {
        let fact = Fact::new(
            REQUESTED,
            &RequestedFact {
                thread_id: declared.thread_id,
                name: &declared.name,
                media_type: &declared.media_type,
                size: declared.size,
                sha256: &declared.sha256,
            },
            meta,
        )?;
        Ok(Self {
            id: declared.id,
            thread_id: declared.thread_id,
            message_id: None,
            name: declared.name,
            media_type: declared.media_type,
            size: declared.size,
            sha256: declared.sha256,
            blob_ref: declared.blob_ref,
            status: ArtifactStatus::Pending,
            created_at: now,
            version: 1,
            members: BTreeSet::new(),
            pending: vec![fact],
        })
    }

    /// Whether a message of `thread` may attach it: it is that thread's and
    /// still `PENDING`. Its bytes are judged apart (object storage).
    pub fn attachable_to(&self, thread: Uuid) -> bool {
        self.thread_id == thread && self.status == ArtifactStatus::Pending
    }

    pub fn attach(&mut self, message: Uuid, meta: &FactMeta) -> Result<(), EngineError> {
        self.status = ArtifactStatus::Attached;
        self.message_id = Some(message);
        self.version += 1;
        self.pending.push(Fact::new(
            ATTACHED,
            &AttachedFact {
                message_id: message,
            },
            meta,
        )?);
        Ok(())
    }

    pub fn removed_fact(
        &self,
        reason: RemovalReason,
        meta: &FactMeta,
    ) -> Result<Fact, EngineError> {
        Fact::new(REMOVED, &RemovedFact { reason }, meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_maps_to_and_from_its_column() {
        for status in [ArtifactStatus::Pending, ArtifactStatus::Attached] {
            assert_eq!(ArtifactStatus::from_db(status.as_db()), Ok(status));
        }
        assert!(ArtifactStatus::from_db("pending").is_err());
    }

    #[test]
    fn only_a_pending_artifact_of_the_same_thread_is_attachable() {
        let thread = Uuid::now_v7();
        let meta = FactMeta {
            actor: br_core_integration::Actor::Service(Uuid::now_v7().into()),
            acting_user_id: None,
            impersonator_id: None,
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            occurred_at: Utc::now(),
        };
        let mut artifact = ArtifactState::requested(
            Declared {
                id: Uuid::now_v7(),
                thread_id: thread,
                name: "report.csv".into(),
                media_type: "text/csv".into(),
                size: 3,
                sha256: "a".repeat(64),
                blob_ref: Uuid::now_v7(),
            },
            Utc::now(),
            &meta,
        )
        .unwrap();
        assert!(artifact.attachable_to(thread));
        assert!(!artifact.attachable_to(Uuid::now_v7()));
        artifact.attach(Uuid::now_v7(), &meta).unwrap();
        assert!(!artifact.attachable_to(thread));
        assert_eq!(artifact.version, 2);
    }
}
