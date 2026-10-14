//! The artifact capability: the files runners and AIs produce for a thread, in
//! Threads' object storage behind the engine's blob references.
//!
//! A runner requests an upload for a thread and receives a time-bounded upload
//! access pinned to the declared size and digest (the engine's verified
//! presigned POST); the artifact is `PENDING`. It becomes `ATTACHED` when a
//! message of the same thread names it in an `ARTIFACT` block and its bytes are
//! present with the declared digest; a `PENDING` artifact never attached is
//! removed after the upload window. A member reads it through a time-bounded
//! read access; it lives as long as its message.
//!
//! The scaffold registers the blob kind — so that a pod without object storage
//! fails loud at boot — and the roots; the artifact aggregate, its store and
//! the gestures are the implementation pass's.

pub mod graphql;

use std::time::Duration;

use service_engine::error::EngineError;
use service_engine::graphql::SliceFragment;
use service_engine::{BlobPolicy, Blobs, Engine};

use crate::principal::ThreadsPrincipal;

/// How long an upload access stays valid, and how long a `PENDING` artifact
/// waits for its bytes before it is removed. The blob's orphan window is the
/// same: the engine refuses one shorter than the upload access.
pub const UPLOAD_WINDOW: Duration = Duration::from_secs(15 * 60);

/// The largest artifact Threads stores. The specification names no bound; this
/// is the scaffold's, to ratify.
pub const MAX_ARTIFACT_BYTES: u64 = 256 << 20;

/// The blob kind of an artifact's bytes.
pub struct ThreadArtifact;

impl Blobs for ThreadArtifact {
    const KIND: &'static str = "thread_artifact";
}

pub const ARTIFACT_POLICY: BlobPolicy = BlobPolicy {
    max_bytes: MAX_ARTIFACT_BYTES,
    orphan_after: UPLOAD_WINDOW,
};

pub fn register(engine: &mut Engine<ThreadsPrincipal>) -> Result<(), EngineError> {
    if !engine.blobs_configured() {
        return Err(EngineError::Config(
            "Threads stores artifacts: its object storage (S3_ENDPOINT, S3_BUCKET, \
             S3_ACCESS_KEY, S3_SECRET_KEY) must be configured"
                .to_string(),
        ));
    }
    engine.register_blobs::<ThreadArtifact>(ARTIFACT_POLICY)?;
    engine.register_schema_slice(SliceFragment::derive::<
        graphql::ArtifactQuery,
        graphql::ArtifactMutation,
        async_graphql::EmptySubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
