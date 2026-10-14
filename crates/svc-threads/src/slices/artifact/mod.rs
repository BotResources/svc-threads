//! The artifact capability: the files runners and AIs produce for a thread, in
//! Threads' object storage behind the engine's blob references.
//!
//! - `state`: the canonical artifact, its visibility (the thread's members) and
//!   its facts;
//! - `store`: its row, soft EDA; it lists its blob reference, so deleting it
//!   releases its bytes;
//! - `lifecycle`: the upload request, attaching (by a message naming it), the
//!   end of the upload window, removal with a message or a thread, and the
//!   purge of the bytes once a removal committed;
//! - `view`: the download's gate;
//! - `graphql`: the roots.
//!
//! A runner requests an upload for a thread and receives a time-bounded upload
//! access pinned to the declared size and digest (the engine's verified
//! presigned POST); the artifact is `PENDING`. It becomes `ATTACHED` when a
//! message of the same thread names it in an `ARTIFACT` block and its bytes are
//! present with the declared digest; a `PENDING` artifact never attached is
//! removed at the end of the upload window. A member reads it through a
//! time-bounded read access once the engine verified its bytes; it lives as long
//! as its message.

pub mod graphql;
pub mod lifecycle;
pub mod state;
pub mod store;
pub mod view;

use service_engine::error::EngineError;
use service_engine::graphql::SliceFragment;
use service_engine::{BlobPolicy, Blobs, Engine};

use crate::principal::ThreadsPrincipal;

pub use lifecycle::{delete_all_of_message, delete_all_of_thread};

/// The largest artifact Threads stores. The specification names no bound; this
/// one is to ratify.
pub const MAX_ARTIFACT_BYTES: u64 = 256 << 20;

/// The blob kind of an artifact's bytes.
pub struct ThreadArtifact;

impl Blobs for ThreadArtifact {
    const KIND: &'static str = "thread_artifact";
}

pub fn register(engine: &mut Engine<ThreadsPrincipal>) -> Result<(), EngineError> {
    if !engine.blobs_configured() {
        return Err(EngineError::Config(
            "Threads stores artifacts: its object storage (S3_ENDPOINT, S3_BUCKET, \
             S3_ACCESS_KEY, S3_SECRET_KEY) must be configured"
                .to_string(),
        ));
    }
    // The orphan window of a blob may not be shorter than its upload access
    // (the engine refuses it at bind): both are the upload window.
    engine.register_blobs::<ThreadArtifact>(BlobPolicy {
        max_bytes: MAX_ARTIFACT_BYTES,
        orphan_after: crate::config::installed().upload_window,
    })?;
    lifecycle::install_reader(engine.blob_reader());
    engine.register_mutation::<lifecycle::RequestUpload, _>(lifecycle::request_upload)?;
    engine.register_reaction(
        "threads-artifact-upload-window-ended",
        lifecycle::upload_window_ended,
    )?;
    engine.register_reaction("threads-artifact-bytes-released", lifecycle::bytes_released)?;
    engine.register_schema_slice(SliceFragment::derive::<
        graphql::ArtifactQuery,
        graphql::ArtifactMutation,
        async_graphql::EmptySubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
