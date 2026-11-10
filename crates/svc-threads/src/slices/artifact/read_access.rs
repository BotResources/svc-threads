//! A time-bounded read access to an artifact, signed by the engine behind the
//! visibility of one of the artifact's views: a member's
//! ([`super::view::ArtifactsView`]) or a runner's
//! ([`super::view::RunnerArtifactsView`]). Both paths sign through here, so
//! that they wait for the engine alike.

use async_graphql::Result;
use service_engine::blobs::Disposition;
use service_engine::view::Projector;
use service_engine::{BlobRef, OrInternal, Query};
use uuid::Uuid;

use super::lifecycle;
use super::state::Artifact;
use super::store::ArtifactStore;
use crate::principal::ThreadsPrincipal;

/// How long a read access waits for the engine to promote verified bytes, and
/// how often it asks.
const PROMOTION_WAIT: std::time::Duration = std::time::Duration::from_secs(3);
const PROMOTION_STEP: std::time::Duration = std::time::Duration::from_millis(100);

/// The signed URL of `artifact`'s bytes behind `reference`, or `None` when the
/// view `V` does not show the artifact to the caller, the reference is not
/// the artifact's, or its bytes are absent.
pub async fn sign<V>(
    query: &Query<'_, ThreadsPrincipal>,
    artifact: Uuid,
    reference: BlobRef,
    disposition: Disposition,
) -> Result<Option<String>>
where
    V: Projector<Principal = ThreadsPrincipal, Noun = Artifact, Store = ArtifactStore>,
{
    // ENGINE GAP (br-service-engine v0.4.1): the engine presigns a verified
    // upload only once its reaper promoted it, and offers no promotion on
    // demand. An attached artifact's bytes were verified when its message
    // attached it, so its read access waits for that promotion — within a
    // beat, the reaper sweeping on every beat (`crate::boot`) — rather than
    // answering as for an artifact that does not exist. Bounded: past it,
    // `null`. The engine would need a download of a reference whose storage
    // HEAD verifies, or a promotion on demand.
    let deadline = tokio::time::Instant::now() + PROMOTION_WAIT;
    loop {
        if let Some(url) = query
            .download::<V>(&artifact, reference, disposition)
            .await?
        {
            return Ok(Some(url.into_string()));
        }
        let verified = lifecycle::bytes_verified(reference)
            .await
            .or_internal("read an artifact's bytes")?;
        if !verified || tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(PROMOTION_STEP).await;
    }
}
