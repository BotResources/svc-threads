//! A time-bounded read access to an artifact, signed by the engine behind the
//! visibility of one of the artifact's views: a member's
//! ([`super::view::ArtifactsView`]) or a runner's
//! ([`super::view::RunnerArtifactsView`]). Both paths sign through here, so
//! that they wait for the engine alike, under a bounded number of signings at
//! once ([`SigningPermits`]).

use std::sync::Arc;

use async_graphql::Result;
use service_engine::blobs::Disposition;
use service_engine::view::Projector;
use service_engine::{BlobRef, OrInternal, Query};
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::lifecycle;
use super::state::Artifact;
use super::store::ArtifactStore;
use crate::principal::ThreadsPrincipal;

/// How long a read access waits for the engine to promote verified bytes, and
/// how often it asks.
const PROMOTION_WAIT: std::time::Duration = std::time::Duration::from_secs(3);
const PROMOTION_STEP: std::time::Duration = std::time::Duration::from_millis(100);

/// How many read accesses one read signs at once. The engine signs one
/// reference at a time — a gated load (two statements), then a read of its blob
/// row, each taking a pool connection — and async-graphql resolves the items
/// of a list concurrently: unbounded, a context of N artifacts would ask the
/// pool (20 connections) for N of them at once. Four keeps a context read well
/// under the pool whatever N, at three statements per artifact still.
pub const SIGNING_CONCURRENCY: usize = 4;

const _: () = assert!(SIGNING_CONCURRENCY < 20, "well under the engine's pool");

/// The signings one read may run at once. A context read holds one, shared by
/// all its artifacts; an attempt to sign holds a permit for its statements and
/// releases it before waiting for the engine's promotion.
#[derive(Clone)]
pub struct SigningPermits(Arc<Semaphore>);

impl Default for SigningPermits {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(SIGNING_CONCURRENCY)))
    }
}

impl std::fmt::Debug for SigningPermits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigningPermits")
            .field("available", &self.0.available_permits())
            .finish()
    }
}

/// Delivery state, not part of a view: two views equal whatever their permits.
impl PartialEq for SigningPermits {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SigningPermits {}

impl SigningPermits {
    /// Whether `other` draws from the same permits.
    pub fn shared_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// The signed URL of `artifact`'s bytes behind `reference`, or `None` when the
/// view `V` does not show the artifact to the caller, the reference is not
/// the artifact's, or its bytes are absent.
pub async fn sign<V>(
    query: &Query<'_, ThreadsPrincipal>,
    permits: &SigningPermits,
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
        {
            let _permit = permits.0.acquire().await.or_internal("wait to sign")?;
            if let Some(url) = query
                .download::<V>(&artifact, reference, disposition)
                .await?
            {
                return Ok(Some(url.into_string()));
            }
            if query.load_visible::<V>(&artifact).await?.is_none() {
                return Ok(None);
            }
            let verified = lifecycle::bytes_verified(reference)
                .await
                .or_internal("read an artifact's bytes")?;
            if !verified || tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
        }
        tokio::time::sleep(PROMOTION_STEP).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_more_than_the_bound_sign_at_once_whatever_the_number_waiting() {
        let permits = SigningPermits::default();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let signings = (0..40).map(|_| {
            let (permits, in_flight, most) = (permits.clone(), in_flight.clone(), most.clone());
            tokio::spawn(async move {
                let _permit = permits.0.acquire().await.expect("never closed");
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
            })
        });
        for signing in futures_util::future::join_all(signings).await {
            signing.expect("a signing ends");
        }
        assert_eq!(most.load(Ordering::SeqCst), SIGNING_CONCURRENCY);
    }

    #[test]
    fn a_clone_shares_the_permits_and_a_new_read_has_its_own() {
        let read = SigningPermits::default();
        assert!(read.clone().shared_with(&read));
        assert!(!SigningPermits::default().shared_with(&read));
    }
}
