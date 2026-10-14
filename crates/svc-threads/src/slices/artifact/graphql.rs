//! The artifact roots: a runner's upload request (runner surface) and a
//! member's read access (human surface).

use async_graphql::{Context, Object, Result, SimpleObject};
use service_engine::JsonScalar;
use service_engine::blobs::Disposition;
use uuid::Uuid;

use crate::access;
use crate::fault::not_implemented;
use crate::principal::ThreadsPrincipal;

/// A time-bounded upload access, pinned to the declared size and digest: POST
/// the bytes to `url` as a form carrying `fields`, the file last. Answered once,
/// to the runner that asked; never shown anywhere else.
#[derive(Debug, Clone, SimpleObject)]
pub struct ArtifactUploadAccess {
    pub url: String,
    pub fields: JsonScalar,
}

#[derive(Default)]
pub struct ArtifactQuery;

#[Object]
impl ArtifactQuery {
    /// A time-bounded read access to an artifact of a thread the caller is a
    /// member of, or `null` — exactly as for an artifact that does not exist.
    async fn threads_artifact_download(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        artifact_id: Uuid,
        #[graphql(default_with = "Disposition::Attachment")] disposition: Disposition,
    ) -> Result<Option<String>> {
        access::human_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let _ = (thread_id, artifact_id, disposition);
        Err(not_implemented("threadsArtifactDownload"))
    }
}

#[derive(Default)]
pub struct ArtifactMutation;

#[Object]
impl ArtifactMutation {
    /// A runner requests an upload for a thread, under a UUIDv7 it minted
    /// (`KEY_REUSED` if held): the artifact is `PENDING` until a message of the
    /// thread names it with its bytes present.
    #[allow(clippy::too_many_arguments)]
    async fn threads_request_artifact_upload(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        artifact_id: Uuid,
        name: String,
        media_type: String,
        size: u64,
        sha256: String,
    ) -> Result<ArtifactUploadAccess> {
        access::runner_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let _ = (thread_id, artifact_id, name, media_type, size, sha256);
        Err(not_implemented("threadsRequestArtifactUpload"))
    }
}
