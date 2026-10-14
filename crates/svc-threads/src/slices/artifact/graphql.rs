//! The artifact roots: a runner's upload request (runner surface) and a
//! member's read access (human surface).

use async_graphql::{Context, Object, Result, SimpleObject};
use service_engine::blobs::Disposition;
use service_engine::{BlobRef, JsonScalar, OrInternal, Query};
use uuid::Uuid;

use super::lifecycle::RequestUpload;
use super::view::ArtifactsView;
use crate::access;
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
    /// A time-bounded read access to an attached artifact of a thread the
    /// caller is a member of, or `null` — exactly as for an artifact that does
    /// not exist, or whose bytes are not verified yet.
    async fn threads_artifact_download(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        artifact_id: Uuid,
        #[graphql(default_with = "Disposition::Attachment")] disposition: Disposition,
    ) -> Result<Option<String>> {
        access::human_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let query = Query::<ThreadsPrincipal>::new(ctx)?;
        let Some(artifact) = query.load_visible::<ArtifactsView>(&artifact_id).await? else {
            return Ok(None);
        };
        if artifact.thread_id != thread_id || artifact.message_id.is_none() {
            return Ok(None);
        }
        Ok(query
            .download::<ArtifactsView>(&artifact_id, BlobRef(artifact.blob_ref), disposition)
            .await?
            .map(|url| url.into_string()))
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
        let upload = service_engine::execute::<ThreadsPrincipal, RequestUpload>(
            ctx,
            RequestUpload {
                thread_id,
                artifact_id,
                name,
                media_type,
                size,
                sha256,
            },
        )
        .await?
        .into_inner();
        let (url, fields) = upload.into_parts();
        let fields: serde_json::Map<String, serde_json::Value> = fields
            .into_iter()
            .map(|(key, value)| (key, serde_json::Value::String(value)))
            .collect();
        let fields = serde_json::to_value(fields).or_internal("encode the upload fields")?;
        Ok(ArtifactUploadAccess {
            url,
            fields: async_graphql::Json(fields),
        })
    }
}
