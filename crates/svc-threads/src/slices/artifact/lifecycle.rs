//! An artifact's life: requested by a runner (a time-bounded upload access
//! pinned to the declared size and digest), attached by a message of its thread
//! that names it once its bytes are present, removed — bytes included — with its
//! message or its thread, or at the end of the upload window when no message
//! named it.

use std::sync::OnceLock;

use chrono::TimeDelta;
use contract_threads::ReasonCode;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::blobs::{Sha256Digest, UploadExpectation};
use service_engine::gate::Reason;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::{Mutation, MutationInput, OneShot, Ops, Reaction};
use service_engine::{BlobReader, BlobRef, PersistenceExt, UploadUrl};
use uuid::Uuid;

use super::state::{ArtifactState, Declared, RemovalReason};
use super::store::{self, ArtifactStore};
use super::{MAX_ARTIFACT_BYTES, ThreadArtifact};
use crate::access;
use crate::command::Refusal;
use crate::facts::{self, FactMeta, FactNoun};
use crate::fault::Fault;
use crate::principal::ThreadsPrincipal;
use crate::slices::thread::store::ThreadStore;

/// An upload request whose name or media type is blank.
pub const ARTIFACT_DECLARATION_INVALID: Reason = Reason::new("ARTIFACT_DECLARATION_INVALID");
/// A declared digest that is not 64 lowercase hexadecimal characters.
pub const ARTIFACT_DIGEST_INVALID: Reason = Reason::new("ARTIFACT_DIGEST_INVALID");
/// An artifact id that is not a UUIDv7.
pub const ARTIFACT_ID_NOT_UUIDV7: Reason = Reason::new("ARTIFACT_ID_NOT_UUIDV7");
/// A declared size above what Threads stores.
pub const ARTIFACT_TOO_LARGE: Reason = Reason::new("ARTIFACT_TOO_LARGE");

/// The object storage reader, for the presence and digest of an artifact's
/// bytes: bound once at registration, read by the gestures that attach.
static READER: OnceLock<BlobReader> = OnceLock::new();

pub fn install_reader(reader: BlobReader) {
    let _ = READER.set(reader);
}

fn reader() -> Result<&'static BlobReader, Fault> {
    READER.get().ok_or_else(|| {
        Fault::Engine(service_engine::error::EngineError::Config(
            "the artifact capability is not registered".into(),
        ))
    })
}

// ── the upload request ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RequestUpload {
    pub thread_id: Uuid,
    pub artifact_id: Uuid,
    pub name: String,
    pub media_type: String,
    pub size: u64,
    pub sha256: String,
}

impl MutationInput for RequestUpload {
    type Output = OneShot<UploadUrl>;
    type Error = Fault;
    const NAME: &'static str = "request_artifact_upload";
}

/// A runner requests an upload for a thread under a UUIDv7 it minted
/// (`KEY_REUSED` if held, refused by the engine's create): the artifact is
/// `PENDING`, and its removal is scheduled for the end of the upload window.
pub fn request_upload<'m>(
    cx: &'m mut Mutation<'m, ThreadsPrincipal>,
    input: RequestUpload,
) -> BoxFuture<'m, Result<OneShot<UploadUrl>, Fault>> {
    Box::pin(async move {
        access::runner_surface(cx.principal()).require()?;
        if input.artifact_id.get_version_num() != 7 {
            return Err(ARTIFACT_ID_NOT_UUIDV7.into());
        }
        if input.name.trim().is_empty() || input.media_type.trim().is_empty() {
            return Err(ARTIFACT_DECLARATION_INVALID.into());
        }
        let sha256 = contract_threads::SealDigest::parse(&input.sha256)
            .map_err(|_| Fault::from(ARTIFACT_DIGEST_INVALID))?;
        let digest = Sha256Digest::from_hex(sha256.as_str())
            .map_err(|_| Fault::from(ARTIFACT_DIGEST_INVALID))?;
        if input.size > MAX_ARTIFACT_BYTES {
            return Err(ARTIFACT_TOO_LARGE.into());
        }
        if ThreadStore::load(cx.connection(), &input.thread_id)
            .await?
            .is_none()
        {
            return Err(access::THREAD_NOT_FOUND.into());
        }
        let blob = cx.blob_verified::<ThreadArtifact>(
            input.name.clone(),
            input.media_type.clone(),
            UploadExpectation::new(input.size, digest),
        )?;
        let now = cx.now();
        let meta = FactMeta::of_principal(cx.principal(), now.as_datetime());
        let artifact = ArtifactState::requested(
            Declared {
                id: input.artifact_id,
                thread_id: input.thread_id,
                name: input.name,
                media_type: input.media_type,
                size: i64::try_from(input.size).map_err(|_| Fault::from(ARTIFACT_TOO_LARGE))?,
                sha256: sha256.as_str().to_owned(),
                blob_ref: blob.reference().as_uuid(),
            },
            now.as_datetime(),
            &meta,
        )?;
        cx.create(&artifact).await?;
        let window = crate::config::installed().upload_window;
        let at = now
            + TimeDelta::from_std(window).map_err(|_| {
                Fault::Engine(service_engine::error::EngineError::Config(
                    "the upload window does not fit a date".into(),
                ))
            })?;
        cx.schedule_at(
            at,
            UploadWindowEnded {
                artifact_id: artifact.id,
            },
        )?;
        Ok(OneShot(blob.upload_url()))
    })
}

// ── the end of the upload window ────────────────────────────────────────────

/// Threads' own scheduled message: the upload window of an artifact ended.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadWindowEnded {
    pub artifact_id: Uuid,
}

impl ReactionMessage for UploadWindowEnded {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(contract_threads::coords::command_coords(
            "artifact",
            "upload_window_ended",
        ))
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

/// A `PENDING` artifact whose upload window ended is removed, its bytes with
/// it. Judged on the artifact's own date, so an early or repeated message
/// changes nothing.
pub fn upload_window_ended<'r>(
    cx: &'r mut Reaction<'r>,
    message: UploadWindowEnded,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let Some(thread) = artifact_thread(cx.connection(), message.artifact_id).await? else {
            return Ok(());
        };
        // The thread first: the one lock order of the service.
        cx.load::<crate::slices::thread::ThreadState>(&thread)
            .await?;
        let Some(artifact) = cx.load::<ArtifactState>(&message.artifact_id).await? else {
            return Ok(());
        };
        let window =
            TimeDelta::from_std(crate::config::installed().upload_window).unwrap_or(TimeDelta::MAX);
        let ended = artifact.created_at + window <= cx.now().as_datetime();
        if !ended || !artifact.attachable_to(artifact.thread_id) {
            return Ok(());
        }
        let meta = reaction_meta(cx);
        remove(cx, &artifact, Some((RemovalReason::NeverAttached, &meta))).await?;
        purge_after_commit(cx, &[artifact])
    })
}

fn reaction_meta(cx: &Reaction<'_>) -> FactMeta {
    FactMeta {
        actor: cx
            .actor()
            .copied()
            .unwrap_or_else(|| ThreadsPrincipal::threads_itself().actor()),
        acting_user_id: None,
        impersonator_id: None,
        correlation_id: cx.correlation_id().unwrap_or_else(|| cx.message_id()),
        causation_id: Some(cx.message_id()),
        occurred_at: cx.now().as_datetime(),
    }
}

/// Whether the bytes behind a reference are present at their declared digest,
/// as object storage answers now.
pub async fn bytes_verified(reference: BlobRef) -> Result<bool, Fault> {
    Ok(reader()?
        .head(reference)
        .await?
        .and_then(|head| head.verified())
        == Some(true))
}

// ── attaching ───────────────────────────────────────────────────────────────

/// The artifacts a message of `thread` names, loaded and locked, when every one
/// is that thread's, still `PENDING`, and its bytes are present at the declared
/// digest; `ARTIFACT_NOT_READY` naming the first that is not. Writes nothing.
pub async fn ready(
    ops: &mut Ops<'_>,
    thread: Uuid,
    named: &[Uuid],
) -> Result<Result<Vec<ArtifactState>, Refusal>, Fault> {
    let mut ids: Vec<Uuid> = named.to_vec();
    ids.sort();
    ids.dedup();
    if ids.is_empty() {
        return Ok(Ok(Vec::new()));
    }
    let loaded = ops.load_many::<ArtifactState>(&ids).await?;
    let not_ready = |id: Uuid| Refusal::new(ReasonCode::ArtifactNotReady).with("artifact_id", id);
    for id in &ids {
        let Some(artifact) = loaded.iter().find(|artifact| artifact.id == *id) else {
            return Ok(Err(not_ready(*id)));
        };
        if !artifact.attachable_to(thread) {
            return Ok(Err(not_ready(*id)));
        }
        if !bytes_verified(BlobRef(artifact.blob_ref)).await? {
            return Ok(Err(not_ready(*id)));
        }
    }
    Ok(Ok(loaded))
}

/// Attaches ready artifacts to `message`, which exists in this transaction.
pub async fn attach(
    ops: &mut Ops<'_>,
    artifacts: Vec<ArtifactState>,
    message: Uuid,
    meta: &FactMeta,
) -> Result<(), Fault> {
    for mut artifact in artifacts {
        artifact.attach(message, meta)?;
        ops.save(&artifact).await?;
    }
    Ok(())
}

// ── removing ────────────────────────────────────────────────────────────────

/// Deletes an artifact through the pipeline, so that the engine releases its
/// bytes in the same transaction; records its `Removed` fact when it has a
/// reason of its own (a thread's `Deleted` fact covers the thread's).
async fn remove(
    ops: &mut Ops<'_>,
    artifact: &ArtifactState,
    removal: Option<(RemovalReason, &FactMeta)>,
) -> Result<(), Fault> {
    if let Some((reason, meta)) = removal {
        let fact = artifact.removed_fact(reason, meta)?;
        facts::append(
            ops.connection(),
            FactNoun::Artifact,
            artifact.id,
            artifact.version + 1,
            &[fact],
        )
        .await?;
    }
    ops.delete(artifact).await?;
    Ok(())
}

/// Removes every artifact of a thread being deleted. The thread is loaded and
/// locked by the caller.
pub async fn delete_all_of_thread(ops: &mut Ops<'_>, thread: Uuid) -> Result<(), Fault> {
    let ids = store::ids_of(ops.connection(), thread, None).await?;
    let artifacts = ops.load_many::<ArtifactState>(&ids).await?;
    for artifact in &artifacts {
        remove(ops, artifact, None).await?;
    }
    purge_after_commit(ops, &artifacts)
}

/// Removes the artifacts of a message its author deleted.
pub async fn delete_all_of_message(
    ops: &mut Ops<'_>,
    thread: Uuid,
    message: Uuid,
    meta: &FactMeta,
) -> Result<(), Fault> {
    let ids = store::ids_of(ops.connection(), thread, Some(message)).await?;
    let artifacts = ops.load_many::<ArtifactState>(&ids).await?;
    for artifact in &artifacts {
        remove(ops, artifact, Some((RemovalReason::MessageDeleted, meta))).await?;
    }
    purge_after_commit(ops, &artifacts)
}

// ── purging the bytes ───────────────────────────────────────────────────────
//
// ENGINE GAP (br-service-engine v0.4.1): deleting an artifact through the
// pipeline releases its blob in the same transaction, but the engine's reaper
// deletes a released blob only past its kind's orphan window — at least the
// upload access's lifetime, 15 minutes by default — and offers no purge on
// commit. Threads therefore schedules its own message in the deleting
// transaction: it is published only once that transaction committed, and its
// reaction purges the released bytes through the engine's blob reader. The
// engine would need a released reference to be purged at commit.

/// Threads' own scheduled message: blobs whose artifacts were deleted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BytesReleased {
    pub blob_refs: Vec<Uuid>,
}

impl ReactionMessage for BytesReleased {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(contract_threads::coords::command_coords(
            "artifact",
            "bytes_released",
        ))
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

fn purge_after_commit(ops: &mut Ops<'_>, artifacts: &[ArtifactState]) -> Result<(), Fault> {
    if artifacts.is_empty() {
        return Ok(());
    }
    let now = ops.now();
    ops.schedule_at(
        now,
        BytesReleased {
            blob_refs: artifacts.iter().map(|artifact| artifact.blob_ref).collect(),
        },
    )?;
    Ok(())
}

/// Purges released bytes. A reference some artifact still holds is never
/// purged, so a forged or repeated message changes nothing.
pub fn bytes_released<'r>(
    cx: &'r mut Reaction<'r>,
    message: BytesReleased,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let held: Vec<Uuid> =
            sqlx::query_scalar("SELECT blob_ref FROM threads.artifact WHERE blob_ref = ANY($1)")
                .bind(&message.blob_refs)
                .fetch_all(cx.connection())
                .await?;
        let released: Vec<BlobRef> = message
            .blob_refs
            .iter()
            .filter(|reference| !held.contains(reference))
            .map(|reference| BlobRef(*reference))
            .collect();
        reader()?.purge_references(&released).await?;
        Ok(())
    })
}

/// The artifact itself, unlocked: its thread, for a reaction that must lock the
/// thread first.
pub async fn artifact_thread(
    conn: &mut sqlx::PgConnection,
    artifact: Uuid,
) -> Result<Option<Uuid>, Fault> {
    Ok(ArtifactStore::load(conn, &artifact)
        .await?
        .map(|artifact| artifact.thread_id))
}
