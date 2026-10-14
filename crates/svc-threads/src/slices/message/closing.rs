//! Closing a `STREAMING` message, once its stream is sealed: what the close
//! commands (`message.complete`, `fail`, `cancel`) and the stream watch share.
//!
//! The sealed draft closes to its stored content ([`super::draft::close`]);
//! the `ARTIFACT` blocks it keeps attach their artifacts; the message row, its
//! items and blocks, and its facts are written; its watch ends.
//!
//! What a close that nobody can refuse (a failure, a cancel, the watch) keeps
//! must still be storable: an `ARTIFACT` block whose artifact is not ready is
//! dropped, and a block id some other message already holds drops the content
//! entirely — the message is then stored without items, which no conversation
//! shows. A completion is judged before its seal instead
//! ([`precheck_completion`]), so that it can be refused and change nothing.

use contract_threads::{BlockKind, ReasonCode};
use service_engine::pipeline::Ops;
use uuid::Uuid;

use super::content::named_artifacts;
use super::draft::{self, CloseKind, MessageDraft};
use super::state::{Message, MessageState, StoredItem};
use super::store;
use super::watch;
use super::{MessageCause, message_cause};
use crate::command::{MessageOutcome, Refusal};
use crate::facts::FactMeta;
use crate::fault::Fault;
use crate::slices::artifact::lifecycle;
use crate::slices::artifact::state::ArtifactState;

/// Judges a completion on the content the stream holds now, before its seal: the
/// artifacts it names must be ready and its block ids unused. A refusal here
/// changes nothing. Answers the artifacts, loaded and locked.
pub async fn precheck_completion(
    ops: &mut Ops<'_>,
    message: &MessageState,
    preview: &MessageDraft,
) -> Result<Result<Vec<ArtifactState>, Refusal>, Fault> {
    let closed = draft::close(preview, CloseKind::Complete);
    let ids: Vec<Uuid> = closed
        .items
        .iter()
        .flat_map(|item| item.blocks.iter().map(|block| block.id))
        .collect();
    let held = store::held_block_ids(ops.connection(), &ids).await?;
    if !held.is_empty() {
        return Ok(Err(
            Refusal::new(ReasonCode::KeyReused).with("block_ids", held)
        ));
    }
    let named = named_artifacts(closed.items.iter().flat_map(|item| item.blocks.iter()));
    lifecycle::ready(ops, message.thread_id, &named).await
}

/// Stores a sealed stream's content on its message and ends its watch.
/// `prechecked` holds the artifacts a completion already judged ready;
/// `fallback_reason` names why the message failed when its stream does not
/// (the watch's silent writer).
pub async fn store_closed(
    ops: &mut Ops<'_>,
    mut message: MessageState,
    sealed: &MessageDraft,
    kind: CloseKind,
    prechecked: Vec<ArtifactState>,
    fallback_reason: Option<&'static str>,
    meta: &FactMeta,
) -> Result<MessageOutcome, Fault> {
    let mut closed = draft::close(sealed, kind);
    let held = store::held_block_ids(
        ops.connection(),
        &closed
            .items
            .iter()
            .flat_map(|item| item.blocks.iter().map(|block| block.id))
            .collect::<Vec<_>>(),
    )
    .await?;
    if !held.is_empty() {
        closed.items.clear();
    }
    let mut artifacts = prechecked;
    let named = named_artifacts(closed.items.iter().flat_map(|item| item.blocks.iter()));
    let missing: Vec<Uuid> = named
        .iter()
        .copied()
        .filter(|id| !artifacts.iter().any(|artifact| artifact.id == *id))
        .collect();
    for id in missing {
        match lifecycle::ready(ops, message.thread_id, &[id]).await? {
            Ok(mut ready) => artifacts.append(&mut ready),
            Err(_) => drop_artifact_blocks(&mut closed.items, id),
        }
    }
    let reason = closed.failure.or(fallback_reason);
    match message.close(closed, reason, meta)? {
        Ok(()) => {}
        Err(_) => {
            return Err(Fault::Engine(service_engine::error::EngineError::Service(
                "a message closed twice in one transaction".into(),
            )));
        }
    }
    ops.save(&message).await?;
    lifecycle::attach(ops, artifacts, message.id, meta).await?;
    watch::end(ops, message.id).await?;
    ops.impact_caused::<Message, _>(
        &message.id,
        message_cause(MessageCause::Closed, message.thread_id),
    )?;
    Ok(MessageOutcome {
        status: message.status,
        deleted: false,
    })
}

/// Drops the `ARTIFACT` blocks naming `artifact`, renumbering their items.
fn drop_artifact_blocks(items: &mut [StoredItem], artifact: Uuid) {
    for item in items.iter_mut() {
        item.blocks.retain(|block| {
            block.kind != BlockKind::Artifact
                || block.content.get("artifact_id").and_then(|id| id.as_str())
                    != Some(artifact.to_string().as_str())
        });
        for (index, block) in item.blocks.iter_mut().enumerate() {
            block.index = index as i32;
        }
    }
}
