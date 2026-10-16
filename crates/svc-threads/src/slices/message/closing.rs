//! Closing a `STREAMING` message, once its stream is sealed: what the close
//! commands (`message.complete`, `fail`, `cancel`) and the stream watch share.
//!
//! The sealed draft closes to its stored content ([`super::draft::close`]),
//! judged against the tool call ids the branches through the message already
//! use ([`store::used_call_ids`]); the `ARTIFACT` blocks it keeps attach their
//! artifacts; the message row, its items and blocks, and its facts are
//! written; its watch ends.
//!
//! What a close that nobody can refuse (a failure, a cancel, the watch) keeps
//! must still be storable: an `ARTIFACT` block whose artifact is not ready is
//! dropped, and a block id some other message already holds drops the content
//! entirely — the message is then stored without items, which no conversation
//! shows: a completion then fails (`block_id_reused`), since a `COMPLETED`
//! message always holds its turn. A completion is judged before its seal instead
//! ([`precheck_completion`]), so that it can be refused and change nothing.

use contract_threads::{BlockKind, ReasonCode};
use service_engine::error::{AccumulatorError, EngineError};
use service_engine::pipeline::Ops;
use uuid::Uuid;

use super::content::{UsedCallIds, named_artifacts};
use super::draft::{self, CloseKind, MessageDraft};
use super::state::{Message, MessageState, StoredItem};
use super::store;
use super::stream::MessageContent;
use super::watch;
use super::{MessageCause, message_cause};
use crate::command::{MessageOutcome, Refusal};
use crate::facts::FactMeta;
use crate::fault::Fault;
use crate::slices::artifact::lifecycle;
use crate::slices::artifact::state::ArtifactState;

/// The tool call ids the branches through a closing message already use, read
/// at most once per close and only for a stream that calls tools: the
/// message's thread is locked, so they do not change while it closes.
#[derive(Default)]
pub struct BranchCalls {
    loaded: Option<UsedCallIds>,
    none: UsedCallIds,
}

impl BranchCalls {
    async fn for_draft(
        &mut self,
        ops: &mut Ops<'_>,
        message: &MessageState,
        draft: &MessageDraft,
    ) -> Result<&UsedCallIds, Fault> {
        if self.loaded.is_none() && draft.calls_tools() {
            self.loaded = Some(
                store::used_call_ids(ops.connection(), message.parent_message_id, message.id)
                    .await?,
            );
        }
        Ok(self.loaded.as_ref().unwrap_or(&self.none))
    }
}

/// Judges a completion on the content the stream holds now, before its seal: the
/// artifacts it names must be ready and its block ids unused. A refusal here
/// changes nothing. Answers the artifacts, loaded and locked.
pub async fn precheck_completion(
    ops: &mut Ops<'_>,
    message: &MessageState,
    preview: &MessageDraft,
    calls: &mut BranchCalls,
) -> Result<Result<Vec<ArtifactState>, Refusal>, Fault> {
    let used = calls.for_draft(ops, message, preview).await?;
    let closed = draft::close(preview, CloseKind::Complete, used);
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

/// What a close already knows when it stores: the artifacts a completion
/// judged ready, the branch's calls it read.
#[derive(Default)]
pub struct Prior {
    pub artifacts: Vec<ArtifactState>,
    pub calls: BranchCalls,
}

/// Stores a sealed stream's content on its message and ends its watch.
/// `fallback_reason` names why the message failed when its stream does not
/// (the watch's silent writer), before the cut its close made.
pub async fn store_closed(
    ops: &mut Ops<'_>,
    mut message: MessageState,
    sealed: &MessageDraft,
    kind: CloseKind,
    mut prior: Prior,
    fallback_reason: Option<&'static str>,
    meta: &FactMeta,
) -> Result<MessageOutcome, Fault> {
    let used = prior.calls.for_draft(ops, &message, sealed).await?;
    let mut closed = draft::close(sealed, kind, used);
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
        closed = closed.without_content();
    }
    let mut artifacts = prior.artifacts;
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
    let reason = closed.failure.or(fallback_reason).or(closed.cut);
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

/// Seals the stream of every message of `thread` still `STREAMING`, in the
/// transaction that deletes the thread: its stored chunks go with the seal, and
/// a chunk pushed after it is refused on every pod, so nothing of a deleted
/// thread's content survives in the accumulated lane. The thread is loaded and
/// locked by the caller; the messages go with it by cascade.
pub async fn seal_streams_of_thread(ops: &mut Ops<'_>, thread: Uuid) -> Result<(), Fault> {
    let streaming: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM threads.message WHERE thread_id = $1 AND status = 'STREAMING' ORDER BY id",
    )
    .bind(thread)
    .fetch_all(ops.connection())
    .await?;
    for message in streaming {
        sealed_or_already(
            ops.seal_current::<MessageContent>(&message)
                .await
                .map(|_| ()),
        )?;
    }
    Ok(())
}

/// A stream already sealed is as sealed as it gets: a lost race, not a fault.
fn sealed_or_already(outcome: Result<(), EngineError>) -> Result<(), Fault> {
    match outcome {
        Ok(()) | Err(EngineError::Accumulator(AccumulatorError::AlreadySealed { .. })) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use service_engine::name::AccumulatorName;

    use super::*;

    #[test]
    fn a_stream_already_sealed_counts_as_sealed_and_any_other_error_is_a_fault() {
        assert!(sealed_or_already(Ok(())).is_ok());
        let already = EngineError::Accumulator(AccumulatorError::AlreadySealed {
            accumulator: AccumulatorName::from_static("message_content"),
            high_water: 3,
        });
        assert!(sealed_or_already(Err(already)).is_ok());
        assert!(sealed_or_already(Err(EngineError::Config("down".into()))).is_err());
    }
}
