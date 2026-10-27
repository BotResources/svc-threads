//! Closing a `STREAMING` message, once its stream is sealed: what the close
//! commands (`message.complete`, `fail`, `cancel`) and the stream watch share.
//!
//! The sealed draft is judged under the message — its id, agent and author —
//! and against the ids other messages already declared
//! ([`store::held_ids`]), then closed with the library's closure
//! ([`draft::close`]); the `ARTIFACT` annotations it keeps attach their
//! artifacts; the message row, its records, annotations and identities, and its
//! facts are written; its watch ends.
//!
//! What a close that nobody can refuse (a failure, a cancel, the watch) keeps
//! must still be storable: an `ARTIFACT` annotation whose artifact is not ready
//! is dropped. A completion is judged before its seal instead
//! ([`precheck_completion`]), so that it can be refused `ARTIFACT_NOT_READY`
//! and change nothing.

use std::collections::HashSet;

use contract_threads::llm::{AgentId, Author, TurnId};
use service_engine::error::{AccumulatorError, EngineError};
use service_engine::pipeline::Ops;
use uuid::Uuid;

use super::draft::{self, CloseKind, Judged, StreamRefusal, TurnDraft};
use super::state::{Message, MessageAuthor, MessageState};
use super::store;
use super::stream::{LegacyContent, TurnContent};
use super::watch;
use super::{MessageCause, message_cause};
use crate::command::{MessageOutcome, Refusal};
use crate::facts::FactMeta;
use crate::fault::{Fault, corrupt};
use crate::slices::artifact::lifecycle;
use crate::slices::artifact::state::ArtifactState;
use crate::slices::thread::state::UnknownStoredValue;

/// The id, agent and author a streaming message's turn is judged under.
pub fn turn_of(message: &MessageState) -> Result<(TurnId, AgentId, Author), EngineError> {
    match &message.author {
        MessageAuthor::Agent { agent, name } => Ok((message.turn_id()?, *agent, name.clone())),
        MessageAuthor::User { .. } => Err(corrupt(UnknownStoredValue {
            column: "message.author",
            value: "a streaming message without its agent".to_owned(),
        })),
    }
}

/// A stream judged under its message, the ids other messages declared cutting
/// it where they appear. One read of the identities, under the message's
/// thread lock: they do not change while it closes.
pub async fn judge_held(
    ops: &mut Ops<'_>,
    message: &MessageState,
    stream: &TurnDraft,
) -> Result<Judged, Fault> {
    let (turn, agent, author) = turn_of(message)?;
    let first = stream.judge(turn, agent, &author, &HashSet::new())?;
    let held: HashSet<Uuid> =
        store::held_ids(ops.connection(), &first.identities()?, Some(message.id))
            .await?
            .into_iter()
            .collect();
    if held.is_empty() {
        return Ok(first);
    }
    Ok(stream.judge(turn, agent, &author, &held)?)
}

/// Judges a completion on what the stream holds now, before its seal: the
/// artifacts it names must be ready. A refusal here changes nothing. Answers the
/// artifacts, loaded and locked.
pub async fn precheck_completion(
    ops: &mut Ops<'_>,
    message: &MessageState,
    preview: &TurnDraft,
) -> Result<Result<Vec<ArtifactState>, Refusal>, Fault> {
    let judged = judge_held(ops, message, preview).await?;
    let closed = draft::close(judged, CloseKind::Complete)?;
    lifecycle::ready(ops, message.thread_id, &closed.content.artifacts()).await
}

/// The refusal of a stream a 0.4 writer pushed to under the 0.4 accumulator
/// name, if it did: its frames are sealed (gone from the lane) and the stream
/// refused there.
pub async fn legacy_refusal(
    ops: &mut Ops<'_>,
    message: Uuid,
) -> Result<Option<StreamRefusal>, Fault> {
    let Some(seq) = ops
        .accumulated::<LegacyContent>(&message)
        .await?
        .state
        .first
    else {
        return Ok(None);
    };
    sealed_or_already(
        ops.seal_current::<LegacyContent>(&message)
            .await
            .map(|_| ()),
    )?;
    Ok(Some(StreamRefusal {
        seq: Some(seq),
        code: contract_threads::MessageInvalidError::ChunkUnreadable.as_str(),
        object_id: None,
        location: Some("accumulator".to_owned()),
    }))
}

/// Stores a sealed stream's content on its message and ends its watch.
pub async fn store_closed(
    ops: &mut Ops<'_>,
    mut message: MessageState,
    sealed: &TurnDraft,
    kind: CloseKind,
    mut artifacts: Vec<ArtifactState>,
    meta: &FactMeta,
) -> Result<MessageOutcome, Fault> {
    let mut sealed = sealed.clone();
    if let Some(refusal) = legacy_refusal(ops, message.id).await? {
        sealed.refuse(refusal);
    }
    let judged = judge_held(ops, &message, &sealed).await?;
    let mut closed = draft::close(judged, kind)?;
    let missing: Vec<Uuid> = closed
        .content
        .artifacts()
        .into_iter()
        .filter(|id| !artifacts.iter().any(|artifact| artifact.id == *id))
        .collect();
    for id in missing {
        match lifecycle::ready(ops, message.thread_id, &[id]).await? {
            Ok(mut ready) => artifacts.append(&mut ready),
            Err(_) => closed
                .content
                .annotations
                .retain(|placed| placed.annotation.body.artifact() != Some(id)),
        }
    }
    if message.close(closed, meta)?.is_err() {
        return Err(Fault::Engine(EngineError::Service(
            "a message closed twice in one transaction".into(),
        )));
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
        sealed_or_already(ops.seal_current::<TurnContent>(&message).await.map(|_| ()))?;
        legacy_refusal(ops, message).await?;
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
            accumulator: AccumulatorName::from_static("turn_content"),
            high_water: 3,
        });
        assert!(sealed_or_already(Err(already)).is_ok());
        assert!(sealed_or_already(Err(EngineError::Config("down".into()))).is_err());
    }
}
