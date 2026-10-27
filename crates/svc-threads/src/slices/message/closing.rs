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
//! and change nothing — and it never drops an annotation: one its seal reaches
//! that the stream did not hold yet when it was judged (the stream's chunks
//! were still being stored) is a retried fault ([`UnjudgedArtifact`]), never a
//! silent loss.
//!
//! Every reader of a stream — the close, the members' view, the agent context
//! — judges it the same way, with the ids other messages declared
//! ([`judge_streams`]): what members see is what the message closes to.

use std::collections::HashSet;

use contract_threads::llm::{AgentId, Author, TurnId};
use service_engine::error::{AccumulatorError, EngineError};
use service_engine::pipeline::Ops;
use sqlx::PgConnection;
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

/// Streams judged under their messages ([`TurnDraft::judge`]), the ids other
/// messages declared cutting each where they appear: one read of the
/// identities for every stream, whatever their number.
pub async fn judge_streams(
    conn: &mut PgConnection,
    streams: &[(&MessageState, &TurnDraft)],
) -> Result<Vec<Judged>, EngineError> {
    let mut judged = Vec::with_capacity(streams.len());
    let mut declared = Vec::new();
    for (message, stream) in streams {
        let (turn, agent, author) = turn_of(message)?;
        let first = stream.judge(turn, agent, &author, &HashSet::new())?;
        declared.extend(first.identities()?);
        judged.push(first);
    }
    if declared.is_empty() {
        return Ok(judged);
    }
    let holders = store::holders(conn, &declared).await?;
    for ((message, stream), first) in streams.iter().zip(judged.iter_mut()) {
        let held: HashSet<Uuid> = holders
            .iter()
            .filter(|(_, holder)| *holder != message.id)
            .map(|(id, _)| *id)
            .collect();
        let clashes = first.identities()?.into_iter().any(|id| held.contains(&id));
        if clashes {
            let (turn, agent, author) = turn_of(message)?;
            *first = stream.judge(turn, agent, &author, &held)?;
        }
    }
    Ok(judged)
}

/// A stream judged under its message, as [`judge_streams`] judges it. Read
/// under the message's thread lock: the identities do not change while it
/// closes.
pub async fn judge_held(
    ops: &mut Ops<'_>,
    message: &MessageState,
    stream: &TurnDraft,
) -> Result<Judged, Fault> {
    let mut judged = judge_streams(ops.connection(), &[(message, stream)]).await?;
    judged
        .pop()
        .ok_or_else(|| Fault::Engine(EngineError::Service("a stream judged to nothing".into())))
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

/// A completion whose seal reached an `ARTIFACT` annotation the stream did not
/// hold yet when the completion was judged ([`precheck_completion`] reads the
/// chunks stored so far, the seal every chunk up to `last_seq`), its artifact
/// not ready. A fault, retried: the transaction rolls back, the seal with it,
/// and the redelivery judges the completion on a stream that holds the
/// annotation — refused `ARTIFACT_NOT_READY`, or stored once the artifact is
/// ready.
#[derive(Debug, thiserror::Error)]
#[error("artifact_not_ready_past_the_judged_stream message_id={message} artifact_id={artifact}")]
pub struct UnjudgedArtifact {
    pub message: Uuid,
    pub artifact: Uuid,
}

/// What a close does with the `ARTIFACT` annotations of its sealed stream whose
/// artifact is not ready (`not_ready`, sorted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unready {
    /// Every artifact named is ready.
    None,
    /// A failure, a cancel, the watch: nobody can refuse them, and what they
    /// keep must be storable — the annotations go.
    Drop(Vec<Uuid>),
    /// A completion: never stored without an annotation it holds.
    Retry(Uuid),
}

pub fn unready(kind: CloseKind, not_ready: &[Uuid]) -> Unready {
    match (kind, not_ready.first()) {
        (_, None) => Unready::None,
        (CloseKind::Complete, Some(first)) => Unready::Retry(*first),
        (CloseKind::Fail | CloseKind::Cancel | CloseKind::Watch, Some(_)) => {
            Unready::Drop(not_ready.to_vec())
        }
    }
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
    let (mut ready, not_ready) = lifecycle::sort_ready(ops, message.thread_id, &missing).await?;
    artifacts.append(&mut ready);
    match unready(kind, &not_ready) {
        Unready::None => {}
        Unready::Drop(gone) => closed.content.annotations.retain(|placed| {
            placed
                .annotation
                .body
                .artifact()
                .is_none_or(|artifact| !gone.contains(&artifact))
        }),
        Unready::Retry(artifact) => {
            return Err(Fault::Engine(EngineError::Service(Box::new(
                UnjudgedArtifact {
                    message: message.id,
                    artifact,
                },
            ))));
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
    fn a_completion_never_drops_an_artifact_annotation_and_a_close_nobody_refuses_does() {
        let (first, second) = (Uuid::now_v7(), Uuid::now_v7());
        for kind in [
            CloseKind::Complete,
            CloseKind::Fail,
            CloseKind::Cancel,
            CloseKind::Watch,
        ] {
            assert_eq!(unready(kind, &[]), Unready::None, "{kind:?}");
        }
        assert_eq!(
            unready(CloseKind::Complete, &[first, second]),
            Unready::Retry(first)
        );
        for kind in [CloseKind::Fail, CloseKind::Cancel, CloseKind::Watch] {
            assert_eq!(
                unready(kind, &[first, second]),
                Unready::Drop(vec![first, second]),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn the_retried_completion_is_a_fault_the_pipeline_retries() {
        use service_engine::inbound::{Disposition, ReactionError};
        let fault = Fault::Engine(EngineError::Service(Box::new(UnjudgedArtifact {
            message: Uuid::now_v7(),
            artifact: Uuid::now_v7(),
        })));
        assert_eq!(fault.disposition(), Disposition::Retry);
    }

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
