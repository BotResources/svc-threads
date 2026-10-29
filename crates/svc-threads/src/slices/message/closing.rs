use std::collections::HashSet;

use contract_threads::MessageStatus;
use contract_threads::llm::{AgentId, Author, TurnId};
use service_engine::error::{AccumulatorError, EngineError};
use service_engine::pipeline::Ops;
use sqlx::PgConnection;
use uuid::Uuid;

use super::close;
use super::closure::Closing;
use super::draft::{Judged, StreamRefusal, TurnDraft};
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
use crate::slices::thread::state::{Thread, ThreadState, UnknownStoredValue};
use crate::slices::thread::{ThreadCause, title_job};

pub fn turn_of(message: &MessageState) -> Result<(TurnId, AgentId, Author), EngineError> {
    match &message.author {
        MessageAuthor::Agent { agent, name } => Ok((message.turn_id()?, *agent, name.clone())),
        MessageAuthor::User { .. } => Err(corrupt(UnknownStoredValue {
            column: "message.author",
            value: "a streaming message without its agent".to_owned(),
        })),
    }
}

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

pub async fn precheck_completion(
    ops: &mut Ops<'_>,
    message: &MessageState,
    preview: &TurnDraft,
    closing: &Closing,
) -> Result<Result<Vec<ArtifactState>, Refusal>, Fault> {
    let judged = judge_held(ops, message, preview).await?;
    let closed = close::close(judged, closing)?;
    lifecycle::ready(ops, message.thread_id, &closed.content.artifacts()).await
}

#[derive(Debug, thiserror::Error)]
#[error("artifact_not_ready_past_the_judged_stream message_id={message} artifact_id={artifact}")]
pub struct UnjudgedArtifact {
    pub message: Uuid,
    pub artifact: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unready {
    None,
    Drop(Vec<Uuid>),
    Retry(Uuid),
}

pub fn unready(closing: &Closing, not_ready: &[Uuid]) -> Unready {
    match (closing.strict(), not_ready.first()) {
        (_, None) => Unready::None,
        (true, Some(first)) => Unready::Retry(*first),
        (false, Some(_)) => Unready::Drop(not_ready.to_vec()),
    }
}

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

pub async fn store_closed(
    ops: &mut Ops<'_>,
    mut message: MessageState,
    thread: &mut ThreadState,
    sealed: &TurnDraft,
    closing: &Closing,
    mut artifacts: Vec<ArtifactState>,
    meta: &FactMeta,
) -> Result<MessageOutcome, Fault> {
    let mut sealed = sealed.clone();
    if let Some(refusal) = legacy_refusal(ops, message.id).await? {
        sealed.refuse(refusal);
    }
    let judged = judge_held(ops, &message, &sealed).await?;
    let mut closed = close::close(judged, closing)?;
    let missing: Vec<Uuid> = closed
        .content
        .artifacts()
        .into_iter()
        .filter(|id| !artifacts.iter().any(|artifact| artifact.id == *id))
        .collect();
    let (mut ready, not_ready) = lifecycle::sort_ready(ops, message.thread_id, &missing).await?;
    artifacts.append(&mut ready);
    match unready(closing, &not_ready) {
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
    if message.status == MessageStatus::Completed
        && title_job::trigger(ops, thread, meta.occurred_at, meta.acting_user_id).await?
    {
        ops.save(&*thread).await?;
        ops.impact_caused::<Thread, _>(&thread.id, ThreadCause::MessageStored)?;
    }
    ops.impact_caused::<Message, _>(
        &message.id,
        message_cause(MessageCause::Closed, message.thread_id),
    )?;
    Ok(MessageOutcome {
        status: message.status,
        deleted: false,
    })
}

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

fn sealed_or_already(outcome: Result<(), EngineError>) -> Result<(), Fault> {
    match outcome {
        Ok(()) | Err(EngineError::Accumulator(AccumulatorError::AlreadySealed { .. })) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use contract_threads::llm::ClosureReason;
    use service_engine::name::AccumulatorName;

    use super::super::close::CloseKind;

    use super::*;

    #[test]
    fn a_closing_that_must_be_whole_never_drops_an_artifact_annotation_and_one_nobody_refuses_does()
    {
        let (first, second) = (Uuid::now_v7(), Uuid::now_v7());
        let whole = [
            Closing::of(CloseKind::Complete),
            Closing::naming(CloseKind::Fail, Some(ClosureReason::completed())),
            Closing::naming(CloseKind::Fail, Some(ClosureReason::budget_exhausted())),
        ];
        let droppable = [
            Closing::of(CloseKind::Fail),
            Closing::of(CloseKind::Cancel),
            Closing::of(CloseKind::Watch),
            Closing::naming(CloseKind::Fail, Some(ClosureReason::interrupted())),
        ];
        for closing in whole.iter().chain(droppable.iter()) {
            assert_eq!(unready(closing, &[]), Unready::None, "{closing:?}");
        }
        for closing in &whole {
            assert_eq!(
                unready(closing, &[first, second]),
                Unready::Retry(first),
                "{closing:?}"
            );
        }
        for closing in &droppable {
            assert_eq!(
                unready(closing, &[first, second]),
                Unready::Drop(vec![first, second]),
                "{closing:?}"
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
