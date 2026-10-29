use contract_threads::{
    CancelMessage, CompleteMessage, CompleteMessageV2, FailMessage, FailMessageV2, LibraryJson,
    ReasonCode, StreamSeal,
};
use futures_util::future::BoxFuture;
use service_engine::SealHash;
use service_engine::accumulator::ChunkSeq;
use service_engine::error::{AccumulatorError, EngineError};
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::close::CloseKind;
use super::closing;
use super::closure::{self, Closing};
use super::commands::{Verdict, message_of, open_thread, refuse_on};
use super::draft::TurnDraft;
use super::stream::TurnContent;
use crate::command::{Call, Inbound, Refusal, answer_message};
use crate::fault::Fault;

const SEAL_TRUNCATION_ATTEMPTS: u32 = 6;

struct Closer<'a> {
    thread: Uuid,
    message: Uuid,
    kind: CloseKind,
    seal: Option<&'a StreamSeal>,
    closure: Option<&'a LibraryJson>,
}

pub fn complete<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<CompleteMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let closer = Closer {
            thread: command.thread_id,
            message: command.message_id,
            kind: CloseKind::Complete,
            seal: Some(&command.seal),
            closure: None,
        };
        let verdict = close_message(cx, &call, &closer).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

pub fn complete_v2<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<CompleteMessageV2>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let closer = Closer {
            thread: command.thread_id,
            message: command.message_id,
            kind: CloseKind::Complete,
            seal: Some(&command.seal),
            closure: command.closure.as_ref(),
        };
        let verdict = close_message(cx, &call, &closer).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

pub fn fail<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<FailMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let closer = Closer {
            thread: command.thread_id,
            message: command.message_id,
            kind: CloseKind::Fail,
            seal: command.seal.as_ref(),
            closure: None,
        };
        let verdict = close_message(cx, &call, &closer).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

pub fn fail_v2<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<FailMessageV2>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let closer = Closer {
            thread: command.thread_id,
            message: command.message_id,
            kind: CloseKind::Fail,
            seal: command.seal.as_ref(),
            closure: command.closure.as_ref(),
        };
        let verdict = close_message(cx, &call, &closer).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

pub fn cancel<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<CancelMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let closer = Closer {
            thread: command.thread_id,
            message: command.message_id,
            kind: CloseKind::Cancel,
            seal: command.seal.as_ref(),
            closure: None,
        };
        let verdict = close_message(cx, &call, &closer).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

fn seal_refusal(error: &EngineError, delivered: u32) -> Option<Refusal> {
    let mismatch = |detail: &str| Refusal::new(ReasonCode::StreamMismatch).with("error", detail);
    match error {
        EngineError::Accumulator(AccumulatorError::SealHashMismatch { .. })
        | EngineError::Accumulator(AccumulatorError::SealHashFormat(_)) => Some(mismatch("hash")),
        EngineError::Accumulator(AccumulatorError::SealChunkBeyondLastSeq { max_seq, .. }) => {
            Some(mismatch("chunk_beyond_last_seq").with("max_seq", max_seq))
        }
        EngineError::Accumulator(AccumulatorError::SealTruncated { contiguous_to, .. })
            if delivered >= SEAL_TRUNCATION_ATTEMPTS =>
        {
            Some(mismatch("chunks_missing").with("contiguous_to", contiguous_to))
        }
        _ => None,
    }
}

async fn seal(
    cx: &mut Reaction<'_>,
    message: Uuid,
    seal: Option<&StreamSeal>,
) -> Result<Result<TurnDraft, Refusal>, Fault> {
    let sealed = match seal {
        Some(seal) => {
            let last_seq = ChunkSeq::new(seal.last_seq);
            let hash = SealHash::from_hex(&seal.hash);
            match (last_seq, hash) {
                (Ok(last_seq), Ok(hash)) => cx.seal::<TurnContent>(&message, last_seq, hash).await,
                (Err(error), _) | (_, Err(error)) => Err(error),
            }
        }
        None => cx.seal_current::<TurnContent>(&message).await,
    };
    match sealed {
        Ok(draft) => Ok(Ok(draft)),
        Err(error) => match seal_refusal(&error, cx.delivered()) {
            Some(refusal) => Ok(Err(refusal)),
            None => match error {
                EngineError::Accumulator(AccumulatorError::ChunkSeqOutOfRange { .. }) => Ok(Err(
                    Refusal::new(ReasonCode::StreamMismatch).with("error", "last_seq"),
                )),
                other => Err(other.into()),
            },
        },
    }
}

async fn close_message(
    cx: &mut Reaction<'_>,
    call: &Call,
    closer: &Closer<'_>,
) -> Result<Verdict, Fault> {
    let mut thread = refuse_on!(open_thread(cx, closer.thread).await?);
    let state = refuse_on!(message_of(cx, closer.thread, closer.message).await?);
    if let Some(refusal) = state.not_streaming() {
        return Ok(Err(refusal));
    }
    let named = match closer.closure {
        Some(json) => Some(refuse_on!(closure::parse(json))),
        None => None,
    };
    let closing = Closing::naming(closer.kind, named);
    let mut artifacts = Vec::new();
    if closing.strict() {
        let preview = cx.accumulated::<TurnContent>(&state.id).await?.state;
        artifacts = refuse_on!(closing::precheck_completion(cx, &state, &preview, &closing).await?);
    }
    let sealed = refuse_on!(seal(cx, state.id, closer.seal).await?);
    closing::store_closed(
        cx,
        state,
        &mut thread,
        &sealed,
        &closing,
        artifacts,
        &call.meta,
    )
    .await
    .map(Ok)
}
