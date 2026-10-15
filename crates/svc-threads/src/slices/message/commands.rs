//! The message commands, one reaction each: `integration.cmd.threads.message.
//! {store, complete, fail, cancel, delete}.v1`, each answered once —
//! `message.confirmed` with the message's status after it, or
//! `message.refused` — in the transaction of the change.
//!
//! Every message command acts on an `OPEN` thread: the thread is loaded and
//! locked first (`THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`), then the message, then
//! the artifacts it names — the one lock order of the service. A refusal writes
//! nothing; a close that must refuse is judged before its seal.

use contract_threads::llm::Author;
use contract_threads::{
    AiDelivery, CancelMessage, CompleteMessage, DeleteMessage, FailMessage, MessageStatus,
    NewMessage as Incoming, ReasonCode, StoreMessage, StreamSeal,
};
use futures_util::future::BoxFuture;
use service_engine::SealHash;
use service_engine::accumulator::ChunkSeq;
use service_engine::error::{AccumulatorError, EngineError};
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::content::{self, named_artifacts, reason};
use super::draft::{CloseKind, MessageDraft};
use super::state::{Message, MessageAuthor, MessageState};
use super::stream::MessageContent;
use super::transitions::{self, NewMessage};
use super::{MessageCause, closing, message_cause, store, watch};
use crate::command::{Call, Inbound, MessageOutcome, Refusal, answer_message};
use crate::directory;
use crate::fault::Fault;
use crate::slices::artifact::lifecycle;
use crate::slices::thread::state::{Thread, ThreadState};
use crate::slices::thread::transitions::Applied;
use crate::slices::thread::{ThreadCause, title_job};

/// How many deliveries a completion waits for the chunks it seals to be folded
/// before it answers `STREAM_MISMATCH`: the engine's backoff makes that about
/// six seconds of ingestion lag.
const SEAL_TRUNCATION_ATTEMPTS: u32 = 6;

type Verdict = Result<MessageOutcome, Refusal>;

fn thread_not_found() -> Refusal {
    Refusal::new(ReasonCode::ThreadNotFound)
}

fn message_not_found() -> Refusal {
    Refusal::new(ReasonCode::MessageNotFound)
}

/// The thread a message command names, loaded and locked, and `OPEN`.
async fn open_thread(
    cx: &mut Reaction<'_>,
    thread: Uuid,
) -> Result<Result<ThreadState, Refusal>, Fault> {
    let Some(thread) = cx.load::<ThreadState>(&thread).await? else {
        return Ok(Err(thread_not_found()));
    };
    if let Some(refusal) = thread.open_refusal() {
        return Ok(Err(refusal));
    }
    Ok(Ok(thread))
}

/// The message a command names, loaded and locked, when it is one of `thread`'s.
async fn message_of(
    cx: &mut Reaction<'_>,
    thread: Uuid,
    message: Uuid,
) -> Result<Result<MessageState, Refusal>, Fault> {
    Ok(match cx.load::<MessageState>(&message).await? {
        Some(state) if state.thread_id == thread => Ok(state),
        _ => Err(message_not_found()),
    })
}

macro_rules! refuse_on {
    ($verdict:expr) => {
        match $verdict {
            Ok(value) => value,
            Err(refusal) => return Ok(Err(refusal)),
        }
    };
}

// ── message.store ───────────────────────────────────────────────────────────

pub fn store<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<StoreMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let mut call = Call::of(cx, &command)?;
        call.meta = call.meta.with_impersonator(command.impersonator_id);
        let verdict = store_message(cx, &call, &command).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

async fn store_message(
    cx: &mut Reaction<'_>,
    call: &Call,
    command: &StoreMessage,
) -> Result<Verdict, Fault> {
    let mut thread = refuse_on!(open_thread(cx, command.thread_id).await?);
    refuse_on!(crate::command::minted("message_id", command.message_id));
    if cx
        .load::<MessageState>(&command.message_id)
        .await?
        .is_some()
    {
        return Ok(Err(
            Refusal::new(ReasonCode::KeyReused).with("message_id", command.message_id)
        ));
    }
    // The parent is a message the command names: one nobody stored is not
    // found; one of another thread is not a parent this message can have.
    if let Some(parent) = command.parent_message_id {
        match store::thread_of(cx.connection(), parent).await? {
            None => {
                return Ok(Err(
                    Refusal::new(ReasonCode::MessageNotFound).with("parent_message_id", parent)
                ));
            }
            Some(of) if of != thread.id => {
                return Ok(Err(content::invalid(reason::PARENT_NOT_IN_THREAD)
                    .with("parent_message_id", parent)));
            }
            Some(_) => {}
        }
    }
    let (author, status, blocks, items) = match &command.message {
        Incoming::User(user) => {
            if !thread.is_member(user.author_user_id) {
                return Ok(Err(
                    Refusal::new(ReasonCode::NotAMember).with("user_id", user.author_user_id)
                ));
            }
            let blocks = refuse_on!(content::user_blocks(&user.blocks));
            let author = MessageAuthor::User {
                user_id: user.author_user_id,
                display_name: None,
                in_roster: true,
            };
            (author, MessageStatus::Completed, blocks, Vec::new())
        }
        Incoming::Ai(ai) => {
            if Author::new(ai.author.id.clone(), ai.author.name.clone()).is_err() {
                return Ok(Err(content::invalid(reason::AUTHOR_INVALID)));
            }
            let author = MessageAuthor::Ai {
                id: ai.author.id.clone(),
                name: ai.author.name.clone(),
            };
            match &ai.delivery {
                AiDelivery::Completed { items } => {
                    let items = refuse_on!(content::ai_items(command.message_id, items));
                    (author, MessageStatus::Completed, Vec::new(), items)
                }
                AiDelivery::Streaming => (author, MessageStatus::Streaming, Vec::new(), Vec::new()),
            }
        }
    };
    let block_ids: Vec<Uuid> = blocks
        .iter()
        .chain(items.iter().flat_map(|item| item.blocks.iter()))
        .map(|block| block.id)
        .collect();
    let held = store::held_block_ids(cx.connection(), &block_ids).await?;
    if !held.is_empty() {
        return Ok(Err(
            Refusal::new(ReasonCode::KeyReused).with("block_ids", held)
        ));
    }
    let named = named_artifacts(
        blocks
            .iter()
            .chain(items.iter().flat_map(|item| item.blocks.iter())),
    );
    let artifacts = refuse_on!(lifecycle::ready(cx, thread.id, &named).await?);

    let now = call.meta.occurred_at;
    let author_user = match &author {
        MessageAuthor::User { user_id, .. } => Some(*user_id),
        MessageAuthor::Ai { .. } => None,
    };
    let message = transitions::store(
        NewMessage {
            id: command.message_id,
            thread_id: thread.id,
            parent_message_id: command.parent_message_id,
            author,
            job_id: command.job_id,
            status,
            blocks,
            items,
            created_at: now,
        },
        &call.meta,
    )?;
    cx.create(&message).await?;
    lifecycle::attach(cx, artifacts, message.id, &call.meta).await?;
    if status == MessageStatus::Streaming {
        watch::open(cx, message.id, now).await?;
    }

    thread.record_activity(now);
    if let Some(user) = author_user
        && let Some(job) = title_job::start(&mut thread, now)
    {
        let name = directory::display_name(cx.connection(), user).await?;
        title_job::publish_create(
            cx,
            thread.id,
            job,
            &crate::config::installed().title_runner_type,
            user,
            name,
        )?;
    }
    cx.save(&thread).await?;
    cx.impact_caused::<Message, _>(&message.id, message_cause(MessageCause::Stored, thread.id))?;
    cx.impact_caused::<Thread, _>(&thread.id, ThreadCause::MessageStored)?;
    Ok(Ok(MessageOutcome {
        status,
        deleted: false,
    }))
}

// ── message.complete / fail / cancel ────────────────────────────────────────

pub fn complete<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<CompleteMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let verdict = close_message(
            cx,
            &call,
            command.thread_id,
            command.message_id,
            CloseKind::Complete,
            Some(&command.seal),
        )
        .await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

pub fn fail<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<FailMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let verdict = close_message(
            cx,
            &call,
            command.thread_id,
            command.message_id,
            CloseKind::Fail,
            command.seal.as_ref(),
        )
        .await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

pub fn cancel<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<CancelMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let verdict = close_message(
            cx,
            &call,
            command.thread_id,
            command.message_id,
            CloseKind::Cancel,
            command.seal.as_ref(),
        )
        .await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

/// `STREAM_MISMATCH`, when a seal's refusal is one the writer can correct; any
/// other seal error is a fault (retried).
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
) -> Result<Result<MessageDraft, Refusal>, Fault> {
    let sealed = match seal {
        Some(seal) => {
            let last_seq = ChunkSeq::new(seal.last_seq);
            let hash = SealHash::from_hex(&seal.hash);
            match (last_seq, hash) {
                (Ok(last_seq), Ok(hash)) => {
                    cx.seal::<MessageContent>(&message, last_seq, hash).await
                }
                (Err(error), _) | (_, Err(error)) => Err(error),
            }
        }
        None => cx.seal_current::<MessageContent>(&message).await,
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
    thread: Uuid,
    message: Uuid,
    kind: CloseKind,
    seal_declared: Option<&StreamSeal>,
) -> Result<Verdict, Fault> {
    refuse_on!(open_thread(cx, thread).await?);
    let state = refuse_on!(message_of(cx, thread, message).await?);
    if let Some(refusal) = state.not_streaming() {
        return Ok(Err(refusal));
    }
    // A completion can still be refused once its stream is judged: judged on
    // what the stream holds now, before the seal writes anything.
    let prechecked = if kind == CloseKind::Complete {
        let preview = cx.accumulated::<MessageContent>(&state.id).await?.state;
        refuse_on!(closing::precheck_completion(cx, &state, &preview).await?)
    } else {
        Vec::new()
    };
    let sealed = refuse_on!(seal(cx, state.id, seal_declared).await?);
    closing::store_closed(cx, state, &sealed, kind, prechecked, None, &call.meta)
        .await
        .map(Ok)
}

// ── message.delete ──────────────────────────────────────────────────────────

pub fn delete<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<DeleteMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let verdict = delete_message(cx, &call, &command).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

async fn delete_message(
    cx: &mut Reaction<'_>,
    call: &Call,
    command: &DeleteMessage,
) -> Result<Verdict, Fault> {
    refuse_on!(open_thread(cx, command.thread_id).await?);
    let mut state = refuse_on!(message_of(cx, command.thread_id, command.message_id).await?);
    let applied = refuse_on!(state.delete(command.acting_user_id, &call.meta)?);
    if applied == Applied::Changed {
        lifecycle::delete_all_of_message(cx, state.thread_id, state.id, &call.meta).await?;
        cx.save(&state).await?;
        cx.impact_caused::<Message, _>(
            &state.id,
            message_cause(MessageCause::Deleted, state.thread_id),
        )?;
    }
    Ok(Ok(MessageOutcome {
        status: state.status,
        deleted: state.deleted,
    }))
}
