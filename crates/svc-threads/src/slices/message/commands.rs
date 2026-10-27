//! The message commands, one reaction each:
//! `integration.cmd.threads.message.{store (v2, and the deprecated v1),
//! complete, fail, cancel, delete}.v{N}`, each answered once —
//! `message.confirmed` with the message's status after it, or
//! `message.refused` — in the transaction of the change.
//!
//! Every message command acts on an `OPEN` thread: the thread is loaded and
//! locked first (`THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`), then the message, then
//! the artifacts it names — the one lock order of the service. A refusal writes
//! nothing; a close that must refuse is judged before its seal.
//!
//! A resolved thread refuses the commands that would change its conversation —
//! store, delete, and the edit of [`super::edit`] — with `THREAD_RESOLVED`
//! ([`writable_thread`]); a message already `STREAMING` still completes, fails
//! or cancels ([`open_thread`] alone).
//!
//! A store judges, in this order: the thread, the message id (`ID_NOT_UUIDV7`,
//! `KEY_REUSED`), its parent (`MESSAGE_NOT_FOUND`, `MESSAGE_INVALID`), its
//! content by the library's rules (`MESSAGE_INVALID`, the library's code) and
//! its annotations (`ID_NOT_UUIDV7`, `MESSAGE_INVALID`), its author
//! (`NOT_A_MEMBER`), the ids its content declares (`KEY_REUSED`), the artifacts
//! it names (`ARTIFACT_NOT_READY`).

use std::collections::HashSet;

use contract_threads::llm::{
    AgentId, Author, ClosureReason, IncomingEntry, InputTurn, ObservedAt, Turn, TurnId, TurnKind,
};
use contract_threads::{
    AiDelivery, CancelMessage, CompleteMessage, DeleteMessage, FailMessage, MessageInvalidError,
    MessageStatus, MessageTurn, PlacedAnnotation, ReasonCode, StoreMessageV2, StreamSeal, v1,
};
use futures_util::future::BoxFuture;
use service_engine::SealHash;
use service_engine::accumulator::ChunkSeq;
use service_engine::error::{AccumulatorError, EngineError};
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::content::{Content, first_repeated, invalid, refusal_of};
use super::draft::{self, CloseKind, TurnDraft};
use super::state::{Message, MessageAuthor, MessageState};
use super::stream::TurnContent;
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

pub(super) type Verdict = Result<MessageOutcome, Refusal>;

fn thread_not_found() -> Refusal {
    Refusal::new(ReasonCode::ThreadNotFound)
}

fn message_not_found() -> Refusal {
    Refusal::new(ReasonCode::MessageNotFound)
}

/// The thread a message command names, loaded and locked, and `OPEN`: what a
/// close of a `STREAMING` message needs, resolved or not.
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

/// The thread a command changing its conversation names — store, edit,
/// delete — loaded and locked, `OPEN` and not resolved (`THREAD_RESOLVED`).
pub(super) async fn writable_thread(
    cx: &mut Reaction<'_>,
    thread: Uuid,
) -> Result<Result<ThreadState, Refusal>, Fault> {
    Ok(match open_thread(cx, thread).await? {
        Ok(thread) => match thread.resolved_refusal() {
            Some(refusal) => Err(refusal),
            None => Ok(thread),
        },
        Err(refusal) => Err(refusal),
    })
}

/// The message a command names, loaded and locked, when it is one of `thread`'s.
pub(super) async fn message_of(
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
pub(super) use refuse_on;

// ── message.store ───────────────────────────────────────────────────────────

/// What a store names, whichever version of the command carried it.
struct Store<'a> {
    message_id: Uuid,
    thread_id: Uuid,
    parent_message_id: Option<Uuid>,
    job_id: Option<Uuid>,
    body: StoreBody<'a>,
}

enum StoreBody<'a> {
    V2(&'a MessageTurn),
    V1(&'a v1::NewMessage),
}

pub fn store_v2<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<StoreMessageV2>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let mut call = Call::of(cx, &command)?;
        call.meta = call.meta.with_impersonator(command.impersonator_id);
        let store = Store {
            message_id: command.message_id,
            thread_id: command.thread_id,
            parent_message_id: command.parent_message_id,
            job_id: command.job_id,
            body: StoreBody::V2(&command.message),
        };
        let verdict = store_message(cx, &call, &store).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

/// `message.store` v1, deprecated: converted to the 2.0 model, answered on the
/// same events.
pub fn store_v1<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<v1::StoreMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let mut call = Call::of(cx, &command)?;
        call.meta = call.meta.with_impersonator(command.impersonator_id);
        let store = Store {
            message_id: command.message_id,
            thread_id: command.thread_id,
            parent_message_id: command.parent_message_id,
            job_id: command.job_id,
            body: StoreBody::V1(&command.message),
        };
        let verdict = store_message(cx, &call, &store).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

/// A store's content, judged by the library's rules.
#[derive(Debug)]
struct Judged {
    author: MessageAuthor,
    status: MessageStatus,
    turn_kind: TurnKind,
    observed_at: Option<ObservedAt>,
    content: Content,
}

/// The library's agent and author of an AI message.
fn ai_author(agent: Uuid, name: &str) -> Result<(AgentId, Author), Refusal> {
    let agent = AgentId::from_uuid(agent).map_err(|error| refusal_of(&error))?;
    let author = Author::new(name).map_err(|error| refusal_of(&error))?;
    Ok((agent, author))
}

/// Every annotation follows an entry of the turn, under a UUIDv7.
fn judge_annotations(content: &Content, annotations: &[PlacedAnnotation]) -> Result<(), Refusal> {
    for placed in annotations {
        crate::command::minted("annotation_id", placed.annotation.id)?;
        if !content.holds_entry(placed.after_entry_id) {
            return Err(invalid(MessageInvalidError::AnnotationEntryUnknown)
                .with("object_id", placed.after_entry_id));
        }
    }
    Ok(())
}

/// A whole AI turn ends completed: Threads closes it when it holds no closure,
/// and a closure it holds must be the completed one.
fn closed_completed(turn: Turn) -> Result<Turn, Refusal> {
    let Turn::AITurn(mut ai) = turn else {
        return Ok(turn);
    };
    if ai.is_closed() {
        return match ai.state().closure() {
            Some(ClosureReason::Completed { .. }) => Ok(Turn::AITurn(ai)),
            _ => Err(invalid(MessageInvalidError::ClosureNotCompleted)),
        };
    }
    let closure = draft::closure_entry(Uuid::now_v7(), ClosureReason::completed())
        .map_err(|_| invalid(MessageInvalidError::LibraryRefusal))?;
    ai.push_entry(closure).map_err(|error| refusal_of(&error))?;
    Ok(Turn::AITurn(ai))
}

fn whole(
    turn: Turn,
    author: MessageAuthor,
    annotations: Vec<PlacedAnnotation>,
) -> Result<Judged, Refusal> {
    let content = Content::of_turn(&turn, Vec::new()).map_err(|error| refusal_of(&error))?;
    judge_annotations(&content, &annotations)?;
    Ok(Judged {
        author,
        status: MessageStatus::Completed,
        turn_kind: turn.kind(),
        observed_at: turn.observed_at().cloned(),
        content: Content {
            annotations,
            ..content
        },
    })
}

/// A user's human message, as the input turn under the message's id holding
/// it.
fn user_turn(
    message_id: Uuid,
    entry: &contract_threads::LibraryJson,
) -> Result<(Turn, Uuid), Refusal> {
    let entry = entry.to_entry().map_err(|error| refusal_of(&error))?;
    let entry = IncomingEntry::try_from(entry).map_err(|error| refusal_of(&error))?;
    let IncomingEntry::HumanMessage(human) = &entry else {
        return Err(invalid(MessageInvalidError::NotAUserTurn));
    };
    let author = human.human().as_uuid();
    let turn = TurnId::from_uuid(message_id).map_err(|error| refusal_of(&error))?;
    let turn = InputTurn::new(turn, vec![entry]).map_err(|error| refusal_of(&error))?;
    Ok((turn.into(), author))
}

fn judge_content(message_id: Uuid, body: &StoreBody<'_>) -> Result<Judged, Refusal> {
    match body {
        StoreBody::V2(MessageTurn::User(user)) => {
            let (turn, human) = user_turn(message_id, &user.entry)?;
            if !user.annotations.is_empty() {
                return Err(invalid(MessageInvalidError::AnnotationInUserMessage));
            }
            whole(turn, user_author(human), Vec::new())
        }
        StoreBody::V2(MessageTurn::Ai(ai)) => {
            let (agent, author) = ai_author(ai.agent_id, &ai.author_name)?;
            let message_author = MessageAuthor::Agent {
                agent,
                name: author.clone(),
            };
            match &ai.delivery {
                AiDelivery::Streaming => Ok(Judged {
                    author: message_author,
                    status: MessageStatus::Streaming,
                    turn_kind: TurnKind::AITurn,
                    observed_at: None,
                    content: Content::default(),
                }),
                AiDelivery::Completed { turn, annotations } => {
                    let turn = turn.to_turn().map_err(|error| refusal_of(&error))?;
                    if turn.id().as_uuid() != message_id {
                        return Err(invalid(MessageInvalidError::TurnIdMismatch));
                    }
                    if let Turn::AITurn(held) = &turn
                        && (held.agent() != agent || held.author() != &author)
                    {
                        return Err(invalid(MessageInvalidError::AuthorMismatch));
                    }
                    whole(
                        closed_completed(turn)?,
                        message_author,
                        annotations.to_vec(),
                    )
                }
            }
        }
        StoreBody::V1(v1::NewMessage::User(user)) => {
            let (turn, annotations) =
                super::v1::user_turn(message_id, user.author_user_id, &user.blocks)
                    .map_err(|fault| fault.refusal())?;
            if !annotations.is_empty() {
                return Err(invalid(MessageInvalidError::AnnotationInUserMessage));
            }
            whole(turn.into(), user_author(user.author_user_id), Vec::new())
        }
        StoreBody::V1(v1::NewMessage::Ai(ai)) => {
            let agent = AgentId::parse(&ai.author.id).map_err(|error| refusal_of(&error))?;
            let (agent, author) = ai_author(agent.as_uuid(), &ai.author.name)?;
            match &ai.delivery {
                v1::AiDelivery::Streaming => Err(invalid(MessageInvalidError::StreamingV1)),
                v1::AiDelivery::Completed { items } => {
                    let (turn, annotations) =
                        super::v1::ai_turn(message_id, agent, author.clone(), items)
                            .map_err(|fault| fault.refusal())?;
                    whole(
                        closed_completed(turn.into())?,
                        MessageAuthor::Agent {
                            agent,
                            name: author,
                        },
                        annotations,
                    )
                }
            }
        }
    }
}

fn user_author(user_id: Uuid) -> MessageAuthor {
    MessageAuthor::User {
        user_id,
        display_name: None,
        in_roster: true,
    }
}

async fn store_message(
    cx: &mut Reaction<'_>,
    call: &Call,
    command: &Store<'_>,
) -> Result<Verdict, Fault> {
    let mut thread = refuse_on!(writable_thread(cx, command.thread_id).await?);
    refuse_on!(crate::command::minted("message_id", command.message_id));
    let held_message = cx
        .load::<MessageState>(&command.message_id)
        .await?
        .is_some()
        || !store::held_ids(cx.connection(), &[command.message_id], None)
            .await?
            .is_empty();
    if held_message || command.message_id == thread.id {
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
                return Ok(Err(invalid(MessageInvalidError::ParentNotInThread)
                    .with("parent_message_id", parent)));
            }
            Some(_) => {}
        }
    }
    let judged = refuse_on!(judge_content(command.message_id, &command.body));
    if let MessageAuthor::User { user_id, .. } = &judged.author
        && !thread.is_member(*user_id)
    {
        return Ok(Err(
            Refusal::new(ReasonCode::NotAMember).with("user_id", *user_id)
        ));
    }
    let ids = judged.content.identities();
    if let Some(repeated) = first_repeated(ids.iter().copied().chain([command.message_id])) {
        return Ok(Err(
            Refusal::new(ReasonCode::KeyReused).with("ids", [repeated])
        ));
    }
    let mut held: HashSet<Uuid> = store::held_ids(cx.connection(), &ids, None)
        .await?
        .into_iter()
        .collect();
    if ids.contains(&thread.id) {
        held.insert(thread.id);
    }
    if !held.is_empty() {
        let mut held: Vec<Uuid> = held.into_iter().collect();
        held.sort();
        return Ok(Err(Refusal::new(ReasonCode::KeyReused).with("ids", held)));
    }
    let artifacts = refuse_on!(lifecycle::ready(cx, thread.id, &judged.content.artifacts()).await?);

    let now = call.meta.occurred_at;
    let author_user = judged.author_user();
    let status = judged.status;
    let message = transitions::store(
        NewMessage {
            id: command.message_id,
            thread_id: thread.id,
            parent_message_id: command.parent_message_id,
            author: judged.author,
            job_id: command.job_id,
            status,
            turn_kind: judged.turn_kind,
            observed_at: judged.observed_at,
            content: judged.content,
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

impl Judged {
    fn author_user(&self) -> Option<Uuid> {
        match &self.author {
            MessageAuthor::User { user_id, .. } => Some(*user_id),
            MessageAuthor::Agent { .. } => None,
        }
    }
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
    let mut artifacts = Vec::new();
    if kind == CloseKind::Complete {
        let preview = cx.accumulated::<TurnContent>(&state.id).await?.state;
        artifacts = refuse_on!(closing::precheck_completion(cx, &state, &preview).await?);
    }
    let sealed = refuse_on!(seal(cx, state.id, seal_declared).await?);
    closing::store_closed(cx, state, &sealed, kind, artifacts, &call.meta)
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
    refuse_on!(writable_thread(cx, command.thread_id).await?);
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

#[cfg(test)]
mod tests {
    use contract_threads::llm::{
        AITurn, BlockId, ContentBlock, EntryId, HumanId, HumanMessage, ModelResponse, PlainBlock,
        ResponseEnding, ResponseMetadata, TextBlock, TurnEntry,
    };
    use contract_threads::{AiTurn, Annotation, AnnotationBody, LibraryJson, Notice, UserTurn};

    use super::*;

    fn v7() -> Uuid {
        Uuid::now_v7()
    }

    fn text(words: &str) -> TextBlock {
        TextBlock::new(BlockId::from_uuid(v7()).unwrap(), words).unwrap()
    }

    fn human_entry(human: Uuid, entry: Uuid) -> LibraryJson {
        let said = HumanMessage::new(
            EntryId::from_uuid(entry).unwrap(),
            HumanId::from_uuid(human).unwrap(),
            vec![PlainBlock::try_from(text("hi")).unwrap()],
        )
        .unwrap();
        LibraryJson::of(&TurnEntry::from(said)).unwrap()
    }

    fn user(human: Uuid) -> MessageTurn {
        MessageTurn::User(UserTurn {
            entry: human_entry(human, v7()),
            annotations: Vec::new(),
        })
    }

    fn warning() -> Annotation {
        Annotation::new(
            v7(),
            AnnotationBody::Warning(Notice {
                code: "W".into(),
                text: None,
            }),
        )
    }

    fn answered(
        message: Uuid,
        agent: AgentId,
        response: EntryId,
        annotations: Vec<PlacedAnnotation>,
    ) -> MessageTurn {
        let mut ai = AITurn::open(
            TurnId::from_uuid(message).unwrap(),
            agent,
            Author::new("Analyst").unwrap(),
        );
        ai.push_entry(
            ModelResponse::new(
                response,
                vec![ContentBlock::Text(text("done"))],
                ResponseEnding::finished(),
                ResponseMetadata::new(),
            )
            .unwrap()
            .into(),
        )
        .unwrap();
        MessageTurn::Ai(AiTurn {
            agent_id: agent.as_uuid(),
            author_name: "Analyst".into(),
            delivery: AiDelivery::Completed {
                turn: LibraryJson::of(&Turn::AITurn(ai)).unwrap(),
                annotations,
            },
        })
    }

    fn judge(message: Uuid, turn: &MessageTurn) -> Result<Judged, Refusal> {
        judge_content(message, &StoreBody::V2(turn))
    }

    fn error_of(refusal: &Refusal) -> &str {
        refusal.params["error"].as_str().unwrap_or_default()
    }

    #[test]
    fn a_user_message_is_the_input_turn_holding_its_authors_human_message() {
        let (message, human) = (v7(), v7());
        let judged = judge(message, &user(human)).unwrap();
        assert_eq!(judged.author_user(), Some(human));
        assert_eq!(judged.turn_kind, TurnKind::InputTurn);
        assert_eq!(judged.status, MessageStatus::Completed);
        assert_eq!(judged.content.entries.len(), 1);
    }

    #[test]
    fn a_user_message_holding_an_annotation_or_an_entry_under_its_id_is_invalid() {
        let (message, human) = (v7(), v7());
        let MessageTurn::User(mut annotated) = user(human) else {
            unreachable!()
        };
        annotated.annotations = vec![PlacedAnnotation::new(v7(), warning())];
        let refused = judge(message, &MessageTurn::User(annotated)).unwrap_err();
        assert_eq!(error_of(&refused), "annotation_in_user_message");
        let same = MessageTurn::User(UserTurn {
            entry: human_entry(human, message),
            annotations: Vec::new(),
        });
        let refused = judge(message, &same).unwrap_err();
        assert_eq!(error_of(&refused), "reused_identity");
    }

    #[test]
    fn a_whole_ai_turn_is_closed_completed_by_threads_and_keeps_its_annotations() {
        let message = v7();
        let agent = AgentId::from_uuid(v7()).unwrap();
        let response = EntryId::from_uuid(v7()).unwrap();
        let placed = PlacedAnnotation::new(response.as_uuid(), warning());
        let judged = judge(message, &answered(message, agent, response, vec![placed])).unwrap();
        assert_eq!(judged.turn_kind, TurnKind::AITurn);
        let kinds: Vec<_> = judged
            .content
            .entries
            .iter()
            .map(|e| e.kind().as_str())
            .collect();
        assert_eq!(kinds, ["model_response", "turn_closure"]);
        assert_eq!(judged.content.annotations.len(), 1);
        let unknown = PlacedAnnotation::new(v7(), warning());
        let refused =
            judge(message, &answered(message, agent, response, vec![unknown])).unwrap_err();
        assert_eq!(error_of(&refused), "annotation_entry_unknown");
        let mut not_v7 = warning();
        not_v7.id = Uuid::new_v4();
        let refused = judge(
            message,
            &answered(
                message,
                agent,
                response,
                vec![PlacedAnnotation::new(response.as_uuid(), not_v7)],
            ),
        )
        .unwrap_err();
        assert_eq!(refused.code, ReasonCode::IdNotUuidV7);
    }

    #[test]
    fn an_agent_id_that_is_no_uuidv7_or_another_than_the_turns_is_invalid() {
        let message = v7();
        let MessageTurn::Ai(mut ai) = answered(
            message,
            AgentId::from_uuid(v7()).unwrap(),
            EntryId::from_uuid(v7()).unwrap(),
            Vec::new(),
        ) else {
            unreachable!()
        };
        ai.agent_id = v7();
        let refused = judge(message, &MessageTurn::Ai(ai.clone())).unwrap_err();
        assert_eq!(error_of(&refused), "author_mismatch");
        ai.agent_id = Uuid::new_v4();
        let refused = judge(message, &MessageTurn::Ai(ai)).unwrap_err();
        assert_eq!(refused.code, ReasonCode::MessageInvalid);
        assert_eq!(error_of(&refused), "invalid_identity");
    }

    #[test]
    fn a_v1_streaming_opening_is_invalid_and_a_v1_ai_author_must_be_a_uuidv7() {
        let streaming = v1::NewMessage::Ai(v1::AiMessage {
            author: v1::AiAuthor {
                id: v7().to_string(),
                name: "Analyst".into(),
            },
            delivery: v1::AiDelivery::Streaming,
        });
        let refused = judge_content(v7(), &StoreBody::V1(&streaming)).unwrap_err();
        assert_eq!(error_of(&refused), "streaming_v1");
        let free_text = v1::NewMessage::Ai(v1::AiMessage {
            author: v1::AiAuthor {
                id: "a1".into(),
                name: "Analyst".into(),
            },
            delivery: v1::AiDelivery::Completed { items: Vec::new() },
        });
        let refused = judge_content(v7(), &StoreBody::V1(&free_text)).unwrap_err();
        assert_eq!(refused.code, ReasonCode::MessageInvalid);
    }
}
