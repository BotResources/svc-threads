use std::collections::HashSet;

use contract_threads::llm::{
    AgentId, Author, ClosureReason, IncomingEntry, InputTurn, ObservedAt, Turn, TurnId, TurnKind,
};
use contract_threads::{
    AiDelivery, DeleteMessage, LibraryJson, MessageInvalidError, MessageStatus, MessageTurn,
    PlacedAnnotation, ReasonCode, StoreMessageV2, v1,
};
use futures_util::future::BoxFuture;
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::close;
use super::closure::Kind;
use super::content::{
    Content, declared_refusal_of, first_repeated, invalid, nul_refusal, refusal_of,
};
use super::state::{Message, MessageAuthor, MessageState};
use super::transitions::{self, NewMessage};
use super::{MessageCause, message_cause, store, watch};
use crate::command::{Call, Inbound, MessageOutcome, Refusal, answer_message};
use crate::fault::Fault;
use crate::slices::artifact::lifecycle;
use crate::slices::thread::state::{Thread, ThreadState};
use crate::slices::thread::transitions::Applied;
use crate::slices::thread::{ThreadCause, title_job};

pub(super) type Verdict = Result<MessageOutcome, Refusal>;

fn thread_not_found() -> Refusal {
    Refusal::new(ReasonCode::ThreadNotFound)
}

fn message_not_found() -> Refusal {
    Refusal::new(ReasonCode::MessageNotFound)
}

pub(super) async fn open_thread(
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

#[derive(Debug)]
struct Judged {
    author: MessageAuthor,
    status: MessageStatus,
    turn_kind: TurnKind,
    observed_at: Option<ObservedAt>,
    content: Content,
}

fn ai_author(agent: Uuid, name: &str) -> Result<(AgentId, Author), Refusal> {
    let agent = AgentId::from_uuid(agent).map_err(|error| refusal_of(&error))?;
    let author = Author::new(name).map_err(|error| refusal_of(&error))?;
    Ok((agent, author))
}

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

fn status_of_closure(closure: &ClosureReason) -> Result<MessageStatus, Refusal> {
    Kind::of(closure)
        .map(Kind::status)
        .ok_or_else(|| invalid(MessageInvalidError::LibraryRefusal))
}

fn closed_whole(turn: Turn) -> Result<(Turn, MessageStatus), Refusal> {
    let Turn::AITurn(mut ai) = turn else {
        return Ok((turn, MessageStatus::Completed));
    };
    if let Some(closure) = ai.state().closure() {
        let status = status_of_closure(closure)?;
        return Ok((Turn::AITurn(ai), status));
    }
    let closure = close::closure_entry(Uuid::now_v7(), ClosureReason::completed())
        .map_err(|_| invalid(MessageInvalidError::LibraryRefusal))?;
    ai.push_entry(closure).map_err(|error| refusal_of(&error))?;
    Ok((Turn::AITurn(ai), MessageStatus::Completed))
}

fn declared_by_turn(json: serde_json::Value) -> Option<Vec<Uuid>> {
    let turn = LibraryJson::from_json(json).to_turn().ok()?;
    let mut ids = Content::of_turn(&turn, Vec::new()).ok()?.identities();
    ids.push(turn.id().as_uuid());
    Some(ids)
}

fn whole(
    (turn, status): (Turn, MessageStatus),
    author: MessageAuthor,
    annotations: Vec<PlacedAnnotation>,
) -> Result<Judged, Refusal> {
    let content = Content::of_turn(&turn, Vec::new()).map_err(|error| refusal_of(&error))?;
    judge_annotations(&content, &annotations)?;
    Ok(Judged {
        author,
        status,
        turn_kind: turn.kind(),
        observed_at: turn.observed_at().cloned(),
        content: Content {
            annotations,
            ..content
        },
    })
}

fn user_turn(message_id: Uuid, entry: &LibraryJson) -> Result<(Turn, Uuid), Refusal> {
    let turn = TurnId::from_uuid(message_id).map_err(|error| refusal_of(&error))?;
    let parsed = entry.to_entry().map_err(|error| {
        declared_refusal_of(entry.as_json(), &error, |probe| {
            let entry = IncomingEntry::try_from(LibraryJson::from_json(probe).to_entry().ok()?);
            let turn = InputTurn::new(turn, vec![entry.ok()?]).ok()?;
            Some(
                Content::of_turn(&turn.into(), Vec::new())
                    .ok()?
                    .identities(),
            )
        })
    })?;
    let entry = IncomingEntry::try_from(parsed).map_err(|error| refusal_of(&error))?;
    let IncomingEntry::HumanMessage(human) = &entry else {
        return Err(invalid(MessageInvalidError::NotAUserTurn));
    };
    let author = human.human().as_uuid();
    let turn = InputTurn::new(turn, vec![entry]).map_err(|error| refusal_of(&error))?;
    Ok((turn.into(), author))
}

fn holds_a_human_message(turn: &Turn) -> bool {
    match turn {
        Turn::InputTurn(input) => input
            .entries()
            .iter()
            .any(|entry| matches!(entry, IncomingEntry::HumanMessage(_))),
        _ => false,
    }
}

fn unstorable(body: &StoreBody<'_>) -> Option<Refusal> {
    match body {
        StoreBody::V2(message) => nul_refusal(message),
        StoreBody::V1(message) => nul_refusal(message),
    }
}

fn judge_content(message_id: Uuid, body: &StoreBody<'_>) -> Result<Judged, Refusal> {
    let storable = || unstorable(body).map_or(Ok(()), Err);
    match body {
        StoreBody::V2(MessageTurn::User(user)) => {
            storable()?;
            let (turn, human) = user_turn(message_id, &user.entry)?;
            if !user.annotations.is_empty() {
                return Err(invalid(MessageInvalidError::AnnotationInUserMessage));
            }
            whole(
                (turn, MessageStatus::Completed),
                user_author(human),
                Vec::new(),
            )
        }
        StoreBody::V2(MessageTurn::Ai(ai)) => {
            let (agent, author) = ai_author(ai.agent_id, &ai.author_name)?;
            storable()?;
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
                    let turn = turn.to_turn().map_err(|error| {
                        declared_refusal_of(turn.as_json(), &error, declared_by_turn)
                    })?;
                    if turn.id().as_uuid() != message_id {
                        return Err(invalid(MessageInvalidError::TurnIdMismatch));
                    }
                    if let Turn::AITurn(held) = &turn
                        && (held.agent() != agent || held.author() != &author)
                    {
                        return Err(invalid(MessageInvalidError::AuthorMismatch));
                    }
                    if holds_a_human_message(&turn) {
                        return Err(invalid(MessageInvalidError::HumanMessageInAiMessage));
                    }
                    whole(closed_whole(turn)?, message_author, annotations.to_vec())
                }
            }
        }
        StoreBody::V1(v1::NewMessage::User(user)) => {
            storable()?;
            super::v1::precheck(&user.blocks)?;
            let (turn, annotations) =
                super::v1::user_turn(message_id, user.author_user_id, &user.blocks)
                    .map_err(|fault| fault.refusal())?;
            if !annotations.is_empty() {
                return Err(invalid(MessageInvalidError::AnnotationInUserMessage));
            }
            whole(
                (turn.into(), MessageStatus::Completed),
                user_author(user.author_user_id),
                Vec::new(),
            )
        }
        StoreBody::V1(v1::NewMessage::Ai(ai)) => {
            let agent = AgentId::parse(&ai.author.id).map_err(|error| refusal_of(&error))?;
            let (agent, author) = ai_author(agent.as_uuid(), &ai.author.name)?;
            storable()?;
            match &ai.delivery {
                v1::AiDelivery::Streaming => Err(invalid(MessageInvalidError::StreamingV1)),
                v1::AiDelivery::Completed { items } => {
                    super::v1::precheck(super::v1::item_blocks(items))?;
                    let (turn, annotations) =
                        super::v1::ai_turn(message_id, agent, author.clone(), items)
                            .map_err(|fault| fault.refusal())?;
                    whole(
                        closed_whole(turn.into())?,
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
    let held_message = !store::held_ids(cx.connection(), &[command.message_id], None)
        .await?
        .is_empty();
    if held_message || command.message_id == thread.id {
        return Ok(Err(
            Refusal::new(ReasonCode::KeyReused).with("message_id", command.message_id)
        ));
    }
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
    if let StoreBody::V1(v1::NewMessage::User(user)) = &command.body
        && !thread.is_member(user.author_user_id)
    {
        return Ok(Err(
            Refusal::new(ReasonCode::NotAMember).with("user_id", user.author_user_id)
        ));
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
    if status == MessageStatus::Completed {
        title_job::trigger(
            cx,
            &mut thread,
            now,
            author_user.or(call.meta.acting_user_id),
        )
        .await?;
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
        AITurn, AgentMail, BlockId, ContentBlock, ConversationId, EntryId, HumanId, HumanMessage,
        ModelResponse, PlainBlock, ResponseEnding, ResponseMetadata, TextBlock, TurnEntry,
        TurnEntryKind,
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

    fn closed_by(message: Uuid, agent: AgentId, closure: ClosureReason) -> MessageTurn {
        let MessageTurn::Ai(ai) = answered(
            message,
            agent,
            EntryId::from_uuid(v7()).unwrap(),
            Vec::new(),
        ) else {
            unreachable!()
        };
        let AiDelivery::Completed { turn, .. } = ai.delivery else {
            unreachable!()
        };
        let Turn::AITurn(mut turn) = turn.to_turn().unwrap() else {
            unreachable!()
        };
        turn.push_entry(close::closure_entry(v7(), closure).unwrap())
            .unwrap();
        MessageTurn::Ai(AiTurn {
            agent_id: agent.as_uuid(),
            author_name: "Analyst".into(),
            delivery: AiDelivery::Completed {
                turn: LibraryJson::of(&Turn::AITurn(turn)).unwrap(),
                annotations: Vec::new(),
            },
        })
    }

    #[test]
    fn a_whole_ai_turn_its_writer_closed_is_stored_under_the_status_its_closure_gives() {
        let message = v7();
        let agent = AgentId::from_uuid(v7()).unwrap();
        for (closure, status) in [
            (ClosureReason::completed(), MessageStatus::Completed),
            (ClosureReason::budget_exhausted(), MessageStatus::Completed),
            (ClosureReason::cancelled(), MessageStatus::Cancelled),
            (
                ClosureReason::failed("tool_down").unwrap(),
                MessageStatus::Failed,
            ),
            (ClosureReason::interrupted(), MessageStatus::Failed),
        ] {
            let judged = judge(message, &closed_by(message, agent, closure.clone()))
                .unwrap_or_else(|refused| panic!("{closure:?} refused: {refused:?}"));
            assert_eq!(judged.status, status, "{closure:?}");
            let kinds: Vec<_> = judged.content.entries.iter().map(|e| e.kind()).collect();
            assert_eq!(
                kinds,
                [TurnEntryKind::ModelResponse, TurnEntryKind::TurnClosure],
                "the writer's closure is kept, none added: {closure:?}"
            );
        }
    }

    #[test]
    fn a_whole_ai_turn_holding_results_after_its_closure_is_stored_under_its_closure() {
        use contract_threads::llm::{
            OpaqueJson, RecordSet, RuntimeToolCall, RuntimeToolResult, RuntimeToolResults, ToolName,
        };

        let message = v7();
        let agent = AgentId::from_uuid(v7()).unwrap();
        let name = ToolName::new("t".repeat(256)).unwrap();
        let call = RuntimeToolCall::new(
            BlockId::from_uuid(v7()).unwrap(),
            name.clone(),
            OpaqueJson::from_serialize(&serde_json::json!({ "q": "late" })).unwrap(),
        )
        .unwrap();
        let mut ai = AITurn::open(
            TurnId::from_uuid(message).unwrap(),
            agent,
            Author::new("Analyst").unwrap(),
        );
        for entry in [
            ModelResponse::new(
                EntryId::from_uuid(v7()).unwrap(),
                vec![ContentBlock::from(call.clone())],
                ResponseEnding::tool_use(),
                ResponseMetadata::new(),
            )
            .unwrap()
            .into(),
            close::closure_entry(v7(), ClosureReason::cancelled()).unwrap(),
            RuntimeToolResults::new(
                EntryId::from_uuid(v7()).unwrap(),
                vec![
                    RuntimeToolResult::new(
                        BlockId::from_uuid(v7()).unwrap(),
                        call.id(),
                        name.clone(),
                        false,
                        vec![PlainBlock::try_from(text("found, too late")).unwrap()],
                    )
                    .unwrap(),
                ],
            )
            .unwrap()
            .into(),
        ] {
            ai.push_entry(entry).unwrap();
        }
        let turn = Turn::AITurn(ai);
        let stored = MessageTurn::Ai(AiTurn {
            agent_id: agent.as_uuid(),
            author_name: "Analyst".into(),
            delivery: AiDelivery::Completed {
                turn: LibraryJson::of(&turn).unwrap(),
                annotations: Vec::new(),
            },
        });

        let judged = judge(message, &stored).unwrap();
        assert_eq!(judged.status, MessageStatus::Cancelled);
        let kinds: Vec<_> = judged.content.entries.iter().map(|e| e.kind()).collect();
        assert_eq!(
            kinds,
            [
                TurnEntryKind::ModelResponse,
                TurnEntryKind::TurnClosure,
                TurnEntryKind::RuntimeToolResults
            ]
        );
        let record = RecordSet::from_turn(&turn).unwrap().turns()[0].clone();
        assert_eq!(judged.content.to_turn(record).unwrap(), turn);
    }

    fn input_turn_of_an_ai(message: Uuid, entry: TurnEntry) -> MessageTurn {
        let incoming = IncomingEntry::try_from(entry).unwrap();
        let turn: Turn = InputTurn::new(TurnId::from_uuid(message).unwrap(), vec![incoming])
            .unwrap()
            .into();
        MessageTurn::Ai(AiTurn {
            agent_id: v7(),
            author_name: "Analyst".into(),
            delivery: AiDelivery::Completed {
                turn: LibraryJson::of(&turn).unwrap(),
                annotations: Vec::new(),
            },
        })
    }

    #[test]
    fn an_ai_message_holding_a_human_message_is_invalid_and_other_incoming_entries_are_stored() {
        let message = v7();
        let human = HumanMessage::new(
            EntryId::from_uuid(v7()).unwrap(),
            HumanId::from_uuid(v7()).unwrap(),
            vec![PlainBlock::try_from(text("hi")).unwrap()],
        )
        .unwrap();
        let refused = judge(message, &input_turn_of_an_ai(message, human.into())).unwrap_err();
        assert_eq!(refused.code, ReasonCode::MessageInvalid);
        assert_eq!(error_of(&refused), "human_message_in_ai_message");
        let mail = AgentMail::new(
            EntryId::from_uuid(v7()).unwrap(),
            AgentId::from_uuid(v7()).unwrap(),
            Author::new("Critic").unwrap(),
            ConversationId::from_uuid(v7()).unwrap(),
            vec![PlainBlock::try_from(text("a note")).unwrap()],
        )
        .unwrap();
        let judged = judge(message, &input_turn_of_an_ai(message, mail.into())).unwrap();
        assert_eq!(judged.turn_kind, TurnKind::InputTurn);
        assert_eq!(judged.status, MessageStatus::Completed);
    }

    fn user_with_v4_at(human: Uuid, path: &[&str]) -> (MessageTurn, Uuid) {
        let mut entry = human_entry(human, v7()).into_json();
        let v4 = Uuid::new_v4();
        let mut at = &mut entry;
        for step in path {
            at = match step.parse::<usize>() {
                Ok(index) => &mut at[index],
                Err(_) => &mut at[*step],
            };
        }
        *at = serde_json::Value::String(v4.to_string());
        let turn = MessageTurn::User(UserTurn {
            entry: LibraryJson::from_json(entry),
            annotations: Vec::new(),
        });
        (turn, v4)
    }

    #[test]
    fn an_id_declared_for_a_new_object_that_is_no_uuidv7_is_id_not_uuidv7_and_a_reference_is_invalid()
     {
        let (message, human) = (v7(), v7());
        for path in [&["id"][..], &["content", "0", "id"][..]] {
            let (turn, v4) = user_with_v4_at(human, path);
            let refused = judge(message, &turn).unwrap_err();
            assert_eq!(refused.code, ReasonCode::IdNotUuidV7, "{path:?}");
            assert_eq!(refused.params["id"], serde_json::json!(v4), "{path:?}");
        }
        let (turn, _) = user_with_v4_at(human, &["human"]);
        let refused = judge(message, &turn).unwrap_err();
        assert_eq!(
            refused.code,
            ReasonCode::MessageInvalid,
            "a human is a reference"
        );
        assert_eq!(error_of(&refused), "invalid_identity");

        let agent = AgentId::from_uuid(v7()).unwrap();
        let MessageTurn::Ai(ai) = answered(
            message,
            agent,
            EntryId::from_uuid(v7()).unwrap(),
            Vec::new(),
        ) else {
            unreachable!()
        };
        let AiDelivery::Completed { turn, .. } = &ai.delivery else {
            unreachable!()
        };
        let swapped = |path: &[&str]| {
            let mut json = turn.as_json().clone();
            let mut at = &mut json;
            for step in path {
                at = match step.parse::<usize>() {
                    Ok(index) => &mut at[index],
                    Err(_) => &mut at[*step],
                };
            }
            *at = serde_json::Value::String(Uuid::new_v4().to_string());
            MessageTurn::Ai(AiTurn {
                delivery: AiDelivery::Completed {
                    turn: LibraryJson::from_json(json),
                    annotations: Vec::new(),
                },
                ..ai.clone()
            })
        };
        let refused = judge(message, &swapped(&["entries", "0", "id"])).unwrap_err();
        assert_eq!(refused.code, ReasonCode::IdNotUuidV7, "an entry id");
        let refused =
            judge(message, &swapped(&["entries", "0", "content", "0", "id"])).unwrap_err();
        assert_eq!(refused.code, ReasonCode::IdNotUuidV7, "a block id");
        let refused = judge(message, &swapped(&["agent"])).unwrap_err();
        assert_eq!(
            refused.code,
            ReasonCode::MessageInvalid,
            "the turn's agent is a reference"
        );
    }

    #[test]
    fn a_store_carrying_nul_is_invalid_before_the_library_reads_it() {
        let (message, human) = (v7(), v7());
        let said = HumanMessage::new(
            EntryId::from_uuid(v7()).unwrap(),
            HumanId::from_uuid(human).unwrap(),
            vec![PlainBlock::try_from(text("a\u{0}b")).unwrap()],
        )
        .unwrap();
        let turn = MessageTurn::User(UserTurn {
            entry: LibraryJson::of(&TurnEntry::from(said)).unwrap(),
            annotations: Vec::new(),
        });
        let refused = judge(message, &turn).unwrap_err();
        assert_eq!(refused.code, ReasonCode::MessageInvalid);
        assert_eq!(error_of(&refused), "nul_character");
    }

    #[test]
    fn a_v1_message_meets_the_0_4_judgement_of_its_block_ids_before_it_is_converted() {
        let block = |id: Uuid| -> v1::Block {
            serde_json::from_value(serde_json::json!({
                "id": id, "kind": "TEXT", "content": { "type": "text", "text": "hi" }
            }))
            .unwrap()
        };
        let id = v7();
        let user = |blocks: Vec<v1::Block>| {
            v1::NewMessage::User(v1::UserMessage {
                author_user_id: v7(),
                blocks,
            })
        };
        let refused =
            judge_content(v7(), &StoreBody::V1(&user(vec![block(id), block(id)]))).unwrap_err();
        assert_eq!(refused.code, ReasonCode::KeyReused);
        assert_eq!(refused.params["block_id"], serde_json::json!(id));
        let refused =
            judge_content(v7(), &StoreBody::V1(&user(vec![block(Uuid::new_v4())]))).unwrap_err();
        assert_eq!(refused.code, ReasonCode::IdNotUuidV7);
        let ai = v1::NewMessage::Ai(v1::AiMessage {
            author: v1::AiAuthor {
                id: v7().to_string(),
                name: "Analyst".into(),
            },
            delivery: v1::AiDelivery::Completed {
                items: vec![v1::Item::Step(v1::StepItem {
                    stop_reason: serde_json::json!({ "type": "end_turn" }),
                    usage: None,
                    model: None,
                    blocks: vec![block(Uuid::new_v4())],
                })],
            },
        });
        assert_eq!(
            judge_content(v7(), &StoreBody::V1(&ai)).unwrap_err().code,
            ReasonCode::IdNotUuidV7
        );
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
