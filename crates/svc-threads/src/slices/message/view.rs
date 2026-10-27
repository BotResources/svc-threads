//! The messages of a thread, page by page along a branch, for the queries, the
//! snapshot and the live deltas: one projection, [`MessagesView`], whatever the
//! entry point.
//!
//! A window is one page of one branch of one thread, the newest first: the
//! active branch (ending at the thread's most recent message) unless the window
//! names another tip, the head page unless it names a `before` cursor. A
//! deleted message stays in its branch, without content, so that its forks
//! remain readable.
//!
//! A `STREAMING` message shows the content its stream folded so far, read from
//! the engine's accumulated lane: the view is the engine's low-level projector,
//! the one that receives the chunk reader (`view::Projector` does not). Its
//! status is always its row's: a stream the library refused shows what folded
//! before the refused chunk until the stream watch stores the failure, so that
//! what a member sees and what the facts and answers say never disagree.
//!
//! ENGINE GAP (br-service-engine v0.4.2): the chunk reader reads one stream per
//! call. A frame reads the streams of the `STREAMING` messages it renders
//! concurrently, one read each (typically one per thread); the engine would
//! need a batched `ChunkReader::states`.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use contract_threads::llm::{DraftSnapshot, RepresentationState, Turn, TurnKind};
use contract_threads::{AnnotationKind, MessageStatus};
use futures_util::future::{BoxFuture, try_join_all};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::impact::Dims;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::Persistence;
use service_engine::population::{Interest, Inverse, Population, WindowQuery};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::visibility::Visibility;
use service_engine::wire::Noun;
use service_engine::{JsonScalar, KeyCeiling, Page};
use sqlx::PgPool;
use uuid::Uuid;

use super::closing::turn_of;
use super::content::Content;
use super::draft::TurnDraft;
use super::state::{Message, MessageAuthor, MessageState};
use super::store::{self, MessageStore};
use super::stream::{MessageStream, TurnContent};
use crate::directory::KNOWN_USER_NAMESPACE;
use crate::fault::{corrupt, encode};
use crate::principal::{MEMBERSHIP_DEPS, ROSTER_DEPS, ThreadsPrincipal};
use crate::slices::thread::state::UnknownStoredValue;

/// A message's status, as GraphQL shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[graphql(name = "MessageStatus")]
pub enum MessageStatusView {
    Streaming,
    Completed,
    Failed,
    Cancelled,
}

impl From<MessageStatus> for MessageStatusView {
    fn from(status: MessageStatus) -> Self {
        match status {
            MessageStatus::Streaming => Self::Streaming,
            MessageStatus::Completed => Self::Completed,
            MessageStatus::Failed => Self::Failed,
            MessageStatus::Cancelled => Self::Cancelled,
        }
    }
}

/// A turn's kind, as GraphQL shows the library's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[graphql(name = "TurnKind")]
pub enum TurnKindView {
    InputTurn,
    AiTurn,
}

/// The state of an entry, as GraphQL shows the library's record states: a
/// response being produced, an entry complete, or a response interrupted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[graphql(name = "EntryState")]
pub enum EntryStateView {
    Producing,
    Complete,
    Interrupted,
}

/// An annotation's kind, as GraphQL shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[graphql(name = "AnnotationKind")]
pub enum AnnotationKindView {
    Artifact,
    ThreadReference,
    JobReference,
    Error,
    Warning,
}

impl From<AnnotationKind> for AnnotationKindView {
    fn from(kind: AnnotationKind) -> Self {
        match kind {
            AnnotationKind::Artifact => Self::Artifact,
            AnnotationKind::ThreadReference => Self::ThreadReference,
            AnnotationKind::JobReference => Self::JobReference,
            AnnotationKind::Error => Self::Error,
            AnnotationKind::Warning => Self::Warning,
        }
    }
}

/// A kind or a state of a later library, which this build cannot show.
fn unshown(column: &'static str, value: &str) -> EngineError {
    corrupt(UnknownStoredValue {
        column,
        value: value.to_owned(),
    })
}

fn turn_kind_view(kind: TurnKind) -> Result<TurnKindView, EngineError> {
    Ok(match kind {
        TurnKind::InputTurn => TurnKindView::InputTurn,
        TurnKind::AITurn => TurnKindView::AiTurn,
        other => return Err(unshown("message.turn_kind", other.as_str())),
    })
}

fn state_view(state: RepresentationState) -> Result<EntryStateView, EngineError> {
    Ok(match state {
        RepresentationState::Producing => EntryStateView::Producing,
        RepresentationState::Complete => EntryStateView::Complete,
        RepresentationState::Interrupted => EntryStateView::Interrupted,
        other => return Err(unshown("message_entry.state", other.as_str())),
    })
}

/// Who wrote a message: a user (shown under the roster copy's current name) or
/// an AI (its agent id and name, as given by the service that stored it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[graphql(name = "AuthorKind")]
pub enum AuthorKindView {
    User,
    Ai,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct AuthorView {
    pub kind: AuthorKindView,
    pub user_id: Option<Uuid>,
    /// An AI author's agent id: the library's, a UUIDv7.
    pub ai_id: Option<String>,
    /// The user's current display name, or the AI's name.
    pub name: Option<String>,
}

/// One annotation after an entry: a Threads block, not part of the library's
/// conversation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct AnnotationView {
    pub id: Uuid,
    /// Its place among the annotations after its entry, from 0.
    pub index: i32,
    pub kind: AnnotationKindView,
    /// Its content, typed by its kind.
    pub content: JsonScalar,
}

/// One entry of a message's turn, with the annotations placed after it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct EntryView {
    pub id: Uuid,
    /// Its position in the turn, from 0.
    pub index: i32,
    /// The library's entry kind: `human_message`, `model_response`, …
    pub kind: String,
    pub state: EntryStateView,
    /// A complete entry: the library's JSON for the entry. A response still
    /// produced or interrupted: the library's draft snapshot
    /// (`br-llm-messages-draft/1`) of what arrived.
    pub content: JsonScalar,
    pub annotations: Vec<AnnotationView>,
}

/// One message as a member of its thread sees it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct MessageView {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub parent_message_id: Option<Uuid>,
    pub author: AuthorView,
    pub job_id: Option<Uuid>,
    pub status: MessageStatusView,
    pub deleted: bool,
    pub created_at: DateTime<Utc>,
    /// When its author last edited it; `null` until the first edit.
    pub edited_at: Option<DateTime<Utc>>,
    pub version: i64,
    /// The library's kind of the message's turn.
    pub turn_kind: TurnKindView,
    /// The entries of its turn, in order: while it streams, those its stream
    /// folded so far, a response still produced included; none once deleted.
    pub entries: Vec<EntryView>,
    /// `{ markRead: { allowed, reason? } }`.
    pub affordances: Affordances,
}

fn json_of<T: Serialize>(value: &T, what: &'static str) -> Result<JsonScalar, EngineError> {
    serde_json::to_value(value)
        .map(async_graphql::Json)
        .map_err(|source| encode(what, source))
}

/// The annotations placed after `entry`, as members see them.
fn annotation_views(content: &Content, entry: Uuid) -> Result<Vec<AnnotationView>, EngineError> {
    content
        .annotations
        .iter()
        .filter(|placed| placed.after_entry_id == entry)
        .enumerate()
        .map(|(index, placed)| {
            let mut body = serde_json::to_value(&placed.annotation.body)
                .map_err(|source| encode("an annotation", source))?;
            Ok(AnnotationView {
                id: placed.annotation.id,
                index: index as i32,
                kind: placed.annotation.kind().into(),
                content: async_graphql::Json(
                    body.get_mut("content")
                        .map(serde_json::Value::take)
                        .unwrap_or_default(),
                ),
            })
        })
        .collect()
}

/// The entries of a message's content, as members see them: each complete
/// entry as the library reads it back (its turn rebuilt from the records, so
/// what is shown is what the library loads), each response still produced or
/// interrupted as the library's draft snapshot.
fn entry_views(message: &MessageState, content: &Content) -> Result<Vec<EntryView>, EngineError> {
    if content.entries.is_empty() {
        return Ok(Vec::new());
    }
    let turn = content
        .to_turn(message.turn_record()?)
        .map_err(|error| stored_fault(message.id, &error))?;
    let mut complete: Vec<serde_json::Value> = match &turn {
        Turn::AITurn(ai) => ai
            .entries()
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>(),
        Turn::InputTurn(input) => input
            .entries()
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>(),
        _ => Ok(Vec::new()),
    }
    .map_err(|source| encode("an entry", source))?;
    complete.reverse();
    let mut views = Vec::with_capacity(content.entries.len());
    for (index, entry) in content.entries.iter().enumerate() {
        let state = entry.state();
        let shown = if state == RepresentationState::Complete {
            async_graphql::Json(
                complete
                    .pop()
                    .ok_or_else(|| unshown("message_entry", "an entry the turn does not hold"))?,
            )
        } else {
            let blocks = content
                .blocks
                .iter()
                .filter(|block| block.entry() == entry.id())
                .cloned()
                .collect();
            let snapshot = DraftSnapshot::new(entry.clone(), blocks)
                .map_err(|error| stored_fault(message.id, &error))?;
            json_of(&snapshot, "a draft snapshot")?
        };
        views.push(EntryView {
            id: entry.id().as_uuid(),
            index: index as i32,
            kind: entry.kind().as_str().to_owned(),
            state: state_view(state)?,
            content: shown,
            annotations: annotation_views(content, entry.id().as_uuid())?,
        });
    }
    Ok(views)
}

/// Content Threads stored that the library cannot read back: a fault of the
/// store — Threads only stores what the library loads.
fn stored_fault(message: Uuid, error: &contract_threads::llm::Error) -> EngineError {
    EngineError::Service(format!("message {message} does not load: {error}").into())
}

/// What a window of [`MessagesView`] is: one page of one branch of one thread.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessagesWindow {
    thread: Option<Uuid>,
    tip: Option<Uuid>,
    page: Option<Page<Uuid>>,
}

impl MessagesWindow {
    /// A page of the branch ending at `tip`, or of the active branch.
    pub fn of(thread: Uuid, tip: Option<Uuid>, page: Page<Uuid>) -> Self {
        Self {
            thread: Some(thread),
            tip,
            page: Some(page),
        }
    }
}

/// What a frame loads: the messages, and the folded stream of each one that
/// streams.
pub struct MessageFacts {
    rows: BTreeMap<Uuid, MessageState>,
    streams: BTreeMap<Uuid, TurnDraft>,
}

static NOUNS: [NounName; 2] = [<Message as Noun>::NAME, <MessageStream as Noun>::NAME];

#[derive(Default)]
pub struct MessagesView;

impl MessagesView {
    pub const NAME: ProjectorName = ProjectorName::from_static("messages");

    /// The view of one message, its stream's content included: the one
    /// projection of every entry point.
    ///
    /// A streaming message shows what its stream holds, judged under the
    /// message as its close will judge it: drafts included.
    pub fn render(
        message: &MessageState,
        stream: Option<&TurnDraft>,
        principal: &ThreadsPrincipal,
    ) -> Result<MessageView, EngineError> {
        let streamed = match (message.status, stream) {
            (MessageStatus::Streaming, Some(stream)) => {
                let (turn, agent, author) = turn_of(message)?;
                Some(
                    stream
                        .judge(turn, agent, &author, &HashSet::new())?
                        .content()?,
                )
            }
            _ => None,
        };
        let entries = entry_views(message, streamed.as_ref().unwrap_or(&message.content))?;
        Ok(MessageView {
            id: message.id,
            thread_id: message.thread_id,
            parent_message_id: message.parent_message_id,
            author: author_view(&message.author),
            job_id: message.job_id,
            status: message.status.into(),
            deleted: message.deleted,
            created_at: message.created_at,
            edited_at: message.edited_at,
            version: message.version,
            turn_kind: turn_kind_view(message.turn_kind)?,
            entries,
            affordances: message.affordances(principal),
        })
    }
}

impl Projector for MessagesView {
    type Principal = ThreadsPrincipal;
    type Key = Uuid;
    type Facts = MessageFacts;
    type View = MessageView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        &NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        window: &'a WindowParams,
        ceiling: KeyCeiling,
        principal: &'a ThreadsPrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let query: MessagesWindow = if window.as_value().is_null() {
                MessagesWindow::default()
            } else {
                window.decode()?
            };
            match (query.thread, &query.page, principal.active_user()) {
                (Some(thread), Some(page), Some(member)) => {
                    let limit = i64::from(page.size().get()).min(ceiling.limit());
                    let keys = store::branch_page(
                        pg,
                        member,
                        thread,
                        query.tip,
                        page.cursor().copied(),
                        limit,
                    )
                    .await?;
                    Ok(match page.cursor() {
                        None => live_head(keys),
                        Some(_) => page.population(keys),
                    })
                }
                _ => Ok(Population::Keys(BTreeSet::new())),
            }
        })
    }

    /// A change of a known user's row (their name, their standing) re-renders
    /// the messages they wrote.
    fn inverse(&self, foreign: &ForeignKey) -> Inverse<Uuid> {
        if foreign.namespace().as_str() != KNOWN_USER_NAMESPACE {
            return Inverse::None;
        }
        Inverse::Lookup(Arc::new(|conn, foreign: &ForeignKey| {
            let user = Uuid::parse_str(foreign.key().as_str());
            Box::pin(async move {
                let Ok(user) = user else {
                    return Ok(BTreeSet::new());
                };
                let messages: Vec<Uuid> =
                    sqlx::query_scalar("SELECT id FROM threads.message WHERE author_user_id = $1")
                        .bind(user)
                        .fetch_all(conn)
                        .await?;
                Ok(messages.into_iter().collect())
            })
        }))
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, ThreadsPrincipal>,
    ) -> BoxFuture<'a, Result<MessageFacts, EngineError>> {
        Box::pin(async move {
            let keys = scope.keys().to_vec();
            let chunks = scope.chunks();
            let rows = match scope {
                LoadScope::Bulk { pg, .. } => {
                    let mut conn = pg.acquire().await.map_err(EngineError::from)?;
                    MessageStore::read_many(&mut conn, &keys).await?
                }
                LoadScope::PerPrincipal { conn, .. } => {
                    MessageStore::read_many(conn, &keys).await?
                }
            };
            let rows: BTreeMap<Uuid, MessageState> = rows.into_iter().collect();
            let streaming: Vec<Uuid> = rows
                .values()
                .filter(|message| message.status == MessageStatus::Streaming)
                .map(|message| message.id)
                .collect();
            let streams = try_join_all(streaming.iter().map(|id| async move {
                let held = chunks.state::<TurnContent>(id).await?;
                Ok::<_, EngineError>((*id, held.state))
            }))
            .await?;
            Ok(MessageFacts {
                rows,
                streams: streams.into_iter().collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &MessageFacts,
        key: &Uuid,
        principal: &ThreadsPrincipal,
    ) -> Result<Option<MessageView>, EngineError> {
        facts
            .rows
            .get(key)
            .filter(|message| <Message as Visibility>::visible(message, principal))
            .map(|message| Self::render(message, facts.streams.get(key), principal))
            .transpose()
    }
}

/// The head page of a branch, live: repopulated when a message changes (a new
/// message enters, a fork moves the branch) or the caller's membership or
/// standing does — never when a stream folds, which re-renders the streaming
/// message only (`message_stream` noun, a member of the window by key).
///
/// An open head (`Population::Ordered`) would be repopulated by every impact on
/// any noun the projector renders, folds included; a query window is
/// repopulated only by the impacts its interest names. Its keys are a set: the
/// client orders the branch by its messages' dates.
fn live_head(keys: Vec<Uuid>) -> Population<Uuid> {
    let interest = Interest::new()
        .on_noun(<Message as Noun>::NAME, Dims::EMPTY)
        .on_deps(ROSTER_DEPS.union(MEMBERSHIP_DEPS));
    Population::Query(WindowQuery::new(interest, Arc::new(|_, _| false)).with_keys(keys))
}

fn author_view(author: &MessageAuthor) -> AuthorView {
    match author {
        MessageAuthor::User {
            user_id,
            display_name,
            ..
        } => AuthorView {
            kind: AuthorKindView::User,
            user_id: Some(*user_id),
            ai_id: None,
            name: display_name.clone(),
        },
        MessageAuthor::Agent { agent, name } => AuthorView {
            kind: AuthorKindView::Ai,
            user_id: None,
            ai_id: Some(agent.to_string()),
            name: Some(name.as_str().to_owned()),
        },
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::resolver_utils::EnumType;
    use contract_threads::llm::{
        BlockId, BlockStart, DeltaOp, EntryId, HumanId, HumanMessage, InputTurn, PlainBlock,
        ResponseDelta, ResponseDraft, ResponseMetadata, TextBlock, TurnEntry,
    };
    use contract_threads::{Chunk, MessageStatus};

    use super::*;
    use crate::principal::tests::{active, person};
    use crate::slices::message::draft;
    use crate::slices::message::state::tests::message_in;

    fn literal<T: EnumType + PartialEq + Copy>(shown: T) -> &'static str {
        T::items()
            .iter()
            .find(|item| item.value == shown)
            .expect("every value is an item")
            .name
    }

    #[test]
    fn every_contract_and_library_literal_shows_under_its_own_name() {
        for status in MessageStatus::ALL {
            assert_eq!(literal(MessageStatusView::from(*status)), status.as_str());
        }
        for state in RepresentationState::ALL {
            assert_eq!(
                literal(state_view(*state).unwrap()),
                state.as_str().to_uppercase()
            );
        }
        for kind in TurnKind::ALL {
            assert_eq!(
                literal(turn_kind_view(*kind).unwrap()),
                kind.as_str().to_uppercase()
            );
        }
        for kind in AnnotationKind::ALL {
            assert_eq!(literal(AnnotationKindView::from(*kind)), kind.as_str());
        }
    }

    #[test]
    fn a_streaming_message_shows_the_response_its_writer_is_producing_as_its_draft() {
        let member = person(false, active());
        let mut message = message_in(&[member.user().unwrap()]);
        message.status = MessageStatus::Streaming;
        let (turn, entry, block) = (
            message.turn_id().unwrap(),
            EntryId::from_uuid(Uuid::now_v7()).unwrap(),
            BlockId::from_uuid(Uuid::now_v7()).unwrap(),
        );
        let mut stream = TurnDraft::default();
        let deltas = [
            DeltaOp::response_started(ResponseMetadata::new()),
            DeltaOp::block_started(block, BlockStart::text()),
            DeltaOp::text_appended(block, "so far"),
        ];
        for (seq, op) in deltas.into_iter().enumerate() {
            let chunk = Chunk::delta(&ResponseDelta::new(turn, entry, op)).unwrap();
            draft::fold(
                &mut stream,
                seq as u64,
                serde_json::to_value(chunk).unwrap(),
            );
        }
        let view = MessagesView::render(&message, Some(&stream), &member).unwrap();
        assert_eq!(view.status, MessageStatusView::Streaming);
        assert_eq!(view.entries.len(), 1);
        assert_eq!(view.entries[0].kind, "model_response");
        assert_eq!(view.entries[0].state, EntryStateView::Producing);
        let snapshot: DraftSnapshot =
            serde_json::from_value(view.entries[0].content.0.clone()).unwrap();
        let restored = ResponseDraft::from_snapshot(&snapshot).unwrap();
        assert_eq!(restored.view().slots()[0].text(), Some("so far"));
        assert_eq!(
            serde_json::to_value(&view.affordances).unwrap(),
            serde_json::json!({ "markRead": { "allowed": true } })
        );
    }

    #[test]
    fn a_complete_entry_shows_as_the_librarys_entry() {
        let member = person(false, active());
        let user = member.user().unwrap();
        let mut message = message_in(&[user]);
        message.author = MessageAuthor::User {
            user_id: user,
            display_name: Some("Ada".into()),
            in_roster: true,
        };
        message.turn_kind = TurnKind::InputTurn;
        let said = HumanMessage::new(
            EntryId::from_uuid(Uuid::now_v7()).unwrap(),
            HumanId::from_uuid(user).unwrap(),
            vec![
                PlainBlock::try_from(
                    TextBlock::new(BlockId::from_uuid(Uuid::now_v7()).unwrap(), "hi").unwrap(),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let entry = TurnEntry::from(said.clone());
        let turn: Turn = InputTurn::new(message.turn_id().unwrap(), vec![said.into()])
            .unwrap()
            .into();
        message.content = Content::of_turn(&turn, Vec::new()).unwrap();
        let view = MessagesView::render(&message, None, &member).unwrap();
        assert_eq!(view.author.name.as_deref(), Some("Ada"));
        assert_eq!(view.turn_kind, TurnKindView::InputTurn);
        assert_eq!(view.entries[0].kind, "human_message");
        assert_eq!(view.entries[0].state, EntryStateView::Complete);
        let read: TurnEntry = serde_json::from_value(view.entries[0].content.0.clone()).unwrap();
        assert_eq!(read, entry);
        assert!(view.entries[0].annotations.is_empty());
    }
}
