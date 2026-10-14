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
//! the one that receives the chunk reader (`view::Projector` does not). A
//! stream the library refused shows as `FAILED`, with the content its failure
//! keeps ([`super::draft::close`]), from the moment the refused chunk folds —
//! the stream watch stores that failure on its next look.
//!
//! ENGINE GAP (br-service-engine v0.4.1): the chunk reader reads one stream per
//! call. A frame reads the streams of the `STREAMING` messages it renders
//! concurrently, one read each (typically one per thread); the engine would
//! need a batched `ChunkReader::states`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use contract_threads::{BlockKind, MessageStatus};
use futures_util::future::{BoxFuture, try_join_all};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::Persistence;
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::visibility::Visibility;
use service_engine::wire::Noun;
use service_engine::{JsonScalar, KeyCeiling, Page};
use sqlx::PgPool;
use uuid::Uuid;

use super::draft::{self, CloseKind, MessageDraft};
use super::state::{ItemKind, Message, MessageAuthor, MessageState, StoredBlock, StoredItem};
use super::store::{self, MessageStore};
use super::stream::MessageContent;
use crate::directory::KNOWN_USER_NAMESPACE;
use crate::principal::ThreadsPrincipal;

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

/// A block's kind, as GraphQL shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[graphql(name = "BlockKind")]
pub enum BlockKindView {
    Text,
    Thinking,
    RedactedThinking,
    Structured,
    ToolCall,
    ToolResult,
    Artifact,
    ThreadReference,
    JobReference,
    Citation,
    Error,
    Warning,
}

impl From<BlockKind> for BlockKindView {
    fn from(kind: BlockKind) -> Self {
        match kind {
            BlockKind::Text => Self::Text,
            BlockKind::Thinking => Self::Thinking,
            BlockKind::RedactedThinking => Self::RedactedThinking,
            BlockKind::Structured => Self::Structured,
            BlockKind::ToolCall => Self::ToolCall,
            BlockKind::ToolResult => Self::ToolResult,
            BlockKind::Artifact => Self::Artifact,
            BlockKind::ThreadReference => Self::ThreadReference,
            BlockKind::JobReference => Self::JobReference,
            BlockKind::Citation => Self::Citation,
            BlockKind::Error => Self::Error,
            BlockKind::Warning => Self::Warning,
        }
    }
}

/// An item's kind, as GraphQL shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[graphql(name = "ItemKind")]
pub enum ItemKindView {
    Step,
    ToolResults,
}

impl From<ItemKind> for ItemKindView {
    fn from(kind: ItemKind) -> Self {
        match kind {
            ItemKind::Step => Self::Step,
            ItemKind::ToolResults => Self::ToolResults,
        }
    }
}

/// Who wrote a message: a user (shown under the roster copy's current name) or
/// an AI (its id and name, as given by the service that stored it).
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
    pub ai_id: Option<String>,
    /// The user's current display name, or the AI's name.
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct BlockView {
    pub id: Uuid,
    pub index: i32,
    pub kind: BlockKindView,
    /// The library's JSON for a library block (its text so far while its item
    /// streams), the typed content of a Threads block.
    pub content: JsonScalar,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ItemView {
    pub index: i32,
    pub kind: ItemKindView,
    pub status: MessageStatusView,
    /// A step's stop reason, as the library's JSON.
    pub stop_reason: Option<JsonScalar>,
    /// A step's usage, as the library's JSON.
    pub usage: Option<JsonScalar>,
    pub model: Option<String>,
    pub blocks: Vec<BlockView>,
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
    pub version: i64,
    /// A user message's blocks.
    pub blocks: Vec<BlockView>,
    /// An AI message's items: while it streams, those its stream folded so far.
    pub items: Vec<ItemView>,
    /// `{ markRead: { allowed, reason? } }`.
    pub affordances: Affordances,
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
    streams: BTreeMap<Uuid, MessageDraft>,
}

static NOUNS: [NounName; 1] = [<Message as Noun>::NAME];

#[derive(Default)]
pub struct MessagesView;

impl MessagesView {
    pub const NAME: ProjectorName = ProjectorName::from_static("messages");

    /// The view of one message, its stream's content included: the one
    /// projection of every entry point.
    pub fn render(
        message: &MessageState,
        stream: Option<&MessageDraft>,
        principal: &ThreadsPrincipal,
    ) -> MessageView {
        let (status, items) = match (message.status, stream) {
            (MessageStatus::Streaming, Some(stream)) if stream.refusal().is_some() => {
                let closed = draft::close(stream, CloseKind::Fail);
                (closed.status, closed.items)
            }
            (MessageStatus::Streaming, Some(stream)) => {
                (MessageStatus::Streaming, stream.preview())
            }
            (status, _) => (status, message.items.clone()),
        };
        MessageView {
            id: message.id,
            thread_id: message.thread_id,
            parent_message_id: message.parent_message_id,
            author: author_view(&message.author),
            job_id: message.job_id,
            status: status.into(),
            deleted: message.deleted,
            created_at: message.created_at,
            version: message.version,
            blocks: message.blocks.iter().map(block_view).collect(),
            items: items.iter().map(item_view).collect(),
            affordances: message.affordances(principal),
        }
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
                    Ok(page.population(keys))
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
                let held = chunks.state::<MessageContent>(id).await?;
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
        Ok(facts
            .rows
            .get(key)
            .filter(|message| <Message as Visibility>::visible(message, principal))
            .map(|message| Self::render(message, facts.streams.get(key), principal)))
    }
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
        MessageAuthor::Ai { id, name } => AuthorView {
            kind: AuthorKindView::Ai,
            user_id: None,
            ai_id: Some(id.clone()),
            name: Some(name.clone()),
        },
    }
}

fn block_view(block: &StoredBlock) -> BlockView {
    BlockView {
        id: block.id,
        index: block.index,
        kind: block.kind.into(),
        content: async_graphql::Json(block.content.clone()),
    }
}

fn item_view(item: &StoredItem) -> ItemView {
    ItemView {
        index: item.index,
        kind: item.kind.into(),
        status: item.status.into(),
        stop_reason: item.stop_reason.clone().map(async_graphql::Json),
        usage: item.usage.clone().map(async_graphql::Json),
        model: item.model.clone(),
        blocks: item.blocks.iter().map(block_view).collect(),
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::resolver_utils::EnumType;

    use super::*;
    use crate::principal::tests::{active, person};
    use crate::slices::message::state::tests::message_in;

    #[test]
    fn every_contract_literal_shows_under_its_own_name() {
        for status in MessageStatus::ALL {
            let shown = MessageStatusView::from(*status);
            let item = MessageStatusView::items()
                .iter()
                .find(|item| item.value == shown)
                .expect("every value is an item");
            assert_eq!(item.name, status.as_str());
        }
        for kind in BlockKind::ALL {
            let shown = BlockKindView::from(*kind);
            let item = BlockKindView::items()
                .iter()
                .find(|item| item.value == shown)
                .expect("every value is an item");
            assert_eq!(item.name, kind.as_str());
        }
        for kind in [ItemKind::Step, ItemKind::ToolResults] {
            let shown = ItemKindView::from(kind);
            let item = ItemKindView::items()
                .iter()
                .find(|item| item.value == shown)
                .expect("every value is an item");
            assert_eq!(item.name, kind.as_db());
        }
    }

    #[test]
    fn an_ai_message_shows_its_items_and_its_author() {
        let member = person(false, active());
        let mut message = message_in(&[member.user().unwrap()]);
        message.items = vec![StoredItem {
            index: 0,
            kind: ItemKind::Step,
            status: MessageStatus::Completed,
            stop_reason: Some(serde_json::json!({ "type": "end_turn" })),
            usage: None,
            model: Some("m-1".into()),
            blocks: vec![StoredBlock {
                id: Uuid::now_v7(),
                index: 0,
                kind: BlockKind::Text,
                content: serde_json::json!({ "type": "text", "text": "hi" }),
            }],
        }];
        let view = MessagesView::render(&message, None, &member);
        assert_eq!(view.author.kind, AuthorKindView::Ai);
        assert_eq!(view.author.name.as_deref(), Some("Analyst"));
        assert_eq!(view.items[0].model.as_deref(), Some("m-1"));
        assert_eq!(view.items[0].blocks[0].kind, BlockKindView::Text);
        assert_eq!(
            serde_json::to_value(&view.affordances).unwrap(),
            serde_json::json!({ "markRead": { "allowed": true } })
        );
    }

    #[test]
    fn a_message_whose_stream_was_refused_shows_failed_at_once() {
        let member = person(false, active());
        let mut message = message_in(&[member.user().unwrap()]);
        message.status = MessageStatus::Streaming;
        let mut stream = MessageDraft::default();
        draft::fold(
            &mut stream,
            0,
            serde_json::json!({
                "item_index": 0,
                "kind": "STEP_EVENT",
                "event": { "type": "text_delta", "index": 0, "text": "orphan" }
            }),
        );
        assert!(stream.refusal().is_some());
        let view = MessagesView::render(&message, Some(&stream), &member);
        assert_eq!(view.status, MessageStatusView::Failed);
        assert!(view.items.is_empty());
        let open = MessagesView::render(&message, Some(&MessageDraft::default()), &member);
        assert_eq!(open.status, MessageStatusView::Streaming);
    }
}
