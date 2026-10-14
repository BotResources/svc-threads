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
//! The view renders what the store holds. The live content of a `STREAMING`
//! message is the accumulated lane's ([`super::stream`]); rendering it needs the
//! engine's chunk reader, which only the low-level projector receives — the
//! implementation pass moves this view there (see the scaffold's report).

use chrono::{DateTime, Utc};
use contract_threads::{BlockKind, MessageStatus};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::{JsonScalar, Page};
use uuid::Uuid;

use super::state::{ItemKind, Message, MessageAuthor, MessageState, StoredBlock, StoredItem};
use super::store::{self, MessageStore};
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
    /// The library's JSON for a library block, the typed content of a Threads
    /// block.
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
    /// An AI message's items.
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

#[derive(Default)]
pub struct MessagesView;

impl MessagesView {
    pub const NAME: ProjectorName = ProjectorName::from_static("messages");
}

impl Projector for MessagesView {
    type Principal = ThreadsPrincipal;
    type Noun = Message;
    type Store = MessageStore;
    type Query = MessagesWindow;
    type Out = MessageView;
    type Visibility = Message;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, ThreadsPrincipal>,
        query: &MessagesWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        match (query.thread, &query.page, cx.principal().active_user()) {
            (Some(thread), Some(page), Some(member)) => {
                let keys = store::branch_page(
                    cx.pool(),
                    member,
                    thread,
                    query.tip,
                    page.cursor().copied(),
                    cx.limit(page),
                )
                .await?;
                Ok(page.population(keys))
            }
            _ => Ok(Population::Keys(Default::default())),
        }
    }

    fn project(
        message: &MessageState,
        principal: &ThreadsPrincipal,
    ) -> Result<MessageView, EngineError> {
        Ok(MessageView {
            id: message.id,
            thread_id: message.thread_id,
            parent_message_id: message.parent_message_id,
            author: author_view(&message.author),
            job_id: message.job_id,
            status: message.status.into(),
            deleted: message.deleted,
            created_at: message.created_at,
            version: message.version,
            blocks: message.blocks.iter().map(block_view).collect(),
            items: message.items.iter().map(item_view).collect(),
            affordances: message.affordances(principal),
        })
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
        let view = MessagesView::project(&message, &member).unwrap();
        assert_eq!(view.author.kind, AuthorKindView::Ai);
        assert_eq!(view.author.name.as_deref(), Some("Analyst"));
        assert_eq!(view.items[0].model.as_deref(), Some("m-1"));
        assert_eq!(view.items[0].blocks[0].kind, BlockKindView::Text);
        assert_eq!(
            serde_json::to_value(&view.affordances).unwrap(),
            serde_json::json!({ "markRead": { "allowed": true } })
        );
    }
}
