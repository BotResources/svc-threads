//! A message's canonical state, as its store reads it back: the message row,
//! its author (with the roster copy's name for a user author), its blocks or
//! its items with theirs, and the members of its thread, which decide who sees
//! it — plus the changes a transition made and not yet saved
//! ([`MessageChange`]).
//!
//! While a message streams, its content is the accumulated lane's
//! ([`super::draft`]): the row holds none until the stream closes. The guarded
//! transitions live in [`super::transitions`].

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use contract_threads::{BlockKind, MessageStatus};
use service_engine::name::NounName;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use service_engine::{Cohort, Deps};
use uuid::Uuid;

use crate::facts::Fact;
use crate::principal::{MEMBERSHIP_DEPS, ROSTER_DEPS, ThreadsPrincipal};
use crate::slices::thread::state::{MEMBER_DIM, UnknownStoredValue};

/// The message noun: what a message's impacts name — the accumulated lane's
/// too, whose chunks fold into the message they belong to.
pub struct Message;

impl Noun for Message {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("message");
}

/// A message is seen by the members of its thread only, while they are active
/// users of the roster copy.
impl Visibility for Message {
    type Row = MessageState;
    type Principal = ThreadsPrincipal;

    const DEPS: Deps = ROSTER_DEPS.union(MEMBERSHIP_DEPS);

    fn cohorts(row: &MessageState) -> Cohorts {
        row.members
            .iter()
            .map(|user| Cohort::uuid(MEMBER_DIM, *user))
            .collect()
    }

    fn memberships(principal: &ThreadsPrincipal) -> Cohorts {
        principal
            .active_user()
            .map(|user| Cohort::uuid(MEMBER_DIM, user))
            .into_iter()
            .collect()
    }
}

/// Who wrote a message: exactly one of a user and an AI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageAuthor {
    /// A user, shown under the roster copy's current name (never copied).
    User {
        user_id: Uuid,
        display_name: Option<String>,
        in_roster: bool,
    },
    /// An AI, as the storing service named it; Threads does not read the id.
    Ai { id: String, name: String },
}

/// The kind of an item of an AI message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    Step,
    ToolResults,
}

impl ItemKind {
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Step => "STEP",
            Self::ToolResults => "TOOL_RESULTS",
        }
    }

    pub fn from_db(stored: &str) -> Result<Self, UnknownStoredValue> {
        match stored {
            "STEP" => Ok(Self::Step),
            "TOOL_RESULTS" => Ok(Self::ToolResults),
            other => Err(UnknownStoredValue {
                column: "message_item.kind",
                value: other.to_owned(),
            }),
        }
    }
}

/// A stored block: its kind and its content (the library's JSON, or a Threads
/// block's typed content), at its index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBlock {
    pub id: Uuid,
    pub index: i32,
    pub kind: BlockKind,
    pub content: serde_json::Value,
}

/// A stored item of an AI message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredItem {
    pub index: i32,
    pub kind: ItemKind,
    pub status: MessageStatus,
    /// A step's stop reason, usage and model, as the library's JSON.
    pub stop_reason: Option<serde_json::Value>,
    pub usage: Option<serde_json::Value>,
    pub model: Option<String>,
    pub blocks: Vec<StoredBlock>,
}

/// What a transition changed and the store has yet to write beside the message
/// row.
#[derive(Debug, Clone, PartialEq)]
pub enum MessageChange {
    /// The message's content is written: at its creation when it arrives
    /// whole, at its close when it streamed.
    ContentWritten,
    /// Its author deleted it: its content goes, the node stays. (An edit
    /// rewrites the content whole: `ContentWritten` replaces what was there.)
    ContentRemoved,
    Recorded(Fact),
}

/// One message, whole.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageState {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub parent_message_id: Option<Uuid>,
    pub author: MessageAuthor,
    pub job_id: Option<Uuid>,
    pub status: MessageStatus,
    pub deleted: bool,
    pub created_at: DateTime<Utc>,
    /// The date of its author's latest edit; `None` until the first.
    pub edited_at: Option<DateTime<Utc>>,
    pub version: i64,
    /// A user message's blocks, in order; empty for an AI message and once
    /// deleted.
    pub blocks: Vec<StoredBlock>,
    /// An AI message's items, in order; empty for a user message, and while a
    /// message streams (its content is then the accumulated lane's).
    pub items: Vec<StoredItem>,
    /// The members of the message's thread.
    pub members: BTreeSet<Uuid>,
    /// Unsaved changes, in order.
    pub pending: Vec<MessageChange>,
}

/// Whether a message of a branch is part of the conversation the branch is,
/// for a runner: not deleted, and complete, or failed after its first step. A
/// cancelled message, one still streaming and one failed before its first step
/// are not.
pub fn in_conversation(status: MessageStatus, deleted: bool, has_a_step: bool) -> bool {
    !deleted
        && match status {
            MessageStatus::Completed => true,
            MessageStatus::Failed => has_a_step,
            MessageStatus::Cancelled | MessageStatus::Streaming => false,
        }
}

impl MessageState {
    pub fn pending_facts(&self) -> Vec<Fact> {
        self.pending
            .iter()
            .filter_map(|change| match change {
                MessageChange::Recorded(fact) => Some(fact.clone()),
                _ => None,
            })
            .collect()
    }

    /// The user who wrote it, when a user did.
    pub fn author_user(&self) -> Option<Uuid> {
        match &self.author {
            MessageAuthor::User { user_id, .. } => Some(*user_id),
            MessageAuthor::Ai { .. } => None,
        }
    }

    /// Every block of the message, its own and its items'.
    pub fn all_blocks(&self) -> impl Iterator<Item = &StoredBlock> {
        self.blocks
            .iter()
            .chain(self.items.iter().flat_map(|item| item.blocks.iter()))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn message_in(members: &[Uuid]) -> MessageState {
        MessageState {
            id: Uuid::now_v7(),
            thread_id: Uuid::now_v7(),
            parent_message_id: None,
            author: MessageAuthor::Ai {
                id: "a1".into(),
                name: "Analyst".into(),
            },
            job_id: None,
            status: MessageStatus::Completed,
            deleted: false,
            created_at: Utc::now(),
            edited_at: None,
            version: 1,
            blocks: Vec::new(),
            items: Vec::new(),
            members: members.iter().copied().collect(),
            pending: Vec::new(),
        }
    }

    #[test]
    fn an_item_kind_maps_to_and_from_its_column() {
        for kind in [ItemKind::Step, ItemKind::ToolResults] {
            assert_eq!(ItemKind::from_db(kind.as_db()), Ok(kind));
        }
        assert!(ItemKind::from_db("step").is_err());
    }

    #[test]
    fn a_message_is_visible_to_the_active_members_of_its_thread() {
        use crate::principal::RosterStanding;
        use crate::principal::tests::{active, person};

        let member = person(false, active());
        let message = message_in(&[member.user().unwrap()]);
        assert!(<Message as Visibility>::visible(&message, &member));
        assert!(!<Message as Visibility>::visible(
            &message,
            &person(true, active())
        ));
        let gone = person(false, RosterStanding::Deactivated);
        assert!(!<Message as Visibility>::visible(
            &message_in(&[gone.user().unwrap()]),
            &gone
        ));
    }
}
