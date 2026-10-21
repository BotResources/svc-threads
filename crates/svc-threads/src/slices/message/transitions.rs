//! The guarded transitions of a message: stored (whole, or opened
//! `STREAMING`), closed (completed, failed or cancelled), deleted by its
//! author. Each is the one decision of its command and records the facts of
//! the change: never the content of the message, only its kinds, positions,
//! author and status.

use chrono::{DateTime, Utc};
use contract_threads::{MessageStatus, ReasonCode};
use serde::Serialize;
use service_engine::error::EngineError;
use uuid::Uuid;

use super::draft::Closed;
use super::state::{MessageAuthor, MessageChange, MessageState, StoredBlock, StoredItem};
use crate::command::Refusal;
use crate::facts::{Fact, FactMeta};
use crate::slices::thread::transitions::Applied;

// The fact types of the noun `message`.
pub const CREATED: &str = "Created";
pub const ITEM_ADDED: &str = "ItemAdded";
pub const COMPLETED: &str = "Completed";
pub const FAILED: &str = "Failed";
pub const CANCELLED: &str = "Cancelled";
pub const EDITED: &str = "Edited";
pub const DELETED: &str = "Deleted";

#[derive(Serialize)]
struct AuthorShape<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ai_id: Option<&'a str>,
}

#[derive(Serialize)]
pub(super) struct BlockShape {
    index: i32,
    kind: &'static str,
}

#[derive(Serialize)]
struct CreatedFact<'a> {
    author: AuthorShape<'a>,
    parent_message_id: Option<Uuid>,
    job_id: Option<Uuid>,
    status: &'static str,
    blocks: Vec<BlockShape>,
}

#[derive(Serialize)]
struct ItemFact {
    index: i32,
    kind: &'static str,
    status: &'static str,
    blocks: Vec<BlockShape>,
}

#[derive(Serialize)]
struct ClosedFact<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
}

#[derive(Serialize)]
struct NoPayload {}

/// The kinds and positions of blocks — what a fact may say of content.
pub(super) fn shapes(blocks: &[StoredBlock]) -> Vec<BlockShape> {
    blocks
        .iter()
        .map(|block| BlockShape {
            index: block.index,
            kind: block.kind.as_str(),
        })
        .collect()
}

fn item_fact(item: &StoredItem, meta: &FactMeta) -> Result<Fact, EngineError> {
    Fact::new(
        ITEM_ADDED,
        &ItemFact {
            index: item.index,
            kind: item.kind.as_db(),
            status: item.status.as_str(),
            blocks: shapes(&item.blocks),
        },
        meta,
    )
}

/// What a `message.store` stores, once judged.
pub struct NewMessage {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub parent_message_id: Option<Uuid>,
    pub author: MessageAuthor,
    pub job_id: Option<Uuid>,
    /// `COMPLETED` with its content, or `STREAMING` with none yet.
    pub status: MessageStatus,
    pub blocks: Vec<StoredBlock>,
    pub items: Vec<StoredItem>,
    pub created_at: DateTime<Utc>,
}

/// A message stored at version 1: `Created`, and `ItemAdded` for every item of
/// an AI message given whole.
pub fn store(new: NewMessage, meta: &FactMeta) -> Result<MessageState, EngineError> {
    let author = match &new.author {
        MessageAuthor::User { user_id, .. } => AuthorShape {
            user_id: Some(*user_id),
            ai_id: None,
        },
        MessageAuthor::Ai { id, .. } => AuthorShape {
            user_id: None,
            ai_id: Some(id),
        },
    };
    let mut pending = vec![MessageChange::Recorded(Fact::new(
        CREATED,
        &CreatedFact {
            author,
            parent_message_id: new.parent_message_id,
            job_id: new.job_id,
            status: new.status.as_str(),
            blocks: shapes(&new.blocks),
        },
        meta,
    )?)];
    for item in &new.items {
        pending.push(MessageChange::Recorded(item_fact(item, meta)?));
    }
    if new.status == MessageStatus::Completed {
        pending.push(MessageChange::ContentWritten);
    }
    Ok(MessageState {
        id: new.id,
        thread_id: new.thread_id,
        parent_message_id: new.parent_message_id,
        author: new.author,
        job_id: new.job_id,
        status: new.status,
        deleted: false,
        created_at: new.created_at,
        edited_at: None,
        version: 1,
        blocks: new.blocks,
        items: new.items,
        members: Default::default(),
        pending,
    })
}

impl MessageState {
    /// `MESSAGE_NOT_STREAMING` when the message is completed, failed or
    /// cancelled: it never changes again.
    pub fn not_streaming(&self) -> Option<Refusal> {
        (self.status != MessageStatus::Streaming).then(|| {
            Refusal::new(ReasonCode::MessageNotStreaming).with("status", self.status.as_str())
        })
    }

    /// Closes a `STREAMING` message with what its stream closed to: its items
    /// stored, each recorded (`ItemAdded`), then its end. `reason` names why a
    /// message failed when nobody asked for it (a refused chunk, a silent
    /// writer, an unfinishable completion).
    pub fn close(
        &mut self,
        closed: Closed,
        reason: Option<&str>,
        meta: &FactMeta,
    ) -> Result<Result<(), Refusal>, EngineError> {
        if let Some(refusal) = self.not_streaming() {
            return Ok(Err(refusal));
        }
        self.status = closed.status;
        self.items = closed.items;
        self.version += 1;
        self.pending.push(MessageChange::ContentWritten);
        for item in &self.items {
            let fact = item_fact(item, meta)?;
            self.pending.push(MessageChange::Recorded(fact));
        }
        let end = match self.status {
            MessageStatus::Completed => COMPLETED,
            MessageStatus::Cancelled => CANCELLED,
            MessageStatus::Failed | MessageStatus::Streaming => FAILED,
        };
        let fact = Fact::new(end, &ClosedFact { reason }, meta)?;
        self.pending.push(MessageChange::Recorded(fact));
        Ok(Ok(()))
    }

    /// Deleted on its author's own gesture: its content goes, its node stays so
    /// that its forks remain readable. An AI message is never deleted: no user
    /// is its author. A deleted message deleted again changes nothing.
    pub fn delete(
        &mut self,
        acting_user: Uuid,
        meta: &FactMeta,
    ) -> Result<Result<Applied, Refusal>, EngineError> {
        if self.author_user() != Some(acting_user) {
            return Ok(Err(
                Refusal::new(ReasonCode::NotTheAuthor).with("acting_user_id", acting_user)
            ));
        }
        if self.deleted {
            return Ok(Ok(Applied::Unchanged));
        }
        self.deleted = true;
        self.blocks.clear();
        self.items.clear();
        self.version += 1;
        self.pending.push(MessageChange::ContentRemoved);
        let fact = Fact::new(DELETED, &NoPayload {}, meta)?;
        self.pending.push(MessageChange::Recorded(fact));
        Ok(Ok(Applied::Changed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slices::message::state::tests::message_in;

    fn meta() -> FactMeta {
        FactMeta {
            actor: br_core_integration::Actor::Service(Uuid::now_v7().into()),
            acting_user_id: None,
            impersonator_id: None,
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            occurred_at: Utc::now(),
        }
    }

    fn by_user(user: Uuid) -> MessageState {
        let mut message = message_in(&[user]);
        message.author = MessageAuthor::User {
            user_id: user,
            display_name: Some("Ada".into()),
            in_roster: true,
        };
        message
    }

    #[test]
    fn only_its_author_deletes_a_message_and_once() {
        let author = Uuid::now_v7();
        let mut message = by_user(author);
        let other = Uuid::now_v7();
        assert_eq!(
            message.delete(other, &meta()).unwrap().unwrap_err().code,
            ReasonCode::NotTheAuthor
        );
        assert_eq!(
            message.delete(author, &meta()).unwrap(),
            Ok(Applied::Changed)
        );
        assert!(message.deleted);
        assert_eq!(message.version, 2);
        assert_eq!(
            message.delete(author, &meta()).unwrap(),
            Ok(Applied::Unchanged)
        );
        assert_eq!(message.version, 2);
    }

    #[test]
    fn an_ai_message_is_never_deleted() {
        let mut message = message_in(&[Uuid::now_v7()]);
        assert_eq!(
            message
                .delete(Uuid::now_v7(), &meta())
                .unwrap()
                .unwrap_err()
                .code,
            ReasonCode::NotTheAuthor
        );
    }

    #[test]
    fn a_closed_message_never_closes_again() {
        let mut message = message_in(&[Uuid::now_v7()]);
        message.status = MessageStatus::Streaming;
        let closed = Closed {
            status: MessageStatus::Cancelled,
            items: Vec::new(),
            failure: None,
            cut: None,
        };
        assert_eq!(
            message.close(closed.clone(), None, &meta()).unwrap(),
            Ok(())
        );
        assert_eq!(message.status, MessageStatus::Cancelled);
        assert_eq!(
            message
                .close(closed, None, &meta())
                .unwrap()
                .unwrap_err()
                .code,
            ReasonCode::MessageNotStreaming
        );
    }

    #[test]
    fn no_fact_carries_content() {
        let author = Uuid::now_v7();
        let block = StoredBlock {
            id: Uuid::now_v7(),
            index: 0,
            kind: contract_threads::BlockKind::Text,
            content: serde_json::json!({ "type": "text", "text": "a secret" }),
        };
        let message = store(
            NewMessage {
                id: Uuid::now_v7(),
                thread_id: Uuid::now_v7(),
                parent_message_id: None,
                author: MessageAuthor::User {
                    user_id: author,
                    display_name: Some("Ada".into()),
                    in_roster: true,
                },
                job_id: None,
                status: MessageStatus::Completed,
                blocks: vec![block],
                items: Vec::new(),
                created_at: Utc::now(),
            },
            &meta(),
        )
        .unwrap();
        for fact in message.pending_facts() {
            let payload = fact.payload.to_string();
            assert!(!payload.contains("a secret"), "{payload}");
            assert!(!payload.contains("Ada"), "{payload}");
        }
        assert_eq!(message.pending_facts()[0].event_type, CREATED);
    }
}
