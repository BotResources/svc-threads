use chrono::{DateTime, Utc};
use contract_threads::llm::{ObservedAt, TurnEntryKind, TurnKind};
use contract_threads::{MessageStatus, ReasonCode};
use serde::Serialize;
use service_engine::error::EngineError;
use uuid::Uuid;

use super::close::Closed;
use super::content::Content;
use super::state::{MessageAuthor, MessageChange, MessageState};
use crate::command::Refusal;
use crate::facts::{Fact, FactMeta};
use crate::slices::thread::transitions::Applied;

pub const CREATED: &str = "Created";
pub const ENTRY_ADDED: &str = "EntryAdded";
pub const COMPLETED: &str = "Completed";
pub const FAILED: &str = "Failed";
pub const CANCELLED: &str = "Cancelled";
pub const EDITED: &str = "Edited";
pub const DELETED: &str = "Deleted";

#[derive(Serialize)]
struct AuthorShape {
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_id: Option<Uuid>,
}

#[derive(Serialize)]
pub(super) struct BlockShape {
    position: usize,
    kind: &'static str,
    state: &'static str,
}

#[derive(Serialize)]
struct AnnotationShape {
    position: usize,
    kind: &'static str,
}

#[derive(Serialize)]
struct CreatedFact<'a> {
    author: AuthorShape,
    parent_message_id: Option<Uuid>,
    job_id: Option<Uuid>,
    status: &'static str,
    turn_kind: &'a str,
    entries: Vec<EntryFact>,
}

#[derive(Serialize)]
struct EntryFact {
    position: usize,
    kind: &'static str,
    state: &'static str,
    blocks: Vec<BlockShape>,
    annotations: Vec<AnnotationShape>,
}

#[derive(Serialize)]
struct ClosedFact<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
    closure: &'static str,
}

#[derive(Serialize)]
struct NoPayload {}

pub(super) fn block_shapes(content: &Content, entry: usize) -> Vec<BlockShape> {
    let Some(row) = content.entries.get(entry) else {
        return Vec::new();
    };
    content
        .blocks
        .iter()
        .enumerate()
        .filter(|(_, block)| block.entry() == row.id())
        .map(|(position, block)| BlockShape {
            position,
            kind: block.kind().as_str(),
            state: block.state().as_str(),
        })
        .collect()
}

fn entry_facts(content: &Content, meta: &FactMeta) -> Result<Vec<Fact>, EngineError> {
    entry_shapes(content)
        .into_iter()
        .map(|shape| Fact::new(ENTRY_ADDED, &shape, meta))
        .collect()
}

fn entry_shapes(content: &Content) -> Vec<EntryFact> {
    let mut shapes = Vec::new();
    for (position, entry) in content.entries.iter().enumerate() {
        if entry.kind() == TurnEntryKind::TurnClosure {
            continue;
        }
        let annotations = content
            .annotations
            .iter()
            .filter(|placed| placed.after_entry_id == entry.id().as_uuid())
            .enumerate()
            .map(|(position, placed)| AnnotationShape {
                position,
                kind: placed.annotation.kind().as_str(),
            })
            .collect();
        shapes.push(EntryFact {
            position,
            kind: entry.kind().as_str(),
            state: entry.state().as_str(),
            blocks: block_shapes(content, position),
            annotations,
        });
    }
    shapes
}

pub struct NewMessage {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub parent_message_id: Option<Uuid>,
    pub author: MessageAuthor,
    pub job_id: Option<Uuid>,
    pub status: MessageStatus,
    pub turn_kind: TurnKind,
    pub observed_at: Option<ObservedAt>,
    pub content: Content,
    pub created_at: DateTime<Utc>,
}

pub fn store(new: NewMessage, meta: &FactMeta) -> Result<MessageState, EngineError> {
    let author = match &new.author {
        MessageAuthor::User { user_id, .. } => AuthorShape {
            user_id: Some(*user_id),
            agent_id: None,
        },
        MessageAuthor::Agent { agent, .. } => AuthorShape {
            user_id: None,
            agent_id: Some(agent.as_uuid()),
        },
    };
    let mut pending = vec![MessageChange::Recorded(Fact::new(
        CREATED,
        &CreatedFact {
            author,
            parent_message_id: new.parent_message_id,
            job_id: new.job_id,
            status: new.status.as_str(),
            turn_kind: new.turn_kind.as_str(),
            entries: entry_shapes(&new.content),
        },
        meta,
    )?)];
    if new.status != MessageStatus::Streaming {
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
        turn_kind: new.turn_kind,
        observed_at: new.observed_at,
        content: new.content,
        members: Default::default(),
        pending,
    })
}

impl MessageState {
    pub fn not_streaming(&self) -> Option<Refusal> {
        (self.status != MessageStatus::Streaming).then(|| {
            Refusal::new(ReasonCode::MessageNotStreaming).with("status", self.status.as_str())
        })
    }

    pub fn close(
        &mut self,
        closed: Closed,
        meta: &FactMeta,
    ) -> Result<Result<(), Refusal>, EngineError> {
        if let Some(refusal) = self.not_streaming() {
            return Ok(Err(refusal));
        }
        self.status = closed.status;
        self.content = closed.content;
        self.version += 1;
        self.pending.push(MessageChange::ContentWritten);
        for fact in entry_facts(&self.content, meta)? {
            self.pending.push(MessageChange::Recorded(fact));
        }
        let end = match self.status {
            MessageStatus::Completed => COMPLETED,
            MessageStatus::Cancelled => CANCELLED,
            MessageStatus::Failed | MessageStatus::Streaming => FAILED,
        };
        let fact = Fact::new(
            end,
            &ClosedFact {
                reason: closed.reason,
                closure: closed.closure.as_str(),
            },
            meta,
        )?;
        self.pending.push(MessageChange::Recorded(fact));
        Ok(Ok(()))
    }

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
        self.content = Content::default();
        self.version += 1;
        self.pending.push(MessageChange::ContentRemoved);
        let fact = Fact::new(DELETED, &NoPayload {}, meta)?;
        self.pending.push(MessageChange::Recorded(fact));
        Ok(Ok(Applied::Changed))
    }

    pub fn edit(&mut self, content: Content, meta: &FactMeta) -> Result<(), EngineError> {
        let fact = Fact::new(
            EDITED,
            &EditedFact {
                blocks: block_shapes(&content, 0),
            },
            meta,
        )?;
        self.content = content;
        self.edited_at = Some(meta.occurred_at);
        self.version += 1;
        self.pending.push(MessageChange::ContentWritten);
        self.pending.push(MessageChange::Recorded(fact));
        Ok(())
    }
}

#[derive(Serialize)]
struct EditedFact {
    blocks: Vec<BlockShape>,
}

#[cfg(test)]
mod tests {
    use contract_threads::llm::{
        BlockId, EntryId, HumanId, HumanMessage, InputTurn, PlainBlock, TextBlock, Turn, TurnId,
    };

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
        message.turn_kind = TurnKind::InputTurn;
        message
    }

    fn said(message: Uuid, user: Uuid, words: &str) -> Content {
        let text = TextBlock::new(BlockId::from_uuid(Uuid::now_v7()).unwrap(), words).unwrap();
        let human = HumanMessage::new(
            EntryId::from_uuid(Uuid::now_v7()).unwrap(),
            HumanId::from_uuid(user).unwrap(),
            vec![PlainBlock::try_from(text).unwrap()],
        )
        .unwrap();
        let turn: Turn = InputTurn::new(TurnId::from_uuid(message).unwrap(), vec![human.into()])
            .unwrap()
            .into();
        Content::of_turn(&turn, Vec::new()).unwrap()
    }

    #[test]
    fn only_its_author_deletes_a_message_and_once() {
        let author = Uuid::now_v7();
        let mut message = by_user(author);
        message.content = said(message.id, author, "a secret");
        assert_eq!(
            message
                .delete(Uuid::now_v7(), &meta())
                .unwrap()
                .unwrap_err()
                .code,
            ReasonCode::NotTheAuthor
        );
        assert_eq!(
            message.delete(author, &meta()).unwrap(),
            Ok(Applied::Changed)
        );
        assert!(message.deleted);
        assert!(message.content.is_empty());
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
    fn a_message_given_whole_writes_its_content_whatever_its_closure_and_a_stream_none() {
        let author = Uuid::now_v7();
        let id = Uuid::now_v7();
        let stored = |status: MessageStatus| {
            store(
                NewMessage {
                    id,
                    thread_id: Uuid::now_v7(),
                    parent_message_id: None,
                    author: MessageAuthor::User {
                        user_id: author,
                        display_name: None,
                        in_roster: true,
                    },
                    job_id: None,
                    status,
                    turn_kind: TurnKind::InputTurn,
                    observed_at: None,
                    content: said(id, author, "words"),
                    created_at: Utc::now(),
                },
                &meta(),
            )
            .unwrap()
        };
        for status in [
            MessageStatus::Completed,
            MessageStatus::Cancelled,
            MessageStatus::Failed,
        ] {
            assert!(
                stored(status)
                    .pending
                    .contains(&MessageChange::ContentWritten),
                "{status:?}"
            );
        }
        assert!(
            !stored(MessageStatus::Streaming)
                .pending
                .contains(&MessageChange::ContentWritten)
        );
    }

    #[test]
    fn a_closed_message_never_closes_again() {
        let mut message = message_in(&[Uuid::now_v7()]);
        message.status = MessageStatus::Streaming;
        let closed = Closed {
            status: MessageStatus::Cancelled,
            content: Content::default(),
            reason: None,
            closure: super::super::closure::Kind::Cancelled,
        };
        assert_eq!(message.close(closed.clone(), &meta()).unwrap(), Ok(()));
        assert_eq!(message.status, MessageStatus::Cancelled);
        assert_eq!(
            message.close(closed, &meta()).unwrap().unwrap_err().code,
            ReasonCode::MessageNotStreaming
        );
    }

    #[test]
    fn created_carries_the_shapes_of_the_entries_and_no_content_nor_a_name() {
        let author = Uuid::now_v7();
        let id = Uuid::now_v7();
        let message = store(
            NewMessage {
                id,
                thread_id: Uuid::now_v7(),
                parent_message_id: None,
                author: MessageAuthor::User {
                    user_id: author,
                    display_name: Some("Ada".into()),
                    in_roster: true,
                },
                job_id: None,
                status: MessageStatus::Completed,
                turn_kind: TurnKind::InputTurn,
                observed_at: None,
                content: said(id, author, "a secret"),
                created_at: Utc::now(),
            },
            &meta(),
        )
        .unwrap();
        let facts = message.pending_facts();
        assert_eq!(facts.len(), 1, "a message given whole is one fact");
        assert_eq!(facts[0].event_type, CREATED);
        let entry = &facts[0].payload["entries"][0];
        assert_eq!(entry["position"], 0);
        assert_eq!(entry["kind"], "human_message");
        assert_eq!(entry["blocks"][0]["kind"], "text");
        for fact in facts {
            let payload = fact.payload.to_string();
            assert!(!payload.contains("a secret"), "{payload}");
            assert!(!payload.contains("Ada"), "{payload}");
        }
    }

    #[test]
    fn an_edit_replaces_the_content_and_records_no_text() {
        let author = Uuid::now_v7();
        let mut message = by_user(author);
        message.content = said(message.id, author, "the old secret");
        let at = meta();
        message
            .edit(said(message.id, author, "the new secret"), &at)
            .unwrap();
        assert_eq!(message.version, 2);
        assert_eq!(message.edited_at, Some(at.occurred_at));
        let facts = message.pending_facts();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].event_type, EDITED);
        let payload = facts[0].payload.to_string();
        assert!(!payload.contains("secret"), "{payload}");
        assert_eq!(facts[0].payload["blocks"][0]["kind"], "text");
    }
}
