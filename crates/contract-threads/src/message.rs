//! The message commands: `integration.cmd.threads.message.{verb}.v1`, each
//! answered by one `message.confirmed` or `message.refused` naming its envelope
//! id.
//!
//! A user message is always stored complete. An AI message is stored complete,
//! or opened `STREAMING`: its content then travels on Threads' message stream
//! as ordered chunks ([`crate::chunk`]), never as commands, and the writer
//! completes, fails or cancels it with a seal. A completed user message may be
//! edited in place by its author.
//!
//! Every message command acts on an `OPEN` thread (`THREAD_NOT_FOUND`,
//! `THREAD_NOT_OPEN`). While the thread is resolved, storing, editing and
//! deleting a message are refused `THREAD_RESOLVED`; a message already
//! `STREAMING` can still be completed, failed or cancelled.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::content::{Block, Item};
use crate::coords::{MESSAGE_AGGREGATE, ThreadsCommand};

/// Implements [`ThreadsCommand`] for a command carrying `thread_id` and
/// `acting_user_id`.
macro_rules! message_command {
    ($command:ty, $verb:literal) => {
        impl ThreadsCommand for $command {
            const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
            const VERB: &'static str = $verb;

            fn thread_id(&self) -> Uuid {
                self.thread_id
            }

            fn acting_user_id(&self) -> Option<Uuid> {
                self.acting_user_id
            }
        }
    };
}

/// `message.store` — a message on an `OPEN` thread, complete or opened
/// `STREAMING`. The first user message of an untitled thread starts its title
/// Job.
///
/// Refused `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`, `THREAD_RESOLVED`,
/// `KEY_REUSED` (the message id
/// or a block id), `MESSAGE_NOT_FOUND` (a parent nobody stored),
/// `MESSAGE_INVALID` (content the library's rules refuse, or a parent of
/// another thread), `ARTIFACT_NOT_READY`, `NOT_A_MEMBER` (a user author outside
/// the thread). No fact carries content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    /// The message it answers, in the same thread; absent only for a first
    /// message. Two messages with one parent are a fork.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_message_id: Option<Uuid>,
    /// The Job that produced it; no foreign key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
    /// The impersonator when the author acted through impersonation: recorded
    /// in the message's `Created` fact only, never on the message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impersonator_id: Option<Uuid>,
    pub message: NewMessage,
}
message_command!(StoreMessage, "store");

/// The message itself: by a user, or by an AI.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum NewMessage {
    /// A user input, always complete.
    #[serde(rename = "USER")]
    User(UserMessage),
    /// A turn of an AI, complete or opened `STREAMING`.
    #[serde(rename = "AI")]
    Ai(AiMessage),
}

/// A user input, holding its blocks directly. Its author must be a member of
/// the thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserMessage {
    pub author_user_id: Uuid,
    pub blocks: Vec<Block>,
}

/// A turn of an AI, given by the service that stores it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiMessage {
    pub author: AiAuthor,
    #[serde(flatten)]
    pub delivery: AiDelivery,
}

/// The AI that wrote a message: an id that stays when the AI is renamed, and a
/// name. Opaque to Threads, which keeps no link between an AI and a thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiAuthor {
    pub id: String,
    pub name: String,
}

impl AiAuthor {
    /// The library's author, by its rules (neither part blank).
    pub fn to_library(&self) -> Result<br_llm_messages::Author, br_llm_messages::MessageError> {
        br_llm_messages::Author::new(self.id.clone(), self.name.clone())
    }
}

/// How an AI message arrives: whole, or opened for a stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum AiDelivery {
    #[serde(rename = "COMPLETED")]
    Completed { items: Vec<Item> },
    /// Opened `STREAMING`, empty: the content follows on
    /// `stream.threads.{message_id}`.
    #[serde(rename = "STREAMING")]
    Streaming,
}

/// The seal a writer declares when it closes a stream: the sequence of the last
/// chunk it pushed (the chunk count is `last_seq + 1`) and the digest of every
/// chunk, in order ([`crate::seal_hash`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSeal {
    pub last_seq: u64,
    /// 64 lowercase hex characters ([`crate::SealDigest`]); any other value
    /// cannot match and is refused `STREAM_MISMATCH`.
    pub hash: String,
}

/// `message.complete` — a `STREAMING` message sealed at the declared last
/// chunk, its folded content stored. Refused `MESSAGE_NOT_FOUND`,
/// `MESSAGE_NOT_STREAMING`, `STREAM_MISMATCH` (the writer may send a corrected
/// completion); a step the library cannot finish fails the message instead
/// (confirmed `FAILED`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    pub seal: StreamSeal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
message_command!(CompleteMessage, "complete");

/// `message.fail` — a `STREAMING` message failed: an unfinished step is kept
/// when the library can finish it, every tool call still without a result
/// receives an interrupted error result. With a seal, the stream is sealed at
/// its last chunk after the digest check; without, on what arrived. Refused
/// `MESSAGE_NOT_FOUND`, `MESSAGE_NOT_STREAMING`, `STREAM_MISMATCH`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seal: Option<StreamSeal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
message_command!(FailMessage, "fail");

/// `message.cancel` — as [`FailMessage`], ending `CANCELLED`; a service relaying
/// a user's stop sends no seal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seal: Option<StreamSeal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
message_command!(CancelMessage, "cancel");

/// `message.delete` — on its author's gesture: content, items, blocks and
/// artifacts go, the empty node stays in the tree. An AI message is never
/// deleted. Refused `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`, `THREAD_RESOLVED`,
/// `MESSAGE_NOT_FOUND`, `NOT_THE_AUTHOR` (judged before anything else of the
/// message, so an AI message is refused it); an already deleted message is
/// confirmed and changes nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    /// Required: the deletion is the author's own gesture.
    pub acting_user_id: Uuid,
}

impl ThreadsCommand for DeleteMessage {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const VERB: &'static str = "delete";

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        Some(self.acting_user_id)
    }
}

/// `message.edit` — on its author's gesture, a `COMPLETED` user message that is
/// not deleted is rewritten in place: its blocks are replaced whole (judged as
/// a stored user message's: at least one text, no artifact), and it keeps its
/// id, its place in the tree and its descendants. Its `version` is raised and
/// its `edited_at` set.
///
/// Refused, in this order: `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`,
/// `THREAD_RESOLVED`, `MESSAGE_NOT_FOUND`, `MESSAGE_NOT_EDITABLE` (an AI
/// message, one not `COMPLETED`, a deleted one), `NOT_THE_AUTHOR`,
/// `VERSION_CONFLICT` (the message left `expected_version`; the refusal names
/// the current one in `current_version`), then the content: `ID_NOT_UUIDV7`,
/// `KEY_REUSED` (a block id held by another message, or named twice),
/// `MESSAGE_INVALID`. A block id of the edited message may be kept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EditMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    /// Required: the edit is the author's own gesture.
    pub acting_user_id: Uuid,
    /// The version of the message the edit was made from.
    pub expected_version: u64,
    /// The message's new blocks, whole.
    pub blocks: Vec<Block>,
}

impl ThreadsCommand for EditMessage {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const VERB: &'static str = "edit";

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        Some(self.acting_user_id)
    }
}

#[cfg(test)]
mod tests {
    use br_llm_messages::{AssistantBlock, StopReason, Text};
    use serde_json::json;

    use super::*;
    use crate::content::{BlockBody, StepItem};

    #[test]
    fn a_user_message_travels_with_its_author_and_its_blocks() {
        let (message_id, thread_id, author, block) = (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
        );
        let command = StoreMessage {
            message_id,
            thread_id,
            parent_message_id: None,
            job_id: None,
            acting_user_id: Some(author),
            impersonator_id: None,
            message: NewMessage::User(UserMessage {
                author_user_id: author,
                blocks: vec![Block::new(
                    block,
                    BlockBody::user_text(&Text::new("hi").unwrap()).unwrap(),
                )],
            }),
        };
        let wire = serde_json::to_value(&command).unwrap();
        assert_eq!(
            wire,
            json!({
                "message_id": message_id,
                "thread_id": thread_id,
                "acting_user_id": author,
                "message": {
                    "kind": "USER",
                    "author_user_id": author,
                    "blocks": [{
                        "id": block,
                        "kind": "TEXT",
                        "content": { "type": "text", "text": "hi" }
                    }]
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<StoreMessage>(wire).unwrap(),
            command
        );
    }

    #[test]
    fn an_ai_message_is_stored_whole_or_opened_streaming() {
        let streaming = NewMessage::Ai(AiMessage {
            author: AiAuthor {
                id: "a1".into(),
                name: "Analyst".into(),
            },
            delivery: AiDelivery::Streaming,
        });
        let wire = serde_json::to_value(&streaming).unwrap();
        assert_eq!(
            wire,
            json!({ "kind": "AI", "author": { "id": "a1", "name": "Analyst" }, "status": "STREAMING" })
        );
        assert_eq!(
            serde_json::from_value::<NewMessage>(wire).unwrap(),
            streaming
        );

        let whole = NewMessage::Ai(AiMessage {
            author: AiAuthor {
                id: "a1".into(),
                name: "Analyst".into(),
            },
            delivery: AiDelivery::Completed {
                items: vec![Item::Step(StepItem {
                    stop_reason: StopReason::EndTurn,
                    usage: None,
                    model: None,
                    blocks: vec![Block::new(
                        Uuid::now_v7(),
                        BlockBody::assistant(&AssistantBlock::Text {
                            text: Text::new("done").unwrap(),
                        })
                        .unwrap(),
                    )],
                })],
            },
        });
        let wire = serde_json::to_value(&whole).unwrap();
        assert_eq!(wire["status"], "COMPLETED");
        assert_eq!(wire["items"][0]["kind"], "STEP");
        assert_eq!(serde_json::from_value::<NewMessage>(wire).unwrap(), whole);
    }

    #[test]
    fn a_blank_ai_author_decodes_and_is_refused_by_the_library() {
        let author = AiAuthor {
            id: String::new(),
            name: "Analyst".into(),
        };
        assert!(author.to_library().is_err());
    }

    #[test]
    fn a_fail_without_a_seal_is_sealed_on_what_arrived() {
        let thread_id = Uuid::now_v7();
        let message_id = Uuid::now_v7();
        let fail: FailMessage = serde_json::from_value(json!({
            "message_id": message_id,
            "thread_id": thread_id,
        }))
        .unwrap();
        assert_eq!(fail.seal, None);
        let complete: CompleteMessage = serde_json::from_value(json!({
            "message_id": message_id,
            "thread_id": thread_id,
            "seal": { "last_seq": 4, "hash": "ab" },
        }))
        .unwrap();
        assert_eq!(complete.seal.last_seq, 4);
        assert!(
            serde_json::from_value::<CompleteMessage>(json!({
                "message_id": message_id,
                "thread_id": thread_id,
            }))
            .is_err(),
            "a completion always carries its seal"
        );
    }

    #[test]
    fn a_deletion_always_names_its_acting_user() {
        assert_eq!(DeleteMessage::command_type(), "message.delete");
        assert!(
            serde_json::from_value::<DeleteMessage>(json!({
                "message_id": Uuid::now_v7(),
                "thread_id": Uuid::now_v7(),
            }))
            .is_err()
        );
    }

    #[test]
    fn an_edit_names_its_author_its_version_and_its_new_blocks() {
        assert_eq!(EditMessage::command_type(), "message.edit");
        assert_eq!(EditMessage::coords().verb.as_str(), "edit");
        let (message_id, thread_id, author, block) = (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
        );
        let edit: EditMessage = serde_json::from_value(json!({
            "message_id": message_id,
            "thread_id": thread_id,
            "acting_user_id": author,
            "expected_version": 3,
            "blocks": [{ "id": block, "kind": "TEXT", "content": { "type": "text", "text": "hi" } }]
        }))
        .unwrap();
        assert_eq!(edit.expected_version, 3);
        assert_eq!(edit.acting_user_id(), Some(author));
        assert!(
            serde_json::from_value::<EditMessage>(json!({
                "message_id": message_id,
                "thread_id": thread_id,
                "acting_user_id": author,
                "blocks": []
            }))
            .is_err(),
            "an edit always names the version it was made from"
        );
    }

    #[test]
    fn each_message_command_has_its_type() {
        assert_eq!(StoreMessage::command_type(), "message.store");
        assert_eq!(CompleteMessage::command_type(), "message.complete");
        assert_eq!(FailMessage::command_type(), "message.fail");
        assert_eq!(CancelMessage::command_type(), "message.cancel");
        assert_eq!(StoreMessage::coords().verb.as_str(), "store");
        assert_eq!(CancelMessage::coords().aggregate.as_str(), "message");
    }
}
