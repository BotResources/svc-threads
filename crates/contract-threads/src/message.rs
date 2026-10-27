//! The message commands: `integration.cmd.threads.message.{verb}.v{N}`, each
//! answered by one `message.confirmed` or `message.refused` naming its envelope
//! id.
//!
//! A message is a turn of the library's 2.0 model (schema `br-llm-messages/5`)
//! under the message's id. A user message is an input turn holding one human
//! message of a member, always stored complete. Any other turn is an AI
//! message: an AI turn of its agent, stored whole or opened `STREAMING` — its
//! content then travels on Threads' message stream as ordered chunks
//! ([`crate::chunk`]), never as commands, and the writer completes, fails or
//! cancels it with a seal — or an input turn holding other incoming entries.
//! A completed user message may be edited in place by its author.
//!
//! Every message command acts on an `OPEN` thread (`THREAD_NOT_FOUND`,
//! `THREAD_NOT_OPEN`). While the thread is resolved, storing, editing and
//! deleting a message are refused `THREAD_RESOLVED`; a message already
//! `STREAMING` can still be completed, failed or cancelled.
//!
//! The library content travels as the library's JSON ([`LibraryJson`]): what
//! the library refuses is refused `MESSAGE_INVALID`, its `error` param naming
//! the library's error code (with `object_id` and `location` when the library
//! gives them); an id the caller supplies for a new object — a message's, an
//! entry's, a block's, a citation's, an annotation's — that is no UUIDv7 is
//! `ID_NOT_UUIDV7`, while a reference that is none (an agent, a human, the call
//! a result answers) is the library's `MESSAGE_INVALID`. A string holding
//! U+0000 is `MESSAGE_INVALID` (`nul_character`): the store cannot keep it.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::annotation::PlacedAnnotation;
use crate::coords::{MESSAGE_AGGREGATE, ThreadsCommand};
use crate::library::LibraryJson;

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

/// `message.store` v2 — a message on an `OPEN` thread, a turn of the library's
/// 2.0 model under the message's id, complete or opened `STREAMING`. The first
/// user message of an untitled thread starts its title Job.
///
/// Refused, in this order: `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`,
/// `THREAD_RESOLVED`, `ID_NOT_UUIDV7` (the message id), `KEY_REUSED` (the
/// message id), `MESSAGE_NOT_FOUND` (a parent nobody stored),
/// `MESSAGE_INVALID` (a parent of another thread; an agent id that is not a
/// UUIDv7), then the content: `ID_NOT_UUIDV7` (an id of an entry, a block or a
/// citation that is not a UUIDv7), `MESSAGE_INVALID` (what else the library
/// refuses, a user message holding an annotation, an AI message holding a
/// human message, a string holding U+0000, …), `ID_NOT_UUIDV7` (an
/// annotation's id), `NOT_A_MEMBER` (a human outside the thread), `KEY_REUSED`
/// (an annotation's id its turn names too, or an id of the turn or of its
/// annotations another message already holds), `ARTIFACT_NOT_READY`. An id
/// the turn names twice is the library's `MESSAGE_INVALID` (`reused_identity`).
/// No fact carries content.
///
/// A whole AI turn is stored as the library accepts it: closed by its writer,
/// its closure gives the message's status (`completed` or `budget_exhausted`
/// → `COMPLETED`, `cancelled` → `CANCELLED`, `failed` or `interrupted` →
/// `FAILED`); holding no closure, Threads closes it completed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreMessageV2 {
    /// The message's id, which is its turn's id.
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
    pub message: MessageTurn,
}

impl ThreadsCommand for StoreMessageV2 {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const VERB: &'static str = "store";
    const VERSION: u8 = 2;

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        self.acting_user_id
    }
}

/// The message's turn: a user's, or an AI's.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum MessageTurn {
    /// The human message of a user: Threads stores it as the input turn,
    /// under the message's id, holding it. Always complete.
    #[serde(rename = "USER")]
    User(UserTurn),
    /// A turn stored by an AI.
    #[serde(rename = "AI")]
    Ai(AiTurn),
}

/// A user message: the library's human message (`{"type":"human_message",
/// …}`), whose human — a member of the thread — is the message's author, its
/// content the library's plain blocks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserTurn {
    pub entry: LibraryJson,
    /// A user message holds no annotation: any is refused `MESSAGE_INVALID`
    /// (`annotation_in_user_message`). Carried so that the refusal can answer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub annotations: Vec<PlacedAnnotation>,
}

/// A turn stored by an AI: its agent (the library's agent id, a UUIDv7 that
/// stays when the agent is renamed) and its name, as the storing service gives
/// them. Threads keeps no link between an agent and a thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiTurn {
    pub agent_id: Uuid,
    pub author_name: String,
    #[serde(flatten)]
    pub delivery: AiDelivery,
}

/// How an AI message arrives: whole, or opened for a stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum AiDelivery {
    /// The whole turn — an AI turn of `agent_id` under `author_name`, its
    /// closure giving the message's status (Threads closes it completed when
    /// it holds none), or an input turn holding incoming entries other than a
    /// human message — and its annotations, each after one of its entries.
    #[serde(rename = "COMPLETED")]
    Completed {
        turn: LibraryJson,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        annotations: Vec<PlacedAnnotation>,
    },
    /// Opened `STREAMING`, an AI turn of `agent_id` holding nothing yet: its
    /// content follows on `stream.threads.{message_id}`.
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
/// chunk, its turn closed completed. A stream the library cannot complete — a
/// response still producing, a tool call without its result — fails the
/// message instead (confirmed `FAILED`).
///
/// Refused, in this order: `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN` (a resolved
/// thread still completes a message already `STREAMING`), `MESSAGE_NOT_FOUND`,
/// `MESSAGE_NOT_STREAMING`, `ARTIFACT_NOT_READY` (an `ARTIFACT` annotation the
/// stream holds when the completion is judged names an artifact that is not
/// ready: judged before the seal, so that the refusal changes nothing — a
/// stream also holding chunks past `last_seq` is therefore refused
/// `ARTIFACT_NOT_READY` before `STREAM_MISMATCH`), `STREAM_MISMATCH` (the
/// digest, a chunk past `last_seq`, a chunk missing below it once the
/// ingestion lag is waited out; the writer may send a corrected completion).
/// An annotation the seal reaches but the stream did not yet hold when the
/// completion was judged is judged again on the command's redelivery: a
/// completion never drops an `ARTIFACT` annotation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    pub seal: StreamSeal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
message_command!(CompleteMessage, "complete");

/// `message.fail` — a `STREAMING` message failed: its turn closed failed with
/// the library's closure, a response still producing kept interrupted (shown to
/// members, never to a runner); a tool call without a result stays
/// unanswered, as the library allows on an abnormal closure. With a seal, the
/// stream is sealed at its last chunk after the digest check; without, on what
/// arrived. Refused, in this order: `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`,
/// `MESSAGE_NOT_FOUND`, `MESSAGE_NOT_STREAMING`, `STREAM_MISMATCH`. Closing on
/// what arrived, a fail or a cancel cannot be refused for an artifact: an
/// `ARTIFACT` annotation whose artifact is not ready is not kept.
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

/// `message.cancel` — as [`FailMessage`], its turn closed cancelled; a service
/// relaying a user's stop sends no seal.
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

/// `message.delete` — on its author's gesture: its content, entries, blocks,
/// annotations and artifacts go, the empty node stays in the tree. An AI
/// message is never deleted. Refused `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`,
/// `THREAD_RESOLVED`, `MESSAGE_NOT_FOUND`, `NOT_THE_AUTHOR` (judged before
/// anything else of the message, so an AI message is refused it); an already
/// deleted message is confirmed and changes nothing.
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
/// not deleted is rewritten in place: its human message's content is replaced
/// whole by `content` — the library's plain blocks (text, image, structured
/// content), at least one — and it keeps its id, its entry's id, its place in
/// the tree and its descendants. Its `version` is raised and its `edited_at`
/// set.
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
    /// The human message's new content, whole: the library's plain blocks.
    /// (A sender still on 0.4 sends `blocks` in the 0.4 form instead: Threads
    /// converts them.)
    #[serde(default)]
    pub content: Vec<LibraryJson>,
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
    use serde_json::json;

    use super::*;

    #[test]
    fn a_user_message_travels_as_its_human_message() {
        let (message_id, thread_id) = (Uuid::now_v7(), Uuid::now_v7());
        let entry = json!({ "type": "human_message", "id": Uuid::now_v7(), "human": Uuid::now_v7(), "content": [] });
        let wire = json!({
            "message_id": message_id,
            "thread_id": thread_id,
            "message": { "kind": "USER", "entry": entry },
        });
        let store: StoreMessageV2 = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&store).unwrap(), wire);
        let MessageTurn::User(user) = &store.message else {
            panic!("a user message");
        };
        assert!(user.annotations.is_empty());
        assert_eq!(StoreMessageV2::coords().version, 2);
        assert_eq!(StoreMessageV2::command_type(), "message.store");
    }

    #[test]
    fn a_user_message_holding_annotations_still_decodes_so_that_threads_refuses_it() {
        let store: StoreMessageV2 = serde_json::from_value(json!({
            "message_id": Uuid::now_v7(),
            "thread_id": Uuid::now_v7(),
            "message": {
                "kind": "USER",
                "entry": { "type": "human_message" },
                "annotations": [{
                    "after_entry_id": Uuid::now_v7(),
                    "annotation": { "id": Uuid::now_v7(), "kind": "WARNING", "content": { "code": "W" } }
                }]
            },
        }))
        .unwrap();
        let MessageTurn::User(user) = store.message else {
            panic!("a user message");
        };
        assert_eq!(user.annotations.len(), 1);
    }

    #[test]
    fn an_ai_message_is_stored_whole_with_its_annotations_or_opened_streaming() {
        let agent = Uuid::now_v7();
        let streaming: MessageTurn = serde_json::from_value(json!({
            "kind": "AI", "agent_id": agent, "author_name": "Analyst", "status": "STREAMING"
        }))
        .unwrap();
        assert_eq!(
            streaming,
            MessageTurn::Ai(AiTurn {
                agent_id: agent,
                author_name: "Analyst".into(),
                delivery: AiDelivery::Streaming,
            })
        );
        let entry = Uuid::now_v7();
        let whole: MessageTurn = serde_json::from_value(json!({
            "kind": "AI", "agent_id": agent, "author_name": "Analyst", "status": "COMPLETED",
            "turn": { "type": "ai_turn" },
            "annotations": [{
                "after_entry_id": entry,
                "annotation": { "id": Uuid::now_v7(), "kind": "ERROR", "content": { "code": "E" } }
            }]
        }))
        .unwrap();
        let MessageTurn::Ai(AiTurn {
            delivery: AiDelivery::Completed { turn, annotations },
            ..
        }) = whole
        else {
            panic!("a whole AI turn");
        };
        assert_eq!(turn.as_json()["type"], "ai_turn");
        assert_eq!(annotations[0].after_entry_id, entry);
    }

    #[test]
    fn a_completion_always_carries_its_seal_and_a_fail_may_not() {
        let (thread_id, message_id) = (Uuid::now_v7(), Uuid::now_v7());
        let fail: FailMessage = serde_json::from_value(json!({
            "message_id": message_id,
            "thread_id": thread_id,
        }))
        .unwrap();
        assert_eq!(fail.seal, None);
        assert!(
            serde_json::from_value::<CompleteMessage>(json!({
                "message_id": message_id,
                "thread_id": thread_id,
            }))
            .is_err()
        );
    }

    #[test]
    fn an_edit_names_its_author_its_version_and_its_new_content() {
        let block = Uuid::now_v7();
        let edit: EditMessage = serde_json::from_value(json!({
            "message_id": Uuid::now_v7(),
            "thread_id": Uuid::now_v7(),
            "acting_user_id": Uuid::now_v7(),
            "expected_version": 3,
            "content": [{ "type": "text", "id": block, "text": "hi" }]
        }))
        .unwrap();
        assert_eq!(edit.expected_version, 3);
        assert_eq!(edit.content.len(), 1);
    }

    #[test]
    fn each_message_command_has_its_type_and_version() {
        assert_eq!(CompleteMessage::command_type(), "message.complete");
        assert_eq!(FailMessage::command_type(), "message.fail");
        assert_eq!(CancelMessage::command_type(), "message.cancel");
        assert_eq!(DeleteMessage::command_type(), "message.delete");
        assert_eq!(EditMessage::command_type(), "message.edit");
        assert_eq!(EditMessage::coords().version, 1);
        assert_eq!(CancelMessage::coords().aggregate.as_str(), "message");
    }
}
