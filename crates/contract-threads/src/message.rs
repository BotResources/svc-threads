use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::annotation::PlacedAnnotation;
use crate::coords::{MESSAGE_AGGREGATE, ThreadsCommand};
use crate::library::LibraryJson;

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

/// `message.store` v3 — a message stored at the end of its thread's list,
/// complete or opened `STREAMING`. A message names no other message: the
/// messages of a thread form one list, in the order they were stored.
///
/// `message.store` v1 and v2, which named a parent, are retired: Threads
/// answers them `message.refused` with `COMMAND_VERSION_RETIRED` and stores
/// nothing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreMessageV3 {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impersonator_id: Option<Uuid>,
    pub message: MessageTurn,
}

impl ThreadsCommand for StoreMessageV3 {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const VERB: &'static str = "store";
    const VERSION: u8 = 3;

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        self.acting_user_id
    }
}

/// The versions of `message.store` that named a parent. Threads still
/// answers them, `message.refused` with `COMMAND_VERSION_RETIRED`, and stores
/// nothing.
pub const RETIRED_STORE_VERSIONS: [u8; 2] = [1, 2];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum MessageTurn {
    #[serde(rename = "USER")]
    User(UserTurn),
    #[serde(rename = "AI")]
    Ai(AiTurn),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserTurn {
    pub entry: LibraryJson,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub annotations: Vec<PlacedAnnotation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiTurn {
    pub agent_id: Uuid,
    pub author_name: String,
    #[serde(flatten)]
    pub delivery: AiDelivery,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum AiDelivery {
    #[serde(rename = "COMPLETED")]
    Completed {
        turn: LibraryJson,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        annotations: Vec<PlacedAnnotation>,
    },
    #[serde(rename = "STREAMING")]
    Streaming,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSeal {
    pub last_seq: u64,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    pub seal: StreamSeal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
message_command!(CompleteMessage, "complete");

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompleteMessageV2 {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    pub seal: StreamSeal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure: Option<LibraryJson>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}

impl ThreadsCommand for CompleteMessageV2 {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const VERB: &'static str = "complete";
    const VERSION: u8 = 2;

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        self.acting_user_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailMessageV2 {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seal: Option<StreamSeal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure: Option<LibraryJson>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}

impl ThreadsCommand for FailMessageV2 {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const VERB: &'static str = "fail";
    const VERSION: u8 = 2;

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        self.acting_user_id
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
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

/// `message.erase` — the end of a thread erased: every message stored after
/// `message_id` (by `created_at`, then id), and `message_id` itself when
/// `including`, whoever wrote them and whatever their status, with their
/// entries, blocks, annotations and artifacts (bytes included).
///
/// Refused `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`, `THREAD_RESOLVED` and
/// `MESSAGE_NOT_FOUND` (the named message is not one of the thread's), in that
/// order. An erase that finds nothing to remove is confirmed and changes
/// nothing. Its confirmation names the named message: `status` is its status
/// and `deleted` its flag when it stays, and the status it had when it went
/// too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EraseMessages {
    pub thread_id: Uuid,
    pub message_id: Uuid,
    /// Whether the named message goes too.
    pub including: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
message_command!(EraseMessages, "erase");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EditMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    pub acting_user_id: Uuid,
    pub expected_version: u64,
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
        let store: StoreMessageV3 = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&store).unwrap(), wire);
        let MessageTurn::User(user) = &store.message else {
            panic!("a user message");
        };
        assert!(user.annotations.is_empty());
        assert_eq!(StoreMessageV3::coords().version, 3);
        assert_eq!(StoreMessageV3::command_type(), "message.store");
    }

    #[test]
    fn a_user_message_holding_annotations_still_decodes_so_that_threads_refuses_it() {
        let store: StoreMessageV3 = serde_json::from_value(json!({
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
    fn the_v2_closers_carry_an_optional_closure_the_library_judges() {
        let (thread_id, message_id) = (Uuid::now_v7(), Uuid::now_v7());
        let seal = json!({ "last_seq": 4, "hash": "00" });
        let complete: CompleteMessageV2 = serde_json::from_value(json!({
            "message_id": message_id,
            "thread_id": thread_id,
            "seal": seal,
        }))
        .unwrap();
        assert_eq!(complete.closure, None);
        let fail: FailMessageV2 = serde_json::from_value(json!({
            "message_id": message_id,
            "thread_id": thread_id,
            "closure": { "type": "failed" },
        }))
        .unwrap();
        let closure = fail.closure.unwrap();
        assert!(closure.to_closure_reason().is_err());
        let named =
            LibraryJson::from_json(json!({ "type": "budget_exhausted", "detail": "limit" }));
        assert_eq!(named.to_closure_reason().unwrap().detail(), Some("limit"));
        assert!(
            LibraryJson::from_json(json!({ "type": "no_such_reason" }))
                .to_closure_reason()
                .is_err()
        );
        assert_eq!(CompleteMessageV2::coords().version, 2);
        assert_eq!(FailMessageV2::coords().version, 2);
        assert_eq!(CompleteMessageV2::command_type(), "message.complete");
        assert_eq!(FailMessageV2::command_type(), "message.fail");
    }

    #[test]
    fn each_message_command_has_its_type_and_version() {
        assert_eq!(CompleteMessage::command_type(), "message.complete");
        assert_eq!(FailMessage::command_type(), "message.fail");
        assert_eq!(CancelMessage::command_type(), "message.cancel");
        assert_eq!(DeleteMessage::command_type(), "message.delete");
        assert_eq!(EditMessage::command_type(), "message.edit");
        assert_eq!(EditMessage::coords().version, 1);
        assert_eq!(EraseMessages::command_type(), "message.erase");
        assert_eq!(EraseMessages::coords().version, 1);
        assert_eq!(CancelMessage::coords().aggregate.as_str(), "message");
    }

    #[test]
    fn a_v3_store_names_no_parent_and_an_old_parent_field_is_not_read() {
        let wire = json!({
            "message_id": Uuid::now_v7(),
            "thread_id": Uuid::now_v7(),
            "parent_message_id": Uuid::now_v7(),
            "message": { "kind": "AI", "agent_id": Uuid::now_v7(), "author_name": "Analyst", "status": "STREAMING" },
        });
        let store: StoreMessageV3 = serde_json::from_value(wire).unwrap();
        let back = serde_json::to_value(&store).unwrap();
        assert!(back.get("parent_message_id").is_none(), "{back}");
        assert_eq!(RETIRED_STORE_VERSIONS, [1, 2]);
    }

    #[test]
    fn an_erase_names_its_message_and_whether_it_goes_too() {
        let (thread_id, message_id) = (Uuid::now_v7(), Uuid::now_v7());
        let erase: EraseMessages = serde_json::from_value(json!({
            "thread_id": thread_id,
            "message_id": message_id,
            "including": true,
        }))
        .unwrap();
        assert!(erase.including);
        assert_eq!(erase.acting_user_id, None);
        assert!(
            serde_json::from_value::<EraseMessages>(json!({
                "thread_id": thread_id,
                "message_id": message_id,
            }))
            .is_err(),
            "including is required"
        );
    }
}
