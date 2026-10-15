//! The thread commands: `integration.cmd.threads.thread.{verb}.v1`, each
//! answered by one `thread.confirmed` or `thread.refused` naming its envelope
//! id.
//!
//! Threads judges a command on the coherence of the conversation, never on
//! rights: the commanding service took the decision. A no-op (adding a member
//! already there, removing an absent id) is confirmed and changes nothing.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::coords::{THREAD_AGGREGATE, ThreadsCommand};
use crate::status::ClosingStatus;

/// Implements [`ThreadsCommand`] for a command carrying `thread_id` and
/// `acting_user_id`.
macro_rules! thread_command {
    ($command:ty, $verb:literal) => {
        impl ThreadsCommand for $command {
            const AGGREGATE: &'static str = THREAD_AGGREGATE;
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

/// `thread.create` — a thread under a caller-minted UUIDv7.
///
/// Refused `KEY_REUSED` (the id is held), `USER_UNKNOWN` (the owner or a member
/// is not a known user still in the roster), `TITLE_TOO_LONG`,
/// `THREAD_NOT_FOUND` (the parent), `OWNER_NOT_PARENT_MEMBER` and the proposed
/// `ORIGIN_BLOCK_REQUIRED` (a parent without its origin block).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateThread {
    pub thread_id: Uuid,
    /// The human member the thread belongs to: a member from the creation, never
    /// removable. Absent, the thread has no owner, for good.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<Uuid>,
    /// The members besides the owner. A group is expanded by the commanding
    /// service.
    #[serde(default)]
    pub user_ids: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Named id lists Threads never reads (`{"kms": [..], "documents": [..]}`);
    /// an empty list is ignored.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub resources: BTreeMap<String, Vec<String>>,
    /// The thread this one was started from; no foreign key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<Uuid>,
    /// The block of the parent it was started from; required with
    /// `parent_thread_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_block_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
thread_command!(CreateThread, "create");

/// `thread.add_members` — refused `THREAD_NOT_FOUND`, or `USER_UNKNOWN`, in
/// which case no one of the list is added.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddMembers {
    pub thread_id: Uuid,
    pub user_ids: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
thread_command!(AddMembers, "add_members");

/// `thread.remove_members` — removes their read marks too. Refused
/// `THREAD_NOT_FOUND`, or `OWNER_NOT_REMOVABLE`, in which case no one is
/// removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveMembers {
    pub thread_id: Uuid,
    pub user_ids: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
thread_command!(RemoveMembers, "remove_members");

/// `thread.add_resource_ids` — ids added to one named list. Refused
/// `THREAD_NOT_FOUND`. Resources are never published; runners receive them
/// with the thread's context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddResourceIds {
    pub thread_id: Uuid,
    /// The list's name, unique in its thread.
    pub key: String,
    /// Opaque to Threads.
    pub ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
thread_command!(AddResourceIds, "add_resource_ids");

/// `thread.remove_resource_ids` — an emptied list disappears. Refused
/// `THREAD_NOT_FOUND`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveResourceIds {
    pub thread_id: Uuid,
    pub key: String,
    pub ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
thread_command!(RemoveResourceIds, "remove_resource_ids");

/// `thread.set_title` — a title set by command while the title Job runs wins:
/// Threads cancels the Job. Refused `THREAD_NOT_FOUND`, `TITLE_TOO_LONG`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetTitle {
    pub thread_id: Uuid,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
thread_command!(SetTitle, "set_title");

/// `thread.close` — a terminal status that never changes; messages are
/// refused from then on. Refused `THREAD_NOT_FOUND`, `THREAD_NOT_OPEN`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseThread {
    pub thread_id: Uuid,
    pub status: ClosingStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
thread_command!(CloseThread, "close");

/// `thread.delete` — the thread with its members, resources, messages, items,
/// blocks, artifacts (bytes included) and read marks; its running title Job is
/// cancelled and its members entry retracted. Child threads survive. Refused
/// `THREAD_NOT_FOUND`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteThread {
    pub thread_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}
thread_command!(DeleteThread, "delete");

#[cfg(test)]
mod tests {
    use br_core_integration::CommandCoords;
    use serde_json::json;

    use super::*;

    fn subject(coords: &CommandCoords) -> String {
        format!(
            "integration.cmd.{}.{}.{}.v{}",
            coords.receiver.as_str(),
            coords.aggregate.as_str(),
            coords.verb.as_str(),
            coords.version
        )
    }

    #[test]
    fn each_thread_command_has_its_coordinates_and_type() {
        assert_eq!(
            subject(&CreateThread::coords()),
            "integration.cmd.threads.thread.create.v1"
        );
        assert_eq!(
            subject(&AddMembers::coords()),
            "integration.cmd.threads.thread.add_members.v1"
        );
        assert_eq!(
            subject(&RemoveMembers::coords()),
            "integration.cmd.threads.thread.remove_members.v1"
        );
        assert_eq!(
            subject(&AddResourceIds::coords()),
            "integration.cmd.threads.thread.add_resource_ids.v1"
        );
        assert_eq!(
            subject(&RemoveResourceIds::coords()),
            "integration.cmd.threads.thread.remove_resource_ids.v1"
        );
        assert_eq!(
            subject(&SetTitle::coords()),
            "integration.cmd.threads.thread.set_title.v1"
        );
        assert_eq!(
            subject(&CloseThread::coords()),
            "integration.cmd.threads.thread.close.v1"
        );
        assert_eq!(
            subject(&DeleteThread::coords()),
            "integration.cmd.threads.thread.delete.v1"
        );
        assert_eq!(CreateThread::command_type(), "thread.create");
    }

    #[test]
    fn a_creation_travels_with_only_what_it_names() {
        let thread_id = Uuid::now_v7();
        let owner = Uuid::now_v7();
        let command = CreateThread {
            thread_id,
            owner_id: Some(owner),
            user_ids: vec![],
            title: None,
            resources: BTreeMap::from([("kms".to_string(), vec!["k1".to_string()])]),
            parent_thread_id: None,
            origin_block_id: None,
            acting_user_id: None,
        };
        let wire = serde_json::to_value(&command).unwrap();
        assert_eq!(
            wire,
            json!({
                "thread_id": thread_id,
                "owner_id": owner,
                "user_ids": [],
                "resources": { "kms": ["k1"] }
            })
        );
        assert_eq!(
            serde_json::from_value::<CreateThread>(wire).unwrap(),
            command
        );
    }

    #[test]
    fn an_ownerless_creation_decodes_from_its_minimal_form() {
        let thread_id = Uuid::now_v7();
        let command: CreateThread =
            serde_json::from_value(json!({ "thread_id": thread_id })).unwrap();
        assert_eq!(command.owner_id, None);
        assert!(command.user_ids.is_empty());
        assert!(command.resources.is_empty());
        assert_eq!(command.thread_id(), thread_id);
    }

    #[test]
    fn a_close_names_a_terminal_status_and_nothing_else() {
        let thread_id = Uuid::now_v7();
        let close: CloseThread =
            serde_json::from_value(json!({ "thread_id": thread_id, "status": "CANCELLED" }))
                .unwrap();
        assert_eq!(close.status, ClosingStatus::Cancelled);
        assert!(
            serde_json::from_value::<CloseThread>(json!({
                "thread_id": thread_id,
                "status": "OPEN"
            }))
            .is_err()
        );
    }
}
