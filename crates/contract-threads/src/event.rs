//! The answers Threads publishes: exactly one confirmation or refusal per
//! command, naming the command's envelope id, staged in the transaction of the
//! change (or of nothing, for a refusal). A redelivered command re-publishes
//! the same answer: fold them idempotently on `command_id`.
//!
//! One pair per aggregate, so that a commanding service binds one durable on
//! the aggregate it commands. An answer never carries content.

use br_core_integration::EventCoords;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::coords::{MESSAGE_AGGREGATE, THREAD_AGGREGATE, event_coords};
use crate::reason::ReasonCode;
use crate::status::MessageStatus;

/// An event Threads publishes.
pub trait ThreadsEvent {
    const AGGREGATE: &'static str;
    const FACT: &'static str;

    fn coords() -> EventCoords {
        event_coords(Self::AGGREGATE, Self::FACT)
    }

    /// The envelope's `event_type`: `{aggregate}.{fact}`.
    fn event_type() -> String {
        format!("{}.{}", Self::AGGREGATE, Self::FACT)
    }
}

/// `thread.confirmed` — a thread command was accepted and committed, or was an
/// accepted no-op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadCommandConfirmed {
    pub command_id: Uuid,
    /// The command's `command_type`, e.g. `thread.add_members`.
    pub command_type: String,
    pub thread_id: Uuid,
}

impl ThreadsEvent for ThreadCommandConfirmed {
    const AGGREGATE: &'static str = THREAD_AGGREGATE;
    const FACT: &'static str = "confirmed";
}

/// `thread.refused` — a thread command was refused and changed nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadCommandRefused {
    pub command_id: Uuid,
    pub command_type: String,
    pub thread_id: Uuid,
    pub reason_code: ReasonCode,
    /// Structured detail (e.g. the unknown user ids); the consumer localizes.
    #[serde(default)]
    pub params: serde_json::Map<String, serde_json::Value>,
}

impl ThreadsEvent for ThreadCommandRefused {
    const AGGREGATE: &'static str = THREAD_AGGREGATE;
    const FACT: &'static str = "refused";
}

/// `message.confirmed` — a message command was accepted and committed, or was
/// an accepted no-op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageCommandConfirmed {
    pub command_id: Uuid,
    pub command_type: String,
    pub thread_id: Uuid,
    pub message_id: Uuid,
    /// The message's status after the command: a completion whose step the
    /// library cannot finish answers `FAILED`.
    pub status: MessageStatus,
    pub deleted: bool,
}

impl ThreadsEvent for MessageCommandConfirmed {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const FACT: &'static str = "confirmed";
}

/// `message.refused` — a message command was refused and changed nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageCommandRefused {
    pub command_id: Uuid,
    pub command_type: String,
    pub thread_id: Uuid,
    pub message_id: Uuid,
    pub reason_code: ReasonCode,
    /// Structured detail (for `MESSAGE_INVALID`, the library's refusal and where
    /// it applies).
    #[serde(default)]
    pub params: serde_json::Map<String, serde_json::Value>,
}

impl ThreadsEvent for MessageCommandRefused {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const FACT: &'static str = "refused";
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn subject(coords: &EventCoords) -> String {
        format!(
            "integration.evt.{}.{}.{}.v{}",
            coords.producer.as_str(),
            coords.aggregate.as_str(),
            coords.fact.as_str(),
            coords.version
        )
    }

    #[test]
    fn each_answer_has_its_coordinates_and_type() {
        assert_eq!(
            subject(&ThreadCommandConfirmed::coords()),
            "integration.evt.threads.thread.confirmed.v1"
        );
        assert_eq!(
            subject(&ThreadCommandRefused::coords()),
            "integration.evt.threads.thread.refused.v1"
        );
        assert_eq!(
            subject(&MessageCommandConfirmed::coords()),
            "integration.evt.threads.message.confirmed.v1"
        );
        assert_eq!(
            subject(&MessageCommandRefused::coords()),
            "integration.evt.threads.message.refused.v1"
        );
        assert_eq!(MessageCommandRefused::event_type(), "message.refused");
    }

    #[test]
    fn a_refusal_carries_a_code_and_structured_params() {
        let refused = ThreadCommandRefused {
            command_id: Uuid::now_v7(),
            command_type: "thread.add_members".into(),
            thread_id: Uuid::now_v7(),
            reason_code: ReasonCode::UserUnknown,
            params: json!({ "user_ids": [Uuid::nil()] })
                .as_object()
                .cloned()
                .unwrap(),
        };
        let wire = serde_json::to_value(&refused).unwrap();
        assert_eq!(wire["reason_code"], "USER_UNKNOWN");
        assert_eq!(
            serde_json::from_value::<ThreadCommandRefused>(wire).unwrap(),
            refused
        );
    }

    #[test]
    fn a_confirmation_names_the_messages_resulting_status() {
        let confirmed: MessageCommandConfirmed = serde_json::from_value(json!({
            "command_id": Uuid::now_v7(),
            "command_type": "message.complete",
            "thread_id": Uuid::now_v7(),
            "message_id": Uuid::now_v7(),
            "status": "FAILED",
            "deleted": false
        }))
        .unwrap();
        assert_eq!(confirmed.status, MessageStatus::Failed);
    }
}
