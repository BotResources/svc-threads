//! `integration.cmd.threads.message.store.v1` and `.v2`: the versions of
//! `message.store` that named a parent. A thread's messages form one list
//! since 0.12, so Threads stores nothing they send. It still answers each of
//! them, `message.refused` with `COMMAND_VERSION_RETIRED`, so that a sender
//! still on them learns it at once instead of waiting for an answer that never
//! comes.
//!
//! Only the coordinates of the answer are read from the payload (the thread
//! and the message the sender named). The rest of a retired payload is never
//! judged.

use contract_threads::{ReasonCode, StoreMessageV3, ThreadsCommand};
use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use crate::command::{Call, Inbound, Refusal, answer_message};
use crate::fault::Fault;

/// A `message.store` of a retired `VERSION`, read for its answer only.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RetiredStore<const VERSION: u8> {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    #[serde(default)]
    pub acting_user_id: Option<Uuid>,
}

impl<const VERSION: u8> ThreadsCommand for RetiredStore<VERSION> {
    const AGGREGATE: &'static str = StoreMessageV3::AGGREGATE;
    const VERB: &'static str = StoreMessageV3::VERB;
    const VERSION: u8 = VERSION;

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        self.acting_user_id
    }
}

/// The one answer of a retired store, whatever the thread and the message:
/// nothing is loaded, nothing is written but the answer.
pub fn refusal(version: u8) -> Refusal {
    Refusal::new(ReasonCode::CommandVersionRetired)
        .with("version", version)
        .with("current_version", StoreMessageV3::VERSION)
}

pub fn store<'r, const VERSION: u8>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<RetiredStore<VERSION>>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        answer_message(
            cx,
            &call,
            command.thread_id,
            command.message_id,
            Err(refusal(VERSION)),
        )
    })
}

#[cfg(test)]
mod tests {
    use contract_threads::RETIRED_STORE_VERSIONS;
    use serde_json::json;

    use super::*;

    fn subject<C: ThreadsCommand>() -> String {
        let coords = C::coords();
        format!(
            "integration.cmd.{}.{}.{}.v{}",
            coords.receiver.as_str(),
            coords.aggregate.as_str(),
            coords.verb.as_str(),
            coords.version
        )
    }

    #[test]
    fn the_retired_versions_are_heard_on_their_own_subjects_as_message_store() {
        assert_eq!(RETIRED_STORE_VERSIONS, [1, 2]);
        assert_eq!(
            subject::<RetiredStore<1>>(),
            "integration.cmd.threads.message.store.v1"
        );
        assert_eq!(
            subject::<RetiredStore<2>>(),
            "integration.cmd.threads.message.store.v2"
        );
        assert_eq!(RetiredStore::<2>::command_type(), "message.store");
    }

    /// A v1 or v2 payload decodes whatever its message holds: only the answer's
    /// coordinates are read.
    #[test]
    fn a_retired_payload_is_read_for_its_answer_only() {
        let (message, thread, parent) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let v2: RetiredStore<2> = serde_json::from_value(json!({
            "message_id": message,
            "thread_id": thread,
            "parent_message_id": parent,
            "message": { "kind": "USER", "entry": { "type": "not even a library entry" } },
        }))
        .unwrap();
        assert_eq!((v2.message_id, v2.thread_id), (message, thread));
        let v1: RetiredStore<1> = serde_json::from_value(json!({
            "message_id": message,
            "thread_id": thread,
            "message": { "kind": "USER", "author_user_id": parent, "blocks": [] },
        }))
        .unwrap();
        assert_eq!(v1.acting_user_id, None);
    }

    #[test]
    fn the_refusal_names_the_retired_and_the_current_versions() {
        let refused = refusal(2);
        assert_eq!(refused.code, ReasonCode::CommandVersionRetired);
        assert_eq!(
            serde_json::Value::Object(refused.params),
            json!({ "version": 2, "current_version": 3 })
        );
    }
}
