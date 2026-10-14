//! The mechanics every command reaction shares: the contract's command decoded
//! from its envelope, the commanding service as the only accepted sender, the
//! fact metadata a command carries, and its one answer — `confirmed` or
//! `refused` with a reason code and structured params — staged in the
//! transaction of the change (or of nothing, for a refusal).
//!
//! A reaction that refuses writes nothing before it decides: every handler
//! loads, decides, and only then writes, so the refusal commits the answer
//! alone. The engine records the answer on the command's idempotency claim, so
//! a redelivered command re-publishes the same answer and changes nothing.

use std::marker::PhantomData;

use contract_threads::{
    MessageCommandConfirmed, MessageCommandRefused, MessageStatus, ReasonCode,
    ThreadCommandConfirmed, ThreadCommandRefused, ThreadsCommand, ThreadsEvent,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::{OutboundEvent, Reaction};
use uuid::Uuid;

use crate::facts::FactMeta;
use crate::fault::Fault;

/// A contract command, decoded from the payload of its envelope.
pub struct Inbound<C>(pub C);

impl<C> ReactionMessage for Inbound<C>
where
    C: ThreadsCommand + DeserializeOwned + Send + 'static,
{
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(C::coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload).map(Inbound)
    }
}

/// Why a command was refused: a stable code and structured detail, never a
/// sentence.
#[derive(Debug, Clone, PartialEq)]
pub struct Refusal {
    pub code: ReasonCode,
    pub params: serde_json::Map<String, serde_json::Value>,
}

impl Refusal {
    pub fn new(code: ReasonCode) -> Self {
        Self {
            code,
            params: serde_json::Map::new(),
        }
    }

    /// The same code, as a GraphQL refusal carries it.
    pub fn reason(&self) -> Reason {
        Reason::parse(self.code.as_str()).expect("a contract reason code is SCREAMING_SNAKE_CASE")
    }

    pub fn with(mut self, key: &str, value: impl Serialize) -> Self {
        self.params.insert(
            key.to_string(),
            serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
        );
        self
    }
}

/// `ID_NOT_UUIDV7` when an id the caller mints for a new object is not a
/// UUIDv7.
pub fn minted(field: &str, id: Uuid) -> Result<(), Refusal> {
    if id.get_version_num() == 7 {
        Ok(())
    } else {
        Err(Refusal::new(ReasonCode::IdNotUuidV7)
            .with("field", field)
            .with("id", id))
    }
}

/// What a command reaction knows of the command it answers.
#[derive(Debug, Clone)]
pub struct Call {
    pub command_id: Uuid,
    pub command_type: String,
    pub meta: FactMeta,
}

impl Call {
    /// Commands are accepted from service accounts only: the commanding service
    /// took the decision. A command from anything else is not Threads' to
    /// answer; it is dead-lettered.
    pub fn of<C: ThreadsCommand>(cx: &Reaction<'_>, command: &C) -> Result<Self, Fault> {
        let actor = match cx.actor() {
            Some(actor) if actor.is_service() => *actor,
            _ => return Err(Fault::NotACommandingService),
        };
        Ok(Self {
            command_id: cx.message_id(),
            command_type: C::command_type(),
            meta: FactMeta {
                actor,
                acting_user_id: command.acting_user_id(),
                impersonator_id: None,
                correlation_id: cx.correlation_id().unwrap_or_else(|| cx.message_id()),
                causation_id: Some(cx.message_id()),
                occurred_at: cx.now().as_datetime(),
            },
        })
    }
}

/// One answer, on its contract coordinates, under a fresh event id. A replayed
/// command re-publishes the stored record, so the id stays the first one's.
struct Answer<E> {
    event_id: Uuid,
    event: E,
    _event: PhantomData<fn() -> E>,
}

impl<E: Serialize> Serialize for Answer<E> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.event.serialize(serializer)
    }
}

impl<E> OutboundEvent for Answer<E>
where
    E: ThreadsEvent + Serialize + Send + Sync + 'static,
{
    fn coords(&self) -> br_core_integration::EventCoords {
        E::coords()
    }

    fn event_id(&self) -> Uuid {
        self.event_id
    }
}

fn emit<E>(cx: &mut Reaction<'_>, event: E) -> Result<(), EngineError>
where
    E: ThreadsEvent + Serialize + Send + Sync + 'static,
{
    cx.emit(Answer {
        event_id: Uuid::now_v7(),
        event,
        _event: PhantomData,
    })
}

/// Answers a thread command.
pub fn answer_thread(
    cx: &mut Reaction<'_>,
    call: &Call,
    thread_id: Uuid,
    verdict: Result<(), Refusal>,
) -> Result<(), Fault> {
    match verdict {
        Ok(()) => emit(
            cx,
            ThreadCommandConfirmed {
                command_id: call.command_id,
                command_type: call.command_type.clone(),
                thread_id,
            },
        )?,
        Err(refusal) => emit(
            cx,
            ThreadCommandRefused {
                command_id: call.command_id,
                command_type: call.command_type.clone(),
                thread_id,
                reason_code: refusal.code,
                params: refusal.params,
            },
        )?,
    }
    Ok(())
}

/// The message a confirmed message command leaves behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageOutcome {
    pub status: MessageStatus,
    pub deleted: bool,
}

/// Answers a message command.
pub fn answer_message(
    cx: &mut Reaction<'_>,
    call: &Call,
    thread_id: Uuid,
    message_id: Uuid,
    verdict: Result<MessageOutcome, Refusal>,
) -> Result<(), Fault> {
    match verdict {
        Ok(outcome) => emit(
            cx,
            MessageCommandConfirmed {
                command_id: call.command_id,
                command_type: call.command_type.clone(),
                thread_id,
                message_id,
                status: outcome.status,
                deleted: outcome.deleted,
            },
        )?,
        Err(refusal) => emit(
            cx,
            MessageCommandRefused {
                command_id: call.command_id,
                command_type: call.command_type.clone(),
                thread_id,
                message_id,
                reason_code: refusal.code,
                params: refusal.params,
            },
        )?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minted_id_must_be_a_uuidv7() {
        assert!(minted("thread_id", Uuid::now_v7()).is_ok());
        let refused = minted("thread_id", Uuid::new_v4()).unwrap_err();
        assert_eq!(refused.code, ReasonCode::IdNotUuidV7);
        assert_eq!(refused.params["field"], "thread_id");
    }

    #[test]
    fn a_refusal_carries_its_code_and_structured_params() {
        let unknown = Uuid::now_v7();
        let refusal = Refusal::new(ReasonCode::UserUnknown).with("user_ids", [unknown]);
        assert_eq!(refusal.code, ReasonCode::UserUnknown);
        assert_eq!(
            serde_json::Value::Object(refusal.params),
            serde_json::json!({ "user_ids": [unknown] })
        );
    }
}
