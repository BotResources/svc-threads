//! Playing a commanding service — the project service that owns a
//! conversation, the agents service, a runner — on the published language of
//! `contract-threads` only: commands go out through the typed Fabric surface
//! under their contract coordinates, and the one answer of each comes back on
//! the contract's event coordinates, correlated by the command's id.

use br_core_integration::{
    Actor, EventMetadata, IntegrationCommand, IntegrationEvent, ServiceAccountId,
};
use br_test_harness::{EventCapture, FabricTestNats, wait_until};
use chrono::Utc;
use contract_threads::{
    MessageCommandConfirmed, MessageCommandRefused, ThreadCommandConfirmed, ThreadCommandRefused,
    ThreadsCommand, ThreadsEvent,
};
use serde::Serialize;
use uuid::Uuid;

use super::constants::PUSH_TIMEOUT;

/// The service account a scenario commands Threads with.
pub fn commanding_service() -> Actor {
    Actor::Service(ServiceAccountId::from(Uuid::from_u128(
        0x0190_0000_0000_7000_8000_c0de_0000_0001,
    )))
}

/// The one answer of a command.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    ThreadConfirmed(ThreadCommandConfirmed),
    ThreadRefused(ThreadCommandRefused),
    MessageConfirmed(MessageCommandConfirmed),
    MessageRefused(MessageCommandRefused),
}

/// A capture of every answer Threads publishes, bound before the first command
/// so that none escapes it.
pub struct Answers {
    capture: EventCapture,
}

impl Answers {
    pub async fn bind(nats: &FabricTestNats) -> Self {
        let coords = [
            ThreadCommandConfirmed::coords(),
            ThreadCommandRefused::coords(),
            MessageCommandConfirmed::coords(),
            MessageCommandRefused::coords(),
        ];
        let refs: Vec<_> = coords.iter().collect();
        Self {
            capture: nats.capture_events(&refs).await,
        }
    }

    /// Every answer published under `correlation` — a command's own id, as
    /// [`send`] mints it.
    pub fn for_command(&self, correlation: Uuid) -> Vec<Answer> {
        self.capture
            .for_correlation(correlation)
            .into_iter()
            .map(|captured| decode(&captured.subject, &captured.payload))
            .collect()
    }

    /// The first answer to the command, waited for.
    pub async fn await_answer(&self, command_id: Uuid, what: &str) -> Answer {
        let answered = wait_until(PUSH_TIMEOUT, || async {
            !self.capture.for_correlation(command_id).is_empty()
        })
        .await;
        assert!(answered, "{what}: no answer to command {command_id}");
        self.for_command(command_id).remove(0)
    }

    pub async fn stop(self) {
        self.capture.stop().await;
    }
}

fn decode(subject: &str, payload: &[u8]) -> Answer {
    fn inner<T: serde::de::DeserializeOwned>(payload: &[u8]) -> T {
        serde_json::from_slice::<IntegrationEvent<T>>(payload)
            .expect("an answer in the integration envelope")
            .payload
    }
    match subject {
        s if s.ends_with(".thread.confirmed.v1") => Answer::ThreadConfirmed(inner(payload)),
        s if s.ends_with(".thread.refused.v1") => Answer::ThreadRefused(inner(payload)),
        s if s.ends_with(".message.confirmed.v1") => Answer::MessageConfirmed(inner(payload)),
        s if s.ends_with(".message.refused.v1") => Answer::MessageRefused(inner(payload)),
        other => panic!("not an answer subject: {other}"),
    }
}

/// Sends `command` as `actor`, under a fresh UUIDv7 that is both the envelope's
/// id and its correlation; answers that id.
pub async fn send<C: ThreadsCommand + Serialize>(
    nats: &FabricTestNats,
    actor: Actor,
    command: &C,
) -> Uuid {
    let command_id = Uuid::now_v7();
    send_as(nats, actor, command_id, command).await;
    command_id
}

/// Sends `command` under `command_id` — published again under the same id, it
/// is the bus redelivering one command.
pub async fn send_as<C: ThreadsCommand + Serialize>(
    nats: &FabricTestNats,
    actor: Actor,
    command_id: Uuid,
    command: &C,
) {
    let envelope = IntegrationCommand::new(
        command_id,
        C::command_type(),
        contract_threads::coords::SCHEMA_VERSION,
        Utc::now(),
        EventMetadata::new(actor, command_id),
        command,
    );
    nats.fabric()
        .publish_command(&C::coords(), &envelope)
        .await
        .expect("the commanding service publishes its command");
}
