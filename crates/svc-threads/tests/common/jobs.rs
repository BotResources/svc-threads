//! Playing Jobs — the title Job's commands Threads sends it, and the facts it
//! answers with.
//!
//! Jobs owns dispatch and the runner fleet; Threads only requests the title Job
//! (`job.create`), declares its end (`job.finish` / `job.fail`) and asks for its
//! cancellation (`job.cancel`), and it reacts to `creation_rejected`, `failed`
//! and `cancelled`. This stand-in answers **nothing on its own**: a scenario
//! publishes every fact explicitly.
//!
//! Observation is a capture of the command stream from its start, decoded
//! through Jobs' own published language (`contract-jobs`): a command that does
//! not decode into its contract type is a wire Jobs would refuse, and fails the
//! scenario here. Every subject and coordinate is the contract's.

use br_core_integration::{
    Actor, CommandCoords, EventCoords, EventMetadata, IntegrationCommand, IntegrationEvent,
    ServiceAccountId,
};
use br_test_harness::{CapturedMessage, CommandCapture, FabricTestNats, wait_until};
use chrono::Utc;
use contract_jobs::command::{CancelJob, CreateJob, FailJob, FinishJob};
use contract_jobs::event::{
    EVENT_TYPE_CANCELLED, EVENT_TYPE_CREATION_REJECTED, EVENT_TYPE_FAILED, FailureReport,
    JobCancelled, JobCreationRejected, JobFailed,
};
use contract_jobs::{
    cmd_job_cancel_v2_coords, cmd_job_create_v1_coords, cmd_job_fail_v2_coords,
    cmd_job_finish_v2_coords, evt_job_cancelled_v1_coords, evt_job_creation_rejected_v1_coords,
    evt_job_failed_v1_coords,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use super::constants::{PUSH_TIMEOUT, QUIET_WINDOW};

const EVENT_VERSION: u8 = 1;

/// One command Threads sent Jobs, as it arrived.
#[derive(Debug, Clone)]
pub struct Sent<T> {
    pub envelope: IntegrationCommand<Value>,
    pub payload: T,
}

pub struct Jobs {
    capture: CommandCapture,
    /// Jobs' own service account, the sender of every fact it publishes.
    actor: Uuid,
}

fn subject_of(coords: CommandCoords) -> String {
    br_util_nats_fabric::command_subject(&coords)
}

impl Jobs {
    /// Bind before the boot: the capture replays the command stream from its
    /// start.
    pub async fn bind(nats: &FabricTestNats) -> Self {
        let create = cmd_job_create_v1_coords().expect("job.create coordinates");
        let finish = cmd_job_finish_v2_coords().expect("job.finish coordinates");
        let fail = cmd_job_fail_v2_coords().expect("job.fail coordinates");
        let cancel = cmd_job_cancel_v2_coords().expect("job.cancel coordinates");
        Self {
            capture: nats
                .capture_commands(&[&create, &finish, &fail, &cancel])
                .await,
            actor: Uuid::now_v7(),
        }
    }

    /// Every captured command, once each.
    fn messages(&self) -> Vec<CapturedMessage> {
        let mut seen = Vec::new();
        let mut messages = Vec::new();
        for correlation in self.capture.correlation_ids() {
            if seen.contains(&correlation) {
                continue;
            }
            seen.push(correlation);
            messages.extend(self.capture.for_correlation(correlation));
        }
        messages
    }

    fn sent<T: DeserializeOwned>(&self, subject: &str) -> Vec<Sent<T>> {
        self.messages()
            .into_iter()
            .filter(|message| message.subject == subject)
            .map(|message| {
                let envelope: IntegrationCommand<Value> = serde_json::from_slice(&message.payload)
                    .unwrap_or_else(|e| {
                        panic!(
                            "a command on {} is not an integration command envelope ({e}): {}",
                            message.subject,
                            String::from_utf8_lossy(&message.payload)
                        )
                    });
                let payload: T =
                    serde_json::from_value(envelope.payload.clone()).unwrap_or_else(|e| {
                        panic!(
                            "a command on {} does not decode into Jobs' contract type ({e}): {}",
                            message.subject, envelope.payload
                        )
                    });
                Sent { envelope, payload }
            })
            .collect()
    }

    /// Every `job.create` Threads sent about `thread`.
    pub fn creates_for(&self, thread: Uuid) -> Vec<Sent<CreateJob>> {
        self.sent::<CreateJob>(&subject_of(cmd_job_create_v1_coords().unwrap()))
            .into_iter()
            .filter(|sent| sent.payload.source_entity_id == Some(thread))
            .collect()
    }

    /// Wait for the first `job.create` about `thread` and answer it.
    pub async fn await_create(&self, thread: Uuid, what: &str) -> Sent<CreateJob> {
        let arrived = wait_until(PUSH_TIMEOUT, || async {
            !self.creates_for(thread).is_empty()
        })
        .await;
        assert!(
            arrived,
            "{what}: no job.create about thread {thread} reached Jobs"
        );
        self.creates_for(thread).remove(0)
    }

    /// Exactly `expected` `job.create` about `thread`, held over the quiet
    /// window: absence of a further one, proven.
    pub async fn expect_creates(&self, thread: Uuid, expected: usize, what: &str) {
        tokio::time::sleep(QUIET_WINDOW).await;
        let creates = self.creates_for(thread);
        assert_eq!(
            creates.len(),
            expected,
            "{what}: Jobs received {} job.create about {thread}, {expected} expected: {:?}",
            creates.len(),
            creates.iter().map(|sent| &sent.payload).collect::<Vec<_>>()
        );
    }

    pub fn finishes_for(&self, job: Uuid) -> Vec<Sent<FinishJob>> {
        self.sent::<FinishJob>(&subject_of(cmd_job_finish_v2_coords().unwrap()))
            .into_iter()
            .filter(|sent| sent.payload.job_id == job)
            .collect()
    }

    pub fn fails_for(&self, job: Uuid) -> Vec<Sent<FailJob>> {
        self.sent::<FailJob>(&subject_of(cmd_job_fail_v2_coords().unwrap()))
            .into_iter()
            .filter(|sent| sent.payload.job_id == job)
            .collect()
    }

    pub fn cancels_for(&self, job: Uuid) -> Vec<Sent<CancelJob>> {
        self.sent::<CancelJob>(&subject_of(cmd_job_cancel_v2_coords().unwrap()))
            .into_iter()
            .filter(|sent| sent.payload.job_id == job)
            .collect()
    }

    /// Every command of any kind captured so far — for "publishes nothing".
    pub fn total(&self) -> usize {
        self.capture.count()
    }

    /// Exactly one `job.finish` for `job`, held over the quiet window.
    pub async fn await_one_finish(&self, job: Uuid, what: &str) -> Sent<FinishJob> {
        let arrived = wait_until(PUSH_TIMEOUT, || async {
            !self.finishes_for(job).is_empty()
        })
        .await;
        assert!(arrived, "{what}: no job.finish for {job} reached Jobs");
        tokio::time::sleep(QUIET_WINDOW).await;
        let finishes = self.finishes_for(job);
        assert_eq!(finishes.len(), 1, "{what}: a Job ends once, one job.finish");
        finishes.into_iter().next().expect("counted above")
    }

    /// Exactly one `job.fail` for `job`, held over the quiet window.
    pub async fn await_one_fail(&self, job: Uuid, what: &str) -> Sent<FailJob> {
        let arrived = wait_until(PUSH_TIMEOUT, || async { !self.fails_for(job).is_empty() }).await;
        assert!(arrived, "{what}: no job.fail for {job} reached Jobs");
        tokio::time::sleep(QUIET_WINDOW).await;
        let fails = self.fails_for(job);
        assert_eq!(fails.len(), 1, "{what}: a Job ends once, one job.fail");
        fails.into_iter().next().expect("counted above")
    }

    /// Exactly one `job.cancel` for `job`, held over the quiet window.
    pub async fn await_one_cancel(&self, job: Uuid, what: &str) {
        let arrived =
            wait_until(PUSH_TIMEOUT, || async { !self.cancels_for(job).is_empty() }).await;
        assert!(arrived, "{what}: no job.cancel for {job} reached Jobs");
        tokio::time::sleep(QUIET_WINDOW).await;
        assert_eq!(
            self.cancels_for(job).len(),
            1,
            "{what}: exactly one job.cancel for {job}"
        );
    }

    /// Nothing further reached Jobs over the quiet window, beyond `already`.
    pub async fn expect_nothing_beyond(&self, already: usize, what: &str) {
        tokio::time::sleep(QUIET_WINDOW).await;
        assert_eq!(
            self.total(),
            already,
            "{what}: Threads sent Jobs a command it must not have"
        );
    }

    pub async fn stop(self) {
        self.capture.stop().await;
    }

    // ── the facts Jobs publishes ────────────────────────────────────────────

    async fn publish<T: Serialize>(
        &self,
        nats: &FabricTestNats,
        coords: EventCoords,
        event_type: &str,
        payload: T,
    ) -> IntegrationEvent<T> {
        let event = IntegrationEvent::new(
            Uuid::now_v7(),
            event_type,
            EVENT_VERSION,
            Utc::now(),
            EventMetadata::new(
                Actor::Service(ServiceAccountId::from(self.actor)),
                Uuid::now_v7(),
            ),
            payload,
        );
        nats.fabric()
            .publish_event(&coords, &event)
            .await
            .expect("Jobs publishes its fact");
        event
    }

    /// The broker delivering one fact a second time: the very same envelope.
    pub async fn redeliver<T: Serialize>(
        &self,
        nats: &FabricTestNats,
        coords: EventCoords,
        event: &IntegrationEvent<T>,
    ) {
        nats.fabric()
            .publish_event(&coords, event)
            .await
            .expect("the broker redelivers the fact");
    }

    /// Jobs' own failure of a running Job (a terminal run failure).
    pub async fn failed(&self, nats: &FabricTestNats, job: Uuid) -> IntegrationEvent<JobFailed> {
        self.publish(
            nats,
            evt_job_failed_v1_coords().unwrap(),
            EVENT_TYPE_FAILED,
            JobFailed {
                job_id: job,
                failure_cause: "runner_failed".to_string(),
                failure_report: Some(FailureReport {
                    kind: "PERMANENT".to_string(),
                    reason_code: "model_unavailable".to_string(),
                    params: Value::Object(Default::default()),
                    diagnostic: serde_json::json!({ "log": "the title runner gave up" }),
                }),
                note: None,
            },
        )
        .await
    }

    /// Jobs confirms a cancellation.
    pub async fn cancelled(
        &self,
        nats: &FabricTestNats,
        job: Uuid,
    ) -> IntegrationEvent<JobCancelled> {
        self.publish(
            nats,
            evt_job_cancelled_v1_coords().unwrap(),
            EVENT_TYPE_CANCELLED,
            JobCancelled { job_id: job },
        )
        .await
    }

    /// Jobs refuses to create a Job.
    pub async fn creation_rejected(
        &self,
        nats: &FabricTestNats,
        job: Uuid,
        reason_code: &str,
    ) -> IntegrationEvent<JobCreationRejected> {
        self.publish(
            nats,
            evt_job_creation_rejected_v1_coords().unwrap(),
            EVENT_TYPE_CREATION_REJECTED,
            JobCreationRejected {
                job_id: job,
                reason_code: reason_code.to_string(),
                params: Value::Object(Default::default()),
            },
        )
        .await
    }
}

/// The coordinates of Jobs' `failed` fact, for a redelivery.
pub fn failed_coords() -> EventCoords {
    evt_job_failed_v1_coords().unwrap()
}

/// Anything in `value`, at any depth, that looks like a URL.
pub fn urls_in(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) if text.contains("://") => vec![text.clone()],
        Value::Array(items) => items.iter().flat_map(urls_in).collect(),
        Value::Object(map) => map.values().flat_map(urls_in).collect(),
        _ => Vec::new(),
    }
}
