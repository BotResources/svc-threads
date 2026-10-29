use chrono::{DateTime, Utc};
use contract_jobs::command::{CancelJob, CreateJob, FailJob, FinishJob, TriggeredBy};
use contract_jobs::event::{JobCancelled, JobCreationRejected, JobFailed};
use contract_threads::TitleJobConfig;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::gate::{Gate, Reason};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::{Mutation, MutationInput, Ops, OutboundCommand, Reaction};
use uuid::Uuid;

use super::ThreadCause;
use super::state::{Thread, ThreadState, TitleJob, TitleJobState};
use super::transitions::TitleSource;
use crate::access;
use crate::facts::FactMeta;
use crate::fault::Fault;
use crate::principal::ThreadsPrincipal;

pub const JOB_NOT_ACTIVE: Reason = Reason::new("JOB_NOT_ACTIVE");
pub const FAILURE_REASON_INVALID: Reason = Reason::new("FAILURE_REASON_INVALID");

const PRODUCER: &str = contract_threads::SERVICE;

pub fn job_gate(thread: &ThreadState, job_id: Uuid) -> Gate {
    match thread.active_title_job() {
        Some(job) if job.job_id == job_id => Gate::allowed(),
        _ => Gate::blocked(JOB_NOT_ACTIVE),
    }
}

pub fn start(thread: &mut ThreadState, now: DateTime<Utc>) -> Option<Uuid> {
    if thread.title.is_some() || thread.title_job.is_some() {
        return None;
    }
    let job_id = Uuid::now_v7();
    thread.title_job = Some(TitleJob {
        job_id,
        state: TitleJobState::Running,
        created_at: now,
        ended_at: None,
    });
    Some(job_id)
}

fn end(thread: &mut ThreadState, state: TitleJobState, now: DateTime<Utc>) -> Option<Uuid> {
    let job = thread.title_job.as_mut().filter(|job| job.is_active())?;
    job.state = state;
    job.ended_at = Some(now);
    Some(job.job_id)
}

pub fn cancel_active(thread: &mut ThreadState, now: DateTime<Utc>) -> Option<Uuid> {
    end(thread, TitleJobState::Cancelled, now)
}

pub fn end_by_jobs(
    thread: &mut ThreadState,
    job_id: Uuid,
    state: TitleJobState,
    now: DateTime<Utc>,
) -> bool {
    if thread.active_title_job().map(|job| job.job_id) != Some(job_id) {
        return false;
    }
    end(thread, state, now).is_some()
}

pub fn is_failure_reason(code: &str) -> bool {
    let mut bytes = code.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

struct JobsCommand<T> {
    coords: br_core_integration::CommandCoords,
    command_id: Uuid,
    payload: T,
}

impl<T: Serialize> Serialize for JobsCommand<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.payload.serialize(serializer)
    }
}

impl<T: Serialize + Send + Sync + 'static> OutboundCommand for JobsCommand<T> {
    fn coords(&self) -> br_core_integration::CommandCoords {
        self.coords.clone()
    }

    fn command_id(&self) -> Uuid {
        self.command_id
    }
}

fn jobs_command<T>(
    coords: Result<br_core_integration::CommandCoords, br_core_integration::CoordError>,
    payload: T,
) -> JobsCommand<T> {
    JobsCommand {
        coords: coords.expect("the coordinates of Jobs' contract are static and well formed"),
        command_id: Uuid::now_v7(),
        payload,
    }
}

pub async fn trigger(
    ops: &mut Ops<'_>,
    thread: &mut ThreadState,
    now: DateTime<Utc>,
    triggered_by: Option<Uuid>,
) -> Result<bool, Fault> {
    let Some(job) = start(thread, now) else {
        return Ok(false);
    };
    let name = match triggered_by {
        Some(user) => crate::directory::display_name(ops.connection(), user).await?,
        None => None,
    };
    publish_create(
        ops,
        thread.id,
        job,
        &crate::config::installed().title_runner_type,
        triggered_by,
        name,
    )?;
    Ok(true)
}

fn publish_create(
    ops: &mut Ops<'_>,
    thread_id: Uuid,
    job_id: Uuid,
    runner_type: &str,
    triggered_by: Option<Uuid>,
    triggered_by_name: Option<String>,
) -> Result<(), Fault> {
    let config = serde_json::to_value(TitleJobConfig::new(thread_id, job_id))
        .map_err(|source| crate::fault::encode("a title Job configuration", source))?;
    ops.command(jobs_command(
        contract_jobs::cmd_job_create_v1_coords(),
        CreateJob {
            job_id,
            runner_type: runner_type.to_string(),
            producer: PRODUCER.to_string(),
            config: Some(config),
            parent_job_id: None,
            triggered_by: triggered_by.map(|id| match triggered_by_name {
                Some(display_name) => TriggeredBy::Identified { id, display_name },
                None => TriggeredBy::Anonymous(id),
            }),
            source_bc: Some(PRODUCER.to_string()),
            source_entity_id: Some(thread_id),
            max_attempts: None,
        },
    ))?;
    Ok(())
}

pub fn publish_cancel(ops: &mut Ops<'_>, job_id: Uuid) -> Result<(), Fault> {
    ops.command(jobs_command(
        contract_jobs::cmd_job_cancel_v2_coords(),
        CancelJob { job_id },
    ))?;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct RunnerSetTitle {
    pub thread_id: Uuid,
    pub job_id: Uuid,
    pub title: String,
}

impl MutationInput for RunnerSetTitle {
    type Output = ();
    type Error = Fault;
    const NAME: &'static str = "runner_set_title";
}

pub fn runner_set_title<'m>(
    cx: &'m mut Mutation<'m, ThreadsPrincipal>,
    input: RunnerSetTitle,
) -> BoxFuture<'m, Result<(), Fault>> {
    Box::pin(async move {
        access::runner_surface(cx.principal()).require()?;
        let mut thread = cx
            .load::<ThreadState>(&input.thread_id)
            .await?
            .ok_or(access::THREAD_NOT_FOUND)?;
        job_gate(&thread, input.job_id).require()?;
        let now = cx.now().as_datetime();
        let meta = FactMeta::of_principal(cx.principal(), now);
        if let Err(refusal) = thread.set_title(&input.title, TitleSource::TitleJob, &meta)? {
            return Err(Fault::Refused(refusal.reason()));
        }
        end(&mut thread, TitleJobState::Done, now);
        cx.save(&thread).await?;
        cx.command(jobs_command(
            contract_jobs::cmd_job_finish_v2_coords(),
            FinishJob {
                job_id: input.job_id,
            },
        ))?;
        cx.impact_caused::<Thread, _>(&thread.id, ThreadCause::TitleChanged)?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct RunnerFailTitleJob {
    pub thread_id: Uuid,
    pub job_id: Uuid,
    pub reason_code: String,
    pub message: Option<String>,
}

impl MutationInput for RunnerFailTitleJob {
    type Output = ();
    type Error = Fault;
    const NAME: &'static str = "runner_fail_title_job";
}

pub fn runner_fail_title_job<'m>(
    cx: &'m mut Mutation<'m, ThreadsPrincipal>,
    input: RunnerFailTitleJob,
) -> BoxFuture<'m, Result<(), Fault>> {
    Box::pin(async move {
        access::runner_surface(cx.principal()).require()?;
        if !is_failure_reason(&input.reason_code) {
            return Err(FAILURE_REASON_INVALID.into());
        }
        let mut thread = cx
            .load::<ThreadState>(&input.thread_id)
            .await?
            .ok_or(access::THREAD_NOT_FOUND)?;
        job_gate(&thread, input.job_id).require()?;
        let now = cx.now().as_datetime();
        end(&mut thread, TitleJobState::Failed, now);
        cx.save(&thread).await?;
        let note = match input.message.as_deref().map(str::trim) {
            Some(message) if !message.is_empty() => format!("{}: {message}", input.reason_code),
            _ => input.reason_code.clone(),
        };
        cx.command(jobs_command(
            contract_jobs::cmd_job_fail_v2_coords(),
            FailJob {
                job_id: input.job_id,
                note: Some(note),
            },
        ))?;
        cx.impact_caused::<Thread, _>(&thread.id, ThreadCause::TitleJobEnded)?;
        Ok(())
    })
}

pub struct JobsEvent<T>(pub T);

macro_rules! jobs_event {
    ($payload:ty, $coords:path) => {
        impl ReactionMessage for JobsEvent<$payload> {
            fn coordinates() -> ReactionCoordinates {
                ReactionCoordinates::Event(
                    $coords().expect("the coordinates of Jobs' contract are static"),
                )
            }

            fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
                serde_json::from_slice(payload).map(JobsEvent)
            }
        }
    };
}

jobs_event!(
    JobCreationRejected,
    contract_jobs::evt_job_creation_rejected_v1_coords
);
jobs_event!(JobFailed, contract_jobs::evt_job_failed_v1_coords);
jobs_event!(JobCancelled, contract_jobs::evt_job_cancelled_v1_coords);

async fn thread_of_job(cx: &mut Reaction<'_>, job_id: Uuid) -> Result<Option<ThreadState>, Fault> {
    let thread: Option<Uuid> =
        sqlx::query_scalar("SELECT thread_id FROM threads.title_job WHERE job_id = $1")
            .bind(job_id)
            .fetch_optional(cx.connection())
            .await?;
    match thread {
        Some(thread) => Ok(cx.load::<ThreadState>(&thread).await?),
        None => Ok(None),
    }
}

async fn end_title_job(
    cx: &mut Reaction<'_>,
    job_id: Uuid,
    state: TitleJobState,
) -> Result<(), Fault> {
    let Some(mut thread) = thread_of_job(cx, job_id).await? else {
        return Ok(());
    };
    if end_by_jobs(&mut thread, job_id, state, cx.now().as_datetime()) {
        cx.save(&thread).await?;
        cx.impact_caused::<Thread, _>(&thread.id, ThreadCause::TitleJobEnded)?;
    }
    Ok(())
}

pub fn on_creation_rejected<'r>(
    cx: &'r mut Reaction<'r>,
    JobsEvent(event): JobsEvent<JobCreationRejected>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let Some(mut thread) = thread_of_job(cx, event.job_id).await? else {
            return Ok(());
        };
        if !end_by_jobs(
            &mut thread,
            event.job_id,
            TitleJobState::Rejected,
            cx.now().as_datetime(),
        ) {
            return Ok(());
        }
        cx.save(&thread).await?;
        if event.reason_code == contract_jobs::event::REASON_DUPLICATE_ACTIVE_ENTITY
            && let Some(other) = event
                .params
                .get("activeJobId")
                .and_then(serde_json::Value::as_str)
                .and_then(|raw| Uuid::parse_str(raw).ok())
        {
            publish_cancel(cx, other)?;
        }
        cx.impact_caused::<Thread, _>(&thread.id, ThreadCause::TitleJobEnded)?;
        Ok(())
    })
}

pub fn on_failed<'r>(
    cx: &'r mut Reaction<'r>,
    JobsEvent(event): JobsEvent<JobFailed>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move { end_title_job(cx, event.job_id, TitleJobState::Failed).await })
}

pub fn on_cancelled<'r>(
    cx: &'r mut Reaction<'r>,
    JobsEvent(event): JobsEvent<JobCancelled>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move { end_title_job(cx, event.job_id, TitleJobState::Cancelled).await })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slices::thread::state::tests::thread_with;

    #[test]
    fn an_untitled_thread_without_a_title_job_is_owed_one_once() {
        let mut thread = thread_with(&[Uuid::now_v7()]);
        let now = Utc::now();
        let job = start(&mut thread, now).expect("an untitled thread is owed a title Job");
        assert_eq!(job_gate(&thread, job), Gate::allowed());
        assert_eq!(start(&mut thread, now), None, "at most one title Job, ever");
        assert!(end_by_jobs(&mut thread, job, TitleJobState::Failed, now));
        assert_eq!(start(&mut thread, now), None, "even once it ended");
        assert_eq!(job_gate(&thread, job), Gate::blocked(JOB_NOT_ACTIVE));
    }

    #[test]
    fn a_titled_thread_starts_no_title_job() {
        let mut thread = thread_with(&[Uuid::now_v7()]);
        thread.title = Some(contract_threads::ThreadTitle::parse("Q3").unwrap());
        assert_eq!(start(&mut thread, Utc::now()), None);
    }

    #[test]
    fn only_the_active_title_job_is_admitted() {
        let mut thread = thread_with(&[Uuid::now_v7()]);
        let job = start(&mut thread, Utc::now()).unwrap();
        assert_eq!(
            job_gate(&thread, Uuid::now_v7()),
            Gate::blocked(JOB_NOT_ACTIVE)
        );
        assert!(!end_by_jobs(
            &mut thread,
            Uuid::now_v7(),
            TitleJobState::Cancelled,
            Utc::now()
        ));
        assert_eq!(job_gate(&thread, job), Gate::allowed());
        assert_eq!(cancel_active(&mut thread, Utc::now()), Some(job));
        assert_eq!(cancel_active(&mut thread, Utc::now()), None);
        assert_eq!(
            thread.title_job.as_ref().map(|job| job.state),
            Some(TitleJobState::Cancelled)
        );
    }

    #[test]
    fn a_failure_reason_is_a_lowercase_code() {
        assert!(is_failure_reason("no_content"));
        assert!(is_failure_reason("e2"));
        for invalid in ["", "No", "2x", "no-content", "no content"] {
            assert!(!is_failure_reason(invalid), "{invalid}");
        }
    }
}
