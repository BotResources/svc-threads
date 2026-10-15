//! **T12 — Title Job: created at the first user message, ended by the runner.**
//!
//! > On untitled thread T, storing user message M1 publishes, in its
//! > transaction, one job.create for the title runner type with producer
//! > threads, source_bc threads, source_entity_id T, triggered_by M1's author
//! > and a config carrying T, the job id and the names of the runner
//! > operations, no URL. An AI message or a second user message starts no other
//! > Job; a thread created with a title starts none. Playing the runner with a
//! > threads:runner service account, the harness reads T's context then sets
//! > the title "Q3 review" naming the Job: the title shows to members, a
//! > TitleChanged fact records it, and job.finish is published in the same
//! > transaction. Replaying the set_title is refused JOB_NOT_ACTIVE and
//! > publishes nothing. On thread T2, the runner declares the Job failed with
//! > reason no_content: job.fail is published with "no_content: …" as note and
//! > T2 stays untitled for good. Setting a title for a Job that is not the
//! > thread's active title Job, or on another thread, is refused JOB_NOT_ACTIVE.
//! > Jobs answering creation_rejected, failed or cancelled for T3's Job ends it:
//! > T3 stays untitled and its runner is refused.
//!
//! Also pinned, from the specification (§ Title Job) though the gesture is
//! silent: "A title set by command while the title Job runs wins: Threads
//! cancels the Job." And, from § Thread (`title`: "optional text of 1 to 200
//! characters; an empty title is refused `TITLE_EMPTY`, a longer one
//! `TITLE_TOO_LONG`"), both bounds on the runner's operation and on
//! `thread.set_title`, a refused title cancelling nothing.
//!
//! `JOB_NOT_ACTIVE` is the draft registry test's code; the specification
//! describes the refusal without naming one.
//!
//! Channels: the commands Threads sends Jobs, decoded through `contract-jobs`
//! (4); the facts Jobs answers with (4); the runner operations' verdicts (1);
//! the thread as members read it (2); the fact log.

mod common;

use br_core_auth::Passport;
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    ai, answer, command, context, create, create_thread, creation, say, thread_confirmed,
    thread_refused, thread_view,
};
use common::jobs::{await_consumed, failed_coords, urls_in};
use common::{
    JOB_NOT_ACTIVE, M_FAIL_TITLE_JOB, M_SET_TITLE, RUNNER_SCOPE_REQUIRED, TITLE_EMPTY,
    TITLE_RUNNER_TYPE, TITLE_TOO_LONG, TestContext, gql,
};
use contract_threads::{ReasonCode, SetTitle, TitleJobConfig};
use serde_json::{Value, json};
use uuid::Uuid;

async fn set_title(
    ctx: &TestContext,
    runner: &Passport,
    thread: Uuid,
    job: Uuid,
    title: &str,
) -> Value {
    ctx.run(
        runner,
        M_SET_TITLE,
        json!({ "thread": thread, "job": job, "title": title }),
    )
    .await
}

async fn fail_title_job(
    ctx: &TestContext,
    runner: &Passport,
    thread: Uuid,
    job: Uuid,
    reason: &str,
    message: Option<&str>,
) -> Value {
    ctx.run(
        runner,
        M_FAIL_TITLE_JOB,
        json!({ "thread": thread, "job": job, "reason": reason, "message": message }),
    )
    .await
}

fn assert_ack(answer: &Value, root: &str) {
    assert_eq!(
        gql::data(answer, root),
        &json!({ "success": true }),
        "{answer}"
    );
}

/// An untitled thread of `owner`'s whose first user message started its title
/// Job: the thread and the Job's id.
async fn thread_with_title_job(ctx: &TestContext, owner: &common::Person) -> (Uuid, Uuid) {
    let thread = create_thread(ctx, Some(owner), &[]).await;
    say(ctx, thread, None, owner, "What were our Q3 numbers?").await;
    let job_create = ctx.jobs.await_create(thread, "the title Job").await;
    (thread, job_create.payload.job_id)
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_the_first_user_message_starts_one_title_job_which_the_runner_ends_by_setting_the_title()
 {
    // Given untitled T with owner A and member B, and a runner.
    let ctx = TestContext::setup().await;
    let (a, b) = (ctx.enrol("ada").await, ctx.enrol("bob").await);
    let t = create_thread(&ctx, Some(&a), &[&b]).await;
    let runner = ctx.service_account(true);

    // When the first user message of T, by A, is stored.
    let m1 = say(&ctx, t, None, &a, "What were our Q3 numbers?").await;

    // Then (4) one job.create reaches Jobs: the title runner type, from
    // threads, about T, triggered by A, configured with T, the Job and the
    // names of the runner operations — and no URL.
    let job_create = ctx.jobs.await_create(t, "M1 starts the title Job").await;
    let job = job_create.payload.job_id;
    assert_eq!(job_create.payload.runner_type, TITLE_RUNNER_TYPE);
    assert_eq!(job_create.payload.producer, "threads");
    assert_eq!(job_create.payload.source_bc.as_deref(), Some("threads"));
    assert_eq!(job_create.payload.source_entity_id, Some(t));
    assert_eq!(
        job_create.payload.triggered_by.as_ref().map(|by| by.id()),
        Some(a.id)
    );
    assert_eq!(job_create.payload.parent_job_id, None);
    let config = job_create
        .payload
        .config
        .clone()
        .expect("a title Job carries its config");
    assert_eq!(
        serde_json::from_value::<TitleJobConfig>(config.clone()).expect("a TitleJobConfig"),
        TitleJobConfig::new(t, job)
    );
    assert!(
        urls_in(&config).is_empty(),
        "no URL in the config: {config}"
    );

    // When an AI message, then a second user message, are stored on T.
    let m2 = answer(
        &ctx,
        t,
        Some(m1),
        ai("a1", "Analyst"),
        common::content::answer("42"),
    )
    .await;
    say(&ctx, t, Some(m2), &b, "Thanks").await;
    // Then no other Job is started.
    ctx.jobs.expect_creates(t, 1, "later messages").await;

    // When a thread created with a title receives its first user message.
    let mut titled = creation(Some(a.id), &[]);
    titled.title = Some("Named already".to_string());
    let titled = create(&ctx, titled).await;
    say(&ctx, titled, None, &a, "Hello").await;
    // Then it starts no Job.
    ctx.jobs.expect_creates(titled, 0, "a titled thread").await;

    // When the runner reads T's context, then sets the title naming the Job.
    let mut b_feed = Live::thread(&ctx, &b.passport, t).await;
    let read = context(&ctx, &runner.passport, t, None).await;
    assert!(read.is_object(), "the runner reads T's context: {read}");
    let set = set_title(&ctx, &runner.passport, t, job, "Q3 review").await;

    // Then (1) acknowledged, and nothing else.
    assert_ack(&set, "threadsSetTitle");
    // And (2) members read the title.
    for person in [&a, &b] {
        assert_eq!(
            thread_view(&ctx, &person.passport, t).await["title"],
            "Q3 review"
        );
    }
    // And (3) B's live view of T carries it.
    b_feed
        .until("the title reaches B's feed", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["title"] == "Q3 review")
        })
        .await;
    // And a TitleChanged fact records it, sent by the runner.
    let t_facts = facts::of(&ctx.pool, "thread", t).await;
    let changed = facts::the(&t_facts, "TitleChanged");
    assert_eq!(changed.actor_id, runner.id);
    assert_eq!(changed.actor_kind, "service");
    // And (4) Threads declares the Job finished, once.
    ctx.jobs
        .await_one_finish(job, "the runner set the title")
        .await;

    // When the runner replays the set_title.
    let commands_before = ctx.jobs.total();
    let facts_before = facts::count(&ctx.pool).await;
    let replay = set_title(&ctx, &runner.passport, t, job, "Q3 review").await;

    // Then refused JOB_NOT_ACTIVE, and nothing is published nor recorded.
    gql::assert_refused(&replay, JOB_NOT_ACTIVE);
    ctx.jobs
        .expect_nothing_beyond(commands_before, "a replayed set_title")
        .await;
    assert_eq!(facts::count(&ctx.pool).await, facts_before);

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_a_runner_declaring_its_title_job_failed_leaves_the_thread_untitled_for_good() {
    // Given T2, untitled, whose title Job J2 runs.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let runner = ctx.service_account(true);
    let (t2, j2) = thread_with_title_job(&ctx, &a).await;

    // When the runner declares J2 failed: no_content.
    let failed = fail_title_job(
        &ctx,
        &runner.passport,
        t2,
        j2,
        "no_content",
        Some("nothing to name"),
    )
    .await;

    // Then (1) acknowledged; (4) job.fail is published once, its note
    // "no_content: nothing to name".
    assert_ack(&failed, "threadsFailTitleJob");
    let fail = ctx.jobs.await_one_fail(j2, "the runner's failure").await;
    assert_eq!(
        fail.payload.note.as_deref(),
        Some("no_content: nothing to name")
    );
    // And (2) T2 stays untitled.
    assert_eq!(
        thread_view(&ctx, &a.passport, t2).await["title"],
        Value::Null
    );

    // When the runner then tries to set a title for J2.
    let late = set_title(&ctx, &runner.passport, t2, j2, "Too late").await;
    // Then refused JOB_NOT_ACTIVE.
    gql::assert_refused(&late, JOB_NOT_ACTIVE);

    // When T2 receives another user message.
    say(&ctx, t2, None, &a, "Another try").await;
    // Then no other title Job is started: untitled for good.
    ctx.jobs
        .expect_creates(t2, 1, "T2 after its failed Job")
        .await;
    assert_eq!(
        thread_view(&ctx, &a.passport, t2).await["title"],
        Value::Null
    );

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_only_the_threads_active_title_job_through_a_runner_may_set_its_title() {
    // Given T4 and T5, untitled, each with its running title Job.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let s = ctx.enrol_super_user("sam").await;
    let runner = ctx.service_account(true);
    let plain_service = ctx.service_account(false);
    let (t4, j4) = thread_with_title_job(&ctx, &a).await;
    let (t5, _j5) = thread_with_title_job(&ctx, &a).await;
    let commands_before = ctx.jobs.total();
    let facts_before = facts::count(&ctx.pool).await;

    // When the runner names a Job that is not T4's, or names T4's Job on T5.
    for (answer, what) in [
        (
            set_title(&ctx, &runner.passport, t4, Uuid::now_v7(), "Wrong job").await,
            "another Job",
        ),
        (
            set_title(&ctx, &runner.passport, t5, j4, "Wrong thread").await,
            "another thread",
        ),
        (
            fail_title_job(
                &ctx,
                &runner.passport,
                t4,
                Uuid::now_v7(),
                "no_content",
                None,
            )
            .await,
            "failing another Job",
        ),
    ] {
        // Then each is refused JOB_NOT_ACTIVE.
        gql::assert_refused(&answer, JOB_NOT_ACTIVE);
        assert!(answer["data"].is_null(), "{what}: {answer}");
    }

    // When a member, a super user, or a service account without
    // threads:runner set T4's title naming J4.
    for passport in [&a.passport, &s.passport, &plain_service.passport] {
        // Then each is refused the operation.
        let refused = set_title(&ctx, passport, t4, j4, "Not a runner").await;
        gql::assert_refused(&refused, RUNNER_SCOPE_REQUIRED);
    }

    // When the runner sets a title of 201 characters.
    let too_long = set_title(&ctx, &runner.passport, t4, j4, &"é".repeat(201)).await;
    // Then refused TITLE_TOO_LONG.
    gql::assert_refused(&too_long, TITLE_TOO_LONG);

    // When the runner sets an empty title.
    let empty = set_title(&ctx, &runner.passport, t4, j4, "").await;
    // Then refused TITLE_EMPTY.
    gql::assert_refused(&empty, TITLE_EMPTY);

    // And nothing of all that changed anything, nor reached Jobs.
    ctx.jobs
        .expect_nothing_beyond(commands_before, "refused title operations")
        .await;
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    for thread in [t4, t5] {
        assert_eq!(
            thread_view(&ctx, &a.passport, thread).await["title"],
            Value::Null
        );
    }

    // When the runner then sets a title of 200 characters for J4.
    let kept = set_title(&ctx, &runner.passport, t4, j4, &"é".repeat(200)).await;
    // Then J4 was still active: the title is set and the Job finished.
    assert_ack(&kept, "threadsSetTitle");
    ctx.jobs
        .await_one_finish(j4, "the 200-character title")
        .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_a_title_job_jobs_rejected_failed_or_cancelled_ends_and_its_runner_is_refused() {
    // Given three untitled threads, each with its running title Job.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let runner = ctx.service_account(true);
    let (rejected_thread, rejected_job) = thread_with_title_job(&ctx, &a).await;
    let (failed_thread, failed_job) = thread_with_title_job(&ctx, &a).await;
    let (cancelled_thread, cancelled_job) = thread_with_title_job(&ctx, &a).await;
    let commands_before = ctx.jobs.total();

    // When Jobs answers creation_rejected, failed (delivered twice) and
    // cancelled.
    let rejection = ctx
        .jobs
        .creation_rejected(&ctx.nats, rejected_job, "runner_type_unknown")
        .await
        .event_id;
    let failure = ctx.jobs.failed(&ctx.nats, failed_job).await;
    ctx.jobs
        .redeliver(&ctx.nats, failed_coords(), &failure)
        .await;
    let cancellation = ctx.jobs.cancelled(&ctx.nats, cancelled_job).await.event_id;

    // Then each Job is ended — once Threads consumed Jobs' fact, its runner is
    // refused JOB_NOT_ACTIVE, and the thread stays untitled.
    for (thread, job, fact, what) in [
        (rejected_thread, rejected_job, rejection, "rejected"),
        (failed_thread, failed_job, failure.event_id, "failed"),
        (cancelled_thread, cancelled_job, cancellation, "cancelled"),
    ] {
        await_consumed(&ctx.pool, fact, what).await;
        let answer = set_title(&ctx, &runner.passport, thread, job, "Late").await;
        gql::assert_refused(&answer, JOB_NOT_ACTIVE);
        assert_eq!(
            thread_view(&ctx, &a.passport, thread).await["title"],
            Value::Null,
            "{what}"
        );
    }
    // And Threads tells Jobs nothing back about Jobs' own ends.
    ctx.jobs
        .expect_nothing_beyond(commands_before, "Jobs' own ends")
        .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_a_title_set_by_command_while_the_title_job_runs_wins_and_cancels_the_job() {
    // Given T6, untitled, whose title Job J6 runs.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let runner = ctx.service_account(true);
    let (t6, j6) = thread_with_title_job(&ctx, &a).await;

    // When the commanding service sets a title of 201 characters.
    let too_long = command(
        &ctx,
        &SetTitle {
            thread_id: t6,
            title: "é".repeat(201),
            acting_user_id: Some(a.id),
        },
    )
    .await;
    // Then refused TITLE_TOO_LONG; J6 runs on, nothing cancelled.
    thread_refused(
        &ctx,
        too_long,
        ReasonCode::TitleTooLong,
        "a 201-character title",
    )
    .await;
    assert!(ctx.jobs.cancels_for(j6).is_empty());

    // When the commanding service sets an empty title.
    let empty = command(
        &ctx,
        &SetTitle {
            thread_id: t6,
            title: String::new(),
            acting_user_id: Some(a.id),
        },
    )
    .await;
    // Then refused TITLE_EMPTY; J6 runs on, nothing cancelled.
    thread_refused(&ctx, empty, ReasonCode::TitleEmpty, "an empty title").await;
    assert!(ctx.jobs.cancels_for(j6).is_empty());

    // When the commanding service sets T6's title on A's gesture.
    let set = command(
        &ctx,
        &SetTitle {
            thread_id: t6,
            title: "Manual".to_string(),
            acting_user_id: Some(a.id),
        },
    )
    .await;

    // Then confirmed, the title shows, and Threads cancels J6, once.
    thread_confirmed(&ctx, set, "the manual title").await;
    assert_eq!(thread_view(&ctx, &a.passport, t6).await["title"], "Manual");
    ctx.jobs.await_one_cancel(j6, "the manual title wins").await;
    assert!(ctx.jobs.finishes_for(j6).is_empty() && ctx.jobs.fails_for(j6).is_empty());

    // When the runner then sets its own title for J6.
    let late = set_title(&ctx, &runner.passport, t6, j6, "Runner's").await;
    // Then refused JOB_NOT_ACTIVE; the manual title stays.
    gql::assert_refused(&late, JOB_NOT_ACTIVE);
    assert_eq!(thread_view(&ctx, &a.passport, t6).await["title"], "Manual");

    ctx.teardown().await;
}
