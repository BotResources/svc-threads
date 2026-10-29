mod common;

use br_core_auth::Passport;
use common::content::{Streamed, closure, input_turn, request, text, to_json};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    ai, answer, command, completion, context, create, create_thread, creation, message_confirmed,
    open_stream, say, store, thread_confirmed, thread_refused, thread_view, whole_message,
};
use common::jobs::{await_consumed, failed_coords, urls_in};
use common::{
    JOB_NOT_ACTIVE, M_FAIL_TITLE_JOB, M_SET_TITLE, RUNNER_SCOPE_REQUIRED, TITLE_EMPTY,
    TITLE_RUNNER_TYPE, TITLE_TOO_LONG, TestContext, gql, stream,
};
use contract_threads::llm::{ClosureReason, ResponseEnding, Turn};
use contract_threads::{
    CancelMessage, FailMessage, FailMessageV2, LibraryJson, MessageStatus, ReasonCode, SetTitle,
    StreamSeal, TitleJobConfig,
};
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
    let ctx = TestContext::setup().await;
    let (a, b) = (ctx.enrol("ada").await, ctx.enrol("bob").await);
    let t = create_thread(&ctx, Some(&a), &[&b]).await;
    let runner = ctx.service_account(true);

    let m1 = say(&ctx, t, None, &a, "What were our Q3 numbers?").await;

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

    let m2 = answer(
        &ctx,
        t,
        Some(m1),
        ai("a1", "Analyst"),
        common::content::reply("42"),
    )
    .await;
    say(&ctx, t, Some(m2), &b, "Thanks").await;
    ctx.jobs.expect_creates(t, 1, "later messages").await;

    let mut titled = creation(Some(a.id), &[]);
    titled.title = Some("Named already".to_string());
    let titled = create(&ctx, titled).await;
    say(&ctx, titled, None, &a, "Hello").await;
    ctx.jobs.expect_creates(titled, 0, "a titled thread").await;

    let mut b_feed = Live::thread(&ctx, &b.passport, t).await;
    let read = context(&ctx, &runner.passport, t, None).await;
    assert!(read.is_object(), "the runner reads T's context: {read}");
    let set = set_title(&ctx, &runner.passport, t, job, "Q3 review").await;

    assert_ack(&set, "threadsSetTitle");
    for person in [&a, &b] {
        assert_eq!(
            thread_view(&ctx, &person.passport, t).await["title"],
            "Q3 review"
        );
    }
    b_feed
        .until("the title reaches B's feed", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["title"] == "Q3 review")
        })
        .await;
    let t_facts = facts::of(&ctx.pool, "thread", t).await;
    let changed = facts::the(&t_facts, "TitleChanged");
    assert_eq!(changed.actor_id, runner.id);
    assert_eq!(changed.actor_kind, "service");
    ctx.jobs
        .await_one_finish(job, "the runner set the title")
        .await;

    let commands_before = ctx.jobs.total();
    let facts_before = facts::count(&ctx.pool).await;
    let replay = set_title(&ctx, &runner.passport, t, job, "Q3 review").await;

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
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let runner = ctx.service_account(true);
    let (t2, j2) = thread_with_title_job(&ctx, &a).await;

    let failed = fail_title_job(
        &ctx,
        &runner.passport,
        t2,
        j2,
        "no_content",
        Some("nothing to name"),
    )
    .await;

    assert_ack(&failed, "threadsFailTitleJob");
    let fail = ctx.jobs.await_one_fail(j2, "the runner's failure").await;
    assert_eq!(
        fail.payload.note.as_deref(),
        Some("no_content: nothing to name")
    );
    assert_eq!(
        thread_view(&ctx, &a.passport, t2).await["title"],
        Value::Null
    );

    let late = set_title(&ctx, &runner.passport, t2, j2, "Too late").await;
    gql::assert_refused(&late, JOB_NOT_ACTIVE);

    say(&ctx, t2, None, &a, "Another try").await;
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
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let s = ctx.enrol_super_user("sam").await;
    let runner = ctx.service_account(true);
    let plain_service = ctx.service_account(false);
    let (t4, j4) = thread_with_title_job(&ctx, &a).await;
    let (t5, _j5) = thread_with_title_job(&ctx, &a).await;
    let commands_before = ctx.jobs.total();
    let facts_before = facts::count(&ctx.pool).await;

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
        gql::assert_refused(&answer, JOB_NOT_ACTIVE);
        assert!(answer["data"].is_null(), "{what}: {answer}");
    }

    for passport in [&a.passport, &s.passport, &plain_service.passport] {
        let refused = set_title(&ctx, passport, t4, j4, "Not a runner").await;
        gql::assert_refused(&refused, RUNNER_SCOPE_REQUIRED);
    }

    let too_long = set_title(&ctx, &runner.passport, t4, j4, &"é".repeat(201)).await;
    gql::assert_refused(&too_long, TITLE_TOO_LONG);

    let empty = set_title(&ctx, &runner.passport, t4, j4, "").await;
    gql::assert_refused(&empty, TITLE_EMPTY);

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

    let kept = set_title(&ctx, &runner.passport, t4, j4, &"é".repeat(200)).await;
    assert_ack(&kept, "threadsSetTitle");
    ctx.jobs
        .await_one_finish(j4, "the 200-character title")
        .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_a_title_job_jobs_rejected_failed_or_cancelled_ends_and_its_runner_is_refused() {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let runner = ctx.service_account(true);
    let (rejected_thread, rejected_job) = thread_with_title_job(&ctx, &a).await;
    let (failed_thread, failed_job) = thread_with_title_job(&ctx, &a).await;
    let (cancelled_thread, cancelled_job) = thread_with_title_job(&ctx, &a).await;
    let commands_before = ctx.jobs.total();

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
    ctx.jobs
        .expect_nothing_beyond(commands_before, "Jobs' own ends")
        .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_a_title_set_by_command_while_the_title_job_runs_wins_and_cancels_the_job() {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let runner = ctx.service_account(true);
    let (t6, j6) = thread_with_title_job(&ctx, &a).await;

    let too_long = command(
        &ctx,
        &SetTitle {
            thread_id: t6,
            title: "é".repeat(201),
            acting_user_id: Some(a.id),
        },
    )
    .await;
    thread_refused(
        &ctx,
        too_long,
        ReasonCode::TitleTooLong,
        "a 201-character title",
    )
    .await;
    assert!(ctx.jobs.cancels_for(j6).is_empty());

    let empty = command(
        &ctx,
        &SetTitle {
            thread_id: t6,
            title: String::new(),
            acting_user_id: Some(a.id),
        },
    )
    .await;
    thread_refused(&ctx, empty, ReasonCode::TitleEmpty, "an empty title").await;
    assert!(ctx.jobs.cancels_for(j6).is_empty());

    let set = command(
        &ctx,
        &SetTitle {
            thread_id: t6,
            title: "Manual".to_string(),
            acting_user_id: Some(a.id),
        },
    )
    .await;

    thread_confirmed(&ctx, set, "the manual title").await;
    assert_eq!(thread_view(&ctx, &a.passport, t6).await["title"], "Manual");
    ctx.jobs.await_one_cancel(j6, "the manual title wins").await;
    assert!(ctx.jobs.finishes_for(j6).is_empty() && ctx.jobs.fails_for(j6).is_empty());

    let late = set_title(&ctx, &runner.passport, t6, j6, "Runner's").await;
    gql::assert_refused(&late, JOB_NOT_ACTIVE);
    assert_eq!(thread_view(&ctx, &a.passport, t6).await["title"], "Manual");

    ctx.teardown().await;
}

async fn streamed(
    ctx: &TestContext,
    thread: Uuid,
    parent: Option<Uuid>,
    words: &str,
) -> (Uuid, u64, String) {
    let message = open_stream(ctx, thread, parent, ai("a1", "Analyst")).await;
    let mut response = Streamed::start(message, None);
    response.text(&[words], true);
    response.complete(ResponseEnding::finished());
    let (last_seq, hash) = stream::push(&ctx.nats.url(), message, 0, &response.chunks()).await;
    (message, last_seq, hash)
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_a_message_ending_cancelled_or_failed_starts_no_title_job_and_the_first_completed_one_does()
 {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let analyst = ai("a1", "Analyst");

    let failed_id = Uuid::now_v7();
    let failed_turn = Turn::from(common::content::ai_turn(
        failed_id,
        &analyst,
        vec![closure(
            ClosureReason::failed("provider_down").expect("a reason"),
        )],
    ));
    let failed_whole = store(
        &ctx,
        whole_message(t, None, failed_id, &analyst, &failed_turn, Vec::new()),
        MessageStatus::Failed,
    )
    .await;
    ctx.jobs.expect_creates(t, 0, "a whole FAILED turn").await;

    let (cancelled, last_seq, hash) =
        streamed(&ctx, t, Some(failed_whole), "cancelled words").await;
    let cancel = command(
        &ctx,
        &CancelMessage {
            message_id: cancelled,
            thread_id: t,
            seal: Some(StreamSeal { last_seq, hash }),
            acting_user_id: Some(a.id),
        },
    )
    .await;
    message_confirmed(&ctx, cancel, MessageStatus::Cancelled, "cancelling X").await;
    ctx.jobs.expect_creates(t, 0, "a CANCELLED message").await;

    let broken = open_stream(&ctx, t, Some(failed_whole), analyst.clone()).await;
    let fail = command(
        &ctx,
        &FailMessage {
            message_id: broken,
            thread_id: t,
            seal: None,
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(&ctx, fail, MessageStatus::Failed, "failing F").await;
    ctx.jobs.expect_creates(t, 0, "a FAILED message").await;

    let (finished, last_seq, hash) = streamed(&ctx, t, Some(failed_whole), "the answer").await;
    let complete = command(&ctx, &completion(t, finished, last_seq, &hash)).await;
    message_confirmed(&ctx, complete, MessageStatus::Completed, "completing S").await;

    let started = ctx.jobs.await_create(t, "S starts the title Job").await;
    assert_eq!(started.payload.runner_type, TITLE_RUNNER_TYPE);
    assert_eq!(started.payload.source_entity_id, Some(t));
    assert_eq!(started.payload.triggered_by, None);
    assert_eq!(
        serde_json::from_value::<TitleJobConfig>(started.payload.config.clone().expect("a config"))
            .expect("a TitleJobConfig"),
        TitleJobConfig::new(t, started.payload.job_id)
    );

    say(&ctx, t, Some(finished), &a, "And then?").await;
    ctx.jobs
        .expect_creates(t, 1, "later COMPLETED messages")
        .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_a_message_closed_completed_by_fail_v2_starts_the_title_job() {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;

    let (message, last_seq, hash) = streamed(&ctx, t, None, "the whole answer").await;
    let fail = command(
        &ctx,
        &FailMessageV2 {
            message_id: message,
            thread_id: t,
            seal: Some(StreamSeal { last_seq, hash }),
            closure: Some(LibraryJson::from_json(to_json(&ClosureReason::completed()))),
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(
        &ctx,
        fail,
        MessageStatus::Completed,
        "fail.v2 naming completed",
    )
    .await;

    let started = ctx.jobs.await_create(t, "the title Job").await;
    assert_eq!(started.payload.triggered_by, None);

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t12_an_agent_request_stored_first_starts_the_title_job_triggered_by_the_acting_user_when_there_is_one()
 {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let analyst = ai("a1", "Analyst");

    let with_user = create_thread(&ctx, Some(&a), &[]).await;
    let message = Uuid::now_v7();
    let turn = Turn::from(input_turn(
        message,
        vec![request(
            &analyst,
            Uuid::now_v7(),
            vec![text("Summarise Q3")],
        )],
    ));
    let mut stored = whole_message(with_user, None, message, &analyst, &turn, Vec::new());
    stored.acting_user_id = Some(a.id);
    store(&ctx, stored, MessageStatus::Completed).await;
    let started = ctx
        .jobs
        .await_create(with_user, "the request starts the Job")
        .await;
    assert_eq!(
        started.payload.triggered_by.as_ref().map(|by| by.id()),
        Some(a.id)
    );

    let without_user = create_thread(&ctx, Some(&a), &[]).await;
    let message = Uuid::now_v7();
    let turn = Turn::from(input_turn(
        message,
        vec![request(
            &analyst,
            Uuid::now_v7(),
            vec![text("Summarise Q4")],
        )],
    ));
    store(
        &ctx,
        whole_message(without_user, None, message, &analyst, &turn, Vec::new()),
        MessageStatus::Completed,
    )
    .await;
    let started = ctx
        .jobs
        .await_create(without_user, "the request starts the Job")
        .await;
    assert_eq!(started.payload.triggered_by, None);

    ctx.teardown().await;
}
