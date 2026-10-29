mod common;

use common::content::{
    Streamed, closure_kind, entries_of, input_turn, mail, said, text, text_as, to_json,
};
use common::facts;
use common::flows::{
    active_branch, ai, await_message, command, completion, context, create_thread,
    message_confirmed, message_refused, message_view, open_stream, projected_of, say, store,
    user_message, whole_message,
};
use common::{SHORT_STREAM_IDLE, TestContext, Windows, stream};
use contract_threads::llm::{ClosureReason, ResponseEnding, Turn, TurnEntry};
use contract_threads::{FailMessageV2, LibraryJson, MessageStatus, ReasonCode};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t31_the_restarted_run_fails_the_open_turn_interrupted_and_stores_a_retry_under_new_ids() {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let analyst = ai("a1", "Analyst");
    let runner = ctx.service_account(true);
    let nats = ctx.nats.url();

    let m1 = say(&ctx, t, None, &a, "Review this").await;
    let mail_id = Uuid::now_v7();
    let outgoing = Turn::from(input_turn(
        mail_id,
        vec![mail(&analyst, t, vec![text("Critic, a question")])],
    ));
    store(
        &ctx,
        whole_message(t, Some(m1), mail_id, &analyst, &outgoing, Vec::new()),
        MessageStatus::Completed,
    )
    .await;

    let found = projected_of(&context(&ctx, &runner.passport, t, Some(&analyst)).await);
    let last = found.turns().last().expect("a turn in the context");
    assert_eq!(
        last.id().as_uuid(),
        mail_id,
        "the mail has no answer after it"
    );
    assert!(
        format!("{last:?}").contains("Critic, a question"),
        "the runner finds the mail it stored: {last:?}"
    );

    let m2 = open_stream(&ctx, t, Some(mail_id), analyst.clone()).await;
    let mut response = Streamed::start(m2, None);
    let held_block = response.text(&["half of a"], false);
    stream::push(&nats, m2, 0, &response.chunks()).await;
    let held_entry = response.entry;
    await_message(&ctx, &a.passport, t, m2, "M2's draft folded", |view| {
        entries_of(view)
            .first()
            .is_some_and(|entry| entry["content"]["blocks"][0]["payload"]["text"] == "half of a")
    })
    .await;

    let found = projected_of(&context(&ctx, &runner.passport, t, Some(&analyst)).await);
    let open = found
        .turns()
        .iter()
        .find(|turn| turn.id().as_uuid() == m2)
        .and_then(|turn| turn.as_ai())
        .expect("the left-open turn is in the agent's context");
    assert!(!open.is_closed(), "{open:?}");

    let interrupted = ClosureReason::interrupted()
        .with_detail("the runner died")
        .expect("a detail");
    let sent = command(
        &ctx,
        &FailMessageV2 {
            message_id: m2,
            thread_id: t,
            seal: None,
            closure: Some(LibraryJson::from_json(to_json(&interrupted))),
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(
        &ctx,
        sent,
        MessageStatus::Failed,
        "failing the left-open turn",
    )
    .await;
    let view = message_view(&ctx, &a.passport, t, m2).await;
    assert_eq!(
        closure_kind(&view).as_deref(),
        Some("interrupted"),
        "{view}"
    );
    assert!(
        facts::of(&ctx.pool, "message", m2)
            .await
            .iter()
            .all(|fact| !fact.payload.to_string().contains("the runner died")),
        "no fact carries the detail"
    );

    let retry_id = Uuid::now_v7();
    let reused_entry = Turn::from(common::content::ai_turn(
        retry_id,
        &analyst,
        common::content::closed(vec![TurnEntry::from(
            contract_threads::llm::ModelResponse::new(
                held_entry,
                vec![said("again")],
                ResponseEnding::finished(),
                contract_threads::llm::ResponseMetadata::new(),
            )
            .expect("a model response on its own"),
        )]),
    ));
    let sent = command(
        &ctx,
        &whole_message(
            t,
            Some(mail_id),
            retry_id,
            &analyst,
            &reused_entry,
            Vec::new(),
        ),
    )
    .await;
    message_refused(
        &ctx,
        sent,
        ReasonCode::KeyReused,
        "a retry reusing the entry id",
    )
    .await;

    let mut reused_block = user_message(
        t,
        Some(mail_id),
        a.id,
        vec![text_as(held_block.as_uuid(), "again")],
    );
    reused_block.message_id = Uuid::now_v7();
    let sent = command(&ctx, &reused_block).await;
    message_refused(
        &ctx,
        sent,
        ReasonCode::KeyReused,
        "a retry reusing the block id",
    )
    .await;

    let m3 = open_stream(&ctx, t, Some(mail_id), analyst.clone()).await;
    let mut retry = Streamed::start(m3, None);
    retry.text(&["the answer, again"], true);
    retry.complete(ResponseEnding::finished());
    let (last_seq, hash) = stream::push(&nats, m3, 0, &retry.chunks()).await;
    let sent = command(&ctx, &completion(t, m3, last_seq, &hash)).await;
    message_confirmed(&ctx, sent, MessageStatus::Completed, "completing the retry").await;
    let retried = message_view(&ctx, &a.passport, t, m3).await;
    assert_eq!(retried["parentMessageId"], to_json(&mail_id), "{retried}");
    assert!(active_branch(&ctx, &a.passport, t).await.contains(&m3));

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t31_failing_a_turn_silence_already_closed_is_refused_message_not_streaming() {
    let ctx = TestContext::setup_with(Windows {
        stream_idle: Some(SHORT_STREAM_IDLE),
        ..Windows::default()
    })
    .await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Review this").await;
    let m2 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let mut response = Streamed::start(m2, None);
    response.text(&["half"], false);
    stream::push(&ctx.nats.url(), m2, 0, &response.chunks()).await;

    tokio::time::sleep(SHORT_STREAM_IDLE * 2).await;
    let view = await_message(&ctx, &a.passport, t, m2, "M2 failed by silence", |view| {
        view["status"] == "FAILED"
    })
    .await;
    assert_eq!(
        closure_kind(&view).as_deref(),
        Some("interrupted"),
        "{view}"
    );

    let interrupted = ClosureReason::interrupted();
    let sent = command(
        &ctx,
        &FailMessageV2 {
            message_id: m2,
            thread_id: t,
            seal: None,
            closure: Some(LibraryJson::from_json(to_json(&interrupted))),
            acting_user_id: None,
        },
    )
    .await;
    message_refused(
        &ctx,
        sent,
        ReasonCode::MessageNotStreaming,
        "failing a silenced turn",
    )
    .await;
    assert_eq!(message_view(&ctx, &a.passport, t, m2).await, view);

    ctx.teardown().await;
}
