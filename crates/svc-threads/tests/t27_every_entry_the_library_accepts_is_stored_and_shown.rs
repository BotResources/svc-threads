//! **T27 — Every entry the library accepts is stored and shown.**
//!
//! > The harness stores on thread T, COMPLETED: an AI turn of agent a1 holding
//! > an agent's mail received mid-turn, a task started, the task completed with
//! > its summary, a model response and its closure; an input turn holding a
//! > request of agent a2 with its source conversation; an input turn holding a
//! > queue position. Each is confirmed; members read every entry with its
//! > kind, in order; a runner's context holds them as the library gives them;
//! > no Job, no thread and no command comes of them. A turn whose entries the
//! > library refuses in that order (runtime tool results without a call before
//! > them) is refused MESSAGE_INVALID naming the library's error code, and
//! > nothing is stored.
//!
//! T is created with a title, so that no title Job is in play: any Job, any
//! command to Jobs, would come of these messages. Threads commands nothing but
//! Jobs, and creates no thread but on a command; the threads it holds are read
//! through the members' lists and the members entries it publishes.
//!
//! Channels: the answers (4), the messages as a member reads them (2), a
//! runner's context, Jobs' side of the bus and the published members entries
//! (4), the members' lists.

mod common;

use common::content::{
    ai_turn, assert_same_turn, call, closed, conversation_id, input_turn, kinds_of, mail,
    queue_position, reply, request, result_of, results, task, task_completed, task_started, text,
    to_json, turn_of, turn_refusal,
};
use common::facts;
use common::flows::{
    active_branch, ai, command, context, conversation_of, create, creation, listed,
    message_refused, raw_whole_message, say, store, whole_message,
};
use common::{QUIET_WINDOW, TestContext};
use contract_threads::llm::{Conversation, Turn};
use contract_threads::{MessageStatus, ReasonCode};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t27_every_entry_the_library_accepts_is_stored_and_shown_with_no_behaviour_of_its_own() {
    // Given T, titled, with member A; agents a1, a2, a3; a runner.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let mut born = creation(Some(a.id), &[]);
    born.title = Some("agents at work".to_string());
    let t = create(&ctx, born).await;
    let (analyst, critic, scout) = (ai("a1", "Analyst"), ai("a2", "Critic"), ai("a3", "Scout"));
    let runner = ctx.service_account(true);
    let m0 = say(&ctx, t, None, &a, "Over to the agents").await;
    let jobs_before = ctx.jobs.total();
    let listed_before = listed(&ctx, &a.passport).await;

    // When the harness stores, COMPLETED, an AI turn of a1: an agent's mail
    // received mid-turn, a task started, the task completed with its summary,
    // a model response and its closure.
    let source = Uuid::now_v7();
    let job = task();
    let m1 = Uuid::now_v7();
    let a1_turn = Turn::from(ai_turn(
        m1,
        &analyst,
        closed(
            [
                vec![
                    mail(&scout, source, vec![text("the scout's findings")]),
                    task_started(job),
                    task_completed(job, "the findings are summarised"),
                ],
                reply("Here is the summary."),
            ]
            .concat(),
        ),
    ));
    store(
        &ctx,
        whole_message(t, Some(m0), m1, &analyst, &a1_turn, Vec::new()),
        MessageStatus::Completed,
    )
    .await;
    // And an input turn holding a request of a2 with its source conversation.
    let m2 = Uuid::now_v7();
    let request_turn = Turn::from(input_turn(
        m2,
        vec![request(
            &critic,
            source,
            vec![text("please review the summary")],
        )],
    ));
    store(
        &ctx,
        whole_message(t, Some(m1), m2, &critic, &request_turn, Vec::new()),
        MessageStatus::Completed,
    )
    .await;
    // And an input turn holding a queue position.
    let m3 = Uuid::now_v7();
    let queued_turn = Turn::from(input_turn(m3, vec![queue_position(2)]));
    store(
        &ctx,
        whole_message(t, Some(m2), m3, &analyst, &queued_turn, Vec::new()),
        MessageStatus::Completed,
    )
    .await;

    // Then (2) members read every entry with its kind, in order — each the
    // library's.
    for (message, turn, kinds) in [
        (
            m1,
            &a1_turn,
            vec![
                "agent_mail",
                "task_started",
                "task_completed",
                "model_response",
                "turn_closure",
            ],
        ),
        (m2, &request_turn, vec!["agent_request"]),
        (m3, &queued_turn, vec!["queue_position"]),
    ] {
        let view = common::flows::message_view(&ctx, &a.passport, t, message).await;
        assert_eq!(kinds_of(&view), kinds, "{view}");
        assert_eq!(view["status"], "COMPLETED");
        assert_same_turn(&turn_of(&view), turn, "a turn of every entry");
    }

    // And a runner's context holds them as the library gives them.
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    let mut expected = vec![conversation.turns()[0].clone()];
    expected.extend([a1_turn.clone(), request_turn.clone(), queued_turn.clone()]);
    assert_eq!(
        conversation,
        Conversation::from_parts(conversation_id(t), expected)
            .expect("the branch is a conversation the library accepts"),
    );

    // And no Job, no thread and no command comes of them.
    tokio::time::sleep(QUIET_WINDOW).await;
    ctx.jobs
        .expect_nothing_beyond(jobs_before, "the entries start nothing")
        .await;
    assert_eq!(
        listed(&ctx, &a.passport).await,
        listed_before,
        "no thread is created"
    );
    assert!(
        ctx.published_members(source).await.is_none(),
        "the source conversation of a request is no thread of Threads"
    );

    // When a turn holds runtime tool results without a call before them.
    let lookup = call("search", serde_json::json!({ "q": "orphan" }));
    let orphan = Uuid::now_v7();
    let mut orphan_turn = to_json(&Turn::from(ai_turn(orphan, &analyst, Vec::new())));
    orphan_turn["entries"] = serde_json::json!([
        to_json(&results(vec![result_of(&lookup, "found", false)])),
        to_json(&common::content::closure(
            contract_threads::llm::ClosureReason::completed()
        )),
    ]);
    let code = turn_refusal(&orphan_turn).expect("the library refuses that order");
    let refused = command(
        &ctx,
        &raw_whole_message(t, Some(m3), orphan, &analyst, orphan_turn, Vec::new()),
    )
    .await;

    // Then refused MESSAGE_INVALID naming the library's error code, and
    // nothing is stored.
    let answer = message_refused(
        &ctx,
        refused,
        ReasonCode::MessageInvalid,
        "tool results without a call",
    )
    .await;
    assert_eq!(
        answer.params["error"],
        serde_json::json!(code),
        "{answer:?}"
    );
    assert!(facts::of(&ctx.pool, "message", orphan).await.is_empty());
    assert_eq!(
        active_branch(&ctx, &a.passport, t).await,
        vec![m0, m1, m2, m3]
    );

    ctx.teardown().await;
}
