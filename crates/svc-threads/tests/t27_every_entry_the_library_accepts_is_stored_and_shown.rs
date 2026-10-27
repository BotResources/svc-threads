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
//! > nothing is stored. A whole AI turn closed budget_exhausted is stored
//! > COMPLETED, one closed cancelled is stored CANCELLED, and one closed failed
//! > or interrupted is stored FAILED. An AI message whose input turn holds a
//! > human message is refused MESSAGE_INVALID (human_message_in_ai_message): a
//! > human message only travels in a user message.
//!
//! "Every entry the library accepts" is the library's own list,
//! `TurnEntryKind::ALL`: the scenario stores at least one entry of each kind
//! and checks every kind of that list is read back. A library bump that adds a
//! kind turns this scenario red until a message holding it is stored here.
//! Beside the gesture's turns, a fourth AI turn of a1 holds what the gesture's
//! do not — a request accepted, a task that failed, a runtime tool call, a
//! runtime notice while the call waits for its results (an order the library
//! accepts since 2.1), then the results — a fifth message is an input turn
//! holding a runtime notice, and A's opening question is the human message.
//!
//! T is created with a title, so that no title Job is in play: any Job, any
//! command to Jobs, would come of these messages. Threads commands nothing but
//! Jobs, and creates no thread but on a command; the threads it holds are read
//! through the members' lists and the members entries it publishes.
//!
//! Channels: the answers (4), the messages as a member reads them (2), A's live
//! feed of T — each stored message pushed, nothing for a refused one (3) — a
//! runner's context, Jobs' side of the bus and the published members entries
//! (4), the members' lists.

mod common;

use std::collections::BTreeSet;

use common::content::{
    ai_turn, assert_same_turn, call, calling, closed, closure, closure_kind, conversation_id,
    human, input_turn, kinds_of, mail, queue_position, reply, request, request_accepted, result_of,
    results, runtime_notice, task, task_completed, task_failed, task_started, text, to_json,
    turn_of, turn_refusal,
};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    active_branch, ai, command, context, conversation_of, create, creation, listed,
    message_refused, message_view, raw_whole_message, say, store, whole_message,
};
use common::{QUIET_WINDOW, TestContext};
use contract_threads::llm::{ClosureReason, Conversation, Turn, TurnEntryKind};
use contract_threads::{MessageStatus, ReasonCode};
use serde_json::Value;
use uuid::Uuid;

/// Reads `feed` until it has pushed every message of `messages`.
async fn pushed_all(feed: &mut Live, messages: &[Uuid], what: &str) {
    feed.collect_until(what, |seen: &[Value]| {
        messages.iter().all(|message| {
            seen.iter()
                .any(|delta| upsert_of(delta, *message).is_some())
        })
    })
    .await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t27_every_entry_the_library_accepts_is_stored_and_shown_with_no_behaviour_of_its_own() {
    // Given T, titled, with member A, following it; agents a1, a2, a3; a
    // runner.
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
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;

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
    // And an AI turn of a1 holding the kinds the others do not: a request
    // accepted, a task started then failed, a runtime tool call, a runtime
    // notice while the call waits, its results, an answer and its closure.
    let failing = task();
    let lookup = call("search", serde_json::json!({ "q": "sources" }));
    let m4 = Uuid::now_v7();
    let a1_second_turn = Turn::from(ai_turn(
        m4,
        &analyst,
        closed(
            [
                vec![
                    request_accepted(),
                    task_started(failing),
                    task_failed(failing, "the source was unreachable"),
                    calling(&[&lookup]),
                    runtime_notice("the search is slow to answer"),
                    results(vec![result_of(&lookup, "two sources", false)]),
                ],
                reply("Done with what was reachable."),
            ]
            .concat(),
        ),
    ));
    store(
        &ctx,
        whole_message(t, Some(m3), m4, &analyst, &a1_second_turn, Vec::new()),
        MessageStatus::Completed,
    )
    .await;
    // And an input turn holding a runtime notice.
    let m5 = Uuid::now_v7();
    let notice_turn = Turn::from(input_turn(
        m5,
        vec![runtime_notice("the analyst resumes after a restart")],
    ));
    store(
        &ctx,
        whole_message(t, Some(m4), m5, &analyst, &notice_turn, Vec::new()),
        MessageStatus::Completed,
    )
    .await;

    // Then (3) A's feed receives each of them.
    pushed_all(
        &mut a_feed,
        &[m1, m2, m3, m4, m5],
        "each stored message reaches A",
    )
    .await;

    // And (2) members read every entry with its kind, in order — each the
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
        (
            m4,
            &a1_second_turn,
            vec![
                "request_accepted",
                "task_started",
                "task_failed",
                "model_response",
                "runtime_notice",
                "runtime_tool_results",
                "model_response",
                "turn_closure",
            ],
        ),
        (m5, &notice_turn, vec!["runtime_notice"]),
    ] {
        let view = message_view(&ctx, &a.passport, t, message).await;
        assert_eq!(kinds_of(&view), kinds, "{view}");
        assert_eq!(view["status"], "COMPLETED");
        assert_same_turn(&turn_of(&view), turn, "a turn of every entry");
    }
    // And every kind the library lists is among what members read: A's
    // question holds the human message.
    let mut read = BTreeSet::new();
    for message in [m0, m1, m2, m3, m4, m5] {
        read.extend(kinds_of(&message_view(&ctx, &a.passport, t, message).await));
    }
    for kind in TurnEntryKind::ALL {
        assert!(
            read.contains(kind.as_str()),
            "the library accepts {kind:?}: a stored message holds one ({read:?})"
        );
    }

    // And a runner's context holds them as the library gives them.
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    let mut expected = vec![conversation.turns()[0].clone()];
    expected.extend([
        a1_turn.clone(),
        request_turn.clone(),
        queued_turn.clone(),
        a1_second_turn.clone(),
        notice_turn.clone(),
    ]);
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
    a_feed.drain().await;
    let lookup = call("search", serde_json::json!({ "q": "orphan" }));
    let orphan = Uuid::now_v7();
    let mut orphan_turn = to_json(&Turn::from(ai_turn(orphan, &analyst, Vec::new())));
    orphan_turn["entries"] = serde_json::json!([
        to_json(&results(vec![result_of(&lookup, "found", false)])),
        to_json(&closure(ClosureReason::completed())),
    ]);
    let code = turn_refusal(&orphan_turn).expect("the library refuses that order");
    let refused = command(
        &ctx,
        &raw_whole_message(t, Some(m5), orphan, &analyst, orphan_turn, Vec::new()),
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

    // When an AI message's input turn holds a human message.
    let posing = Uuid::now_v7();
    let posing_turn = Turn::from(input_turn(
        posing,
        vec![human(a.id, vec![text("not an agent's words")])],
    ));
    let refused = command(
        &ctx,
        &whole_message(t, Some(m5), posing, &analyst, &posing_turn, Vec::new()),
    )
    .await;
    // Then refused MESSAGE_INVALID (human_message_in_ai_message), and nothing
    // is stored: a human message only travels in a user message.
    let answer = message_refused(
        &ctx,
        refused,
        ReasonCode::MessageInvalid,
        "a human message in an AI message",
    )
    .await;
    assert_eq!(
        answer.params["error"], "human_message_in_ai_message",
        "{answer:?}"
    );
    assert!(facts::of(&ctx.pool, "message", posing).await.is_empty());
    // And neither refused message reaches A, nor the branch.
    a_feed.silence("a refused message reaches no one").await;
    assert_eq!(
        active_branch(&ctx, &a.passport, t).await,
        vec![m0, m1, m2, m3, m4, m5]
    );

    // When whole AI turns closed otherwise than completed are stored:
    // budget_exhausted, cancelled, failed, interrupted.
    let mut parent = m5;
    let mut closed_otherwise = Vec::new();
    for (reason, status, read_as) in [
        (
            ClosureReason::budget_exhausted(),
            MessageStatus::Completed,
            "COMPLETED",
        ),
        (
            ClosureReason::cancelled(),
            MessageStatus::Cancelled,
            "CANCELLED",
        ),
        (
            ClosureReason::failed("the model gave up").expect("a failure detail"),
            MessageStatus::Failed,
            "FAILED",
        ),
        (
            ClosureReason::interrupted(),
            MessageStatus::Failed,
            "FAILED",
        ),
    ] {
        let message = Uuid::now_v7();
        let kind = to_json(&reason)["type"]
            .as_str()
            .expect("a closure type")
            .to_string();
        let mut entries = reply(&format!("an answer closed {kind}"));
        entries.push(closure(reason));
        let turn = Turn::from(ai_turn(message, &analyst, entries));
        // Then each is accepted, under the status its closure gives it.
        store(
            &ctx,
            whole_message(t, Some(parent), message, &analyst, &turn, Vec::new()),
            status,
        )
        .await;
        let view = message_view(&ctx, &a.passport, t, message).await;
        assert_eq!(view["status"], read_as, "a turn closed {kind}: {view}");
        assert_eq!(closure_kind(&view).as_deref(), Some(kind.as_str()));
        assert_same_turn(&turn_of(&view), &turn, "a turn closed otherwise");
        closed_otherwise.push(message);
        parent = message;
    }
    // And (3) each reaches A's feed.
    pushed_all(
        &mut a_feed,
        &closed_otherwise,
        "each turn closed otherwise reaches A",
    )
    .await;

    ctx.teardown().await;
}
