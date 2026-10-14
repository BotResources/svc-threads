//! **T08 — Fail or cancel answers every open tool call.**
//!
//! > The harness opens AI message M7 STREAMING and pushes a step that ends with
//! > two tool calls c1 and c2 and finish tool_use, then a tool results item
//! > answering c1 only. Relaying a user's stop, it sends message.cancel with no
//! > count and no hash: confirmed CANCELLED; M7 holds the step, c1's result and
//! > an interrupted error result for c2 (is_error true). A runner reading the
//! > context of the thread gets a conversation the library loads and renders.
//! > The same with message.fail and last_seq + hash: FAILED, the same
//! > interrupted results, and a seal that refused chunks beyond last_seq.
//! > Cancelling M7 again is refused MESSAGE_NOT_STREAMING. A COMPLETED
//! > message's items never change.
//!
//! A cancelled message is left out of a runner's conversation (specification,
//! § Message), so the "loads and renders" check bites on the FAILED one, which
//! the conversation keeps: rendered from its own author's perspective, the
//! library refuses a turn of its own still awaiting a result.
//!
//! Channels: the answers (4), the messages as a member reads them (2), the live
//! feed (3), a runner's context rendered by the library, the fact log.

mod common;

use common::content::{
    answer, calling_step, event, results_chunk, text_delta, tool_result, tool_results_of,
};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    ai, command, context, conversation_of, create_thread, message_confirmed, message_refused,
    message_view, open_stream, say,
};
use common::{QUIET_WINDOW, TestContext, stream};
use contract_threads::llm::{Entry, Perspective, render};
use contract_threads::{CancelMessage, FailMessage, MessageStatus, ReasonCode, StreamSeal};
use serde_json::{Value, json};
use uuid::Uuid;

fn cancel(thread: Uuid, message: Uuid, seal: Option<StreamSeal>) -> CancelMessage {
    CancelMessage {
        message_id: message,
        thread_id: thread,
        seal,
        acting_user_id: None,
    }
}

fn fail(thread: Uuid, message: Uuid, seal: Option<StreamSeal>) -> FailMessage {
    FailMessage {
        message_id: message,
        thread_id: thread,
        seal,
        acting_user_id: None,
    }
}

/// The tool results of a message view, as (call, is_error) pairs, sorted.
fn answered(view: &Value) -> Vec<(String, bool)> {
    let mut pairs: Vec<(String, bool)> = tool_results_of(view)
        .into_iter()
        .map(|result| (result.tool_call_id.as_str().to_string(), result.is_error))
        .collect();
    pairs.sort();
    pairs
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t08_a_cancelled_or_failed_message_answers_every_open_tool_call_as_interrupted() {
    // Given T with member A, A's question M1, A following T, and a runner.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Look it up").await;
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let runner = ctx.service_account(true);
    let nats = ctx.nats.url();

    // Given M7 streaming: a step calling c1 and c2, then c1's result only.
    let m7 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let mut chunks = calling_step(
        0,
        &[
            ("c1", "search", json!({ "q": "q3" })),
            ("c2", "fetch", json!({ "url": "doc-7" })),
        ],
    );
    chunks.push(results_chunk(1, vec![tool_result("c1", "search", "found")]));
    stream::push(&nats, m7, 0, &chunks).await;
    a_feed
        .until("M7's first result is folded", |delta| {
            upsert_of(delta, m7).is_some_and(|view| view["items"][1]["kind"] == "TOOL_RESULTS")
        })
        .await;

    // When the agents service relays a user's stop: message.cancel, no seal.
    let cancelled = command(&ctx, &cancel(t, m7, None)).await;

    // Then (4) confirmed CANCELLED.
    message_confirmed(&ctx, cancelled, MessageStatus::Cancelled, "cancelling M7").await;
    // And (2) M7 holds its step, c1's result, and an interrupted error result
    // for c2 — under c2's own tool name.
    let view = message_view(&ctx, &a.passport, t, m7).await;
    assert_eq!(view["status"], "CANCELLED");
    assert_eq!(view["items"][0]["kind"], "STEP");
    assert_eq!(
        answered(&view),
        vec![("c1".to_string(), false), ("c2".to_string(), true)],
        "every call answered, the open one as interrupted: {view}"
    );
    let interrupted = tool_results_of(&view)
        .into_iter()
        .find(|result| result.tool_call_id.as_str() == "c2")
        .expect("c2's result");
    assert_eq!(interrupted.tool_name.as_str(), "fetch");
    assert!(
        !interrupted.content.is_empty(),
        "the result says the call was interrupted"
    );
    // And (3) A's feed carries the end.
    a_feed
        .until("A sees M7 CANCELLED", |delta| {
            upsert_of(delta, m7).is_some_and(|view| view["status"] == "CANCELLED")
        })
        .await;
    assert!(facts::types(&facts::of(&ctx.pool, "message", m7).await).contains(&"Cancelled"));

    // When M7 is cancelled again.
    let again = command(&ctx, &cancel(t, m7, None)).await;
    // Then refused MESSAGE_NOT_STREAMING.
    message_refused(
        &ctx,
        again,
        ReasonCode::MessageNotStreaming,
        "cancelling twice",
    )
    .await;

    // Given M8, a regeneration of M7: the same calls c3 and c4, c3's result.
    let analyst = ai("a1", "Analyst");
    let m8 = open_stream(&ctx, t, Some(m1), analyst.clone()).await;
    let mut chunks = calling_step(
        0,
        &[
            ("c3", "search", json!({ "q": "q3" })),
            ("c4", "fetch", json!({ "url": "doc-7" })),
        ],
    );
    chunks.push(results_chunk(1, vec![tool_result("c3", "search", "found")]));
    let (last_seq, hash) = stream::push(&nats, m8, 0, &chunks).await;

    // When the writer fails M8 with its seal: last_seq and the digest.
    let failed = command(&ctx, &fail(t, m8, Some(StreamSeal { last_seq, hash }))).await;

    // Then (4) confirmed FAILED, with the same interrupted result for c4.
    message_confirmed(&ctx, failed, MessageStatus::Failed, "failing M8").await;
    let view = message_view(&ctx, &a.passport, t, m8).await;
    assert_eq!(view["status"], "FAILED");
    assert_eq!(
        answered(&view),
        vec![("c3".to_string(), false), ("c4".to_string(), true)]
    );
    assert!(facts::types(&facts::of(&ctx.pool, "message", m8).await).contains(&"Failed"));

    // When a chunk beyond last_seq arrives after the seal.
    stream::push(&nats, m8, last_seq + 1, &[event(2, text_delta(0, "late"))]).await;
    tokio::time::sleep(QUIET_WINDOW).await;
    // Then it is refused: M8 reads as sealed.
    assert_eq!(message_view(&ctx, &a.passport, t, m8).await, view);

    // When a runner reads T's context — M8 ends the active branch.
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);

    // Then the library loads it, M8's turn whole with its interrupted result,
    // and renders it — from M8's own author's perspective too.
    assert!(
        conversation
            .entries()
            .iter()
            .any(|entry| matches!(entry, Entry::Turn(turn)
                if turn.author().map(|author| author.id().as_str()) == Some("a1"))),
        "M8's turn is in the conversation: {conversation:?}"
    );
    let perspective = Perspective::new(analyst.to_library().expect("a library author"));
    render(&conversation, &perspective)
        .unwrap_or_else(|e| panic!("the library renders the conversation: {e}"));

    // Given M9, an AI answer stored COMPLETED.
    let m9 = common::flows::answer(&ctx, t, Some(m1), ai("a2", "Critic"), answer("Fine.")).await;
    let completed = message_view(&ctx, &a.passport, t, m9).await;

    // When the writer cancels, then fails, M9.
    for (command_id, what) in [
        (command(&ctx, &cancel(t, m9, None)).await, "cancelling M9"),
        (command(&ctx, &fail(t, m9, None)).await, "failing M9"),
    ] {
        // Then each is refused MESSAGE_NOT_STREAMING and its items never change.
        message_refused(&ctx, command_id, ReasonCode::MessageNotStreaming, what).await;
    }
    assert_eq!(message_view(&ctx, &a.passport, t, m9).await, completed);

    ctx.teardown().await;
}
