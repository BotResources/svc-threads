//! **T08 — Fail or cancel closes the turn.**
//!
//! > The harness opens AI message M7 STREAMING and pushes a response ending
//! > with two runtime tool calls c1 and c2, then runtime tool results answering
//! > c1 only. Relaying a user's stop, it sends message.cancel with no seal:
//! > confirmed CANCELLED; M7 holds the response, c1's result and a cancelled
//! > closure, c2 without a result, and the library loads the turn; a member
//! > following T sees the end. Cancelling M7 again is refused
//! > MESSAGE_NOT_STREAMING. M8, a regeneration of M7, pushes a response with
//! > calls c3 and c4 and c3's result, then the deltas of a text response that
//! > never ends, and is failed with its seal (last_seq and hash): confirmed
//! > FAILED, its turn closed failed and the unfinished response kept
//! > interrupted: members read it, a runner never receives it; a chunk beyond
//! > last_seq arriving after the seal is refused and M8 reads as sealed. A
//! > runner reading the thread's context gets M8, which ends the active branch,
//! > with its complete entries and its closure, in a conversation the library
//! > loads (the cancelled M7 is left out). Completing a streaming message M10
//! > whose last response left a tool call without a result is confirmed FAILED:
//! > the library refuses a completed closure while a call is unanswered. M9, an
//! > AI answer stored COMPLETED, cancelled then failed: each refused
//! > MESSAGE_NOT_STREAMING, its entries never change. Completing, failing or
//! > cancelling a message nobody stored is refused MESSAGE_NOT_FOUND.
//!
//! No result is ever fabricated: an abnormal closure leaves the open calls
//! unanswered, as the library allows. The closure is Threads' own entry, its
//! id minted by Threads.
//!
//! The fact log records each closure as the end fact itself: M7's opening, its
//! two entries and `Cancelled`; M8's opening first and `Failed` last. The text
//! of the response M8 never finished is in no fact.
//!
//! Channels: the answers (4), the messages as a member reads them (2), the live
//! feed (3), a runner's context loaded by the library, the fact log.

mod common;

use common::content::{
    Streamed, call, calling, closure_kind, complete_entry, delta_chunk, draft_entry, draft_text,
    entries_of, entry_chunk, kinds_of, reply, result_of, results, to_json, turn_of,
};
use common::feed::{Live, upsert_of};
use common::flows::{
    ai, answer, command, completion, context, conversation_of, create_thread, message_confirmed,
    message_refused, message_view, open_stream, say, turn_ids,
};
use common::{QUIET_WINDOW, TestContext, facts, stream};
use contract_threads::llm::{ResponseEnding, RuntimeToolResult, TurnEntry};
use contract_threads::{CancelMessage, FailMessage, MessageStatus, ReasonCode, StreamSeal};
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

/// Every runtime tool result a message view holds.
fn answered(view: &serde_json::Value) -> Vec<RuntimeToolResult> {
    entries_of(view)
        .iter()
        .filter(|entry| entry["kind"] == "runtime_tool_results")
        .flat_map(|entry| match complete_entry(entry) {
            TurnEntry::RuntimeToolResults(delivered) => delivered.results().to_vec(),
            other => panic!("tool results: {other:?}"),
        })
        .collect()
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t08_a_cancelled_or_failed_message_closes_its_turn_and_leaves_its_open_tool_calls_unanswered()
 {
    // Given T with member A, A's question M1, a runner, and A following T.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Look it up").await;
    let runner = ctx.service_account(true);
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let nats = ctx.nats.url();
    let analyst = ai("a1", "Analyst");

    // When M7 streams a response ending with calls c1 and c2, then the results
    // answering c1 only, and a user's stop is relayed with no seal.
    let m7 = open_stream(&ctx, t, Some(m1), analyst.clone()).await;
    let mut r7 = Streamed::start(m7, None);
    let c1 = r7.call("search", &["{\"q\":\"one\"}"]);
    let c2 = r7.call("search", &["{\"q\":\"two\"}"]);
    r7.complete(ResponseEnding::tool_use());
    let c1_answer = results(vec![result_of(&r7.runtime_call(c1), "found one", false)]);
    let mut m7_chunks = r7.chunks();
    m7_chunks.push(entry_chunk(&c1_answer));
    stream::push(&nats, m7, 0, &m7_chunks).await;
    // A cancel with no seal ends the message where its folds are: the stop is
    // relayed once the response and c1's result are both folded COMPLETE, as a
    // user sees them before stopping.
    a_feed
        .until("M7's response and result for c1 are folded", |delta| {
            upsert_of(delta, m7).is_some_and(|view| {
                let complete = |kind: &str| {
                    entries_of(view)
                        .iter()
                        .any(|e| e["kind"] == kind && e["state"] == "COMPLETE")
                };
                complete("model_response") && complete("runtime_tool_results")
            })
        })
        .await;
    let cancelled = command(&ctx, &cancel(t, m7, None)).await;

    // Then (4) confirmed CANCELLED.
    message_confirmed(&ctx, cancelled, MessageStatus::Cancelled, "cancelling M7").await;
    // And (2) M7 holds the response, c1's result and a cancelled closure — c2
    // without a result, none fabricated — and the library loads the turn.
    let view = message_view(&ctx, &a.passport, t, m7).await;
    assert_eq!(view["status"], "CANCELLED");
    assert_eq!(
        kinds_of(&view),
        vec!["model_response", "runtime_tool_results", "turn_closure"],
        "{view}"
    );
    assert_eq!(
        to_json(&complete_entry(&entries_of(&view)[0])),
        to_json(&TurnEntry::from(r7.response()))
    );
    assert_eq!(
        to_json(&complete_entry(&entries_of(&view)[1])),
        to_json(&c1_answer)
    );
    assert_eq!(closure_kind(&view).as_deref(), Some("cancelled"));
    let results_held = answered(&view);
    assert_eq!(
        results_held.iter().map(|r| r.call()).collect::<Vec<_>>(),
        vec![c1],
        "c2 stays unanswered: no result is fabricated"
    );
    assert!(!results_held.iter().any(|r| r.call() == c2));
    let turn = turn_of(&view);
    let state = turn.as_ai().expect("an AI turn").state();
    assert!(state.is_closed());
    assert_eq!(state.outstanding_runtime_calls(), &[c2]);
    // And (3) A sees the end.
    a_feed
        .until("A sees M7 CANCELLED", |delta| {
            upsert_of(delta, m7).is_some_and(|view| view["status"] == "CANCELLED")
        })
        .await;

    // And the facts record M7's opening, its two entries and the cancellation
    // as its end.
    assert_eq!(
        facts::types(&facts::of(&ctx.pool, "message", m7).await),
        vec!["Created", "EntryAdded", "EntryAdded", "Cancelled"],
        "M7's facts"
    );

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

    // When M8, a regeneration of M7, streams a response calling c3 and c4,
    // c3's result, then the deltas of a text response that never ends, and is
    // failed with its seal.
    let m8 = open_stream(&ctx, t, Some(m1), analyst.clone()).await;
    let mut r8 = Streamed::start(m8, None);
    let c3 = r8.call("search", &["{\"q\":\"three\"}"]);
    let c4 = r8.call("search", &["{\"q\":\"four\"}"]);
    r8.complete(ResponseEnding::tool_use());
    let c3_answer = results(vec![result_of(&r8.runtime_call(c3), "found three", false)]);
    let mut unfinished = Streamed::start(m8, None);
    unfinished.text(&["so far, the answer is"], false);
    let mut m8_chunks = r8.chunks();
    m8_chunks.push(entry_chunk(&c3_answer));
    m8_chunks.extend(unfinished.chunks());
    let (last_seq, hash) = stream::push(&nats, m8, 0, &m8_chunks).await;
    let failed = command(&ctx, &fail(t, m8, Some(StreamSeal { last_seq, hash }))).await;

    // Then (4) confirmed FAILED, its turn closed failed, and members read the
    // unfinished response kept interrupted.
    message_confirmed(&ctx, failed, MessageStatus::Failed, "failing M8").await;
    let sealed = message_view(&ctx, &a.passport, t, m8).await;
    assert_eq!(
        kinds_of(&sealed),
        vec![
            "model_response",
            "runtime_tool_results",
            "model_response",
            "turn_closure"
        ],
        "{sealed}"
    );
    assert_eq!(closure_kind(&sealed).as_deref(), Some("failed"));
    let kept = &entries_of(&sealed)[2];
    assert_eq!(kept["state"], "INTERRUPTED", "{sealed}");
    assert_eq!(
        draft_text(&draft_entry(kept)),
        draft_text(unfinished.draft()),
        "members read the interrupted response as the library folded it"
    );
    // And c4 stays unanswered: the library loads the turn, closed, c4 its one
    // outstanding call.
    let m8_state = turn_of(&sealed).as_ai().expect("an AI turn").state();
    assert!(m8_state.is_closed());
    assert_eq!(m8_state.outstanding_runtime_calls(), &[c4]);
    // And the facts record M8's opening first and its failure last; no fact
    // carries the text of the response it never finished.
    let m8_facts = facts::of(&ctx.pool, "message", m8).await;
    let m8_types = facts::types(&m8_facts);
    assert_eq!(
        m8_types.first(),
        Some(&"Created"),
        "M8's facts: {m8_types:?}"
    );
    assert_eq!(m8_types.last(), Some(&"Failed"), "M8's facts: {m8_types:?}");
    facts::assert_no_fact_carries(&ctx.pool, "so far, the answer is", "M8's unfinished text").await;

    // When a chunk beyond last_seq arrives after the seal.
    let late = Streamed::start(m8, None);
    stream::push(&nats, m8, last_seq + 1, &[delta_chunk(&late.deltas()[0])]).await;
    tokio::time::sleep(QUIET_WINDOW).await;
    // Then it is refused: M8 reads as sealed.
    assert_eq!(message_view(&ctx, &a.passport, t, m8).await, sealed);

    // When a runner reads the thread's context.
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    // Then M8 ends the active branch, with its complete entries and its
    // closure, in a conversation the library loads — the cancelled M7 left out,
    // the interrupted response never received.
    assert_eq!(turn_ids(&conversation), vec![m1, m8]);
    let m8_turn = conversation.turns()[1].as_ai().expect("M8, an AI turn");
    assert_eq!(m8_turn.entries().len(), 3, "{m8_turn:?}");
    assert_eq!(
        to_json(&m8_turn.entries()[0]),
        to_json(&TurnEntry::from(r8.response()))
    );
    assert_eq!(to_json(&m8_turn.entries()[1]), to_json(&c3_answer));
    assert!(matches!(m8_turn.entries()[2], TurnEntry::TurnClosure(_)));
    assert!(
        !conversation
            .turns()
            .iter()
            .any(|turn| turn.id().as_uuid() == m7)
    );

    // When a streaming message M10, whose last response leaves a call without
    // a result, is completed.
    let m10 = open_stream(&ctx, t, Some(m8), analyst.clone()).await;
    let mut r10 = Streamed::start(m10, None);
    r10.call("search", &["{\"q\":\"ten\"}"]);
    r10.complete(ResponseEnding::tool_use());
    let (last_seq, hash) = stream::push(&nats, m10, 0, &r10.chunks()).await;
    let completed = command(&ctx, &completion(t, m10, last_seq, &hash)).await;
    // Then confirmed FAILED: the library refuses a completed closure while a
    // call is unanswered, so the turn is closed failed.
    message_confirmed(&ctx, completed, MessageStatus::Failed, "completing M10").await;
    let view = message_view(&ctx, &a.passport, t, m10).await;
    assert_eq!(view["status"], "FAILED");
    assert_eq!(closure_kind(&view).as_deref(), Some("failed"));
    assert_eq!(
        to_json(&complete_entry(&entries_of(&view)[0])),
        to_json(&TurnEntry::from(r10.response()))
    );

    // When M9, an AI answer stored COMPLETED, is cancelled then failed.
    let lookup = call("search", serde_json::json!({ "q": "nine" }));
    let mut m9_entries = vec![calling(&[&lookup])];
    m9_entries.push(results(vec![result_of(&lookup, "found nine", false)]));
    m9_entries.extend(reply("All done."));
    let m9 = answer(&ctx, t, Some(m10), analyst.clone(), m9_entries).await;
    let before = message_view(&ctx, &a.passport, t, m9).await;
    let cancelled = command(&ctx, &cancel(t, m9, None)).await;
    let failed = command(&ctx, &fail(t, m9, None)).await;
    // Then each is refused MESSAGE_NOT_STREAMING, its entries never changing.
    message_refused(
        &ctx,
        cancelled,
        ReasonCode::MessageNotStreaming,
        "cancelling a COMPLETED message",
    )
    .await;
    message_refused(
        &ctx,
        failed,
        ReasonCode::MessageNotStreaming,
        "failing a COMPLETED message",
    )
    .await;
    assert_eq!(message_view(&ctx, &a.passport, t, m9).await, before);

    // When a message nobody stored is completed, failed, cancelled.
    let nobody = Uuid::now_v7();
    let completed = command(&ctx, &completion(t, nobody, 0, &"0".repeat(64))).await;
    let failed = command(&ctx, &fail(t, nobody, None)).await;
    let cancelled = command(&ctx, &cancel(t, nobody, None)).await;
    // Then each is refused MESSAGE_NOT_FOUND.
    for (sent, what) in [
        (completed, "completing an unknown message"),
        (failed, "failing an unknown message"),
        (cancelled, "cancelling an unknown message"),
    ] {
        message_refused(&ctx, sent, ReasonCode::MessageNotFound, what).await;
    }

    ctx.teardown().await;
}
