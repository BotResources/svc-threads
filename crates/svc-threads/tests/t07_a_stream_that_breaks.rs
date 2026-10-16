//! **T07 — A stream that breaks: invalid step, silent writer, refused chunk.**
//!
//! > The harness opens M3 STREAMING and pushes, for item 0, a text_delta before
//! > any block_start: the library refuses it, M3 becomes FAILED and a
//! > subscriber sees it. It opens M4 STREAMING, pushes a block_start and two
//! > text deltas, then nothing: once the stream window elapses M4 is FAILED with
//! > its unfinished step kept as the library finishes it, and later chunks or a
//! > completion for M4 are refused MESSAGE_NOT_STREAMING. It opens M5, pushes
//! > only a block_start and nothing the library can finish, then fails it: M5
//! > is FAILED without that step. Opening M6 on a closed thread is refused
//! > THREAD_NOT_OPEN and its chunks are never folded.
//!
//! The rules of `br-llm-messages` 0.5 a stream meets: a tool name every
//! provider accepts, judged as the chunk folds (M7); a tool call id used once
//! in the conversation, judged when the stream closes against the messages of
//! its branch, before it (M8) and after it — a message stored under it while
//! it streamed (M9). Each failure names the library's code in its `Failed`
//! fact's reason.
//!
//! The stream window is the specification's 10 minutes; this scenario — and
//! only this one — runs the binary with it configured short (`THREADS_STREAM_IDLE_MS`, see
//! `common::constants`) — a configuration value, never a test seam.
//!
//! Channels: the live feed (3), the messages as a member reads them (2), the
//! answers (4), the fact log.

mod common;

use common::content::{
    ai_text, calling_step, event, opening, raw_event, results_chunk, step, text_delta, text_start,
    text_step, tool_call, tool_result, tool_results,
};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    ai, ai_message, await_message, command, completion, create_thread, message_confirmed,
    message_refused, message_view, open_stream, say, store, streaming_message, thread_confirmed,
};
use common::{QUIET_WINDOW, SHORT_STREAM_IDLE, TestContext, Windows, stream};
use contract_threads::llm::StopReason;
use contract_threads::{
    CloseThread, ClosingStatus, FailMessage, MessageStatus, ReasonCode, StreamSeal, seal_hash,
};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t07_a_broken_stream_fails_its_message_and_an_unfinished_step_is_kept_or_dropped() {
    // Given Threads running with a short stream window; T with member A, A's
    // question M1, and A following T.
    let ctx = TestContext::setup_with(Windows {
        stream_idle: Some(SHORT_STREAM_IDLE),
        ..Windows::default()
    })
    .await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Go on").await;
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let nats = ctx.nats.url();

    // When M3 is opened and its first chunk is a text_delta before any
    // block_start.
    let m3 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    stream::push(&nats, m3, 0, &[event(0, text_delta(0, "orphan"))]).await;

    // Then the library refuses it: M3 is FAILED, and (3) A sees it.
    a_feed
        .until("A sees M3 FAILED", |delta| {
            upsert_of(delta, m3).is_some_and(|view| view["status"] == "FAILED")
        })
        .await;
    let failed = message_view(&ctx, &a.passport, t, m3).await;
    assert_eq!(failed["status"], "FAILED");
    assert_eq!(
        failed["items"],
        json!([]),
        "nothing the library refused is kept"
    );
    assert!(
        facts::types(&facts::of(&ctx.pool, "message", m3).await).contains(&"Failed"),
        "a Failed fact records M3's end"
    );

    // When M4 is opened and receives a block_start and two text deltas, each
    // within the stream window of the previous one, though the three span
    // more than a window.
    let m4 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let m4_chunks = vec![
        opening(0, text_start(0), Uuid::now_v7(), Some("m-1")),
        event(0, text_delta(0, "half ")),
        event(0, text_delta(0, "said")),
    ];
    let gap = SHORT_STREAM_IDLE * 3 / 5;
    for (seq, chunk) in m4_chunks.iter().enumerate() {
        stream::push(&nats, m4, seq as u64, std::slice::from_ref(chunk)).await;
        tokio::time::sleep(gap).await;
    }
    let (last_seq, hash) = (
        (m4_chunks.len() - 1) as u64,
        seal_hash(&m4_chunks).expect("the chunks hash"),
    );

    // Then M4 still streams: the window runs from the last chunk, not from
    // the opening.
    assert_eq!(
        message_view(&ctx, &a.passport, t, m4).await["status"],
        "STREAMING",
        "a writer that keeps pushing within the window is not failed"
    );

    // When nothing more arrives for longer than the window.
    tokio::time::sleep(SHORT_STREAM_IDLE).await;

    // Then M4 is FAILED, its unfinished step kept as the library finishes it.
    let view = await_message(
        &ctx,
        &a.passport,
        t,
        m4,
        "M4 failed by the window",
        |view| view["status"] == "FAILED",
    )
    .await;
    assert_eq!(view["items"][0]["kind"], "STEP", "{view}");
    assert_eq!(view["items"][0]["status"], "FAILED", "{view}");
    assert_eq!(view["items"][0]["model"], "m-1", "{view}");
    assert_eq!(
        view["items"][0]["blocks"][0]["content"]["text"], "half said",
        "{view}"
    );

    // When a chunk, then a completion, arrive for M4.
    stream::push(&nats, m4, last_seq + 1, &[event(0, text_delta(0, " more"))]).await;
    tokio::time::sleep(QUIET_WINDOW).await;
    let late_completion = command(&ctx, &completion(t, m4, last_seq, &hash)).await;

    // Then the chunk is never folded and the completion is refused
    // MESSAGE_NOT_STREAMING.
    message_refused(
        &ctx,
        late_completion,
        ReasonCode::MessageNotStreaming,
        "completing M4 after its window",
    )
    .await;
    assert_eq!(message_view(&ctx, &a.passport, t, m4).await, view);

    // When M5 is opened, receives only a block_start, and is failed.
    let m5 = open_stream(&ctx, t, Some(m1), ai("a2", "Critic")).await;
    let (last_seq, hash) = stream::push(
        &nats,
        m5,
        0,
        &[opening(0, text_start(0), Uuid::now_v7(), None)],
    )
    .await;
    let fail = command(
        &ctx,
        &FailMessage {
            message_id: m5,
            thread_id: t,
            seal: Some(StreamSeal { last_seq, hash }),
            acting_user_id: None,
        },
    )
    .await;

    // Then M5 is FAILED, without the step the library could not finish.
    message_confirmed(&ctx, fail, MessageStatus::Failed, "failing M5").await;
    let view = message_view(&ctx, &a.passport, t, m5).await;
    assert_eq!(view["status"], "FAILED");
    assert_eq!(view["items"], json!([]), "an unfinishable step is dropped");

    // When M7 is opened and its first step calls a tool no provider accepts.
    let m7 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    stream::push(
        &nats,
        m7,
        0,
        &[raw_event(
            0,
            json!({ "type": "tool_call_start", "index": 0, "id": "c1", "name": "web.search" }),
            Some(Uuid::now_v7()),
        )],
    )
    .await;
    // Then the library refuses it: M7 is FAILED with nothing kept, and its
    // Failed fact names the library's code.
    let view = await_message(&ctx, &a.passport, t, m7, "M7 failed", |view| {
        view["status"] == "FAILED"
    })
    .await;
    assert_eq!(view["items"], json!([]), "{view}");
    let m7_facts = facts::of(&ctx.pool, "message", m7).await;
    assert_eq!(
        facts::the(&m7_facts, "Failed").payload["reason"],
        "invalid_tool_name"
    );

    // Given an answer of the branch that called tool c-used.
    let streamed_call = |id: &str| {
        let mut chunks = calling_step(0, &[(id, "search", json!({}))]);
        chunks.push(results_chunk(1, vec![tool_result(id, "search", "found")]));
        chunks.extend(text_step(2, None, &["answered"], StopReason::EndTurn));
        chunks
    };
    let whole_call = |id: &str| {
        vec![
            step(
                StopReason::AwaitingToolResults,
                None,
                vec![tool_call(id, "search", json!({}))],
            ),
            tool_results(vec![tool_result(id, "search", "found")]),
            step(StopReason::EndTurn, None, vec![ai_text("answered")]),
        ]
    };
    let used = store(
        &ctx,
        ai_message(t, Some(m1), ai("a1", "Analyst"), whole_call("c-used")),
        MessageStatus::Completed,
    )
    .await;
    // When M8, answering it, streams a step calling c-used again and is
    // completed.
    let m8 = open_stream(&ctx, t, Some(used), ai("a2", "Critic")).await;
    let (last_seq, hash) = stream::push(&nats, m8, 0, &streamed_call("c-used")).await;
    let completed = command(&ctx, &completion(t, m8, last_seq, &hash)).await;
    // Then M8 is FAILED from the step reusing the id, and its Failed fact
    // names the library's code.
    message_confirmed(&ctx, completed, MessageStatus::Failed, "completing M8").await;
    let view = message_view(&ctx, &a.passport, t, m8).await;
    assert_eq!(view["status"], "FAILED");
    assert_eq!(view["items"], json!([]), "its first step reuses c-used");
    let m8_facts = facts::of(&ctx.pool, "message", m8).await;
    assert_eq!(
        facts::the(&m8_facts, "Failed").payload["reason"],
        "duplicate_tool_call_id_in_conversation"
    );

    // When M9 is opened, an answer to it calling c-later is stored while it
    // streams, then M9 streams a step calling c-later and is completed.
    let m9 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let later = store(
        &ctx,
        ai_message(t, Some(m9), ai("a2", "Critic"), whole_call("c-later")),
        MessageStatus::Completed,
    )
    .await;
    let (last_seq, hash) = stream::push(&nats, m9, 0, &streamed_call("c-later")).await;
    let completed = command(&ctx, &completion(t, m9, last_seq, &hash)).await;
    // Then M9 is FAILED the same way, and the answer stored under it stays.
    message_confirmed(&ctx, completed, MessageStatus::Failed, "completing M9").await;
    let m9_facts = facts::of(&ctx.pool, "message", m9).await;
    assert_eq!(
        facts::the(&m9_facts, "Failed").payload["reason"],
        "duplicate_tool_call_id_in_conversation"
    );
    assert_eq!(
        message_view(&ctx, &a.passport, t, later).await["status"],
        "COMPLETED"
    );

    // When T is closed and M6 is opened on it, its chunks pushed all the same.
    let closed = command(
        &ctx,
        &CloseThread {
            thread_id: t,
            status: ClosingStatus::Completed,
            acting_user_id: None,
        },
    )
    .await;
    thread_confirmed(&ctx, closed, "closing T").await;
    let open_m6 = streaming_message(t, Some(m1), ai("a1", "Analyst"));
    let m6 = open_m6.message_id;
    let refused = command(&ctx, &open_m6).await;
    let m6_chunks = vec![
        opening(0, text_start(0), Uuid::now_v7(), None),
        event(0, text_delta(0, "never")),
    ];
    stream::push(&nats, m6, 0, &m6_chunks).await;

    // Then M6 is refused THREAD_NOT_OPEN, and nothing of it is ever folded:
    // no message, no fact, nothing pushed.
    message_refused(&ctx, refused, ReasonCode::ThreadNotOpen, "opening M6").await;
    a_feed
        .until("T's closure reaches A's feed", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["status"] == "COMPLETED")
        })
        .await;
    a_feed.silence("M6's chunks are never folded").await;
    assert!(facts::of(&ctx.pool, "message", m6).await.is_empty());

    ctx.teardown().await;
}
