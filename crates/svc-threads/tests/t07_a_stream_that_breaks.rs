//! **T07 — A stream that breaks: refused delta, silent writer, refused chunk.**
//!
//! > Threads runs with a short stream window (THREADS_STREAM_IDLE_MS, a
//! > configuration value). The harness opens M3 STREAMING and pushes a response
//! > delta naming a block its response never opened: the library refuses it, M3
//! > becomes FAILED with its turn closed failed, and a member following T sees
//! > it. It opens M4 STREAMING and pushes three response deltas of a text, each
//! > within the window of the previous one though the three span more than a
//! > window: M4 still streams, the window running from the last chunk. Nothing
//! > more arrives for longer than the window: M4 is FAILED with its turn closed
//! > interrupted, and the response is kept as the library's interrupted
//! > response holding the text received: members read it, a runner's context
//! > holds M4's turn without it. A later chunk for M4 is never folded and a
//! > completion is refused MESSAGE_NOT_STREAMING. It opens M5 and pushes a frame
//! > under the accumulator name of the previous version (message_content): it is
//! > refused and M5 is FAILED. The thread is closed; opening M6 on it is refused
//! > THREAD_NOT_OPEN, and the chunks pushed for M6 are never folded: no message,
//! > no fact, nothing pushed.
//!
//! A response M3 had started when its stream broke is, by the specification,
//! "a model response still streaming when the message is failed": it is kept
//! as the library's interrupted response, empty — the refused delta itself is
//! never folded. The library's code of the refusal is computed by the library
//! on the very delta sent.
//!
//! Channels: the live feed (3), the messages as a member reads them (2), the
//! answers (4), the fact log, a runner's context.

mod common;

use common::content::{
    Streamed, block_id, closure_kind, delta_chunk, draft_entry, draft_text, entries_of, kinds_of,
};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    active_branch, ai, await_message, command, completion, context, conversation_of, create_thread,
    message_refused, message_view, open_stream, say, streaming_message, thread_confirmed,
};
use common::{QUIET_WINDOW, SHORT_STREAM_IDLE, TestContext, Windows, stream};
use contract_threads::llm::{DeltaOp, DraftPhase, TurnEntry};
use contract_threads::{CloseThread, ClosingStatus, ReasonCode, seal_hash};
use serde_json::Value;

/// The one interrupted response of a failed message view, as the library's
/// draft.
fn interrupted_response(view: &Value) -> contract_threads::llm::ResponseDraft {
    let responses: Vec<Value> = entries_of(view)
        .into_iter()
        .filter(|entry| entry["kind"] == "model_response")
        .collect();
    assert_eq!(responses.len(), 1, "one response kept: {view}");
    assert_eq!(responses[0]["state"], "INTERRUPTED", "{view}");
    let draft = draft_entry(&responses[0]);
    assert!(
        matches!(draft.phase(), DraftPhase::Interrupted { .. }),
        "the library's interrupted response: {view}"
    );
    draft
}

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
    let analyst = ai("a1", "Analyst");

    // When M3 is opened, its response started, then a delta names a block the
    // response never opened.
    let m3 = open_stream(&ctx, t, Some(m1), analyst.clone()).await;
    let r3 = Streamed::start(m3, None);
    let orphan = r3.refused(DeltaOp::text_appended(block_id(), "orphan"));
    let refusal = r3
        .draft()
        .clone()
        .apply(&orphan)
        .expect_err("the library refuses the delta")
        .code()
        .as_str()
        .to_string();
    let mut m3_chunks = r3.chunks();
    m3_chunks.push(delta_chunk(&orphan));
    stream::push(&nats, m3, 0, &m3_chunks).await;

    // Then the library refuses it: M3 is FAILED, its turn closed failed, and
    // (3) A sees it.
    a_feed
        .until("A sees M3 FAILED", |delta| {
            upsert_of(delta, m3).is_some_and(|view| view["status"] == "FAILED")
        })
        .await;
    let failed = message_view(&ctx, &a.passport, t, m3).await;
    assert_eq!(failed["status"], "FAILED");
    assert_eq!(
        kinds_of(&failed),
        vec!["model_response", "turn_closure"],
        "the started response is kept, the closure ends the turn: {failed}"
    );
    assert_eq!(closure_kind(&failed).as_deref(), Some("failed"));
    assert_eq!(
        draft_text(&interrupted_response(&failed)),
        "",
        "the refused delta is never folded"
    );
    let m3_facts = facts::of(&ctx.pool, "message", m3).await;
    assert_eq!(
        facts::the(&m3_facts, "Failed").payload["reason"],
        refusal.as_str(),
        "the Failed fact names the library's code"
    );

    // When M4 is opened and receives three pushes of a text response, each
    // within the stream window of the previous one, though the three span more
    // than a window.
    let m4 = open_stream(&ctx, t, Some(m1), analyst.clone()).await;
    let mut r4 = Streamed::start(m4, Some("m-1"));
    r4.text(&["half ", "said"], false);
    let m4_chunks = r4.chunks();
    let pushes: [&[_]; 3] = [&m4_chunks[0..2], &m4_chunks[2..3], &m4_chunks[3..4]];
    let gap = SHORT_STREAM_IDLE * 3 / 5;
    let mut seq = 0;
    for push in pushes {
        stream::push(&nats, m4, seq, push).await;
        seq += push.len() as u64;
        tokio::time::sleep(gap).await;
    }
    let (last_seq, hash) = (seq - 1, seal_hash(&m4_chunks).expect("the chunks hash"));

    // Then M4 still streams: the window runs from the last chunk.
    assert_eq!(
        message_view(&ctx, &a.passport, t, m4).await["status"],
        "STREAMING",
        "a writer that keeps pushing within the window is not failed"
    );

    // When nothing more arrives for longer than the window.
    tokio::time::sleep(SHORT_STREAM_IDLE).await;

    // Then M4 is FAILED, its turn closed interrupted, and members read the
    // response kept as the library's interrupted response, holding the text.
    let view = await_message(
        &ctx,
        &a.passport,
        t,
        m4,
        "M4 failed by the window",
        |view| view["status"] == "FAILED",
    )
    .await;
    assert_eq!(
        kinds_of(&view),
        vec!["model_response", "turn_closure"],
        "{view}"
    );
    assert_eq!(
        closure_kind(&view).as_deref(),
        Some("interrupted"),
        "{view}"
    );
    let kept = interrupted_response(&view);
    assert_eq!(draft_text(&kept), draft_text(r4.draft()));
    assert_eq!(draft_text(&kept), "half said");
    // And a runner's context holds M4's turn without it: the closure alone.
    let runner = ctx.service_account(true);
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    let m4_turn = conversation
        .turns()
        .iter()
        .find(|turn| turn.id().as_uuid() == m4)
        .and_then(|turn| turn.as_ai())
        .expect("M4's turn in the runner's context")
        .clone();
    assert!(
        m4_turn
            .entries()
            .iter()
            .all(|entry| matches!(entry, TurnEntry::TurnClosure(_))),
        "a runner never receives the interrupted response: {m4_turn:?}"
    );
    assert!(m4_turn.is_closed());

    // When a chunk, then a completion, arrive for M4.
    let late = Streamed::start(m4, None);
    stream::push(&nats, m4, last_seq + 1, &late.chunks()).await;
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

    // When M5 is opened and receives a frame under the previous version's
    // accumulator name.
    let m5 = open_stream(&ctx, t, Some(m1), ai("a2", "Critic")).await;
    let r5 = Streamed::start(m5, None);
    stream::push_under_previous_accumulator(&nats, m5, 0, &r5.chunks()[0]).await;

    // Then it is refused and M5 is FAILED, nothing of the frame folded.
    let view = await_message(&ctx, &a.passport, t, m5, "M5 failed", |view| {
        view["status"] == "FAILED"
    })
    .await;
    assert_eq!(kinds_of(&view), vec!["turn_closure"], "{view}");
    assert_eq!(closure_kind(&view).as_deref(), Some("failed"));

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
    let open_m6 = streaming_message(t, Some(m1), &analyst);
    let m6 = open_m6.message_id;
    let refused = command(&ctx, &open_m6).await;
    let mut r6 = Streamed::start(m6, None);
    r6.text(&["never"], false);
    stream::push(&nats, m6, 0, &r6.chunks()).await;

    // Then M6 is refused THREAD_NOT_OPEN, and nothing of it is ever folded: no
    // message, no fact, nothing pushed.
    message_refused(&ctx, refused, ReasonCode::ThreadNotOpen, "opening M6").await;
    a_feed
        .until("T's closure reaches A's feed", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["status"] == "COMPLETED")
        })
        .await;
    a_feed.silence("M6's chunks are never folded").await;
    assert!(facts::of(&ctx.pool, "message", m6).await.is_empty());
    assert!(
        !active_branch(&ctx, &a.passport, t).await.contains(&m6),
        "no message M6"
    );

    ctx.teardown().await;
}
