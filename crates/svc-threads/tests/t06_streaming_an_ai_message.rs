//! **T06 — Streaming an AI message: chunks folded live, sealed by count and
//! hash.**
//!
//! > Playing the agents service, the harness stores AI message M2 (author
//! > ai_id a1, ai_name "Analyst", job_id J) STREAMING, parent M1: confirmed
//! > with status STREAMING, no content. Playing the runner, it publishes on
//! > stream.threads.{M2} chunks seq 0..n: for item 0, a block_start text with a
//! > block id and model "m-1", text deltas, block_end, a tool_call_start with a
//! > block id, argument deltas, block_end and finish tool_use; for item 1, a
//! > whole tool results item answering the call; for item 2, a step with text
//! > and finish end_turn, then an ARTIFACT block. A member subscribed to T sees
//! > the text grow and each item complete on its own; a member joining
//! > mid-stream sees everything already folded. The harness completes M2 with
//! > last_seq n and the SHA-256 of the chunks: confirmed COMPLETED; M2 reads as
//! > a turn of three items, item 0 carrying model m-1, and never changes again.
//! > ItemAdded facts were recorded per item, never per chunk. A completion with
//! > a wrong hash, or with last_seq below a chunk that arrived, is refused
//! > STREAM_MISMATCH and the message stays STREAMING; a corrected completion
//! > then succeeds. A chunk published after the seal is ignored on every pod.
//! > Completing M2 again is refused MESSAGE_NOT_STREAMING.
//!
//! One deliberate reading: the specification says "Blocks are added in order
//! while their item streams and never change once it is complete", and an item
//! completes on its `finish`. The ARTIFACT block of item 2 is therefore pushed
//! before item 2's `finish`, not after it as the gesture's wording orders it.
//!
//! Channels: the answers (4); the live feed of a member over SSE and of a late
//! joiner over WebSocket (3); the message as members read it, on two pods (2);
//! the fact log; the artifact the stream attaches.

mod common;

use common::artifacts::{File, download_url, fetch, upload};
use common::content::{
    arguments_delta, artifact_block, block_chunk, block_end, call_start, event, finish, opening,
    results_chunk, text_content, text_delta, text_start, tool_result,
};
use common::facts;
use common::feed::{Live, THREAD_FEED, in_reset, thread_feed, upsert_of};
use common::flows::{
    ai, command, completion, create_thread, message_confirmed, message_refused, message_view, say,
    store, streaming_message,
};
use common::{Q_MESSAGES, TestContext, gql, stream};
use contract_threads::llm::{AssistantBlock, StopReason, ToolCall, ToolCallId, ToolName};
use contract_threads::{MessageStatus, ReasonCode, seal_hash};
use serde_json::{Value, json};
use uuid::Uuid;

/// The text of item `item`'s first block in a message view, if any.
fn item_text(view: &Value, item: usize) -> Option<String> {
    view["items"][item]["blocks"][0]["content"]["text"]
        .as_str()
        .map(str::to_string)
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t06_a_streaming_ai_message_is_folded_live_and_sealed_by_its_last_chunk_and_digest() {
    // Given T with members O, A and B, A's question M1, an artifact X a runner
    // uploaded for T, a second pod of the deployment, and A following T.
    let mut ctx = TestContext::setup().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b]).await;
    let m1 = say(&ctx, t, None, &a, "Please analyse Q3").await;
    let runner = ctx.service_account(true);
    let file = File::csv("quarter,revenue\nQ3,42\n");
    let x = upload(&ctx, &runner.passport, t, &file).await;
    let pod2 = ctx.spawn_pod().await;
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;

    // When the agents service opens M2 STREAMING, by a1 "Analyst", for Job J.
    let job = Uuid::now_v7();
    let mut open = streaming_message(t, Some(m1), ai("a1", "Analyst"));
    open.job_id = Some(job);
    let m2 = store(&ctx, open, MessageStatus::Streaming).await;

    // Then (2) M2 reads STREAMING, by the AI, for J, with no content yet.
    let view = message_view(&ctx, &a.passport, t, m2).await;
    assert_eq!(view["status"], "STREAMING");
    assert_eq!(
        view["author"],
        json!({ "kind": "AI", "userId": null, "aiId": "a1", "name": "Analyst" })
    );
    assert_eq!(view["jobId"], Value::String(job.to_string()));
    assert_eq!(view["parentMessageId"], Value::String(m1.to_string()));
    assert_eq!(
        (view["items"].clone(), view["blocks"].clone()),
        (json!([]), json!([]))
    );
    // And (3) A's feed receives it.
    a_feed
        .until("M2 enters A's feed", |delta| upsert_of(delta, m2).is_some())
        .await;

    // Item 0: a text block growing, a tool call, finish awaiting its result.
    let (b0, b1) = (Uuid::now_v7(), Uuid::now_v7());
    let item0 = vec![
        opening(0, text_start(0), b0, Some("m-1")),
        event(0, text_delta(0, "Hel")),
        event(0, text_delta(0, "lo")),
        event(0, block_end(0)),
        opening(0, call_start(1, "c1", "search"), b1, None),
        event(0, arguments_delta(1, "{\"q\":")),
        event(0, arguments_delta(1, "\"rust\"}")),
        event(0, block_end(1)),
        event(0, finish(StopReason::AwaitingToolResults)),
    ];
    // Item 1: the whole tool results item answering c1.
    let item1 = vec![results_chunk(1, vec![tool_result("c1", "search", "found")])];
    // Item 2: a text step carrying the ARTIFACT block, ending the turn.
    let item2 = vec![
        opening(2, text_start(0), Uuid::now_v7(), Some("m-1")),
        event(2, text_delta(0, "Done.")),
        event(2, block_end(0)),
        block_chunk(2, artifact_block(x)),
        event(2, finish(StopReason::EndTurn)),
    ];
    let chunks: Vec<_> = [item0.clone(), item1.clone(), item2.clone()].concat();
    let n = (chunks.len() - 1) as u64;
    let hash = seal_hash(&chunks).expect("the chunks hash");

    // When the runner pushes the opening and "Hel".
    stream::push(&ctx.nats.url(), m2, 0, &chunks[0..2]).await;
    // Then (3) A sees the text so far.
    a_feed
        .until("A sees `Hel`", |delta| {
            upsert_of(delta, m2).is_some_and(|view| item_text(view, 0).as_deref() == Some("Hel"))
        })
        .await;

    // When it pushes "lo".
    stream::push(&ctx.nats.url(), m2, 2, &chunks[2..3]).await;
    // Then (3) A sees the text grow.
    a_feed
        .until("A sees `Hello`", |delta| {
            upsert_of(delta, m2).is_some_and(|view| item_text(view, 0).as_deref() == Some("Hello"))
        })
        .await;

    // When B joins mid-stream, over WebSocket.
    let b_feed = Live::ws(&ctx, &b.passport, THREAD_FEED, &thread_feed(t, 20)).await;
    // Then B's Reset holds everything folded so far.
    let joined = in_reset(&b_feed.reset, m2).expect("M2 is in B's Reset");
    assert_eq!(joined["status"], "STREAMING");
    assert_eq!(item_text(&joined, 0).as_deref(), Some("Hello"));

    // When the rest of item 0 arrives.
    stream::push(&ctx.nats.url(), m2, 3, &chunks[3..item0.len()]).await;
    // Then (3) item 0 completes on its own while M2 still streams.
    a_feed
        .until("item 0 completes on its own", |delta| {
            upsert_of(delta, m2).is_some_and(|view| {
                view["status"] == "STREAMING" && view["items"][0]["status"] == "COMPLETED"
            })
        })
        .await;

    // When items 1 and 2 arrive.
    stream::push(
        &ctx.nats.url(),
        m2,
        item0.len() as u64,
        &chunks[item0.len()..],
    )
    .await;
    a_feed
        .until("item 2 completes", |delta| {
            upsert_of(delta, m2).is_some_and(|view| view["items"][2]["status"] == "COMPLETED")
        })
        .await;

    // When the writer completes M2 with a wrong digest, then with a last_seq
    // below a chunk that arrived.
    let wrong_hash = command(&ctx, &completion(t, m2, n, &"0".repeat(64))).await;
    message_refused(
        &ctx,
        wrong_hash,
        ReasonCode::StreamMismatch,
        "a wrong digest",
    )
    .await;
    let short = command(
        &ctx,
        &completion(t, m2, n - 1, &seal_hash(&chunks[..n as usize]).unwrap()),
    )
    .await;
    message_refused(
        &ctx,
        short,
        ReasonCode::StreamMismatch,
        "a last_seq too low",
    )
    .await;
    // Then M2 stays STREAMING.
    assert_eq!(
        message_view(&ctx, &a.passport, t, m2).await["status"],
        "STREAMING"
    );

    // When the corrected completion arrives: last_seq n, the chunks' digest.
    let completed = command(&ctx, &completion(t, m2, n, &hash)).await;

    // Then (4) confirmed COMPLETED.
    let confirmed =
        message_confirmed(&ctx, completed, MessageStatus::Completed, "the completion").await;
    assert_eq!(confirmed.message_id, m2);

    // And (2) M2 reads as a turn of three items, every one COMPLETED.
    let sealed = message_view(&ctx, &a.passport, t, m2).await;
    assert_eq!(sealed["status"], "COMPLETED");
    let items = sealed["items"].as_array().expect("items");
    assert_eq!(items.len(), 3, "{sealed}");
    assert!(
        items.iter().all(|item| item["status"] == "COMPLETED"),
        "{sealed}"
    );
    // Item 0: a step by m-1, awaiting its tool's result, its text and its call.
    assert_eq!(items[0]["kind"], "STEP");
    assert_eq!(items[0]["model"], "m-1");
    assert_eq!(
        items[0]["stopReason"],
        json!({ "type": "awaiting_tool_results" })
    );
    let call = serde_json::to_value(AssistantBlock::ToolCall(ToolCall {
        id: ToolCallId::new("c1").unwrap(),
        name: ToolName::new("search").unwrap(),
        arguments: json!({ "q": "rust" }),
    }))
    .unwrap();
    assert_eq!(
        items[0]["blocks"],
        json!([
            { "id": b0, "index": 0, "kind": "TEXT", "content": text_content("Hello") },
            { "id": b1, "index": 1, "kind": "TOOL_CALL", "content": call },
        ])
    );
    // Item 1: the tool results item.
    assert_eq!(items[1]["kind"], "TOOL_RESULTS");
    assert_eq!(common::content::tool_results_of(&sealed).len(), 1);
    // Item 2: the text, then the ARTIFACT block at the next index.
    assert_eq!(items[2]["kind"], "STEP");
    assert_eq!(items[2]["stopReason"], json!({ "type": "end_turn" }));
    assert_eq!(items[2]["blocks"][0]["content"], text_content("Done."));
    assert_eq!(items[2]["blocks"][1]["kind"], "ARTIFACT");
    assert_eq!(items[2]["blocks"][1]["index"], 1);
    assert_eq!(
        items[2]["blocks"][1]["content"],
        json!({ "artifact_id": x })
    );
    // And the artifact it names is attached: members read its bytes.
    let url = download_url(&ctx, &b.passport, t, x)
        .await
        .expect("a member reads the attached artifact");
    assert_eq!(fetch(&url).await, (200, file.bytes.clone()));
    // And (3) A's feed carries the completion.
    a_feed
        .until("A sees M2 COMPLETED", |delta| {
            upsert_of(delta, m2).is_some_and(|view| view["status"] == "COMPLETED")
        })
        .await;

    // And the facts recorded M2's opening, each item, and its end — never a
    // chunk.
    let m2_facts = facts::of(&ctx.pool, "message", m2).await;
    assert_eq!(
        facts::types(&m2_facts),
        vec![
            "Created",
            "ItemAdded",
            "ItemAdded",
            "ItemAdded",
            "Completed"
        ],
        "one ItemAdded per item, none per chunk"
    );
    facts::assert_no_fact_carries(&ctx.pool, "Hello", "a streamed text").await;

    // When a chunk is published after the seal.
    let late = event(2, text_delta(0, " Or not."));
    stream::push(&ctx.nats.url(), m2, n + 1, &[late]).await;

    // Then it is ignored on every pod: M2 reads exactly as sealed on both, and
    // nothing is pushed.
    a_feed.silence("a chunk after the seal").await;
    assert_eq!(message_view(&ctx, &a.passport, t, m2).await, sealed);
    let on_pod2 = pod2
        .run(
            &a.passport,
            Q_MESSAGES,
            json!({ "thread": t, "size": 1, "before": null, "tip": m2 }),
        )
        .await;
    assert_eq!(gql::data(&on_pod2, "threadsMessages")[0], sealed);

    // When M2 is completed again.
    let again = command(&ctx, &completion(t, m2, n, &hash)).await;
    // Then refused MESSAGE_NOT_STREAMING, and M2 never changes.
    message_refused(
        &ctx,
        again,
        ReasonCode::MessageNotStreaming,
        "completing twice",
    )
    .await;
    assert_eq!(message_view(&ctx, &a.passport, t, m2).await, sealed);
    assert_eq!(
        facts::of(&ctx.pool, "message", m2).await.len(),
        m2_facts.len()
    );

    ctx.teardown().await;
}
