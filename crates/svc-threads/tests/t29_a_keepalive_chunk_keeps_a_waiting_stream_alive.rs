mod common;

use common::content::{Streamed, closure_kind, entries_of};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    ai, await_message, command, completion, create_thread, message_confirmed, message_refused,
    message_view, open_stream, resolution_of, say, thread_confirmed,
};
use common::{SHORT_STREAM_IDLE, TestContext, Windows, stream};
use contract_threads::llm::ResponseEnding;
use contract_threads::{Chunk, MessageStatus, ReasonCode, seal_hash};

async fn short_window() -> TestContext {
    TestContext::setup_with(Windows {
        stream_idle: Some(SHORT_STREAM_IDLE),
        ..Windows::default()
    })
    .await
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t29_keepalives_alone_keep_a_message_streaming_past_the_window_and_enter_the_seal() {
    let ctx = short_window().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Wait for the other runner").await;
    let nats = ctx.nats.url();

    let m2 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let opened = message_view(&ctx, &a.passport, t, m2).await;
    let facts_opened = facts::of(&ctx.pool, "message", m2).await.len();
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;

    let mut sent: Vec<Chunk> = Vec::new();
    let gap = SHORT_STREAM_IDLE * 3 / 5;
    for seq in 0..4u64 {
        let keepalive = vec![Chunk::keepalive()];
        stream::push(&nats, m2, seq, &keepalive).await;
        sent.extend(keepalive);
        tokio::time::sleep(gap).await;
    }
    assert_eq!(
        message_view(&ctx, &a.passport, t, m2).await,
        opened,
        "keepalives change nothing a member reads"
    );
    assert_eq!(
        facts::of(&ctx.pool, "message", m2).await.len(),
        facts_opened,
        "keepalives add no fact"
    );
    a_feed
        .silence("keepalives change no view a live member holds")
        .await;

    let mut response = Streamed::start(m2, None);
    response.text(&["the other runner answered"], true);
    response.complete(ResponseEnding::finished());
    let content = response.chunks();
    stream::push(&nats, m2, sent.len() as u64, &content).await;
    sent.extend(content);
    let last_seq = sent.len() as u64 - 1;

    let without_keepalives: Vec<Chunk> = sent
        .iter()
        .filter(|chunk| !matches!(chunk, Chunk::Keepalive))
        .cloned()
        .collect();
    let unsealed = command(
        &ctx,
        &completion(
            t,
            m2,
            last_seq,
            &seal_hash(&without_keepalives).expect("the chunks hash"),
        ),
    )
    .await;
    message_refused(
        &ctx,
        unsealed,
        ReasonCode::StreamMismatch,
        "a digest that leaves the keepalives out",
    )
    .await;
    assert_eq!(
        message_view(&ctx, &a.passport, t, m2).await["status"],
        "STREAMING"
    );

    let completed = command(
        &ctx,
        &completion(t, m2, last_seq, &seal_hash(&sent).expect("the chunks hash")),
    )
    .await;
    message_confirmed(
        &ctx,
        completed,
        MessageStatus::Completed,
        "the sealed stream",
    )
    .await;
    a_feed
        .until("the completion reaches the feed", |delta| {
            upsert_of(delta, m2).is_some_and(|view| view["status"] == "COMPLETED")
        })
        .await;
    let view = message_view(&ctx, &a.passport, t, m2).await;
    assert_eq!(
        entries_of(&view)
            .iter()
            .map(|entry| entry["kind"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>(),
        vec!["model_response", "turn_closure"],
        "{view}"
    );

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t29_a_keepalive_comes_inside_a_producing_response_and_on_a_resolved_thread() {
    let ctx = short_window().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Go on").await;
    let nats = ctx.nats.url();

    let m2 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let mut response = Streamed::start(m2, None);
    response.text(&["half ", "said"], true);
    response.complete(ResponseEnding::finished());
    let content = response.chunks();
    let (head, rest) = content.split_at(3);
    let mut sent = head.to_vec();
    sent.push(Chunk::keepalive());
    stream::push(&nats, m2, 0, &sent).await;

    let resolved = command(&ctx, &resolution_of(t, a.id)).await;
    thread_confirmed(&ctx, resolved, "resolving T").await;

    let gap = SHORT_STREAM_IDLE * 3 / 5;
    for _ in 0..3 {
        tokio::time::sleep(gap).await;
        stream::push(&nats, m2, sent.len() as u64, &[Chunk::keepalive()]).await;
        sent.push(Chunk::keepalive());
    }
    assert_eq!(
        message_view(&ctx, &a.passport, t, m2).await["status"],
        "STREAMING",
        "keepalives count on a resolved thread"
    );

    stream::push(&nats, m2, sent.len() as u64, rest).await;
    sent.extend(rest.iter().cloned());
    let completed = command(
        &ctx,
        &completion(
            t,
            m2,
            sent.len() as u64 - 1,
            &seal_hash(&sent).expect("the chunks hash"),
        ),
    )
    .await;
    message_confirmed(
        &ctx,
        completed,
        MessageStatus::Completed,
        "completing on a resolved thread",
    )
    .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t29_silence_still_fails_the_message_interrupted_and_a_later_keepalive_changes_nothing() {
    let ctx = short_window().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Go on").await;
    let nats = ctx.nats.url();

    let m2 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let mut response = Streamed::start(m2, None);
    response.text(&["half"], false);
    let mut sent = response.chunks();
    sent.push(Chunk::keepalive());
    let (last_seq, hash) = stream::push(&nats, m2, 0, &sent).await;

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
    let failed = facts::of(&ctx.pool, "message", m2).await;
    assert_eq!(
        facts::the(&failed, "Failed").payload["reason"],
        "stream_idle"
    );
    assert_eq!(
        facts::the(&failed, "Failed").payload["closure"],
        "interrupted"
    );

    stream::push(&nats, m2, last_seq + 1, &[Chunk::keepalive()]).await;
    tokio::time::sleep(common::QUIET_WINDOW).await;
    assert_eq!(message_view(&ctx, &a.passport, t, m2).await, view);
    assert_eq!(
        facts::of(&ctx.pool, "message", m2).await.len(),
        failed.len()
    );

    let late = command(&ctx, &completion(t, m2, last_seq, &hash)).await;
    message_refused(
        &ctx,
        late,
        ReasonCode::MessageNotStreaming,
        "completing after the silence",
    )
    .await;

    ctx.teardown().await;
}
