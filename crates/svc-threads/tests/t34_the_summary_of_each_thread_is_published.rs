//! **T34 — The summary of each thread is published: title, origin, status,
//! resolution, creation and last activity, under its own key.**
//!
//! > The harness creates thread T owned by O with member A: confirmed, and
//! > threads.thread_summary.{T} holds T untitled, no parent, OPEN, its last
//! > activity its creation; the manifest threads.thread_summary_manifest
//! > declares the prefix and version 1. The harness sets T's title: the entry
//! > carries it, the activity unmoved. A's complete message M1 moves the
//! > activity to M1's storage, the date T's members read. An AI message M2
//! > opened STREAMING moves it once, at its opening; the chunks of its stream
//! > and its completion rewrite nothing — the entry's revision holds. Resolving
//! > T publishes its resolution, T still OPEN. A child C of T, started from a
//! > block of T, carries T as its parent and that block as its origin. Deleting
//! > T retracts its summary (and its members entry); C's summary stays,
//! > its parent now leading nowhere.
//!
//! The summary is a new offer beside `threads.members.{thread_id}`, which keeps
//! its key and payload (T02 and every members scenario hold it).
//!
//! Channels: the answers (4), the summary entry, its revision and its manifest
//! (4), the thread as its members read it (2).

mod common;

use br_util_nats_fabric::Revision;
use chrono::{DateTime, Utc};
use common::content::Streamed;
use common::flows::{
    ai, command, completion, create, create_thread, creation, message_confirmed, open_stream,
    resolution_of, say, thread_confirmed, thread_view,
};
use common::{PUSH_TIMEOUT, QUIET_WINDOW, TestContext, eventually, stream};
use contract_threads::llm::ResponseEnding;
use contract_threads::{
    DeleteThread, MessageStatus, PublishedThreadSummary, SetTitle, ThreadStatus,
};
use serde_json::{Value, json};
use uuid::Uuid;

/// Waits until T's summary entry satisfies `wanted`, and answers it with its
/// revision.
async fn summary_until(
    ctx: &TestContext,
    thread: Uuid,
    what: &str,
    wanted: impl Fn(&PublishedThreadSummary) -> bool,
) -> (PublishedThreadSummary, Revision) {
    eventually(what, PUSH_TIMEOUT, || async {
        ctx.summary_entry(thread)
            .await
            .is_some_and(|(entry, _)| wanted(&entry))
    })
    .await;
    ctx.summary_entry(thread).await.expect("waited above")
}

/// The entry once the offer's relay had the quiet window to write anything
/// still owed.
async fn settled(ctx: &TestContext, thread: Uuid) -> (PublishedThreadSummary, Revision) {
    tokio::time::sleep(QUIET_WINDOW).await;
    ctx.summary_entry(thread)
        .await
        .unwrap_or_else(|| panic!("threads.thread_summary.{thread} is published"))
}

fn at(view: &Value, field: &str) -> DateTime<Utc> {
    view[field]
        .as_str()
        .unwrap_or_else(|| panic!("{field} is a date: {view}"))
        .parse()
        .unwrap_or_else(|e| panic!("{field} parses ({e}): {view}"))
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t34_each_thread_publishes_its_summary_which_follows_title_activity_and_deletion() {
    let ctx = TestContext::setup().await;
    let (o, a) = (ctx.enrol("olga").await, ctx.enrol("ada").await);

    // ── creation ─────────────────────────────────────────────────────────

    // When the harness creates T, owned by O, with A.
    let t = create_thread(&ctx, Some(&o), &[&a]).await;

    // Then (4) its summary is published: untitled, no parent, OPEN, its last
    // activity its creation — the dates its members read.
    let (born, _) = summary_until(&ctx, t, "T's summary is published", |_| true).await;
    let view = thread_view(&ctx, &a.passport, t).await;
    assert_eq!(born.thread_id, t);
    assert_eq!(born.title, None);
    assert_eq!(born.parent_thread_id, None);
    assert_eq!(born.origin_block_id, None);
    assert_eq!(born.status, ThreadStatus::Open);
    assert_eq!(born.resolved_at, None);
    assert_eq!(born.created_at, at(&view, "createdAt"));
    assert_eq!(born.last_activity_at, born.created_at);
    // And the manifest declares the family and its wire version.
    assert_eq!(
        ctx.summary_manifest().await,
        Some(json!({ "prefix": "threads.thread_summary.", "version": 1 }))
    );

    // ── the title ────────────────────────────────────────────────────────

    // When the harness sets T's title.
    let titled = command(
        &ctx,
        &SetTitle {
            thread_id: t,
            title: "Q3 review".to_string(),
            acting_user_id: Some(o.id),
        },
    )
    .await;

    // Then (4) the entry carries it; the activity has not moved.
    thread_confirmed(&ctx, titled, "setting T's title").await;
    let (entry, _) = summary_until(&ctx, t, "T's title is published", |entry| {
        entry.title.as_deref() == Some("Q3 review")
    })
    .await;
    assert_eq!(entry.last_activity_at, born.last_activity_at);
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["title"],
        "Q3 review"
    );

    // ── a complete message ───────────────────────────────────────────────

    // When A's complete message M1 is stored.
    let m1 = say(&ctx, t, None, &a, "Please analyse Q3").await;

    // Then (4) the activity moves to M1's storage, as T's members read it.
    let (entry, _) = summary_until(&ctx, t, "M1 moves T's activity", |entry| {
        entry.last_activity_at > born.last_activity_at
    })
    .await;
    let view = thread_view(&ctx, &a.passport, t).await;
    assert_eq!(entry.last_activity_at, at(&view, "lastActivityAt"));
    let after_m1 = entry.last_activity_at;

    // ── a streamed message ───────────────────────────────────────────────

    // When the agents service opens M2 STREAMING.
    let m2 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;

    // Then (4) the activity moves once, at the opening.
    let (opened, _) = summary_until(&ctx, t, "M2's opening moves T's activity", |entry| {
        entry.last_activity_at > after_m1
    })
    .await;
    assert_eq!(
        opened.last_activity_at,
        at(&thread_view(&ctx, &a.passport, t).await, "lastActivityAt")
    );
    let (_, revision_opened) = settled(&ctx, t).await;

    // When the runner streams M2's answer, chunk by chunk.
    let mut response = Streamed::start(m2, None);
    response.text(&["Revenue ", "grew ", "by ", "four ", "percent."], true);
    response.complete(ResponseEnding::finished());
    let chunks = response.chunks();
    let nats = ctx.nats.url();
    for (seq, chunk) in chunks.iter().enumerate() {
        stream::push(&nats, m2, seq as u64, std::slice::from_ref(chunk)).await;
    }

    // Then (4) no chunk rewrites the entry: its revision holds.
    let (streaming, revision_streamed) = settled(&ctx, t).await;
    assert_eq!(
        revision_streamed, revision_opened,
        "a chunk rewrote the summary"
    );
    assert_eq!(streaming, opened);

    // When the stream is completed.
    let (last_seq, hash) = (
        chunks.len() as u64 - 1,
        contract_threads::seal_hash(&chunks).expect("the chunks hash"),
    );
    let completed = command(&ctx, &completion(t, m2, last_seq, &hash)).await;

    // Then (4) M2 is COMPLETED and the entry is still the one its opening
    // wrote: the completion rewrites nothing.
    message_confirmed(&ctx, completed, MessageStatus::Completed, "M2 sealed").await;
    let (sealed, revision_sealed) = settled(&ctx, t).await;
    assert_eq!(
        revision_sealed, revision_opened,
        "the completion rewrote the summary"
    );
    assert_eq!(sealed, opened);
    assert_eq!(
        sealed.last_activity_at,
        at(&thread_view(&ctx, &a.passport, t).await, "lastActivityAt"),
        "the summary's activity is the one T's members read"
    );

    // ── the resolution ───────────────────────────────────────────────────

    // When O resolves T.
    let resolved = command(&ctx, &resolution_of(t, o.id)).await;

    // Then (4) the entry carries its resolution; T is still OPEN.
    thread_confirmed(&ctx, resolved, "resolving T").await;
    let (entry, _) = summary_until(&ctx, t, "T's resolution is published", |entry| {
        entry.resolved_at.is_some()
    })
    .await;
    assert_eq!(entry.status, ThreadStatus::Open);
    assert_eq!(
        entry.resolved_at,
        Some(at(&thread_view(&ctx, &o.passport, t).await, "resolvedAt"))
    );

    // ── a child thread ───────────────────────────────────────────────────

    // When A starts a child C of T from one of its blocks.
    let block = Uuid::now_v7();
    let mut child = creation(Some(a.id), &[]);
    child.parent_thread_id = Some(t);
    child.origin_block_id = Some(block);
    let c = create(&ctx, child).await;

    // Then (4) C's summary names T as its parent and the block as its origin.
    let (child_entry, _) = summary_until(&ctx, c, "C's summary is published", |_| true).await;
    assert_eq!(child_entry.parent_thread_id, Some(t));
    assert_eq!(child_entry.origin_block_id, Some(block));
    assert_eq!(child_entry.title, None);
    let child_view = thread_view(&ctx, &a.passport, c).await;
    assert_eq!(child_view["parentThreadId"], json!(t));
    assert_eq!(child_view["originBlockId"], json!(block));

    // ── deletion ─────────────────────────────────────────────────────────

    // When O deletes T.
    let deleted = command(
        &ctx,
        &DeleteThread {
            thread_id: t,
            acting_user_id: Some(o.id),
        },
    )
    .await;

    // Then (4) T's summary is retracted, with its members entry.
    thread_confirmed(&ctx, deleted, "deleting T").await;
    eventually("T's summary is retracted", PUSH_TIMEOUT, || async {
        ctx.summary_entry(t).await.is_none()
    })
    .await;
    eventually("T's members entry is retracted", PUSH_TIMEOUT, || async {
        ctx.published_members(t).await.is_none()
    })
    .await;
    // And (4) C's summary stays, its parent now leading nowhere.
    let (kept, _) = settled(&ctx, c).await;
    assert_eq!(kept, child_entry);

    ctx.teardown().await;
}
