//! **T22 — Resolving and reopening a thread.**
//!
//! > Thread T is OPEN, with messages M1 (a user message of A) and a STREAMING AI
//! > message M2. The harness sends thread.resolve for T with acting user B:
//! > confirmed; T reads as resolved by B at the time of the command, still OPEN,
//! > and a member following T receives T updated with its resolution. A
//! > Resolved fact records it. Storing a message, editing M1 and deleting M1
//! > are each refused THREAD_RESOLVED and change nothing; M2 can still be
//! > completed, failed or cancelled; the title, the members, the resources and
//! > A's read mark can still change. Resolving T again is refused
//! > THREAD_ALREADY_RESOLVED, and nothing changes. thread.reopen with acting
//! > user C: confirmed; T reads as not resolved, a Reopened fact records it,
//! > messages are accepted again, edit and delete are allowed again for A.
//! > Reopening T again is refused THREAD_NOT_RESOLVED. Resolve and reopen on an
//! > unknown thread are refused THREAD_NOT_FOUND. T resolved then closed
//! > COMPLETED keeps its resolution as it was; reopening it is refused
//! > THREAD_NOT_OPEN; resolving a thread closed before is refused
//! > THREAD_NOT_OPEN. Deleting a resolved thread is confirmed.
//!
//! A message ends once, so "M2 can still be completed, failed or cancelled"
//! is played on three streaming messages opened before the resolution: M2 is
//! completed, M2f failed and M2c cancelled while T is resolved.
//!
//! Channels: the answers (4), the thread and its messages as members read
//! them (2), B's live feed (3), the fact log.

mod common;

use common::content::{Streamed, text};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    ai, command, completion, create, create_thread, creation, deletion_of, edit_of,
    message_confirmed, message_refused, message_version, message_view, open_stream, reopening_of,
    resolution_of, say, thread_confirmed, thread_refused, thread_view, user_message, version,
};
use common::{M_SET_READ_MARK, TestContext, gql, stream};
use contract_threads::llm::ResponseEnding;
use contract_threads::{
    AddMembers, AddResourceIds, CancelMessage, CloseThread, ClosingStatus, DeleteThread,
    FailMessage, MessageStatus, ReasonCode, SetTitle,
};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t22_a_resolved_thread_accepts_no_message_until_it_is_reopened_and_is_not_closed() {
    // Given T, OPEN, owned by O with members A, B and C; M1 by A; three AI
    // messages STREAMING (M2, M2f, M2c), M2's step pushed; B follows T.
    let ctx = TestContext::setup().await;
    let (o, a, b, c, d) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
        ctx.enrol("chloe").await,
        ctx.enrol("dora").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b, &c]).await;
    let m1 = say(&ctx, t, None, &a, "is this settled?").await;
    let m2 = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let m2f = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let m2c = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let mut settled = Streamed::start(m2, None);
    settled.text(&["it is ", "settled"], true);
    settled.complete(ResponseEnding::finished());
    let (last_seq, hash) = stream::push(&ctx.nats.url(), m2, 0, &settled.chunks()).await;
    let mut b_feed = Live::thread(&ctx, &b.passport, t).await;
    b_feed.drain().await;

    // When the harness resolves T with acting user B.
    let before_command = chrono::Utc::now();
    let resolved = command(&ctx, &resolution_of(t, b.id)).await;
    thread_confirmed(&ctx, resolved, "resolving T").await;
    let after_answer = chrono::Utc::now();

    // Then T reads as resolved by B at the time of the command, still OPEN.
    let view = thread_view(&ctx, &a.passport, t).await;
    assert_eq!(view["status"], "OPEN", "resolving is not closing: {view}");
    assert_eq!(view["resolvedBy"], json!(b.id), "{view}");
    let at: chrono::DateTime<chrono::Utc> = view["resolvedAt"]
        .as_str()
        .and_then(|at| at.parse().ok())
        .unwrap_or_else(|| panic!("a resolution date: {view}"));
    assert!(
        before_command <= at && at <= after_answer,
        "{at} is the command's time"
    );
    // And B, following T, receives T updated with its resolution.
    b_feed
        .until("B is told T is resolved", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["resolvedBy"] == json!(b.id))
        })
        .await;
    // And a Resolved fact records it.
    let thread_facts = facts::of(&ctx.pool, "thread", t).await;
    let fact = facts::the(&thread_facts, "Resolved");
    assert_eq!(fact.acting_user_id, Some(b.id));
    assert_eq!(fact.payload["resolved_by"], json!(b.id));

    // When a message is stored, M1 edited and M1 deleted — then each is refused
    // THREAD_RESOLVED and nothing changes.
    let m1_before = message_view(&ctx, &a.passport, t, m1).await;
    let facts_before = facts::count(&ctx.pool).await;
    let stored = command(&ctx, &user_message(t, Some(m1), a.id, vec![text("more")])).await;
    message_refused(
        &ctx,
        stored,
        ReasonCode::ThreadResolved,
        "storing on a resolved T",
    )
    .await;
    let v1 = message_version(&ctx, &a.passport, t, m1).await;
    let edited = command(&ctx, &edit_of(t, m1, a.id, v1, vec![text("rephrased")])).await;
    message_refused(
        &ctx,
        edited,
        ReasonCode::ThreadResolved,
        "editing on a resolved T",
    )
    .await;
    let deleted = command(&ctx, &deletion_of(t, m1, a.id)).await;
    message_refused(
        &ctx,
        deleted,
        ReasonCode::ThreadResolved,
        "deleting on a resolved T",
    )
    .await;
    // And the resolution is judged before the message: an unknown one too.
    let unknown = command(&ctx, &edit_of(t, Uuid::now_v7(), a.id, 1, vec![text("x")])).await;
    message_refused(
        &ctx,
        unknown,
        ReasonCode::ThreadResolved,
        "an unknown message on a resolved T",
    )
    .await;
    assert_eq!(message_view(&ctx, &a.passport, t, m1).await, m1_before);
    assert_eq!(facts::count(&ctx.pool).await, facts_before);

    // When the streaming messages end — then M2 completes, M2f fails and M2c
    // is cancelled.
    let completed = command(&ctx, &completion(t, m2, last_seq, &hash)).await;
    message_confirmed(&ctx, completed, MessageStatus::Completed, "completing M2").await;
    let failed = command(
        &ctx,
        &FailMessage {
            message_id: m2f,
            thread_id: t,
            seal: None,
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(&ctx, failed, MessageStatus::Failed, "failing M2f").await;
    let cancelled = command(
        &ctx,
        &CancelMessage {
            message_id: m2c,
            thread_id: t,
            seal: None,
            acting_user_id: Some(a.id),
        },
    )
    .await;
    message_confirmed(&ctx, cancelled, MessageStatus::Cancelled, "cancelling M2c").await;

    // When the title, the members, the resources and A's read mark change —
    // then each is accepted.
    let titled = command(
        &ctx,
        &SetTitle {
            thread_id: t,
            title: "Settled".to_string(),
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, titled, "titling a resolved T").await;
    let added = command(
        &ctx,
        &AddMembers {
            thread_id: t,
            user_ids: vec![d.id],
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, added, "adding D to a resolved T").await;
    let resourced = command(
        &ctx,
        &AddResourceIds {
            thread_id: t,
            key: "kms".to_string(),
            ids: vec!["k1".to_string()],
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, resourced, "adding a resource to a resolved T").await;
    let marked = ctx
        .run(
            &a.passport,
            M_SET_READ_MARK,
            json!({ "thread": t, "message": m2 }),
        )
        .await;
    assert_eq!(
        gql::data(&marked, "threadsSetReadMark")["success"],
        true,
        "{marked}"
    );
    let view = thread_view(&ctx, &a.passport, t).await;
    assert_eq!(view["title"], "Settled");
    assert_eq!(view["readMark"], json!(m2));
    assert_eq!(
        view["resolvedBy"],
        json!(b.id),
        "the resolution holds: {view}"
    );

    // When T is resolved again — then refused THREAD_ALREADY_RESOLVED, and
    // nothing changes.
    let version_before = version(&ctx, &a.passport, t).await;
    let again = command(&ctx, &resolution_of(t, c.id)).await;
    thread_refused(
        &ctx,
        again,
        ReasonCode::ThreadAlreadyResolved,
        "resolving T again",
    )
    .await;
    let view = thread_view(&ctx, &a.passport, t).await;
    assert_eq!(view["version"].as_i64(), Some(version_before));
    assert_eq!(view["resolvedBy"], json!(b.id));

    // When the harness reopens T with acting user C.
    let reopened = command(&ctx, &reopening_of(t, c.id)).await;
    thread_confirmed(&ctx, reopened, "reopening T").await;
    // Then T reads as not resolved, and a Reopened fact records it.
    let view = thread_view(&ctx, &a.passport, t).await;
    assert!(
        view["resolvedBy"].is_null() && view["resolvedAt"].is_null(),
        "{view}"
    );
    assert_eq!(view["status"], "OPEN");
    let fact_reopened = facts::of(&ctx.pool, "thread", t).await;
    assert_eq!(
        facts::the(&fact_reopened, "Reopened").acting_user_id,
        Some(c.id)
    );
    b_feed
        .until("B is told T is reopened", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["resolvedBy"].is_null())
        })
        .await;
    // And messages are accepted again; A edits and deletes again.
    let m3 = say(&ctx, t, Some(m2), &a, "thanks").await;
    let edited = command(&ctx, &edit_of(t, m1, a.id, v1, vec![text("rephrased")])).await;
    message_confirmed(
        &ctx,
        edited,
        MessageStatus::Completed,
        "editing M1 after reopening",
    )
    .await;
    let deleted = command(&ctx, &deletion_of(t, m3, a.id)).await;
    let confirmed = message_confirmed(
        &ctx,
        deleted,
        MessageStatus::Completed,
        "deleting after reopening",
    )
    .await;
    assert!(confirmed.deleted);

    // When T is reopened again — then refused THREAD_NOT_RESOLVED.
    let again = command(&ctx, &reopening_of(t, c.id)).await;
    thread_refused(
        &ctx,
        again,
        ReasonCode::ThreadNotResolved,
        "reopening T again",
    )
    .await;

    // When resolve and reopen name an unknown thread — then THREAD_NOT_FOUND.
    let unknown = Uuid::now_v7();
    let sent = command(&ctx, &resolution_of(unknown, b.id)).await;
    thread_refused(
        &ctx,
        sent,
        ReasonCode::ThreadNotFound,
        "resolving an unknown thread",
    )
    .await;
    let sent = command(&ctx, &reopening_of(unknown, b.id)).await;
    thread_refused(
        &ctx,
        sent,
        ReasonCode::ThreadNotFound,
        "reopening an unknown thread",
    )
    .await;

    // When T is resolved, then closed COMPLETED.
    let resolved = command(&ctx, &resolution_of(t, b.id)).await;
    thread_confirmed(&ctx, resolved, "resolving T once more").await;
    let resolution = thread_view(&ctx, &a.passport, t).await;
    let closed = command(
        &ctx,
        &CloseThread {
            thread_id: t,
            status: ClosingStatus::Completed,
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, closed, "closing a resolved T").await;
    // Then it keeps its resolution as it was.
    let view = thread_view(&ctx, &a.passport, t).await;
    assert_eq!(view["status"], "COMPLETED");
    assert_eq!(
        (&view["resolvedBy"], &view["resolvedAt"]),
        (&resolution["resolvedBy"], &resolution["resolvedAt"])
    );
    // And reopening it is refused THREAD_NOT_OPEN.
    let sent = command(&ctx, &reopening_of(t, c.id)).await;
    thread_refused(
        &ctx,
        sent,
        ReasonCode::ThreadNotOpen,
        "reopening a closed T",
    )
    .await;

    // When a thread closed before is resolved — then THREAD_NOT_OPEN.
    let u = create_thread(&ctx, Some(&o), &[]).await;
    let closed = command(
        &ctx,
        &CloseThread {
            thread_id: u,
            status: ClosingStatus::Cancelled,
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, closed, "closing U").await;
    let sent = command(&ctx, &resolution_of(u, o.id)).await;
    thread_refused(
        &ctx,
        sent,
        ReasonCode::ThreadNotOpen,
        "resolving a closed thread",
    )
    .await;

    // When a resolved, OPEN thread is deleted — then confirmed.
    let w = create(&ctx, creation(Some(o.id), &[a.id])).await;
    let resolved = command(&ctx, &resolution_of(w, o.id)).await;
    thread_confirmed(&ctx, resolved, "resolving W").await;
    let deleted = command(
        &ctx,
        &DeleteThread {
            thread_id: w,
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, deleted, "deleting a resolved thread").await;
    assert!(thread_view(&ctx, &o.passport, w).await.is_null());

    ctx.teardown().await;
}
