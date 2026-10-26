//! **T09 — Deleting a message: author only, the node stays.**
//!
//! > M1, by user A, has two answers M2 (an AI message carrying an attached
//! > artifact X) and M2b, a fork. The harness sends message.delete for M1 with
//! > acting user B, also a member: refused NOT_THE_AUTHOR, nothing changes.
//! > With acting user A: confirmed; M1 reads as deleted with no content, still
//! > in the tree, M2 and M2b stay readable under it, and a member following T
//! > is told once. A Deleted fact records it; no fact, before or after, carries
//! > M1's text, and no block of M1 is left in the store. A runner's context no
//! > longer contains M1. Deleting M1 again is confirmed and changes nothing.
//! > Deleting an unknown message is refused MESSAGE_NOT_FOUND. Deleting M2, the
//! > AI's message, with acting user A is refused NOT_THE_AUTHOR: an AI message
//! > is never deleted, it is regenerated; M2 and its artifact X stay, X's read
//! > access still returns its bytes (an artifact goes with its thread — T17).
//!
//! Channels: the answers (4), the messages as members read them (2), B's live
//! feed (3), the fact log, a runner's context, the artifact's read access.

mod common;

use common::artifacts::{File, download_url, fetch, upload};
use common::content::{ai_text, artifact_block, step, user_text};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    ai, answer, branch, command, context, conversation_of, create_thread, message_confirmed,
    message_refused, message_view, store, user_message,
};
use common::{TestContext, flows::uuid_of};
use contract_threads::llm::{Entry, StopReason};
use contract_threads::{DeleteMessage, MessageStatus, ReasonCode};
use serde_json::json;
use uuid::Uuid;

fn delete(thread: Uuid, message: Uuid, acting: Uuid) -> DeleteMessage {
    DeleteMessage {
        message_id: message,
        thread_id: thread,
        acting_user_id: acting,
    }
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t09_only_its_author_deletes_a_message_whose_node_stays_and_an_ai_message_is_never_deleted()
{
    // Given T with members O, A and B; M1 by A; two answers M2 (carrying an
    // artifact X) and M2b; B following T; a runner.
    let ctx = TestContext::setup().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b]).await;
    const SECRET: &str = "my salary is in this message";
    let m1 = store(
        &ctx,
        user_message(t, None, a.id, vec![user_text(SECRET)]),
        MessageStatus::Completed,
    )
    .await;
    let runner = ctx.service_account(true);
    let file = File::csv("salary\n1\n");
    let x = upload(&ctx, &runner.passport, t, &file).await;
    let m2 = answer(
        &ctx,
        t,
        Some(m1),
        ai("a1", "Analyst"),
        vec![step(
            StopReason::EndTurn,
            None,
            vec![ai_text("noted"), artifact_block(x)],
        )],
    )
    .await;
    let m2b = answer(
        &ctx,
        t,
        Some(m1),
        ai("a1", "Analyst"),
        common::content::answer("noted, again"),
    )
    .await;
    let mut b_feed = Live::thread(&ctx, &b.passport, t).await;
    facts::assert_no_fact_carries(&ctx.pool, SECRET, "before the deletion").await;

    // When the harness deletes M1 with acting user B.
    let before = message_view(&ctx, &a.passport, t, m1).await;
    let facts_before = facts::count(&ctx.pool).await;
    let not_author = command(&ctx, &delete(t, m1, b.id)).await;

    // Then refused NOT_THE_AUTHOR, and nothing changes.
    message_refused(
        &ctx,
        not_author,
        ReasonCode::NotTheAuthor,
        "B deleting A's message",
    )
    .await;
    assert_eq!(message_view(&ctx, &a.passport, t, m1).await, before);
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    b_feed.silence("a refused deletion pushes nothing").await;

    // When the harness deletes M1 with acting user A.
    let deleted = command(&ctx, &delete(t, m1, a.id)).await;

    // Then (4) confirmed, the message deleted.
    let confirmed =
        message_confirmed(&ctx, deleted, MessageStatus::Completed, "A deleting M1").await;
    assert!(confirmed.deleted, "{confirmed:?}");
    // And (2) M1 reads as deleted, without content, still in the tree.
    let gone = message_view(&ctx, &o.passport, t, m1).await;
    assert_eq!(gone["deleted"], true, "{gone}");
    assert_eq!(
        (gone["blocks"].clone(), gone["items"].clone()),
        (json!([]), json!([]))
    );
    // And M2 and M2b stay readable under it.
    for answer in [m2, m2b] {
        let page: Vec<_> = branch(&ctx, &b.passport, t, 10, Some(answer))
            .await
            .iter()
            .map(|view| uuid_of(&view["id"]))
            .collect();
        assert_eq!(page, vec![answer, m1], "the fork stays readable under M1");
    }
    // And (3) B is told once.
    let pushed = b_feed
        .until("B is told M1 is deleted", |delta| {
            upsert_of(delta, m1).is_some_and(|view| view["deleted"] == true)
        })
        .await;
    assert_eq!(upsert_of(&pushed, m1).unwrap()["blocks"], json!([]));
    let rest = b_feed.drain().await;
    assert!(
        rest.iter().all(|delta| upsert_of(delta, m1).is_none()),
        "B is told once: {rest:?}"
    );
    // And a Deleted fact records it — no fact, before or after, carries the text.
    assert!(facts::types(&facts::of(&ctx.pool, "message", m1).await).contains(&"Deleted"));
    facts::assert_no_fact_carries(&ctx.pool, SECRET, "after the deletion").await;
    // And its content is gone from the store: no block of M1 is left.
    let blocks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM threads.message_block WHERE message_id = $1")
            .bind(m1)
            .fetch_one(&ctx.pool)
            .await
            .expect("count M1's blocks");
    assert_eq!(blocks, 0, "a deleted message keeps no content anywhere");
    // And a runner's context no longer contains M1.
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    assert!(
        conversation
            .entries()
            .iter()
            .all(|entry| !matches!(entry, Entry::UserInput(_))),
        "the deleted user message is left out: {conversation:?}"
    );
    assert!(!format!("{conversation:?}").contains(SECRET));

    // When M1 is deleted again.
    let facts_before = facts::count(&ctx.pool).await;
    let again = command(&ctx, &delete(t, m1, a.id)).await;
    // Then confirmed, and nothing changes.
    let confirmed =
        message_confirmed(&ctx, again, MessageStatus::Completed, "deleting again").await;
    assert!(confirmed.deleted);
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    assert_eq!(message_view(&ctx, &o.passport, t, m1).await, gone);

    // When an unknown message is deleted.
    let unknown = command(&ctx, &delete(t, Uuid::now_v7(), a.id)).await;
    // Then refused MESSAGE_NOT_FOUND.
    message_refused(
        &ctx,
        unknown,
        ReasonCode::MessageNotFound,
        "an unknown message",
    )
    .await;

    // When the harness deletes M2, the AI's message, with acting user A.
    let ai_delete = command(&ctx, &delete(t, m2, a.id)).await;
    // Then refused NOT_THE_AUTHOR: an AI message is never deleted — it and its
    // artifact stay.
    message_refused(
        &ctx,
        ai_delete,
        ReasonCode::NotTheAuthor,
        "deleting an AI message",
    )
    .await;
    assert_eq!(
        message_view(&ctx, &a.passport, t, m2).await["deleted"],
        false
    );
    let url = download_url(&ctx, &b.passport, t, x)
        .await
        .expect("the artifact stays with its message");
    assert_eq!(fetch(&url).await, (200, file.bytes));

    ctx.teardown().await;
}
