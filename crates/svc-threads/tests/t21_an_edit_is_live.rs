//! **T21 — An edit is live.**
//!
//! > Members A and B follow thread T live; A is the author of M1, a COMPLETED
//! > user message. The harness edits M1: A and B each receive one update of M1
//! > carrying the new blocks, edited_at and the raised version, and no reading
//! > of the thread is needed to show it. A user who is not a member of T
//! > receives nothing.
//!
//! A follows over WebSocket (the direct leg), B over SSE (the gateway's leg).
//! U, outside T, tries to follow T itself, before and after the edit: each
//! time the subscription opens exactly as one to a thread nobody created (as
//! T04 pins), so no frame of T — the edit included — ever reaches U.
//!
//! Channels: the live feeds (3); the answer (4).

mod common;

use common::TestContext;
use common::content::{texts_of, user_text};
use common::feed::{Live, thread_feed, upsert_of, ws_opening};
use common::flows::{command, create_thread, edit_of, message_confirmed, message_version, say};
use contract_threads::MessageStatus;
use serde_json::Value;
use uuid::Uuid;

/// The one update of `message` among `deltas`.
fn one_update(deltas: &[Value], message: Uuid, who: &str) -> Value {
    let updates: Vec<&Value> = deltas
        .iter()
        .filter_map(|delta| upsert_of(delta, message))
        .collect();
    assert_eq!(
        updates.len(),
        1,
        "{who} receives one update of M1: {deltas:?}"
    );
    updates[0].clone()
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t21_an_edit_reaches_the_followers_as_the_updated_message() {
    // Given T with members A and B, M1 by A; A and B follow T live; U, not a
    // member, tries to follow T.
    let ctx = TestContext::setup().await;
    let (a, b, u) = (
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
        ctx.enrol("ursula").await,
    );
    let t = create_thread(&ctx, Some(&a), &[&b]).await;
    let m1 = say(&ctx, t, None, &a, "a first wording").await;
    let v = message_version(&ctx, &a.passport, t, m1).await;
    let mut a_feed = Live::thread_ws(&ctx, &a.passport, t).await;
    let mut b_feed = Live::thread(&ctx, &b.passport, t).await;
    let nobodys = Uuid::now_v7();
    let as_unknown = ws_opening(&ctx, &u.passport, &thread_feed(nobodys, 20), &[nobodys]).await;
    assert_eq!(
        ws_opening(&ctx, &u.passport, &thread_feed(t, 20), &[t]).await,
        as_unknown,
        "U following T is refused as for an unknown thread"
    );
    // The title Job's start and anything else the store set off settle first.
    a_feed.drain().await;
    b_feed.drain().await;

    // When the harness edits M1.
    const NEW: &str = "a second wording";
    let edited = command(&ctx, &edit_of(t, m1, a.id, v, vec![user_text(NEW)])).await;
    message_confirmed(&ctx, edited, MessageStatus::Completed, "editing M1").await;

    // Then A and B each receive one update of M1, carrying its new blocks, its
    // edited_at and its raised version — no read of the thread involved.
    for (feed, who) in [(&mut a_feed, "A"), (&mut b_feed, "B")] {
        let first = feed
            .until(&format!("{who} is told of the edit"), |delta| {
                upsert_of(delta, m1).is_some()
            })
            .await;
        let mut deltas = vec![first];
        deltas.extend(feed.drain().await);
        let update = one_update(&deltas, m1, who);
        assert_eq!(texts_of(&update), vec![NEW.to_string()], "{who}: {update}");
        assert!(update["editedAt"].is_string(), "{who}: {update}");
        assert_eq!(update["version"].as_u64(), Some(v + 1), "{who}: {update}");
        assert_eq!(update["status"], "COMPLETED");
    }
    // And U, who is not a member of T, receives nothing.
    let after = ws_opening(&ctx, &u.passport, &thread_feed(t, 20), &[t]).await;
    assert_eq!(
        after, as_unknown,
        "after the edit too: nothing of T reaches U"
    );
    assert!(!after.contains(NEW), "{after}");

    ctx.teardown().await;
}
