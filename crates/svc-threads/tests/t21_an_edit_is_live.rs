//! **T21 — An edit is live.**
//!
//! > Members A and B follow thread T live; A is the author of M1, a COMPLETED
//! > user message. The harness edits M1: A and B each receive one update of M1
//! > carrying the new blocks, edited_at and the raised version, and no reading
//! > of the thread is needed to show it. A user who is not a member of T
//! > receives nothing.
//!
//! A follows over WebSocket (the direct leg), B over SSE (the gateway's leg).
//!
//! "A user who is not a member receives nothing" is proven two ways:
//!
//! - U, never a member, asking to follow T is answered exactly as for a thread
//!   nobody created (as T04 pins), before and after the edit — the gate.
//! - B, a member following T, is removed from T while the feed stays open;
//!   A's next edit then reaches B's feed in no frame. This pins the message
//!   feed's own cohort end to end: past the gate, an open feed stops carrying
//!   a message once its reader leaves the thread's members.
//!
//! Channels: the live feeds (3); the answer (4).

mod common;

use common::TestContext;
use common::content::{texts_of, user_text};
use common::feed::{Live, Revoked, thread_feed, upsert_of, ws_opening};
use common::flows::{
    command, create_thread, edit_of, message_confirmed, message_version, say, thread_confirmed,
};
use contract_threads::{MessageStatus, RemoveMembers};
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
    // The reference is an answer, not a timeout or a closed socket that every
    // thread would share.
    assert!(as_unknown.starts_with("data:"), "{as_unknown}");
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
    assert_eq!(
        ws_opening(&ctx, &u.passport, &thread_feed(t, 20), &[t]).await,
        as_unknown,
        "after the edit too: U following T is answered as for an unknown thread"
    );

    // When B, still following T, is removed from its members.
    let removed = command(
        &ctx,
        &RemoveMembers {
            thread_id: t,
            user_ids: vec![b.id],
            acting_user_id: Some(a.id),
        },
    )
    .await;
    thread_confirmed(&ctx, removed, "removing B").await;
    let revoked = b_feed.until_revoked("B loses T", t).await;
    // The feed stays open: B is told T left the window, not cut off — so the
    // silence below is the cohort's, not a closed stream's.
    assert_eq!(revoked, Revoked::Removed);
    // And A edits M1 again.
    const NEWER: &str = "a third wording, for members only";
    let again = command(&ctx, &edit_of(t, m1, a.id, v + 1, vec![user_text(NEWER)])).await;
    message_confirmed(&ctx, again, MessageStatus::Completed, "editing M1 again").await;
    // Then A is told, and B's open feed carries the edit in no frame.
    a_feed
        .until("A is told of the second edit", |delta| {
            upsert_of(delta, m1).is_some_and(|view| texts_of(view) == [NEWER])
        })
        .await;
    let rest = b_feed.rest().await;
    assert!(
        rest.iter()
            .all(|delta| upsert_of(delta, m1).is_none() && !delta.to_string().contains(NEWER)),
        "a removed member's feed carries nothing of the edit: {rest:?}"
    );

    ctx.teardown().await;
}
