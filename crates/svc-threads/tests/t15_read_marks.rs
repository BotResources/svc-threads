//! **T15 — Read marks.**
//!
//! > Member A sets their read mark on thread T at message M2: A reads it back;
//! > B, another member, sees their own, unset. T's version and facts are
//! > unchanged. A moves it to M3. A user who is not a member sets a mark on T:
//! > answered not found, exactly as for an unknown thread. Marking a message
//! > of another thread is refused. Removing A from T removes A's mark; added
//! > back, A's mark is unset.
//!
//! "Marking a message of another thread is refused" names no code; the
//! scenario expects `MESSAGE_NOT_FOUND`, the specification's code for "a
//! message that does not exist" — to be ratified.
//!
//! Channels: the mutation's verdict (1) and the affordances announcing it (2);
//! the thread as each member reads it (2); the fact log and the event stream,
//! proven untouched.

mod common;

use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    command, create_thread, message_view, say, thread_confirmed, thread_view, version,
};
use common::{
    M_SET_READ_MARK, MESSAGE_NOT_FOUND, QUIET_WINDOW, THREAD_NOT_FOUND, TestContext, gql,
};
use contract_threads::{AddMembers, RemoveMembers};
use serde_json::{Value, json};
use uuid::Uuid;

async fn mark(
    ctx: &TestContext,
    passport: &br_core_auth::Passport,
    thread: Uuid,
    message: Uuid,
) -> Value {
    ctx.run(
        passport,
        M_SET_READ_MARK,
        json!({ "thread": thread, "message": message }),
    )
    .await
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t15_a_member_sets_their_own_read_mark_the_readers_own_state_with_no_fact() {
    // Given T with owner O and members A and B, its messages M1, M2, M3; T',
    // another thread of A's, with its message N; U, not a member of T.
    let ctx = TestContext::setup().await;
    let (o, a, b, u) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
        ctx.enrol("ugo").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b]).await;
    let m1 = say(&ctx, t, None, &o, "one").await;
    let m2 = say(&ctx, t, Some(m1), &o, "two").await;
    let m3 = say(&ctx, t, Some(m2), &o, "three").await;
    let other = create_thread(&ctx, Some(&a), &[&u]).await;
    let n = say(&ctx, other, None, &a, "elsewhere").await;

    // And (2) the affordances offer A to mark T and M2.
    gql::assert_allowed(&thread_view(&ctx, &a.passport, t).await, "setReadMark");
    gql::assert_allowed(&message_view(&ctx, &a.passport, t, m2).await, "markRead");

    // Given A and B following T.
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let mut b_feed = Live::thread(&ctx, &b.passport, t).await;

    // When A sets their read mark on T at M2.
    let version_before = version(&ctx, &o.passport, t).await;
    let facts_before = facts::count(&ctx.pool).await;
    let events_before = ctx.nats.event_stream_len().await;
    let set = mark(&ctx, &a.passport, t, m2).await;

    // Then (1) acknowledged, and nothing else.
    assert_eq!(
        gql::data(&set, "threadsSetReadMark"),
        &json!({ "success": true })
    );
    // And (2) A reads it back; B's own is unset.
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["readMark"],
        Value::String(m2.to_string())
    );
    assert_eq!(
        thread_view(&ctx, &b.passport, t).await["readMark"],
        Value::Null
    );
    // And (3) A's live view carries A's own mark; B's is told nothing — the
    // mark is the reader's own state.
    a_feed
        .until("A's mark reaches A's feed", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["readMark"] == m2.to_string())
        })
        .await;
    b_feed.silence("A's mark is not B's").await;
    // And T's version, the facts and the event stream are untouched.
    tokio::time::sleep(QUIET_WINDOW).await;
    assert_eq!(version(&ctx, &o.passport, t).await, version_before);
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    assert_eq!(ctx.nats.event_stream_len().await, events_before);

    // When A moves it to M3.
    let moved = mark(&ctx, &a.passport, t, m3).await;
    // Then A reads M3.
    assert_eq!(
        gql::data(&moved, "threadsSetReadMark"),
        &json!({ "success": true })
    );
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["readMark"],
        Value::String(m3.to_string())
    );

    // When U, not a member, sets a mark on T.
    let stranger = mark(&ctx, &u.passport, t, m1).await;
    let unknown = mark(&ctx, &u.passport, Uuid::now_v7(), m1).await;
    // Then answered not found, exactly as for an unknown thread.
    gql::assert_refused(&stranger, THREAD_NOT_FOUND);
    assert_eq!(
        (gql::code(&stranger), stranger["data"].clone()),
        (gql::code(&unknown), unknown["data"].clone())
    );

    // When A marks, on T, a message of T'.
    let foreign = mark(&ctx, &a.passport, t, n).await;
    // Then refused; A's mark stays at M3.
    gql::assert_refused(&foreign, MESSAGE_NOT_FOUND);
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["readMark"],
        Value::String(m3.to_string())
    );

    // When A is removed from T, then added back.
    let removed = command(
        &ctx,
        &RemoveMembers {
            thread_id: t,
            user_ids: vec![a.id],
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, removed, "removing A").await;
    let added = command(
        &ctx,
        &AddMembers {
            thread_id: t,
            user_ids: vec![a.id],
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, added, "adding A back").await;

    // Then A's mark went with the membership: unset.
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["readMark"],
        Value::Null
    );

    ctx.teardown().await;
}
