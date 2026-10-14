//! **T02 — Members: add, remove, the owner stays, the KV follows.**
//!
//! > On thread T owned by O with member A, the harness adds B and C:
//! > confirmed, threads.members.{T} lists O, A, B and C in the same
//! > transaction, and B and C read T at once. Adding A again is confirmed and
//! > changes nothing, no fact and no version bump. A sets a read mark on T. The
//! > harness removes A: confirmed, the KV entry drops A, A's read mark is gone
//! > and A's next read of T answers not found. Removing a user who is not a
//! > member is confirmed and changes nothing. Removing [B, O] is refused
//! > OWNER_NOT_REMOVABLE and removes no one: B is still listed. Adding a user
//! > unknown to the roster is refused USER_UNKNOWN and adds no one of the list.
//! > On an ownerless thread every member can be removed, and the KV entry
//! > follows down to an empty users list.
//!
//! Channels: the answers (4), the members entry and its revision (4), the fact
//! log, the thread as each person reads it (2), the read mark (1 + 2), and the
//! live lists of the people added and removed (3) — a removal is told once.

mod common;

use common::facts;
use common::feed::{Live, removes, upsert_of};
use common::flows::{
    await_members_equal, command, create_thread, member_ids, say, settled_revision, sorted,
    thread_confirmed, thread_refused, thread_view, version,
};
use common::{M_SET_READ_MARK, TestContext, gql};
use contract_threads::{AddMembers, PublishedThreadMembers, ReasonCode, RemoveMembers};
use serde_json::{Value, json};
use uuid::Uuid;

/// The members entry of `thread` and its revision.
async fn entry(
    ctx: &TestContext,
    thread: Uuid,
) -> Option<(PublishedThreadMembers, br_util_nats_fabric::Revision)> {
    ctx.members_entry(thread).await
}

fn add(thread: Uuid, users: &[Uuid], acting: Uuid) -> AddMembers {
    AddMembers {
        thread_id: thread,
        user_ids: users.to_vec(),
        acting_user_id: Some(acting),
    }
}

fn remove(thread: Uuid, users: &[Uuid], acting: Uuid) -> RemoveMembers {
    RemoveMembers {
        thread_id: thread,
        user_ids: users.to_vec(),
        acting_user_id: Some(acting),
    }
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t02_members_are_added_and_removed_the_owner_stays_and_the_kv_follows() {
    // Given T owned by O with member A; B and C in the roster, following their
    // lists; A following theirs.
    let ctx = TestContext::setup().await;
    let (o, a, b, c, d) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
        ctx.enrol("cyd").await,
        ctx.enrol("dee").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a]).await;
    let mut b_list = Live::list(&ctx, &b.passport).await;
    let mut a_list = Live::list(&ctx, &a.passport).await;
    let mut a_feed_sse = Live::thread(&ctx, &a.passport, t).await;
    let mut a_feed_ws = Live::thread_ws(&ctx, &a.passport, t).await;
    let version_born = version(&ctx, &o.passport, t).await;

    // When the harness adds B and C.
    let added = command(&ctx, &add(t, &[b.id, c.id], o.id)).await;

    // Then (4) confirmed, and the members entry lists O, A, B and C.
    thread_confirmed(&ctx, added, "adding B and C").await;
    await_members_equal(
        &ctx,
        t,
        Some(o.id),
        &[o.id, a.id, b.id, c.id],
        "the entry lists B and C",
    )
    .await;

    // And (2) B and C read T at once, listed among its members.
    for person in [&b, &c] {
        let view = thread_view(&ctx, &person.passport, t).await;
        assert_eq!(
            member_ids(&view),
            sorted(&[o.id, a.id, b.id, c.id]),
            "{view}"
        );
    }
    // And (3) T enters B's live list.
    b_list
        .until("T enters B's list", |delta| upsert_of(delta, t).is_some())
        .await;
    // And a MembersAdded fact records the change, led by O; the version moved.
    let added_fact = facts::of(&ctx.pool, "thread", t)
        .await
        .into_iter()
        .rfind(|fact| fact.event_type == "MembersAdded")
        .expect("a MembersAdded fact");
    assert_eq!(added_fact.acting_user_id, Some(o.id));
    let version_after_add = version(&ctx, &o.passport, t).await;
    assert!(version_after_add > version_born);

    // When the harness adds A again.
    let facts_before = facts::count(&ctx.pool).await;
    let revision_before = settled_revision(&ctx, t).await;
    let again = command(&ctx, &add(t, &[a.id], o.id)).await;

    // Then confirmed, and nothing changes: no fact, no version bump, no
    // rewrite of the entry.
    thread_confirmed(&ctx, again, "adding A again").await;
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    assert_eq!(version(&ctx, &o.passport, t).await, version_after_add);
    assert_eq!(settled_revision(&ctx, t).await, revision_before);

    // Given A sets their read mark on T, at a message of T.
    let m1 = say(&ctx, t, None, &o, "first words").await;
    let marked = ctx
        .run(
            &a.passport,
            M_SET_READ_MARK,
            json!({ "thread": t, "message": m1 }),
        )
        .await;
    assert_eq!(
        gql::data(&marked, "threadsSetReadMark"),
        &json!({ "success": true })
    );
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["readMark"],
        Value::String(m1.to_string())
    );

    // When the harness removes A.
    let removed = command(&ctx, &remove(t, &[a.id], o.id)).await;

    // Then (4) confirmed, and the entry drops A.
    thread_confirmed(&ctx, removed, "removing A").await;
    await_members_equal(
        &ctx,
        t,
        Some(o.id),
        &[o.id, b.id, c.id],
        "the entry drops A",
    )
    .await;
    // And A's read mark is gone with their membership.
    let marks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM threads.read_mark WHERE thread_id = $1 AND user_id = $2",
    )
    .bind(t)
    .bind(a.id)
    .fetch_one(&ctx.pool)
    .await
    .expect("read the read marks");
    assert_eq!(marks, 0, "removing a member removes their read mark");
    // And (2) A's next read of T answers not found, exactly as an unknown id.
    assert_eq!(thread_view(&ctx, &a.passport, t).await, Value::Null);
    // And (3) A's live list is told once that T left it.
    a_list
        .until("T leaves A's list", |delta| removes(delta, t))
        .await;
    a_list.silence("T leaves A's list once").await;
    // And (3) A's open live views of T, over both transports, are revoked:
    // T removed, or the stream ended — and nothing of T reaches A after.
    for (feed, transport) in [(&mut a_feed_sse, "SSE"), (&mut a_feed_ws, "WS")] {
        feed.until_revoked(&format!("A loses T over {transport}"), t)
            .await;
    }
    say(&ctx, t, Some(m1), &o, "after A left").await;
    for (feed, transport) in [(&mut a_feed_sse, "SSE"), (&mut a_feed_ws, "WS")] {
        feed.silent_or_ended(&format!("a removed member's {transport} view"))
            .await;
    }

    // When the harness removes D, who is not a member.
    let facts_before = facts::count(&ctx.pool).await;
    let version_before = version(&ctx, &o.passport, t).await;
    let revision_before = settled_revision(&ctx, t).await;
    let absent = command(&ctx, &remove(t, &[d.id], o.id)).await;

    // Then confirmed, and nothing changes.
    thread_confirmed(&ctx, absent, "removing a non-member").await;
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    assert_eq!(version(&ctx, &o.passport, t).await, version_before);
    assert_eq!(settled_revision(&ctx, t).await, revision_before);

    // When the harness removes [B, O].
    let owner_out = command(&ctx, &remove(t, &[b.id, o.id], b.id)).await;

    // Then refused OWNER_NOT_REMOVABLE, and no one is removed: B is still
    // listed, in the entry and on the thread.
    thread_refused(
        &ctx,
        owner_out,
        ReasonCode::OwnerNotRemovable,
        "removing the owner",
    )
    .await;
    assert_eq!(settled_revision(&ctx, t).await, revision_before);
    let (members, _) = entry(&ctx, t).await.expect("T's entry");
    assert_eq!(sorted(&members.users), sorted(&[o.id, b.id, c.id]));
    assert!(thread_view(&ctx, &b.passport, t).await.is_object());
    assert_eq!(facts::count(&ctx.pool).await, facts_before);

    // When the harness adds [D, a user unknown to the roster].
    let unknown = command(&ctx, &add(t, &[d.id, Uuid::now_v7()], o.id)).await;

    // Then refused USER_UNKNOWN, and no one of the list is added.
    thread_refused(
        &ctx,
        unknown,
        ReasonCode::UserUnknown,
        "adding an unknown user",
    )
    .await;
    assert_eq!(settled_revision(&ctx, t).await, revision_before);
    let (members, _) = entry(&ctx, t).await.expect("T's entry");
    assert!(!members.users.contains(&d.id), "D was not added");
    assert_eq!(thread_view(&ctx, &d.passport, t).await, Value::Null);
    assert_eq!(facts::count(&ctx.pool).await, facts_before);

    // Given an ownerless thread W with members A and B.
    let w = create_thread(&ctx, None, &[&a, &b]).await;

    // When the harness removes A, then B.
    let first = command(&ctx, &remove(w, &[a.id], b.id)).await;
    thread_confirmed(&ctx, first, "removing A from W").await;
    let second = command(&ctx, &remove(w, &[b.id], b.id)).await;
    thread_confirmed(&ctx, second, "removing B from W").await;

    // Then the entry follows down to an empty users list, with no owner.
    await_members_equal(&ctx, w, None, &[], "W's entry empties").await;

    ctx.teardown().await;
}
