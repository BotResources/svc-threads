//! **T33 — Listing the child threads of a thread.**
//!
//! > Thread P has three children C1, C2, C3 created in that order; user A is a
//! > member of C1 and C3, not of C2. A lists the children of P: C1 then C3,
//! > each with its origin block, C2 left out without error. With a page size
//! > of 1, A gets C1, then after C1 gets C3, then after C3 an empty page. P is
//! > deleted: A still lists C1 and C3. A thread without child, and a thread id
//! > that does not exist, give an empty list.
//!
//! Channel: the children as A reads them (2), each child compared whole with
//! the thread as A reads it — the same view as A's thread list.
//!
//! The second test is not the registry's gesture: it pins what the
//! specification leaves open — an `after` that is not a child A sees gives an
//! empty page — and that the root admits and bounds its page as the thread
//! list does.

mod common;

use common::content::text;
use common::flows::{
    child_threads, child_threads_answer, command, create, create_thread, creation, store,
    thread_confirmed, thread_view, user_message, uuid_of,
};
use common::{ACTIVE_USER_REQUIRED, Person, TestContext, WINDOW_SIZE_INVALID, gql};
use contract_threads::{DeleteThread, MessageStatus};
use serde_json::Value;
use uuid::Uuid;

/// A child of `parent` started from `block`, ownerless, with `members`.
async fn child(ctx: &TestContext, parent: Uuid, block: Uuid, members: &[&Person]) -> Uuid {
    let users: Vec<Uuid> = members.iter().map(|person| person.id).collect();
    let mut creation = creation(None, &users);
    creation.parent_thread_id = Some(parent);
    creation.origin_block_id = Some(block);
    create(ctx, creation).await
}

fn ids(views: &[Value]) -> Vec<Uuid> {
    views.iter().map(|view| uuid_of(&view["id"])).collect()
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t33_a_member_lists_the_children_they_see_oldest_first_by_pages() {
    // Given P, owned by O with member A, and a message of A carrying the blocks
    // K1, K2, K3.
    let ctx = TestContext::setup().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let p = create_thread(&ctx, Some(&o), &[&a]).await;
    let blocks = [
        text("first branch"),
        text("second branch"),
        text("third branch"),
    ];
    let [k1, k2, k3] = [0, 1, 2].map(|at| blocks[at].id().as_uuid());
    store(
        &ctx,
        user_message(p, None, a.id, blocks.to_vec()),
        MessageStatus::Completed,
    )
    .await;
    // And three children created in that order: C1 with A, C2 without A, C3
    // with A.
    let c1 = child(&ctx, p, k1, &[&a, &b]).await;
    let c2 = child(&ctx, p, k2, &[&b]).await;
    let c3 = child(&ctx, p, k3, &[&a]).await;

    // When A lists the children of P.
    let children = child_threads(&ctx, &a.passport, p, 10, None).await;
    // Then C1 then C3, each with its origin block, C2 left out without error,
    // each the very view A reads of that thread.
    assert_eq!(ids(&children), vec![c1, c3], "{children:?}");
    for (view, block) in children.iter().zip([k1, k3]) {
        assert_eq!(view["parentThreadId"], Value::String(p.to_string()));
        assert_eq!(view["originBlockId"], Value::String(block.to_string()));
        assert_eq!(
            *view,
            thread_view(&ctx, &a.passport, uuid_of(&view["id"])).await,
            "a child is listed with the view of the thread list"
        );
    }
    assert!(!ids(&children).contains(&c2));

    // When A pages through them one by one.
    // Then C1, then after C1 C3, then after C3 an empty page.
    assert_eq!(
        ids(&child_threads(&ctx, &a.passport, p, 1, None).await),
        vec![c1]
    );
    assert_eq!(
        ids(&child_threads(&ctx, &a.passport, p, 1, Some(c1)).await),
        vec![c3]
    );
    assert!(
        child_threads(&ctx, &a.passport, p, 1, Some(c3))
            .await
            .is_empty()
    );

    // When P is deleted.
    let deleted = command(
        &ctx,
        &DeleteThread {
            thread_id: p,
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, deleted, "deleting P").await;
    assert_eq!(thread_view(&ctx, &a.passport, p).await, Value::Null);
    // Then A still lists C1 and C3.
    assert_eq!(
        ids(&child_threads(&ctx, &a.passport, p, 10, None).await),
        vec![c1, c3]
    );

    // When A lists the children of a thread without child, and of an id that
    // does not exist.
    // Then both lists are empty.
    assert!(
        child_threads(&ctx, &a.passport, c1, 10, None)
            .await
            .is_empty()
    );
    assert!(
        child_threads(&ctx, &a.passport, Uuid::now_v7(), 10, None)
            .await
            .is_empty()
    );

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t33_an_after_that_is_not_a_child_the_caller_sees_gives_an_empty_page() {
    // Given P with member A, and its children C1 (with A), C2 (without A) and
    // C3 (with A), created in that order; Q, another thread of A.
    let ctx = TestContext::setup().await;
    let (a, b) = (ctx.enrol("ada").await, ctx.enrol("bob").await);
    let p = create_thread(&ctx, None, &[&a, &b]).await;
    let q = create_thread(&ctx, None, &[&a]).await;
    let block = Uuid::now_v7();
    let c1 = child(&ctx, p, block, &[&a]).await;
    let c2 = child(&ctx, p, block, &[&b]).await;
    let c3 = child(&ctx, p, block, &[&a]).await;
    assert_eq!(
        ids(&child_threads(&ctx, &a.passport, p, 10, None).await),
        vec![c1, c3]
    );

    // When A pages after C2, a child A does not see; after Q, a thread that is
    // not a child of P; after an id nobody created.
    // Then each page is empty, without error: a cursor tells nothing of a
    // thread the caller does not see.
    for after in [c2, q, Uuid::now_v7()] {
        assert!(
            child_threads(&ctx, &a.passport, p, 10, Some(after))
                .await
                .is_empty(),
            "after {after}"
        );
    }
    // And B, who sees C2, pages after it.
    assert!(
        child_threads(&ctx, &b.passport, p, 10, Some(c2))
            .await
            .is_empty(),
        "C2 is B's last child of P"
    );
    assert_eq!(
        ids(&child_threads(&ctx, &b.passport, p, 10, None).await),
        vec![c2]
    );

    // When the page size is out of its bounds.
    // Then it is refused as the thread list refuses it.
    for size in [0, -1] {
        gql::assert_refused(
            &child_threads_answer(&ctx, &a.passport, p, size, None).await,
            WINDOW_SIZE_INVALID,
        );
    }

    // When a service account asks.
    // Then it is refused before any read, as on the thread list.
    let service = ctx.service_account(true);
    gql::assert_refused(
        &child_threads_answer(&ctx, &service.passport, p, 10, None).await,
        ACTIVE_USER_REQUIRED,
    );

    ctx.teardown().await;
}
