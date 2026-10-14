//! **T16 — Child threads.**
//!
//! > Thread P has members O and A and a message with block K. The harness
//! > creates child C with parent P, origin block K, owner A and member B (not a
//! > member of P): confirmed; C's members are A and B, not O. Creating a child
//! > owned by B is refused OWNER_NOT_PARENT_MEMBER. A child with a parent and
//! > no origin block is refused. Removing A from P changes nothing on C. P is
//! > deleted: C stays with its messages, members and resources, its
//! > parent_thread_id and origin_block_id now leading nowhere. An ownerless
//! > child of P is created without the owner check.
//!
//! "A child with a parent and no origin block is refused" names no code; the
//! scenario expects `ORIGIN_BLOCK_REQUIRED`, the contract's proposed code —
//! to be ratified.
//!
//! Channels: the answers (4), the members entries (4), the threads as members
//! read them (2), the messages of the child.

mod common;

use std::collections::BTreeMap;

use common::TestContext;
use common::content::user_text;
use common::flows::{
    command, create, create_thread, creation, member_ids, resources_of, say, sorted, store,
    thread_confirmed, thread_refused, thread_view, user_message,
};
use contract_threads::{DeleteThread, MessageStatus, ReasonCode, RemoveMembers};
use serde_json::Value;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t16_a_child_thread_names_its_parent_and_origin_block_then_lives_on_its_own() {
    // Given P with members O (owner) and A, and a message carrying block K.
    let ctx = TestContext::setup().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let p = create_thread(&ctx, Some(&o), &[&a]).await;
    let block = user_text("let us dig into this one");
    let k = block.id;
    store(
        &ctx,
        user_message(p, None, a.id, vec![block]),
        MessageStatus::Completed,
    )
    .await;

    // When the harness creates child C of P from K, owned by A, with member B
    // (not a member of P) and resources.
    let mut child = creation(Some(a.id), &[b.id]);
    child.parent_thread_id = Some(p);
    child.origin_block_id = Some(k);
    child.resources = BTreeMap::from([("kms".to_string(), vec!["k1".to_string()])]);
    let c = create(&ctx, child).await;

    // Then (4) confirmed, and C's members are A and B — not O.
    let members = ctx.published_members(c).await.expect("C's entry");
    assert_eq!(members.owner_id, Some(a.id));
    assert_eq!(sorted(&members.users), sorted(&[a.id, b.id]));
    // And (2) C names its parent and origin block; O cannot read it.
    let view = thread_view(&ctx, &b.passport, c).await;
    assert_eq!(view["parentThreadId"], Value::String(p.to_string()));
    assert_eq!(view["originBlockId"], Value::String(k.to_string()));
    assert_eq!(member_ids(&view), sorted(&[a.id, b.id]));
    assert_eq!(thread_view(&ctx, &o.passport, c).await, Value::Null);
    let on_c = say(&ctx, c, None, &b, "child's own talk").await;

    // When a child of P is to be owned by B, not a member of P.
    let mut b_owned = creation(Some(b.id), &[]);
    b_owned.parent_thread_id = Some(p);
    b_owned.origin_block_id = Some(k);
    let b_owned_thread = b_owned.thread_id;
    let refused = command(&ctx, &b_owned).await;
    // Then refused OWNER_NOT_PARENT_MEMBER, and nothing is created.
    thread_refused(
        &ctx,
        refused,
        ReasonCode::OwnerNotParentMember,
        "a non-member owner",
    )
    .await;
    assert!(ctx.published_members(b_owned_thread).await.is_none());

    // When a child names its parent without an origin block.
    let mut no_origin = creation(Some(a.id), &[]);
    no_origin.parent_thread_id = Some(p);
    let no_origin_thread = no_origin.thread_id;
    let refused = command(&ctx, &no_origin).await;
    // Then refused, and nothing is created.
    thread_refused(
        &ctx,
        refused,
        ReasonCode::OriginBlockRequired,
        "no origin block",
    )
    .await;
    assert!(ctx.published_members(no_origin_thread).await.is_none());

    // When A is removed from P.
    let removed = command(
        &ctx,
        &RemoveMembers {
            thread_id: p,
            user_ids: vec![a.id],
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, removed, "removing A from P").await;
    // Then nothing changes on C: nothing passes from a parent to a child.
    assert_eq!(
        sorted(&ctx.published_members(c).await.expect("C's entry").users),
        sorted(&[a.id, b.id])
    );
    assert!(thread_view(&ctx, &a.passport, c).await.is_object());

    // When an ownerless child of P is created with B, not a member of P.
    let mut ownerless = creation(None, &[b.id]);
    ownerless.parent_thread_id = Some(p);
    ownerless.origin_block_id = Some(k);
    // Then it is confirmed: no owner, no owner check.
    let ownerless_child = create(&ctx, ownerless).await;
    assert!(
        thread_view(&ctx, &b.passport, ownerless_child)
            .await
            .is_object()
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

    // Then C stays, with its messages, members and resources; its parent and
    // origin block lead nowhere.
    assert_eq!(thread_view(&ctx, &o.passport, p).await, Value::Null);
    let view = thread_view(&ctx, &b.passport, c).await;
    assert_eq!(view["parentThreadId"], Value::String(p.to_string()));
    assert_eq!(view["originBlockId"], Value::String(k.to_string()));
    assert_eq!(member_ids(&view), sorted(&[a.id, b.id]));
    assert_eq!(
        resources_of(&view),
        BTreeMap::from([("kms".to_string(), vec!["k1".to_string()])])
    );
    assert_eq!(
        common::flows::active_branch(&ctx, &b.passport, c).await,
        vec![on_c]
    );
    assert!(ctx.published_members(c).await.is_some());

    ctx.teardown().await;
}
