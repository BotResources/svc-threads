//! **T03 — Resources: named id lists, never published.**
//!
//! > On thread T, the harness adds ids d1 and d2 under key documents:
//! > confirmed, a member reading T sees documents [d1, d2]. Adding d2 again is
//! > confirmed and changes nothing. Removing d3, never added, is confirmed and
//! > changes nothing. Removing d1 and d2 empties the key: it disappears from
//! > the thread. Ids under key kms are added; a runner reading T's context
//! > receives resources {kms: [...]}. No KV entry and no event other than the
//! > command's confirmation is ever published for resources. Adding resource
//! > ids to an unknown thread is refused THREAD_NOT_FOUND.
//!
//! Channels: the answers (4) — and, proven, nothing else on the event stream
//! nor in the published language (4); the thread as a member reads it (2); a
//! runner's context; the fact log.

mod common;

use std::collections::BTreeMap;

use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    command, context, create_thread, resources_of, thread_confirmed, thread_refused, thread_view,
    version,
};
use common::{QUIET_WINDOW, TestContext};
use contract_threads::{AddResourceIds, ReasonCode, RemoveResourceIds};
use uuid::Uuid;

fn ids(values: &[&str]) -> Vec<String> {
    values.iter().map(|id| id.to_string()).collect()
}

fn add(thread: Uuid, key: &str, values: &[&str]) -> AddResourceIds {
    AddResourceIds {
        thread_id: thread,
        key: key.to_string(),
        ids: ids(values),
        acting_user_id: None,
    }
}

fn remove(thread: Uuid, key: &str, values: &[&str]) -> RemoveResourceIds {
    RemoveResourceIds {
        thread_id: thread,
        key: key.to_string(),
        ids: ids(values),
        acting_user_id: None,
    }
}

/// What a resources command publishes: exactly its one answer on the event
/// stream, and no write to the published language.
struct Published {
    events: u64,
    members_revision: br_util_nats_fabric::Revision,
    buckets: std::collections::BTreeSet<String>,
}

async fn published(ctx: &TestContext, thread: Uuid) -> Published {
    tokio::time::sleep(QUIET_WINDOW).await;
    Published {
        events: ctx.nats.event_stream_len().await,
        members_revision: ctx.members_entry(thread).await.expect("T's entry").1,
        buckets: ctx.nats.kv_bucket_names().await,
    }
}

fn assert_only_the_answer(before: &Published, after: &Published, what: &str) {
    assert_eq!(
        after.events,
        before.events + 1,
        "{what}: the command's answer is the one event published"
    );
    assert_eq!(
        after.members_revision, before.members_revision,
        "{what}: resources are never published"
    );
    assert_eq!(after.buckets, before.buckets, "{what}: no new bucket");
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t03_resources_are_named_id_lists_shown_to_members_and_runners_never_published() {
    // Given thread T with member A, and a runner.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, None, &[&a]).await;
    let runner = ctx.service_account(true);
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;

    // When the harness adds d1 and d2 under `documents`.
    let before = published(&ctx, t).await;
    let added = command(&ctx, &add(t, "documents", &["d1", "d2"])).await;

    // Then confirmed, and (2) A sees documents [d1, d2] on T.
    thread_confirmed(&ctx, added, "adding d1 and d2").await;
    let view = thread_view(&ctx, &a.passport, t).await;
    assert_eq!(
        resources_of(&view),
        BTreeMap::from([("documents".to_string(), ids(&["d1", "d2"]))])
    );
    assert!(
        !facts::of(&ctx.pool, "thread", t)
            .await
            .iter()
            .filter(|fact| fact.event_type == "ResourceIdsAdded")
            .collect::<Vec<_>>()
            .is_empty(),
        "a ResourceIdsAdded fact records the change"
    );
    // And (3) A's live view of T carries the resources.
    a_feed
        .until("the resources reach A's feed", |delta| {
            upsert_of(delta, t).is_some_and(|view| {
                resources_of(view)
                    == BTreeMap::from([("documents".to_string(), ids(&["d1", "d2"]))])
            })
        })
        .await;
    // And (4) the confirmation is all that was published.
    assert_only_the_answer(&before, &published(&ctx, t).await, "adding d1 and d2");

    // When the harness adds d2 again, then removes d3, never added.
    let facts_before = facts::count(&ctx.pool).await;
    let version_before = version(&ctx, &a.passport, t).await;
    for (what, command_id) in [
        (
            "adding d2 again",
            command(&ctx, &add(t, "documents", &["d2"])).await,
        ),
        (
            "removing d3",
            command(&ctx, &remove(t, "documents", &["d3"])).await,
        ),
    ] {
        // Then each is confirmed and changes nothing.
        thread_confirmed(&ctx, command_id, what).await;
        assert_eq!(
            facts::count(&ctx.pool).await,
            facts_before,
            "{what}: no fact"
        );
        assert_eq!(
            version(&ctx, &a.passport, t).await,
            version_before,
            "{what}"
        );
        assert_eq!(
            resources_of(&thread_view(&ctx, &a.passport, t).await),
            BTreeMap::from([("documents".to_string(), ids(&["d1", "d2"]))]),
            "{what}"
        );
    }

    // When the harness removes d1 and d2.
    let before = published(&ctx, t).await;
    let emptied = command(&ctx, &remove(t, "documents", &["d1", "d2"])).await;

    // Then confirmed, and the emptied key disappears from the thread.
    thread_confirmed(&ctx, emptied, "emptying documents").await;
    a_feed
        .until("the emptied key leaves A's feed", |delta| {
            upsert_of(delta, t).is_some_and(|view| resources_of(view).is_empty())
        })
        .await;
    assert!(resources_of(&thread_view(&ctx, &a.passport, t).await).is_empty());
    assert_only_the_answer(&before, &published(&ctx, t).await, "emptying documents");

    // When ids are added under `kms`.
    let kms = command(&ctx, &add(t, "kms", &["k1", "k2"])).await;
    thread_confirmed(&ctx, kms, "adding kms ids").await;

    // Then a runner reading T's context receives resources {kms: [k1, k2]}.
    let context = context(&ctx, &runner.passport, t, None).await;
    assert_eq!(
        resources_of(&context),
        BTreeMap::from([("kms".to_string(), ids(&["k1", "k2"]))])
    );

    // When resource ids are added to a thread nobody created.
    let nowhere = command(&ctx, &add(Uuid::now_v7(), "kms", &["k9"])).await;

    // Then refused THREAD_NOT_FOUND.
    thread_refused(
        &ctx,
        nowhere,
        ReasonCode::ThreadNotFound,
        "an unknown thread",
    )
    .await;

    ctx.teardown().await;
}
