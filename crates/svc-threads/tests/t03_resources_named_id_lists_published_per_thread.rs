//! **T03 — Resources: named id lists, published per thread.**
//!
//! > On thread T, the harness adds ids d1 and d2 under key documents:
//! > confirmed, a member reading T sees documents [d1, d2] and their live view
//! > of T carries them. Adding d2 again is confirmed and changes nothing;
//! > removing d3, never added, is confirmed and changes nothing. Removing d1
//! > and d2 empties the key: it disappears from the thread. Ids k1 and k2 are
//! > added under key kms; a runner reading T's context receives resources
//! > {kms: [k1, k2]}. Each change rewrites threads.resources.{T} with T's keys
//! > and ids; the commands that change nothing leave the entry as it was.
//! > Adding resource ids to an unknown thread is refused THREAD_NOT_FOUND.
//!
//! "Leave the entry as it was" is read on the entry's KV revision: unchanged
//! once the relay had the quiet window to write anything owed. A resources
//! command publishes its one answer and no other event, and leaves the members
//! entry as it was.
//!
//! Channels: the answers (4); the resources entry and its revision, the
//! members entry's revision and the event stream (4); the thread as a member
//! reads it (2) and live (3); a runner's context; the fact log.

mod common;

use std::collections::BTreeMap;

use br_util_nats_fabric::Revision;
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    command, context, create_thread, resources_of, thread_confirmed, thread_refused, thread_view,
    version,
};
use common::{QUIET_WINDOW, TestContext};
use contract_threads::{AddResourceIds, PublishedThreadResources, ReasonCode, RemoveResourceIds};
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

fn entry(resources: &[(&str, &[&str])]) -> PublishedThreadResources {
    PublishedThreadResources {
        resources: resources
            .iter()
            .map(|(key, values)| (key.to_string(), ids(values)))
            .collect(),
    }
}

/// What Threads has published about T once the quiet window passed: the
/// events on the stream, T's resources entry with its revision, and the
/// revision of T's members entry.
struct Published {
    events: u64,
    resources: (PublishedThreadResources, Revision),
    members_revision: Revision,
}

async fn published(ctx: &TestContext, thread: Uuid) -> Published {
    tokio::time::sleep(QUIET_WINDOW).await;
    Published {
        events: ctx.nats.event_stream_len().await,
        resources: ctx
            .resources_entry(thread)
            .await
            .expect("T's resources entry"),
        members_revision: ctx.members_entry(thread).await.expect("T's entry").1,
    }
}

/// A change: the command's answer is the one event, the resources entry is
/// rewritten to `expected`, the members entry is left as it was.
fn assert_rewritten(
    before: &Published,
    after: &Published,
    expected: &PublishedThreadResources,
    what: &str,
) {
    assert_eq!(
        after.events,
        before.events + 1,
        "{what}: the command's answer is the one event published"
    );
    assert_eq!(&after.resources.0, expected, "{what}: the entry");
    assert_ne!(
        after.resources.1, before.resources.1,
        "{what}: the entry is rewritten"
    );
    assert_eq!(
        after.members_revision, before.members_revision,
        "{what}: the members entry is left as it was"
    );
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t03_resources_are_named_id_lists_shown_to_members_and_runners_and_published_per_thread() {
    // Given thread T with member A, and a runner; T's resources entry is
    // published empty.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, None, &[&a]).await;
    ctx.resources_until(t, "T's resources entry is published", |_| true)
        .await;
    let runner = ctx.service_account(true);
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;

    // When the harness adds d1 and d2 under `documents`.
    let before = published(&ctx, t).await;
    assert_eq!(before.resources.0, entry(&[]), "an empty object at first");
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
    // And (4) threads.resources.{T} is rewritten with documents [d1, d2].
    let documents = entry(&[("documents", &["d1", "d2"])]);
    ctx.resources_until(t, "d1 and d2 are published", |held| held == &documents)
        .await;
    let after = published(&ctx, t).await;
    assert_rewritten(&before, &after, &documents, "adding d1 and d2");

    // When the harness adds d2 again, then removes d3, never added.
    let facts_before = facts::count(&ctx.pool).await;
    let version_before = version(&ctx, &a.passport, t).await;
    let entry_before = after.resources.clone();
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
        // And (4) the entry is left as it was: same value, same revision.
        assert_eq!(
            ctx.resources_settled(t).await,
            entry_before,
            "{what}: the entry is left as it was"
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
    // And (4) from the entry: T's resources are an empty object again.
    ctx.resources_until(t, "the emptied key leaves the entry", |held| {
        held == &entry(&[])
    })
    .await;
    assert_rewritten(
        &before,
        &published(&ctx, t).await,
        &entry(&[]),
        "emptying documents",
    );

    // When k1 and k2 are added under `kms`.
    let before = published(&ctx, t).await;
    let kms = command(&ctx, &add(t, "kms", &["k1", "k2"])).await;
    thread_confirmed(&ctx, kms, "adding kms ids").await;

    // Then a runner reading T's context receives resources {kms: [k1, k2]}.
    let context = context(&ctx, &runner.passport, t, None).await;
    assert_eq!(
        resources_of(&context),
        BTreeMap::from([("kms".to_string(), ids(&["k1", "k2"]))])
    );
    // And (4) the entry is rewritten with kms [k1, k2].
    let kms_entry = entry(&[("kms", &["k1", "k2"])]);
    ctx.resources_until(t, "k1 and k2 are published", |held| held == &kms_entry)
        .await;
    assert_rewritten(
        &before,
        &published(&ctx, t).await,
        &kms_entry,
        "adding kms ids",
    );

    // When resource ids are added to a thread nobody created.
    let unknown = Uuid::now_v7();
    let nowhere = command(&ctx, &add(unknown, "kms", &["k9"])).await;

    // Then refused THREAD_NOT_FOUND, and nothing is published for it.
    thread_refused(
        &ctx,
        nowhere,
        ReasonCode::ThreadNotFound,
        "an unknown thread",
    )
    .await;
    tokio::time::sleep(QUIET_WINDOW).await;
    assert!(ctx.resources_entry(unknown).await.is_none());

    ctx.teardown().await;
}
