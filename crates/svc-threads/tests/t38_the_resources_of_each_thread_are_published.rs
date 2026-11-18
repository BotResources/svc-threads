//! **T38 — Thread resources: published, rewritten on change, removed on
//! deletion.**
//!
//! > The KV offer threads.resources.{thread_id} follows a thread's resource
//! > keys and ids, from its creation to its deletion, and covers the threads
//! > created before the upgrade.
//!
//! The registry gives this test its short description alone; the gesture below
//! is the oracle's reading of it, against the specification's published
//! language ("`{ resources }`, the thread's keys with their ids (an empty
//! object for a thread without resources), rewritten when its resources change
//! and removed when the thread is deleted") and the contract
//! (`contract-threads` 0.9.0, module `resources`):
//!
//! > The harness creates thread T, owned by O with member A, with resources
//! > {kms: [k1], documents: [d1, d2]}: threads.resources.{T} holds them, and
//! > the manifest threads.resources_manifest declares the prefix and version
//! > 1. Thread U, created without resources, publishes an empty object. Adding
//! > k2 under kms rewrites T's entry. Setting T's title, adding a member and
//! > A's message change T but not its resources: the entry is not rewritten.
//! > Removing d1 and d2 drops documents from the entry. Deleting T on command
//! > retracts its entry; U's stays as it was. Thread V, owned by P, with
//! > resources {kms: [k3]}: P leaves the roster, V is deleted and its entry
//! > retracted.
//! >
//! > Upgrade: the previous release stores thread W with resources {kms: [k1]}
//! > and thread X without resources, publishing no resources entry. The new
//! > version starts on the same database: W's entry holds {kms: [k1]}, X's an
//! > empty object, and the manifest is published. Adding k2 to W afterwards
//! > rewrites its entry.
//!
//! "Not rewritten" is read on the entry's KV revision, unchanged once the
//! relay had the quiet window to write anything owed. The previous release is
//! `svc-threads` 0.4.0, as T26 runs it (`common::previous`): it never
//! published resources, as no release before 0.11 did.
//!
//! Channels: the answers (4); the resources entries, their revisions and the
//! manifest (4); the thread as its members read it (2); Identity's roster (4,
//! consumed).

mod common;

use common::flows::{command, create, creation, resources_of, say, thread_confirmed, thread_view};
use common::{PUSH_TIMEOUT, TestContext, eventually};
use contract_threads::{
    AddMembers, AddResourceIds, DeleteThread, PublishedThreadResources, RemoveResourceIds, SetTitle,
};
use serde_json::json;
use uuid::Uuid;

fn ids(values: &[&str]) -> Vec<String> {
    values.iter().map(|id| id.to_string()).collect()
}

fn entry(resources: &[(&str, &[&str])]) -> PublishedThreadResources {
    PublishedThreadResources {
        resources: resources
            .iter()
            .map(|(key, values)| (key.to_string(), ids(values)))
            .collect(),
    }
}

fn add(thread: Uuid, key: &str, values: &[&str]) -> AddResourceIds {
    AddResourceIds {
        thread_id: thread,
        key: key.to_string(),
        ids: ids(values),
        acting_user_id: None,
    }
}

/// The manifest of the family: its prefix and wire version.
fn manifest() -> serde_json::Value {
    json!({ "prefix": "threads.resources.", "version": 1 })
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t38_each_thread_publishes_its_resources_from_its_creation_to_its_deletion() {
    let ctx = TestContext::setup().await;
    let (o, a, p) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("paul").await,
    );

    // ── creation ─────────────────────────────────────────────────────────

    // When the harness creates T, owned by O with A, with resources.
    let mut born = creation(Some(o.id), &[a.id]);
    born.resources = entry(&[("kms", &["k1"]), ("documents", &["d1", "d2"])]).resources;
    let t = create(&ctx, born).await;

    // Then (4) T's entry holds its keys with their ids, as its members read
    // them (2); the manifest declares the family and its wire version.
    let created = entry(&[("documents", &["d1", "d2"]), ("kms", &["k1"])]);
    ctx.resources_until(t, "T's resources are published", |held| held == &created)
        .await;
    assert_eq!(
        resources_of(&thread_view(&ctx, &a.passport, t).await),
        created.resources
    );
    assert_eq!(ctx.resources_manifest().await, Some(manifest()));

    // When U is created without resources.
    let u = create(&ctx, creation(Some(o.id), &[])).await;

    // Then (4) U's entry is an empty object.
    ctx.resources_until(u, "U's empty resources are published", |held| {
        held == &entry(&[])
    })
    .await;
    let u_entry = ctx.resources_settled(u).await;
    assert_eq!(u_entry.0, entry(&[]));

    // ── a change of the resources ────────────────────────────────────────

    // When k2 is added under kms.
    let (_, revision_created) = ctx.resources_settled(t).await;
    let added = command(&ctx, &add(t, "kms", &["k2"])).await;

    // Then (4) T's entry is rewritten with it.
    thread_confirmed(&ctx, added, "adding k2").await;
    let grown = entry(&[("documents", &["d1", "d2"]), ("kms", &["k1", "k2"])]);
    let (_, revision_grown) = ctx
        .resources_until(t, "k2 is published", |held| held == &grown)
        .await;
    assert_ne!(revision_grown, revision_created);

    // ── changes outside the resources ────────────────────────────────────

    // When T's title is set, B is added and A writes a message.
    let held = ctx.resources_settled(t).await;
    let titled = command(
        &ctx,
        &SetTitle {
            thread_id: t,
            title: "Q3 review".to_string(),
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, titled, "setting T's title").await;
    let b = ctx.enrol("bob").await;
    let joined = command(
        &ctx,
        &AddMembers {
            thread_id: t,
            user_ids: vec![b.id],
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, joined, "adding B").await;
    say(&ctx, t, None, &a, "Here are the Q3 figures").await;

    // Then T changed — B reads its title — but (4) its resources entry is not
    // rewritten: same value, same revision.
    assert_eq!(
        thread_view(&ctx, &b.passport, t).await["title"],
        "Q3 review"
    );
    assert_eq!(
        ctx.resources_settled(t).await,
        held,
        "a change outside the resources rewrites no resources entry"
    );

    // ── a key emptied ────────────────────────────────────────────────────

    // When d1 and d2 are removed.
    let removed = command(
        &ctx,
        &RemoveResourceIds {
            thread_id: t,
            key: "documents".to_string(),
            ids: ids(&["d1", "d2"]),
            acting_user_id: None,
        },
    )
    .await;

    // Then (4) documents leaves T's entry.
    thread_confirmed(&ctx, removed, "emptying documents").await;
    let shrunk = entry(&[("kms", &["k1", "k2"])]);
    ctx.resources_until(t, "documents leaves T's entry", |held| held == &shrunk)
        .await;

    // ── deletion on command ──────────────────────────────────────────────

    // When O deletes T.
    let deleted = command(
        &ctx,
        &DeleteThread {
            thread_id: t,
            acting_user_id: Some(o.id),
        },
    )
    .await;

    // Then (4) T's entry is retracted; U's stays as it was.
    thread_confirmed(&ctx, deleted, "deleting T").await;
    eventually("T's resources entry is retracted", PUSH_TIMEOUT, || async {
        ctx.resources_entry(t).await.is_none()
    })
    .await;
    assert_eq!(ctx.resources_settled(u).await, u_entry);

    // ── deletion because the owner left the roster ───────────────────────

    // Given V, owned by P, with resources {kms: [k3]}.
    let mut born = creation(Some(p.id), &[]);
    born.resources = entry(&[("kms", &["k3"])]).resources;
    let v = create(&ctx, born).await;
    ctx.resources_until(v, "V's resources are published", |held| {
        held == &entry(&[("kms", &["k3"])])
    })
    .await;

    // When Identity retracts P's roster entry.
    ctx.retract(&p).await;

    // Then V is deleted and (4) its entry retracted; U's stays.
    eventually("V's resources entry is retracted", PUSH_TIMEOUT, || async {
        ctx.resources_entry(v).await.is_none()
    })
    .await;
    assert_eq!(ctx.resources_settled(u).await, u_entry);

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH, and builds the previous release"]
async fn t38_the_threads_created_before_the_upgrade_are_published() {
    // Given the previous release, which publishes no resources, holding W
    // with resources {kms: [k1]} and X without resources.
    let mut ctx = TestContext::setup_previous().await;
    let o = ctx.enrol("olga").await;
    let mut born = creation(Some(o.id), &[]);
    born.resources = entry(&[("kms", &["k1"])]).resources;
    let w = create(&ctx, born).await;
    let x = create(&ctx, creation(Some(o.id), &[])).await;
    assert!(ctx.resources_entry(w).await.is_none());
    assert!(ctx.resources_entry(x).await.is_none());
    assert_eq!(ctx.resources_manifest().await, None);

    // When the new version starts on the same database.
    let upgrade = ctx.upgrade().await;
    assert!(
        upgrade.migrated && upgrade.ready,
        "the new version becomes ready:\n{}",
        upgrade.output
    );

    // Then (4) W's entry holds {kms: [k1]}, X's an empty object, and the
    // manifest is published.
    ctx.resources_until(w, "W's resources are published", |held| {
        held == &entry(&[("kms", &["k1"])])
    })
    .await;
    ctx.resources_until(x, "X's empty resources are published", |held| {
        held == &entry(&[])
    })
    .await;
    assert_eq!(ctx.resources_manifest().await, Some(manifest()));

    // When k2 is added to W.
    let (_, before) = ctx.resources_settled(w).await;
    let added = command(&ctx, &add(w, "kms", &["k2"])).await;

    // Then (4) W's entry is rewritten with it.
    thread_confirmed(&ctx, added, "adding k2 to W").await;
    let (_, after) = ctx
        .resources_until(w, "k2 is published", |held| {
            held == &entry(&[("kms", &["k1", "k2"])])
        })
        .await;
    assert_ne!(after, before);

    ctx.teardown().await;
}
