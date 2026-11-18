//! **T38 — Thread resources: published, rewritten on change, removed on
//! deletion.**
//!
//! > A caller creates thread T with resources {kms: [k1, k2]}: the KV entry
//! > threads.resources.{T} appears with resources {kms: [k1, k2]}. A thread U
//! > created without resources gets an entry with an empty resources object.
//! > Id d1 is added to T under key documents: the entry is rewritten with both
//! > keys. Adding k1 again leaves the entry as it was. Removing k1 and k2
//! > empties key kms: the entry keeps documents only. A title change or a
//! > stored message leaves the entry as it was. T is deleted: its entry is
//! > removed; U, whose owner leaves the roster, is deleted and its entry
//! > removed too. Threads created by the previous version, with and without
//! > resources, get their entry when the new version starts, and after any
//! > restart the entries match the threads.
//!
//! "Leaves the entry as it was" is read on the entry's value and its KV
//! revision. Where the gesture changes the thread (a title, a message), the
//! comparison waits first for a positive sign that the relays drained the
//! change — the summary entry showing the title, then the new last activity —
//! and then for the quiet window. A no-op command stages nothing: only the
//! quiet window precedes its comparison. The scenario also reads the manifest
//! `threads.resources_manifest` (`contract-threads` 0.9.0).
//!
//! "The previous version" is the last release, `svc-threads` 0.10.1
//! (`common::previous::last_release_binary`), which publishes no resources.
//! "After any restart the entries match the threads" is read after one
//! restart of this version on the same database: a probe thread Y, created
//! with resources after the restart, is published — the sign that the
//! restarted pod leads the offer and has run its boot reconciliation, which
//! comes before its first drain; awaited past the killed pod's lease — and W's
//! and X's entries then hold the same values at the same revisions.
//!
//! Channels: the answers (4); the resources entries, their revisions and the
//! manifest, the summary entry as a drain signal (4); the thread as its
//! members read it (2); Identity's roster (4, consumed); the readiness of the
//! upgraded and restarted boots.

mod common;

use std::time::Duration;

use common::flows::{command, create, creation, resources_of, say, thread_confirmed, thread_view};
use common::{PUSH_TIMEOUT, TestContext, eventually};
use contract_threads::{
    AddResourceIds, DeleteThread, PublishedThreadResources, RemoveResourceIds, SetTitle,
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

/// Long enough for a restarted pod to take over the offer from a killed one:
/// the engine's default lease (30 s), its beat and a margin.
const LEASE_EXPIRY: Duration = Duration::from_secs(60);

/// The manifest of the family: its prefix and wire version.
fn manifest() -> serde_json::Value {
    json!({ "prefix": "threads.resources.", "version": 1 })
}

/// Waits until T's summary entry satisfies `wanted`: the relays drained the
/// change that moved it.
async fn summary_shows(
    ctx: &TestContext,
    thread: Uuid,
    what: &str,
    wanted: impl Fn(&contract_threads::PublishedThreadSummary) -> bool,
) {
    eventually(what, PUSH_TIMEOUT, || async {
        ctx.summary_entry(thread)
            .await
            .is_some_and(|(summary, _)| wanted(&summary))
    })
    .await;
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

    // When a caller creates T, owned by O with A, with resources
    // {kms: [k1, k2]}.
    let mut born = creation(Some(o.id), &[a.id]);
    born.resources = entry(&[("kms", &["k1", "k2"])]).resources;
    let t = create(&ctx, born).await;

    // Then (4) threads.resources.{T} appears with {kms: [k1, k2]}, as T's
    // members read them (2); the manifest declares the family and its wire
    // version.
    let created = entry(&[("kms", &["k1", "k2"])]);
    ctx.resources_until(t, "T's resources are published", |held| held == &created)
        .await;
    assert_eq!(
        resources_of(&thread_view(&ctx, &a.passport, t).await),
        created.resources
    );
    assert_eq!(ctx.resources_manifest().await, Some(manifest()));

    // When U, owned by P, is created without resources.
    let u = create(&ctx, creation(Some(p.id), &[])).await;

    // Then (4) U's entry holds an empty resources object.
    ctx.resources_until(u, "U's empty resources are published", |held| {
        held == &entry(&[])
    })
    .await;

    // ── d1 added under documents ─────────────────────────────────────────

    // When d1 is added to T under documents.
    let (_, revision_created) = ctx.resources_settled(t).await;
    let added = command(&ctx, &add(t, "documents", &["d1"])).await;

    // Then (4) the entry is rewritten with both keys.
    thread_confirmed(&ctx, added, "adding d1").await;
    let both = entry(&[("documents", &["d1"]), ("kms", &["k1", "k2"])]);
    ctx.resources_until(t, "T's entry holds both keys", |held| held == &both)
        .await;
    let both_held = ctx.resources_settled(t).await;
    assert_eq!(both_held.0, both);
    assert_ne!(both_held.1, revision_created, "the entry is rewritten");

    // ── k1 added again ───────────────────────────────────────────────────

    // When k1 is added again.
    let again = command(&ctx, &add(t, "kms", &["k1"])).await;

    // Then confirmed, and (4) the entry is left as it was: same value, same
    // revision.
    thread_confirmed(&ctx, again, "adding k1 again").await;
    assert_eq!(
        ctx.resources_settled(t).await,
        both_held,
        "adding k1 again leaves the entry as it was"
    );

    // ── kms emptied ──────────────────────────────────────────────────────

    // When k1 and k2 are removed.
    let removed = command(
        &ctx,
        &RemoveResourceIds {
            thread_id: t,
            key: "kms".to_string(),
            ids: ids(&["k1", "k2"]),
            acting_user_id: None,
        },
    )
    .await;

    // Then (4) the emptied kms leaves the entry, which keeps documents only.
    thread_confirmed(&ctx, removed, "emptying kms").await;
    let documents = entry(&[("documents", &["d1"])]);
    ctx.resources_until(t, "T's entry keeps documents only", |held| {
        held == &documents
    })
    .await;
    let documents_held = ctx.resources_settled(t).await;
    assert_eq!(documents_held.0, documents);

    // ── a title change, a stored message ─────────────────────────────────

    // When T's title is changed.
    let titled = command(
        &ctx,
        &SetTitle {
            thread_id: t,
            title: "Q3 review".to_string(),
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, titled, "changing T's title").await;

    // Then, once the change is drained (the summary shows the title), (4)
    // the resources entry is left as it was.
    summary_shows(&ctx, t, "the title is drained", |summary| {
        summary.title.as_deref() == Some("Q3 review")
    })
    .await;
    assert_eq!(
        ctx.resources_settled(t).await,
        documents_held,
        "a title change leaves the entry as it was"
    );

    // When A's message is stored on T.
    let (summary_before, _) = ctx.summary_entry(t).await.expect("T's summary");
    say(&ctx, t, None, &a, "Here are the Q3 figures").await;

    // Then, once the change is drained (the summary's last activity moved),
    // (4) the resources entry is left as it was.
    summary_shows(&ctx, t, "the message is drained", |summary| {
        summary.last_activity_at > summary_before.last_activity_at
    })
    .await;
    assert_eq!(
        ctx.resources_settled(t).await,
        documents_held,
        "a stored message leaves the entry as it was"
    );

    // ── deletions ────────────────────────────────────────────────────────

    // When T is deleted.
    let deleted = command(
        &ctx,
        &DeleteThread {
            thread_id: t,
            acting_user_id: Some(o.id),
        },
    )
    .await;

    // Then (4) its entry is removed.
    thread_confirmed(&ctx, deleted, "deleting T").await;
    eventually("T's resources entry is removed", PUSH_TIMEOUT, || async {
        ctx.resources_entry(t).await.is_none()
    })
    .await;

    // When P, U's owner, leaves the roster.
    ctx.retract(&p).await;

    // Then U is deleted and (4) its entry removed too.
    eventually("U's resources entry is removed", PUSH_TIMEOUT, || async {
        ctx.resources_entry(u).await.is_none()
    })
    .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH, and builds the last release"]
async fn t38_the_threads_of_the_previous_version_get_their_entry_and_keep_it_across_a_restart() {
    // Given the previous version (0.10.1), which publishes no resources,
    // holding W with resources {kms: [k1]} and X without resources.
    let mut ctx = TestContext::setup_last_release().await;
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

    // Then (4) W's entry holds {kms: [k1]}, X's an empty resources object,
    // and the manifest is published.
    ctx.resources_until(w, "W's resources are published", |held| {
        held == &entry(&[("kms", &["k1"])])
    })
    .await;
    ctx.resources_until(x, "X's empty resources are published", |held| {
        held == &entry(&[])
    })
    .await;
    assert_eq!(ctx.resources_manifest().await, Some(manifest()));
    let (w_held, x_held) = (
        ctx.resources_settled(w).await,
        ctx.resources_settled(x).await,
    );

    // When the new version restarts on the same database.
    let restart = ctx.restart_current().await;
    assert!(
        restart.migrated && restart.ready,
        "the new version restarts:\n{}",
        restart.output
    );

    // Then, once the restarted pod leads the offer (a probe thread Y created
    // with resources is published), (4) W's and X's entries still match their
    // threads, at the same revisions.
    // The killed pod's offer leases expire first: the restarted pod leads
    // once the engine's lease (30 s by default) has run out, so the probe is
    // awaited past it rather than within `PUSH_TIMEOUT`.
    let mut probe = creation(Some(o.id), &[]);
    probe.resources = entry(&[("kms", &["y1"])]).resources;
    let y = probe.thread_id;
    let probed = command(&ctx, &probe).await;
    thread_confirmed(&ctx, probed, "creating the probe Y").await;
    eventually("the restarted pod publishes Y", LEASE_EXPIRY, || async {
        ctx.resources_entry(y)
            .await
            .is_some_and(|(held, _)| held == entry(&[("kms", &["y1"])]))
    })
    .await;
    assert_eq!(ctx.resources_settled(w).await, w_held);
    assert_eq!(ctx.resources_settled(x).await, x_held);

    ctx.teardown().await;
}
