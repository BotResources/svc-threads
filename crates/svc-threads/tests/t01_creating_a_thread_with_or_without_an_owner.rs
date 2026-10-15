//! **T01 — Creating a thread, with or without an owner.**
//!
//! > Playing a commanding service, the harness publishes thread.create with a
//! > client-minted UUIDv7, owner O and members A and B: thread.confirmed names
//! > the command id; the thread is OPEN at version 1 with members O, A and B,
//! > and threads.members.{id} holds owner_id O and users [O, A, B]; the Created
//! > and MembersAdded facts carry the acting user. A second thread is created
//! > with members A and B and no owner: confirmed, its KV entry carries no
//! > owner_id. Replaying the first command (same envelope id) re-publishes the
//! > same confirmation and changes nothing. A new command reusing the thread id
//! > is refused KEY_REUSED. A creation naming a user absent from the roster, or
//! > a user the roster marks out, as owner or as member is refused USER_UNKNOWN
//! > and creates nothing, no KV entry included. A title of 201 characters is
//! > refused; one of 200 is kept. A creation carrying resources {kms: [k1, k2]}
//! > shows them on the thread.
//!
//! Channels: the one answer per command (4), the members entry (4), the fact
//! log, the thread as each member reads it (2), and A's live list (3).

mod common;

use std::collections::BTreeMap;

use br_core_integration::{Actor, UserId};
use br_util_nats_fabric::KvKey;
use common::commands::{Answer, commanding_service, send};
use common::facts;
use common::feed::{Live, upsert_of, views};
use common::flows::{
    answered_once, await_members, command, create, creation, member_ids, resources_of, sorted,
    thread_confirmed, thread_refused, thread_view, version,
};
use common::{TestContext, identity};
use contract_threads::{ReasonCode, ThreadCommandConfirmed};
use serde_json::Value;
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t01_a_thread_is_created_with_or_without_an_owner_each_command_answered_once() {
    // Given O, A and B in the roster, and A following their list.
    let ctx = TestContext::setup().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let mut a_list = Live::list(&ctx, &a.passport).await;
    assert!(views(&a_list.reset).is_empty(), "A's list starts empty");

    // When the commanding service creates T, owned by O, with members A and B,
    // on O's gesture.
    let first = creation(Some(o.id), &[a.id, b.id]);
    let t = first.thread_id;
    let first_command = command(&ctx, &first).await;

    // Then (4) thread.confirmed names the command, once.
    let confirmed = thread_confirmed(&ctx, first_command, "the creation of T").await;
    assert_eq!(
        confirmed,
        ThreadCommandConfirmed {
            command_id: first_command,
            command_type: "thread.create".to_string(),
            thread_id: t,
        }
    );
    answered_once(&ctx, first_command, "the creation of T").await;

    // And (4) threads.members.{T} holds owner O and users O, A and B.
    let members = await_members(&ctx, t, "T's members entry").await;
    assert_eq!(members.owner_id, Some(o.id));
    assert_eq!(sorted(&members.users), sorted(&[o.id, a.id, b.id]));

    // And (2) every member reads T OPEN at version 1, owned by O, with its
    // three members under the roster's names.
    for person in [&o, &a, &b] {
        let view = thread_view(&ctx, &person.passport, t).await;
        assert_eq!(view["id"], Value::String(t.to_string()), "{view}");
        assert_eq!(view["status"], "OPEN", "{view}");
        assert_eq!(view["version"], 1, "{view}");
        assert_eq!(view["ownerId"], Value::String(o.id.to_string()), "{view}");
        assert_eq!(view["title"], Value::Null, "{view}");
        assert_eq!(view["parentThreadId"], Value::Null, "{view}");
        assert_eq!(member_ids(&view), sorted(&[o.id, a.id, b.id]));
        for member in view["members"].as_array().expect("members") {
            let label = [&o, &a, &b]
                .into_iter()
                .find(|p| member["userId"] == Value::String(p.id.to_string()))
                .expect("a known member")
                .label
                .clone();
            assert_eq!(member["displayName"], identity::display_name(&label));
            assert_eq!(member["inRoster"], true);
        }
    }

    // And (3) A's live list receives T.
    a_list
        .until("A's list receives T", |delta| upsert_of(delta, t).is_some())
        .await;

    // And the Created and MembersAdded facts carry the acting user O, sent by
    // the commanding service, and no name.
    let t_facts = facts::of(&ctx.pool, "thread", t).await;
    for event_type in ["Created", "MembersAdded"] {
        let fact = facts::the(&t_facts, event_type);
        assert_eq!(fact.acting_user_id, Some(o.id), "{fact:?}");
        assert_eq!(fact.actor_kind, "service", "{fact:?}");
        assert_eq!(
            fact.actor_id,
            commanding_service().id(),
            "the commanding service's account sent it: {fact:?}"
        );
    }
    for person in [&o, &a, &b] {
        facts::assert_no_fact_carries(&ctx.pool, &identity::display_name(&person.label), "a name")
            .await;
    }

    // When a second thread is created with members A and B and no owner.
    let ownerless = creation(None, &[a.id, b.id]);
    let u = ownerless.thread_id;
    let ownerless_command = command(&ctx, &ownerless).await;

    // Then it is confirmed, and its members entry carries no owner_id at all.
    thread_confirmed(&ctx, ownerless_command, "the ownerless creation").await;
    let members = await_members(&ctx, u, "U's members entry").await;
    assert_eq!(members.owner_id, None);
    assert_eq!(sorted(&members.users), sorted(&[a.id, b.id]));
    let raw = ctx
        .nats
        .pl_get_raw(&KvKey::new(contract_threads::members::members_key(u)).expect("a key"))
        .await
        .expect("U's entry");
    let raw: Value = serde_json::from_slice(&raw).expect("a JSON entry");
    assert!(
        raw.get("owner_id").is_none(),
        "an ownerless entry carries no owner_id: {raw}"
    );
    assert_eq!(
        thread_view(&ctx, &a.passport, u).await["ownerId"],
        Value::Null
    );

    // When the bus redelivers the first command, under its own envelope id.
    let facts_before = facts::count(&ctx.pool).await;
    let kv_before = ctx.members_entry(t).await.expect("T's entry").1;
    common::flows::redeliver(&ctx, first_command, &first).await;

    // Then nothing changes — no fact, no version, no rewrite of the members
    // entry — and no second distinct answer is published.
    common::flows::replayed_alike(&ctx, first_command, "the redelivered creation").await;
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    assert_eq!(version(&ctx, &a.passport, t).await, 1);
    let kv_after = ctx.members_entry(t).await.expect("T's entry").1;
    assert_eq!(kv_after, kv_before, "the replay rewrites no members entry");

    // When a NEW command reuses T's id.
    let mut reuse = creation(Some(a.id), &[b.id]);
    reuse.thread_id = t;
    let reuse_command = command(&ctx, &reuse).await;

    // Then it is refused KEY_REUSED, and T is as it was.
    thread_refused(&ctx, reuse_command, ReasonCode::KeyReused, "reusing T's id").await;
    assert_eq!(
        thread_view(&ctx, &o.passport, t).await["ownerId"],
        Value::String(o.id.to_string())
    );
    assert_eq!(facts::count(&ctx.pool).await, facts_before);

    // When a person — not a service account — sends a creation on the bus.
    let by_hand = creation(Some(a.id), &[]);
    let by_hand_thread = by_hand.thread_id;
    let by_hand_command = send(&ctx.nats, Actor::Human(UserId::from(a.id)), &by_hand).await;

    // Then it is never confirmed and creates nothing: commands are accepted
    // from service accounts only.
    tokio::time::sleep(common::QUIET_WINDOW).await;
    assert!(
        ctx.answers
            .for_command(by_hand_command)
            .iter()
            .all(|answer| !matches!(answer, Answer::ThreadConfirmed(_))),
        "a person's command is never confirmed"
    );
    assert_eq!(
        thread_view(&ctx, &a.passport, by_hand_thread).await,
        Value::Null
    );
    assert!(ctx.published_members(by_hand_thread).await.is_none());
    assert!(
        facts::of(&ctx.pool, "thread", by_hand_thread)
            .await
            .is_empty()
    );

    // When creations name a user absent from the roster, or one the roster
    // marks out — as owner, or as member.
    let never = Uuid::now_v7();
    let gone = ctx.enrol("gina").await;
    ctx.retract(&gone).await;
    for (owner, members, what) in [
        (Some(never), vec![a.id], "an absent owner"),
        (Some(gone.id), vec![a.id], "an out-of-roster owner"),
        (Some(o.id), vec![never], "an absent member"),
        (None, vec![a.id, gone.id], "an out-of-roster member"),
    ] {
        let refused = creation(owner, &members);
        let refused_thread = refused.thread_id;
        let refused_command = command(&ctx, &refused).await;

        // Then each is refused USER_UNKNOWN and creates nothing: no thread, no
        // members entry, no fact.
        thread_refused(&ctx, refused_command, ReasonCode::UserUnknown, what).await;
        assert_eq!(
            thread_view(&ctx, &a.passport, refused_thread).await,
            Value::Null,
            "{what}: no thread"
        );
        assert!(
            ctx.published_members(refused_thread).await.is_none(),
            "{what}: no members entry"
        );
        assert!(
            facts::of(&ctx.pool, "thread", refused_thread)
                .await
                .is_empty(),
            "{what}: no fact"
        );
    }

    // When a creation carries a title of 201 characters.
    let mut too_long = creation(Some(o.id), &[]);
    too_long.title = Some("é".repeat(201));
    let too_long_thread = too_long.thread_id;
    let too_long_command = command(&ctx, &too_long).await;

    // Then it is refused TITLE_TOO_LONG and creates nothing.
    thread_refused(
        &ctx,
        too_long_command,
        ReasonCode::TitleTooLong,
        "a 201-character title",
    )
    .await;
    assert_eq!(
        thread_view(&ctx, &o.passport, too_long_thread).await,
        Value::Null
    );

    // When one carries a title of 200 characters and resources {kms: [k1, k2]}.
    let mut titled = creation(Some(o.id), &[a.id]);
    titled.title = Some("é".repeat(200));
    titled.resources =
        BTreeMap::from([("kms".to_string(), vec!["k1".to_string(), "k2".to_string()])]);
    let titled_thread = create(&ctx, titled).await;

    // Then the title is kept whole and the resources show on the thread.
    let view = thread_view(&ctx, &a.passport, titled_thread).await;
    assert_eq!(view["title"], Value::String("é".repeat(200)));
    assert_eq!(
        resources_of(&view),
        BTreeMap::from([("kms".to_string(), vec!["k1".to_string(), "k2".to_string()])])
    );

    ctx.teardown().await;
}
