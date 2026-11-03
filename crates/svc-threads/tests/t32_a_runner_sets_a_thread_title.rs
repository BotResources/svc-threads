//! **T32 — A runner sets a thread's title.**
//!
//! > Thread T, owned by O with member A, has no title; A follows T and O
//! > follows their list. A threads:runner service account R sets T's title to
//! > "Q3 review": acknowledged; O and A read it, T's version is raised by one,
//! > A's feed and O's list receive T with its title, and a TitleChanged fact
//! > records it, by RUNNER, with R as its actor and no acting user. Setting the
//! > same title again is acknowledged and changes nothing. An empty title is
//! > refused TITLE_EMPTY and one of 201 characters TITLE_TOO_LONG; one of 200
//! > characters is set. Member A and a service account without threads:runner
//! > are refused RUNNER_SCOPE_REQUIRED; an unknown thread is refused
//! > THREAD_NOT_FOUND. No refusal changes the title, the version or the facts,
//! > nor reaches A's feed. A resolved thread's title, then a closed one's, can
//! > still be set by R, as by command; the resolution and the status stay.
//!
//! The registry's 0.7.0 names no scenario for this runner operation (T12, the
//! title Job, is deleted): the oracle proposes this one, to be ratified.
//!
//! Channels: the runner operation's verdict (1); the thread as members read it
//! (2); A's feed and O's list, over SSE (3); the fact log.

mod common;

use br_core_auth::Passport;
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    command, create_thread, resolution_of, thread_confirmed, thread_view, version,
};
use common::{
    M_SET_TITLE, RUNNER_SCOPE_REQUIRED, THREAD_NOT_FOUND, TITLE_EMPTY, TITLE_TOO_LONG, TestContext,
    gql,
};
use contract_threads::{CloseThread, ClosingStatus};
use serde_json::{Value, json};
use uuid::Uuid;

async fn set_title(ctx: &TestContext, passport: &Passport, thread: Uuid, title: &str) -> Value {
    ctx.run(
        passport,
        M_SET_TITLE,
        json!({ "thread": thread, "title": title }),
    )
    .await
}

fn assert_acknowledged(answer: &Value) {
    assert_eq!(
        gql::data(answer, "threadsSetTitle"),
        &json!({ "success": true }),
        "{answer}"
    );
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t32_a_runner_sets_the_title_of_a_thread_its_members_see_it_live() {
    // Given T, owned by O with member A, untitled; A follows T, O their list;
    // R, a runner.
    let ctx = TestContext::setup().await;
    let (o, a) = (ctx.enrol("olga").await, ctx.enrol("ada").await);
    let t = create_thread(&ctx, Some(&o), &[&a]).await;
    let runner = ctx.service_account(true);
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let mut o_list = Live::list(&ctx, &o.passport).await;
    let before = version(&ctx, &a.passport, t).await;
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["title"],
        Value::Null
    );

    // When R sets T's title.
    let set = set_title(&ctx, &runner.passport, t, "Q3 review").await;

    // Then it is acknowledged, the members read it at the next version, ...
    assert_acknowledged(&set);
    for person in [&o, &a] {
        let view = thread_view(&ctx, &person.passport, t).await;
        assert_eq!(view["title"], "Q3 review", "{}", person.label);
        assert_eq!(view["version"].as_i64(), Some(before + 1), "{view}");
    }
    // ... both live channels receive T with its title, ...
    a_feed
        .until("the title reaches A's feed", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["title"] == "Q3 review")
        })
        .await;
    o_list
        .until("the title reaches O's list", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["title"] == "Q3 review")
        })
        .await;
    // ... and a TitleChanged fact records it, by the runner.
    let t_facts = facts::of(&ctx.pool, "thread", t).await;
    let changed = facts::the(&t_facts, "TitleChanged");
    assert_eq!(
        changed.payload,
        json!({ "title": "Q3 review", "by": "RUNNER" })
    );
    assert_eq!(changed.actor_id, runner.id);
    assert_eq!(changed.actor_kind, "service");
    assert_eq!(changed.acting_user_id, None);
    assert_eq!(i64::from(changed.version), before + 1);

    // When R sets the same title again — then it is acknowledged and changes
    // nothing.
    let facts_before = facts::count(&ctx.pool).await;
    assert_acknowledged(&set_title(&ctx, &runner.passport, t, "Q3 review").await);
    assert_eq!(version(&ctx, &a.passport, t).await, before + 1);
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    a_feed.silence("the same title again").await;

    // When R sets a title out of its bounds, or a caller without the runner
    // scope sets one, or R names a thread nobody created — then each is
    // refused with its code ...
    let plain_service = ctx.service_account(false);
    let refusals = [
        (set_title(&ctx, &runner.passport, t, "").await, TITLE_EMPTY),
        (
            set_title(&ctx, &runner.passport, t, &"x".repeat(201)).await,
            TITLE_TOO_LONG,
        ),
        (
            set_title(&ctx, &a.passport, t, "By a member").await,
            RUNNER_SCOPE_REQUIRED,
        ),
        (
            set_title(&ctx, &plain_service.passport, t, "By a stranger").await,
            RUNNER_SCOPE_REQUIRED,
        ),
        (
            set_title(&ctx, &runner.passport, Uuid::now_v7(), "Nowhere").await,
            THREAD_NOT_FOUND,
        ),
    ];
    for (answer, code) in &refusals {
        gql::assert_refused(answer, code);
    }
    // ... and none changes the title, the version, the facts or the feed.
    let view = thread_view(&ctx, &a.passport, t).await;
    assert_eq!(view["title"], "Q3 review");
    assert_eq!(view["version"].as_i64(), Some(before + 1));
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    a_feed.silence("the refused titles").await;

    // When R sets a title of 200 characters — then it is set.
    let longest = "é".repeat(200);
    assert_acknowledged(&set_title(&ctx, &runner.passport, t, &longest).await);
    assert_eq!(thread_view(&ctx, &a.passport, t).await["title"], longest);

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t32_a_resolved_or_closed_thread_title_can_still_be_set_by_a_runner() {
    // Given T, owned by O, resolved by O; R, a runner.
    let ctx = TestContext::setup().await;
    let o = ctx.enrol("olga").await;
    let t = create_thread(&ctx, Some(&o), &[]).await;
    let runner = ctx.service_account(true);
    let resolved = command(&ctx, &resolution_of(t, o.id)).await;
    thread_confirmed(&ctx, resolved, "resolving T").await;

    // When R sets T's title — then it is set, and T stays resolved.
    assert_acknowledged(&set_title(&ctx, &runner.passport, t, "Settled").await);
    let view = thread_view(&ctx, &o.passport, t).await;
    assert_eq!(view["title"], "Settled");
    assert_eq!(view["resolvedBy"], o.id.to_string());
    assert_eq!(view["status"], "OPEN");

    // Given T closed COMPLETED — when R sets its title, then it is set, and T
    // stays COMPLETED and resolved.
    let closed = command(
        &ctx,
        &CloseThread {
            thread_id: t,
            status: ClosingStatus::Completed,
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, closed, "closing T").await;
    assert_acknowledged(&set_title(&ctx, &runner.passport, t, "Settled, closed").await);
    let view = thread_view(&ctx, &o.passport, t).await;
    assert_eq!(view["title"], "Settled, closed");
    assert_eq!(view["status"], "COMPLETED");
    assert_eq!(view["resolvedBy"], o.id.to_string());
    let t_facts = facts::of(&ctx.pool, "thread", t).await;
    assert_eq!(
        facts::types(&t_facts)
            .into_iter()
            .filter(|kind| *kind == "TitleChanged")
            .count(),
        2
    );

    ctx.teardown().await;
}
