//! **T14 — The owner leaves the roster.**
//!
//! > User O owns thread T1 and is a member of ownerless thread T2 and of T3
//! > owned by P. O is deactivated in Identity: O is refused everything, T1
//! > stays and its members keep reading and writing. The harness, playing
//! > Identity, retracts O's roster entry: T1 is deleted with its messages,
//! > artifacts and read marks, its Deleted fact says the owner left the roster,
//! > and threads.members.{T1} is retracted; T2 and T3 stay, O still listed
//! > among their members with O's name on O's messages. Naming O in a new
//! > command is refused USER_UNKNOWN. O's entry is written back: O reads T2
//! > and T3 again. Threads is stopped, O2, the owner of T4, is retracted while
//! > it is down, Threads restarts: T4 is deleted at reconciliation.
//!
//! "Says the owner left the roster": the specification names no field, so the
//! scenario asserts that the Deleted fact of a departure and the Deleted fact
//! of a command deletion differ once their thread ids are blanked out — the
//! cause is told apart, whatever its field is called. T1's running title Job
//! is cancelled with it (specification, § Title Job: deleting a thread cancels
//! it).
//!
//! Channels: Identity's roster (4, consumed); the members entries (4); Jobs'
//! commands (4); the answers (4); the threads and messages as members read
//! them (2); A's live list (3); the artifact's read access; the fact log.

mod common;

use common::artifacts::{File, download_url, fetch, upload};
use common::content::{ai_text, artifact_block, step};
use common::facts;
use common::feed::{Live, removes};
use common::flows::{
    ai, answer, command, create_thread, mask_uuids, member_ids, message_view, say, sorted,
    thread_confirmed, thread_refused, thread_view,
};
use common::{
    ACTIVE_USER_REQUIRED, Handshake, M_SET_READ_MARK, Q_THREADS, TestContext, gql, identity,
};
use contract_threads::llm::StopReason;
use contract_threads::{AddMembers, DeleteThread, ReasonCode};
use serde_json::{Value, json};

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t14_an_owner_leaving_the_roster_takes_their_threads_and_only_theirs() {
    // Given O owning T1 (with A), member of ownerless T2 and of T3 owned by P.
    // T1 holds O's message, an attached artifact and A's read mark, and its
    // title Job runs. A follows their list.
    let ctx = TestContext::setup().await;
    let (o, a, p) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("pia").await,
    );
    let t1 = create_thread(&ctx, Some(&o), &[&a]).await;
    let t2 = create_thread(&ctx, None, &[&o, &a]).await;
    let t3 = create_thread(&ctx, Some(&p), &[&o]).await;
    let o_on_t1 = say(&ctx, t1, None, &o, "my thread").await;
    let t1_job = ctx
        .jobs
        .await_create(t1, "T1's title Job")
        .await
        .payload
        .job_id;
    let runner = ctx.service_account(true);
    let file = File::csv("owner\nO\n");
    let x = upload(&ctx, &runner.passport, t1, &file).await;
    answer(
        &ctx,
        t1,
        Some(o_on_t1),
        ai("a1", "Analyst"),
        vec![step(
            StopReason::EndTurn,
            None,
            vec![ai_text("attached"), artifact_block(x)],
        )],
    )
    .await;
    let x_url = download_url(&ctx, &a.passport, t1, x)
        .await
        .expect("A reads T1's artifact");
    let marked = ctx
        .run(
            &a.passport,
            M_SET_READ_MARK,
            json!({ "thread": t1, "message": o_on_t1 }),
        )
        .await;
    assert_eq!(
        gql::data(&marked, "threadsSetReadMark"),
        &json!({ "success": true })
    );
    let o_on_t2 = say(&ctx, t2, None, &o, "on the shared thread").await;
    let mut a_list = Live::list(&ctx, &a.passport).await;
    let mut o_feed_sse = Live::thread(&ctx, &o.passport, t2).await;
    let mut o_feed_ws = Live::thread_ws(&ctx, &o.passport, t2).await;

    // When Identity deactivates O.
    ctx.deactivate(&o).await;

    // Then (3) O's open live views of T2, over both transports, are revoked —
    // T2 removed, or the stream ended — and nothing of T2 reaches O after.
    for (feed, transport) in [(&mut o_feed_sse, "SSE"), (&mut o_feed_ws, "WS")] {
        feed.until_revoked(&format!("O loses T2 over {transport}"), t2)
            .await;
    }
    say(&ctx, t2, Some(o_on_t2), &a, "while O is away").await;
    for (feed, transport) in [(&mut o_feed_sse, "SSE"), (&mut o_feed_ws, "WS")] {
        feed.silent_or_ended(&format!("a deactivated member's {transport} view"))
            .await;
    }

    // Then O is refused everything…
    gql::assert_refused(
        &ctx.run(&o.passport, Q_THREADS, json!({ "size": 10 })).await,
        ACTIVE_USER_REQUIRED,
    );
    // …and T1 stays: A reads it and writes on it.
    assert!(thread_view(&ctx, &a.passport, t1).await.is_object());
    say(&ctx, t1, Some(o_on_t1), &a, "still here").await;
    assert!(ctx.published_members(t1).await.is_some());

    // When Identity retracts O's roster entry.
    ctx.retract(&o).await;

    // Then T1 is deleted: A gets not found, its members entry is retracted.
    common::eventually("T1 is deleted", common::PUSH_TIMEOUT, || async {
        thread_view(&ctx, &a.passport, t1).await.is_null()
    })
    .await;
    common::eventually("T1's entry is retracted", common::PUSH_TIMEOUT, || async {
        ctx.published_members(t1).await.is_none()
    })
    .await;
    // And its messages and read marks went with it, and its artifact's bytes.
    for table in ["threads.message", "threads.read_mark", "threads.artifact"] {
        let rows: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {table} WHERE thread_id = $1"
        ))
        .bind(t1)
        .fetch_one(&ctx.pool)
        .await
        .expect("count T1's rows");
        assert_eq!(rows, 0, "{table} holds nothing of T1");
    }
    common::eventually(
        "T1's artifact bytes are gone",
        common::PUSH_TIMEOUT,
        || async { fetch(&x_url).await.0 != 200 },
    )
    .await;
    // And T1's running title Job is cancelled.
    ctx.jobs.await_one_cancel(t1_job, "T1's deletion").await;
    // And (3) A's list is told once that T1 left it.
    a_list
        .until("T1 leaves A's list", |delta| removes(delta, t1))
        .await;
    let rest = a_list.drain().await;
    assert!(
        rest.iter().all(|delta| !removes(delta, t1)),
        "told once: {rest:?}"
    );

    // And T2 and T3 stay, O listed among their members — out of the roster,
    // under their name — and named on their messages.
    // A reads T2 (a member); P reads T3 (its owner).
    for (thread, reader) in [(t2, &a), (t3, &p)] {
        let view = thread_view(&ctx, &reader.passport, thread).await;
        assert!(member_ids(&view).contains(&o.id), "{view}");
        let o_member = view["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|member| member["userId"] == Value::String(o.id.to_string()))
            .cloned()
            .expect("O listed");
        assert_eq!(o_member["inRoster"], false);
        assert_eq!(o_member["displayName"], identity::display_name("olga"));
        assert!(ctx.published_members(thread).await.is_some());
    }
    assert_eq!(
        message_view(&ctx, &a.passport, t2, o_on_t2).await["author"]["name"],
        identity::display_name("olga")
    );

    // When a command names O.
    let naming_o = command(
        &ctx,
        &AddMembers {
            thread_id: t3,
            user_ids: vec![o.id],
            acting_user_id: Some(p.id),
        },
    )
    .await;
    // Then refused USER_UNKNOWN.
    thread_refused(&ctx, naming_o, ReasonCode::UserUnknown, "naming O").await;

    // And T1's Deleted fact was not sent by the commanding service.
    let departure = facts::of(&ctx.pool, "thread", t1).await;
    assert_ne!(
        facts::the(&departure, "Deleted").actor_id,
        common::commands::commanding_service().id()
    );

    // Given two threads of the same shape — an owner and one member, nothing
    // else: TQ owned by Q, TC owned by P.
    let q = ctx.enrol("quin").await;
    let tq = create_thread(&ctx, Some(&q), &[&p]).await;
    let tc = create_thread(&ctx, Some(&p), &[&q]).await;

    // When TC is deleted on command and Q, TQ's owner, leaves the roster.
    let deleted = command(
        &ctx,
        &DeleteThread {
            thread_id: tc,
            acting_user_id: Some(p.id),
        },
    )
    .await;
    thread_confirmed(&ctx, deleted, "a deletion on command").await;
    ctx.retract(&q).await;
    common::eventually("TQ is deleted", common::PUSH_TIMEOUT, || async {
        ctx.published_members(tq).await.is_none()
    })
    .await;

    // Then the two Deleted facts, every id masked, still differ: each says why
    // the thread was deleted, in a field of its own.
    let on_command = facts::of(&ctx.pool, "thread", tc).await;
    let on_command = facts::the(&on_command, "Deleted");
    let on_departure = facts::of(&ctx.pool, "thread", tq).await;
    let on_departure = facts::the(&on_departure, "Deleted");
    assert_ne!(
        mask_uuids(&on_command.payload.to_string()),
        mask_uuids(&on_departure.payload.to_string()),
        "the Deleted fact says why the thread was deleted"
    );
    let reason = |payload: &Value| {
        payload["reason"]
            .as_str()
            .unwrap_or_else(|| panic!("a Deleted fact names its reason: {payload}"))
            .to_string()
    };
    assert_ne!(reason(&on_command.payload), reason(&on_departure.payload));

    // When Identity writes O's entry back.
    identity::publish_user(&ctx.nats, o.id, &identity::roster_entry("olga", None)).await;
    ctx.await_known_user(o.id, "olga").await;

    // Then O reads T2 and T3 again — T1 is gone for good.
    assert!(thread_view(&ctx, &o.passport, t2).await.is_object());
    assert!(thread_view(&ctx, &o.passport, t3).await.is_object());
    assert_eq!(thread_view(&ctx, &o.passport, t1).await, Value::Null);
    assert_eq!(
        sorted(&common::flows::listed(&ctx, &o.passport).await),
        sorted(&[t2, t3])
    );

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t14_an_owner_retracted_while_threads_is_down_loses_their_threads_at_reconciliation() {
    // Given O2 owning T4, with member A.
    let mut ctx = TestContext::setup().await;
    let (o2, a) = (ctx.enrol("otto").await, ctx.enrol("ada").await);
    let t4 = create_thread(&ctx, Some(&o2), &[&a]).await;
    let kept = create_thread(&ctx, None, &[&o2, &a]).await;

    // When Threads is stopped, O2 is retracted meanwhile, and Threads restarts.
    ctx.stop().await;
    identity::retract_user(&ctx.nats, o2.id).await;
    ctx.start_again(Handshake::Accept).await;

    // Then T4 is deleted at reconciliation: not found, its entry retracted.
    ctx.await_out_of_roster(o2.id).await;
    common::eventually(
        "T4 is deleted at reconciliation",
        common::PUSH_TIMEOUT,
        || async {
            thread_view(&ctx, &a.passport, t4).await.is_null()
                && ctx.published_members(t4).await.is_none()
        },
    )
    .await;
    assert!(facts::types(&facts::of(&ctx.pool, "thread", t4).await).contains(&"Deleted"));
    // And the ownerless thread stays.
    assert!(thread_view(&ctx, &a.passport, kept).await.is_object());

    ctx.teardown().await;
}
