//! **T17 — Closing and deleting a thread.**
//!
//! > The harness closes thread T as CANCELLED: confirmed, T reads CANCELLED,
//! > storing a message is refused THREAD_NOT_OPEN, closing it again as
//! > COMPLETED is refused THREAD_NOT_OPEN; members still read it. Thread U,
//! > with a running title Job, messages, an attached artifact, resources and
//! > read marks, is deleted: confirmed; U is answered not found to its former
//! > members, its artifact's bytes are gone, threads.members.{U} is retracted,
//! > job.cancel is published for its title Job, and the Deleted fact says it
//! > was deleted on command. The later job.cancelled changes nothing. Deleting
//! > U again is refused THREAD_NOT_FOUND.
//!
//! Channels: the answers (4), the members entry (4), Jobs' commands and facts
//! (4), the threads as members read them (2), the live feed and list (3), the
//! artifact's read access and the bucket, the fact log.

mod common;

use std::collections::BTreeMap;

use common::artifacts::{File, download_url, fetch, object_count, upload};
use common::content::{ai_text, artifact_block, step, user_text};
use common::facts;
use common::feed::{Live, removes, upsert_of};
use common::flows::{
    ai, answer, command, create, create_thread, creation, say, thread_confirmed, thread_refused,
    thread_view, user_message,
};
use common::{M_SET_READ_MARK, PUSH_TIMEOUT, TestContext, eventually, gql};
use contract_threads::llm::StopReason;
use contract_threads::{CloseThread, ClosingStatus, DeleteThread, ReasonCode};
use serde_json::{Value, json};
use uuid::Uuid;

fn close(thread: Uuid, status: ClosingStatus, acting: Uuid) -> CloseThread {
    CloseThread {
        thread_id: thread,
        status,
        acting_user_id: Some(acting),
    }
}

fn delete(thread: Uuid, acting: Uuid) -> DeleteThread {
    DeleteThread {
        thread_id: thread,
        acting_user_id: Some(acting),
    }
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t17_a_closed_thread_never_changes_status_and_stops_messages() {
    // Given T, OPEN, with owner O and member A, following it.
    let ctx = TestContext::setup().await;
    let (o, a) = (ctx.enrol("olga").await, ctx.enrol("ada").await);
    let t = create_thread(&ctx, Some(&o), &[&a]).await;
    let m1 = say(&ctx, t, None, &a, "before the close").await;
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;

    // When the harness closes T as CANCELLED.
    let closed = command(&ctx, &close(t, ClosingStatus::Cancelled, o.id)).await;

    // Then (4) confirmed; (2) T reads CANCELLED to its members; (3) A's feed
    // carries it; a Cancelled fact records it.
    thread_confirmed(&ctx, closed, "closing T").await;
    for person in [&o, &a] {
        assert_eq!(
            thread_view(&ctx, &person.passport, t).await["status"],
            "CANCELLED"
        );
    }
    a_feed
        .until("A sees T CANCELLED", |delta| {
            upsert_of(delta, t).is_some_and(|view| view["status"] == "CANCELLED")
        })
        .await;
    assert!(facts::types(&facts::of(&ctx.pool, "thread", t).await).contains(&"Cancelled"));

    // When a message is stored on T, then T is closed again as COMPLETED.
    let message = command(
        &ctx,
        &user_message(t, Some(m1), a.id, vec![user_text("late")]),
    )
    .await;
    let reclose = command(&ctx, &close(t, ClosingStatus::Completed, o.id)).await;

    // Then both are refused THREAD_NOT_OPEN, and T still reads CANCELLED, with
    // its message, to its members.
    common::flows::message_refused(&ctx, message, ReasonCode::ThreadNotOpen, "a message").await;
    thread_refused(&ctx, reclose, ReasonCode::ThreadNotOpen, "closing again").await;
    let view = thread_view(&ctx, &a.passport, t).await;
    assert_eq!(view["status"], "CANCELLED");
    assert_eq!(
        common::flows::active_branch(&ctx, &a.passport, t).await,
        vec![m1]
    );

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t17_a_deleted_thread_goes_with_everything_it_holds_its_entry_and_its_title_job() {
    // Given U, owned by O with member A: resources, a first user message (its
    // title Job running), an AI answer attaching artifact X, A's read mark. A
    // follows their list.
    let ctx = TestContext::setup().await;
    let (o, a) = (ctx.enrol("olga").await, ctx.enrol("ada").await);
    let mut born = creation(Some(o.id), &[a.id]);
    born.resources = BTreeMap::from([("documents".to_string(), vec!["d1".to_string()])]);
    let u = create(&ctx, born).await;
    let m1 = say(&ctx, u, None, &a, "summarise this").await;
    let job = ctx
        .jobs
        .await_create(u, "U's title Job")
        .await
        .payload
        .job_id;
    let runner = ctx.service_account(true);
    let file = File::csv("summary\nok\n");
    let x = upload(&ctx, &runner.passport, u, &file).await;
    let m2 = answer(
        &ctx,
        u,
        Some(m1),
        ai("a1", "Analyst"),
        vec![step(
            StopReason::EndTurn,
            None,
            vec![ai_text("done"), artifact_block(x)],
        )],
    )
    .await;
    let marked = ctx
        .run(
            &a.passport,
            M_SET_READ_MARK,
            json!({ "thread": u, "message": m2 }),
        )
        .await;
    assert_eq!(
        gql::data(&marked, "threadsSetReadMark"),
        &json!({ "success": true })
    );
    let x_url = download_url(&ctx, &a.passport, u, x)
        .await
        .expect("A reads X before the deletion");
    let objects = object_count(&ctx).await;
    let mut a_list = Live::list(&ctx, &a.passport).await;

    // When the harness deletes U on O's gesture.
    let deleted = command(&ctx, &delete(u, o.id)).await;

    // Then (4) confirmed.
    thread_confirmed(&ctx, deleted, "deleting U").await;
    // And (2) U is answered not found to its former members.
    for person in [&o, &a] {
        assert_eq!(thread_view(&ctx, &person.passport, u).await, Value::Null);
        assert_eq!(download_url(&ctx, &person.passport, u, x).await, None);
    }
    // And X's bytes are gone: the read access obtained before fails.
    eventually("X's bytes are gone", PUSH_TIMEOUT, || async {
        fetch(&x_url).await.0 != 200 && object_count(&ctx).await == objects - 1
    })
    .await;
    // And (4) threads.members.{U} is retracted.
    eventually("U's entry is retracted", PUSH_TIMEOUT, || async {
        ctx.published_members(u).await.is_none()
    })
    .await;
    // And (4) job.cancel is published for its title Job, once.
    ctx.jobs.await_one_cancel(job, "U's deletion").await;
    // And (3) A's list is told once that U left it.
    a_list
        .until("U leaves A's list", |delta| removes(delta, u))
        .await;
    let rest = a_list.drain().await;
    assert!(
        rest.iter().all(|delta| !removes(delta, u)),
        "told once: {rest:?}"
    );
    // And the Deleted fact records a deletion on command, led by O.
    let u_facts = facts::of(&ctx.pool, "thread", u).await;
    let deleted_fact = facts::the(&u_facts, "Deleted");
    assert_eq!(deleted_fact.acting_user_id, Some(o.id));
    assert_eq!(deleted_fact.actor_kind, "service");
    assert_eq!(
        deleted_fact.actor_id,
        common::commands::commanding_service().id()
    );
    // And nothing of U is left: members, resources, messages, read marks.
    for table in [
        "threads.thread_member",
        "threads.thread_resource",
        "threads.message",
        "threads.read_mark",
        "threads.artifact",
    ] {
        let rows: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {table} WHERE thread_id = $1"
        ))
        .bind(u)
        .fetch_one(&ctx.pool)
        .await
        .expect("count U's rows");
        assert_eq!(rows, 0, "{table} holds nothing of U");
    }

    // When Jobs then confirms the cancellation.
    let facts_before = facts::count(&ctx.pool).await;
    let commands_before = ctx.jobs.total();
    let events_before = ctx.nats.event_stream_len().await;
    ctx.jobs.cancelled(&ctx.nats, job).await;

    // Then it changes nothing: no fact, no command, no event of Threads'.
    ctx.jobs
        .expect_nothing_beyond(commands_before, "the later job.cancelled")
        .await;
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    assert_eq!(
        ctx.nats.event_stream_len().await,
        events_before + 1,
        "only Jobs' own fact is on the event stream"
    );

    // When U is deleted again.
    let again = command(&ctx, &delete(u, o.id)).await;
    // Then refused THREAD_NOT_FOUND.
    thread_refused(&ctx, again, ReasonCode::ThreadNotFound, "deleting U again").await;

    ctx.teardown().await;
}
