//! **T39 — Erasing the end of a thread.**
//!
//! > Thread T holds, in order: user message M1 by A; AI answer M2 of agent a1;
//! > AI answer M3 of agent a2, carrying an attached artifact X; user message M4
//! > by B; AI answer M5 of a1, STREAMING with a text half produced. Member B
//! > has a read mark on M4. The harness sends message.erase naming M2 with
//! > including false: confirmed; M3, M4 and M5 are gone, M1 and M2 stay; a
//! > member following T is told of the three removals; X's read access no
//! > longer returns its bytes; B has no read mark; a MessagesErased fact names
//! > M2, including false and 3, and no fact carries content; no entry or block
//! > of M3, M4 or M5 is left in the store. A chunk for M5 and its completion
//! > are refused MESSAGE_NOT_FOUND. A runner's context holds M1 and M2. The
//! > same erase sent under a new command id is confirmed and changes nothing.
//! > Erase naming M2 with including true: confirmed; only M1 stays, and a
//! > message stored next takes its place after M1. Erase naming M4: refused
//! > MESSAGE_NOT_FOUND. Erase naming a message of another thread: refused
//! > MESSAGE_NOT_FOUND. On a resolved thread: refused THREAD_RESOLVED, nothing
//! > changes; on a closed thread THREAD_NOT_OPEN; on an unknown thread
//! > THREAD_NOT_FOUND. A child thread started from a block of M3 before the
//! > erase is still read by its members.
//!
//! The command is `integration.cmd.threads.message.erase.v1` (`thread_id`,
//! `message_id`, `including`, `acting_user_id`), built in the suite
//! (`common::wire`); its answer is `message.confirmed` naming the command, T
//! and the message it named — which status it carries is left unasserted, the
//! registry does not say. The `MessagesErased` fact is read under the payload
//! keys `message_id`, `including` and `count` (an assumption of the oracle:
//! the registry names the three values, not their keys).
//!
//! "X's read access no longer returns its bytes" is read twice: a read
//! access asked for afterwards is "not found", and the one obtained before
//! the erase fetches nothing; the bucket holds one object less, and X's facts
//! end on `Removed` (the specification: an artifact is removed, bytes
//! included, when its message is erased).
//!
//! "No entry or block of M3, M4 or M5 is left in the store" is read over every
//! table of the store (`common::store`): no row holds their texts, and no row
//! but the fact log's names one of their entries, blocks or annotations. The
//! one exception is the block of M3 the child thread C was started from: C
//! names it as its origin, by design (the origin then leads nowhere), so the
//! rows naming it are C's own and nothing else.
//!
//! "A chunk for M5 is refused": a chunk has no answer, so the refusal is read
//! as nothing happening — no message back in the list, no fact, nothing
//! pushed. "Nothing changes" (the erase sent again, the refusals) is read on
//! the store's `threads` schema, row for row, on the fact log and on the live
//! feeds.
//!
//! The refusals follow the order the specification fixes for an erase:
//! THREAD_NOT_FOUND, THREAD_NOT_OPEN, THREAD_RESOLVED, MESSAGE_NOT_FOUND — a
//! resolved or closed thread is refused as such even when the message named is
//! not one of its.
//!
//! An erasure does not move the thread's activity (the specification): its
//! `lastActivityAt` and its published summary keep the date they had.
//!
//! Channels: the answers (4); the list as members read it, read marks, the
//! thread view (2); A's feed over SSE and O's over WebSocket (3); the fact
//! log, the store and the bucket; the runner's context; the summary entry.

mod common;

use std::collections::BTreeSet;

use common::artifacts::{File, download_url, fetch, object_count, upload};
use common::content::{
    Streamed, after, ai_turn, artifact, closed, draft_entry, draft_text, entries_of, human, reply,
    text, to_json,
};
use common::feed::{Live, removes, upsert_of};
use common::flows::{
    ai, answer, command, completion, context, conversation_of, create, create_thread, creation,
    erase_confirmed, erasure_of, message_ids, message_refused, message_view, open_stream, say,
    store, thread_confirmed, thread_view, turn_ids, user_entry_message, whole_message,
};
use common::wire::EraseMessages;
use common::{
    M_SET_READ_MARK, PUSH_TIMEOUT, TestContext, eventually, facts, gql, store as stored, stream,
};
use contract_threads::llm::{DeltaOp, Turn};
use contract_threads::{CloseThread, ClosingStatus, MessageStatus, ReasonCode};
use serde_json::{Value, json};
use uuid::Uuid;

/// Every id a message view holds: its entries', their blocks' (children
/// included) and its annotations'.
fn ids_held(view: &Value) -> BTreeSet<Uuid> {
    fn walk(value: &Value, ids: &mut BTreeSet<Uuid>) {
        match value {
            Value::Object(map) => {
                for (key, inner) in map {
                    if key == "id"
                        && let Some(id) = inner.as_str().and_then(|s| Uuid::parse_str(s).ok())
                    {
                        ids.insert(id);
                    }
                    walk(inner, ids);
                }
            }
            Value::Array(items) => items.iter().for_each(|item| walk(item, ids)),
            _ => {}
        }
    }
    let mut ids = BTreeSet::new();
    walk(&view["entries"], &mut ids);
    ids
}

/// Sends `erase`, refused with `code`, and nothing changes: not a row of the
/// store, not a fact, not T's list.
async fn refused_changing_nothing(
    ctx: &TestContext,
    reader: &common::Person,
    thread: Uuid,
    erase: &EraseMessages,
    code: ReasonCode,
    what: &str,
) {
    let listed = message_ids(ctx, &reader.passport, thread).await;
    let facts_before = facts::count(&ctx.pool).await;
    let snapshot = stored::snapshot(&ctx.pool).await;
    let sent = command(ctx, erase).await;
    message_refused(ctx, sent, code, what).await;
    assert_eq!(
        message_ids(ctx, &reader.passport, thread).await,
        listed,
        "{what}"
    );
    assert_eq!(
        facts::count(&ctx.pool).await,
        facts_before,
        "{what}: no fact"
    );
    assert_eq!(
        stored::snapshot(&ctx.pool).await,
        snapshot,
        "{what}: the store is untouched"
    );
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t39_the_erase_command_removes_every_message_after_the_named_one_with_its_content_and_artifacts()
 {
    // Given T with owner O and members A and B, a runner, agents a1 and a2.
    let ctx = TestContext::setup().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b]).await;
    let runner = ctx.service_account(true);
    let (a1, a2) = (ai("a1", "Analyst"), ai("a2", "Critic"));
    let file = File::csv("erased,1\n");
    let x = upload(&ctx, &runner.passport, t, &file).await;
    const M3_TEXT: &str = "the critic's answer goes";
    const M4_TEXT: &str = "bob's follow-up goes";
    const M5_TEXT: &str = "a half produced answer goes";

    // And T holds, in order: M1 by A; M2 of a1; M3 of a2 carrying X; M4 by B;
    // M5 of a1, STREAMING with a text half produced.
    let m1 = say(&ctx, t, &a, "What were the Q3 figures?").await;
    let m2 = answer(&ctx, t, a1.clone(), reply("Q3 was 42.")).await;
    let m3_reply = reply(M3_TEXT);
    let m3 = Uuid::now_v7();
    let m3_turn = Turn::from(ai_turn(m3, &a2, closed(m3_reply.clone())));
    store(
        &ctx,
        whole_message(t, m3, &a2, &m3_turn, vec![after(&m3_reply[0], artifact(x))]),
        MessageStatus::Completed,
    )
    .await;
    let m4_entry = human(b.id, vec![text(M4_TEXT)]);
    let m4 = store(
        &ctx,
        user_entry_message(t, b.id, &m4_entry),
        MessageStatus::Completed,
    )
    .await;
    let m5 = open_stream(&ctx, t, a1.clone()).await;
    let mut half = Streamed::start(m5, None);
    let m5_block = half.text(&[M5_TEXT], false);
    let (m5_last_seq, _) = stream::push(&ctx.nats.url(), m5, 0, &half.chunks()).await;
    common::flows::await_message(&ctx, &a.passport, t, m5, "M5's half text folded", |view| {
        entries_of(view).first().is_some_and(|entry| {
            entry["state"] == "PRODUCING" && draft_text(&draft_entry(entry)) == M5_TEXT
        })
    })
    .await;
    assert_eq!(
        message_ids(&ctx, &a.passport, t).await,
        vec![m1, m2, m3, m4, m5]
    );

    // And B's read mark on M4.
    let marked = ctx
        .run(
            &b.passport,
            M_SET_READ_MARK,
            json!({ "thread": t, "message": m4 }),
        )
        .await;
    assert_eq!(
        gql::data(&marked, "threadsSetReadMark"),
        &json!({ "success": true })
    );
    assert_eq!(
        thread_view(&ctx, &b.passport, t).await["readMark"],
        json!(m4)
    );

    // And child thread C, owned by B, started from the text block of M3.
    let origin = first_block_of(&m3_reply);
    let mut child = creation(Some(b.id), &[]);
    child.parent_thread_id = Some(t);
    child.origin_block_id = Some(origin);
    let c = create(&ctx, child).await;

    // And what the erase must remove, as members read it before: every id
    // M3, M4 and M5 hold, X's read access, the bucket's objects.
    let mut erased_ids = BTreeSet::new();
    for message in [m3, m4, m5] {
        erased_ids.extend(ids_held(&message_view(&ctx, &a.passport, t, message).await));
    }
    assert!(erased_ids.contains(&origin));
    erased_ids.remove(&origin);
    // (A message's own id may stay named by the removal its followers were
    // told of; what must go is what it held.)
    for message in [m3, m4, m5] {
        erased_ids.remove(&message);
    }
    let x_url = download_url(&ctx, &b.passport, t, x)
        .await
        .expect("a member reads X before the erase");
    assert_eq!(fetch(&x_url).await, (200, file.bytes.clone()));
    let objects_before = object_count(&ctx).await;
    let activity = thread_view(&ctx, &a.passport, t).await["lastActivityAt"].clone();
    // And A follows T over SSE, O over WebSocket.
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let mut o_feed = Live::thread_ws(&ctx, &o.passport, t).await;

    // When the harness erases after M2, M2 itself kept.
    let erase = erasure_of(t, m2, false, Some(a.id));
    let sent = command(&ctx, &erase).await;

    // Then (4) confirmed.
    erase_confirmed(&ctx, sent, &erase, "erasing after M2").await;
    // And (2) M3, M4 and M5 are gone, M1 and M2 stay — for every member.
    for person in [&o, &a, &b] {
        assert_eq!(
            message_ids(&ctx, &person.passport, t).await,
            vec![m1, m2],
            "the list as {} reads it",
            person.label
        );
    }
    // And (3) a member following T is told of the three removals, over either
    // transport, and nothing of them is pushed again.
    for (feed, who) in [
        (&mut a_feed, "A over SSE"),
        (&mut o_feed, "O over WebSocket"),
    ] {
        let seen = feed
            .collect_until(&format!("{who} is told of the three removals"), |seen| {
                [m3, m4, m5]
                    .iter()
                    .all(|id| seen.iter().any(|delta| removes(delta, *id)))
            })
            .await;
        let rest = feed.drain().await;
        for delta in seen.iter().chain(&rest) {
            for id in [m3, m4, m5] {
                assert!(
                    upsert_of(delta, id).is_none(),
                    "{who}: an erased message is never pushed again: {delta}"
                );
            }
        }
    }
    // And X's read access no longer returns its bytes: none is given, and —
    // as for a deleted thread (T17), the bytes leaving the store just after
    // the commit — the one given before fetches nothing and the bucket holds
    // one object less; X's facts end on Removed.
    assert_eq!(download_url(&ctx, &b.passport, t, x).await, None);
    eventually("X's bytes are gone", PUSH_TIMEOUT, || async {
        fetch(&x_url).await.0 != 200 && object_count(&ctx).await == objects_before - 1
    })
    .await;
    eventually("X is removed", PUSH_TIMEOUT, || async {
        facts::types(&facts::of(&ctx.pool, "artifact", x).await).last() == Some(&"Removed")
    })
    .await;
    // And B has no read mark.
    assert_eq!(
        thread_view(&ctx, &b.passport, t).await["readMark"],
        Value::Null
    );
    // And a MessagesErased fact names M2, including false and 3.
    let t_facts = facts::of(&ctx.pool, "thread", t).await;
    let erased = facts::the(&t_facts, "MessagesErased");
    assert_eq!(
        erased.payload["message_id"],
        json!(m2),
        "{:?}",
        erased.payload
    );
    assert_eq!(
        erased.payload["including"],
        json!(false),
        "{:?}",
        erased.payload
    );
    assert_eq!(erased.payload["count"], json!(3), "{:?}", erased.payload);
    assert_eq!(erased.acting_user_id, Some(a.id));
    // And no fact carries content.
    for gone in [M3_TEXT, M4_TEXT, M5_TEXT] {
        facts::assert_no_fact_carries(&ctx.pool, gone, "an erased message's text").await;
    }
    // And no entry or block of M3, M4 or M5 is left in the store: no row holds
    // their texts, no row but the facts names their entries, blocks or
    // annotations, nor X.
    for gone in [M3_TEXT, M4_TEXT, M5_TEXT] {
        stored::assert_nowhere(&ctx.pool, gone, &[], "an erased message's text").await;
    }
    for id in erased_ids.iter().chain([&x]) {
        stored::assert_nowhere(
            &ctx.pool,
            &id.to_string(),
            &["fact"],
            "an erased message's entry, block or annotation, or its artifact",
        )
        .await;
    }
    // But the block C was started from: the only rows naming it are C's.
    for (table, row) in stored::rows_holding(&ctx.pool, &origin.to_string(), &["fact"]).await {
        assert!(
            row.contains(&c.to_string()),
            "the origin block of C is named only by C: {table} {row}"
        );
    }
    // And the erase moves no activity: the thread's and its summary's.
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["lastActivityAt"],
        activity
    );
    tokio::time::sleep(common::QUIET_WINDOW).await;
    let (summary, _) = ctx
        .summary_entry(t)
        .await
        .expect("threads.thread_summary.{T} is published");
    let activity_at: chrono::DateTime<chrono::Utc> = activity
        .as_str()
        .expect("lastActivityAt is a date")
        .parse()
        .expect("lastActivityAt parses");
    assert_eq!(summary.last_activity_at, activity_at);

    // When a chunk for M5 is pushed, then its completion is sent.
    let facts_before = facts::count(&ctx.pool).await;
    let late = half.push(DeltaOp::text_appended(m5_block, " and more"));
    let late_chunk = common::content::delta_chunk(&late);
    stream::push(&ctx.nats.url(), m5, m5_last_seq + 1, &[late_chunk]).await;
    // Then the chunk finds no message: nothing comes back, nothing is pushed.
    a_feed.silence("a chunk for an erased message").await;
    o_feed.silence("a chunk for an erased message").await;
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![m1, m2]);
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    let completed = command(&ctx, &completion(t, m5, m5_last_seq + 1, &"0".repeat(64))).await;
    // And its completion is refused MESSAGE_NOT_FOUND.
    message_refused(
        &ctx,
        completed,
        ReasonCode::MessageNotFound,
        "completing an erased STREAMING message",
    )
    .await;

    // When a runner reads T's context.
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    // Then it holds M1 and M2.
    assert_eq!(turn_ids(&conversation), vec![m1, m2]);

    // When the same erase is sent under a new command id.
    let facts_before = facts::count(&ctx.pool).await;
    let snapshot = stored::snapshot(&ctx.pool).await;
    let again = command(&ctx, &erase).await;
    // Then it is confirmed, and changes nothing.
    erase_confirmed(&ctx, again, &erase, "the same erase, again").await;
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![m1, m2]);
    assert_eq!(facts::count(&ctx.pool).await, facts_before, "no fact");
    assert_eq!(stored::snapshot(&ctx.pool).await, snapshot, "no row");
    a_feed.silence("an erase that finds nothing").await;

    // When the harness erases M2 itself.
    let including = erasure_of(t, m2, true, Some(a.id));
    let sent = command(&ctx, &including).await;
    // Then confirmed; only M1 stays; A is told M2 is removed.
    erase_confirmed(&ctx, sent, &including, "erasing M2 itself").await;
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![m1]);
    a_feed
        .until("A is told M2 is removed", |delta| removes(delta, m2))
        .await;
    let t_facts = facts::of(&ctx.pool, "thread", t).await;
    let second = t_facts
        .iter()
        .rev()
        .find(|fact| fact.event_type == "MessagesErased")
        .expect("a second MessagesErased fact");
    assert_eq!(second.payload["message_id"], json!(m2));
    assert_eq!(second.payload["including"], json!(true));
    assert_eq!(second.payload["count"], json!(1));
    // And a message stored next takes its place after M1.
    let m6 = say(&ctx, t, &b, "Let me ask again").await;
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![m1, m6]);
    a_feed.drain().await;

    // When the harness erases after M4, gone.
    // Then refused MESSAGE_NOT_FOUND, and nothing changes.
    refused_changing_nothing(
        &ctx,
        &a,
        t,
        &erasure_of(t, m4, false, Some(a.id)),
        ReasonCode::MessageNotFound,
        "erasing after an erased message",
    )
    .await;

    // When the harness erases, on T, after a message of another thread.
    let elsewhere = create_thread(&ctx, Some(&a), &[]).await;
    let foreign = say(&ctx, elsewhere, &a, "elsewhere").await;
    // Then refused MESSAGE_NOT_FOUND, nothing changes on either thread.
    refused_changing_nothing(
        &ctx,
        &a,
        t,
        &erasure_of(t, foreign, false, Some(a.id)),
        ReasonCode::MessageNotFound,
        "erasing after a message of another thread",
    )
    .await;
    assert_eq!(
        message_ids(&ctx, &a.passport, elsewhere).await,
        vec![foreign]
    );

    // When T is resolved, an erase — after M1, or after a message T does not
    // hold — is refused THREAD_RESOLVED, and nothing changes.
    let resolved = command(&ctx, &common::flows::resolution_of(t, o.id)).await;
    thread_confirmed(&ctx, resolved, "resolving T").await;
    for (named, what) in [
        (m1, "erasing on a resolved thread"),
        (
            Uuid::now_v7(),
            "erasing after no message, on a resolved thread",
        ),
    ] {
        refused_changing_nothing(
            &ctx,
            &a,
            t,
            &erasure_of(t, named, false, Some(a.id)),
            ReasonCode::ThreadResolved,
            what,
        )
        .await;
    }
    let reopened = command(&ctx, &common::flows::reopening_of(t, o.id)).await;
    thread_confirmed(&ctx, reopened, "reopening T").await;

    // When T is closed, an erase is refused THREAD_NOT_OPEN.
    let closed_t = command(
        &ctx,
        &CloseThread {
            thread_id: t,
            status: ClosingStatus::Completed,
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, closed_t, "closing T").await;
    for (named, what) in [
        (m1, "erasing on a closed thread"),
        (
            Uuid::now_v7(),
            "erasing after no message, on a closed thread",
        ),
    ] {
        refused_changing_nothing(
            &ctx,
            &a,
            t,
            &erasure_of(t, named, false, Some(a.id)),
            ReasonCode::ThreadNotOpen,
            what,
        )
        .await;
    }

    // When an erase names a thread nobody created — then THREAD_NOT_FOUND.
    let nowhere = Uuid::now_v7();
    let sent = command(&ctx, &erasure_of(nowhere, m1, false, Some(a.id))).await;
    message_refused(
        &ctx,
        sent,
        ReasonCode::ThreadNotFound,
        "erasing on an unknown thread",
    )
    .await;

    // And C, started from a block of M3 before the erase, is still read by its
    // member B, its origin leading nowhere.
    let child_view = thread_view(&ctx, &b.passport, c).await;
    assert_eq!(child_view["id"], json!(c), "{child_view}");
    assert_eq!(child_view["parentThreadId"], json!(t));
    assert_eq!(child_view["originBlockId"], json!(origin));

    ctx.teardown().await;
}

/// The id of the first block of the response a reply holds.
fn first_block_of(replied: &[contract_threads::llm::TurnEntry]) -> Uuid {
    common::flows::uuid_of(&to_json(&replied[0])["content"][0]["id"])
}
