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
//! **Bounds of the erase, around the gesture** (the oracle's additions, from
//! the specification: an erase removes the messages of its thread stored after
//! the named one, and only what they hold):
//!
//! - **Order is storage order, not id order.** M3's id is minted before M2's,
//!   so M3 sorts before M2 by id while it is stored after it: the erase tail is
//!   read by `created_at`.
//! - **Another thread.** U holds a message stored between M2 and M3: it stays,
//!   and the count stays 3.
//! - **What M2 holds.** M2 carries artifact Y; A has a read mark on M2. Both
//!   stay after the first erase and go with the `including` one.
//! - **What the thread holds.** P, uploaded for T and never attached (PENDING),
//!   belongs to no message: it survives both erases and the message stored
//!   next attaches it.
//! - **Every pod.** B follows T on a second pod of the deployment: the
//!   removals reach it there, and the late chunk of M5 pushes nothing there.
//! - **The bus redelivers.** The first erase, delivered again under its own
//!   command id once the message after M1 is stored, gets the same answer and
//!   erases nothing: the new message stays, no MessagesErased is added.
//! - **Erased ids stay used.** Every id is used once in the thread (T28): a
//!   message stored afterwards under the id of an erased message, or reusing
//!   an entry, block or annotation id of one, is refused KEY_REUSED — each
//!   tried alone.
//! - **A large erase** (second scenario): more than 1,000 messages after the
//!   named one go with one confirmation; the list ends at the named message,
//!   and a member following the thread ends on a head page holding none of
//!   them.
//!
//! The command is `integration.cmd.threads.message.erase.v1`
//! (`contract_threads::EraseMessages`); its answer is `message.confirmed`
//! naming the command, T and the message it named. The `MessagesErased` fact
//! is read under the payload keys `message_id`, `including` and `count`.
//!
//! "X's read access no longer returns its bytes" is read twice: a read access
//! asked for afterwards is "not found", and the one obtained before the erase
//! fetches nothing; the bucket holds one object less, and X's facts end on
//! `Removed`. The bytes leave the object store just after the commit, as for a
//! deleted thread (T17): they are awaited, within a bounded time.
//!
//! "No entry or block of M3, M4 or M5 is left in the store" is read over every
//! table of the store (`common::store`): no row holds their texts, and no row
//! but the fact log's names one of their entries, blocks or annotations —
//! but a row holding nothing but ids and dates, where an id stays reserved
//! (`store::assert_only_reserved`): the id is still used, the entry is gone.
//! The erased messages' own ids are read alike on the `threads` schema — a
//! ghost row of `threads.message` fails here — while the engine's tables may
//! keep the key of the removal pushed to the followers.
//! The block of M3 the child thread C was started from is named by C's own
//! rows only (C names it as its origin, which then leads nowhere).
//!
//! "A chunk for M5 is refused": a chunk has no answer, so the refusal is read
//! as nothing happening — no message back in the list, no fact, nothing pushed
//! on either pod, and its text stored nowhere, the engine's accumulator
//! included.
//!
//! The registry lists the erase's refusals as THREAD_NOT_FOUND,
//! THREAD_NOT_OPEN, THREAD_RESOLVED, MESSAGE_NOT_FOUND. That a resolved or a
//! closed thread is refused as such even when the message named is not one of
//! its — the thread judged before the message — is the oracle's reading of that
//! order.
//!
//! An erasure does not move the thread's activity (the specification): its
//! `lastActivityAt` and its published summary keep the date they had — the
//! summary read once a later title change shows in it, a positive sign that
//! the relay drained.
//!
//! Channels: the answers (4); the list as members read it, read marks, the
//! thread view (2); A's feed over SSE and O's over WebSocket on the first pod,
//! B's over SSE on a second pod (3); the fact log, the store and the bucket;
//! the runner's context; the summary entry.

mod common;

use std::collections::BTreeSet;

use common::artifacts::{File, download_url, fetch, object_count, upload};
use common::content::{
    Streamed, after, ai_turn, artifact, closed, draft_entry, draft_text, entries_of, human, reply,
    said, text, to_json,
};
use common::feed::{Live, THREAD_FEED, removes, thread_feed, upsert_of};
use common::flows::{
    ai, answer_attaching, command, completion, context, conversation_of, create, create_thread,
    creation, erase_confirmed, erasure_of, message_confirmed, message_ids, message_refused,
    message_view, open_stream, redeliver, replayed_alike, say, store, thread_confirmed,
    thread_view, turn_ids, user_entry_message, user_message, whole_message,
};
use common::{
    M_SET_READ_MARK, PUSH_TIMEOUT, Person, TestContext, eventually, facts, gql, store as stored,
    stream,
};
use contract_threads::llm::{
    BlockId, ContentBlock, DeltaOp, ModelResponse, ResponseEnding, ResponseMetadata, TextBlock,
    Turn, TurnEntry,
};
use contract_threads::{
    CloseThread, ClosingStatus, EraseMessages, MessageStatus, ReasonCode, SetTitle,
};
use serde_json::{Value, json};
use uuid::Uuid;

/// The engine's tables, which may keep the key of a removal pushed to the
/// followers of a thread.
const ENGINE_TABLES: [&str; 4] = [
    "service_engine.event_log",
    "service_engine.event_snapshot",
    "service_engine.accumulator_chunk",
    "service_engine.dead_letter",
];

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

/// The id of the first block of the response a reply holds.
fn first_block_of(replied: &[contract_threads::llm::TurnEntry]) -> Uuid {
    common::flows::uuid_of(&to_json(&replied[0])["content"][0]["id"])
}

/// `person`'s read mark on `thread`, as they read it.
async fn read_mark(ctx: &TestContext, person: &Person, thread: Uuid) -> Value {
    thread_view(ctx, &person.passport, thread).await["readMark"].clone()
}

/// `person` sets their read mark on `thread` at `message`.
async fn mark(ctx: &TestContext, person: &Person, thread: Uuid, message: Uuid) {
    let marked = ctx
        .run(
            &person.passport,
            M_SET_READ_MARK,
            json!({ "thread": thread, "message": message }),
        )
        .await;
    assert_eq!(
        gql::data(&marked, "threadsSetReadMark"),
        &json!({ "success": true })
    );
    assert_eq!(read_mark(ctx, person, thread).await, json!(message));
}

/// The `MessagesErased` facts of `thread`, in order.
async fn erasures(ctx: &TestContext, thread: Uuid) -> Vec<facts::Fact> {
    facts::of(&ctx.pool, "thread", thread)
        .await
        .into_iter()
        .filter(|fact| fact.event_type == "MessagesErased")
        .collect()
}

/// A message's own id is named by no row of the `threads` schema but the fact
/// log's, or a reservation of ids.
async fn no_row_names(ctx: &TestContext, message: Uuid, what: &str) {
    let mut except = vec!["fact"];
    except.extend(ENGINE_TABLES);
    stored::assert_only_reserved(&ctx.pool, &message.to_string(), &except, what).await;
}

/// Sends `erase`, refused with `code`, and nothing changes: not a row of the
/// store, not a fact, not T's list.
async fn refused_changing_nothing(
    ctx: &TestContext,
    reader: &Person,
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
    // Given T with owner O and members A and B, thread U of A, a runner,
    // agents a1 and a2.
    let mut ctx = TestContext::setup().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b]).await;
    let u = create_thread(&ctx, Some(&a), &[]).await;
    let runner = ctx.service_account(true);
    let (a1, a2) = (ai("a1", "Analyst"), ai("a2", "Critic"));
    // And X (for M3), Y (for M2) and P (never attached) uploaded for T.
    let (x_file, y_file, p_file) = (
        File::csv("erased,1\n"),
        File::csv("kept until M2 goes,2\n"),
        File::csv("pending,3\n"),
    );
    let x = upload(&ctx, &runner.passport, t, &x_file).await;
    let y = upload(&ctx, &runner.passport, t, &y_file).await;
    let p = upload(&ctx, &runner.passport, t, &p_file).await;
    const M2_TEXT: &str = "the analyst's answer stays, then goes";
    const M3_TEXT: &str = "the critic's answer goes";
    const M4_TEXT: &str = "bob's follow-up goes";
    const M5_TEXT: &str = "a half produced answer goes";
    const LATE: &str = " and late words never stored";

    // And T holds, in order: M1 by A; M2 of a1 carrying Y; M3 of a2 carrying
    // X; M4 by B; M5 of a1, STREAMING with a text half produced — M3's id
    // minted before M2's, and U's message stored between M2 and M3.
    let m1 = say(&ctx, t, &a, "What were the Q3 figures?").await;
    let m3 = Uuid::now_v7();
    let m2 = answer_attaching(&ctx, t, a1.clone(), M2_TEXT, y).await;
    assert!(m3 < m2, "M3 sorts before M2 by id, and is stored after it");
    let on_u = say(&ctx, u, &a, "on another thread, between M2 and M3").await;
    let m3_reply = reply(M3_TEXT);
    let m3_turn = Turn::from(ai_turn(m3, &a2, closed(m3_reply.clone())));
    let x_note = artifact(x);
    let x_note_id = x_note.id;
    store(
        &ctx,
        whole_message(t, m3, &a2, &m3_turn, vec![after(&m3_reply[0], x_note)]),
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
        vec![m1, m2, m3, m4, m5],
        "the order stored, not the order of the ids"
    );

    // And B's read mark on M4, A's on M2.
    mark(&ctx, &b, t, m4).await;
    mark(&ctx, &a, t, m2).await;

    // And child thread C, owned by B, started from the text block of M3.
    let origin = first_block_of(&m3_reply);
    let mut child = creation(Some(b.id), &[]);
    child.parent_thread_id = Some(t);
    child.origin_block_id = Some(origin);
    let c = create(&ctx, child).await;

    // And what the erase must remove, as members read it before: every id
    // M3, M4 and M5 hold; the read accesses to X and Y; the bucket's objects.
    let mut erased_ids = BTreeSet::new();
    for message in [m3, m4, m5] {
        erased_ids.extend(ids_held(&message_view(&ctx, &a.passport, t, message).await));
    }
    assert!(erased_ids.contains(&origin));
    erased_ids.remove(&origin);
    let m2_ids = ids_held(&message_view(&ctx, &a.passport, t, m2).await);
    let x_url = download_url(&ctx, &b.passport, t, x)
        .await
        .expect("a member reads X before the erase");
    assert_eq!(fetch(&x_url).await, (200, x_file.bytes.clone()));
    let y_url = download_url(&ctx, &b.passport, t, y)
        .await
        .expect("a member reads Y before the erase");
    let objects_before = object_count(&ctx).await;
    let activity = thread_view(&ctx, &a.passport, t).await["lastActivityAt"].clone();
    // And A follows T over SSE and O over WebSocket on the first pod, B over
    // SSE on a second pod of the deployment.
    let pod2 = ctx.spawn_pod().await;
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let mut o_feed = Live::thread_ws(&ctx, &o.passport, t).await;
    let mut b_on_pod2 = Live::sse_on(
        &pod2.base_url,
        &b.passport,
        THREAD_FEED,
        &thread_feed(t, 20),
    )
    .await;

    // When the harness erases after M2, M2 itself kept.
    let erase = erasure_of(t, m2, false, Some(a.id));
    let first_erase = command(&ctx, &erase).await;

    // Then (4) confirmed.
    erase_confirmed(&ctx, first_erase, &erase, "erasing after M2").await;
    // And (2) M3, M4 and M5 are gone, M1 and M2 stay — for every member.
    for person in [&o, &a, &b] {
        assert_eq!(
            message_ids(&ctx, &person.passport, t).await,
            vec![m1, m2],
            "the list as {} reads it",
            person.label
        );
    }
    // And U, whose message was stored between M2 and M3, is intact.
    assert_eq!(message_ids(&ctx, &a.passport, u).await, vec![on_u]);
    // And (3) every member following T is told of the three removals, on
    // either pod and over either transport, and nothing of them is pushed
    // again.
    for (feed, who) in [
        (&mut a_feed, "A over SSE"),
        (&mut o_feed, "O over WebSocket"),
        (&mut b_on_pod2, "B over SSE on the second pod"),
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
            assert!(
                !removes(delta, m1) && !removes(delta, m2),
                "{who}: M1 and M2 stay: {delta}"
            );
        }
    }
    // And X's read access no longer returns its bytes: none is given, the one
    // given before fetches nothing, the bucket holds one object less, X's
    // facts end on Removed.
    assert_eq!(download_url(&ctx, &b.passport, t, x).await, None);
    eventually("X's bytes are gone", PUSH_TIMEOUT, || async {
        fetch(&x_url).await.0 != 200 && object_count(&ctx).await == objects_before - 1
    })
    .await;
    eventually("X is removed", PUSH_TIMEOUT, || async {
        facts::types(&facts::of(&ctx.pool, "artifact", x).await).last() == Some(&"Removed")
    })
    .await;
    // And Y, M2's, stays readable, as P, which no message holds.
    assert_eq!(fetch(&y_url).await, (200, y_file.bytes.clone()));
    let y_again = download_url(&ctx, &b.passport, t, y)
        .await
        .expect("Y stays with M2");
    assert_eq!(fetch(&y_again).await, (200, y_file.bytes.clone()));
    // And B has no read mark; A's, on M2, stays.
    assert_eq!(read_mark(&ctx, &b, t).await, Value::Null);
    assert_eq!(read_mark(&ctx, &a, t).await, json!(m2));
    // And a MessagesErased fact names M2, including false and 3.
    let erased = erasures(&ctx, t).await;
    assert_eq!(erased.len(), 1);
    let erased = &erased[0];
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
    // their texts; no row but the facts names their entries, blocks or
    // annotations, nor X; no row of the threads schema but the facts names
    // the messages themselves.
    for gone in [M3_TEXT, M4_TEXT, M5_TEXT] {
        stored::assert_nowhere(&ctx.pool, gone, &[], "an erased message's text").await;
    }
    for id in erased_ids.iter().chain([&x]) {
        stored::assert_only_reserved(
            &ctx.pool,
            &id.to_string(),
            &["fact"],
            "an erased message's entry, block or annotation, or its artifact",
        )
        .await;
    }
    for message in [m3, m4, m5] {
        no_row_names(&ctx, message, "an erased message").await;
    }
    // But the block C was started from: the only rows naming it are C's.
    for (table, row) in stored::rows_holding(&ctx.pool, &origin.to_string(), &["fact"]).await {
        assert!(
            row.contains(&c.to_string()),
            "the origin block of C is named only by C: {table} {row}"
        );
    }
    // And the erase moves no activity: the thread's, and its summary's once a
    // later title change shows there.
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["lastActivityAt"],
        activity
    );
    let titled = command(
        &ctx,
        &SetTitle {
            thread_id: t,
            title: "Q3 figures".to_string(),
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, titled, "titling T").await;
    eventually("T's summary shows its title", PUSH_TIMEOUT, || async {
        ctx.summary_entry(t)
            .await
            .is_some_and(|(summary, _)| summary.title.as_deref() == Some("Q3 figures"))
    })
    .await;
    let (summary, _) = ctx.summary_entry(t).await.expect("waited above");
    let activity_at: chrono::DateTime<chrono::Utc> = activity
        .as_str()
        .expect("lastActivityAt is a date")
        .parse()
        .expect("lastActivityAt parses");
    assert_eq!(summary.last_activity_at, activity_at);
    for feed in [&mut a_feed, &mut o_feed, &mut b_on_pod2] {
        feed.drain().await;
    }

    // When a chunk for M5 is pushed, then its completion is sent.
    let facts_before = facts::count(&ctx.pool).await;
    let late = half.push(DeltaOp::text_appended(m5_block, LATE));
    let late_chunk = common::content::delta_chunk(&late);
    stream::push(&ctx.nats.url(), m5, m5_last_seq + 1, &[late_chunk]).await;
    // Then the chunk finds no message: nothing is pushed on either pod,
    // nothing comes back in the list, no fact, its text stored nowhere.
    a_feed.silence("a chunk for an erased message").await;
    o_feed.silence("a chunk for an erased message").await;
    b_on_pod2
        .silence("a chunk for an erased message, on the second pod")
        .await;
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![m1, m2]);
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    stored::assert_nowhere(&ctx.pool, LATE, &[], "a late chunk's text").await;
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

    // When a message is stored under the id of erased M3, or reusing — each
    // alone — the id of M3's response, of its first block, or of its ARTIFACT
    // annotation.
    let fresh = |id: Uuid, entry: TurnEntry| Turn::from(ai_turn(id, &a1, closed(vec![entry])));
    let under_m3 = fresh(
        m3,
        TurnEntry::from(common::content::model_response(
            vec![said("again")],
            ResponseEnding::finished(),
            None,
        )),
    );
    let entry_reused = Uuid::now_v7();
    let reusing_entry = fresh(
        entry_reused,
        TurnEntry::from(
            ModelResponse::new(
                m3_reply[0].id(),
                vec![said("again")],
                ResponseEnding::finished(),
                ResponseMetadata::new(),
            )
            .expect("a model response on its own"),
        ),
    );
    let block_reused = Uuid::now_v7();
    let reusing_block = fresh(
        block_reused,
        common::content::response(
            vec![ContentBlock::from(
                TextBlock::new(BlockId::from_uuid(origin).expect("a UUIDv7"), "again")
                    .expect("a text"),
            )],
            ResponseEnding::finished(),
            None,
        ),
    );
    let note_reused = Uuid::now_v7();
    let noted = reply("again, noted");
    let reusing_note = Turn::from(ai_turn(note_reused, &a1, closed(noted.clone())));
    let mut reused_note = common::content::warning("NOTE", "once more");
    reused_note.id = x_note_id;
    for (message, turn, annotations, what) in [
        (m3, under_m3, Vec::new(), "the id of erased M3"),
        (
            entry_reused,
            reusing_entry,
            Vec::new(),
            "an entry id of erased M3",
        ),
        (
            block_reused,
            reusing_block,
            Vec::new(),
            "a block id of erased M3",
        ),
        (
            note_reused,
            reusing_note,
            vec![after(&noted[0], reused_note)],
            "an annotation id of erased M3",
        ),
    ] {
        let facts_before = facts::count(&ctx.pool).await;
        let sent = command(&ctx, &whole_message(t, message, &a1, &turn, annotations)).await;
        // Then each is refused KEY_REUSED, and nothing is stored.
        message_refused(&ctx, sent, ReasonCode::KeyReused, what).await;
        assert_eq!(
            message_ids(&ctx, &a.passport, t).await,
            vec![m1, m2],
            "{what}"
        );
        assert_eq!(
            facts::count(&ctx.pool).await,
            facts_before,
            "{what}: no fact"
        );
    }
    for feed in [&mut a_feed, &mut o_feed, &mut b_on_pod2] {
        feed.silence("a refused store pushes nothing").await;
    }

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
    // Then confirmed; only M1 stays; A, and B on the second pod, are told M2
    // is removed.
    erase_confirmed(&ctx, sent, &including, "erasing M2 itself").await;
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![m1]);
    a_feed
        .until("A is told M2 is removed", |delta| removes(delta, m2))
        .await;
    b_on_pod2
        .until("B is told M2 is removed, on the second pod", |delta| {
            removes(delta, m2)
        })
        .await;
    let erased = erasures(&ctx, t).await;
    assert_eq!(erased.len(), 2);
    assert_eq!(erased[1].payload["message_id"], json!(m2));
    assert_eq!(erased[1].payload["including"], json!(true));
    assert_eq!(erased[1].payload["count"], json!(1));
    // And what M2 held goes with it: its text, its ids, A's read mark on it,
    // Y and its bytes.
    stored::assert_nowhere(&ctx.pool, M2_TEXT, &[], "M2's text").await;
    for id in &m2_ids {
        stored::assert_only_reserved(&ctx.pool, &id.to_string(), &["fact"], "what M2 held").await;
    }
    no_row_names(&ctx, m2, "M2").await;
    assert_eq!(read_mark(&ctx, &a, t).await, Value::Null);
    assert_eq!(download_url(&ctx, &b.passport, t, y).await, None);
    eventually("Y's bytes are gone", PUSH_TIMEOUT, || async {
        fetch(&y_url).await.0 != 200 && object_count(&ctx).await == objects_before - 2
    })
    .await;
    // And a message stored next takes its place after M1 — attaching P, the
    // PENDING upload no erase touched.
    let m6 = answer_attaching(&ctx, t, a1.clone(), "the pending file, attached", p).await;
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![m1, m6]);
    let p_url = download_url(&ctx, &b.passport, t, p)
        .await
        .expect("P is attached to M6");
    assert_eq!(fetch(&p_url).await, (200, p_file.bytes.clone()));

    // When the bus delivers the first erase again, under its own command id.
    redeliver(&ctx, first_erase, &erase).await;
    // Then it is answered alike, and erases nothing: M6 stays, no
    // MessagesErased is added.
    replayed_alike(&ctx, first_erase, "the first erase, redelivered").await;
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![m1, m6]);
    assert_eq!(erasures(&ctx, t).await.len(), 2);
    for feed in [&mut a_feed, &mut o_feed, &mut b_on_pod2] {
        feed.drain().await;
    }

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
    // Then refused MESSAGE_NOT_FOUND, nothing changes on either thread.
    refused_changing_nothing(
        &ctx,
        &a,
        t,
        &erasure_of(t, on_u, false, Some(a.id)),
        ReasonCode::MessageNotFound,
        "erasing after a message of another thread",
    )
    .await;
    assert_eq!(message_ids(&ctx, &a.passport, u).await, vec![on_u]);

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

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t39_a_large_erase_gets_one_answer_and_leaves_the_named_message_last() {
    // Given T with owner A, the named message N, and 1,001 messages stored
    // after it — sent at once, each confirmed.
    const AFTER: usize = 1_001;
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let named = say(&ctx, t, &a, "the named message").await;
    let mut sent = Vec::with_capacity(AFTER);
    for n in 0..AFTER {
        let message = user_message(t, a.id, vec![text(&format!("message {n} after N"))]);
        let id = message.message_id;
        sent.push((command(&ctx, &message).await, id));
    }
    let mut after_named = BTreeSet::new();
    for (command_id, id) in &sent {
        message_confirmed(
            &ctx,
            *command_id,
            MessageStatus::Completed,
            "a message after N",
        )
        .await;
        after_named.insert(*id);
    }
    // And A following T: its head page holds the latest of them.
    let mut feed = Live::thread(&ctx, &a.passport, t).await;
    let head: Vec<Uuid> = common::feed::views(&feed.reset)
        .iter()
        .map(|view| common::flows::uuid_of(&view["id"]))
        .filter(|id| after_named.contains(id))
        .collect();
    assert!(!head.is_empty(), "the head page holds messages after N");

    // When the harness erases after N.
    let erase = erasure_of(t, named, false, Some(a.id));
    let sent = command(&ctx, &erase).await;

    // Then (4) one confirmation answers it.
    erase_confirmed(&ctx, sent, &erase, "a large erase").await;
    common::flows::answered_once(&ctx, sent, "a large erase").await;
    // And (2) the list ends at N.
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![named]);
    let erased = erasures(&ctx, t).await;
    assert_eq!(erased.len(), 1);
    assert_eq!(erased[0].payload["count"], json!(AFTER));
    // And (3) A is told each message of its head page is removed, and none of
    // the erased messages is pushed.
    let seen = feed
        .collect_until("A is told its head page is removed", |seen| {
            head.iter()
                .all(|id| seen.iter().any(|delta| removes(delta, *id)))
        })
        .await;
    for delta in seen.iter().chain(&feed.drain().await) {
        for id in &after_named {
            assert!(
                upsert_of(delta, *id).is_none(),
                "an erased message is never pushed: {delta}"
            );
        }
    }
    // And a member following T now opens on a head page holding none of them:
    // T and N.
    let reopened = Live::thread(&ctx, &a.passport, t).await;
    let ids: Vec<Uuid> = common::feed::views(&reopened.reset)
        .iter()
        .map(|view| common::flows::uuid_of(&view["id"]))
        .collect();
    assert_eq!(
        common::flows::sorted(&ids),
        common::flows::sorted(&[t, named])
    );

    ctx.teardown().await;
}
