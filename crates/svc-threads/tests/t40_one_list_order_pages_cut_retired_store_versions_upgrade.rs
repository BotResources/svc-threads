//! **T40 — One list: order, pages, the cut of a runner's context, retired
//! store versions, the upgrade from branches.**
//!
//! > On thread T the harness stores, with store.v3, twelve messages. A member
//! > reads them in the order stored, five at a time, each page starting before
//! > the oldest message of the previous one, with no gap and no duplicate. A
//! > runner reading T's context naming the fourth message gets the first four;
//! > naming none, all twelve; naming a message of another thread, an empty
//! > conversation. A store.v1 and a store.v2 command are each refused
//! > COMMAND_VERSION_RETIRED and store nothing. The harness then runs the
//! > previous version of Threads on a fresh database and stores: U1; R1 of
//! > agent a1 and R2 of agent a2, both answering U1; R1b, a regeneration of R1
//! > answering U1; U2 answering R2; R3 of a1 answering U2; U2b, a retry of U2
//! > with R2 as parent, and R4 of a1 answering U2b, the most recent message. It
//! > starts the new version on the same database: it becomes ready; T reads
//! > U1, R2, R1b, U2b, R4 in that order, each with its id and its date; R1, U2
//! > and R3 are found nowhere in the store; no fact carries content. Starting
//! > the new version again changes nothing.
//!
//! The order is the order stored (`created_at`, then id), never the order of
//! the ids: the twelve ids are minted first and the messages stored in the
//! reverse order of their ids, so that the pages, the cut and the list can
//! only follow the storage.
//!
//! A page is read the newest first, `before` naming the oldest message of the
//! previous page (the paging of `threadsMessages`, kept from the SDL of
//! 0.11.0 without its `branchTip`): twelve messages read five at a time are
//! three pages, 5 + 5 + 2, and a page before the oldest is empty.
//!
//! The runner names a message with the cut argument of `threadsContext`
//! (`common::constants::context_cut_arg!`, an assumption of the oracle on the
//! name). The cut is read whole and as an agent's context; "an empty
//! conversation" is a context of T — its members, no turn, no artifact — never
//! a refusal nor `null`. Beyond the gesture, from the specification: a named
//! message that is deleted, or cancelled, still bounds the conversation
//! without being in it (read on a thread S of its own).
//!
//! The two retired versions travel exactly as a consumer still on them sends
//! them: `store.v1` through `contract-threads` 0.4.0, `store.v2` through
//! `contract-threads` 0.9.0, each on its own subject. Their refusal is a
//! `message.refused` naming the command, T and the message it would have
//! stored. "Store nothing" is read on the fact log, T's list, a member's live
//! feed, the `threads` schema of the store row for row, and every table of
//! the store, the engine's included, for their texts.
//!
//! "The previous version" is the last release with branches, `svc-threads`
//! 0.11.0, built from its tag (`common::previous`) and commanded with its own
//! `store.v2`: the suite writes no row. "A regeneration of R1" is a second
//! answer of a1 to U1; "a retry of U2" a second user message answering R2.
//! The active branch is the parent chain of R4: U1, R2, U2b, R4. R1b is kept
//! as a1's latest answer to U1, a1's answer not being on the branch; R1, an
//! earlier answer of a1 to U1, goes; U2 and R3 are off the branch. The dates
//! are read on 0.11.0 before the upgrade (a message off its active branch is
//! read as the tip of its own branch there).
//!
//! "Found nowhere in the store" is read over every table of the store
//! (`common::store`), the engine's included: no row holds the texts of R1, U2
//! and R3, and no row names their ids but the fact log, whose append-only
//! history keeps the facts of their creation (which never carry content).
//! R1 attaches artifact V: "removed with its artifacts" is V's row gone, the
//! read access obtained before the upgrade fetching nothing and the bucket
//! holding one object less (awaited: the bytes are purged after the commit).
//!
//! Concurrent stores (an addition the review asked for): ten stores sent at
//! once on one thread are listed in the order Threads answered them — the
//! order of their confirmations on the bus — on every read: the list, its
//! pages and the runner's context. Equal `created_at` cannot be reached
//! through the public surface; this is what the order rule means for a
//! caller.
//!
//! Beyond the gesture, from the specification the gesture states: a message
//! names no other message, so the SDL offers neither `parentMessageId` nor
//! `branchTip`; and the conversation a runner receives after the upgrade is
//! the thread in that same order.
//!
//! Channels: the answers (4); the pages and the list as a member reads them
//! (2); a member's live feed (3); the runner's context, whole and as an agent
//! context; the fact log and the store; the readiness of the upgraded boot;
//! the SDL.

mod common;

use common::artifacts::{File, download_url, fetch, object_count, upload};
use common::content::{ai_turn, closed, reply, text};
use common::feed::Live;
use common::flows::{
    ai, command, context, context_up_to, conversation_of, create, create_thread, creation,
    deletion_of, message_confirmed, message_ids, message_refused, messages, open_stream, page,
    projected_of, say, store, turn_ids, user_message, uuid_of, whole_message,
};
use common::previous::{
    self, ai_message_v2, ai_message_v2_attaching, command_v1, command_v2, message_view_0_11,
    user_message_v2,
};
use common::{CONTEXT_CUT_ARG, PUSH_TIMEOUT, TestContext, eventually, facts, store as rows};
use contract_threads::llm::Turn;
use contract_threads::{CancelMessage, MessageStatus, ReasonCode};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t40_the_messages_of_a_thread_are_one_list_read_by_pages_and_cut_for_a_runner() {
    // Given T with owner O and member A, a runner, agent a1.
    let ctx = TestContext::setup().await;
    let (o, a) = (ctx.enrol("olga").await, ctx.enrol("ada").await);
    let t = create_thread(&ctx, Some(&o), &[&a]).await;
    let runner = ctx.service_account(true);
    let analyst = ai("a1", "Analyst");

    // When the harness stores on T, with store.v3, twelve messages — A's
    // questions and a1's answers, in turn — under twelve ids minted first,
    // stored in the reverse order of their ids.
    let mut ids: Vec<Uuid> = (0..12).map(|_| Uuid::now_v7()).collect();
    ids.reverse();
    let mut stored = Vec::new();
    for (n, id) in ids.into_iter().enumerate() {
        if n % 2 == 0 {
            let mut asked = user_message(t, a.id, vec![text(&format!("question {n}"))]);
            asked.message_id = id;
            store(&ctx, asked, MessageStatus::Completed).await;
        } else {
            let turn = Turn::from(ai_turn(id, &analyst, closed(reply(&format!("answer {n}")))));
            store(
                &ctx,
                whole_message(t, id, &analyst, &turn, Vec::new()),
                MessageStatus::Completed,
            )
            .await;
        }
        stored.push(id);
    }
    assert!(
        stored.windows(2).all(|pair| pair[0] > pair[1]),
        "each message stored under an id smaller than the one before"
    );

    // Then a member reads them in the order stored, five at a time, each page
    // starting before the oldest message of the previous one: the newest five,
    // the five before, the first two — no gap, no duplicate — and nothing
    // before the first.
    let ids =
        |page: &[Value]| -> Vec<Uuid> { page.iter().map(|view| uuid_of(&view["id"])).collect() };
    let first = ids(&page(&ctx, &a.passport, t, 5, None).await);
    let second = ids(&page(&ctx, &a.passport, t, 5, first.last().copied()).await);
    let third = ids(&page(&ctx, &a.passport, t, 5, second.last().copied()).await);
    let newest_first: Vec<Uuid> = stored.iter().rev().copied().collect();
    assert_eq!(first, newest_first[0..5], "the newest five");
    assert_eq!(second, newest_first[5..10], "the five before them");
    assert_eq!(third, newest_first[10..12], "the first two");
    let empty = page(&ctx, &a.passport, t, 5, third.last().copied()).await;
    assert!(empty.is_empty(), "nothing before the first: {empty:?}");
    let mut read: Vec<Uuid> = [first, second, third].concat();
    read.reverse();
    assert_eq!(read, stored, "the order stored, no gap, no duplicate");
    // And every member reads the same list, O as A.
    assert_eq!(message_ids(&ctx, &o.passport, t).await, stored);

    // When a runner reads T's context naming the fourth message.
    let fourth = stored[3];
    let cut = context_up_to(&ctx, &runner.passport, t, None, Some(fourth)).await;
    // Then it gets the first four, whole and as a1's context.
    assert_eq!(
        turn_ids(&conversation_of(&cut)),
        stored[0..4],
        "the conversation up to the fourth message, named by `{CONTEXT_CUT_ARG}`"
    );
    let cut_for_a1 =
        projected_of(&context_up_to(&ctx, &runner.passport, t, Some(&analyst), Some(fourth)).await);
    let cut_ids: Vec<Uuid> = cut_for_a1
        .turns()
        .iter()
        .map(|turn| turn.id().as_uuid())
        .collect();
    assert_eq!(
        cut_ids,
        stored[0..4],
        "a1's context up to the fourth message"
    );

    // When it reads T's context naming none.
    let whole = context_up_to(&ctx, &runner.passport, t, None, None).await;
    // Then it gets all twelve, as the read naming no message at all does.
    assert_eq!(turn_ids(&conversation_of(&whole)), stored);
    assert_eq!(
        turn_ids(&conversation_of(
            &context(&ctx, &runner.passport, t, None).await
        )),
        stored
    );

    // When it reads T's context naming a message of another thread.
    let elsewhere = create_thread(&ctx, Some(&a), &[]).await;
    let foreign = say(&ctx, elsewhere, &a, "elsewhere").await;
    let other = context_up_to(&ctx, &runner.passport, t, None, Some(foreign)).await;
    // Then it gets T's context with an empty conversation.
    assert_eq!(other["threadId"], json!(t), "{other}");
    assert!(
        conversation_of(&other).turns().is_empty(),
        "an empty conversation: {other}"
    );
    assert_eq!(other["artifacts"], json!([]), "{other}");
    assert_eq!(
        other["members"].as_array().map(Vec::len),
        Some(2),
        "T's members: {other}"
    );

    // When ten stores are sent at once on thread W.
    let w = create_thread(&ctx, Some(&a), &[]).await;
    let concurrent: Vec<_> = (0..10)
        .map(|n| user_message(w, a.id, vec![text(&format!("sent at once, {n}"))]))
        .collect();
    let sent: Vec<Uuid> =
        futures_util::future::join_all(concurrent.iter().map(|store| command(&ctx, store))).await;
    for command_id in &sent {
        message_confirmed(
            &ctx,
            *command_id,
            MessageStatus::Completed,
            "a store sent at once",
        )
        .await;
    }
    // Then they are listed in the order their confirmations show they were
    // stored, on every read: the list, its pages, the runner's context.
    let confirmed: Vec<Uuid> = ctx
        .answers
        .order_of(&sent)
        .iter()
        .map(|command_id| {
            let at = sent
                .iter()
                .position(|sent| sent == command_id)
                .expect("sent");
            concurrent[at].message_id
        })
        .collect();
    assert_eq!(confirmed.len(), 10);
    assert_eq!(message_ids(&ctx, &a.passport, w).await, confirmed);
    let mut paged = Vec::new();
    let mut before = None;
    loop {
        let read: Vec<Uuid> = page(&ctx, &a.passport, w, 3, before)
            .await
            .iter()
            .map(|view| uuid_of(&view["id"]))
            .collect();
        if read.is_empty() {
            break;
        }
        before = read.last().copied();
        paged.extend(read);
    }
    paged.reverse();
    assert_eq!(paged, confirmed, "pages of three, in the order confirmed");
    assert_eq!(
        turn_ids(&conversation_of(
            &context(&ctx, &runner.passport, w, None).await
        )),
        confirmed,
        "the runner's context, in the order confirmed"
    );

    // When, on thread S, a runner names a deleted message, then a cancelled
    // one: S holds A's question, A's message deleted, a1's answer, a1's turn
    // cancelled, and A's last word.
    let s = create_thread(&ctx, Some(&a), &[]).await;
    let question = say(&ctx, s, &a, "a question").await;
    let deleted = say(&ctx, s, &a, "scratch that").await;
    let gone = command(&ctx, &deletion_of(s, deleted, a.id)).await;
    message_confirmed(&ctx, gone, MessageStatus::Completed, "deleting it").await;
    let answered = Uuid::now_v7();
    let answer_turn = Turn::from(ai_turn(answered, &analyst, closed(reply("an answer"))));
    store(
        &ctx,
        whole_message(s, answered, &analyst, &answer_turn, Vec::new()),
        MessageStatus::Completed,
    )
    .await;
    let cancelled = open_stream(&ctx, s, analyst.clone()).await;
    let stop = command(
        &ctx,
        &CancelMessage {
            message_id: cancelled,
            thread_id: s,
            seal: None,
            acting_user_id: Some(a.id),
        },
    )
    .await;
    message_confirmed(&ctx, stop, MessageStatus::Cancelled, "cancelling it").await;
    say(&ctx, s, &a, "a last word").await;
    // Then each still bounds the conversation, without being in it.
    let to_deleted = context_up_to(&ctx, &runner.passport, s, None, Some(deleted)).await;
    assert_eq!(
        turn_ids(&conversation_of(&to_deleted)),
        vec![question],
        "up to a deleted message"
    );
    let to_cancelled = context_up_to(&ctx, &runner.passport, s, None, Some(cancelled)).await;
    assert_eq!(
        turn_ids(&conversation_of(&to_cancelled)),
        vec![question, answered],
        "up to a cancelled message"
    );

    // When a store.v1, then a store.v2, are sent for T, A following T.
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let facts_before = facts::count(&ctx.pool).await;
    let snapshot = rows::snapshot(&ctx.pool).await;
    const BY_V1: &str = "stored by a consumer still on v1";
    const BY_V2: &str = "stored by a consumer still on v2";
    let v1 = previous::user_message(t, None, a.id, vec![previous::user_text(BY_V1)]);
    let v2 = user_message_v2(t, None, a.id, BY_V2);
    let sent_v1 = command_v1(&ctx, &v1).await;
    let sent_v2 = command_v2(&ctx, &v2).await;

    // Then each is refused COMMAND_VERSION_RETIRED, naming T and the message.
    for (sent, message, what) in [
        (sent_v1, v1.message_id, "a store.v1"),
        (sent_v2, v2.message_id, "a store.v2"),
    ] {
        let refusal = message_refused(&ctx, sent, ReasonCode::CommandVersionRetired, what).await;
        assert_eq!(
            (refusal.thread_id, refusal.message_id),
            (t, message),
            "{what}: {refusal:?}"
        );
        assert!(
            facts::of(&ctx.pool, "message", message).await.is_empty(),
            "{what}: no fact"
        );
    }
    // And they store nothing: no fact, T's list as it was, no row of the
    // store, their texts nowhere, nothing pushed.
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    assert_eq!(message_ids(&ctx, &a.passport, t).await, stored);
    assert_eq!(rows::snapshot(&ctx.pool).await, snapshot);
    for text in [BY_V1, BY_V2] {
        rows::assert_nowhere(&ctx.pool, text, &[], "a retired store").await;
    }
    a_feed.silence("a retired store pushes nothing").await;

    // And a message names no other message: the SDL offers no parent and no
    // branch tip.
    let sdl = common::schema_sdl().await;
    for gone in ["parentMessageId", "branchTip"] {
        assert!(!sdl.contains(gone), "{gone} is gone from the SDL");
    }

    ctx.teardown().await;
}

/// A `store.v2` sent to 0.11.0 and confirmed COMPLETED; answers the message.
async fn stored_v2(
    ctx: &TestContext,
    store: &contract_threads_v0_9::StoreMessageV2,
    what: &str,
) -> Uuid {
    let sent = command_v2(ctx, store).await;
    message_confirmed(ctx, sent, MessageStatus::Completed, what).await;
    store.message_id
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH, and builds the previous release"]
async fn t40_a_thread_that_held_branches_keeps_its_active_branch_and_each_other_agents_latest_answer()
 {
    // Given the previous version, 0.11.0, on a fresh database, and through
    // its commands T with member A, and agents a1 and a2.
    let mut ctx = TestContext::setup_branches_release().await;
    let a = ctx.enrol("ada").await;
    let t = create(&ctx, creation(Some(a.id), &[])).await;
    let (a1, a2) = (ai("a1", "Analyst"), ai("a2", "Critic"));
    // And V, a runner's file for T, which R1 attaches.
    let runner = ctx.service_account(true);
    let v_file = File::csv("attached to R1\n");
    let v = upload(&ctx, &runner.passport, t, &v_file).await;
    let texts = [
        "U1, the question",
        "R1, a1's first answer",
        "R2, a2's answer",
        "R1b, a1's regeneration",
        "U2, the follow-up",
        "R3, a1's answer to U2",
        "U2b, the follow-up retried",
        "R4, a1's answer to U2b",
    ];
    // U1; R1 of a1 and R2 of a2, both answering U1; R1b of a1 answering U1;
    // U2 answering R2; R3 of a1 answering U2; U2b answering R2; R4 of a1
    // answering U2b, the most recent.
    let u1 = stored_v2(&ctx, &user_message_v2(t, None, a.id, texts[0]), "U1").await;
    let r1 = stored_v2(
        &ctx,
        &ai_message_v2_attaching(t, Some(u1), &a1, texts[1], Some(v)),
        "R1",
    )
    .await;
    let r2 = stored_v2(&ctx, &ai_message_v2(t, Some(u1), &a2, texts[2]), "R2").await;
    let r1b = stored_v2(&ctx, &ai_message_v2(t, Some(u1), &a1, texts[3]), "R1b").await;
    let u2 = stored_v2(&ctx, &user_message_v2(t, Some(r2), a.id, texts[4]), "U2").await;
    let r3 = stored_v2(&ctx, &ai_message_v2(t, Some(u2), &a1, texts[5]), "R3").await;
    let u2b = stored_v2(&ctx, &user_message_v2(t, Some(r2), a.id, texts[6]), "U2b").await;
    let r4 = stored_v2(&ctx, &ai_message_v2(t, Some(u2b), &a1, texts[7]), "R4").await;
    let kept = [u1, r2, r1b, u2b, r4];
    let mut dates = Vec::new();
    for message in kept {
        let view = message_view_0_11(&ctx, &a.passport, t, message).await;
        dates.push(view["createdAt"].clone());
    }
    let v_url = download_url(&ctx, &a.passport, t, v)
        .await
        .expect("A reads V, attached to R1, on 0.11.0");
    assert_eq!(fetch(&v_url).await, (200, v_file.bytes.clone()));
    let objects = object_count(&ctx).await;

    // When the new version starts on the same database.
    let upgrade = ctx.upgrade().await;

    // Then it becomes ready.
    assert!(
        upgrade.migrated && upgrade.ready,
        "the new version becomes ready on the branches 0.11.0 stored:\n{}",
        upgrade.output
    );
    // And T reads U1, R2, R1b, U2b, R4 in that order, each with its id and
    // its date.
    let read = messages(&ctx, &a.passport, t).await;
    let read_ids: Vec<Uuid> = read.iter().map(|view| uuid_of(&view["id"])).collect();
    assert_eq!(
        read_ids, kept,
        "the active branch, and a1's latest answer to U1"
    );
    for ((view, date), message) in read.iter().zip(&dates).zip(kept) {
        assert_eq!(&view["createdAt"], date, "{message} keeps its date: {view}");
    }
    // And the conversation a runner receives is the thread in that order.
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    assert_eq!(turn_ids(&conversation), kept);
    // And R1, U2 and R3 are found nowhere in the store: not their texts, not
    // their ids but in the fact log's history.
    for (gone, text) in [(r1, texts[1]), (u2, texts[4]), (r3, texts[5])] {
        rows::assert_nowhere(&ctx.pool, text, &[], "a removed message's text").await;
        rows::assert_only_reserved(&ctx.pool, &gone.to_string(), &["fact"], "a removed message")
            .await;
    }
    // And R1's artifact V went with it: no read access, its row gone, the
    // read access obtained before fetching nothing, the bucket holding one
    // object less.
    assert_eq!(download_url(&ctx, &a.passport, t, v).await, None);
    rows::assert_only_reserved(&ctx.pool, &v.to_string(), &["fact"], "R1's artifact").await;
    eventually("V's bytes are gone", PUSH_TIMEOUT, || async {
        fetch(&v_url).await.0 != 200 && object_count(&ctx).await == objects - 1
    })
    .await;
    // And no fact carries content.
    for text in texts {
        facts::assert_no_fact_carries(&ctx.pool, text, "a message's text").await;
    }

    // When the new version starts again.
    let snapshot = rows::snapshot(&ctx.pool).await;
    let again = ctx.restart_current().await;

    // Then it is ready, and nothing changes.
    assert!(
        again.ready,
        "the new version starts again:\n{}",
        again.output
    );
    assert_eq!(
        rows::snapshot(&ctx.pool).await,
        snapshot,
        "the second start removes nothing"
    );
    assert_eq!(messages(&ctx, &a.passport, t).await, read);

    ctx.teardown().await;
}
