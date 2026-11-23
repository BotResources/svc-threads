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
//! A page is read the newest first, `before` naming the oldest message of the
//! previous page (the paging of `threadsMessages`, kept from the SDL of
//! 0.11.0 without its `branchTip`): twelve messages read five at a time are
//! three pages, 5 + 5 + 2, and a page before the oldest is empty.
//!
//! The runner names a message with the cut argument of `threadsContext`
//! (`common::constants::context_cut_arg!`, an assumption of the oracle on the
//! name). The cut is read whole and as an agent's context; "an empty
//! conversation" is a context of T — its members, no turn, no artifact — never
//! a refusal nor `null`.
//!
//! The two retired versions travel exactly as a consumer still on them sends
//! them: `store.v1` through `contract-threads` 0.4.0, `store.v2` through
//! `contract-threads` 0.9.0, each on its own subject. Their refusal is a
//! `message.refused` naming the command, T and the message it would have
//! stored. "Store nothing" is read on the fact log, T's list, a member's live
//! feed and the `threads` schema of the store.
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

use common::content::reply;
use common::feed::Live;
use common::flows::{
    ai, answer, context, context_up_to, conversation_of, create, create_thread, creation,
    message_confirmed, message_ids, message_refused_code, messages, page, projected_of, say,
    turn_ids, uuid_of,
};
use common::previous::{
    self, ai_message_v2, command_v1, command_v2, message_view_0_11, user_message_v2,
};
use common::{COMMAND_VERSION_RETIRED, CONTEXT_CUT_ARG, TestContext, facts, store};
use contract_threads::MessageStatus;
use serde_json::{Value, json};
use uuid::Uuid;

/// The engine's tables, which keep their own record of a command received.
const ENGINE_TABLES: [&str; 4] = [
    "service_engine.event_log",
    "service_engine.event_snapshot",
    "service_engine.accumulator_chunk",
    "service_engine.dead_letter",
];

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t40_the_messages_of_a_thread_are_one_list_read_by_pages_and_cut_for_a_runner() {
    // Given T with owner O and member A, a runner, agent a1.
    let ctx = TestContext::setup().await;
    let (o, a) = (ctx.enrol("olga").await, ctx.enrol("ada").await);
    let t = create_thread(&ctx, Some(&o), &[&a]).await;
    let runner = ctx.service_account(true);
    let analyst = ai("a1", "Analyst");

    // When the harness stores on T, with store.v3, twelve messages: A's
    // questions and a1's answers, in turn.
    let mut stored = Vec::new();
    for n in 1..=12 {
        let message = if n % 2 == 1 {
            say(&ctx, t, &a, &format!("question {n}")).await
        } else {
            answer(&ctx, t, analyst.clone(), reply(&format!("answer {n}"))).await
        };
        stored.push(message);
    }

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

    // When a store.v1, then a store.v2, are sent for T, A following T.
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let facts_before = facts::count(&ctx.pool).await;
    let snapshot = store::snapshot(&ctx.pool).await;
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
        let refusal = message_refused_code(&ctx, sent, COMMAND_VERSION_RETIRED, what).await;
        assert_eq!(refusal["thread_id"], json!(t), "{what}: {refusal}");
        assert_eq!(refusal["message_id"], json!(message), "{what}: {refusal}");
        assert!(
            facts::of(&ctx.pool, "message", message).await.is_empty(),
            "{what}: no fact"
        );
    }
    // And they store nothing: no fact, T's list as it was, no row of the
    // store, nothing pushed.
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    assert_eq!(message_ids(&ctx, &a.passport, t).await, stored);
    assert_eq!(store::snapshot(&ctx.pool).await, snapshot);
    for text in [BY_V1, BY_V2] {
        store::assert_nowhere(&ctx.pool, text, &ENGINE_TABLES, "a retired store").await;
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
    let r1 = stored_v2(&ctx, &ai_message_v2(t, Some(u1), &a1, texts[1]), "R1").await;
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
    let runner = ctx.service_account(true);
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    assert_eq!(turn_ids(&conversation), kept);
    // And R1, U2 and R3 are found nowhere in the store: not their texts, not
    // their ids but in the fact log's history.
    for (gone, text) in [(r1, texts[1]), (u2, texts[4]), (r3, texts[5])] {
        store::assert_nowhere(&ctx.pool, text, &[], "a removed message's text").await;
        store::assert_nowhere(&ctx.pool, &gone.to_string(), &["fact"], "a removed message").await;
    }
    // And no fact carries content.
    for text in texts {
        facts::assert_no_fact_carries(&ctx.pool, text, "a message's text").await;
    }

    // When the new version starts again.
    let snapshot = store::snapshot(&ctx.pool).await;
    let again = ctx.restart_current().await;

    // Then it is ready, and nothing changes.
    assert!(
        again.ready,
        "the new version starts again:\n{}",
        again.output
    );
    assert_eq!(
        store::snapshot(&ctx.pool).await,
        snapshot,
        "the second start removes nothing"
    );
    assert_eq!(messages(&ctx, &a.passport, t).await, read);

    ctx.teardown().await;
}
