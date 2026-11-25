//! **T41 — A user message's turn carries the time Threads stored it.**
//!
//! > On thread T with member A, A sends user message M1, then an agent answers
//! > it with AI message R1, then A sends M2. A runner reads T's context up to
//! > M1, then up to M2: in both reads M1's input turn carries an observed time
//! > equal to M1's `created_at`, the same value; M2's turn carries M2's
//! > `created_at`; R1's turn carries no time Threads chose. A edits M1: its
//! > observed time is unchanged. A user message stored before the upgrade,
//! > read after it, carries its `created_at` as its observed time.
//!
//! "The observed time" is the library's observed time of the turn: the
//! `observed_at` of the input turn, in the runner's context loaded by the
//! library (`Turn::observed_at`) and in its raw JSON. A message's `created_at`
//! is its `createdAt` as a member reads it. GraphQL writes `createdAt` in
//! RFC 3339 with as many fraction digits as it needs, so the two are compared
//! as instants, to the microsecond, never as text (`content::created_at`,
//! `content::instant_of`, `content::carries_its_created_at`).
//!
//! "The same value" is what keeps a provider's prompt cache: M1's turn, its
//! raw JSON, is equal value for value from one read to the next, up to M1, up
//! to M2, and the whole thread later. Each read is made whole and as the agent
//! context of a1, R1's author, where M1 and M2 are input turns kept whole.
//!
//! "No time Threads chose" is proven on both paths an AI message takes: R1,
//! stored whole without a time, and R3, streamed then completed, carry none —
//! no `observed_at` in the raw JSON. R2, stored whole with a time its writer
//! gave its turn (nine fraction digits, long before its `created_at`), carries
//! exactly that time, as written.
//!
//! "Before the upgrade" is `svc-threads` 0.12.0, the last release whose user
//! messages carry no observed time and the one a deployment upgrades from
//! (`common::previous::untimed_release_binary`). It stores, through this
//! version's own commands, two user messages, an AI message without a time and
//! one with its writer's time; its runner context shows no time on the user
//! messages. This version then starts on the same database.
//!
//! Channels: the answers (4); the messages as a member reads them, their
//! `createdAt` (2); the runner's context, whole and as an agent context, loaded
//! by the library and raw; the readiness of the upgraded boot. The observed
//! time is not on a member's surface (a message view has no such field), so
//! no subscription is asserted.

mod common;

use chrono::{DateTime, Utc};
use common::content::{
    Agent, Streamed, ai_turn, carries_its_created_at, carries_no_time, carries_the_writers_time,
    closed, created_at, instant_of, raw_turn, reply, text, turn_in, turns_of,
};
use common::flows::{
    ai, answer, command, completion, context, context_up_to, create_thread, edit_of,
    message_confirmed, message_version, message_view, open_stream, say, store, whole_message,
};
use common::{TestContext, stream};
use contract_threads::MessageStatus;
use contract_threads::llm::{IncomingEntry, ObservedAt, ResponseEnding, Turn};
use serde_json::Value;
use uuid::Uuid;

/// The time R2's writer gave its turn: nine fraction digits, long before any
/// message of the scenario is stored.
const WRITER_TIME: &str = "2026-01-15T08:30:00.123456789Z";

/// The time the AI message stored before the upgrade was given by its writer.
const WRITER_TIME_BEFORE: &str = "2026-02-03T04:05:06.7Z";

/// The ids of a context's turns, in order.
fn ids_of(context: &Value) -> Vec<Uuid> {
    turns_of(context)
        .iter()
        .map(|turn| turn.id().as_uuid())
        .collect()
}

/// A whole AI message of `agent` under a fresh id, answering in `answer`, its
/// turn carrying `written` as the observed time its writer gave it.
async fn answer_at(
    ctx: &TestContext,
    thread: Uuid,
    agent: &Agent,
    answer: &str,
    written: &str,
) -> Uuid {
    let message = Uuid::now_v7();
    let turn = ai_turn(message, agent, closed(reply(answer)))
        .with_observed_at(ObservedAt::new(written).expect("a writer's observed time"));
    store(
        ctx,
        whole_message(thread, message, agent, &Turn::from(turn), Vec::new()),
        MessageStatus::Completed,
    )
    .await
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t41_a_user_messages_turn_carries_the_time_threads_stored_it_on_every_read_and_edit() {
    // Given thread T with member A, agent a1, and a runner.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let analyst = ai("a1", "Analyst");
    let runner = ctx.service_account(true);
    const ASKED: &str = "What changed since Monday?";
    const EDITED: &str = "What changed since Tuesday?";

    // When A sends M1, a1 answers it with R1 — no time given to its turn —
    // and A sends M2: (4) each confirmed COMPLETED.
    let m1 = say(&ctx, t, &a, ASKED).await;
    let r1 = answer(&ctx, t, analyst.clone(), reply("Two deployments.")).await;
    let m2 = say(&ctx, t, &a, "Which one failed?").await;
    // (2) Their createdAt, as a member reads it.
    let m1_view = message_view(&ctx, &a.passport, t, m1).await;
    let m2_view = message_view(&ctx, &a.passport, t, m2).await;

    // When a runner reads T's context up to M1, whole and as a1's.
    let whole_to_m1 = context_up_to(&ctx, &runner.passport, t, None, Some(m1)).await;
    let a1_to_m1 = context_up_to(&ctx, &runner.passport, t, Some(&analyst), Some(m1)).await;

    // Then M1 alone is there, its input turn carrying its createdAt as its
    // observed time.
    assert_eq!(ids_of(&whole_to_m1), vec![m1]);
    assert_eq!(ids_of(&a1_to_m1), vec![m1]);
    let m1_time = carries_its_created_at(&whole_to_m1, &m1_view, "M1, read up to M1");
    assert_eq!(
        carries_its_created_at(&a1_to_m1, &m1_view, "M1 for a1, read up to M1"),
        m1_time
    );

    // When the runner reads T's context up to M2, whole and as a1's.
    let whole_to_m2 = context_up_to(&ctx, &runner.passport, t, None, Some(m2)).await;
    let a1_to_m2 = context_up_to(&ctx, &runner.passport, t, Some(&analyst), Some(m2)).await;

    // Then M1, R1 and M2 are there.
    assert_eq!(ids_of(&whole_to_m2), vec![m1, r1, m2]);
    assert_eq!(ids_of(&a1_to_m2), vec![m1, r1, m2]);
    for (read, earlier, form) in [
        (&whole_to_m2, &whole_to_m1, "whole"),
        (&a1_to_m2, &a1_to_m1, "for a1"),
    ] {
        // And M1's turn carries its createdAt, the same value as in the read
        // up to M1: the same turn, its JSON equal value for value.
        assert_eq!(
            carries_its_created_at(read, &m1_view, "M1, read up to M2"),
            m1_time,
            "{form}"
        );
        assert_eq!(
            raw_turn(read, m1),
            raw_turn(earlier, m1),
            "{form}: M1's turn is the same from one read to the next"
        );
        // And M2's turn carries M2's createdAt.
        carries_its_created_at(read, &m2_view, "M2, read up to M2");
        // And R1's turn carries no time Threads chose.
        carries_no_time(read, r1, "R1, stored without a time");
    }
    let m2_time = carries_its_created_at(&whole_to_m2, &m2_view, "M2");
    assert_ne!(m1_time, m2_time, "each user message, its own time");

    // When a1 answers M2 with R2, whose writer gave its turn a time of its
    // own, then with R3, streamed and completed.
    let r2 = answer_at(&ctx, t, &analyst, "The second one.", WRITER_TIME).await;
    let r3 = open_stream(&ctx, t, analyst.clone()).await;
    let mut streamed = Streamed::start(r3, None);
    streamed.text(&["It is rolled back."], true);
    streamed.complete(ResponseEnding::finished());
    let (last_seq, hash) = stream::push(&ctx.nats.url(), r3, 0, &streamed.chunks()).await;
    let completed = command(&ctx, &completion(t, r3, last_seq, &hash)).await;
    message_confirmed(&ctx, completed, MessageStatus::Completed, "completing R3").await;
    let r2_view = message_view(&ctx, &a.passport, t, r2).await;

    // Then, read whole and as a1's, R2 carries exactly its writer's time —
    // not its createdAt — and R3 none; M1 and M2 are as they were.
    let whole = context(&ctx, &runner.passport, t, None).await;
    let for_a1 = context(&ctx, &runner.passport, t, Some(&analyst)).await;
    for (read, earlier, form) in [
        (&whole, &whole_to_m2, "whole"),
        (&for_a1, &a1_to_m2, "for a1"),
    ] {
        assert_eq!(ids_of(read), vec![m1, r1, m2, r2, r3], "{form}");
        carries_the_writers_time(read, r2, WRITER_TIME, "R2, its writer's time");
        carries_no_time(read, r3, "R3, streamed");
        carries_no_time(read, r1, "R1");
        for (message, what) in [(m1, "M1"), (m2, "M2")] {
            assert_eq!(
                raw_turn(read, message),
                raw_turn(earlier, message),
                "{form}: {what}'s turn is the same from one read to the next"
            );
        }
    }
    let writer: DateTime<Utc> = instant_of(&ObservedAt::new(WRITER_TIME).expect("a time"));
    assert_ne!(
        writer,
        created_at(&r2_view),
        "R2's writer gave a time distinct from the time Threads stored it"
    );

    // When A edits M1.
    let version = message_version(&ctx, &a.passport, t, m1).await;
    let edited_block = text(EDITED);
    let edited = command(
        &ctx,
        &edit_of(t, m1, a.id, version, vec![edited_block.clone()]),
    )
    .await;

    // Then (4) confirmed COMPLETED, and (2) M1 reads edited, its createdAt
    // kept.
    message_confirmed(&ctx, edited, MessageStatus::Completed, "A editing M1").await;
    let m1_edited = message_view(&ctx, &a.passport, t, m1).await;
    assert_eq!(m1_edited["version"].as_u64(), Some(version + 1));
    assert!(m1_edited["editedAt"].is_string(), "{m1_edited}");
    assert_eq!(m1_edited["createdAt"], m1_view["createdAt"]);

    // And a runner reads M1 with its new content and its observed time
    // unchanged — whole, as a1's, and up to M1 — M2 and the answers as they
    // were.
    let whole_after = context(&ctx, &runner.passport, t, None).await;
    let a1_after = context(&ctx, &runner.passport, t, Some(&analyst)).await;
    let to_m1_after = context_up_to(&ctx, &runner.passport, t, None, Some(m1)).await;
    for (read, earlier, form) in [
        (&whole_after, &whole, "whole"),
        (&a1_after, &for_a1, "for a1"),
        (&to_m1_after, &whole_to_m1, "up to M1"),
    ] {
        assert_eq!(
            carries_its_created_at(read, &m1_view, "M1, edited"),
            m1_time,
            "{form}: M1's observed time is unchanged by the edit"
        );
        let turn = turn_in(read, m1);
        let IncomingEntry::HumanMessage(human) = &turn.as_input().expect("M1").entries()[0] else {
            panic!("{form}: M1 holds A's human message: {turn:?}");
        };
        assert_eq!(
            human.content(),
            [edited_block.clone()].as_slice(),
            "{form}: M1's new content"
        );
        assert!(
            !raw_turn(read, m1).to_string().contains(ASKED),
            "{form}: the old content is gone"
        );
        assert_ne!(
            raw_turn(read, m1),
            raw_turn(earlier, m1),
            "{form}: M1 edited"
        );
    }
    for message in [m2, r1, r2, r3] {
        assert_eq!(
            raw_turn(&whole_after, message),
            raw_turn(&whole, message),
            "{message} is as it was"
        );
    }

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH, and builds the previous release"]
async fn t41_a_user_message_stored_before_the_upgrade_carries_its_created_at_after_it() {
    // Given the previous version, 0.12.0, on a fresh database, and through its
    // commands thread T with member A: A's U1, a1's answer R1 without a time,
    // a1's answer R2 whose writer gave its turn a time, and A's U2.
    let mut ctx = TestContext::setup_untimed_release().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let analyst = ai("a1", "Analyst");
    let runner = ctx.service_account(true);
    let u1 = say(&ctx, t, &a, "Stored before the upgrade").await;
    let r1 = answer(&ctx, t, analyst.clone(), reply("Noted.")).await;
    let r2 = answer_at(&ctx, t, &analyst, "Noted, with a time.", WRITER_TIME_BEFORE).await;
    let u2 = say(&ctx, t, &a, "Also before the upgrade").await;
    let dates_before: Vec<Value> = {
        let mut dates = Vec::new();
        for message in [u1, u2] {
            dates.push(message_view(&ctx, &a.passport, t, message).await["createdAt"].clone());
        }
        dates
    };
    // And 0.12.0 gives a runner the user messages with no observed time, and
    // R2 with its writer's.
    let before = context(&ctx, &runner.passport, t, None).await;
    assert_eq!(ids_of(&before), vec![u1, r1, r2, u2]);
    carries_no_time(&before, u1, "U1, on 0.12.0");
    carries_no_time(&before, u2, "U2, on 0.12.0");
    carries_no_time(&before, r1, "R1, on 0.12.0");
    carries_the_writers_time(&before, r2, WRITER_TIME_BEFORE, "R2, on 0.12.0");

    // When this version starts on the same database.
    let upgrade = ctx.upgrade().await;

    // Then it becomes ready.
    assert!(
        upgrade.migrated && upgrade.ready,
        "the new version becomes ready on what 0.12.0 stored:\n{}",
        upgrade.output
    );
    // And (2) U1 and U2 keep their createdAt.
    let u1_view = message_view(&ctx, &a.passport, t, u1).await;
    let u2_view = message_view(&ctx, &a.passport, t, u2).await;
    assert_eq!(
        vec![u1_view["createdAt"].clone(), u2_view["createdAt"].clone()],
        dates_before,
        "each user message keeps its date"
    );

    // When a runner reads T's context, whole, as a1's, and up to U1.
    let whole = context(&ctx, &runner.passport, t, None).await;
    let for_a1 = context(&ctx, &runner.passport, t, Some(&analyst)).await;
    let to_u1 = context_up_to(&ctx, &runner.passport, t, None, Some(u1)).await;

    // Then each user message's turn carries its own createdAt as its observed
    // time, the same value on every read; R1 still carries none, R2 still
    // exactly its writer's time.
    assert_eq!(ids_of(&to_u1), vec![u1]);
    let u1_time = carries_its_created_at(&to_u1, &u1_view, "U1, read up to U1");
    for (read, form) in [(&whole, "whole"), (&for_a1, "for a1")] {
        assert_eq!(ids_of(read), vec![u1, r1, r2, u2], "{form}");
        assert_eq!(
            carries_its_created_at(read, &u1_view, "U1, stored before the upgrade"),
            u1_time,
            "{form}"
        );
        assert_eq!(
            raw_turn(read, u1),
            raw_turn(&to_u1, u1),
            "{form}: U1's turn is the same from one read to the next"
        );
        let u2_time = carries_its_created_at(read, &u2_view, "U2, stored before the upgrade");
        assert_ne!(u2_time, u1_time, "{form}: each user message, its own time");
        carries_no_time(read, r1, "R1, stored before the upgrade without a time");
        carries_the_writers_time(read, r2, WRITER_TIME_BEFORE, "R2, its writer's time kept");
    }

    ctx.teardown().await;
}
