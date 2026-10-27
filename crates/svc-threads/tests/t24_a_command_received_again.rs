//! **T24 — A command received again.**
//!
//! > The harness sends message.store for M1, then sends it again under the
//! > same command id: the same confirmed answer is published again, M1 is
//! > stored once, no second Created fact exists. The same holds for a refusal:
//! > a message.edit refused VERSION_CONFLICT, sent again under its command id,
//! > is refused VERSION_CONFLICT again with the same current version, even
//! > though M1 has since changed. The harness then removes the stored answer of
//! > these commands, as the retention sweep does, and sends them again under
//! > the same command ids: message.store is refused KEY_REUSED, a
//! > message.delete already applied is confirmed and changes nothing, a
//! > message.edit made from the previous version is refused VERSION_CONFLICT;
//! > in every case M1 and its facts are those they were before the second
//! > sending. On another OPEN thread T, thread.resolve is confirmed, then
//! > thread.reopen is confirmed. After the stored answer of the resolve command
//! > expires, the harness sends that resolve command again under its original
//! > id: it is judged on the current state and resolves T again. This new
//! > effect is allowed after the stored answer has expired.
//!
//! Two readings the scenario makes explicit:
//!
//! - **The answer published again.** The engine re-publishes a stored answer
//!   under its first event id, which the broker drops as a duplicate inside
//!   its duplicate window. The scenario shortens `INTEGRATION_EVT`'s window to
//!   one second, as a deployment may, and sends each command again past it:
//!   the capture then holds the answer twice, identical.
//! - **The deletion already applied.** Were it M1's, M1 would be deleted and
//!   an edit of it refused `MESSAGE_NOT_EDITABLE` before its version is ever
//!   read; the deletion is therefore of M2, A's second message, while the
//!   edits are M1's.
//!
//! These commands are sent outside the suite's one-answer ledger: answering a
//! command again is what the scenario asserts, answer by answer.
//!
//! Channels: the answers (4), the messages and the thread as members read them
//! (2), the fact log; the engine's idempotency claims, removed as its sweep
//! removes them.

mod common;

use std::time::Duration;

use common::commands::{Answer, commanding_service, send, send_as};
use common::content::text;
use common::facts;
use common::flows::{
    create_thread, deletion_of, edit_of, message_view, reopening_of, resolution_of, say,
    thread_view, user_message,
};
use common::{PUSH_TIMEOUT, TestContext};
use contract_threads::{ReasonCode, ThreadsCommand};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

const DUPLICATE_WINDOW: Duration = Duration::from_secs(1);

/// Past the broker's duplicate window.
async fn past_the_duplicate_window() {
    tokio::time::sleep(DUPLICATE_WINDOW + Duration::from_millis(500)).await;
}

/// The `n`-th answer to `command`, waited for.
async fn nth_answer(ctx: &TestContext, command: Uuid, n: usize, what: &str) -> Answer {
    let held = br_test_harness::wait_until(PUSH_TIMEOUT, || async {
        ctx.answers.for_command(command).len() >= n
    })
    .await;
    let answers = ctx.answers.for_command(command);
    assert!(held, "{what}: {n} answers expected, got {answers:?}");
    answers[n - 1].clone()
}

/// Sends `command` once more under `id`; answers the `n`-th answer to it.
async fn again<C: ThreadsCommand + Serialize>(
    ctx: &TestContext,
    id: Uuid,
    command: &C,
    n: usize,
    what: &str,
) -> Answer {
    send_as(&ctx.nats, commanding_service(), id, command).await;
    nth_answer(ctx, id, n, what).await
}

fn refusal(
    answer: &Answer,
    what: &str,
) -> (ReasonCode, serde_json::Map<String, serde_json::Value>) {
    match answer {
        Answer::MessageRefused(refused) => (refused.reason_code, refused.params.clone()),
        Answer::ThreadRefused(refused) => (refused.reason_code, refused.params.clone()),
        other => panic!("{what}: a refusal expected, got {other:?}"),
    }
}

fn confirmed(answer: &Answer, what: &str) {
    assert!(
        matches!(
            answer,
            Answer::MessageConfirmed(_) | Answer::ThreadConfirmed(_)
        ),
        "{what}: a confirmation expected, got {answer:?}"
    );
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t24_a_command_received_again_gets_the_same_answer_and_never_applies_its_effect_twice() {
    // Given S, OPEN, with member A; the broker's duplicate window on answers
    // shortened.
    let ctx = TestContext::setup().await;
    ctx.shorten_event_duplicate_window(DUPLICATE_WINDOW).await;
    let a = ctx.enrol("ada").await;
    let s = create_thread(&ctx, Some(&a), &[]).await;

    // When the harness stores M1, then sends the store again under its id.
    let store_m1 = user_message(s, None, a.id, vec![text("the first wording")]);
    let m1 = store_m1.message_id;
    let store = send(&ctx.nats, commanding_service(), &store_m1).await;
    let first = nth_answer(&ctx, store, 1, "the store").await;
    confirmed(&first, "the store");
    past_the_duplicate_window().await;
    let second = again(&ctx, store, &store_m1, 2, "the store sent again").await;
    // Then the same confirmed answer is published again; M1 is stored once.
    assert_eq!(second, first, "the stored answer, not a new one");
    let created = facts::of(&ctx.pool, "message", m1).await;
    assert_eq!(
        facts::types(&created)
            .iter()
            .filter(|t| **t == "Created")
            .count(),
        1
    );

    // Given M1 edited from version 1 to 2.
    let to_v2 = send(
        &ctx.nats,
        commanding_service(),
        &edit_of(s, m1, a.id, 1, vec![text("the second wording")]),
    )
    .await;
    confirmed(
        &nth_answer(&ctx, to_v2, 1, "the edit to v2").await,
        "the edit to v2",
    );
    // When an edit made from version 1 is refused, M1 changes, and that edit
    // is sent again under its id.
    let stale_edit = edit_of(s, m1, a.id, 1, vec![text("a stale wording")]);
    let stale = send(&ctx.nats, commanding_service(), &stale_edit).await;
    let first = nth_answer(&ctx, stale, 1, "the stale edit").await;
    let (code, params) = refusal(&first, "the stale edit");
    assert_eq!(
        (code, params["current_version"].clone()),
        (ReasonCode::VersionConflict, json!(2))
    );
    let to_v3_edit = edit_of(s, m1, a.id, 2, vec![text("the third wording")]);
    let to_v3 = send(&ctx.nats, commanding_service(), &to_v3_edit).await;
    confirmed(
        &nth_answer(&ctx, to_v3, 1, "the edit to v3").await,
        "the edit to v3",
    );
    past_the_duplicate_window().await;
    let second = again(&ctx, stale, &stale_edit, 2, "the stale edit sent again").await;
    // Then it is refused VERSION_CONFLICT again with the same current version
    // (2), although M1 is at 3 by now.
    assert_eq!(second, first, "the stored refusal, not a new judgment");

    // Given M2, A's second message, deleted.
    let m2 = say(&ctx, s, Some(m1), &a, "a message A deletes").await;
    let delete_m2 = deletion_of(s, m2, a.id);
    let delete = send(&ctx.nats, commanding_service(), &delete_m2).await;
    confirmed(
        &nth_answer(&ctx, delete, 1, "the deletion").await,
        "the deletion",
    );

    // When the stored answers of these commands expire, as the retention sweep
    // removes them, and each is sent again under its id.
    let m1_before = message_view(&ctx, &a.passport, s, m1).await;
    let m2_before = message_view(&ctx, &a.passport, s, m2).await;
    let m1_facts = facts::of(&ctx.pool, "message", m1).await.len();
    let m2_facts = facts::of(&ctx.pool, "message", m2).await.len();
    ctx.expire_answers(&[store, stale, to_v3, delete]).await;

    // Then each is judged anew on the current state, under its own guards:
    // the store is refused KEY_REUSED…
    let (code, _) = refusal(
        &again(&ctx, store, &store_m1, 3, "the store, expired").await,
        "the store",
    );
    assert_eq!(code, ReasonCode::KeyReused);
    // …the deletion already applied is confirmed and changes nothing…
    match again(&ctx, delete, &delete_m2, 2, "the deletion, expired").await {
        Answer::MessageConfirmed(answer) => assert!(answer.deleted, "{answer:?}"),
        other => panic!("the deletion, expired: confirmed expected, got {other:?}"),
    }
    // …and the edits made from a version M1 has left are refused
    // VERSION_CONFLICT, naming its current version.
    for (id, edit, n, what) in [
        (to_v3, &to_v3_edit, 2, "the applied edit from v2, expired"),
        (stale, &stale_edit, 3, "the stale edit from v1, expired"),
    ] {
        let (code, params) = refusal(&again(&ctx, id, edit, n, what).await, what);
        assert_eq!(
            (code, params["current_version"].clone()),
            (ReasonCode::VersionConflict, json!(3)),
            "{what}"
        );
    }
    // And M1, M2 and their facts are those they were before.
    assert_eq!(message_view(&ctx, &a.passport, s, m1).await, m1_before);
    assert_eq!(message_view(&ctx, &a.passport, s, m2).await, m2_before);
    assert_eq!(facts::of(&ctx.pool, "message", m1).await.len(), m1_facts);
    assert_eq!(facts::of(&ctx.pool, "message", m2).await.len(), m2_facts);

    // Given T, OPEN, resolved then reopened.
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let resolve_t = resolution_of(t, a.id);
    let resolve = send(&ctx.nats, commanding_service(), &resolve_t).await;
    confirmed(
        &nth_answer(&ctx, resolve, 1, "the resolution").await,
        "the resolution",
    );
    let reopen = send(&ctx.nats, commanding_service(), &reopening_of(t, a.id)).await;
    confirmed(
        &nth_answer(&ctx, reopen, 1, "the reopening").await,
        "the reopening",
    );
    assert!(thread_view(&ctx, &a.passport, t).await["resolvedBy"].is_null());

    // When the resolution's stored answer expires and it is sent again under
    // its original id.
    ctx.expire_answers(&[resolve]).await;
    let answer = again(&ctx, resolve, &resolve_t, 2, "the resolution, expired").await;
    // Then it is judged on the current state and resolves T again: a new
    // effect, allowed once the stored answer has expired.
    confirmed(&answer, "the resolution, expired");
    assert_eq!(
        thread_view(&ctx, &a.passport, t).await["resolvedBy"],
        json!(a.id)
    );
    let resolutions = facts::of(&ctx.pool, "thread", t).await;
    assert_eq!(
        facts::types(&resolutions),
        vec![
            "Created",
            "MembersAdded",
            "Resolved",
            "Reopened",
            "Resolved"
        ]
    );

    ctx.teardown().await;
}
