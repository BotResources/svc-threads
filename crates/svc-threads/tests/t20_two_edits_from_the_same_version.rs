//! **T20 — Two edits from the same version: the second is refused.**
//!
//! > M1, by user A, is read at version v. The harness sends message.edit for M1
//! > with expected_version v and new content C1: confirmed, M1 is at version
//! > v+1. It then sends a second message.edit for M1 with expected_version v and
//! > content C2: refused VERSION_CONFLICT naming the current version v+1; M1
//! > still reads C1 and stays at v+1, and no Edited fact records C2. The same
//! > edit sent again with expected_version v+1 is confirmed. A command delivered
//! > twice answers once, with the same answer, and the message is edited once.
//!
//! A fact never carries content, so "no Edited fact records C2" reads: the
//! refused edit adds no Edited fact — M1 holds exactly one until C2 is applied.
//!
//! Channels: the answers (4), M1 as a member reads it (2), the fact log.

mod common;

use common::TestContext;
use common::content::{texts_of, user_text};
use common::facts;
use common::flows::{
    command, create_thread, edit_of, message_confirmed, message_refused, message_version,
    message_view, redeliver, replayed_alike, say,
};
use contract_threads::{MessageStatus, ReasonCode};

async fn edited_facts(ctx: &TestContext, message: uuid::Uuid) -> usize {
    facts::of(&ctx.pool, "message", message)
        .await
        .iter()
        .filter(|fact| fact.event_type == "Edited")
        .count()
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t20_an_edit_from_a_stale_version_is_refused_and_overwrites_nothing() {
    // Given M1, by A, read at version v.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "the first wording").await;
    let v = message_version(&ctx, &a.passport, t, m1).await;
    const C1: &str = "the wording C1";
    const C2: &str = "the wording C2";

    // When the harness edits M1 from v with C1.
    let first = command(&ctx, &edit_of(t, m1, a.id, v, vec![user_text(C1)])).await;
    // Then confirmed, M1 at v+1.
    message_confirmed(&ctx, first, MessageStatus::Completed, "the edit to C1").await;
    assert_eq!(message_version(&ctx, &a.passport, t, m1).await, v + 1);

    // When a second edit, also made from v, brings C2.
    let stale = command(&ctx, &edit_of(t, m1, a.id, v, vec![user_text(C2)])).await;
    // Then refused VERSION_CONFLICT, naming the current version v+1.
    let refused = message_refused(
        &ctx,
        stale,
        ReasonCode::VersionConflict,
        "the edit to C2 from v",
    )
    .await;
    assert_eq!(
        refused.params["current_version"].as_u64(),
        Some(v + 1),
        "{refused:?}"
    );
    assert_eq!(
        refused.params["expected_version"].as_u64(),
        Some(v),
        "{refused:?}"
    );
    // And M1 still reads C1 at v+1; the refused edit recorded nothing.
    let view = message_view(&ctx, &a.passport, t, m1).await;
    assert_eq!(texts_of(&view), vec![C1.to_string()]);
    assert_eq!(view["version"].as_u64(), Some(v + 1));
    assert_eq!(
        edited_facts(&ctx, m1).await,
        1,
        "only the edit to C1 is recorded"
    );

    // When an edit from v also carries content the rules refuse (no text).
    let stale_invalid = command(
        &ctx,
        &edit_of(
            t,
            m1,
            a.id,
            v,
            vec![common::content::warning_block("note", "no text")],
        ),
    )
    .await;
    // Then it is refused VERSION_CONFLICT: the version is judged before the
    // content.
    message_refused(
        &ctx,
        stale_invalid,
        ReasonCode::VersionConflict,
        "a stale edit with invalid content",
    )
    .await;

    // When the same edit is sent again, made from v+1.
    let edit_c2 = edit_of(t, m1, a.id, v + 1, vec![user_text(C2)]);
    let current = command(&ctx, &edit_c2).await;
    // Then confirmed: M1 reads C2 at v+2.
    message_confirmed(
        &ctx,
        current,
        MessageStatus::Completed,
        "the edit to C2 from v+1",
    )
    .await;
    let edited = message_view(&ctx, &a.passport, t, m1).await;
    assert_eq!(texts_of(&edited), vec![C2.to_string()]);
    assert_eq!(edited["version"].as_u64(), Some(v + 2));

    // When the bus delivers that edit command twice.
    redeliver(&ctx, current, &edit_c2).await;
    // Then it answers once, alike, and M1 is edited once.
    replayed_alike(&ctx, current, "the redelivered edit").await;
    assert_eq!(message_view(&ctx, &a.passport, t, m1).await, edited);
    assert_eq!(edited_facts(&ctx, m1).await, 2);

    ctx.teardown().await;
}
