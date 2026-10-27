//! **T19 — Editing a message: author only, the content is replaced, the node
//! stays.**
//!
//! > M1, by user A, has an AI answer M2 written on M1's text and a fork M2b.
//! > The harness sends message.edit for M1 with acting user B, also a member:
//! > refused NOT_THE_AUTHOR, nothing changes. With acting user A and M1's
//! > current version, new content keeping the id of M1's first text block and
//! > adding an image block: confirmed; M1 reads with the new content, an
//! > edited_at and a
//! > version raised by one, still the parent of M2 and M2b, which stay readable
//! > under it; a runner's context carries the new content. An Edited fact
//! > records it; no fact, before or after, carries the old or the new text, and
//! > no block of the old content is left in the store. A new block reusing the
//! > id of a block of another message is refused KEY_REUSED; a list naming one
//! > id twice is refused KEY_REUSED; an empty list, or one holding an annotation
//! > or a block the library does not allow in a human message, is refused
//! > MESSAGE_INVALID; a block id that is not a UUIDv7 is
//! > refused ID_NOT_UUIDV7; nothing changes. Editing M2, the AI's message, with
//! > acting user A is refused MESSAGE_NOT_EDITABLE. A streaming, a failed and a
//! > cancelled AI message, and a deleted user message of A, are refused
//! > MESSAGE_NOT_EDITABLE. Editing an unknown message is refused
//! > MESSAGE_NOT_FOUND. After the thread is closed, editing M1 is refused
//! > THREAD_NOT_OPEN. After the thread is resolved, editing M1 is refused
//! > THREAD_RESOLVED, and is allowed again once the thread is reopened. An edit
//! > made long after M1 was stored, on an OPEN thread, is confirmed: there is no
//! > time limit.
//!
//! "Long after" is not a duration a scenario can wait out; what it proves is
//! that no time limit is ever consulted: the last edit comes after the whole
//! scenario — streams opened and closed, a resolution and its reopening — on a
//! message stored at its start, and nothing but the thread, the message, its
//! author and its version decides it. The closing comes last, since a closed
//! thread never reopens.
//!
//! "No block of the old content is left in the store" is read over every
//! table of the store (`common::store`).
//!
//! Channels: the answers (4), the messages as members read them (2), the fact
//! log and the store, a runner's context.

mod common;

use common::content::{
    entries_of, image, reply, text, text_as, texts_of, thinking, to_json, warning,
};
use common::facts;
use common::flows::{
    ai, answer, branch, command, context, conversation_of, create_thread, deletion_of, edit_of,
    message_confirmed, message_refused, message_version, message_view, open_stream, raw_edit_of,
    reopening_of, resolution_of, say, thread_confirmed, uuid_of,
};
use common::{Person, TestContext, store};
use contract_threads::llm::{IncomingEntry, PlainBlock};
use contract_threads::{
    CancelMessage, CloseThread, ClosingStatus, FailMessage, MessageStatus, ReasonCode,
};
use serde_json::Value;
use uuid::Uuid;

/// Sends `edit` and asserts it refused with `code`, M1 and the fact log left
/// exactly as they were.
async fn refused_changing_nothing(
    ctx: &TestContext,
    reader: &Person,
    t: Uuid,
    m1: Uuid,
    edit: &contract_threads::EditMessage,
    code: ReasonCode,
    what: &str,
) {
    let before = message_view(ctx, &reader.passport, t, m1).await;
    let facts_before = facts::count(&ctx.pool).await;
    let sent = command(ctx, edit).await;
    message_refused(ctx, sent, code, what).await;
    assert_eq!(
        message_view(ctx, &reader.passport, t, m1).await,
        before,
        "{what}: M1 is unchanged"
    );
    assert_eq!(
        facts::count(&ctx.pool).await,
        facts_before,
        "{what}: no fact"
    );
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t19_only_its_author_edits_a_completed_user_message_in_place_and_its_place_and_descendants_stay()
 {
    // Given T with members O, A and B; M1 by A; M2, an AI answer written on
    // M1's text, and its fork M2b; a runner.
    let ctx = TestContext::setup().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b]).await;
    const OLD: &str = "the old wording of my question";
    const NEW: &str = "the new wording of my question";
    const ADDED: &str = "and a second paragraph";
    let first_block = Uuid::now_v7();
    let m1 = common::flows::store(
        &ctx,
        common::flows::user_message(t, None, a.id, vec![text_as(first_block, OLD)]),
        MessageStatus::Completed,
    )
    .await;
    let m2 = answer(
        &ctx,
        t,
        Some(m1),
        ai("a1", "Analyst"),
        reply("an answer to the old wording"),
    )
    .await;
    let m2b = answer(
        &ctx,
        t,
        Some(m1),
        ai("a1", "Analyst"),
        reply("another answer"),
    )
    .await;
    let runner = ctx.service_account(true);
    let v = message_version(&ctx, &a.passport, t, m1).await;
    let created_at = message_view(&ctx, &a.passport, t, m1).await["createdAt"].clone();
    let added = image();
    let new_content: Vec<PlainBlock> = vec![text_as(first_block, NEW), added.clone()];

    // When the harness edits M1 with acting user B.
    // Then refused NOT_THE_AUTHOR, and nothing changes.
    refused_changing_nothing(
        &ctx,
        &a,
        t,
        m1,
        &edit_of(t, m1, b.id, v, new_content.clone()),
        ReasonCode::NotTheAuthor,
        "B editing A's message",
    )
    .await;

    // When the harness edits M1 with acting user A, from its current version,
    // keeping its first text block's id and adding an image block.
    let edited = command(&ctx, &edit_of(t, m1, a.id, v, new_content.clone())).await;

    // Then (4) confirmed: M1 is a COMPLETED message, not deleted.
    let confirmed = message_confirmed(&ctx, edited, MessageStatus::Completed, "A editing M1").await;
    assert_eq!(confirmed.message_id, m1);
    assert!(!confirmed.deleted);
    // And (2) M1 reads with the new content, an edited_at, its version + 1.
    let view = message_view(&ctx, &o.passport, t, m1).await;
    let read = &entries_of(&view)[0];
    assert_eq!(read["kind"], "human_message", "{view}");
    assert_eq!(
        read["content"]["content"],
        Value::Array(new_content.iter().map(to_json).collect()),
        "the human message's content, replaced whole"
    );
    assert_eq!(texts_of(&view), vec![NEW.to_string()]);
    assert_eq!(uuid_of(&read["content"]["content"][0]["id"]), first_block);
    assert!(view["editedAt"].is_string(), "{view}");
    assert_eq!(view["version"].as_u64(), Some(v + 1));
    assert_eq!(
        view["createdAt"], created_at,
        "an edit keeps the creation date"
    );
    // And it is still the parent of M2 and M2b, which stay readable under it.
    for reply in [m2, m2b] {
        let page: Vec<Uuid> = branch(&ctx, &b.passport, t, 10, Some(reply))
            .await
            .iter()
            .map(|view| uuid_of(&view["id"]))
            .collect();
        assert_eq!(
            page,
            vec![reply, m1],
            "the replies stay under the edited M1"
        );
    }
    // And a runner's context carries the new content, not the old.
    let read_context = context(&ctx, &runner.passport, t, None).await;
    let conversation = conversation_of(&read_context);
    let first = conversation.turns()[0]
        .as_input()
        .expect("M1, an input turn");
    let IncomingEntry::HumanMessage(edited) = &first.entries()[0] else {
        panic!("M1 holds A's human message: {conversation:?}");
    };
    assert_eq!(edited.content(), new_content.as_slice());
    assert!(!read_context["conversation"].to_string().contains(OLD));
    // And an Edited fact records it; no fact carries the old or the new text.
    let m1_facts = facts::of(&ctx.pool, "message", m1).await;
    let edited_fact = facts::the(&m1_facts, "Edited");
    assert_eq!(edited_fact.acting_user_id, Some(a.id));
    assert_eq!(edited_fact.version as u64, v + 1);
    for text in [OLD, NEW] {
        facts::assert_no_fact_carries(&ctx.pool, text, "after the edit").await;
    }
    // And no block of the old content is left in the store.
    store::assert_nowhere(&ctx.pool, OLD, &[], "the old content").await;

    // When edits carry content the rules refuse — then each is refused and
    // nothing changes.
    let v = v + 1;
    let m2_block = uuid_of(
        &entries_of(&message_view(&ctx, &a.passport, t, m2).await)[0]["content"]["content"][0]["id"],
    );
    let twice = Uuid::now_v7();
    let mut not_v7 = to_json(&text(NEW));
    not_v7["id"] = Value::String(Uuid::new_v4().to_string());
    for (content, code, what) in [
        (
            vec![to_json(&text_as(m2_block, NEW))],
            ReasonCode::KeyReused,
            "a block id held by another message",
        ),
        (
            vec![
                to_json(&text_as(twice, NEW)),
                to_json(&text_as(twice, ADDED)),
            ],
            ReasonCode::KeyReused,
            "one id named twice",
        ),
        (Vec::new(), ReasonCode::MessageInvalid, "an empty list"),
        (
            vec![
                to_json(&text(NEW)),
                to_json(&warning("NOTE", "an annotation")),
            ],
            ReasonCode::MessageInvalid,
            "a list holding an annotation",
        ),
        (
            vec![
                to_json(&text(NEW)),
                to_json(&thinking("not a human's", "c2ln")),
            ],
            ReasonCode::MessageInvalid,
            "a block the library does not allow in a human message",
        ),
        (
            vec![not_v7],
            ReasonCode::IdNotUuidV7,
            "a block id that is not a UUIDv7",
        ),
    ] {
        refused_changing_nothing(
            &ctx,
            &a,
            t,
            m1,
            &raw_edit_of(t, m1, a.id, v, content),
            code,
            what,
        )
        .await;
    }

    // When the harness edits M2, the AI's message, with acting user A — then
    // refused MESSAGE_NOT_EDITABLE.
    let m2_version = message_version(&ctx, &a.passport, t, m2).await;
    let sent = command(&ctx, &edit_of(t, m2, a.id, m2_version, vec![text(NEW)])).await;
    message_refused(
        &ctx,
        sent,
        ReasonCode::MessageNotEditable,
        "editing an AI message",
    )
    .await;

    // When a streaming, a failed and a cancelled AI message, and a deleted user
    // message of A are edited — then each is refused MESSAGE_NOT_EDITABLE.
    let streaming = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let failed = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let ended = command(
        &ctx,
        &FailMessage {
            message_id: failed,
            thread_id: t,
            seal: None,
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(&ctx, ended, MessageStatus::Failed, "failing an AI message").await;
    let cancelled = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let ended = command(
        &ctx,
        &CancelMessage {
            message_id: cancelled,
            thread_id: t,
            seal: None,
            acting_user_id: Some(a.id),
        },
    )
    .await;
    message_confirmed(
        &ctx,
        ended,
        MessageStatus::Cancelled,
        "cancelling an AI message",
    )
    .await;
    let gone = say(&ctx, t, Some(m2b), &a, "a message A deletes").await;
    let deleted = command(&ctx, &deletion_of(t, gone, a.id)).await;
    message_confirmed(&ctx, deleted, MessageStatus::Completed, "A deleting it").await;
    for (message, what) in [
        (streaming, "a streaming AI message"),
        (failed, "a failed AI message"),
        (cancelled, "a cancelled AI message"),
        (gone, "a deleted user message of A"),
    ] {
        let version = message_version(&ctx, &a.passport, t, message).await;
        let sent = command(&ctx, &edit_of(t, message, a.id, version, vec![text(NEW)])).await;
        message_refused(&ctx, sent, ReasonCode::MessageNotEditable, what).await;
    }

    // When an unknown message is edited — then refused MESSAGE_NOT_FOUND.
    let sent = command(&ctx, &edit_of(t, Uuid::now_v7(), a.id, 1, vec![text(NEW)])).await;
    message_refused(
        &ctx,
        sent,
        ReasonCode::MessageNotFound,
        "an unknown message",
    )
    .await;

    // When the thread is resolved, editing M1 — then refused THREAD_RESOLVED.
    let resolved = command(&ctx, &resolution_of(t, o.id)).await;
    thread_confirmed(&ctx, resolved, "resolving T").await;
    refused_changing_nothing(
        &ctx,
        &a,
        t,
        m1,
        &edit_of(t, m1, a.id, v, vec![text(NEW)]),
        ReasonCode::ThreadResolved,
        "editing on a resolved thread",
    )
    .await;

    // When the thread is reopened, editing M1 — long after it was stored, as
    // the last gesture on the open thread — then confirmed: no time limit.
    let reopened = command(&ctx, &reopening_of(t, o.id)).await;
    thread_confirmed(&ctx, reopened, "reopening T").await;
    const LATE: &str = "a late rewording";
    let late = command(
        &ctx,
        &edit_of(t, m1, a.id, v, vec![text_as(first_block, LATE)]),
    )
    .await;
    message_confirmed(&ctx, late, MessageStatus::Completed, "the late edit").await;
    let view = message_view(&ctx, &a.passport, t, m1).await;
    assert_eq!(texts_of(&view), vec![LATE.to_string()]);
    assert_eq!(view["version"].as_u64(), Some(v + 1));

    // When the thread is closed, editing M1 — then refused THREAD_NOT_OPEN.
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
    refused_changing_nothing(
        &ctx,
        &a,
        t,
        m1,
        &edit_of(t, m1, a.id, v + 1, vec![text(NEW)]),
        ReasonCode::ThreadNotOpen,
        "editing on a closed thread",
    )
    .await;

    ctx.teardown().await;
}
