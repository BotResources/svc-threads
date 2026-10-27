//! **T28 — Ids are UUIDv7 and used once in the thread.**
//!
//! > On thread T holding message M1 with entry e1 and block b1: storing M2 whose
//! > block reuses b1's id, or whose entry reuses e1's id, is refused KEY_REUSED;
//! > an id used twice inside M2 is refused MESSAGE_INVALID naming the library's
//! > error code; an AI message whose agent id is not a UUIDv7 is refused
//! > MESSAGE_INVALID; an annotation whose id is not a UUIDv7 is refused
//! > ID_NOT_UUIDV7. Nothing is stored. M2 with fresh UUIDv7 ids is confirmed.
//!
//! Every attempt is sent under the one id M2 — "nothing is stored" then bites
//! twice: no fact of M2 after each refusal, and the last store of M2, with
//! fresh ids, is confirmed instead of refused KEY_REUSED. The content the
//! library refuses is the library's own JSON, mutated; the code it names is
//! the library's, on that very JSON.
//!
//! Channels: the answers (4), the active branch as a member reads it (2), the
//! fact log.

mod common;

use common::TestContext;
use common::content::{
    after, ai_turn, closed, human, model_response, said, text, text_as, to_json, turn_refusal,
    warning,
};
use common::facts;
use common::flows::{
    active_branch, ai, command, create_thread, message_confirmed, message_refused,
    raw_whole_message, store, streaming_message, user_entry_message, user_message, whole_message,
};
use contract_threads::llm::{ModelResponse, ResponseEnding, ResponseMetadata, Turn, TurnEntry};
use contract_threads::{MessageStatus, MessageTurn, ReasonCode, StoreMessageV2};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t28_every_id_of_a_message_is_a_uuidv7_used_once_in_the_thread() {
    // Given T, with member A, holding M1: entry e1 holding block b1.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let b1 = text("the first block");
    let b1_id = b1.id().as_uuid();
    let e1 = human(a.id, vec![b1]);
    let m1 = store(
        &ctx,
        user_entry_message(t, None, a.id, &e1),
        MessageStatus::Completed,
    )
    .await;
    let analyst = ai("a1", "Analyst");
    let m2 = Uuid::now_v7();
    let refused = |store: StoreMessageV2, code: ReasonCode, what: &'static str| {
        let (ctx, a) = (&ctx, &a);
        async move {
            let sent = command(ctx, &store).await;
            let answer = message_refused(ctx, sent, code, what).await;
            // Then nothing is stored.
            assert!(
                facts::of(&ctx.pool, "message", m2).await.is_empty(),
                "{what}"
            );
            assert_eq!(active_branch(ctx, &a.passport, t).await, vec![m1], "{what}");
            answer
        }
    };
    let as_m2 = |mut store: StoreMessageV2| {
        store.message_id = m2;
        store
    };

    // When M2's block reuses b1's id.
    refused(
        as_m2(user_message(
            t,
            Some(m1),
            a.id,
            vec![text_as(b1_id, "mine")],
        )),
        ReasonCode::KeyReused,
        "a block reusing b1's id",
    )
    .await;

    // When M2's entry reuses e1's id.
    let reusing_entry = TurnEntry::from(
        ModelResponse::new(
            e1.id(),
            vec![said("an answer")],
            ResponseEnding::finished(),
            ResponseMetadata::new(),
        )
        .expect("a model response on its own"),
    );
    let turn = Turn::from(ai_turn(m2, &analyst, closed(vec![reusing_entry])));
    refused(
        whole_message(t, Some(m1), m2, &analyst, &turn, Vec::new()),
        ReasonCode::KeyReused,
        "an entry reusing e1's id",
    )
    .await;

    // When an id is used twice inside M2: its response's two blocks under one.
    let response = TurnEntry::from(model_response(
        vec![said("first"), said("second")],
        ResponseEnding::finished(),
        None,
    ));
    let mut twice = to_json(&Turn::from(ai_turn(m2, &analyst, closed(vec![response]))));
    let first_id = twice["entries"][0]["content"][0]["id"].clone();
    twice["entries"][0]["content"][1]["id"] = first_id;
    let code = turn_refusal(&twice).expect("the library refuses an id used twice");
    let answer = refused(
        raw_whole_message(t, Some(m1), m2, &analyst, twice, Vec::new()),
        ReasonCode::MessageInvalid,
        "an id used twice inside M2",
    )
    .await;
    // Then the refusal names the library's code.
    assert_eq!(answer.params["error"], json!(code), "{answer:?}");

    // When an AI message names an agent id that is not a UUIDv7: opened
    // STREAMING, and whole, its turn naming that agent.
    let v4 = Uuid::new_v4();
    let mut opened = as_m2(streaming_message(t, Some(m1), &analyst));
    if let MessageTurn::Ai(message) = &mut opened.message {
        message.agent_id = v4;
    }
    refused(
        opened,
        ReasonCode::MessageInvalid,
        "a streaming AI message of a v4 agent",
    )
    .await;
    let mut whole = to_json(&Turn::from(ai_turn(
        m2,
        &analyst,
        closed(common::content::reply("hello")),
    )));
    whole["agent"] = Value::String(v4.to_string());
    let mut foreign_agent = raw_whole_message(t, Some(m1), m2, &analyst, whole, Vec::new());
    if let MessageTurn::Ai(message) = &mut foreign_agent.message {
        message.agent_id = v4;
    }
    refused(
        foreign_agent,
        ReasonCode::MessageInvalid,
        "a whole AI turn of a v4 agent",
    )
    .await;

    // When an annotation's id is not a UUIDv7.
    let answered = common::content::reply("annotated");
    let turn = Turn::from(ai_turn(m2, &analyst, closed(answered.clone())));
    let mut note = warning("NOTE", "an annotation");
    note.id = Uuid::new_v4();
    refused(
        whole_message(
            t,
            Some(m1),
            m2,
            &analyst,
            &turn,
            vec![after(&answered[0], note)],
        ),
        ReasonCode::IdNotUuidV7,
        "an annotation id that is not a UUIDv7",
    )
    .await;

    // When M2 is stored with fresh UUIDv7 ids.
    let fresh = common::content::reply("fresh ids");
    let turn = Turn::from(ai_turn(m2, &analyst, closed(fresh.clone())));
    let sent = command(
        &ctx,
        &whole_message(
            t,
            Some(m1),
            m2,
            &analyst,
            &turn,
            vec![after(&fresh[0], warning("NOTE", "a fresh annotation"))],
        ),
    )
    .await;
    // Then it is confirmed.
    message_confirmed(&ctx, sent, MessageStatus::Completed, "M2 with fresh ids").await;
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1, m2]);

    ctx.teardown().await;
}
