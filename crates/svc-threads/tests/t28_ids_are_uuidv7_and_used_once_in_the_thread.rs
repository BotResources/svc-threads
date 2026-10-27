//! **T28 — Ids are UUIDv7 and used once in the thread.**
//!
//! > On thread T holding message M1 with entry e1 and block b1: storing M2 whose
//! > block reuses b1's id, or whose entry reuses e1's id, is refused KEY_REUSED;
//! > an id used twice inside M2 is refused MESSAGE_INVALID naming the library's
//! > error code; an AI message whose agent id is not a UUIDv7 is refused
//! > MESSAGE_INVALID; an annotation whose id is not a UUIDv7 is refused
//! > ID_NOT_UUIDV7. Nothing is stored. M2 with fresh UUIDv7 ids is confirmed.
//! > Threads' own rules are refused MESSAGE_INVALID naming their code, and
//! > nothing is stored: a whole turn whose id is not the message id
//! > (turn_id_mismatch), an AI turn whose agent or author is not the command's
//! > agent_id and author_name (author_mismatch), an annotation placed after an
//! > entry the message does not hold (annotation_entry_unknown), and a USER form
//! > whose turn is not an input turn holding a human message (not_a_user_turn).
//!
//! An entry or a block whose id is not a UUIDv7 is refused ID_NOT_UUIDV7, as
//! an annotation's is: the id rule is Threads', judged before the library is
//! asked. An agent id that is not a UUIDv7 is the library's refusal: its
//! `MESSAGE_INVALID` names the library's code for that very id.
//!
//! Every attempt is sent under the one id M2 — "nothing is stored" then bites
//! twice: no fact of M2 after each refusal, and the last store of M2, with
//! fresh ids, is confirmed instead of refused KEY_REUSED. The content the
//! library refuses is the library's own JSON, mutated; the code it names is
//! the library's, on that very JSON.
//!
//! Channels: the answers (4), the active branch as a member reads it (2), A's
//! live feed of T — silent across every refusal, M2 pushed once confirmed (3) —
//! the fact log.

mod common;

use common::TestContext;
use common::content::{
    after, ai_turn, closed, human, mail, model_response, reply, said, text, text_as, to_json,
    turn_refusal, warning,
};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    active_branch, ai, command, create_thread, message_confirmed, message_refused,
    raw_whole_message, store, streaming_message, user_entry_message, user_message, whole_message,
};
use contract_threads::llm::{
    AgentId, ModelResponse, ResponseEnding, ResponseMetadata, Turn, TurnEntry,
};
use contract_threads::{MessageStatus, MessageTurn, ReasonCode, StoreMessageV2};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t28_every_id_of_a_message_is_a_uuidv7_used_once_in_the_thread() {
    // Given T, with member A following it, holding M1: entry e1 holding block
    // b1.
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
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
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

    // When M2's entry id, then its block id, is not a UUIDv7.
    let mine = to_json(&human(a.id, vec![text("mine")]));
    let mut entry_v4 = mine.clone();
    entry_v4["id"] = Value::String(Uuid::new_v4().to_string());
    let mut block_v4 = mine.clone();
    block_v4["content"][0]["id"] = Value::String(Uuid::new_v4().to_string());
    for (entry, what) in [
        (entry_v4, "an entry id that is not a UUIDv7"),
        (block_v4, "a block id that is not a UUIDv7"),
    ] {
        let mut store = as_m2(user_message(t, Some(m1), a.id, vec![text("placeholder")]));
        if let MessageTurn::User(user) = &mut store.message {
            user.entry = contract_threads::LibraryJson::from_json(entry);
        }
        // Then each is refused ID_NOT_UUIDV7.
        refused(store, ReasonCode::IdNotUuidV7, what).await;
    }

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
    let v4_code = AgentId::from_uuid(v4)
        .expect_err("the library refuses an agent id that is not a UUIDv7")
        .code()
        .as_str()
        .to_string();
    let mut opened = as_m2(streaming_message(t, Some(m1), &analyst));
    if let MessageTurn::Ai(message) = &mut opened.message {
        message.agent_id = v4;
    }
    let answer = refused(
        opened,
        ReasonCode::MessageInvalid,
        "a streaming AI message of a v4 agent",
    )
    .await;
    // Then the refusal names the library's code for that agent id.
    assert_eq!(answer.params["error"], json!(v4_code), "{answer:?}");
    let mut whole = to_json(&Turn::from(ai_turn(
        m2,
        &analyst,
        closed(common::content::reply("hello")),
    )));
    whole["agent"] = Value::String(v4.to_string());
    let whole_code = turn_refusal(&whole).expect("the library refuses a v4 agent's turn");
    let mut foreign_agent = raw_whole_message(t, Some(m1), m2, &analyst, whole, Vec::new());
    if let MessageTurn::Ai(message) = &mut foreign_agent.message {
        message.agent_id = v4;
    }
    let answer = refused(
        foreign_agent,
        ReasonCode::MessageInvalid,
        "a whole AI turn of a v4 agent",
    )
    .await;
    // Then the refusal names the library's code on that very turn.
    assert_eq!(answer.params["error"], json!(whole_code), "{answer:?}");

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

    // When Threads' own rules are broken, each by a message the library
    // accepts: a whole turn under another id than M2's; an AI turn of a1 sent
    // as a2's, then under another author name; an annotation after an entry M2
    // does not hold; a USER form whose entry is no human message.
    let critic = ai("a2", "Critic");
    let renamed = common::content::Agent {
        id: analyst.id,
        name: "Someone Else".to_string(),
    };
    let elsewhere = Turn::from(ai_turn(Uuid::now_v7(), &analyst, closed(reply("misfiled"))));
    let a1_turn = Turn::from(ai_turn(m2, &analyst, closed(reply("mine"))));
    let annotated_turn = Turn::from(ai_turn(m2, &analyst, closed(reply("annotated"))));
    let unheld = reply("never stored");
    let not_human = mail(&critic, t, vec![text("a mail is no user's")]);
    for (store, error, what) in [
        (
            whole_message(t, Some(m1), m2, &analyst, &elsewhere, Vec::new()),
            "turn_id_mismatch",
            "a turn under another id than M2's",
        ),
        (
            whole_message(t, Some(m1), m2, &critic, &a1_turn, Vec::new()),
            "author_mismatch",
            "a1's turn sent as a2's",
        ),
        (
            whole_message(t, Some(m1), m2, &renamed, &a1_turn, Vec::new()),
            "author_mismatch",
            "a1's turn under another author name",
        ),
        (
            whole_message(
                t,
                Some(m1),
                m2,
                &analyst,
                &annotated_turn,
                vec![after(&unheld[0], warning("NOTE", "misplaced"))],
            ),
            "annotation_entry_unknown",
            "an annotation after an entry M2 does not hold",
        ),
        (
            as_m2(user_entry_message(t, Some(m1), a.id, &not_human)),
            "not_a_user_turn",
            "a USER form holding no human message",
        ),
    ] {
        // Then each is refused MESSAGE_INVALID naming Threads' own code, and
        // nothing is stored.
        let answer = refused(store, ReasonCode::MessageInvalid, what).await;
        assert_eq!(answer.params["error"], json!(error), "{what}: {answer:?}");
    }
    // And no refusal reached A.
    a_feed.silence("a refused store reaches no one").await;

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
    // Then it is confirmed, and (3) pushed to A.
    message_confirmed(&ctx, sent, MessageStatus::Completed, "M2 with fresh ids").await;
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1, m2]);
    a_feed
        .until("M2 reaches A's feed", |delta| {
            upsert_of(delta, m2).is_some()
        })
        .await;

    ctx.teardown().await;
}
