mod common;

use common::content::{
    Streamed, after, ai_turn, closed, human, mail, model_response, reply, said, text, text_as,
    to_json, turn_refusal, warning,
};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    active_branch, ai, await_message, command, completion, create_thread, message_confirmed,
    message_refused, open_stream, raw_whole_message, store, streaming_message, user_entry_message,
    user_message, whole_message,
};
use common::{TestContext, stream};
use contract_threads::llm::{
    AgentId, ModelResponse, ResponseEnding, ResponseMetadata, Turn, TurnEntry,
};
use contract_threads::{
    Chunk, LibraryJson, MessageStatus, MessageTurn, ReasonCode, StoreMessageV2,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t28_every_id_of_a_message_is_a_uuidv7_used_once_in_the_thread() {
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
        refused(store, ReasonCode::IdNotUuidV7, what).await;
    }

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
    assert_eq!(answer.params["error"], json!(code), "{answer:?}");

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
    assert_eq!(answer.params["error"], json!(whole_code), "{answer:?}");

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
        let answer = refused(store, ReasonCode::MessageInvalid, what).await;
        assert_eq!(answer.params["error"], json!(error), "{what}: {answer:?}");
    }
    a_feed.silence("a refused store reaches no one").await;

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
    message_confirmed(&ctx, sent, MessageStatus::Completed, "M2 with fresh ids").await;
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1, m2]);
    a_feed
        .until("M2 reaches A's feed", |delta| {
            upsert_of(delta, m2).is_some()
        })
        .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t28_a_streamed_message_reusing_a_held_entry_id_is_stored_failed_with_reused_identity() {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let analyst = ai("a1", "Analyst");
    let e1 = human(a.id, vec![text("the first block")]);
    let m1 = store(
        &ctx,
        user_entry_message(t, None, a.id, &e1),
        MessageStatus::Completed,
    )
    .await;

    let m2 = open_stream(&ctx, t, Some(m1), analyst.clone()).await;
    let mut reusing = to_json(&mail(&analyst, t, vec![text("a note")]));
    reusing["id"] = Value::String(e1.id().as_uuid().to_string());
    let mut response = Streamed::start(m2, None);
    response.text(&["an answer"], true);
    response.complete(ResponseEnding::finished());
    let mut chunks = vec![Chunk::Entry {
        entry: LibraryJson::from_json(reusing),
    }];
    chunks.extend(response.chunks());
    let (last_seq, hash) = stream::push(&ctx.nats.url(), m2, 0, &chunks).await;
    let sent = command(&ctx, &completion(t, m2, last_seq, &hash)).await;
    message_confirmed(&ctx, sent, MessageStatus::Failed, "completing M2").await;

    let view = await_message(&ctx, &a.passport, t, m2, "M2 failed", |view| {
        view["status"] == "FAILED"
    })
    .await;
    assert_eq!(
        common::content::closure_kind(&view).as_deref(),
        Some("failed"),
        "{view}"
    );
    let failed = facts::of(&ctx.pool, "message", m2).await;
    assert_eq!(
        facts::the(&failed, "Failed").payload["reason"],
        "reused_identity"
    );
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1, m2]);

    ctx.teardown().await;
}
