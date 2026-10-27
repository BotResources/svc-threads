//! **T25 — A message stored with store.v1 is converted.**
//!
//! > The harness stores on thread T, with message.store v1, a user message M1
//! > of member A with one TEXT block: confirmed COMPLETED; M1 reads as an input
//! > turn holding A's human message with that text under the block's id, as if
//! > stored with v2. An AI message stored COMPLETED with v1, holding a step with
//! > a text and a tool call, then a tool results item answering it, reads as an
//! > AI turn holding a model response, runtime tool results and a completed
//! > closure; its author id, a UUIDv7, is its agent id and its author name its
//! > name. A v1 AI message whose author id is not a UUIDv7 is refused
//! > MESSAGE_INVALID. A v1 AI message opened STREAMING is refused
//! > MESSAGE_INVALID: its content could only follow under the previous chunk
//! > forms. A v1 store refused for any other reason gets the same refusal as
//! > before.
//!
//! The v1 commands are built with `contract-threads` 0.4.0 itself, by its tag —
//! exactly what a consumer still on v1 sends. "The same refusal as before" is
//! read on the reason code, the contract of a refusal; the vocabulary of a
//! v1 `MESSAGE_INVALID`'s `error` param is not asserted (see the report).
//!
//! Channels: the answers on the same events (4), the messages as members read
//! them (2), a runner's context.

mod common;

use common::content::{
    closure_kind, complete_entry, entries_of, kinds_of, text_as, to_json, turn_of,
};
use common::facts;
use common::flows::{
    active_branch, context, conversation_of, create_thread, message_confirmed, message_refused,
    message_view, say, thread_confirmed, uuid_of,
};
use common::previous::{
    ai_message, ai_text, author, command_v1, step, streaming_message, tool_call, tool_result,
    tool_results, user_message, user_text_as,
};
use common::{TestContext, identity};
use contract_threads::llm::{ContentBlock, IncomingEntry, ResponseEnding, TurnEntry};
use contract_threads::{CloseThread, ClosingStatus, MessageStatus, ReasonCode};
use contract_threads_v0_4::llm::StopReason;
use contract_threads_v0_4::{Block, BlockBody, LibraryBlock};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t25_a_message_stored_with_store_v1_is_converted_and_answered_on_the_same_events() {
    // Given OPEN thread T with members O and A; X in the roster, not a member.
    let ctx = TestContext::setup().await;
    let (o, a, x) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("xan").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a]).await;

    // When the harness stores, with message.store v1, A's user message M1: one
    // TEXT block.
    const QUESTION: &str = "what were the Q3 figures?";
    let block = Uuid::now_v7();
    let store_m1 = user_message(t, None, a.id, vec![user_text_as(block, QUESTION)]);
    let m1 = store_m1.message_id;
    let sent = command_v1(&ctx, &store_m1).await;

    // Then (4) message.confirmed, on the same event as a v2 store, names the
    // command, M1 and COMPLETED.
    let confirmed = message_confirmed(&ctx, sent, MessageStatus::Completed, "M1 by v1").await;
    assert_eq!((confirmed.message_id, confirmed.thread_id), (m1, t));
    assert_eq!(confirmed.command_type, "message.store");
    // And (2) M1 reads as an input turn holding A's human message with that
    // text under the block's id, as if stored with v2.
    let view = message_view(&ctx, &o.passport, t, m1).await;
    assert_eq!(view["turnKind"], "INPUT_TURN", "{view}");
    assert_eq!(view["author"]["userId"], json!(a.id));
    assert_eq!(view["author"]["name"], identity::display_name("ada"));
    assert_eq!(kinds_of(&view), vec!["human_message"]);
    let TurnEntry::HumanMessage(human) = complete_entry(&entries_of(&view)[0]) else {
        panic!("a human message: {view}");
    };
    assert_eq!(human.human().as_uuid(), a.id);
    assert_eq!(
        to_json(&human.content()),
        to_json(&vec![text_as(block, QUESTION)]),
        "the text, under the block's id"
    );
    assert_eq!(uuid_of(&entries_of(&view)[0]["id"]).get_version_num(), 7);
    turn_of(&view);

    // When an AI message is stored COMPLETED with v1: a step with a text and a
    // tool call, then a tool results item answering it — its author id a
    // UUIDv7.
    let agent_id = Uuid::now_v7();
    let (said, called, result) = (
        ai_text("let me look"),
        tool_call("call-1", "search", json!({ "q": "q3" })),
        tool_result("call-1", "search", "42"),
    );
    let (said_id, called_id, result_id) = (said.id, called.id, result.id);
    let store_m2 = ai_message(
        t,
        Some(m1),
        author(&agent_id.to_string(), "Analyst"),
        vec![
            step(
                StopReason::AwaitingToolResults,
                Some("m-1"),
                vec![said, called],
            ),
            tool_results(vec![result]),
        ],
    );
    let m2 = store_m2.message_id;
    let sent = command_v1(&ctx, &store_m2).await;

    // Then confirmed COMPLETED; M2 reads as an AI turn of that agent, under
    // that name, holding a model response, runtime tool results and a
    // completed closure, every block under its id.
    message_confirmed(&ctx, sent, MessageStatus::Completed, "M2 by v1").await;
    let view = message_view(&ctx, &a.passport, t, m2).await;
    assert_eq!(view["turnKind"], "AI_TURN", "{view}");
    assert_eq!(
        view["author"],
        json!({ "kind": "AI", "userId": null, "aiId": agent_id, "name": "Analyst" })
    );
    assert_eq!(
        kinds_of(&view),
        vec!["model_response", "runtime_tool_results", "turn_closure"],
        "{view}"
    );
    assert_eq!(closure_kind(&view).as_deref(), Some("completed"));
    let TurnEntry::ModelResponse(response) = complete_entry(&entries_of(&view)[0]) else {
        panic!("a model response: {view}");
    };
    assert_eq!(response.ending(), &ResponseEnding::tool_use());
    assert_eq!(response.model().map(|model| model.as_str()), Some("m-1"));
    let ids: Vec<Uuid> = response
        .blocks()
        .iter()
        .map(|block| block.id().as_uuid())
        .collect();
    assert_eq!(ids, vec![said_id, called_id]);
    assert!(matches!(response.blocks()[0], ContentBlock::Text(_)));
    assert!(matches!(
        response.blocks()[1],
        ContentBlock::RuntimeToolCall(_)
    ));
    let TurnEntry::RuntimeToolResults(answered) = complete_entry(&entries_of(&view)[1]) else {
        panic!("runtime tool results: {view}");
    };
    assert_eq!(answered.results()[0].id().as_uuid(), result_id);
    assert_eq!(
        answered.results()[0].call().as_uuid(),
        called_id,
        "the result answers the call, by the call's block id"
    );
    let turn = turn_of(&view);
    assert_eq!(
        turn.as_ai().expect("an AI turn").agent().as_uuid(),
        agent_id
    );
    // And a runner's context holds both, as the library loads them.
    let runner = ctx.service_account(true);
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    let first = conversation.turns()[0]
        .as_input()
        .expect("M1, an input turn");
    assert!(matches!(first.entries()[0], IncomingEntry::HumanMessage(_)));
    assert_eq!(
        conversation.turns()[1]
            .as_ai()
            .map(|turn| turn.agent().as_uuid()),
        Some(agent_id)
    );

    // When v1 AI messages arrive: one whose author id is not a UUIDv7, one
    // opened STREAMING.
    for (store, what) in [
        (
            ai_message(
                t,
                Some(m2),
                author("a1", "Analyst"),
                vec![step(StopReason::EndTurn, None, vec![ai_text("hello")])],
            ),
            "a v1 author id that is not a UUIDv7",
        ),
        (
            streaming_message(t, Some(m2), author(&Uuid::now_v7().to_string(), "Analyst")),
            "a v1 AI message opened STREAMING",
        ),
    ] {
        let refused_id = store.message_id;
        let sent = command_v1(&ctx, &store).await;
        // Then each is refused MESSAGE_INVALID, and nothing is stored.
        message_refused(&ctx, sent, ReasonCode::MessageInvalid, what).await;
        assert!(
            facts::of(&ctx.pool, "message", refused_id).await.is_empty(),
            "{what}"
        );
    }
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1, m2]);

    // When v1 stores are refused for the reasons a v1 store always was.
    let other = create_thread(&ctx, Some(&a), &[]).await;
    let elsewhere = say(&ctx, other, None, &a, "elsewhere").await;
    let mut reused = user_message(
        t,
        Some(m2),
        a.id,
        vec![user_text_as(Uuid::now_v7(), "again")],
    );
    reused.message_id = m1;
    let empty = Block::new(
        Uuid::now_v7(),
        BlockBody::Text(LibraryBlock::from_json(
            json!({ "type": "text", "text": "" }),
        )),
    );
    let cases = vec![
        (
            user_message(
                Uuid::now_v7(),
                None,
                a.id,
                vec![user_text_as(Uuid::now_v7(), "hi")],
            ),
            ReasonCode::ThreadNotFound,
            "an unknown thread",
        ),
        (
            user_message(t, Some(m2), x.id, vec![user_text_as(Uuid::now_v7(), "hi")]),
            ReasonCode::NotAMember,
            "a non-member author",
        ),
        (reused, ReasonCode::KeyReused, "a reused id"),
        (
            user_message(
                t,
                Some(Uuid::now_v7()),
                a.id,
                vec![user_text_as(Uuid::now_v7(), "hi")],
            ),
            ReasonCode::MessageNotFound,
            "a parent nobody stored",
        ),
        (
            user_message(
                t,
                Some(elsewhere),
                a.id,
                vec![user_text_as(Uuid::now_v7(), "hi")],
            ),
            ReasonCode::MessageInvalid,
            "a parent of another thread",
        ),
        (
            user_message(t, Some(m2), a.id, vec![empty]),
            ReasonCode::MessageInvalid,
            "an empty text",
        ),
    ];
    for (store, code, what) in cases {
        let sent = command_v1(&ctx, &store).await;
        // Then each gets the same refusal as before.
        message_refused(&ctx, sent, code, what).await;
    }
    let closed = common::flows::command(
        &ctx,
        &CloseThread {
            thread_id: t,
            status: ClosingStatus::Completed,
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, closed, "closing T").await;
    let late = command_v1(
        &ctx,
        &user_message(
            t,
            Some(m2),
            a.id,
            vec![user_text_as(Uuid::now_v7(), "late")],
        ),
    )
    .await;
    message_refused(&ctx, late, ReasonCode::ThreadNotOpen, "a closed thread").await;
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1, m2]);

    ctx.teardown().await;
}
