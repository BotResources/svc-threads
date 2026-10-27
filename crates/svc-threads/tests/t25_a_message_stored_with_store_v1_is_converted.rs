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
//! > before. A v1 AI message holding an ARTIFACT block naming an uploaded
//! > artifact is stored with the ARTIFACT as an annotation after its entry, and
//! > the artifact is ATTACHED. An edit sent in the previous version's form,
//! > with blocks, is converted and applied as an edit with content.
//!
//! The v1 commands are built with `contract-threads` 0.4.0 itself, by its tag —
//! exactly what a consumer still on v1 sends. "The same refusal as before" is
//! read on the reason code, the contract of a refusal; the vocabulary of a
//! v1 `MESSAGE_INVALID`'s `error` param is not asserted (see the report). The
//! refusals before are 0.4's own: a block id that is not a UUIDv7 is refused
//! ID_NOT_UUIDV7, a block id used twice — in the message, or by another
//! message — KEY_REUSED, for a v1 store and an edit in the 0.4 form alike.
//!
//! Channels: the answers on the same events (4), the messages as members read
//! them (2), O's live feed of T — each stored or edited message pushed, nothing
//! for a refused one (3) — a runner's context, the artifact's facts and bytes.

mod common;

use common::artifacts::{File, download_url, fetch, upload};
use common::content::{
    closure_kind, complete_entry, entries_of, kinds_of, text_as, texts_of, to_json, turn_of,
};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    active_branch, context, conversation_of, create_thread, message_confirmed, message_refused,
    message_version, message_view, say, thread_confirmed, uuid_of,
};
use common::previous::{
    ai_message, ai_text, artifact_block, author, command_v1, edit_of, step, streaming_message,
    tool_call, tool_result, tool_results, user_message, user_text_as,
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
    // Given OPEN thread T with members O and A, O following it; X in the
    // roster, not a member.
    let ctx = TestContext::setup().await;
    let (o, a, x) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("xan").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a]).await;
    let mut o_feed = Live::thread(&ctx, &o.passport, t).await;

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
    // And (3) O's feed receives M1.
    o_feed
        .until("M1 reaches O's feed", |delta| {
            upsert_of(delta, m1).is_some()
        })
        .await;

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
    // And (3) O's feed receives M2.
    o_feed
        .until("M2 reaches O's feed", |delta| {
            upsert_of(delta, m2).is_some()
        })
        .await;
    o_feed.drain().await;
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
    o_feed.silence("a refused v1 store pushes nothing").await;

    // When a v1 AI message holds a step with a text and an ARTIFACT block
    // naming an artifact a runner uploaded for T.
    let file = File::csv("quarter,revenue\nQ3,42\n");
    let artifact = upload(&ctx, &runner.passport, t, &file).await;
    let attaching = artifact_block(artifact);
    let attaching_id = attaching.id;
    let store_m3 = ai_message(
        t,
        Some(m2),
        author(&agent_id.to_string(), "Analyst"),
        vec![step(
            StopReason::EndTurn,
            None,
            vec![ai_text("the figures are attached"), attaching],
        )],
    );
    let m3 = store_m3.message_id;
    let sent = command_v1(&ctx, &store_m3).await;
    // Then it is stored, the ARTIFACT an annotation after its entry under the
    // block's id, and the artifact ATTACHED: members read its bytes.
    message_confirmed(&ctx, sent, MessageStatus::Completed, "M3 by v1").await;
    let view = message_view(&ctx, &a.passport, t, m3).await;
    assert_eq!(
        kinds_of(&view),
        vec!["model_response", "turn_closure"],
        "{view}"
    );
    assert_eq!(
        entries_of(&view)[0]["annotations"],
        json!([{
            "id": attaching_id,
            "index": 0,
            "kind": "ARTIFACT",
            "content": { "artifact_id": artifact },
        }]),
        "{view}"
    );
    assert_eq!(
        facts::types(&facts::of(&ctx.pool, "artifact", artifact).await),
        vec!["Requested", "Attached"]
    );
    let url = download_url(&ctx, &a.passport, t, artifact)
        .await
        .expect("a member reads the attached artifact");
    assert_eq!(fetch(&url).await, (200, file.bytes.clone()));
    turn_of(&view);
    // And (3) O's feed receives M3.
    o_feed
        .until("M3 reaches O's feed", |delta| {
            upsert_of(delta, m3).is_some()
        })
        .await;

    // When A edits M1 in the previous version's form, with blocks.
    const REWORDED: &str = "what were the Q3 figures, in euros?";
    let reworded = Uuid::now_v7();
    let version = message_version(&ctx, &a.passport, t, m1).await;
    let sent = command_v1(
        &ctx,
        &edit_of(t, m1, a.id, version, vec![user_text_as(reworded, REWORDED)]),
    )
    .await;
    // Then it is converted and applied as an edit with content: M1 reads the
    // new text under the new block's id, its version raised by one, and (3)
    // O's feed receives the edit.
    message_confirmed(&ctx, sent, MessageStatus::Completed, "a 0.4-form edit").await;
    let view = message_view(&ctx, &o.passport, t, m1).await;
    assert_eq!(view["version"], json!(version + 1), "{view}");
    assert!(view["editedAt"].is_string(), "{view}");
    assert_eq!(texts_of(&view), vec![REWORDED.to_string()]);
    let TurnEntry::HumanMessage(edited) = complete_entry(&entries_of(&view)[0]) else {
        panic!("a human message: {view}");
    };
    assert_eq!(edited.human().as_uuid(), a.id);
    assert_eq!(
        to_json(&edited.content()),
        to_json(&vec![text_as(reworded, REWORDED)]),
        "the new text, under the new block's id"
    );
    o_feed
        .until("the edit of M1 reaches O's feed", |delta| {
            upsert_of(delta, m1).is_some_and(|view| view["version"] == json!(version + 1))
        })
        .await;
    o_feed.drain().await;

    // When 0.4-form edits name a block id that is not a UUIDv7, one id twice,
    // or a block id another message holds.
    let version = version + 1;
    let twice = Uuid::now_v7();
    for (blocks, code, what) in [
        (
            vec![user_text_as(Uuid::new_v4(), "hi")],
            ReasonCode::IdNotUuidV7,
            "a 0.4-form edit with a block id that is not a UUIDv7",
        ),
        (
            vec![user_text_as(twice, "one"), user_text_as(twice, "two")],
            ReasonCode::KeyReused,
            "a 0.4-form edit naming one id twice",
        ),
        (
            vec![user_text_as(said_id, "taken")],
            ReasonCode::KeyReused,
            "a 0.4-form edit reusing M2's block id",
        ),
    ] {
        let sent = command_v1(&ctx, &edit_of(t, m1, a.id, version, blocks)).await;
        // Then each gets the refusal 0.4 gave, and M1 is unchanged.
        message_refused(&ctx, sent, code, what).await;
        assert_eq!(
            message_version(&ctx, &a.passport, t, m1).await,
            version,
            "{what}"
        );
    }
    o_feed
        .silence("a refused 0.4-form edit pushes nothing")
        .await;

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
            user_message(t, Some(m3), a.id, vec![user_text_as(Uuid::new_v4(), "hi")]),
            ReasonCode::IdNotUuidV7,
            "a block id that is not a UUIDv7",
        ),
        (
            user_message(
                t,
                Some(m3),
                a.id,
                vec![user_text_as(twice, "one"), user_text_as(twice, "two")],
            ),
            ReasonCode::KeyReused,
            "one block id twice",
        ),
        (
            user_message(t, Some(m3), a.id, vec![user_text_as(reworded, "taken")]),
            ReasonCode::KeyReused,
            "a block id M1 holds",
        ),
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
        let refused_id = store.message_id;
        let sent = command_v1(&ctx, &store).await;
        // Then each gets the same refusal as before, and nothing is stored.
        message_refused(&ctx, sent, code, what).await;
        // (The reused id is M1's own: M1 is stored, untouched.)
        if refused_id != m1 {
            assert!(
                facts::of(&ctx.pool, "message", refused_id).await.is_empty(),
                "{what}"
            );
        }
    }
    o_feed.silence("a refused v1 store pushes nothing").await;
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
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1, m2, m3]);

    ctx.teardown().await;
}
