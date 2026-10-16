//! **T05 — Storing a complete user message.**
//!
//! > Playing the agents service, the harness stores on OPEN thread T a
//! > COMPLETED user message M1 authored by member A, with one TEXT block under
//! > a minted id and no parent: message.confirmed names the command and status
//! > COMPLETED; every member reads M1 with A's current display name, and a
//! > subscriber of T receives it. The message's Created fact carries kinds,
//! > positions, author and status, and no text. A message whose blocks the
//! > library refuses (an empty text, a TOOL_RESULT in a user input) is refused
//! > MESSAGE_INVALID and nothing is stored. A user author who is not a member
//! > is refused. A parent from another thread is refused MESSAGE_INVALID.
//! > Reusing M1's id is refused KEY_REUSED. The thread is closed COMPLETED:
//! > storing on it is refused THREAD_NOT_OPEN, as is any later close. Renaming
//! > A in Identity shows the new name on M1 everywhere, while no fact carries a
//! > name. An impersonated author's store carries the impersonator: it is in
//! > the Created fact, never on the message.
//!
//! The library's rules include those of `br-llm-messages` 0.5 an AI turn given
//! whole meets: a tool name every provider accepts, and a tool call id used
//! once in the conversation — on the branch of the message, before or after
//! it, a fork being another conversation. Each refusal names the library's
//! code in its `error` param.
//!
//! Channels: the answers (4), M1 as each member reads it with its affordance
//! (2), B's live feed of T (3), the fact log, a runner's context (the name
//! "everywhere"). Adversarial: a non-member author, a foreign parent, a reused
//! id, an impersonator, a redelivered command.

mod common;

use common::content::{
    ai_text, raw_text, step, text_content, tool_call, tool_result, tool_results, user_text,
};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    active_branch, ai, ai_message, command, context, conversation_of, create_thread,
    message_confirmed, message_refused, message_view, say, thread_confirmed, thread_refused,
    user_message,
};
use common::{TestContext, identity};
use contract_threads::llm::{Entry, StopReason, UserBlock};
use contract_threads::{
    Block, BlockBody, CloseThread, ClosingStatus, LibraryBlock, MessageStatus, ReasonCode,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t05_a_complete_user_message_is_stored_checked_by_the_library_and_never_leaks_content() {
    // Given OPEN thread T with members O, A and B; X in the roster, not a
    // member; B following T.
    let ctx = TestContext::setup().await;
    let (o, a, b, x) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
        ctx.enrol("xan").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b]).await;
    let mut b_feed = Live::thread(&ctx, &b.passport, t).await;

    // When the agents service stores M1 by A: one TEXT block, no parent.
    const SECRET: &str = "the quarterly figures are confidential";
    let store_m1 = user_message(t, None, a.id, vec![user_text(SECRET)]);
    let m1 = store_m1.message_id;
    let block = match &store_m1.message {
        contract_threads::NewMessage::User(user) => user.blocks[0].id,
        contract_threads::NewMessage::Ai(_) => unreachable!(),
    };
    let store_command = command(&ctx, &store_m1).await;

    // Then (4) message.confirmed names the command, M1 and COMPLETED.
    let confirmed =
        message_confirmed(&ctx, store_command, MessageStatus::Completed, "storing M1").await;
    assert_eq!(
        (confirmed.thread_id, confirmed.message_id, confirmed.deleted),
        (t, m1, false)
    );
    assert_eq!(confirmed.command_type, "message.store");
    common::flows::answered_once(&ctx, store_command, "storing M1").await;

    // And (2) every member reads M1: A's author line under A's current name,
    // its one TEXT block, and the read mark it offers.
    for person in [&o, &a, &b] {
        let view = message_view(&ctx, &person.passport, t, m1).await;
        assert_eq!(view["status"], "COMPLETED", "{view}");
        assert_eq!(view["deleted"], false);
        assert_eq!(view["parentMessageId"], Value::Null);
        assert_eq!(
            view["author"],
            json!({
                "kind": "USER",
                "userId": a.id,
                "aiId": null,
                "name": identity::display_name("ada"),
            })
        );
        assert_eq!(
            view["blocks"],
            json!([{ "id": block, "index": 0, "kind": "TEXT", "content": text_content(SECRET) }])
        );
        assert_eq!(view["items"], json!([]));
        common::gql::assert_allowed(&view, "markRead");
    }

    // And (3) B's feed receives M1, carrying its content.
    let pushed = b_feed
        .until("M1 reaches B's feed", |delta| {
            upsert_of(delta, m1).is_some()
        })
        .await;
    assert_eq!(
        upsert_of(&pushed, m1).expect("an upsert of M1")["blocks"][0]["content"],
        text_content(SECRET)
    );

    // And the Created fact names the author, the kinds and the status — never
    // the text.
    let m1_facts = facts::of(&ctx.pool, "message", m1).await;
    let created = facts::the(&m1_facts, "Created");
    let payload = created.payload.to_string();
    for needle in [
        a.id.to_string(),
        "TEXT".to_string(),
        "COMPLETED".to_string(),
    ] {
        assert!(
            payload.contains(&needle),
            "Created carries {needle}: {payload}"
        );
    }
    facts::assert_no_fact_carries(&ctx.pool, SECRET, "a message's text").await;

    // When the bus redelivers M1's store, under its own envelope id.
    common::flows::redeliver(&ctx, store_command, &store_m1).await;
    // Then no second distinct answer is published, and M1 is stored once.
    common::flows::replayed_alike(&ctx, store_command, "the redelivered store").await;
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1]);
    assert_eq!(
        facts::of(&ctx.pool, "message", m1).await.len(),
        m1_facts.len()
    );

    // When the agents service stores messages the library refuses: an empty
    // text, a TOOL_RESULT in a user input.
    for (blocks, what, error) in [
        (
            vec![raw_text(json!({ "type": "text", "text": "" }))],
            "an empty text",
            "blank",
        ),
        (
            vec![tool_result("c1", "search", "found")],
            "a TOOL_RESULT in a user input",
            "block_kind_not_allowed",
        ),
    ] {
        let refused = user_message(t, Some(m1), a.id, blocks);
        let refused_id = refused.message_id;
        let refused_command = command(&ctx, &refused).await;

        // Then each is refused MESSAGE_INVALID, naming why, and nothing is
        // stored.
        let answer = message_refused(&ctx, refused_command, ReasonCode::MessageInvalid, what).await;
        assert_eq!(answer.params["error"], error, "{what}: {answer:?}");
        assert!(
            facts::of(&ctx.pool, "message", refused_id).await.is_empty(),
            "{what}"
        );
        assert_eq!(
            active_branch(&ctx, &a.passport, t).await,
            vec![m1],
            "{what}"
        );
    }

    // When X, not a member, is named the author.
    let outsider = command(
        &ctx,
        &user_message(t, Some(m1), x.id, vec![user_text("hello")]),
    )
    .await;
    // Then refused NOT_A_MEMBER.
    message_refused(
        &ctx,
        outsider,
        ReasonCode::NotAMember,
        "a non-member author",
    )
    .await;

    // When a message of T names a parent of another thread.
    let elsewhere = create_thread(&ctx, Some(&a), &[]).await;
    let foreign = say(&ctx, elsewhere, None, &a, "elsewhere").await;
    let foreign_parent = command(
        &ctx,
        &user_message(t, Some(foreign), a.id, vec![user_text("hi")]),
    )
    .await;
    // Then refused MESSAGE_INVALID.
    message_refused(
        &ctx,
        foreign_parent,
        ReasonCode::MessageInvalid,
        "a foreign parent",
    )
    .await;

    // When a message of T names a parent nobody stored.
    let unknown_parent = command(
        &ctx,
        &user_message(t, Some(Uuid::now_v7()), a.id, vec![user_text("hi")]),
    )
    .await;
    // Then refused MESSAGE_NOT_FOUND: "A command naming a thread or a message
    // that does not exist is refused THREAD_NOT_FOUND or MESSAGE_NOT_FOUND".
    message_refused(
        &ctx,
        unknown_parent,
        ReasonCode::MessageNotFound,
        "an unknown parent",
    )
    .await;

    // When a message is stored on a thread nobody created.
    let nowhere = command(
        &ctx,
        &user_message(Uuid::now_v7(), None, a.id, vec![user_text("hi")]),
    )
    .await;
    // Then refused THREAD_NOT_FOUND.
    message_refused(
        &ctx,
        nowhere,
        ReasonCode::ThreadNotFound,
        "an unknown thread",
    )
    .await;

    // When an AI message is stored complete with content the library refuses:
    // a turn opening on tool results instead of a step.
    let no_step = ai_message(
        t,
        Some(m1),
        ai("a1", "Analyst"),
        vec![tool_results(vec![tool_result("c1", "search", "found")])],
    );
    let no_step_id = no_step.message_id;
    let no_step_command = command(&ctx, &no_step).await;
    // Then refused MESSAGE_INVALID, nothing stored.
    message_refused(
        &ctx,
        no_step_command,
        ReasonCode::MessageInvalid,
        "a turn without a first step",
    )
    .await;
    assert!(facts::of(&ctx.pool, "message", no_step_id).await.is_empty());

    // When an AI turn names a tool no provider accepts.
    let dotted = Block::new(
        Uuid::now_v7(),
        BlockBody::ToolCall(LibraryBlock::from_json(json!({
            "type": "tool_call", "id": "call-x", "name": "web.search", "arguments": {}
        }))),
    );
    let dotted_turn = ai_message(
        t,
        Some(m1),
        ai("a1", "Analyst"),
        vec![step(StopReason::AwaitingToolResults, None, vec![dotted])],
    );
    let dotted_id = dotted_turn.message_id;
    let dotted_command = command(&ctx, &dotted_turn).await;
    // Then refused MESSAGE_INVALID with the library's code, nothing stored.
    let refused = message_refused(
        &ctx,
        dotted_command,
        ReasonCode::MessageInvalid,
        "an invalid tool name",
    )
    .await;
    assert_eq!(refused.params["error"], "invalid_tool_name", "{refused:?}");
    assert!(facts::of(&ctx.pool, "message", dotted_id).await.is_empty());

    // When an AI turn calls tool call-1 and answers, and the next turn of its
    // branch calls call-1 again.
    let calling = |id: &str| {
        vec![
            step(
                StopReason::AwaitingToolResults,
                None,
                vec![tool_call(id, "search", json!({ "q": "q3" }))],
            ),
            tool_results(vec![tool_result(id, "search", "found")]),
            step(StopReason::EndTurn, None, vec![ai_text("answered")]),
        ]
    };
    let first_call =
        common::flows::answer(&ctx, t, Some(m1), ai("a1", "Analyst"), calling("call-1")).await;
    let again = ai_message(t, Some(first_call), ai("a2", "Critic"), calling("call-1"));
    let again_id = again.message_id;
    let again_command = command(&ctx, &again).await;
    // Then the second is refused MESSAGE_INVALID with the library's code and
    // the id, nothing stored: the conversation of the branch would not load.
    let refused = message_refused(
        &ctx,
        again_command,
        ReasonCode::MessageInvalid,
        "a tool call id reused on the branch",
    )
    .await;
    assert_eq!(
        refused.params["error"], "duplicate_tool_call_id_in_conversation",
        "{refused:?}"
    );
    assert_eq!(refused.params["tool_call_id"], "call-1", "{refused:?}");
    assert!(facts::of(&ctx.pool, "message", again_id).await.is_empty());

    // When a regeneration of the first answer — a fork, another branch —
    // calls call-1.
    // Then it is stored: no branch holds call-1 twice.
    common::flows::answer(&ctx, t, Some(m1), ai("a1", "Analyst"), calling("call-1")).await;

    // When a new store reuses M1's id.
    let mut reuse = user_message(t, None, b.id, vec![user_text("mine")]);
    reuse.message_id = m1;
    let reuse_command = command(&ctx, &reuse).await;
    // Then refused KEY_REUSED, M1 untouched.
    message_refused(
        &ctx,
        reuse_command,
        ReasonCode::KeyReused,
        "reusing M1's id",
    )
    .await;
    assert_eq!(
        message_view(&ctx, &a.passport, t, m1).await["author"]["userId"],
        Value::String(a.id.to_string())
    );

    // When A's message is stored through impersonation by I.
    let impersonator = Uuid::now_v7();
    let mut impersonated = user_message(t, Some(m1), a.id, vec![user_text("on A's behalf")]);
    impersonated.impersonator_id = Some(impersonator);
    let m2 = impersonated.message_id;
    let impersonated_command = command(&ctx, &impersonated).await;
    message_confirmed(
        &ctx,
        impersonated_command,
        MessageStatus::Completed,
        "impersonated",
    )
    .await;

    // Then the Created fact carries the impersonator, and the message never.
    let created = facts::of(&ctx.pool, "message", m2).await;
    assert_eq!(
        facts::the(&created, "Created").impersonator_id,
        Some(impersonator)
    );
    let view = message_view(&ctx, &a.passport, t, m2).await;
    assert!(
        !view.to_string().contains(&impersonator.to_string()),
        "the impersonator is never on the message: {view}"
    );

    // When Identity renames A.
    ctx.rename(&a, "adele").await;

    // Then the new name shows on M1 everywhere: as members read it, and in a
    // runner's context.
    for person in [&o, &b] {
        let view = message_view(&ctx, &person.passport, t, m1).await;
        assert_eq!(view["author"]["name"], identity::display_name("adele"));
    }
    let runner = ctx.service_account(true);
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    let Entry::UserInput(first) = &conversation.entries()[0] else {
        panic!("the conversation opens on M1: {conversation:?}");
    };
    assert_eq!(
        first
            .author()
            .map(|author| author.name().as_str().to_string()),
        Some(identity::display_name("adele"))
    );
    assert_eq!(
        first.content(),
        &[UserBlock::text(
            contract_threads::llm::Text::new(SECRET).unwrap()
        )]
    );
    // And no fact carries a name, the old nor the new.
    for name in [
        identity::display_name("ada"),
        identity::display_name("adele"),
    ] {
        facts::assert_no_fact_carries(&ctx.pool, &name, "a name").await;
    }

    // When the thread is closed COMPLETED.
    let close = |status| CloseThread {
        thread_id: t,
        status,
        acting_user_id: Some(o.id),
    };
    let closed = command(&ctx, &close(ClosingStatus::Completed)).await;
    thread_confirmed(&ctx, closed, "closing T").await;

    // Then storing on it is refused THREAD_NOT_OPEN, as is any later close.
    let late = command(
        &ctx,
        &user_message(t, Some(m2), a.id, vec![user_text("late")]),
    )
    .await;
    message_refused(
        &ctx,
        late,
        ReasonCode::ThreadNotOpen,
        "a message on a closed thread",
    )
    .await;
    let reclose = command(&ctx, &close(ClosingStatus::Failed)).await;
    thread_refused(&ctx, reclose, ReasonCode::ThreadNotOpen, "closing again").await;

    ctx.teardown().await;
}
