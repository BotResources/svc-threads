//! **T11 — The agent context: sorted by Threads for the agent a runner names.**
//!
//! > Thread T, with members A and B and resources {kms: [k1]}, has an active
//! > branch holding: A's question; a1 "Analyst"'s turn with a thinking and its
//! > replay material, a redacted thinking, a runtime tool call, its result and
//! > an answer by model m-1; B's question; a2 "Critic"'s turn with a thinking, a
//! > runtime tool call, the tool's result, then an answer whose text block
//! > carries a citation and an image block, followed by ARTIFACT and WARNING
//! > annotations, and an agent's mail received in that turn; a deleted message
//! > by A; a1's turn CANCELLED after a whole response; a2's turn FAILED before
//! > its first entry; a2's turn still STREAMING with a text half produced; B's
//! > last word. A service account holding threads:runner reads T's context
//! > naming a1: T's id, T's members with their current display names and T's
//! > resources, and a projected thread for a1 (schema
//! > br-llm-messages-projected-thread/1) that the library loads: A's and B's
//! > questions whole with their user ids and no name; a1's turns whole, its
//! > thinking, replay material, redacted thinking, tool call and result byte for
//! > byte, with the response's model; a2's finished turn as an input turn
//! > holding a2's answer, text and image without the citation, and the agent's
//! > mail whole, without a2's thinking, tool call or result; a2's streaming turn
//! > holding the text produced so far; no deleted, cancelled or
//! > failed-before-first-entry message and no annotation. Members still read the
//! > annotations after their entries. Naming a2 instead gives a2's turns whole
//! > and a1's reduced to its answers. Read naming no agent, the context is the
//! > whole active branch as a br-llm-messages/5 conversation the library loads,
//! > with the same messages left out. Naming an agent id that is not a UUIDv7 is
//! > refused AI_PERSPECTIVE_INVALID. A user, a super user and a service account
//! > without threads:runner are refused RUNNER_SCOPE_REQUIRED. Reading a thread
//! > nobody created answers not found.
//!
//! The oracle is the library itself: the scenario keeps every turn it stored,
//! and the draft of the response still produced, and asks `br-llm-messages`
//! for the projected thread of each agent (`Perspective::project_pending`, the
//! still-produced response given as the pending response of a2's turn). What
//! Threads answers must be that projected thread, equal value for value — the
//! library decides what an agent sees, never the runner and never the suite.
//!
//! Channels: the runner operation (2) and its refusals; the conversation and
//! the projected threads as the library loads them; M4 as members read it.

mod common;

use std::collections::BTreeMap;

use common::artifacts::{File, upload};
use common::content::{
    Streamed, after, ai_turn, annotation_view, artifact, call, cited, closed, conversation_id,
    draft_entry, draft_text, entries_of, entry_view, human, input_turn, mail, model_response,
    redacted_thinking, response_image, result_of, results, said, text, thinking, to_json, turn_id,
    warning,
};
use common::flows::{
    ai, command, context, context_answer, conversation_of, create, creation, message_confirmed,
    open_stream, projected_of, resources_of, say, store, turn_ids, user_entry_message,
    whole_message,
};
use common::{AI_PERSPECTIVE_INVALID, RUNNER_SCOPE_REQUIRED, TestContext, gql, identity, stream};
use contract_threads::llm::{
    AITurn, Conversation, IncomingEntry, PROJECTED_THREAD_SCHEMA, PendingResponse, Perspective,
    ResponseEnding, Turn, TurnEntry,
};
use contract_threads::{CancelMessage, DeleteMessage, FailMessage, MessageStatus, StreamSeal};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t11_a_runner_reads_the_agent_context_sorted_by_threads_or_the_whole_conversation() {
    // Given T, with members A and B and resources {kms: [k1]}, whose active
    // branch holds, in order:
    let ctx = TestContext::setup().await;
    let (a, b) = (ctx.enrol("ada").await, ctx.enrol("bob").await);
    let s = ctx.enrol_super_user("sam").await;
    let mut born = creation(Some(a.id), &[b.id]);
    born.resources = BTreeMap::from([("kms".to_string(), vec!["k1".to_string()])]);
    let t = create(&ctx, born).await;
    let runner = ctx.service_account(true);
    let file = File::csv("x\n1\n");
    let x = upload(&ctx, &runner.passport, t, &file).await;
    let (analyst, critic, scout) = (ai("a1", "Analyst"), ai("a2", "Critic"), ai("a3", "Scout"));
    const THOUGHT: &str = "  let me think\n\twith whitespace ";
    const SIGNATURE: &str = "c2lnPT0=";
    const REDACTED: &str = "ZW5jcnlwdGVkIHRoaW5raW5n";
    // Every turn of the branch the context keeps, as stored.
    let mut kept: Vec<Turn> = Vec::new();

    // M1, A's question;
    let m1_entry = human(a.id, vec![text("Hello team")]);
    let store_m1 = user_entry_message(t, None, a.id, &m1_entry);
    let m1 = store(&ctx, store_m1, MessageStatus::Completed).await;
    kept.push(Turn::from(input_turn(m1, vec![m1_entry])));

    // M2, a1's turn: a thinking and its replay material, a redacted thinking,
    // a runtime tool call, its result, then an answer by m-1;
    let lookup = call("search", json!({ "q": "q3" }));
    let a1_entries = closed(vec![
        TurnEntry::from(model_response(
            vec![
                thinking(THOUGHT, SIGNATURE),
                redacted_thinking(REDACTED),
                lookup.clone().into(),
            ],
            ResponseEnding::tool_use(),
            Some("m-1"),
        )),
        results(vec![result_of(&lookup, "q3 rows", false)]),
        TurnEntry::from(model_response(
            vec![said("Analysis")],
            ResponseEnding::finished(),
            Some("m-1"),
        )),
    ]);
    let m2_id = Uuid::now_v7();
    let m2_turn = Turn::from(ai_turn(m2_id, &analyst, a1_entries));
    let m2 = store(
        &ctx,
        whole_message(t, Some(m1), m2_id, &analyst, &m2_turn, Vec::new()),
        MessageStatus::Completed,
    )
    .await;
    kept.push(m2_turn.clone());

    // M3, B's question to the Critic;
    let m3_entry = human(b.id, vec![text("And you, Critic?")]);
    let m3 = store(
        &ctx,
        user_entry_message(t, Some(m2), b.id, &m3_entry),
        MessageStatus::Completed,
    )
    .await;
    kept.push(Turn::from(input_turn(m3, vec![m3_entry])));

    // M4, a2's turn: a thinking, a runtime tool call, its result, then an
    // answer whose text carries a citation and an image, followed by ARTIFACT
    // and WARNING annotations, and an agent's mail received in that turn;
    let figures = call("search", json!({ "q": "q3 figures" }));
    let critique = TurnEntry::from(model_response(
        vec![cited("Critique", "doc-9"), response_image()],
        ResponseEnding::finished(),
        None,
    ));
    let a2_entries = closed(vec![
        TurnEntry::from(model_response(
            vec![
                thinking("weighing the analysis", "Y3JpdGlj"),
                figures.clone().into(),
            ],
            ResponseEnding::tool_use(),
            None,
        )),
        results(vec![result_of(&figures, "figures found", false)]),
        critique.clone(),
        mail(&scout, Uuid::now_v7(), vec![text("the scout's note")]),
    ]);
    let (pinned, partial) = (
        artifact(x),
        warning("PARTIAL_ANSWER", "a source was unreachable"),
    );
    let m4_id = Uuid::now_v7();
    let m4_turn = Turn::from(ai_turn(m4_id, &critic, a2_entries));
    let m4 = store(
        &ctx,
        whole_message(
            t,
            Some(m3),
            m4_id,
            &critic,
            &m4_turn,
            vec![
                after(&critique, pinned.clone()),
                after(&critique, partial.clone()),
            ],
        ),
        MessageStatus::Completed,
    )
    .await;
    kept.push(m4_turn.clone());

    // M5, A's message, deleted;
    let m5 = say(&ctx, t, Some(m4), &a, "scratch that").await;
    let deleted = command(
        &ctx,
        &DeleteMessage {
            message_id: m5,
            thread_id: t,
            acting_user_id: a.id,
        },
    )
    .await;
    message_confirmed(&ctx, deleted, MessageStatus::Completed, "deleting M5").await;

    // M6, a1's turn, cancelled after a whole response;
    let m6 = open_stream(&ctx, t, Some(m5), analyst.clone()).await;
    let mut r6 = Streamed::start(m6, None);
    r6.text(&["cancelled words"], true);
    r6.complete(ResponseEnding::finished());
    let (last_seq, hash) = stream::push(&ctx.nats.url(), m6, 0, &r6.chunks()).await;
    let cancelled = command(
        &ctx,
        &CancelMessage {
            message_id: m6,
            thread_id: t,
            seal: Some(StreamSeal { last_seq, hash }),
            acting_user_id: Some(a.id),
        },
    )
    .await;
    message_confirmed(&ctx, cancelled, MessageStatus::Cancelled, "cancelling M6").await;

    // M7, a2's turn, failed before its first entry;
    let m7 = open_stream(&ctx, t, Some(m6), critic.clone()).await;
    let failed = command(
        &ctx,
        &FailMessage {
            message_id: m7,
            thread_id: t,
            seal: None,
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(&ctx, failed, MessageStatus::Failed, "failing M7").await;

    // M8, a2's turn, still streaming a text half produced;
    let m8 = open_stream(&ctx, t, Some(m7), critic.clone()).await;
    let mut r8 = Streamed::start(m8, None);
    r8.text(&["Half of a "], false);
    stream::push(&ctx.nats.url(), m8, 0, &r8.chunks()).await;
    kept.push(Turn::from(AITurn::open(
        turn_id(m8),
        critic.id,
        critic.author(),
    )));
    // The context is read only once Threads has folded every delta pushed: the
    // half-produced draft's text is the library's draft's.
    let half = draft_text(r8.draft());
    common::flows::await_message(&ctx, &a.passport, t, m8, "M8's draft folded", |view| {
        let entries = entries_of(view);
        entries.len() == 1
            && entries[0]["state"] == "PRODUCING"
            && draft_text(&draft_entry(&entries[0])) == half
    })
    .await;

    // M9, B's last word.
    let m9_entry = human(b.id, vec![text("Final word")]);
    let m9 = store(
        &ctx,
        user_entry_message(t, Some(m8), b.id, &m9_entry),
        MessageStatus::Completed,
    )
    .await;
    kept.push(Turn::from(input_turn(m9, vec![m9_entry])));
    let stored = Conversation::from_parts(conversation_id(t), kept.clone())
        .expect("the branch the context keeps is a conversation the library accepts");
    let pending = [PendingResponse::from(r8.draft())];

    // When the runner reads T's context naming a1.
    let for_a1 = context(&ctx, &runner.passport, t, Some(&analyst)).await;

    // Then it is T's, with T's members under their current display names and
    // T's resources.
    assert_eq!(for_a1["threadId"], Value::String(t.to_string()));
    let mut members: Vec<(String, String)> = for_a1["members"]
        .as_array()
        .expect("members")
        .iter()
        .map(|member| {
            (
                member["userId"].as_str().unwrap().to_string(),
                member["displayName"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    members.sort();
    let mut expected_members = vec![
        (a.id.to_string(), identity::display_name("ada")),
        (b.id.to_string(), identity::display_name("bob")),
    ];
    expected_members.sort();
    assert_eq!(members, expected_members);
    assert_eq!(
        resources_of(&for_a1),
        BTreeMap::from([("kms".to_string(), vec!["k1".to_string()])])
    );

    // And the agent context is a projected thread for a1 the library loads,
    // exactly the one the library projects for a1 from the branch, a2's
    // response still produced included as far as it goes.
    assert_eq!(for_a1["projectedThread"]["schema"], PROJECTED_THREAD_SCHEMA);
    let projected = projected_of(&for_a1);
    let expected = Perspective::of(analyst.id)
        .project_pending(conversation_id(t), stored.turns(), &pending)
        .expect("the library projects the branch for a1");
    assert_eq!(
        projected, expected,
        "the agent context is the library's, for a1"
    );
    assert_eq!(projected.agent(), analyst.id);
    assert_eq!(projected.conversation(), conversation_id(t));

    // And, read as the gesture states it: no deleted, cancelled or
    // failed-before-first-entry message, the questions and a1's turns whole.
    let projected_ids: Vec<Uuid> = projected
        .turns()
        .iter()
        .map(|turn| turn.id().as_uuid())
        .collect();
    assert_eq!(projected_ids, vec![m1, m2, m3, m4, m8, m9]);
    assert_eq!(
        to_json(&projected.turns()[1]),
        to_json(&m2_turn),
        "a1's turn whole"
    );
    for (position, question) in [(0, m1), (2, m3), (5, m9)] {
        assert_eq!(
            to_json(&projected.turns()[position]),
            to_json(
                &kept[kept
                    .iter()
                    .position(|k| k.id().as_uuid() == question)
                    .unwrap()]
            ),
            "a question whole, by its user's id"
        );
    }
    // a2's finished turn, an input turn: its answer (text and image, no
    // citation) and the scout's mail whole — nothing of its thinking or tool.
    let m4_for_a1 = projected.turns()[3]
        .as_input()
        .expect("a2's finished turn reads as an input turn for a1");
    let kinds: Vec<String> = m4_for_a1
        .entries()
        .iter()
        .map(|entry| to_json(entry)["type"].as_str().unwrap_or("").to_string())
        .collect();
    assert_eq!(kinds, vec!["agent_output", "agent_mail"]);
    let a2_answer = to_json(&m4_for_a1.entries()[0]);
    assert_eq!(a2_answer["content"].as_array().map(Vec::len), Some(2));
    assert!(
        a2_answer["content"][0].get("citations").is_none(),
        "{a2_answer}"
    );
    // a2's streaming turn: the text produced so far.
    let m8_for_a1 = projected.turns()[4]
        .as_input()
        .expect("an input turn for a1");
    assert!(matches!(
        m8_for_a1.entries()[0],
        IncomingEntry::PartialAgentOutput(_)
    ));
    assert!(
        to_json(&m8_for_a1.entries()[0])
            .to_string()
            .contains("Half of a ")
    );
    let raw = for_a1["projectedThread"].to_string();
    for absent in [
        "weighing the analysis",
        "q3 figures",
        "figures found",
        "scratch that",
        "cancelled words",
        "PARTIAL_ANSWER",
        "doc-9",
        x.to_string().as_str(),
        identity::display_name("ada").as_str(),
    ] {
        assert!(
            !raw.contains(absent),
            "{absent} is not in a1's context: {raw}"
        );
    }

    // And members still read the annotations after their entries.
    let m4_view = common::flows::message_view(&ctx, &a.passport, t, m4).await;
    let read: Vec<Value> = entry_view(&m4_view, critique.id())["annotations"]
        .as_array()
        .expect("the annotations after a2's answer")
        .iter()
        .map(|annotation| {
            json!({ "id": annotation["id"], "kind": annotation["kind"], "content": annotation["content"] })
        })
        .collect();
    assert_eq!(
        read,
        vec![annotation_view(&pinned), annotation_view(&partial)]
    );

    // When it reads it naming a2 instead.
    let for_a2 = projected_of(&context(&ctx, &runner.passport, t, Some(&critic)).await);
    // Then a2's turns are whole and a1's reduced to its answers — the
    // library's projection for a2 (its own response still produced is its own:
    // no pending response of its own turn).
    let expected = Perspective::of(critic.id)
        .project_pending(conversation_id(t), stored.turns(), &[])
        .expect("the library projects the branch for a2");
    assert_eq!(
        for_a2, expected,
        "the agent context is the library's, for a2"
    );
    assert_eq!(
        to_json(&for_a2.turns()[3]),
        to_json(&m4_turn),
        "a2's turn whole"
    );
    assert!(for_a2.turns()[1].as_input().is_some(), "a1's turn reduced");

    // When it reads it naming no agent.
    let whole_context = context(&ctx, &runner.passport, t, None).await;
    // Then the context is the whole active branch, a br-llm-messages/5
    // conversation the library loads, the same messages left out.
    assert_eq!(whole_context["conversation"]["schema"], "br-llm-messages/5");
    let whole = conversation_of(&whole_context);
    assert_eq!(turn_ids(&whole), vec![m1, m2, m3, m4, m8, m9]);
    assert_eq!(whole, stored, "the whole branch, as stored");
    assert!(
        !whole_context["conversation"]
            .to_string()
            .contains(&x.to_string())
    );

    // When it names an agent id that is not a UUIDv7.
    for (agent, what) in [
        ("a1".to_string(), "an id that is no UUID"),
        (Uuid::new_v4().to_string(), "a UUIDv4"),
    ] {
        let refused = context_answer(&ctx, &runner.passport, t, Some(&agent)).await;
        // Then refused AI_PERSPECTIVE_INVALID.
        gql::assert_refused(&refused, AI_PERSPECTIVE_INVALID);
        assert!(
            refused["data"].is_null() || refused["data"]["threadsContext"].is_null(),
            "{what}: {refused}"
        );
    }

    // When a user, a super user, or a service account without threads:runner
    // ask for the context.
    let plain_service = ctx.service_account(false);
    for (passport, what) in [
        (&a.passport, "a member"),
        (&s.passport, "a super user"),
        (
            &plain_service.passport,
            "a service account without the scope",
        ),
    ] {
        // Then each is refused RUNNER_SCOPE_REQUIRED, with no data.
        let refused = context_answer(&ctx, passport, t, None).await;
        gql::assert_refused(&refused, RUNNER_SCOPE_REQUIRED);
        assert!(
            refused["data"].is_null() || refused["data"]["threadsContext"].is_null(),
            "{what}: {refused}"
        );
    }

    // When the runner asks for a thread nobody created.
    let unknown = context(&ctx, &runner.passport, Uuid::now_v7(), None).await;
    // Then it is answered not found.
    assert_eq!(unknown, json!(null));

    ctx.teardown().await;
}
