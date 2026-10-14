//! **T11 — The runner's conversation: whole or lightened, authors named.**
//!
//! > Thread T's active branch holds user messages by A and B, AI turns by a1
//! > "Analyst" and a2 "Critic", a deleted message, a CANCELLED turn, a turn
//! > FAILED before its first step, and a turn carrying an ARTIFACT block. A
//! > service account holding threads:runner reads T's context: schema
//! > br-llm-messages/2, the active branch without the deleted, cancelled and
//! > failed-before-first-step messages, no Threads block, every message
//! > carrying its author: {id, name} of the AI, or the user's id and current
//! > display name; plus T's members and resources. It reads it again naming
//! > a1: the conversation is lightened by the library's rule for a1, so that
//! > its render for a1 is the same as from the whole one. A thinking and its
//! > signature come back byte for byte. A user, a super user and a service
//! > account without threads:runner are refused the operation. Reading an
//! > unknown thread answers not found.
//!
//! The expected conversation is computed with `br-llm-messages` itself: its
//! `lighten` and its `render` are the rule the specification names.
//!
//! Channels: the runner operation (2) and its refusals; the conversation as
//! the library loads, lightens and renders it.

mod common;

use std::collections::BTreeMap;

use common::artifacts::{File, upload};
use common::content::{
    ai_text, artifact_block, citation_block, redacted_thinking, step, text_step, thinking,
    tool_call, tool_result, tool_results, user_text, warning_block,
};
use common::flows::{
    ai, answer, command, context, context_answer, conversation_of, create, creation,
    message_confirmed, open_stream, resources_of, say, store, user_message,
};
use common::{RUNNER_SCOPE_REQUIRED, TestContext, gql, identity, stream};
use contract_threads::llm::{
    AssistantBlock, Entry, Perspective, StopReason, Text, Thinking, UserBlock, lighten, render,
};
use contract_threads::{CancelMessage, DeleteMessage, FailMessage, MessageStatus, StreamSeal};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t11_a_runner_reads_the_active_branch_as_a_conversation_whole_or_lightened_for_its_ai() {
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
    let (analyst, critic) = (ai("a1", "Analyst"), ai("a2", "Critic"));
    const THOUGHT: &str = "  let me think\n\twith whitespace ";
    const SIGNATURE: &str = "c2lnPT0=";
    const REDACTED: &str = "ZW5jcnlwdGVkIHRoaW5raW5n";

    // M1, A's question;
    let m1 = say(&ctx, t, None, &a, "Hello team").await;
    // M2, a1's turn: a thinking with its signature, then its answer, by m-1;
    let m2 = answer(
        &ctx,
        t,
        Some(m1),
        analyst.clone(),
        vec![step(
            StopReason::EndTurn,
            Some("m-1"),
            vec![
                thinking(THOUGHT, SIGNATURE),
                redacted_thinking(REDACTED),
                ai_text("Analysis"),
            ],
        )],
    )
    .await;
    // M3, B's question to the Critic;
    let m3 = say(&ctx, t, Some(m2), &b, "And you, Critic?").await;
    // M4, a2's turn: a thinking and a tool call, the tool's result, then its
    // answer carrying an ARTIFACT block and other Threads blocks;
    let m4 = answer(
        &ctx,
        t,
        Some(m3),
        critic.clone(),
        vec![
            step(
                StopReason::AwaitingToolResults,
                None,
                vec![
                    thinking("weighing the analysis", "Y3JpdGlj"),
                    tool_call("c9", "search", json!({ "q": "q3 figures" })),
                ],
            ),
            tool_results(vec![tool_result("c9", "search", "figures found")]),
            step(
                StopReason::EndTurn,
                None,
                vec![
                    ai_text("Critique"),
                    artifact_block(x),
                    warning_block("PARTIAL_ANSWER", "a source was unreachable"),
                    citation_block("quoted passage", &["doc-9"]),
                ],
            ),
        ],
    )
    .await;
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
    // M6, a1's turn, cancelled after a whole step;
    let m6 = open_stream(&ctx, t, Some(m5), analyst.clone()).await;
    let (last_seq, hash) = stream::push(
        &ctx.nats.url(),
        m6,
        0,
        &text_step(0, None, &["cancelled words"], StopReason::EndTurn),
    )
    .await;
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
    // M7, a2's turn, failed before its first step;
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
    // M8, B's last word.
    store(
        &ctx,
        user_message(t, Some(m7), b.id, vec![user_text("Final word")]),
        MessageStatus::Completed,
    )
    .await;

    // When the runner reads T's context, whole.
    let whole_context = context(&ctx, &runner.passport, t, None).await;

    // Then it is T's, in the library's schema, with T's members and resources.
    assert_eq!(whole_context["threadId"], Value::String(t.to_string()));
    assert_eq!(whole_context["conversation"]["schema"], "br-llm-messages/2");
    let mut members: Vec<(String, String)> = whole_context["members"]
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
        resources_of(&whole_context),
        BTreeMap::from([("kms".to_string(), vec!["k1".to_string()])])
    );

    // And the conversation, as the library loads it, is the active branch
    // without M5 (deleted), M6 (cancelled) and M7 (failed before its first
    // step): M1, M2, M3, M4, M8 — each with its author.
    let whole = conversation_of(&whole_context);
    let entries = whole.entries();
    assert_eq!(entries.len(), 5, "{whole:?}");
    let user = |entry: &Entry| match entry {
        Entry::UserInput(input) => input.clone(),
        Entry::Turn(turn) => panic!("a user input expected, got a turn {turn:?}"),
    };
    let turn = |entry: &Entry| match entry {
        Entry::Turn(turn) => turn.clone(),
        Entry::UserInput(input) => panic!("a turn expected, got a user input {input:?}"),
    };
    for (index, person, text) in [
        (0, &a, "Hello team"),
        (2, &b, "And you, Critic?"),
        (4, &b, "Final word"),
    ] {
        let input = user(&entries[index]);
        let author = input.author().expect("a user input carries its author");
        assert_eq!(author.id().as_str(), person.id.to_string());
        assert_eq!(
            author.name().as_str(),
            identity::display_name(&person.label)
        );
        assert_eq!(
            input.content(),
            &[UserBlock::text(Text::new(text).unwrap())]
        );
    }
    for (index, who) in [(1, &analyst), (3, &critic)] {
        let author = turn(&entries[index])
            .author()
            .cloned()
            .expect("a turn carries its AI");
        assert_eq!(author, who.to_library().unwrap());
    }

    // And no Threads block reaches the runner: M4's answering step holds its
    // text only, after its tool call and result.
    let critique = turn(&entries[3]);
    assert_eq!(critique.items().len(), 3, "{critique:?}");
    let contract_threads::llm::TurnItem::Step(critique_step) = &critique.items()[2] else {
        panic!("M4 ends on a step");
    };
    assert_eq!(
        critique_step.content(),
        &[AssistantBlock::Text {
            text: Text::new("Critique").unwrap()
        }]
    );
    let raw = whole_context["conversation"].to_string();
    assert!(!raw.contains(&x.to_string()), "no ARTIFACT block: {raw}");
    for absent in [
        "scratch that",
        "cancelled words",
        "PARTIAL_ANSWER",
        "quoted passage",
    ] {
        assert!(!raw.contains(absent), "{absent} is left out: {raw}");
    }

    // And the thinking and its signature come back byte for byte, with the
    // step's model.
    let analysis = turn(&entries[1]);
    let contract_threads::llm::TurnItem::Step(analysis_step) = &analysis.items()[0] else {
        panic!("M2 opens on a step");
    };
    assert_eq!(
        analysis_step.content()[0],
        AssistantBlock::Thinking(Thinking {
            text: THOUGHT.to_string(),
            signature: Some(contract_threads::llm::Signature::new(SIGNATURE).unwrap()),
        })
    );
    assert_eq!(
        analysis_step.content()[1],
        AssistantBlock::RedactedThinking(
            contract_threads::llm::RedactedThinking::new(REDACTED).unwrap()
        )
    );
    assert_eq!(
        analysis_step
            .model()
            .map(|model| model.as_str().to_string()),
        Some("m-1".to_string())
    );

    // And members still see M4's Threads blocks, in the order the writer gave
    // them.
    let m4_view = common::flows::message_view(&ctx, &a.passport, t, m4).await;
    let kinds: Vec<&str> = m4_view["items"][2]["blocks"]
        .as_array()
        .expect("M4's blocks")
        .iter()
        .map(|block| block["kind"].as_str().expect("a kind"))
        .collect();
    assert_eq!(kinds, vec!["TEXT", "ARTIFACT", "WARNING", "CITATION"]);
    assert_eq!(
        m4_view["items"][2]["blocks"][2]["content"],
        json!({ "code": "PARTIAL_ANSWER", "text": "a source was unreachable" })
    );

    // When the runner reads it again, naming a1.
    let lightened = conversation_of(&context(&ctx, &runner.passport, t, Some(&analyst)).await);

    // Then it is lightened by the library's rule for a1, and renders for a1
    // exactly as the whole one does.
    let perspective = Perspective::new(analyst.to_library().unwrap());
    assert_eq!(lightened, lighten(&whole, &perspective));
    assert_eq!(
        render(&lightened, &perspective).expect("the lightened conversation renders"),
        render(&whole, &perspective).expect("the whole conversation renders")
    );
    // And lightening removed something real: a2's thinking, tool call and
    // tool result are gone for a1, its text kept.
    assert_ne!(lightened, whole, "a2's turn is lightened for a1");
    let lightened_raw = serde_json::to_string(&lightened).expect("a conversation serializes");
    for gone in ["weighing the analysis", "q3 figures", "figures found"] {
        assert!(
            !lightened_raw.contains(gone),
            "{gone} is lightened away: {lightened_raw}"
        );
    }
    assert!(lightened_raw.contains("Critique"));

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
        // Then each is refused the operation.
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
