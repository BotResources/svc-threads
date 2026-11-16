//! **T37 — A runner reads the context of the branch a message ends.**
//!
//! > On thread T, user A asks M1; the analyst answers M2, attaching artifact X;
//! > A follows up with M3, answering M2. A then rewrites the question: M1b, a
//! > new message without parent, answered by M2b attaching artifact Y — the
//! > active branch is M1b, M2b. A deletes M3. A threads:runner service account
//! > reads T's context:
//! > - naming no tip, it receives the active branch M1b, M2b and Y, as before;
//! >   naming M2b, the active tip, the very same read;
//! > - naming M1, the conversation is M1 alone, with no artifact — whole and
//! >   for the analyst;
//! > - naming M2, it is M1, M2, with X — whole and for the analyst;
//! > - naming M3, deleted, it is M1, M2, with X: M3 still ends its branch,
//! >   and is not in the conversation, as no deleted message is;
//! > - naming a message no thread holds, or a message of another thread U, the
//! >   context of T is answered with its members and an empty conversation,
//! >   without artifacts — as `threadsMessages` answers an empty page for the
//! >   same tips; U's message read on U ends U's branch.
//!
//! The rewritten question is the regeneration a downstream project offers on
//! an earlier version of an edited message: the client stores the new version
//! as a sibling, and the runner answering the earlier version names it as the
//! tip, so that the conversation it receives ends at the message it answers.
//!
//! Channels: the runner's context read (whole and for an agent, with its
//! artifacts), and a member's branch read for the same tips.

mod common;

use common::TestContext;
use common::artifacts::{File, upload};
use common::flows::{
    active_branch, ai, answer_attaching, branch, command, context_on, conversation_of,
    create_thread, message_confirmed, projected_of, say, turn_ids, uuid_of,
};
use contract_threads::{DeleteMessage, MessageStatus};
use serde_json::{Value, json};
use uuid::Uuid;

/// The ids of the artifacts a context carries, in order.
fn artifact_ids(context: &Value) -> Vec<Uuid> {
    context["artifacts"]
        .as_array()
        .unwrap_or_else(|| panic!("a context carries its artifacts: {context}"))
        .iter()
        .map(|artifact| uuid_of(&artifact["id"]))
        .collect()
}

/// The ids of the turns of an agent context, in order.
fn projected_ids(context: &Value) -> Vec<Uuid> {
    projected_of(context)
        .turns()
        .iter()
        .map(|turn| turn.id().as_uuid())
        .collect()
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t37_a_runner_reads_the_context_of_the_branch_a_message_ends() {
    // Given T, member A, a runner, the analyst; X and Y uploaded.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let runner = ctx.service_account(true);
    let analyst = ai("a1", "Analyst");
    let x = upload(&ctx, &runner.passport, t, &File::csv("first\n")).await;
    let y = upload(&ctx, &runner.passport, t, &File::csv("second\n")).await;
    // And M1, M2 attaching X, M3 answering M2.
    let m1 = say(&ctx, t, None, &a, "the first question").await;
    let m2 = answer_attaching(&ctx, t, Some(m1), analyst.clone(), "the first answer", x).await;
    let m3 = say(&ctx, t, Some(m2), &a, "a follow-up").await;
    // And the question rewritten: M1b, answered by M2b attaching Y.
    let m1b = say(&ctx, t, None, &a, "the rewritten question").await;
    let m2b = answer_attaching(&ctx, t, Some(m1b), analyst.clone(), "the second answer", y).await;
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1b, m2b]);
    // And M3 deleted by A.
    let deleted = command(
        &ctx,
        &DeleteMessage {
            message_id: m3,
            thread_id: t,
            acting_user_id: a.id,
        },
    )
    .await;
    message_confirmed(&ctx, deleted, MessageStatus::Completed, "deleting M3").await;

    // When the runner reads T's context naming no tip.
    let active = context_on(&ctx, &runner.passport, t, None, None).await;
    // Then it receives the active branch, M1b and M2b, and Y.
    assert_eq!(turn_ids(&conversation_of(&active)), vec![m1b, m2b]);
    assert_eq!(artifact_ids(&active), vec![y], "{active}");
    // And naming M2b, the active tip, it receives the very same read.
    let at_m2b = context_on(&ctx, &runner.passport, t, None, Some(m2b)).await;
    assert_eq!(at_m2b, active, "the active tip names the active branch");
    let for_agent = context_on(&ctx, &runner.passport, t, Some(&analyst), None).await;
    assert_eq!(projected_ids(&for_agent), vec![m1b, m2b]);
    assert_eq!(
        context_on(&ctx, &runner.passport, t, Some(&analyst), Some(m2b)).await,
        for_agent
    );

    // When it names M1, the earlier version of the question.
    let at_m1 = context_on(&ctx, &runner.passport, t, None, Some(m1)).await;
    // Then the conversation is M1 alone, with no artifact.
    assert_eq!(turn_ids(&conversation_of(&at_m1)), vec![m1]);
    assert_eq!(artifact_ids(&at_m1), Vec::<Uuid>::new(), "{at_m1}");
    let raw = at_m1.to_string();
    for absent in [
        "the first answer",
        "the rewritten question",
        "the second answer",
    ] {
        assert!(!raw.contains(absent), "{absent}: {raw}");
    }
    // And so is the analyst's agent context.
    let at_m1_for_agent = context_on(&ctx, &runner.passport, t, Some(&analyst), Some(m1)).await;
    assert_eq!(projected_ids(&at_m1_for_agent), vec![m1]);
    assert_eq!(artifact_ids(&at_m1_for_agent), Vec::<Uuid>::new());

    // When it names M2, the earlier answer.
    let at_m2 = context_on(&ctx, &runner.passport, t, None, Some(m2)).await;
    // Then the conversation is M1, M2, with X — whole and for the analyst.
    assert_eq!(turn_ids(&conversation_of(&at_m2)), vec![m1, m2]);
    assert_eq!(artifact_ids(&at_m2), vec![x], "{at_m2}");
    let raw = at_m2.to_string();
    assert!(
        raw.contains("the first answer") && !raw.contains("the second answer"),
        "the branch through M2, not M2b: {raw}"
    );
    let at_m2_for_agent = context_on(&ctx, &runner.passport, t, Some(&analyst), Some(m2)).await;
    assert_eq!(projected_ids(&at_m2_for_agent), vec![m1, m2]);
    assert_eq!(artifact_ids(&at_m2_for_agent), vec![x]);

    // When it names M3, deleted.
    let at_m3 = context_on(&ctx, &runner.passport, t, None, Some(m3)).await;
    // Then M3 still ends its branch, and is not in the conversation.
    assert_eq!(turn_ids(&conversation_of(&at_m3)), vec![m1, m2]);
    assert_eq!(artifact_ids(&at_m3), vec![x]);
    assert!(!at_m3.to_string().contains("a follow-up"), "{at_m3}");
    // And a member reads the same branch, M3 in it without content.
    let read: Vec<Uuid> = branch(&ctx, &a.passport, t, 10, Some(m3))
        .await
        .iter()
        .map(|view| uuid_of(&view["id"]))
        .collect();
    assert_eq!(read, vec![m3, m2, m1]);

    // Given U, another thread of A's, and its message N1.
    let u = create_thread(&ctx, Some(&a), &[]).await;
    let n1 = say(&ctx, u, None, &a, "a question elsewhere").await;
    for (tip, what) in [
        (Uuid::now_v7(), "a message no thread holds"),
        (n1, "a message of another thread"),
    ] {
        // When the runner reads T's context naming it.
        let read = context_on(&ctx, &runner.passport, t, None, Some(tip)).await;
        // Then T's context is answered: its members, an empty conversation,
        // no artifact.
        assert_eq!(read["threadId"], json!(t), "{what}: {read}");
        assert_eq!(
            read["members"].as_array().map(Vec::len),
            Some(1),
            "{what}: {read}"
        );
        assert_eq!(
            turn_ids(&conversation_of(&read)),
            Vec::<Uuid>::new(),
            "{what}"
        );
        assert_eq!(artifact_ids(&read), Vec::<Uuid>::new(), "{what}");
        let for_agent = context_on(&ctx, &runner.passport, t, Some(&analyst), Some(tip)).await;
        assert_eq!(projected_ids(&for_agent), Vec::<Uuid>::new(), "{what}");
        // And a member reading T's branch ending at it reads an empty page.
        assert!(
            branch(&ctx, &a.passport, t, 10, Some(tip)).await.is_empty(),
            "{what}"
        );
    }
    // And N1 read on U ends U's branch.
    let on_u = context_on(&ctx, &runner.passport, u, None, Some(n1)).await;
    assert_eq!(turn_ids(&conversation_of(&on_u)), vec![n1]);

    ctx.teardown().await;
}
