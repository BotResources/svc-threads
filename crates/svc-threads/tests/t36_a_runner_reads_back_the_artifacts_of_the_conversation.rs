//! **T36 — A runner reads back the artifacts of the conversation.**
//!
//! > Thread T's active branch holds AI message M1 attaching artifact X
//! > (report.csv) and AI message M2 attaching artifact Y; a PENDING artifact Z
//! > was never attached, and message M3 on another branch attaches artifact W.
//! > A threads:runner service account reads T's context: it receives X and Y
//! > with id, name, media type, size, digest and message, and neither Z nor W.
//! > Asking for X's read access, the bytes read are the stored ones; a context
//! > read that does not ask for an access receives none. A service account
//! > without threads:runner is refused RUNNER_SCOPE_REQUIRED.
//!
//! The artifacts come beside the conversation, never inside it, in the order
//! of the messages on the branch, then of the `ARTIFACT` annotations naming
//! them in a message (its entry's position, then its own after that entry) —
//! the second scenario pins that second key. Y is uploaded before X, so that
//! the order of the messages is never the order of the ids. Z's bytes landed:
//! only the attachment makes an artifact one a runner receives.
//!
//! Channels: the runner's context read (2) — whole and as an agent context,
//! without and with read accesses; the read access as the object store serves
//! it; the refusal (1). The read accesses of the members are T13's.

mod common;

use common::artifacts::{File, fetch, upload};
use common::content::{
    after, ai_turn, artifact, call, closed, model_response, result_of, results, said,
};
use common::flows::{
    active_branch, ai, answer_attaching, create_thread, say, store, whole_message,
};
use common::{
    Q_CONTEXT_ARTIFACT_ACCESS, Q_CONTEXT_ARTIFACTS, RUNNER_SCOPE_REQUIRED, TestContext, gql,
};
use contract_threads::MessageStatus;
use contract_threads::llm::{ResponseEnding, Turn, TurnEntry};
use serde_json::{Value, json};
use uuid::Uuid;

/// The artifacts of `thread`'s context as `passport` reads them: whole
/// (`agent` absent) or for an agent, asking for a read access to each or not.
async fn context_artifacts(
    ctx: &TestContext,
    passport: &br_core_auth::Passport,
    thread: Uuid,
    agent: Option<String>,
    with_access: bool,
) -> Value {
    let document = if with_access {
        Q_CONTEXT_ARTIFACT_ACCESS
    } else {
        Q_CONTEXT_ARTIFACTS
    };
    ctx.run(
        passport,
        document,
        json!({ "thread": thread, "agent": agent }),
    )
    .await
}

fn artifacts_of(answer: &Value) -> Vec<Value> {
    gql::data(answer, "threadsContext")["artifacts"]
        .as_array()
        .unwrap_or_else(|| panic!("a context carries its artifacts: {answer}"))
        .clone()
}

/// What a runner receives of `id`: its declaration and its message.
fn received(id: Uuid, file: &File, message: Uuid) -> Value {
    json!({
        "id": id,
        "name": file.name,
        "mediaType": file.media_type,
        "size": file.bytes.len(),
        "sha256": file.sha256(),
        "messageId": message,
    })
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t36_a_runner_receives_the_attached_artifacts_of_the_conversation_and_reads_one_back() {
    // Given T, member A, a runner; Y, X, Z and W uploaded by the runner, Y
    // first.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let runner = ctx.service_account(true);
    let analyst = ai("a1", "Analyst");
    let x_file = File::csv("quarter,revenue\nQ3,42\n");
    let y_file = File {
        name: "summary.json",
        media_type: "application/json",
        bytes: br#"{"quarter":"Q3","revenue":42}"#.to_vec(),
    };
    let y = upload(&ctx, &runner.passport, t, &y_file).await;
    let x = upload(&ctx, &runner.passport, t, &x_file).await;
    let z = upload(&ctx, &runner.passport, t, &File::csv("never attached\n")).await;
    let w = upload(&ctx, &runner.passport, t, &File::csv("another branch\n")).await;
    // And A's question; M3 answering it and attaching W; then M1 answering it
    // and attaching X, and M2 answering M1 and attaching Y — the most recent,
    // so the active branch is the question, M1, M2, and M3 is on another one.
    let question = say(&ctx, t, None, &a, "Send me the reports").await;
    let m3 = answer_attaching(&ctx, t, Some(question), analyst.clone(), "a draft", w).await;
    let m1 = answer_attaching(&ctx, t, Some(question), analyst.clone(), "the report", x).await;
    let m2 = answer_attaching(&ctx, t, Some(m1), analyst.clone(), "and its summary", y).await;
    assert_eq!(
        active_branch(&ctx, &a.passport, t).await,
        vec![question, m1, m2],
        "M3 is off the active branch"
    );

    // When the runner reads T's context, asking for no read access.
    let read = context_artifacts(&ctx, &runner.passport, t, None, false).await;

    // Then it receives X and Y, in the order of their messages, each with its
    // id, name, media type, size, digest and message — and neither Z nor W.
    let artifacts = artifacts_of(&read);
    assert_eq!(
        artifacts,
        vec![received(x, &x_file, m1), received(y, &y_file, m2)],
        "{read}"
    );
    let raw = read.to_string();
    for (absent, what) in [(z, "Z, PENDING"), (w, "W, on another branch"), (m3, "M3")] {
        assert!(!raw.contains(&absent.to_string()), "{what}: {raw}");
    }
    // And, having asked for none, it receives no read access.
    for received in &artifacts {
        assert!(received.get("downloadUrl").is_none(), "{received}");
    }
    assert!(!raw.contains("http"), "no URL at all: {raw}");

    // When it reads T's agent context for the analyst.
    let for_agent = context_artifacts(
        &ctx,
        &runner.passport,
        t,
        Some(analyst.uuid().to_string()),
        false,
    )
    .await;
    // Then the artifacts are the same: they come beside the conversation.
    assert_eq!(artifacts_of(&for_agent), artifacts, "{for_agent}");

    // When it reads T's context asking for a read access to each.
    let with_access = context_artifacts(&ctx, &runner.passport, t, None, true).await;

    // Then each carries one, and X's returns the bytes stored, as Y's does.
    let accessed = artifacts_of(&with_access);
    assert!(
        with_access.get("errors").is_none(),
        "every access signed: {with_access}"
    );
    assert_eq!(accessed.len(), 2, "{with_access}");
    for ((granted, plain), file) in accessed.iter().zip(&artifacts).zip([&x_file, &y_file]) {
        let mut declared = granted.clone();
        let url = declared
            .as_object_mut()
            .and_then(|fields| fields.remove("downloadUrl"))
            .and_then(|url| url.as_str().map(str::to_string))
            .unwrap_or_else(|| panic!("a read access, asked for: {granted}"));
        assert_eq!(&declared, plain, "the same artifact, with its access");
        assert_eq!(fetch(&url).await, (200, file.bytes.clone()), "{url}");
    }

    // When a service account without threads:runner — and member A — read T's
    // context, asking for the read accesses.
    let plain_service = ctx.service_account(false);
    for (passport, what) in [
        (
            &plain_service.passport,
            "a service account without the scope",
        ),
        (&a.passport, "a member of T"),
    ] {
        let refused = context_artifacts(&ctx, passport, t, None, true).await;
        // Then each is refused RUNNER_SCOPE_REQUIRED, and reaches no artifact.
        gql::assert_refused(&refused, RUNNER_SCOPE_REQUIRED);
        assert!(
            refused["data"].is_null() || refused["data"]["threadsContext"].is_null(),
            "{what}: {refused}"
        );
        let raw = refused.to_string();
        assert!(
            !raw.contains(&x.to_string()) && !raw.contains("http"),
            "{what}: {raw}"
        );
    }

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t36_the_artifacts_of_one_message_come_in_the_order_of_their_annotations() {
    // Given T and an AI message whose turn holds two responses: an ARTIFACT
    // annotation naming P after the second, then two naming Q and R after the
    // first — P uploaded first, then R, then Q.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let runner = ctx.service_account(true);
    let analyst = ai("a1", "Analyst");
    let p = upload(&ctx, &runner.passport, t, &File::csv("p\n")).await;
    let r = upload(&ctx, &runner.passport, t, &File::csv("r\n")).await;
    let q = upload(&ctx, &runner.passport, t, &File::csv("q\n")).await;
    let question = say(&ctx, t, None, &a, "Three files, please").await;
    let lookup = call("search", json!({ "q": "files" }));
    let first = TurnEntry::from(model_response(
        vec![said("Looking"), lookup.clone().into()],
        ResponseEnding::tool_use(),
        None,
    ));
    let second = TurnEntry::from(model_response(
        vec![said("Here they are")],
        ResponseEnding::finished(),
        None,
    ));
    let entries = closed(vec![
        first.clone(),
        results(vec![result_of(&lookup, "three files", false)]),
        second.clone(),
    ]);
    let message = Uuid::now_v7();
    let turn = Turn::from(ai_turn(message, &analyst, entries));
    let stored = store(
        &ctx,
        whole_message(
            t,
            Some(question),
            message,
            &analyst,
            &turn,
            vec![
                after(&second, artifact(p)),
                after(&first, artifact(q)),
                after(&first, artifact(r)),
            ],
        ),
        MessageStatus::Completed,
    )
    .await;
    assert_eq!(stored, message);

    // When the runner reads T's context.
    let read = context_artifacts(&ctx, &runner.passport, t, None, false).await;

    // Then Q and R, after the first response in that order, come before P,
    // after the second — neither the order of the ids nor of the annotations
    // as sent.
    let ids: Vec<Value> = artifacts_of(&read)
        .iter()
        .map(|artifact| artifact["id"].clone())
        .collect();
    assert_eq!(ids, vec![json!(q), json!(r), json!(p)], "{read}");
    assert!(
        artifacts_of(&read)
            .iter()
            .all(|artifact| artifact["messageId"] == json!(message)),
        "{read}"
    );

    ctx.teardown().await;
}
