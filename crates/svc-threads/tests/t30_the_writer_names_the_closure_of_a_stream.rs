mod common;

use common::artifacts::{File, post, request_upload};
use common::content::{
    Streamed, annotation_chunk, annotations_of, artifact, closure_kind, closure_of, to_json,
    warning,
};
use common::facts::{self, Fact};
use common::feed::{Live, upsert_of};
use common::flows::{
    ai, await_message, command, completion, create_thread, message_confirmed, message_refused,
    message_view, open_stream, say,
};
use common::{SHORT_STREAM_IDLE, TestContext, Windows, stream};
use contract_threads::llm::{ClosureReason, ResponseEnding};
use contract_threads::{
    CancelMessage, CompleteMessageV2, FailMessage, FailMessageV2, LibraryJson, MessageStatus,
    ReasonCode, StreamSeal,
};
use serde_json::{Value, json};
use uuid::Uuid;

const DETAIL: &str = "the-detail-of-the-closure";

#[derive(Clone, Copy)]
enum Shape {
    Producing,
    Calling,
    Whole,
}

struct Streaming {
    message: Uuid,
    seal: StreamSeal,
}

async fn streaming(ctx: &TestContext, thread: Uuid, parent: Uuid, shape: Shape) -> Streaming {
    let message = open_stream(ctx, thread, Some(parent), ai("a1", "Analyst")).await;
    let mut response = Streamed::start(message, None);
    match shape {
        Shape::Producing => {
            response.text(&["half of an answer"], false);
        }
        Shape::Calling => {
            response.call("search", &["{\"q\":\"q3\"}"]);
            response.complete(ResponseEnding::tool_use());
        }
        Shape::Whole => {
            response.text(&["a whole answer"], true);
            response.complete(ResponseEnding::finished());
        }
    }
    let (last_seq, hash) = stream::push(&ctx.nats.url(), message, 0, &response.chunks()).await;
    Streaming {
        message,
        seal: StreamSeal { last_seq, hash },
    }
}

fn naming(reason: &ClosureReason) -> Option<LibraryJson> {
    Some(LibraryJson::from_json(to_json(reason)))
}

async fn complete_v2(
    ctx: &TestContext,
    thread: Uuid,
    stream: &Streaming,
    closure: Option<LibraryJson>,
) -> Uuid {
    command(
        ctx,
        &CompleteMessageV2 {
            message_id: stream.message,
            thread_id: thread,
            seal: stream.seal.clone(),
            closure,
            acting_user_id: None,
        },
    )
    .await
}

async fn fail_v2(
    ctx: &TestContext,
    thread: Uuid,
    stream: &Streaming,
    closure: Option<LibraryJson>,
) -> Uuid {
    command(
        ctx,
        &FailMessageV2 {
            message_id: stream.message,
            thread_id: thread,
            seal: Some(stream.seal.clone()),
            closure,
            acting_user_id: None,
        },
    )
    .await
}

fn closing_fact(facts: &[Fact]) -> &Fact {
    let closing: Vec<&Fact> = facts
        .iter()
        .filter(|fact| {
            matches!(
                fact.event_type.as_str(),
                "Completed" | "Failed" | "Cancelled"
            )
        })
        .collect();
    assert_eq!(
        closing.len(),
        1,
        "one closing fact: {:?}",
        facts::types(facts)
    );
    closing[0]
}

async fn ended_as(
    ctx: &TestContext,
    viewer: &common::Person,
    thread: Uuid,
    message: Uuid,
    status: MessageStatus,
    kind: &str,
    detail: Option<&str>,
) -> Value {
    let wanted = status.to_string();
    let view = await_message(ctx, &viewer.passport, thread, message, kind, |view| {
        view["status"] == json!(wanted)
    })
    .await;
    assert_eq!(closure_kind(&view).as_deref(), Some(kind), "{view}");
    let reason = to_json(&closure_of(&view).expect("a closure"));
    if let Some(detail) = detail {
        assert_eq!(reason["detail"].as_str(), Some(detail), "{view}");
    }
    let message_facts = facts::of(&ctx.pool, "message", message).await;
    let fact = closing_fact(&message_facts);
    assert_eq!(fact.payload["closure"], json!(kind), "{fact:?}");
    for fact in &message_facts {
        assert!(
            !fact.payload.to_string().contains(DETAIL),
            "no fact carries the detail: {fact:?}"
        );
    }
    view
}

fn budget() -> ClosureReason {
    ClosureReason::budget_exhausted()
        .with_detail(DETAIL)
        .expect("a detail")
}

fn named(reason: ClosureReason) -> ClosureReason {
    reason.with_detail(DETAIL).expect("a detail")
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t30_budget_exhausted_completes_a_turn_with_a_producing_response_or_an_open_call() {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Go on").await;

    for shape in [Shape::Producing, Shape::Calling] {
        let s = streaming(&ctx, t, m1, shape).await;
        let sent = complete_v2(&ctx, t, &s, naming(&budget())).await;
        message_confirmed(&ctx, sent, MessageStatus::Completed, "budget exhausted").await;
        ended_as(
            &ctx,
            &a,
            t,
            s.message,
            MessageStatus::Completed,
            "budget_exhausted",
            Some(DETAIL),
        )
        .await;
    }

    let s = streaming(&ctx, t, m1, Shape::Calling).await;
    let sent = fail_v2(&ctx, t, &s, naming(&budget())).await;
    message_confirmed(
        &ctx,
        sent,
        MessageStatus::Completed,
        "fail naming budget exhausted",
    )
    .await;
    ended_as(
        &ctx,
        &a,
        t,
        s.message,
        MessageStatus::Completed,
        "budget_exhausted",
        Some(DETAIL),
    )
    .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t30_completed_still_needs_a_whole_turn_named_or_not() {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Go on").await;

    for shape in [Shape::Producing, Shape::Calling] {
        for closure in [None, naming(&ClosureReason::completed())] {
            let s = streaming(&ctx, t, m1, shape).await;
            let sent = complete_v2(&ctx, t, &s, closure).await;
            message_confirmed(
                &ctx,
                sent,
                MessageStatus::Failed,
                "completed on a broken turn",
            )
            .await;
            ended_as(
                &ctx,
                &a,
                t,
                s.message,
                MessageStatus::Failed,
                "failed",
                None,
            )
            .await;
        }
    }

    for named_completed in [false, true] {
        let s = streaming(&ctx, t, m1, Shape::Whole).await;
        let closure = named_completed
            .then(|| naming(&named(ClosureReason::completed())))
            .flatten();
        let sent = complete_v2(&ctx, t, &s, closure).await;
        message_confirmed(
            &ctx,
            sent,
            MessageStatus::Completed,
            "completed on a whole turn",
        )
        .await;
        let detail = named_completed.then_some(DETAIL);
        ended_as(
            &ctx,
            &a,
            t,
            s.message,
            MessageStatus::Completed,
            "completed",
            detail,
        )
        .await;
    }

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t30_the_status_follows_the_reason_a_failure_names() {
    let ctx = TestContext::setup().await;
    let (a, b) = (ctx.enrol("ada").await, ctx.enrol("bob").await);
    let t = create_thread(&ctx, Some(&a), &[&b]).await;
    let m1 = say(&ctx, t, None, &a, "Go on").await;
    let mut b_feed = Live::thread(&ctx, &b.passport, t).await;

    let cases = [
        (
            naming(&named(ClosureReason::interrupted())),
            MessageStatus::Failed,
            "interrupted",
            Some(DETAIL),
        ),
        (
            naming(&named(ClosureReason::cancelled())),
            MessageStatus::Cancelled,
            "cancelled",
            Some(DETAIL),
        ),
        (
            naming(&ClosureReason::failed(DETAIL).expect("a detail")),
            MessageStatus::Failed,
            "failed",
            Some(DETAIL),
        ),
        (
            None,
            MessageStatus::Failed,
            "failed",
            Some("failed_by_writer"),
        ),
    ];
    for (closure, status, kind, detail) in cases {
        let s = streaming(&ctx, t, m1, Shape::Producing).await;
        let sent = fail_v2(&ctx, t, &s, closure).await;
        message_confirmed(&ctx, sent, status, kind).await;
        ended_as(&ctx, &a, t, s.message, status, kind, detail).await;
        let wanted = status.to_string();
        let delivered = b_feed
            .until("the closure reaches B's feed", |delta| {
                upsert_of(delta, s.message).is_some_and(|view| {
                    view["status"] == json!(wanted) && closure_kind(view).as_deref() == Some(kind)
                })
            })
            .await;
        let view = upsert_of(&delivered, s.message).expect("an Upsert of the message");
        let reason = to_json(&closure_of(view).expect("a closure"));
        assert_eq!(reason["detail"].as_str(), detail, "{view}");
    }

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t30_a_closure_the_library_cannot_read_is_refused_and_the_message_stays_streaming() {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Go on").await;
    let s = streaming(&ctx, t, m1, Shape::Producing).await;
    let before = facts::of(&ctx.pool, "message", s.message).await.len();

    for unreadable in [json!({ "type": "failed" }), json!({ "type": "sleeping" })] {
        let sent = fail_v2(&ctx, t, &s, Some(LibraryJson::from_json(unreadable))).await;
        message_refused(
            &ctx,
            sent,
            ReasonCode::MessageInvalid,
            "an unreadable closure",
        )
        .await;
        let sent = complete_v2(
            &ctx,
            t,
            &s,
            Some(LibraryJson::from_json(json!({ "type": "failed" }))),
        )
        .await;
        message_refused(
            &ctx,
            sent,
            ReasonCode::MessageInvalid,
            "an unreadable closure",
        )
        .await;
    }
    assert_eq!(
        facts::of(&ctx.pool, "message", s.message).await.len(),
        before
    );
    let view = message_view(&ctx, &a.passport, t, s.message).await;
    assert_eq!(view["status"], "STREAMING", "{view}");

    let sent = fail_v2(
        &ctx,
        t,
        &s,
        naming(&ClosureReason::failed(DETAIL).expect("a detail")),
    )
    .await;
    message_confirmed(&ctx, sent, MessageStatus::Failed, "the readable closure").await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t30_a_stream_the_library_refused_fails_whatever_reason_is_named_and_v1_behaves_as_before()
{
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Go on").await;

    let message = open_stream(&ctx, t, Some(m1), ai("a1", "Analyst")).await;
    let mut response = Streamed::start(message, None);
    response.text(&["fine"], true);
    let mut chunks = response.chunks();
    chunks.push(common::content::delta_chunk(&response.refused(
        contract_threads::llm::DeltaOp::text_appended(
            contract_threads::llm::BlockId::from_uuid(Uuid::now_v7()).expect("a UUIDv7"),
            "to no block",
        ),
    )));
    let (last_seq, hash) = stream::push(&ctx.nats.url(), message, 0, &chunks).await;
    let s = Streaming {
        message,
        seal: StreamSeal { last_seq, hash },
    };
    let sent = complete_v2(&ctx, t, &s, naming(&budget())).await;
    message_confirmed(
        &ctx,
        sent,
        MessageStatus::Failed,
        "budget exhausted on a refused stream",
    )
    .await;
    ended_as(&ctx, &a, t, message, MessageStatus::Failed, "failed", None).await;

    let s = streaming(&ctx, t, m1, Shape::Calling).await;
    let sent = command(
        &ctx,
        &completion(t, s.message, s.seal.last_seq, &s.seal.hash),
    )
    .await;
    message_confirmed(
        &ctx,
        sent,
        MessageStatus::Failed,
        "v1 completion with an open call",
    )
    .await;
    ended_as(
        &ctx,
        &a,
        t,
        s.message,
        MessageStatus::Failed,
        "failed",
        None,
    )
    .await;

    let s = streaming(&ctx, t, m1, Shape::Producing).await;
    let sent = command(
        &ctx,
        &FailMessage {
            message_id: s.message,
            thread_id: t,
            seal: Some(s.seal.clone()),
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(&ctx, sent, MessageStatus::Failed, "v1 failure").await;
    ended_as(
        &ctx,
        &a,
        t,
        s.message,
        MessageStatus::Failed,
        "failed",
        None,
    )
    .await;

    ctx.teardown().await;
}

const NOTE: &str = "careful";

async fn push_unready(
    ctx: &TestContext,
    thread: Uuid,
    parent: Uuid,
    unready: Uuid,
) -> (Uuid, StreamSeal) {
    let message = open_stream(ctx, thread, Some(parent), ai("a1", "Analyst")).await;
    let mut answer = Streamed::start(message, None);
    answer.text(&["the report follows"], true);
    answer.complete(ResponseEnding::finished());
    let mut chunks = answer.chunks();
    chunks.push(annotation_chunk(answer.entry, warning("NOTE", NOTE)));
    chunks.push(annotation_chunk(answer.entry, artifact(unready)));
    let (last_seq, hash) = stream::push(&ctx.nats.url(), message, 0, &chunks).await;
    (message, StreamSeal { last_seq, hash })
}

async fn waiting(ctx: &TestContext, thread: Uuid, parent: Uuid, unready: Uuid) -> Streaming {
    let (message, seal) = push_unready(ctx, thread, parent, unready).await;
    Streaming { message, seal }
}

async fn abandoned(ctx: &TestContext, thread: Uuid, parent: Uuid, unready: Uuid) -> Uuid {
    push_unready(ctx, thread, parent, unready).await.0
}

async fn message_ledger(ctx: &TestContext, message: Uuid) -> Vec<(i64, String)> {
    facts::of(&ctx.pool, "message", message)
        .await
        .into_iter()
        .map(|fact| (fact.seq, fact.event_type))
        .collect()
}

async fn holds_both_annotations(
    ctx: &TestContext,
    viewer: &common::Person,
    thread: Uuid,
    message: Uuid,
) {
    let view = await_message(
        ctx,
        &viewer.passport,
        thread,
        message,
        "the streaming view holds the note and the artifact",
        |view| view["status"] == "STREAMING" && annotations_of(view).len() == 2,
    )
    .await;
    let mut kinds: Vec<String> = annotations_of(&view)
        .iter()
        .map(|(_, annotation)| annotation["kind"].as_str().unwrap_or_default().to_owned())
        .collect();
    kinds.sort();
    assert_eq!(kinds, ["ARTIFACT", "WARNING"], "{view}");
}

async fn requested_not_attached(ctx: &TestContext, artifact: Uuid) {
    assert_eq!(
        facts::types(&facts::of(&ctx.pool, "artifact", artifact).await),
        vec!["Requested"],
        "the artifact is not attached"
    );
}

fn kept_only_the_note(view: &serde_json::Value) {
    let annotations = annotations_of(view);
    assert_eq!(annotations.len(), 1, "only the note stays: {view}");
    assert_eq!(annotations[0].1["kind"], "WARNING", "{view}");
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t30_a_closing_that_gives_completed_is_refused_while_the_artifact_is_not_ready_and_the_stream_stays_open()
 {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Send me the report").await;
    let runner = ctx.service_account(true);
    let file = File::csv("quarter,revenue\nQ3,42\n");
    let z = Uuid::now_v7();
    let access = request_upload(&ctx, &runner.passport, t, z, &file).await;
    let s = waiting(&ctx, t, m1, z).await;
    holds_both_annotations(&ctx, &a, t, s.message).await;
    let facts_before = message_ledger(&ctx, s.message).await;
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;

    let sent = [
        complete_v2(&ctx, t, &s, None).await,
        complete_v2(&ctx, t, &s, naming(&budget())).await,
        complete_v2(&ctx, t, &s, naming(&ClosureReason::completed())).await,
        complete_v2(&ctx, t, &s, naming(&named(ClosureReason::cancelled()))).await,
        complete_v2(&ctx, t, &s, naming(&named(ClosureReason::interrupted()))).await,
        complete_v2(
            &ctx,
            t,
            &s,
            naming(&ClosureReason::failed(DETAIL).expect("a detail")),
        )
        .await,
        fail_v2(&ctx, t, &s, naming(&ClosureReason::completed())).await,
        fail_v2(&ctx, t, &s, naming(&budget())).await,
    ];
    for command_id in sent {
        message_refused(
            &ctx,
            command_id,
            ReasonCode::ArtifactNotReady,
            "a closing that gives COMPLETED, or a completion whatever the reason, the artifact not ready",
        )
        .await;
    }
    assert_eq!(
        message_ledger(&ctx, s.message).await,
        facts_before,
        "a refused closing changes no fact"
    );
    a_feed
        .silence("a refused closing changes no view a live member holds")
        .await;
    let view = message_view(&ctx, &a.passport, t, s.message).await;
    assert_eq!(view["status"], "STREAMING", "{view}");
    assert!(closure_kind(&view).is_none(), "{view}");
    assert_eq!(annotations_of(&view).len(), 2, "{view}");
    requested_not_attached(&ctx, z).await;

    let status = post(&access, &file.bytes).await;
    assert!((200..300).contains(&status), "the store takes the bytes");
    let sent = complete_v2(&ctx, t, &s, None).await;
    message_confirmed(
        &ctx,
        sent,
        MessageStatus::Completed,
        "the same stream, the artifact ready",
    )
    .await;
    a_feed
        .until("the completion reaches the feed", |delta| {
            upsert_of(delta, s.message).is_some_and(|view| view["status"] == "COMPLETED")
        })
        .await;
    assert_eq!(
        facts::types(&facts::of(&ctx.pool, "artifact", z).await),
        vec!["Requested", "Attached"]
    );

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t30_a_fail_or_a_cancel_drops_the_annotation_of_an_artifact_not_ready_and_keeps_the_rest() {
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Send me the report").await;
    let runner = ctx.service_account(true);
    let z = Uuid::now_v7();
    request_upload(&ctx, &runner.passport, t, z, &File::csv("pending\n")).await;

    let fail_v1 = waiting(&ctx, t, m1, z).await;
    holds_both_annotations(&ctx, &a, t, fail_v1.message).await;
    let sent = command(
        &ctx,
        &FailMessage {
            message_id: fail_v1.message,
            thread_id: t,
            seal: Some(fail_v1.seal),
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(&ctx, sent, MessageStatus::Failed, "fail").await;

    let fail_v2 = waiting(&ctx, t, m1, z).await;
    holds_both_annotations(&ctx, &a, t, fail_v2.message).await;
    let sent = command(
        &ctx,
        &FailMessageV2 {
            message_id: fail_v2.message,
            thread_id: t,
            seal: Some(fail_v2.seal),
            closure: naming(&ClosureReason::interrupted()),
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(&ctx, sent, MessageStatus::Failed, "fail naming interrupted").await;

    let cancel = waiting(&ctx, t, m1, z).await;
    holds_both_annotations(&ctx, &a, t, cancel.message).await;
    let sent = command(
        &ctx,
        &CancelMessage {
            message_id: cancel.message,
            thread_id: t,
            seal: Some(cancel.seal),
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(&ctx, sent, MessageStatus::Cancelled, "cancel").await;

    for message in [fail_v1.message, fail_v2.message, cancel.message] {
        let view = message_view(&ctx, &a.passport, t, message).await;
        kept_only_the_note(&view);
    }
    requested_not_attached(&ctx, z).await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t30_a_silence_drops_the_annotation_of_an_artifact_not_ready_and_keeps_the_rest() {
    let ctx = TestContext::setup_with(Windows {
        stream_idle: Some(SHORT_STREAM_IDLE),
        ..Windows::default()
    })
    .await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Send me the report").await;
    let runner = ctx.service_account(true);
    let z = Uuid::now_v7();
    request_upload(&ctx, &runner.passport, t, z, &File::csv("pending\n")).await;
    let message = abandoned(&ctx, t, m1, z).await;
    holds_both_annotations(&ctx, &a, t, message).await;

    tokio::time::sleep(SHORT_STREAM_IDLE * 2).await;
    let view = await_message(&ctx, &a.passport, t, message, "failed by silence", |view| {
        view["status"] == "FAILED"
    })
    .await;
    assert_eq!(
        closure_kind(&view).as_deref(),
        Some("interrupted"),
        "{view}"
    );
    kept_only_the_note(&view);
    requested_not_attached(&ctx, z).await;

    ctx.teardown().await;
}
