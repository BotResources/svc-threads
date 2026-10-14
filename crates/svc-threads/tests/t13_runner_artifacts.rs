//! **T13 — Runner artifacts: upload, attach, not ready.**
//!
//! > A threads:runner service account requests an upload for thread T:
//! > artifact X under a minted id, report.csv, text/csv, a size and a SHA-256:
//! > X is PENDING with a time-bounded upload access; bytes of another size or
//! > digest are refused by object storage. Before the bytes land, storing a
//! > message with an ARTIFACT block naming X is refused ARTIFACT_NOT_READY.
//! > After the upload, the same message is stored: X is ATTACHED to it and a
//! > member obtains a time-bounded read access returning the bytes; a
//! > non-member is answered not found. A message of thread T2 naming X is
//! > refused ARTIFACT_NOT_READY. Requesting an upload under a held id is
//! > refused KEY_REUSED. A user and a service account without threads:runner
//! > are refused the upload request. An upload never attached is removed after
//! > the upload window. Deleting X's message removes X with its bytes.
//!
//! "Deleting X's message" cannot be reached: X is attached by an AI message,
//! and "An AI message is never deleted" (specification, § Message). The
//! removal of an attached artifact with its bytes is asserted where it is
//! reachable, at the thread's deletion (T17); the gap is the operator's.
//!
//! The upload window is configured short for the suite
//! (`THREADS_UPLOAD_WINDOW_MS`, see `common::constants`).
//!
//! Channels: the runner operation's verdict and access (1); the object store's
//! own refusal; the answers (4); the read access as members and strangers get
//! it (2); the artifact's facts; the bucket's content.

mod common;

use common::artifacts::{
    File, download_answer, download_url, fetch, object_count, post, request_upload,
    request_upload_answer, upload,
};
use common::content::{ai_text, artifact_block, step};
use common::facts;
use common::flows::{
    ai, ai_message, command, create_thread, message_confirmed, message_refused, say,
};
use common::{
    KEY_REUSED, PUSH_TIMEOUT, RUNNER_SCOPE_REQUIRED, TestContext, UPLOAD_WINDOW, eventually, gql,
};
use contract_threads::llm::StopReason;
use contract_threads::{MessageStatus, ReasonCode, StoreMessage};
use uuid::Uuid;

/// An AI answer on `thread` naming `artifact` in an ARTIFACT block.
fn naming(thread: Uuid, parent: Uuid, artifact: Uuid) -> StoreMessage {
    ai_message(
        thread,
        Some(parent),
        ai("a1", "Analyst"),
        vec![step(
            StopReason::EndTurn,
            None,
            vec![ai_text("the report is attached"), artifact_block(artifact)],
        )],
    )
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t13_a_runners_artifact_is_uploaded_pinned_attached_when_ready_and_read_by_members_only() {
    // Given T with member A, T2 with member A, U not a member, and a runner.
    let ctx = TestContext::setup().await;
    let (a, u) = (ctx.enrol("ada").await, ctx.enrol("ugo").await);
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let t2 = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Send me the report").await;
    let m1_t2 = say(&ctx, t2, None, &a, "And here?").await;
    let runner = ctx.service_account(true);
    let file = File::csv("quarter,revenue\nQ3,42\n");

    // When the runner requests an upload for T: X, report.csv, text/csv, its
    // size and digest.
    let x = Uuid::now_v7();
    let access = request_upload(&ctx, &runner.passport, t, x, &file).await;

    // Then (1) it receives a time-bounded upload access, and X is requested.
    assert!(access["url"].is_string(), "{access}");
    assert!(access["fields"].is_object(), "{access}");
    let x_facts = facts::of(&ctx.pool, "artifact", x).await;
    assert_eq!(facts::types(&x_facts), vec!["Requested"]);
    assert_eq!(x_facts[0].actor_id, runner.id);

    // When bytes of another size, then of another digest, are posted to it.
    let mut longer = file.bytes.clone();
    longer.extend_from_slice(b"Q4,7\n");
    let mut altered = file.bytes.clone();
    *altered.last_mut().expect("bytes") = b'?';
    // Then the object store refuses both.
    for (bytes, what) in [(longer, "another size"), (altered, "another digest")] {
        let status = post(&access, &bytes).await;
        assert!(
            !(200..300).contains(&status),
            "{what}: the store refuses: {status}"
        );
    }

    // When, before the bytes land, a message naming X is stored.
    let early = naming(t, m1, x);
    let m2 = early.message_id;
    let refused = command(&ctx, &early).await;
    // Then refused ARTIFACT_NOT_READY; nothing is stored.
    message_refused(
        &ctx,
        refused,
        ReasonCode::ArtifactNotReady,
        "X's bytes absent",
    )
    .await;
    assert!(facts::of(&ctx.pool, "message", m2).await.is_empty());

    // When the declared bytes land, and the same message is stored.
    let status = post(&access, &file.bytes).await;
    assert!(
        (200..300).contains(&status),
        "the store takes the bytes: {status}"
    );
    let stored = command(&ctx, &early).await;

    // Then (4) confirmed; X is attached to it.
    message_confirmed(
        &ctx,
        stored,
        MessageStatus::Completed,
        "the message naming X",
    )
    .await;
    assert_eq!(
        facts::types(&facts::of(&ctx.pool, "artifact", x).await),
        vec!["Requested", "Attached"]
    );
    // And (2) A obtains a read access that returns the bytes.
    let url = download_url(&ctx, &a.passport, t, x)
        .await
        .expect("a member obtains a read access");
    assert_eq!(fetch(&url).await, (200, file.bytes.clone()));
    // And U is answered not found, exactly as for an artifact nobody created.
    let blank = |answer: serde_json::Value, id: Uuid| {
        format!("{:?} {}", gql::code(&answer), answer["data"]).replace(&id.to_string(), "<id>")
    };
    let random = Uuid::now_v7();
    assert_eq!(
        blank(download_answer(&ctx, &u.passport, t, x).await, x),
        blank(download_answer(&ctx, &u.passport, t, random).await, random)
    );
    assert_eq!(download_url(&ctx, &u.passport, t, x).await, None);

    // When a message of T2 names X.
    let foreign = command(&ctx, &naming(t2, m1_t2, x)).await;
    // Then refused ARTIFACT_NOT_READY.
    message_refused(
        &ctx,
        foreign,
        ReasonCode::ArtifactNotReady,
        "X named from T2",
    )
    .await;

    // When the runner requests an upload under X's id again.
    let reused = request_upload_answer(&ctx, &runner.passport, t, x, &file).await;
    // Then refused KEY_REUSED.
    gql::assert_refused(&reused, KEY_REUSED);

    // When a member, then a service account without threads:runner, request
    // an upload.
    let plain_service = ctx.service_account(false);
    for passport in [&a.passport, &plain_service.passport] {
        let refused = request_upload_answer(&ctx, passport, t, Uuid::now_v7(), &file).await;
        // Then each is refused the operation.
        gql::assert_refused(&refused, RUNNER_SCOPE_REQUIRED);
    }

    // Given Y, uploaded for T and never attached.
    let y = upload(&ctx, &runner.passport, t, &File::csv("orphan\n")).await;
    let objects = object_count(&ctx).await;

    // When the upload window elapses.
    tokio::time::sleep(UPLOAD_WINDOW).await;

    // Then Y is removed, with its bytes.
    eventually("Y is removed after the window", PUSH_TIMEOUT, || async {
        facts::types(&facts::of(&ctx.pool, "artifact", y).await).contains(&"Removed")
    })
    .await;
    eventually("Y's bytes are gone", PUSH_TIMEOUT, || async {
        object_count(&ctx).await == objects - 1
    })
    .await;
    // And a message naming Y is refused ARTIFACT_NOT_READY.
    let m3 = say(&ctx, t, Some(m2), &a, "and the other one?").await;
    let late = command(&ctx, &naming(t, m3, y)).await;
    message_refused(
        &ctx,
        late,
        ReasonCode::ArtifactNotReady,
        "Y after its window",
    )
    .await;
    // And X, attached, is untouched by the window.
    assert_eq!(fetch(&url).await, (200, file.bytes));

    ctx.teardown().await;
}
