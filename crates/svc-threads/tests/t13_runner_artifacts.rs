//! **T13 — Runner artifacts: upload, attach, not ready.**
//!
//! > A threads:runner service account requests an upload for thread T:
//! > artifact X under a minted id, report.csv, text/csv, a size and a SHA-256:
//! > it receives a time-bounded upload access and a Requested fact is recorded
//! > by the runner; bytes of another size or another digest are refused by the
//! > object store. A declared size above 256 MiB is refused ARTIFACT_TOO_LARGE.
//! > Before the bytes land, storing a message with an ARTIFACT annotation
//! > naming X is refused ARTIFACT_NOT_READY and nothing is stored. After the
//! > upload, the same message is stored: confirmed, X is ATTACHED to it, and
//! > member A obtains a time-bounded read access returning the bytes;
//! > non-member U is answered exactly as for an artifact nobody created, and so
//! > is A reaching X through another thread T2. A message of T2 naming X is
//! > refused ARTIFACT_NOT_READY. Requesting an upload under X's id again is
//! > refused KEY_REUSED. A member and a service account without threads:runner
//! > are refused the upload request with RUNNER_SCOPE_REQUIRED. With a short
//! > upload window (THREADS_UPLOAD_WINDOW_MS), artifact Y uploaded and never
//! > attached is removed with its bytes once the window elapses, a message
//! > naming Y is then refused ARTIFACT_NOT_READY, and X, attached, is untouched.
//!
//! The message naming X is an AI turn whose answer carries X in an ARTIFACT
//! annotation after it.
//!
//! The upload window is configured short for the one scenario that waits it
//! out (`THREADS_UPLOAD_WINDOW_MS`, see `common::constants`); the main one runs
//! on the service's default.
//!
//! Channels: the runner operation's verdict and access (1); the object store's
//! own refusal; the answers (4); the read access as members and strangers get
//! it (2); the artifact's facts; the bucket's content.

mod common;

use common::artifacts::{
    File, download_answer, download_url, fetch, object_count, post, request_upload,
    request_upload_answer, upload,
};
use common::content::{after, ai_turn, artifact, closed, reply};
use common::facts;
use common::flows::{
    ai, command, create_thread, message_confirmed, message_refused, say, whole_message,
};
use common::{
    KEY_REUSED, PUSH_TIMEOUT, RUNNER_SCOPE_REQUIRED, SHORT_UPLOAD_WINDOW, TestContext, Windows,
    eventually, gql,
};
use contract_threads::llm::Turn;
use contract_threads::{MessageStatus, ReasonCode, StoreMessageV2};
use uuid::Uuid;

/// An AI answer on `thread` naming `attached` in an ARTIFACT annotation after
/// its answer.
fn naming(thread: Uuid, parent: Uuid, attached: Uuid) -> StoreMessageV2 {
    let analyst = ai("a1", "Analyst");
    let answer = reply("the report is attached");
    let message = Uuid::now_v7();
    let turn = Turn::from(ai_turn(message, &analyst, closed(answer.clone())));
    whole_message(
        thread,
        Some(parent),
        message,
        &analyst,
        &turn,
        vec![after(&answer[0], artifact(attached))],
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

    // When the runner declares an artifact above 256 MiB.
    let huge = Uuid::now_v7();
    let too_large = ctx
        .run(
            &runner.passport,
            common::M_REQUEST_UPLOAD,
            serde_json::json!({
                "thread": t,
                "artifact": huge,
                "name": file.name,
                "mediaType": file.media_type,
                "size": 256 * 1024 * 1024 + 1,
                "sha256": file.sha256(),
            }),
        )
        .await;
    // Then refused ARTIFACT_TOO_LARGE, and nothing is requested.
    gql::assert_refused(&too_large, "ARTIFACT_TOO_LARGE");
    assert!(facts::of(&ctx.pool, "artifact", huge).await.is_empty());

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
    // And A, a member of T2 too, reaching X through T2 is answered exactly as
    // for an artifact nobody created.
    assert_eq!(
        blank(download_answer(&ctx, &a.passport, t2, x).await, x),
        blank(download_answer(&ctx, &a.passport, t2, random).await, random)
    );
    assert_eq!(download_url(&ctx, &a.passport, t2, x).await, None);

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

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t13_an_upload_never_attached_is_removed_after_the_upload_window() {
    // Given Threads running with a short upload window; T with member A; X,
    // uploaded and attached at once; Y, uploaded and never attached.
    let ctx = TestContext::setup_with(Windows {
        upload: Some(SHORT_UPLOAD_WINDOW),
        ..Windows::default()
    })
    .await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "Send me both").await;
    let runner = ctx.service_account(true);
    let file = File::csv("kept\n");
    let x = upload(&ctx, &runner.passport, t, &file).await;
    let stored = command(&ctx, &naming(t, m1, x)).await;
    message_confirmed(&ctx, stored, MessageStatus::Completed, "attaching X").await;
    let url = download_url(&ctx, &a.passport, t, x)
        .await
        .expect("a member reads X");
    let y = upload(&ctx, &runner.passport, t, &File::csv("orphan\n")).await;
    let objects = object_count(&ctx).await;

    // When the upload window elapses.
    tokio::time::sleep(SHORT_UPLOAD_WINDOW).await;

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
    let late = command(&ctx, &naming(t, m1, y)).await;
    message_refused(
        &ctx,
        late,
        ReasonCode::ArtifactNotReady,
        "Y after its window",
    )
    .await;
    // And X, attached, is untouched by the window.
    assert_eq!(fetch(&url).await, (200, file.bytes));
    assert!(!facts::types(&facts::of(&ctx.pool, "artifact", x).await).contains(&"Removed"));

    ctx.teardown().await;
}
