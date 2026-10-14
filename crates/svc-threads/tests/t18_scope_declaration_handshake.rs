//! **T18 — Scope declaration handshake at boot.**
//!
//! > At boot Threads publishes one scope declaration with manifest key threads
//! > and exactly one scope, threads:runner. Playing Identity, the harness
//! > answers accepted: Threads becomes ready. Restarting re-sends the identical
//! > declaration; accepted again, nothing else changes. The harness answers
//! > rejected with scope_owned_by_another_service: Threads logs the reason and
//! > never reports ready.
//!
//! Channels: the handshake is channel 4 end to end — the declare on
//! `INTEGRATION_CMD`, decoded through the frozen `IntegrationCommand<
//! DeclareServiceScopes>`, and Identity's verdict published by the
//! `conformance-scope` battery's own acceptor — with `/readyz` as the
//! observable the verdict drives. The frozen wire itself is gated by the
//! MANDATORY `conformance-scope` battery, run in attach mode against this very
//! binary (its spawn mode drives the frozen Go subject's environment, which a
//! service with a database and an object store cannot boot under).

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use br_core_integration::IntegrationCommand;
use br_core_scope::DeclareServiceScopes;
use common::{
    DECLARED_SCOPES, Handshake, QUIET_WINDOW, READY_TIMEOUT, SERVICE, TestContext, facts,
};
use conformance_scope::{
    AcceptorBehavior, AttachTarget, DEFAULT_TIMEOUT, ExpectedDeclaration, PlatformOnly,
    attach_default, run_attach,
};

fn expected() -> ExpectedDeclaration {
    ExpectedDeclaration::from_parts(
        SERVICE,
        &DECLARED_SCOPES.map(str::to_string),
        &PlatformOnly::All(false),
    )
}

fn decode(raw: &[u8]) -> IntegrationCommand<DeclareServiceScopes> {
    serde_json::from_slice(raw).unwrap_or_else(|e| {
        panic!(
            "the declare decodes through the frozen IntegrationCommand<DeclareServiceScopes> \
             ({e}): {}",
            String::from_utf8_lossy(raw)
        )
    })
}

/// `/readyz` never answers 200 over `window`.
async fn assert_not_ready_over(ctx: &TestContext, window: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        let (status, body) = ctx.readyz().await;
        assert_ne!(status, 200, "{what}: /readyz reported ready: {body}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn await_ready(ctx: &TestContext, what: &str) {
    let ready =
        br_test_harness::wait_until(READY_TIMEOUT, || async { ctx.readyz().await.0 == 200 }).await;
    assert!(ready, "{what}: /readyz never turned ready\n{}", ctx.logs());
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t18_threads_declares_its_one_scope_at_every_boot_and_is_ready_only_once_accepted() {
    // Given Threads booting, with nobody yet answering for Identity.
    let mut ctx = TestContext::boot(Handshake::Withhold).await;

    // When it boots, it publishes one scope declaration…
    let correlation = ctx.scopes.await_declare(1, "the first boot").await;
    let first = decode(&ctx.scopes.declares()[0].raw);

    // Then (4) it names the manifest key `threads` and exactly one scope,
    // `threads:runner`, decoded through the frozen contract types.
    let declaration = first
        .payload
        .clone()
        .validate()
        .expect("the declaration validates against br-core-scope");
    expected()
        .assert_matches(&declaration)
        .unwrap_or_else(|mismatch| panic!("the declared content: {mismatch}"));
    let keys: BTreeSet<String> = declaration
        .scopes()
        .iter()
        .map(|scope| scope.key.as_str().to_string())
        .collect();
    assert_eq!(keys, BTreeSet::from(["threads:runner".to_string()]));

    // …and holds readiness down while Identity has not answered.
    assert_not_ready_over(&ctx, QUIET_WINDOW, "before Identity's verdict").await;

    // When Identity accepts, Then Threads becomes ready.
    ctx.scopes.accept(&ctx.nats, correlation).await;
    await_ready(&ctx, "after the acceptance").await;
    let facts_before = facts::count(&ctx.pool).await;

    // When Threads restarts.
    ctx.stop().await;
    ctx.start_again(Handshake::Withhold).await;
    let again = ctx.scopes.await_declare(2, "the restart").await;

    // Then it re-sends the identical declaration, and waits for its verdict.
    let declares = ctx.scopes.declares();
    let second = decode(&declares.last().expect("a second declare").raw);
    assert_eq!(
        second.payload, first.payload,
        "a restart re-sends the identical declaration"
    );
    assert_eq!(second.command_type, first.command_type);
    assert_not_ready_over(&ctx, QUIET_WINDOW, "the restart, before its verdict").await;

    // When accepted again, Then it is ready and nothing else changes.
    ctx.scopes.accept(&ctx.nats, again).await;
    await_ready(&ctx, "after the second acceptance").await;
    let declared = ctx.scopes.count();
    tokio::time::sleep(QUIET_WINDOW).await;
    assert_eq!(
        ctx.scopes.count(),
        declared,
        "an accepted declaration is not re-sent"
    );
    assert_eq!(facts::count(&ctx.pool).await, facts_before);

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t18_a_rejected_declaration_is_logged_and_threads_never_reports_ready() {
    // Given Threads booting and declaring.
    let ctx = TestContext::boot(Handshake::Withhold).await;
    let correlation = ctx.scopes.await_declare(1, "the boot").await;

    // When Identity rejects it: the scope is owned by another service.
    ctx.scopes
        .reject_owned_by_another_service(&ctx.nats, correlation)
        .await;

    // Then Threads never reports ready…
    assert_not_ready_over(&ctx, QUIET_WINDOW * 3, "after the rejection").await;
    // …and says why in its log.
    assert!(
        ctx.logs().contains("scope_owned_by_another_service"),
        "the rejection's reason is logged:\n{}",
        ctx.logs()
    );

    ctx.teardown().await;
}

/// The frozen wire handshake, gated by the MANDATORY conformance battery in
/// attach mode against the real binary.
#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t18_the_scope_declaration_conformance_battery_passes_against_the_binary() {
    let ctx = TestContext::boot(Handshake::Withhold).await;

    let report = run_attach(
        &AttachTarget {
            nats_url: ctx.nats.url(),
            readyz_url: ctx.readyz_url(),
        },
        &expected(),
        &AcceptorBehavior::Accept,
        &attach_default(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("the conformance-scope battery runs");
    assert!(
        report.is_conformant(),
        "conformance-scope (attach) must pass: {report:#?}\n{}",
        ctx.logs()
    );

    ctx.teardown().await;
}
