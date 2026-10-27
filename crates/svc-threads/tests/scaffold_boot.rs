//! **The 0.1.0 scaffold boots** — the floor the oracle suite builds on, not a
//! registry test: the binary migrates a fresh database, declares its one scope
//! `threads:runner` under the manifest key `threads`, stays not ready until
//! Identity accepts, then serves; it publishes the manifest of its members
//! offer; its directory mirror projects a roster entry, keeps a retracted one
//! out of the roster, and the two admission rules read it.
//!
//! The registry's e2e tests (T01–T18 in the draft contract) are the oracle
//! author's, one file each.

mod common;

use std::collections::BTreeSet;

use br_core_integration::IntegrationCommand;
use br_core_scope::DeclareServiceScopes;
use br_test_harness::{SseSubscription, WsSubscription};
use common::{
    ACTIVE_USER_REQUIRED, DECLARED_SCOPES, Handshake, PUSH_TIMEOUT, Q_CONTEXT, Q_THREAD, Q_THREADS,
    QUIET_WINDOW, READY_TIMEOUT, RUNNER_SCOPE_REQUIRED, SERVICE, TestContext, eventually, gql,
    identity, person_passport,
};
use conformance_scope::{ExpectedDeclaration, PlatformOnly};
use serde_json::json;
use uuid::Uuid;

/// The tables of the specification's objects, each readable by the app role.
/// Where a message's entries, blocks and annotations are stored is the
/// implementation's choice (0.5.0 replaced 0.4.0's `message_item` and
/// `message_block`): the suite reads them through `common::store`.
const TABLES: [&str; 8] = [
    "threads.known_users",
    "threads.thread",
    "threads.thread_member",
    "threads.thread_resource",
    "threads.title_job",
    "threads.message",
    "threads.artifact",
    "threads.read_mark",
];

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn the_scaffold_migrates_declares_its_scope_and_serves_once_accepted() {
    // Given a fresh database migrated by the binary and a boot nobody answers yet.
    let ctx = TestContext::boot(Handshake::Withhold).await;

    // Then every table of the migration is there, empty, readable by the app role.
    for table in TABLES.iter().chain(["threads.fact"].iter()) {
        let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&ctx.pool)
            .await
            .unwrap_or_else(|e| panic!("the app role reads {table}: {e}"));
        assert_eq!(rows, 0, "{table} starts empty");
    }

    // And the boot declared exactly `threads:runner`, under `threads`.
    let correlation = ctx.scopes.await_declare(1, "the boot").await;
    let declares = ctx.scopes.declares();
    let declare: IntegrationCommand<DeclareServiceScopes> =
        serde_json::from_slice(&declares[0].raw).expect("the frozen declare wire");
    let declaration = declare
        .payload
        .validate()
        .expect("the declaration validates against br-core-scope");
    ExpectedDeclaration::from_parts(
        SERVICE,
        &DECLARED_SCOPES.map(str::to_string),
        &PlatformOnly::All(false),
    )
    .assert_matches(&declaration)
    .unwrap_or_else(|mismatch| panic!("the declared content: {mismatch}"));
    let keys: BTreeSet<String> = declaration
        .scopes()
        .iter()
        .map(|scope| scope.key.as_str().to_string())
        .collect();
    assert_eq!(keys, BTreeSet::from(["threads:runner".to_string()]));

    // And it is not ready until Identity answers, then ready.
    tokio::time::sleep(QUIET_WINDOW).await;
    assert_ne!(ctx.readyz().await.0, 200, "ready before Identity's verdict");
    ctx.scopes.accept(&ctx.nats, correlation).await;
    eventually(
        "/readyz turns ready once accepted",
        READY_TIMEOUT,
        || async { ctx.readyz().await.0 == 200 },
    )
    .await;

    // And the members offer publishes its manifest.
    eventually(
        "the members manifest is published",
        PUSH_TIMEOUT,
        || async {
            ctx.members_manifest().await
                == Some(json!({ "prefix": "threads.members.", "version": 1 }))
        },
    )
    .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn the_two_surfaces_admit_by_roster_and_by_runner_scope() {
    let ctx = TestContext::setup().await;

    // An active user of the roster is admitted, and sees an empty world.
    let ada = ctx.enrol("ada").await;
    let threads = ctx
        .run(&ada.passport, Q_THREADS, json!({ "size": 10 }))
        .await;
    assert_eq!(gql::data(&threads, "threadsThreads"), &json!([]));
    let unknown = ctx
        .run(&ada.passport, Q_THREAD, json!({ "id": Uuid::now_v7() }))
        .await;
    assert_eq!(gql::data(&unknown, "threadsThread"), &json!(null));

    // Nobody is let in anonymously.
    let (status, _) = ctx.run_anonymous(Q_THREADS, json!({ "size": 10 })).await;
    assert_eq!(status, 401);

    // A person the roster never published, and any service account — a runner
    // included — are refused the human surface.
    let stranger = person_passport(Uuid::now_v7(), true);
    let runner = ctx.service_account(true);
    let plain_service = ctx.service_account(false);
    for passport in [&stranger, &runner.passport, &plain_service.passport] {
        let refused = ctx.run(passport, Q_THREADS, json!({ "size": 10 })).await;
        gql::assert_refused(&refused, ACTIVE_USER_REQUIRED);
    }

    // A user — super user or not — and a service account without
    // `threads:runner` are refused the runner surface; a runner is admitted
    // past it.
    let thread = Uuid::now_v7();
    for passport in [&ada.passport, &stranger, &plain_service.passport] {
        let refused = ctx
            .run(passport, Q_CONTEXT, json!({ "thread": thread }))
            .await;
        gql::assert_refused(&refused, RUNNER_SCOPE_REQUIRED);
    }
    let admitted = ctx
        .run(&runner.passport, Q_CONTEXT, json!({ "thread": thread }))
        .await;
    assert_ne!(
        gql::code(&admitted).as_deref(),
        Some(RUNNER_SCOPE_REQUIRED),
        "{admitted}"
    );

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn a_user_who_leaves_the_roster_is_kept_out_of_it_and_refused() {
    let ctx = TestContext::setup().await;
    let ada = ctx.enrol("ada").await;

    identity::retract_user(&ctx.nats, ada.id).await;
    eventually(
        "the roster copy keeps the user, out of the roster",
        PUSH_TIMEOUT,
        || async {
            let row: Option<(String, bool)> = sqlx::query_as(
                "SELECT display_name, in_roster FROM threads.known_users WHERE user_id = $1",
            )
            .bind(ada.id)
            .fetch_optional(&ctx.pool)
            .await
            .expect("read the roster copy");
            row == Some((identity::display_name("ada"), false))
        },
    )
    .await;
    let refused = ctx
        .run(&ada.passport, Q_THREADS, json!({ "size": 10 }))
        .await;
    gql::assert_refused(&refused, ACTIVE_USER_REQUIRED);

    // Published again, the user is back in the roster and admitted.
    identity::publish_user(&ctx.nats, ada.id, &identity::roster_entry("ada", None)).await;
    ctx.await_known_user(ada.id, "ada").await;
    let admitted = ctx
        .run(&ada.passport, Q_THREADS, json!({ "size": 10 }))
        .await;
    assert_eq!(gql::data(&admitted, "threadsThreads"), &json!([]));

    // Deactivated, they are refused everything.
    identity::publish_user(
        &ctx.nats,
        ada.id,
        &identity::roster_entry("ada", Some(chrono::Utc::now())),
    )
    .await;
    eventually("a deactivated user is refused", PUSH_TIMEOUT, || async {
        let answer = ctx
            .run(&ada.passport, Q_THREADS, json!({ "size": 10 }))
            .await;
        gql::code(&answer).as_deref() == Some(ACTIVE_USER_REQUIRED)
    })
    .await;

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn the_live_list_opens_on_a_reset_over_both_transports() {
    let ctx = TestContext::setup().await;
    let ada = ctx.enrol("ada").await;
    let document = "subscription { threadsThreadsDeltas(size: 10) { __typename \
                    ... on ThreadsReset { revision views { ... on ThreadView { id } } } } }";

    let mut sse = SseSubscription::open(&ctx.base_url, &ada.passport, document).await;
    let reset = sse
        .expect_event_on("threadsThreadsDeltas", PUSH_TIMEOUT)
        .await;
    assert_eq!(reset["__typename"], "ThreadsReset", "{reset}");
    assert_eq!(reset["views"], json!([]));

    let mut ws = WsSubscription::open(&ctx.base_url, &ada.passport, document)
        .await
        .expect("open the WebSocket subscription");
    let data = ws.next_data(PUSH_TIMEOUT).await.expect("a first frame");
    assert_eq!(
        data["threadsThreadsDeltas"]["__typename"], "ThreadsReset",
        "{data}"
    );

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: runs the built binary"]
async fn the_schema_subcommand_prints_every_root_of_the_surface() {
    let sdl = common::schema_sdl().await;
    for root in [
        "threadsThreads(",
        "threadsThread(",
        "threadsMessages(",
        "threadsArtifactDownload(",
        "threadsContext(",
        "threadsSetReadMark(",
        "threadsRequestArtifactUpload(",
        "threadsSetTitle(",
        "threadsFailTitleJob(",
        "threadsThreadsDeltas(",
        "threadsThreadDeltas(",
    ] {
        assert!(sdl.contains(root), "{root} is missing from the SDL:\n{sdl}");
    }
}
