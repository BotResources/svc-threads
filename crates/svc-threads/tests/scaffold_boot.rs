//! **The binary's own surface** — not a registry test: what the suite relies
//! on before any scenario boots the service.
//!
//! - `svc-threads schema` prints the GraphQL surface without any infra: every
//!   root the scenarios query, mutate or subscribe to is in it.
//! - Once booted, the members offer publishes its manifest
//!   (`threads.members.`, version 1) — the one fact of a nominal boot no
//!   registry test reads.
//!
//! The boot itself — migration, the declaration of `threads:runner` under
//! `threads`, readiness only once Identity accepts — is T18's; admission by
//! roster and by runner scope is T04's; a user leaving the roster is T14's;
//! the live views opening on a `Reset` over SSE and WebSocket are T01's, T04's
//! and T06's. Each registry test is one file of its own, `tNN_*.rs`.

mod common;

use common::{PUSH_TIMEOUT, TestContext, eventually};
use serde_json::json;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn the_members_offer_publishes_its_manifest_once_booted() {
    // Given the service booted, declared, accepted and ready.
    let ctx = TestContext::setup().await;

    // Then the members offer publishes its manifest.
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
