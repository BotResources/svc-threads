//! Directory (Identity's published language) conformance — the wire profile of
//! the CONSUMER kit.
//!
//! Threads consumes `identity/users/`: its directory mirror decodes every
//! roster entry through `br-core-directory`'s `PublishedUser` into
//! `threads.known_users`, which admission, the owner and member checks and every
//! displayed name read. The battery is the platform's oracle for that wire: an
//! independent Go anchor (which never imports `br-core-directory`) emits the
//! frozen KV wire, and the Rust runner deserializes every value through the
//! real lib types. Divergence in either direction turns it red.
//!
//! | check | what it freezes for this unit |
//! |---|---|
//! | W1–W5 | the roster values the mirror deserializes round-trip through the lib types |
//! | W6    | an extension shadowing a reserved core key is REJECTED, never silently merged |
//!
//! The Cx profiles do not bind: they drive the `br-util-nats-fabric` directory
//! consumer, which Threads does not use (it mirrors through the engine's
//! `mirror`), and C1's rule — a retraction orphan-deletes the row — is the
//! opposite of Threads' specification, which keeps a retracted user, marked out
//! of the roster ("A user who leaves the roster is not removed from the known
//! users"). Threads' own retraction and reconciliation are pinned end to end by
//! T14 and `scaffold_boot`.
//!
//! Prerequisite: `go` on PATH (builds the anchor). A missing prerequisite SKIPS
//! on a laptop and FAILS in CI.

use conformance_directory::{
    DirectorySnapshotWire, build_and_emit, ensure_go_available, reserved_key_rejected,
    run_wire_battery,
};

/// A missing prerequisite SKIPS on a developer laptop and FAILS in CI: a
/// battery that silently skips gates nothing.
fn skip(reason: &str) {
    assert!(
        std::env::var("CI").is_err(),
        "conformance_directory is BINDING for svc-threads (it consumes identity/users/) and \
         cannot be skipped in CI: {reason}. Install the prerequisite on the runner \
         (actions/setup-go for the anchor)."
    );
    eprintln!("SKIP conformance_directory (svc-threads wire profile): {reason}");
}

/// Build the cross-language anchor snapshot, or `None` when `go` is unavailable.
async fn anchor() -> Option<DirectorySnapshotWire> {
    if ensure_go_available().await.is_err() {
        skip("`go` unavailable (needed to build the identity-directory anchor)");
        return None;
    }
    Some(
        build_and_emit()
            .await
            .expect("build + emit the anchor snapshot"),
    )
}

/// W1–W5 — the lib types deserialize the anchor wire the mirror reads.
#[tokio::test]
async fn w1_w5_the_roster_wire_deserializes_through_the_real_types() {
    let Some(snapshot) = anchor().await else {
        return;
    };
    let report = run_wire_battery(&snapshot);
    assert!(
        report.is_conformant(),
        "the wire-deser gate must be conformant: {} passed, {} failed\n{:#?}",
        report.passed(),
        report.failed(),
        report.outcomes,
    );
}

/// W6 — a reserved key in `extensions` is rejected at construction, never
/// silently overwritten. Offline: no infra at all.
#[tokio::test]
async fn w6_a_reserved_extension_key_is_refused_not_merged() {
    let outcome = reserved_key_rejected();
    assert!(
        outcome.is_pass(),
        "W6 reserved-key rejection must pass: expected={:?} observed={:?} detail={:?}",
        outcome.expected,
        outcome.observed,
        outcome.detail,
    );
}
