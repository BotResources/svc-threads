//! Playing Identity — its published language, and nothing else.
//!
//! Threads never asks Identity anything at request time: it reads the roster
//! Identity publishes (`identity/users/…` in the `PUBLISHED_LANGUAGE` bucket),
//! which its directory mirror projects into `threads.known_users`, and it
//! declares its scope at boot and waits for Identity's verdict. The suite plays
//! those wires through the shared roster types (`br-core-directory`) and the
//! typed Fabric surface — never a hand-built key or subject. The scope verdicts
//! are the `conformance-scope` battery's own acceptor.
//!
//! A roster entry carries the two extensions Threads reads by name:
//! `display_name` and `disabled_at`.

use std::collections::BTreeMap;

use br_core_directory::{PublishedUser, user_kv_key};
use br_core_scope::ScopeDeclarationError;
use br_scope_declaration_contract::declare_command_coords;
use br_test_harness::{CommandCapture, FabricTestNats, wait_until};
use br_util_nats_fabric::KvKey;
use chrono::{DateTime, Utc};
use conformance_scope::CapturedDeclare;
use uuid::Uuid;

use super::constants::{PUSH_TIMEOUT, SCOPE_RUNNER, SERVICE};

pub fn user_key(user: Uuid) -> KvKey {
    KvKey::new(user_kv_key(user)).expect("a roster key built from a UUID is a valid KV key")
}

/// The display name Identity publishes for `label`.
pub fn display_name(label: &str) -> String {
    format!("{label} Tester")
}

/// The roster entry Identity publishes for a user; deactivated when
/// `disabled_at`.
pub fn roster_entry(label: &str, disabled_at: Option<DateTime<Utc>>) -> PublishedUser {
    let mut extensions = BTreeMap::new();
    extensions.insert(
        "display_name".to_string(),
        serde_json::Value::String(display_name(label)),
    );
    if let Some(at) = disabled_at {
        extensions.insert(
            "disabled_at".to_string(),
            serde_json::to_value(at).expect("a date serializes"),
        );
    }
    PublishedUser::new(
        format!("{label}@example.test"),
        Some(label.to_string()),
        Some("Tester".to_string()),
        extensions,
    )
    .expect("a roster entry with these extensions is valid")
}

/// Identity publishes (or republishes) `user` on the roster.
pub async fn publish_user(nats: &FabricTestNats, user: Uuid, entry: &PublishedUser) {
    nats.pl_publisher::<PublishedUser>()
        .await
        .put(&user_key(user), entry)
        .await
        .expect("Identity publishes the roster entry");
}

/// Identity retracts `user` from the roster: the key is gone.
pub async fn retract_user(nats: &FabricTestNats, user: Uuid) {
    nats.pl_publisher::<PublishedUser>()
        .await
        .retract(&user_key(user))
        .await
        .expect("Identity retracts the roster entry");
}

/// Identity's side of the scope-declaration handshake: a capture of the declares
/// Threads publishes on `integration.cmd.identity.service_scope.declare.v1` (the
/// contract's own coordinates), and the conformance battery's acceptor.
pub struct ScopeHandshake {
    capture: CommandCapture,
}

impl ScopeHandshake {
    /// Bind before the service boots: the capture replays the stream from its
    /// start, so a declare published before the bind is seen all the same.
    pub async fn bind(nats: &FabricTestNats) -> Self {
        let coords = declare_command_coords().expect("the declare coordinates are well formed");
        Self {
            capture: nats.capture_commands(&[&coords]).await,
        }
    }

    pub fn count(&self) -> usize {
        self.capture.count()
    }

    /// Every declare received so far, one per message.
    pub fn declares(&self) -> Vec<CapturedDeclare> {
        let mut seen = Vec::new();
        let mut declares = Vec::new();
        for correlation in self.capture.correlation_ids() {
            if seen.contains(&correlation) {
                continue;
            }
            seen.push(correlation);
            declares.extend(
                self.capture
                    .for_correlation(correlation)
                    .into_iter()
                    .map(CapturedDeclare::from),
            );
        }
        declares
    }

    /// Wait until at least `count` declares have been published; answer the
    /// correlation id of the latest.
    pub async fn await_declare(&self, count: usize, what: &str) -> Uuid {
        let arrived = wait_until(PUSH_TIMEOUT, || async { self.capture.count() >= count }).await;
        assert!(
            arrived,
            "{what}: Threads published {} scope declaration(s), {count} expected",
            self.capture.count()
        );
        *self
            .capture
            .correlation_ids()
            .last()
            .expect("a declare was captured")
    }

    pub async fn accept(&self, nats: &FabricTestNats, correlation: Uuid) {
        conformance_scope::accept(nats.fabric(), &service_key(), correlation)
            .await
            .expect("Identity accepts the declaration");
    }

    /// Identity rejects the declaration: its scope is owned by another service.
    pub async fn reject_owned_by_another_service(&self, nats: &FabricTestNats, correlation: Uuid) {
        conformance_scope::reject(
            nats.fabric(),
            &service_key(),
            ScopeDeclarationError::ScopeOwnedByAnotherService {
                key: SCOPE_RUNNER.to_string(),
                owner: "another_service".to_string(),
            },
            correlation,
        )
        .await
        .expect("Identity rejects the declaration");
    }

    pub async fn stop(self) {
        self.capture.stop().await;
    }
}

pub fn service_key() -> br_core_scope::ServiceKey {
    br_core_scope::ServiceKey::new(SERVICE.to_string()).expect("`threads` is a valid service key")
}
