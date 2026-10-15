//! The one scope Threads declares to Identity at every boot, under the manifest
//! key `threads`. The engine's handshake holds readiness until Identity accepts
//! the declaration and fails loud on a rejection.
//!
//! Threads declares no human scope: reading depends only on membership, and
//! super-user status gives no access.

use service_engine::ScopeDef;

/// The machine scope of the runners' service accounts: reading a thread's
/// context, requesting an artifact upload, and the title operations of the
/// active title Job. It grants nothing on the human surface.
pub const RUNNER: &str = "threads:runner";

/// The i18n keys of the service, as Identity's scope screens show it.
pub const SERVICE_LABEL_KEY: &str = "threads.service.label";
pub const SERVICE_DESCRIPTION_KEY: &str = "threads.service.description";

pub const DECLARED: [ScopeDef; 1] = [ScopeDef::new(
    RUNNER,
    "threads.scope.runner.label",
    "threads.scope.runner.description",
)];

#[cfg(test)]
mod tests {
    use br_core_auth::ScopeKey;

    use super::*;

    #[test]
    fn exactly_the_runner_scope_is_declared_under_the_service_key() {
        assert_eq!(DECLARED.len(), 1);
        let runner = &DECLARED[0];
        assert_eq!(runner.key, "threads:runner");
        assert!(!runner.platform_only);
        assert!(runner.key.starts_with(&format!("{}:", crate::SERVICE)));
        ScopeKey::new(runner.key).expect("a valid platform scope key");
    }
}
