//! Who Threads admits on each of its two GraphQL surfaces. Every root asks one
//! of these first, so that a caller Threads does not admit is refused with its
//! code before any read — never answered an empty world, never told whether an
//! id exists.
//!
//! - The human surface (reading threads, messages and artifacts, following a
//!   thread live, setting one's read mark) admits an active user of the roster
//!   copy, and nobody else: a service account, a person absent from the roster,
//!   out of it or deactivated are refused everything. Super-user status gives
//!   nothing; past admission, a thread is seen by its members only, and one the
//!   caller is not a member of is reported as not found.
//! - The runner surface (a thread's context, an artifact upload, the title
//!   operations) admits a service account holding `threads:runner`, for any
//!   thread.

use service_engine::gate::{Gate, Reason};

use crate::principal::ThreadsPrincipal;
use crate::scopes;

/// The human surface requires an active user of Identity's roster, as Threads
/// holds it.
pub const ACTIVE_USER_REQUIRED: Reason = Reason::new("ACTIVE_USER_REQUIRED");
/// A runner operation without a service account holding `threads:runner`.
/// Proposed: the specification describes the refusal and names no code.
pub const RUNNER_SCOPE_REQUIRED: Reason = Reason::new("RUNNER_SCOPE_REQUIRED");
/// A thread the caller is not a member of, exactly as one that does not exist.
pub const THREAD_NOT_FOUND: Reason = Reason::new("THREAD_NOT_FOUND");
/// A message the caller cannot see, or that is not a message of the thread
/// named.
pub const MESSAGE_NOT_FOUND: Reason = Reason::new("MESSAGE_NOT_FOUND");

/// Whether the caller may use the human surface at all.
pub fn human_surface(principal: &ThreadsPrincipal) -> Gate {
    if principal.active_user().is_some() {
        Gate::allowed()
    } else {
        Gate::blocked(ACTIVE_USER_REQUIRED)
    }
}

/// Whether the caller may use the runner operations at all. Whether a title
/// operation names the thread's active title Job is the title Job's own
/// decision, on top of this one.
pub fn runner_surface(principal: &ThreadsPrincipal) -> Gate {
    if principal.service_account().is_some() && principal.holds_scope(scopes::RUNNER) {
        Gate::allowed()
    } else {
        Gate::blocked(RUNNER_SCOPE_REQUIRED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::principal::RosterStanding;
    use crate::principal::tests::{active, person, service};

    #[test]
    fn the_human_surface_admits_an_active_user_and_nobody_else() {
        assert_eq!(human_surface(&person(false, active())), Gate::allowed());
        for standing in [
            RosterStanding::Unknown,
            RosterStanding::OutOfRoster,
            RosterStanding::Deactivated,
        ] {
            assert_eq!(
                human_surface(&person(false, standing.clone())),
                Gate::blocked(ACTIVE_USER_REQUIRED),
                "{standing:?}"
            );
            assert_eq!(
                human_surface(&person(true, standing)),
                Gate::blocked(ACTIVE_USER_REQUIRED),
                "super-user status gives nothing"
            );
        }
        assert_eq!(
            human_surface(&service(&[scopes::RUNNER])),
            Gate::blocked(ACTIVE_USER_REQUIRED),
            "threads:runner grants nothing on the human surface"
        );
    }

    #[test]
    fn the_runner_surface_admits_a_service_account_holding_the_runner_scope() {
        assert_eq!(runner_surface(&service(&[scopes::RUNNER])), Gate::allowed());
        assert_eq!(
            runner_surface(&service(&[])),
            Gate::blocked(RUNNER_SCOPE_REQUIRED)
        );
        assert_eq!(
            runner_surface(&person(true, active())),
            Gate::blocked(RUNNER_SCOPE_REQUIRED),
            "a user, super user included, is not a runner"
        );
    }
}
