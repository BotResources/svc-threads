//! The one decision of each human gesture on a thread, which is also its
//! affordance in every thread view. A member who is an active user may set
//! their own read mark; anyone else meets the thread as one that does not
//! exist.
//!
//! The commands' decisions (create, members, resources, title, close, delete)
//! judge the coherence of the conversation, never rights; they are the
//! implementation pass's, beside the transitions they guard.

use service_engine::gate::Gate;

use super::state::ThreadState;
use crate::access::THREAD_NOT_FOUND;
use crate::principal::ThreadsPrincipal;

/// Setting one's own read mark on the thread.
pub fn set_read_mark(thread: &ThreadState, principal: &ThreadsPrincipal) -> Gate {
    match principal.active_user() {
        Some(user) if thread.is_member(user) => Gate::allowed(),
        _ => Gate::blocked(THREAD_NOT_FOUND),
    }
}

service_engine::gated! {
    ThreadState, ThreadsPrincipal;
    "setReadMark" => fn set_read_mark_gate(this, principal) {
        set_read_mark(this, principal)
    }
}

#[cfg(test)]
mod tests {
    use service_engine::gate::Gated;

    use super::*;
    use crate::principal::RosterStanding;
    use crate::principal::tests::{active, person};
    use crate::slices::thread::state::tests::thread_with;

    #[test]
    fn a_member_sets_their_read_mark_and_anyone_else_meets_a_missing_thread() {
        let member = person(false, active());
        let thread = thread_with(&[member.user().unwrap()]);
        assert_eq!(set_read_mark(&thread, &member), Gate::allowed());
        assert_eq!(
            set_read_mark(&thread, &person(true, active())),
            Gate::blocked(THREAD_NOT_FOUND)
        );
        let gone = person(false, RosterStanding::OutOfRoster);
        let theirs = thread_with(&[gone.user().unwrap()]);
        assert_eq!(
            set_read_mark(&theirs, &gone),
            Gate::blocked(THREAD_NOT_FOUND)
        );
    }

    #[test]
    fn the_affordance_is_the_decision() {
        let member = person(false, active());
        let thread = thread_with(&[member.user().unwrap()]);
        let affordances = thread.affordances(&member);
        assert_eq!(
            affordances.get(service_engine::gate::ActionName::from_static("setReadMark")),
            Some(set_read_mark(&thread, &member))
        );
    }
}
