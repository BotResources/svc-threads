//! The one decision of each human gesture on a message, which is also its
//! affordance in every message view: a member of the thread who is an active
//! user may mark it as the last one they read. The message commands judge the
//! coherence of the conversation and are the implementation pass's.

use service_engine::gate::Gate;

use super::state::MessageState;
use crate::access::MESSAGE_NOT_FOUND;
use crate::principal::ThreadsPrincipal;

/// Setting one's read mark at this message.
pub fn mark_read(message: &MessageState, principal: &ThreadsPrincipal) -> Gate {
    match principal.active_user() {
        Some(user) if message.members.contains(&user) => Gate::allowed(),
        _ => Gate::blocked(MESSAGE_NOT_FOUND),
    }
}

service_engine::gated! {
    MessageState, ThreadsPrincipal;
    "markRead" => fn mark_read_gate(this, principal) {
        mark_read(this, principal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::principal::tests::{active, person};
    use crate::slices::message::state::tests::message_in;

    #[test]
    fn a_member_marks_a_message_read_and_anyone_else_meets_a_missing_one() {
        let member = person(false, active());
        let message = message_in(&[member.user().unwrap()]);
        assert_eq!(mark_read(&message, &member), Gate::allowed());
        assert_eq!(
            mark_read(&message, &person(false, active())),
            Gate::blocked(MESSAGE_NOT_FOUND)
        );
    }
}
