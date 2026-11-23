//! The messages of a thread: one list, in the order they were stored.
//!
//! Reactivity ledger of the messages view (`threadsMessages`, and the message
//! member of `threadsThreadDeltas`, a live head page of the list):
//!
//! | input | change cause | impact | delta |
//! |---|---|---|---|
//! | message row and content | store, close (writer or stream watch), edit, delete | resource, `message` noun, by key; the head page repopulates on the noun | Upsert; a new message enters the head page, the oldest leaves it |
//! | streamed content | a chunk folded | resource, `message_stream` noun, by key (no repopulation) | Upsert of the streaming message |
//! | the end of the list | `message.erase` | one resource impact, `message` noun, whatever the number of messages erased | the head page repopulates: Remove for each erased key it held (Reset past the churn threshold), Upsert for the older messages entering it |
//! | an author's name, standing | directory mirror | foreign `threads.known_user`, inverted to the user's messages | Upsert |
//! | the caller's membership, standing | membership changes, directory mirror | principal facts (`MEMBERSHIP_DEPS`, `ROSTER_DEPS`) | the head page repopulates |
//! | an artifact's label | its message attaches it | with the message's own impact | Upsert |

pub mod close;
pub mod closers;
pub mod closing;
pub mod closure;
pub mod commands;
pub mod content;
pub mod decision;
pub mod derived;
pub mod draft;
pub mod edit;
pub mod erase;
pub mod graphql;
pub mod retired;
pub mod state;
pub mod store;
pub mod stream;
pub mod transitions;
pub mod upgrade;
pub mod v1;
pub mod view;
pub mod watch;

use serde::Serialize;
use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::graphql::SliceFragment;
use uuid::Uuid;

use crate::literal::literal_enum;
use crate::principal::ThreadsPrincipal;

pub use state::{Message, MessageState};
pub use view::{MessageView, MessagesView};

literal_enum! {
    pub enum MessageCause {
        Stored => "STORED",
        Closed => "CLOSED",
        Edited => "EDITED",
        Deleted => "DELETED",
        Erased => "ERASED",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MessageImpact {
    pub kind: MessageCause,
    pub thread_id: Uuid,
}

pub fn message_cause(kind: MessageCause, thread_id: Uuid) -> MessageImpact {
    MessageImpact { kind, thread_id }
}

pub fn register(engine: &mut Engine<ThreadsPrincipal>) -> Result<(), EngineError> {
    engine.register_projector(MessagesView)?;
    engine.register_accumulator(stream::TurnContent)?;
    engine.register_accumulator(stream::LegacyContent)?;
    engine.register_reaction("threads-message-store-v3", commands::store_v3)?;
    engine.register_reaction("threads-message-store-v2", retired::store::<2>)?;
    engine.register_reaction("threads-message-store", retired::store::<1>)?;
    engine.register_reaction("threads-message-complete", closers::complete)?;
    engine.register_reaction("threads-message-complete-v2", closers::complete_v2)?;
    engine.register_reaction("threads-message-fail-v2", closers::fail_v2)?;
    engine.register_reaction("threads-message-fail", closers::fail)?;
    engine.register_reaction("threads-message-cancel", closers::cancel)?;
    engine.register_reaction("threads-message-edit", edit::edit)?;
    engine.register_reaction("threads-message-delete", commands::delete)?;
    engine.register_reaction("threads-message-erase", erase::erase)?;
    engine.register_reaction("threads-message-stream-watch", watch::due)?;
    engine.register_schema_slice(SliceFragment::derive::<
        graphql::MessageQuery,
        async_graphql::EmptyMutation,
        graphql::MessageSubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
