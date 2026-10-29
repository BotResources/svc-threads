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
pub mod graphql;
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
    engine.register_reaction("threads-message-store-v2", commands::store_v2)?;
    engine.register_reaction("threads-message-store", commands::store_v1)?;
    engine.register_reaction("threads-message-complete", closers::complete)?;
    engine.register_reaction("threads-message-complete-v2", closers::complete_v2)?;
    engine.register_reaction("threads-message-fail-v2", closers::fail_v2)?;
    engine.register_reaction("threads-message-fail", closers::fail)?;
    engine.register_reaction("threads-message-cancel", closers::cancel)?;
    engine.register_reaction("threads-message-edit", edit::edit)?;
    engine.register_reaction("threads-message-delete", commands::delete)?;
    engine.register_reaction("threads-message-stream-watch", watch::due)?;
    engine.register_schema_slice(SliceFragment::derive::<
        graphql::MessageQuery,
        async_graphql::EmptyMutation,
        graphql::MessageSubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
