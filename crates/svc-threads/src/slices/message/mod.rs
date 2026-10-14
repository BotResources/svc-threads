//! The message capability: the messages of a thread, their items and blocks,
//! the accumulated lane a streaming message folds into, the message commands,
//! and the thread's live feed.
//!
//! - `state`: the canonical message, its visibility (the thread's membership)
//!   and its unsaved changes;
//! - `content`: a message's content judged by the library's rules, stored in
//!   Threads' decomposition, and read back into the library's model;
//! - `draft`: a stream's content as its chunks fold, and as it closes;
//! - `stream`: the `message_content` accumulator of `STREAMING_threads`;
//! - `transitions`: the guarded transition of each command, its facts;
//! - `commands`: the reactions to the message commands, each answered once;
//! - `closing`: closing a sealed stream, shared by the close commands and the
//!   watch;
//! - `watch`: the stream watch (idle writer, refused stream);
//! - `store`: the message's rows and its facts, soft EDA; branch paging;
//! - `decision`: one `gated!` decision per human gesture, which is also its
//!   affordance;
//! - `view`: a page of a branch of a thread, streamed content included;
//! - `graphql`: the roots, and the thread's live feed.
//!
//! Reactivity ledger of the messages view (`threadsMessages`, and the message
//! member of `threadsThreadDeltas`):
//!
//! | input | change cause | impact | delta |
//! |---|---|---|---|
//! | message row (status, deleted) | store, complete, fail, cancel, delete, stream watch | resource, `message` noun, caused | Upsert; the open head repopulates on a new message (it enters the page, the oldest leaves) |
//! | items, blocks | store (whole), close (folded content), delete (content gone) | resource, `message` noun | Upsert |
//! | a streaming message's content | a chunk folded on any pod (lane A) | resource, `message` noun, staged by the engine | Upsert with the content so far; `FAILED` as soon as a refused chunk folds |
//! | the thread's members | add/remove members, thread deleted | principal facts of each user added or removed, `MEMBERSHIP_DEPS` | the member's windows repopulated: Upsert / Remove |
//! | a user author's name | directory mirror | foreign `threads.known_user`, inverted to the messages they wrote | Upsert |
//! | the caller's standing | directory mirror | principal facts, `ROSTER_DEPS`; the resolver ends the sessions of a caller no longer active | windows repopulated, or sessions ended |
//!
//! An open head repopulates on every change of its noun, the accumulated lane's
//! folds included: one branch read per session and render pass while messages
//! stream anywhere. Measured cost, not correctness; a narrower interest would
//! need the engine to filter a repopulation by the impact.

pub mod closing;
pub mod commands;
pub mod content;
pub mod decision;
pub mod draft;
pub mod graphql;
pub mod state;
pub mod store;
pub mod stream;
pub mod transitions;
pub mod view;
pub mod watch;

use serde::Serialize;
use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::graphql::SliceFragment;
use uuid::Uuid;

use crate::principal::ThreadsPrincipal;

pub use state::{Message, MessageState};
pub use view::{MessageView, MessagesView};

/// What changed a message, as its delta's cause says: a small fact naming the
/// thread, never the content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageCause {
    Stored,
    Closed,
    Deleted,
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
    engine.register_accumulator(stream::MessageContent)?;
    engine.register_reaction("threads-message-store", commands::store)?;
    engine.register_reaction("threads-message-complete", commands::complete)?;
    engine.register_reaction("threads-message-fail", commands::fail)?;
    engine.register_reaction("threads-message-cancel", commands::cancel)?;
    engine.register_reaction("threads-message-delete", commands::delete)?;
    engine.register_reaction("threads-message-stream-watch", watch::due)?;
    engine.register_schema_slice(SliceFragment::derive::<
        graphql::MessageQuery,
        async_graphql::EmptyMutation,
        graphql::MessageSubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
