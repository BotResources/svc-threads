//! The message capability: the messages of a thread, their items and blocks,
//! the accumulated lane a streaming message folds into, and the thread's live
//! feed.
//!
//! - `state`: the canonical message and its visibility (the thread's
//!   membership);
//! - `store`: the message's rows, soft EDA (read path, branch paging; the write
//!   path and the facts are the implementation pass's);
//! - `stream`: the `message_content` accumulator of `STREAMING_threads`;
//! - `decision`: one `gated!` decision per human gesture, which is also its
//!   affordance;
//! - `view`: a page of a branch of a thread;
//! - `graphql`: the roots, and the thread's live feed.
//!
//! Reactivity ledger of the messages view (`threadsMessages`, and the message
//! member of `threadsThreadDeltas`):
//!
//! | input | change cause | impact | delta |
//! |---|---|---|---|
//! | message row (status, deleted) | store, complete, fail, cancel, delete, stream window elapsed | resource, `message` noun | Upsert; the open head repopulates on a new message |
//! | items, blocks | store (whole), complete/fail/cancel (folded content), delete (content gone) | resource, `message` noun | Upsert |
//! | a streaming message's content | a chunk folded on any pod (lane A) | resource, `message` noun, staged by the engine | Upsert (once the view reads the folded state) |
//! | the thread's members | add/remove members, thread deleted | principal facts of each user added or removed, `MEMBERSHIP_DEPS` | the member's windows repopulated: Upsert / Remove |
//! | a user author's name | directory mirror | foreign `threads.known_user` (inverse: the implementation pass's) | Upsert |
//! | the caller's standing | directory mirror | principal facts, `ROSTER_DEPS`; the resolver ends the sessions of a caller no longer active | windows repopulated, or sessions ended |
//!
//! An open head repopulates on every change of its noun, and the accumulated
//! lane stages one per folded batch: the implementation pass weighs that cost
//! against a narrower interest when it moves the view to the low-level
//! projector.

pub mod decision;
pub mod graphql;
pub mod state;
pub mod store;
pub mod stream;
pub mod view;

use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::graphql::SliceFragment;

use crate::principal::ThreadsPrincipal;

pub use state::{Message, MessageState};
pub use view::{MessageView, MessagesView};

pub fn register(engine: &mut Engine<ThreadsPrincipal>) -> Result<(), EngineError> {
    engine.register_view(MessagesView)?;
    engine.register_accumulator(stream::MessageContent)?;
    engine.register_schema_slice(SliceFragment::derive::<
        graphql::MessageQuery,
        async_graphql::EmptyMutation,
        graphql::MessageSubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
