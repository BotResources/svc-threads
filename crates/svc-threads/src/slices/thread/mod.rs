//! The thread capability: a thread, its members, resources, read marks and
//! title Job; the thread views; the members KV offer.
//!
//! - `state`: the canonical thread and its visibility (membership);
//! - `store`: the thread's rows, soft EDA (read path; the write path and the
//!   facts are the implementation pass's);
//! - `decision`: one `gated!` decision per human gesture, which is also its
//!   affordance;
//! - `view`: the caller's threads, for the queries, the snapshot and the live
//!   deltas;
//! - `offer`: `threads.members.{thread_id}`;
//! - `graphql`: the roots.
//!
//! Reactivity ledger of the threads view (`threadsThreads`, `threadsThread`,
//! `threadsThreadsDeltas`, and the thread member of `threadsThreadDeltas`):
//!
//! | input | change cause | impact | delta |
//! |---|---|---|---|
//! | thread row (title, status, last activity, origin) | set title, close, title Job ended, a message stored | resource, `thread` noun | Upsert; the open head repopulates on activity |
//! | members | create, add/remove members, owner left the roster (delete) | resource, `thread` noun | Upsert; Remove for a removed member (visibility lost); Upsert for an added one (visibility gained) |
//! | resources | add/remove resource ids | resource, `thread` noun | Upsert |
//! | read marks | the member's own gesture, member removal | resource, `thread` noun | Upsert (only the reader's own mark shows) |
//! | thread deleted | delete command, owner left the roster | resource, `thread` noun | Remove |
//! | a member's name, standing | directory mirror | foreign `threads.known_user`, inverted to the user's threads | Upsert |
//! | the caller's standing | directory mirror | principal facts, `ROSTER_DEPS`; the resolver ends the sessions of a caller no longer active | windows repopulated, or sessions ended |
//!
//! Snapshot-to-stream ordering, lag and multi-pod fan-out are the engine's
//! attach protocol (sessions registered before their window is populated,
//! impacts held and replayed, a lagging session reset).

pub mod decision;
pub mod graphql;
pub mod offer;
pub mod state;
pub mod store;
pub mod view;

use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::graphql::SliceFragment;

use crate::principal::ThreadsPrincipal;

pub use state::{Thread, ThreadState};
pub use view::{ThreadView, ThreadsView};

pub fn register(engine: &mut Engine<ThreadsPrincipal>) -> Result<(), EngineError> {
    engine.register_view(ThreadsView)?;
    engine.register_offer::<offer::MembersOffer>()?;
    engine.register_schema_slice(SliceFragment::derive::<
        graphql::ThreadQuery,
        graphql::ThreadMutation,
        graphql::ThreadSubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
