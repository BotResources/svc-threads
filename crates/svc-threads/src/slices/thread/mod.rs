//! The thread capability: a thread, its members, resources, read marks and
//! title Job; the thread commands; the thread views; the members KV offer.
//!
//! - `state`: the canonical thread, its visibility (membership) and its unsaved
//!   changes;
//! - `transitions`: the guarded transition of each command, which is its one
//!   decision, and the facts it records;
//! - `title_job`: the title Job with Jobs, its runner's operations and Jobs'
//!   events;
//! - `commands`: the reactions to the thread commands, each answered once;
//! - `resolution`: resolving and reopening a thread, and the gate it puts on
//!   the message commands;
//! - `departure`: the threads of an owner who left the roster;
//! - `read_mark`: a member's own read mark;
//! - `store`: the thread's rows and its facts, soft EDA;
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
//! | thread row (title, status, last activity, origin) | set title (command or title Job), close, a message stored | resource, `thread` noun | Upsert; the open head repopulates on activity |
//! | resolution (resolved by, resolved at) | resolve, reopen | resource, `thread` noun | Upsert |
//! | title Job | started, ended by its runner or by Jobs, cancelled | resource, `thread` noun | none shown (no field) — re-render, no delta |
//! | members | create, add/remove members | resource, `thread` noun + principal facts of each user added or removed (`MEMBERSHIP_DEPS`) | Upsert; Remove for a removed member (visibility lost); Upsert for an added one (visibility gained) |
//! | resources | add/remove resource ids | resource, `thread` noun | Upsert |
//! | read marks | the member's own gesture | resource, `thread_reader` noun (re-render by key, no list repopulation) | Upsert to the reader (only their own mark shows); no delta to the others |
//! | read marks | member removal | with the membership change | as members |
//! | thread deleted | delete command, owner left the roster | resource, `thread` noun + principal facts of every member | Remove |
//! | a member's name, standing | directory mirror | foreign `threads.known_user`, inverted to the user's threads | Upsert |
//! | the caller's standing | directory mirror | principal facts, `ROSTER_DEPS`; the resolver ends the sessions of a caller no longer active | windows repopulated, or sessions ended |
//!
//! Snapshot-to-stream ordering, lag and multi-pod fan-out are the engine's
//! attach protocol (sessions registered before their window is populated,
//! impacts held and replayed, a lagging session reset).

pub mod commands;
pub mod decision;
pub mod departure;
pub mod graphql;
pub mod offer;
pub mod read_mark;
pub mod resolution;
pub mod state;
pub mod store;
pub mod title_job;
pub mod transitions;
pub mod view;

use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::graphql::SliceFragment;

use crate::literal::literal_enum;
use crate::principal::ThreadsPrincipal;

pub use state::{Thread, ThreadState};
pub use view::{ThreadView, ThreadsView};

literal_enum! {
    /// What changed a thread, as its delta's cause says. A small fact, never
    /// the content.
    pub enum ThreadCause {
        Created => "CREATED",
        MembersChanged => "MEMBERS_CHANGED",
        ResourcesChanged => "RESOURCES_CHANGED",
        TitleChanged => "TITLE_CHANGED",
        TitleJobEnded => "TITLE_JOB_ENDED",
        Closed => "CLOSED",
        Resolved => "RESOLVED",
        Reopened => "REOPENED",
        Deleted => "DELETED",
        ReadMarkSet => "READ_MARK_SET",
        MessageStored => "MESSAGE_STORED",
    }
}

pub fn register(engine: &mut Engine<ThreadsPrincipal>) -> Result<(), EngineError> {
    engine.register_projector(view::ThreadsFeed::default())?;
    engine.register_offer::<offer::MembersOffer>()?;

    engine.register_reaction("threads-thread-create", commands::create)?;
    engine.register_reaction("threads-thread-add-members", commands::add_members)?;
    engine.register_reaction("threads-thread-remove-members", commands::remove_members)?;
    engine.register_reaction(
        "threads-thread-add-resource-ids",
        commands::add_resource_ids,
    )?;
    engine.register_reaction(
        "threads-thread-remove-resource-ids",
        commands::remove_resource_ids,
    )?;
    engine.register_reaction("threads-thread-set-title", commands::set_title)?;
    engine.register_reaction("threads-thread-close", commands::close)?;
    engine.register_reaction("threads-thread-resolve", commands::resolve)?;
    engine.register_reaction("threads-thread-reopen", commands::reopen)?;
    engine.register_reaction("threads-thread-delete", commands::delete)?;

    engine.register_reaction(
        "threads-title-job-rejected",
        title_job::on_creation_rejected,
    )?;
    engine.register_reaction("threads-title-job-failed", title_job::on_failed)?;
    engine.register_reaction("threads-title-job-cancelled", title_job::on_cancelled)?;
    engine.register_mutation::<title_job::RunnerSetTitle, _>(title_job::runner_set_title)?;
    engine
        .register_mutation::<title_job::RunnerFailTitleJob, _>(title_job::runner_fail_title_job)?;

    engine.register_mutation::<read_mark::SetReadMark, _>(read_mark::set_read_mark)?;

    engine.register_mutation::<departure::DeleteDepartedOwnersThread, _>(
        departure::delete_departed_owners_thread,
    )?;
    // The executor sees the mutations registered so far: taken after the sweep's.
    engine.register_cron(departure::DepartedOwners::new(engine.mutation_executor()))?;

    engine.register_schema_slice(SliceFragment::derive::<
        graphql::ThreadQuery,
        graphql::ThreadMutation,
        graphql::ThreadSubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
