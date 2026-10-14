//! The runner capability: the GraphQL operations reserved to a service account
//! holding `threads:runner`, for any thread.
//!
//! - Reading a thread's context: its active branch as a conversation of the
//!   `br-llm-messages/2` model — without its deleted messages, its cancelled
//!   ones and those failed before their first step, without Threads blocks,
//!   every message carrying its author — whole, or lightened by the library's
//!   rule for the AI the runner answers for; plus its members and resources.
//! - The title Job's operations: when a thread receives its first user message
//!   and has no title, Threads asks Jobs to run one Job of the configured title
//!   runner type ([`contract_threads::TitleJobConfig`]); the runner sets the
//!   title, or declares that it failed, through operations accepted only for
//!   the thread's active title Job, and Threads then declares the Job finished,
//!   or failed. A title set by command while the Job runs wins (Threads cancels
//!   the Job); deleting a thread cancels its running Job.
//!
//! The artifact upload request, the third runner operation, belongs to the
//! artifact capability. The scaffold holds the roots and the runner admission;
//! the context read, the title Job's lifecycle, the Jobs commands and the
//! reactions to Jobs' events are the implementation pass's.

pub mod graphql;

use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::graphql::SliceFragment;

use crate::principal::ThreadsPrincipal;

pub fn register(engine: &mut Engine<ThreadsPrincipal>) -> Result<(), EngineError> {
    engine.register_schema_slice(SliceFragment::derive::<
        graphql::RunnerQuery,
        graphql::RunnerMutation,
        async_graphql::EmptySubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
