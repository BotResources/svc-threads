//! The runner capability: the GraphQL operations reserved to a service account
//! holding `threads:runner`, for any thread.
//!
//! - `context`: a thread's context — its active branch as a conversation of the
//!   `br-llm-messages/2` model, whole or lightened for the AI the runner answers
//!   for, every message carrying its author; plus its members and resources;
//! - `graphql`: the roots, the title Job's operations included (their decision
//!   and their effects are the title Job's, in the thread capability).
//!
//! The artifact upload request, the third runner operation, belongs to the
//! artifact capability.

pub mod context;
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
