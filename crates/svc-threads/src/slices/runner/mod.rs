//! The runner capability: the GraphQL operations reserved to a service account
//! holding `threads:runner`, for any thread.
//!
//! - `context`: a thread's context — the agent context of the agent a runner
//!   names (the library's projected thread), or the whole active branch as a
//!   `br-llm-messages/5` conversation; plus its members and resources;
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
    engine.register_projector(context::ContextView)?;
    engine.register_schema_slice(SliceFragment::derive::<
        graphql::RunnerQuery,
        graphql::RunnerMutation,
        async_graphql::EmptySubscription,
    >(crate::slices::FRAGMENT))?;
    Ok(())
}
