//! The runner surface's roots. Each asks the runner admission first: a user —
//! super user included — and a service account without `threads:runner` are
//! refused with its code.

use async_graphql::{Context, Object, Result};
use contract_threads::llm::AgentId;
use service_engine::gate::Reason;
use service_engine::session::WindowParams;
use service_engine::{OrInternal, Query};
use uuid::Uuid;

use super::context::{ContextView, ContextWindow, ThreadContext};
use crate::access;
use crate::principal::ThreadsPrincipal;

/// An agent id that is not a UUIDv7: the library's agent id is one.
pub const AI_PERSPECTIVE_INVALID: Reason = Reason::new("AI_PERSPECTIVE_INVALID");

#[derive(Default)]
pub struct RunnerQuery;

#[Object]
impl RunnerQuery {
    /// A thread's context: the agent context of the agent `agentId` names —
    /// the library's projected thread for it, built by Threads — or, naming
    /// none, the whole active branch as a `br-llm-messages/5` conversation;
    /// with the thread's members and resources. `null` for a thread that does
    /// not exist. An agent id that is not a UUIDv7 is refused
    /// `AI_PERSPECTIVE_INVALID`.
    async fn threads_context(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        agent_id: Option<String>,
    ) -> Result<Option<ThreadContext>> {
        access::runner_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let agent = match agent_id {
            Some(agent) => Some(
                AgentId::parse(&agent)
                    .map_err(|_| AI_PERSPECTIVE_INVALID)?
                    .as_uuid(),
            ),
            None => None,
        };
        let window = ContextWindow {
            thread: Some(thread_id),
            agent,
        };
        let mut contexts = Query::<ThreadsPrincipal>::new(ctx)?
            .fetch_window::<ContextView>(
                WindowParams::encode(&window).or_internal("encode the context window")?,
            )
            .await?;
        Ok(contexts.pop())
    }
}
