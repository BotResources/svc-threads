//! The runner surface's roots. Each asks the runner admission first: a user —
//! super user included — and a service account without `threads:runner` are
//! refused with its code.

use async_graphql::{Context, InputObject, Object, Result, SimpleObject};
use service_engine::{JsonScalar, MutationAck};
use uuid::Uuid;

use crate::access;
use crate::fault::not_implemented;
use crate::principal::ThreadsPrincipal;
use crate::slices::thread::view::{MemberView, ResourceView};

/// The AI a runner answers for: the context is then lightened for it by the
/// library's rule, so that its render is the same as from the whole one.
#[derive(Debug, Clone, InputObject)]
pub struct AiPerspective {
    pub id: String,
    pub name: String,
}

/// What a runner reads of a thread.
#[derive(Debug, Clone, SimpleObject)]
pub struct ThreadContext {
    pub thread_id: Uuid,
    /// The active branch as a `br-llm-messages/2` conversation (its JSON).
    pub conversation: JsonScalar,
    pub members: Vec<MemberView>,
    pub resources: Vec<ResourceView>,
}

#[derive(Default)]
pub struct RunnerQuery;

#[Object]
impl RunnerQuery {
    /// A thread's context, whole or lightened for the AI named; `null` for a
    /// thread that does not exist.
    async fn threads_context(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        ai: Option<AiPerspective>,
    ) -> Result<Option<ThreadContext>> {
        access::runner_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let _ = (thread_id, ai);
        Err(not_implemented("threadsContext"))
    }
}

#[derive(Default)]
pub struct RunnerMutation;

#[Object]
impl RunnerMutation {
    /// Sets the title of a thread for its active title Job (at most 200
    /// characters, `TITLE_TOO_LONG`); Threads then declares the Job finished.
    async fn threads_set_title(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        job_id: Uuid,
        title: String,
    ) -> Result<MutationAck> {
        access::runner_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let _ = (thread_id, job_id, title);
        Err(not_implemented("threadsSetTitle"))
    }

    /// Declares the thread's active title Job failed, with a reason code
    /// (`[a-z][a-z0-9_]*`) and an optional message; the thread stays untitled
    /// for good and Threads declares the Job failed.
    async fn threads_fail_title_job(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        job_id: Uuid,
        reason_code: String,
        message: Option<String>,
    ) -> Result<MutationAck> {
        access::runner_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let _ = (thread_id, job_id, reason_code, message);
        Err(not_implemented("threadsFailTitleJob"))
    }
}
