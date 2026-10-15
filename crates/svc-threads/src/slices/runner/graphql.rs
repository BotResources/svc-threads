//! The runner surface's roots. Each asks the runner admission first: a user —
//! super user included — and a service account without `threads:runner` are
//! refused with its code.

use async_graphql::{Context, InputObject, Object, Result, SimpleObject};
use contract_threads::llm::Author;
use service_engine::gate::Reason;
use service_engine::{JsonScalar, MutationAck, Query};
use uuid::Uuid;

use super::context::{self, RunnerThreads};
use crate::access;
use crate::principal::ThreadsPrincipal;
use crate::slices::thread::title_job::{RunnerFailTitleJob, RunnerSetTitle};
use crate::slices::thread::view::{MemberView, ResourceView};

/// An AI perspective whose id or name is blank.
pub const AI_PERSPECTIVE_INVALID: Reason = Reason::new("AI_PERSPECTIVE_INVALID");

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
        let ai = match ai {
            Some(ai) => Some(Author::new(ai.id, ai.name).map_err(|_| AI_PERSPECTIVE_INVALID)?),
            None => None,
        };
        Query::<ThreadsPrincipal>::new(ctx)?
            .behind::<RunnerThreads>(&thread_id)
            .read(|thread, conn| {
                Box::pin(async move {
                    let conversation = context::read(conn, thread.id, ai).await?;
                    Ok(ThreadContext {
                        thread_id: thread.id,
                        conversation: async_graphql::Json(conversation),
                        members: thread
                            .members
                            .iter()
                            .map(|(user_id, member)| MemberView {
                                user_id: *user_id,
                                display_name: member.display_name.clone(),
                                in_roster: member.in_roster,
                            })
                            .collect(),
                        resources: thread
                            .resources
                            .iter()
                            .map(|(key, ids)| ResourceView {
                                key: key.clone(),
                                ids: ids.clone(),
                            })
                            .collect(),
                    })
                })
            })
            .await
    }
}

#[derive(Default)]
pub struct RunnerMutation;

#[Object]
impl RunnerMutation {
    /// Sets the title of a thread for its active title Job (1 to 200
    /// characters, `TITLE_EMPTY`, `TITLE_TOO_LONG`); Threads then declares the
    /// Job finished.
    /// A Job that is not the thread's active title Job is refused
    /// `JOB_NOT_ACTIVE`.
    async fn threads_set_title(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        job_id: Uuid,
        title: String,
    ) -> Result<MutationAck> {
        access::runner_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        service_engine::ack::<ThreadsPrincipal, RunnerSetTitle>(
            ctx,
            RunnerSetTitle {
                thread_id,
                job_id,
                title,
            },
        )
        .await
    }

    /// Declares the thread's active title Job failed, with a reason code
    /// (`[a-z][a-z0-9_]*`) and an optional message; the thread stays untitled
    /// for good and Threads declares the Job failed. A Job that is not the
    /// thread's active title Job is refused `JOB_NOT_ACTIVE`.
    async fn threads_fail_title_job(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        job_id: Uuid,
        reason_code: String,
        message: Option<String>,
    ) -> Result<MutationAck> {
        access::runner_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        service_engine::ack::<ThreadsPrincipal, RunnerFailTitleJob>(
            ctx,
            RunnerFailTitleJob {
                thread_id,
                job_id,
                reason_code,
                message,
            },
        )
        .await
    }
}
