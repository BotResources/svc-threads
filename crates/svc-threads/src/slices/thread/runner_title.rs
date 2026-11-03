//! A runner sets a thread's title (`threadsSetTitle`), for any thread: a
//! default title, or the title of a thread it created. The second ingress of
//! the `thread.set_title` command: the same transition decides it
//! ([`ThreadState::set_title`] — `TITLE_EMPTY`, `TITLE_TOO_LONG`, the same title
//! changes nothing), the same save writes it and stages the same impact. Only
//! the attribution differs: the `TitleChanged` fact says `by: RUNNER`, its actor
//! is the runner's service account.

use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::{Mutation, MutationInput};
use uuid::Uuid;

use super::ThreadCause;
use super::commands::save_changed;
use super::state::ThreadState;
use super::transitions::{Applied, TitleSource};
use crate::access;
use crate::facts::FactMeta;
use crate::fault::Fault;
use crate::principal::ThreadsPrincipal;

#[derive(Debug, Deserialize)]
pub struct RunnerSetTitle {
    pub thread_id: Uuid,
    pub title: String,
}

impl MutationInput for RunnerSetTitle {
    type Output = ();
    type Error = Fault;
    const NAME: &'static str = "runner_set_title";
}

/// The thread is loaded under the engine's per-key lock, as a command's is, so
/// a runner's title and a concurrent command on the thread are judged one after
/// the other on committed state.
pub fn runner_set_title<'m>(
    cx: &'m mut Mutation<'m, ThreadsPrincipal>,
    input: RunnerSetTitle,
) -> BoxFuture<'m, Result<(), Fault>> {
    Box::pin(async move {
        access::runner_surface(cx.principal()).require()?;
        let mut thread = cx
            .load::<ThreadState>(&input.thread_id)
            .await?
            .ok_or(access::THREAD_NOT_FOUND)?;
        let meta = FactMeta::of_principal(cx.principal(), cx.now().as_datetime());
        match thread.set_title(&input.title, TitleSource::Runner, &meta)? {
            Ok(Applied::Changed) => save_changed(cx, &thread, ThreadCause::TitleChanged, &[]).await,
            Ok(Applied::Unchanged) => Ok(()),
            Err(refusal) => Err(Fault::Refused(refusal.reason())),
        }
    })
}
