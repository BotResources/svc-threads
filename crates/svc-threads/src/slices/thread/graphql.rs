//! The thread roots of the human surface: the caller's threads, the children of
//! a thread, one thread, the caller's read mark, and the live list. Every root
//! asks the human admission first, so a caller Threads does not admit is
//! refused with its code before any read; a thread the caller is not a member
//! of is `null`, exactly as one that does not exist.

use async_graphql::{Context, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::session::WindowSpec;
use service_engine::{MutationAck, OrInternal, Page, Query, WindowSize};
use uuid::Uuid;

use super::read_mark::SetReadMark;
use super::view::{ThreadView, ThreadsView, ThreadsWindow};
use crate::access;
use crate::principal::ThreadsPrincipal;

service_engine::subscription_union! {
    view = ThreadsViewUnion;
    delta = ThreadsDelta { reset = ThreadsReset, upsert = ThreadsUpsert, remove = ThreadsRemove };
    Thread => service_engine::view::ViewProjector<ThreadsView> => ThreadView,
}

#[derive(Default)]
pub struct ThreadQuery;

#[Object]
impl ThreadQuery {
    /// One page of the threads the caller is a member of, the most recently
    /// active first; `before` names the last thread of the previous page.
    async fn threads_threads(
        &self,
        ctx: &Context<'_>,
        size: WindowSize,
        before: Option<Uuid>,
    ) -> Result<Vec<ThreadView>> {
        access::human_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let mut threads = Query::<ThreadsPrincipal>::new(ctx)?
            .fetch_view_window::<ThreadsView>(&ThreadsWindow::page(Page::new(before, size)))
            .await?;
        // A head page is a set of keys (see `view::ThreadsFeed`): the most
        // recently active first.
        threads.sort_by_key(|thread| std::cmp::Reverse((thread.last_activity_at, thread.id)));
        Ok(threads)
    }

    /// One page of the child threads of a thread that the caller is a member
    /// of, the oldest first (creation, then id); `after` names the last child
    /// of the previous page. A child the caller is not a member of is left out;
    /// the list still works once the parent is deleted, and is empty for a
    /// thread that does not exist or has no child the caller sees. An `after`
    /// that is not one of these children gives an empty page.
    async fn threads_child_threads(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        size: WindowSize,
        after: Option<Uuid>,
    ) -> Result<Vec<ThreadView>> {
        access::human_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let mut children = Query::<ThreadsPrincipal>::new(ctx)?
            .fetch_view_window::<ThreadsView>(&ThreadsWindow::children(
                thread_id,
                Page::new(after, size),
            ))
            .await?;
        // A children page is a set of keys (see `ThreadsView::populate`).
        children.sort_by_key(|child| (child.created_at, child.id));
        Ok(children)
    }

    /// One thread, or `null` when the caller is not a member of it — exactly as
    /// for an id nobody created.
    async fn threads_thread(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<ThreadView>> {
        access::human_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        Query::<ThreadsPrincipal>::new(ctx)?
            .fetch_view::<ThreadsView>(&id)
            .await
    }
}

#[derive(Default)]
pub struct ThreadMutation;

#[Object]
impl ThreadMutation {
    /// Sets the caller's own read mark on a thread they are a member of, at one
    /// of its messages. The reader's own state: no fact, no thread version. A
    /// thread the caller is not a member of is refused `THREAD_NOT_FOUND`; a
    /// message that is not one of its, `MESSAGE_NOT_FOUND`.
    async fn threads_set_read_mark(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        message_id: Uuid,
    ) -> Result<MutationAck> {
        access::human_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        service_engine::ack::<ThreadsPrincipal, SetReadMark>(
            ctx,
            SetReadMark {
                thread_id,
                message_id,
            },
        )
        .await
    }
}

#[derive(Default)]
pub struct ThreadSubscription;

#[Subscription]
impl ThreadSubscription {
    /// The caller's list, live: a `ThreadsReset` snapshot of the page, then
    /// `ThreadsUpsert` / `ThreadsRemove` deltas on a contiguous revision. The
    /// head page (no `before`) is open: a thread that becomes the most recently
    /// active enters it and the oldest leaves.
    async fn threads_threads_deltas(
        &self,
        ctx: &Context<'_>,
        size: WindowSize,
        before: Option<Uuid>,
    ) -> Result<impl Stream<Item = Result<ThreadsDelta>>> {
        access::human_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
        let window = ThreadsWindow::page(Page::new(before, size));
        let stream = service_engine::attach::<ThreadsPrincipal>(
            ctx,
            vec![
                WindowSpec::view::<ThreadsView>(&window, false)
                    .or_internal("encode the threads window")?,
            ],
        )
        .await?;
        Ok(stream.map(|delta| ThreadsDelta::from_delta(&delta)))
    }
}
