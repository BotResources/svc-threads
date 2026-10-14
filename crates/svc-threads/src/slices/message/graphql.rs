//! The message roots of the human surface: a thread's messages page by page,
//! and the thread's live feed (the thread itself and the head page of its
//! active branch). A thread the caller is not a member of is refused
//! `THREAD_NOT_FOUND`, exactly as one that does not exist.

use async_graphql::{Context, Object, Result, Subscription};
use futures_util::Stream;
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::{OrInternal, Page, Query, WindowSize};
use uuid::Uuid;

use super::view::{MessageView, MessagesView, MessagesWindow};
use crate::access::{self, THREAD_NOT_FOUND};
use crate::principal::ThreadsPrincipal;
use crate::slices::thread::view::ThreadsWindow;
use crate::slices::thread::{ThreadView, ThreadsView};

service_engine::subscription_union! {
    view = ThreadFeedView;
    delta = ThreadFeedDelta { reset = ThreadFeedReset, upsert = ThreadFeedUpsert, remove = ThreadFeedRemove };
    Thread => service_engine::view::ViewProjector<ThreadsView> => ThreadView,
    Message => MessagesView => MessageView,
}

/// The human admission, then the thread's visibility: what every message root
/// asks before it reads.
async fn admit_to_thread(ctx: &Context<'_>, thread_id: Uuid) -> Result<()> {
    access::human_surface(ctx.data::<ThreadsPrincipal>()?).require()?;
    let seen = Query::<ThreadsPrincipal>::new(ctx)?
        .fetch_view::<ThreadsView>(&thread_id)
        .await?;
    match seen {
        Some(_) => Ok(()),
        None => Err(THREAD_NOT_FOUND.into()),
    }
}

#[derive(Default)]
pub struct MessageQuery;

#[Object]
impl MessageQuery {
    /// One page of a branch of a thread, the newest message first: the active
    /// branch (ending at the thread's most recent message) unless `branchTip`
    /// names the message another branch ends at; `before` names the oldest
    /// message of the previous page.
    async fn threads_messages(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        size: WindowSize,
        before: Option<Uuid>,
        branch_tip: Option<Uuid>,
    ) -> Result<Vec<MessageView>> {
        admit_to_thread(ctx, thread_id).await?;
        let window = MessagesWindow::of(thread_id, branch_tip, Page::new(before, size));
        Query::<ThreadsPrincipal>::new(ctx)?
            .fetch_window::<MessagesView>(
                WindowParams::encode(&window).or_internal("encode the messages window")?,
            )
            .await
    }
}

#[derive(Default)]
pub struct MessageSubscription;

#[Subscription]
impl MessageSubscription {
    /// Follows a thread live: a `ThreadFeedReset` snapshot of the thread and of
    /// the head page of its active branch, then `ThreadFeedUpsert` /
    /// `ThreadFeedRemove` deltas on a contiguous revision — a streaming
    /// message's content as it grows, a new message entering the page.
    async fn threads_thread_deltas(
        &self,
        ctx: &Context<'_>,
        thread_id: Uuid,
        size: WindowSize,
    ) -> Result<impl Stream<Item = Result<ThreadFeedDelta>>> {
        admit_to_thread(ctx, thread_id).await?;
        let stream = service_engine::attach::<ThreadsPrincipal>(
            ctx,
            vec![
                WindowSpec::view::<ThreadsView>(&ThreadsWindow::one(thread_id), false)
                    .or_internal("encode the thread window")?,
                WindowSpec::new(
                    MessagesView::NAME,
                    WindowParams::encode(&MessagesWindow::of(thread_id, None, Page::head(size)))
                        .or_internal("encode the messages window")?,
                    false,
                ),
            ],
        )
        .await?;
        let notices = service_engine::graphql::lane_notice_stream::<ThreadsPrincipal>(ctx)?;
        Ok(ThreadFeedDelta::subscribe(stream, notices))
    }
}
