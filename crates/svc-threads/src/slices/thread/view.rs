//! The caller's threads, for the queries, the snapshot and the live deltas: one
//! projection, [`ThreadsView`], whatever the entry point.
//!
//! A window is either one thread (`threadsThread`, the live feed of one thread)
//! or one page of the caller's list, the most recently active first
//! (`threadsThreads`, `threadsThreadsDeltas`). Visibility is membership
//! ([`super::state::Thread`]): a thread the caller is not a member of is
//! invisible, exactly as one that does not exist.

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use contract_threads::ThreadStatus;
use serde::{Deserialize, Serialize};
use service_engine::Page;
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::impact::ForeignKey;
use service_engine::name::ProjectorName;
use service_engine::population::{Inverse, Population};
use service_engine::view::{Populate, Projector};
use uuid::Uuid;

use super::state::{Thread, ThreadState};
use super::store::{self, ThreadStore};
use crate::directory::KNOWN_USER_NAMESPACE;
use crate::principal::ThreadsPrincipal;

/// A thread's status, as GraphQL shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[graphql(name = "ThreadStatus")]
pub enum ThreadStatusView {
    Open,
    Completed,
    Failed,
    Cancelled,
}

impl From<ThreadStatus> for ThreadStatusView {
    fn from(status: ThreadStatus) -> Self {
        match status {
            ThreadStatus::Open => Self::Open,
            ThreadStatus::Completed => Self::Completed,
            ThreadStatus::Failed => Self::Failed,
            ThreadStatus::Cancelled => Self::Cancelled,
        }
    }
}

/// A member, under the name the roster copy holds — never a name copied
/// anywhere, so a change of name shows at once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct MemberView {
    pub user_id: Uuid,
    /// `null` for a user the roster copy never held.
    pub display_name: Option<String>,
    /// False once the user left the roster; they stay listed.
    pub in_roster: bool,
}

/// One named list of ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ResourceView {
    pub key: String,
    pub ids: Vec<String>,
}

/// One thread as one of its members sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ThreadView {
    pub id: Uuid,
    pub owner_id: Option<Uuid>,
    pub title: Option<String>,
    pub parent_thread_id: Option<Uuid>,
    pub origin_block_id: Option<Uuid>,
    pub status: ThreadStatusView,
    pub created_at: DateTime<Utc>,
    /// The date of the latest message, or the creation date while there is
    /// none: the order of the list.
    pub last_activity_at: DateTime<Utc>,
    pub version: i64,
    pub members: Vec<MemberView>,
    pub resources: Vec<ResourceView>,
    /// The caller's own read mark: the last message they read, if any.
    pub read_mark: Option<Uuid>,
    /// `{ setReadMark: { allowed, reason? } }`.
    pub affordances: Affordances,
}

/// What a window of [`ThreadsView`] is: one thread, or one page of the
/// caller's list.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThreadsWindow {
    thread: Option<Uuid>,
    page: Option<Page<Uuid>>,
}

impl ThreadsWindow {
    pub fn one(thread: Uuid) -> Self {
        Self {
            thread: Some(thread),
            page: None,
        }
    }

    pub fn page(page: Page<Uuid>) -> Self {
        Self {
            thread: None,
            page: Some(page),
        }
    }
}

#[derive(Default)]
pub struct ThreadsView;

impl ThreadsView {
    pub const NAME: ProjectorName = ProjectorName::from_static("threads");
}

impl Projector for ThreadsView {
    type Principal = ThreadsPrincipal;
    type Noun = Thread;
    type Store = ThreadStore;
    type Query = ThreadsWindow;
    type Out = ThreadView;
    type Visibility = Thread;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, ThreadsPrincipal>,
        query: &ThreadsWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        match (query.thread, &query.page, cx.principal().active_user()) {
            // One thread: its key, seen or not — the visibility gate decides,
            // and a thread the caller joins later enters the window then.
            (Some(thread), _, _) => Ok(Population::Keys(BTreeSet::from([thread]))),
            (None, Some(page), Some(member)) => {
                let keys = store::page_of_member(
                    cx.pool(),
                    member,
                    page.cursor().copied(),
                    cx.limit(page),
                )
                .await?;
                Ok(page.population(keys))
            }
            _ => Ok(Population::Keys(BTreeSet::new())),
        }
    }

    fn project(
        thread: &ThreadState,
        principal: &ThreadsPrincipal,
    ) -> Result<ThreadView, EngineError> {
        Ok(ThreadView {
            id: thread.id,
            owner_id: thread.owner_id,
            title: thread.title.as_ref().map(|title| title.as_str().to_owned()),
            parent_thread_id: thread.origin.map(|origin| origin.thread_id),
            origin_block_id: thread.origin.map(|origin| origin.block_id),
            status: thread.status.into(),
            created_at: thread.created_at,
            last_activity_at: thread.last_activity_at,
            version: thread.version,
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
            read_mark: principal
                .active_user()
                .and_then(|user| thread.read_marks.get(&user).copied()),
            affordances: thread.affordances(principal),
        })
    }

    /// A change of a known user's row (their name, their standing) re-renders
    /// the threads they are a member of.
    fn inverse(foreign: &ForeignKey) -> Inverse<Uuid> {
        if foreign.namespace().as_str() != KNOWN_USER_NAMESPACE {
            return Inverse::None;
        }
        Inverse::Lookup(Arc::new(|conn, foreign: &ForeignKey| {
            let user = Uuid::parse_str(foreign.key().as_str());
            Box::pin(async move {
                let Ok(user) = user else {
                    return Ok(BTreeSet::new());
                };
                let threads: Vec<Uuid> = sqlx::query_scalar(
                    "SELECT thread_id FROM threads.thread_member WHERE user_id = $1",
                )
                .bind(user)
                .fetch_all(conn)
                .await?;
                Ok(threads.into_iter().collect())
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::principal::tests::{active, person};
    use crate::slices::thread::state::tests::thread_with;

    #[test]
    fn a_member_sees_the_thread_with_their_own_read_mark_and_affordance() {
        let reader = person(false, active());
        let other = person(false, active());
        let (me, them) = (reader.user().unwrap(), other.user().unwrap());
        let mut thread = thread_with(&[me, them]);
        let (mine, theirs) = (Uuid::now_v7(), Uuid::now_v7());
        thread.read_marks.insert(me, mine);
        thread.read_marks.insert(them, theirs);

        let view = ThreadsView::project(&thread, &reader).unwrap();
        assert_eq!(view.read_mark, Some(mine));
        assert_eq!(view.members.len(), 2);
        assert_eq!(view.status, ThreadStatusView::Open);
        assert_eq!(
            serde_json::to_value(&view.affordances).unwrap(),
            serde_json::json!({ "setReadMark": { "allowed": true } })
        );
        assert_eq!(
            ThreadsView::project(&thread, &other).unwrap().read_mark,
            Some(theirs)
        );
    }

    #[test]
    fn every_contract_status_shows_under_its_published_literal() {
        use async_graphql::resolver_utils::EnumType;

        for status in ThreadStatus::ALL {
            let shown = ThreadStatusView::from(*status);
            let item = ThreadStatusView::items()
                .iter()
                .find(|item| item.value == shown)
                .expect("every value is an item");
            assert_eq!(item.name, status.as_str());
        }
    }
}
