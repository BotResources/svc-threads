//! A thread's canonical state, as its store reads it back: the thread row, its
//! members (with the name the roster copy holds for each), its resources and
//! its read marks — plus the changes a transition made to it and not yet saved
//! ([`ThreadChange`]), which its store writes and appends to the fact log in the
//! transaction of the change.
//!
//! The guarded transitions live in [`super::transitions`].

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use contract_threads::{ThreadStatus, ThreadTitle};
use serde::{Deserialize, Serialize};
use service_engine::name::NounName;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use service_engine::{Cohort, Deps};
use uuid::Uuid;

use crate::facts::Fact;
use crate::principal::{ROSTER_DEPS, ThreadsPrincipal};

/// The thread noun: what a thread's impacts name.
pub struct Thread;

impl Noun for Thread {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("thread");
}

/// The visibility cohort of a thread: one per member.
pub const MEMBER_DIM: &str = "member";

/// A thread is seen by its members only, and only while they are active users
/// of the roster copy: super-user status gives nothing.
impl Visibility for Thread {
    type Row = ThreadState;
    type Principal = ThreadsPrincipal;

    /// An active user's standing is a principal fact: a deactivation or a
    /// departure from the roster takes every thread out of their windows.
    const DEPS: Deps = ROSTER_DEPS;

    fn cohorts(row: &ThreadState) -> Cohorts {
        row.members
            .keys()
            .map(|user| Cohort::uuid(MEMBER_DIM, *user))
            .collect()
    }

    fn memberships(principal: &ThreadsPrincipal) -> Cohorts {
        principal
            .active_user()
            .map(|user| Cohort::uuid(MEMBER_DIM, user))
            .into_iter()
            .collect()
    }
}

/// The thread and block a child thread was started from. Set together at the
/// creation, never changed; no foreign key — they may lead nowhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadOrigin {
    pub thread_id: Uuid,
    pub block_id: Uuid,
}

/// The resolution of a thread whose discussion is settled: who resolved it and
/// when. Set together by a resolution, cleared together by a reopening. It is
/// not a status — a resolved thread is still `OPEN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resolution {
    pub by: Uuid,
    pub at: DateTime<Utc>,
}

/// A member, with what the roster copy holds for them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    /// The roster copy's name; `None` for a user it never held.
    pub display_name: Option<String>,
    /// False once the user left the roster.
    pub in_roster: bool,
}

/// A stored value outside the domain of its column.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{value}` is not a value of {column}")]
pub struct UnknownStoredValue {
    pub column: &'static str,
    pub value: String,
}

/// What a transition changed and the store has yet to write: the rows beside
/// the thread row (members, resources, read marks), and the facts the change
/// records. The thread row is written whole at every save.
#[derive(Debug, Clone, PartialEq)]
pub enum ThreadChange {
    MembersAdded(Vec<Uuid>),
    /// Their read marks go with them.
    MembersRemoved(Vec<Uuid>),
    /// The whole list of `key` after the change; an empty list removes the key.
    ResourceSet {
        key: String,
        ids: Vec<String>,
    },
    /// The reader's own state: no fact, no version.
    ReadMarkSet {
        user: Uuid,
        message: Uuid,
    },
    Recorded(Fact),
}

/// One thread, whole.
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadState {
    pub id: Uuid,
    pub owner_id: Option<Uuid>,
    pub title: Option<ThreadTitle>,
    pub origin: Option<ThreadOrigin>,
    pub status: ThreadStatus,
    /// Set while the thread is resolved: it then accepts no new message, edit
    /// or deletion.
    pub resolution: Option<Resolution>,
    pub created_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub version: i64,
    pub members: BTreeMap<Uuid, Member>,
    /// Each key's ids, in the order they were added; a key is never empty.
    pub resources: BTreeMap<String, Vec<String>>,
    /// Each member's last read message.
    pub read_marks: BTreeMap<Uuid, Uuid>,
    /// Unsaved changes, in order.
    pub pending: Vec<ThreadChange>,
}

impl ThreadState {
    /// The facts the unsaved changes record.
    pub fn pending_facts(&self) -> Vec<Fact> {
        self.pending
            .iter()
            .filter_map(|change| match change {
                ThreadChange::Recorded(fact) => Some(fact.clone()),
                _ => None,
            })
            .collect()
    }

    /// Whether `user` is the thread's owner.
    pub fn is_owner(&self, user: Uuid) -> bool {
        self.owner_id == Some(user)
    }

    pub fn is_member(&self, user: Uuid) -> bool {
        self.members.contains_key(&user)
    }

    /// Every member, the owner included, each once: the published entry's
    /// `users`.
    pub fn member_ids(&self) -> BTreeSet<Uuid> {
        self.members.keys().copied().collect()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::principal::RosterStanding;
    use crate::principal::tests::{active, person, service};

    pub fn thread_with(members: &[Uuid]) -> ThreadState {
        let now = Utc::now();
        ThreadState {
            id: Uuid::now_v7(),
            owner_id: members.first().copied(),
            title: None,
            origin: None,
            status: ThreadStatus::Open,
            resolution: None,
            created_at: now,
            last_activity_at: now,
            version: 1,
            members: members
                .iter()
                .map(|user| {
                    (
                        *user,
                        Member {
                            display_name: Some("Ada".into()),
                            in_roster: true,
                        },
                    )
                })
                .collect(),
            resources: BTreeMap::new(),
            read_marks: BTreeMap::new(),
            pending: Vec::new(),
        }
    }

    #[test]
    fn a_thread_is_visible_to_its_active_members_only() {
        let member = person(false, active());
        let user = member.user().unwrap();
        let thread = thread_with(&[user]);
        assert!(<Thread as Visibility>::visible(&thread, &member));

        let stranger = person(true, active());
        assert!(
            !<Thread as Visibility>::visible(&thread, &stranger),
            "a super user who is not a member sees nothing"
        );
        assert!(!<Thread as Visibility>::visible(
            &thread,
            &service(&[crate::scopes::RUNNER])
        ));
    }

    #[test]
    fn a_member_who_is_no_longer_active_sees_nothing() {
        let member = person(false, RosterStanding::Deactivated);
        let thread = thread_with(&[member.user().unwrap()]);
        assert!(!<Thread as Visibility>::visible(&thread, &member));
    }
}
