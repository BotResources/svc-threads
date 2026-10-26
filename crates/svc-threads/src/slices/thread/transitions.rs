//! The guarded transitions of a thread, one per command. Each judges the
//! coherence of the conversation — never rights: the commanding service took
//! that decision — and either refuses with its code, changes nothing (a no-op
//! is confirmed), or changes the thread and records the facts of the change.
//!
//! A transition is the one decision of its command: the reaction calls it on
//! the state it loaded and locked, and writes only what it returns.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use contract_threads::{
    ClosingStatus, CreateThread, ReasonCode, ThreadStatus, ThreadTitle, ThreadTitleError,
};
use serde::Serialize;
use service_engine::error::EngineError;
use uuid::Uuid;

use super::state::{Member, ThreadChange, ThreadOrigin, ThreadState};
use crate::command::Refusal;
use crate::facts::{Fact, FactMeta};
use crate::literal::literal_enum;

/// Whether a transition changed the thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    Changed,
    Unchanged,
}

literal_enum! {
    /// Why a thread was deleted, as its `Deleted` fact says.
    pub enum DeletionReason {
        /// A `thread.delete` command.
        Command => "COMMAND",
        /// Its owner left the roster.
        OwnerLeftRoster => "OWNER_LEFT_ROSTER",
    }
}

literal_enum! {
    /// Who set a title, as its `TitleChanged` fact says.
    pub enum TitleSource {
        Command => "COMMAND",
        TitleJob => "TITLE_JOB",
    }
}

// The fact types of the noun `thread`.
pub const CREATED: &str = "Created";
pub const TITLE_CHANGED: &str = "TitleChanged";
pub const MEMBERS_ADDED: &str = "MembersAdded";
pub const MEMBERS_REMOVED: &str = "MembersRemoved";
pub const RESOURCE_IDS_ADDED: &str = "ResourceIdsAdded";
pub const RESOURCE_IDS_REMOVED: &str = "ResourceIdsRemoved";
pub const COMPLETED: &str = "Completed";
pub const FAILED: &str = "Failed";
pub const CANCELLED: &str = "Cancelled";
pub const DELETED: &str = "Deleted";

#[derive(Serialize)]
struct CreatedFact<'a> {
    owner_id: Option<Uuid>,
    title: Option<&'a str>,
    parent_thread_id: Option<Uuid>,
    origin_block_id: Option<Uuid>,
    resources: &'a BTreeMap<String, Vec<String>>,
}

#[derive(Serialize)]
struct MembersFact<'a> {
    user_ids: &'a [Uuid],
}

#[derive(Serialize)]
struct TitleFact<'a> {
    title: &'a str,
    by: TitleSource,
}

#[derive(Serialize)]
struct ResourceFact<'a> {
    key: &'a str,
    ids: &'a [String],
}

#[derive(Serialize)]
struct DeletedFact {
    reason: DeletionReason,
}

#[derive(Serialize)]
struct NoPayload {}

/// The parent a creation names, as the store found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentLookup {
    NotNamed,
    Missing,
    /// Its members at the creation of the child.
    Found(BTreeSet<Uuid>),
}

/// The one refusal of a title out of its domain, whichever gesture brought it:
/// a creation, `thread.set_title` or the title Job's runner.
fn title_refused(error: ThreadTitleError) -> Refusal {
    let refusal = Refusal::new(error.reason_code());
    match error {
        ThreadTitleError::Empty => refusal.with("min_chars", ThreadTitle::MIN_CHARS),
        ThreadTitleError::TooLong(too_long) => refusal
            .with("max_chars", ThreadTitle::MAX_CHARS)
            .with("chars", too_long.chars),
    }
}

fn record<P: Serialize>(
    thread: &mut ThreadState,
    event_type: &'static str,
    payload: &P,
    meta: &FactMeta,
) -> Result<(), EngineError> {
    thread.pending.push(ThreadChange::Recorded(Fact::new(
        event_type, payload, meta,
    )?));
    Ok(())
}

/// `thread.create`: a thread born `OPEN` at version 1, its owner a member.
/// `exists` says whether the id is already held; `in_roster` holds, of the
/// owner and members named, those the roster copy has in the roster.
pub fn create(
    command: &CreateThread,
    exists: bool,
    in_roster: &BTreeSet<Uuid>,
    parent: &ParentLookup,
    meta: &FactMeta,
    now: DateTime<Utc>,
) -> Result<Result<ThreadState, Refusal>, EngineError> {
    if exists {
        return Ok(Err(Refusal::new(ReasonCode::KeyReused)));
    }
    let title = match command.title.clone().map(ThreadTitle::parse).transpose() {
        Ok(title) => title,
        Err(error) => return Ok(Err(title_refused(error))),
    };
    let origin = match (command.parent_thread_id, command.origin_block_id) {
        (Some(thread_id), Some(block_id)) => Some(ThreadOrigin {
            thread_id,
            block_id,
        }),
        (None, None) => None,
        (parent, block) => {
            return Ok(Err(Refusal::new(ReasonCode::OriginBlockRequired)
                .with("parent_thread_id", parent)
                .with("origin_block_id", block)));
        }
    };
    let mut users: Vec<Uuid> = Vec::new();
    for user in command.owner_id.iter().chain(command.user_ids.iter()) {
        if !users.contains(user) {
            users.push(*user);
        }
    }
    let unknown: Vec<Uuid> = users
        .iter()
        .copied()
        .filter(|user| !in_roster.contains(user))
        .collect();
    if !unknown.is_empty() {
        return Ok(Err(
            Refusal::new(ReasonCode::UserUnknown).with("user_ids", unknown)
        ));
    }
    match parent {
        ParentLookup::NotNamed => {}
        ParentLookup::Missing => {
            return Ok(Err(Refusal::new(ReasonCode::ThreadNotFound)
                .with("parent_thread_id", command.parent_thread_id)));
        }
        ParentLookup::Found(parent_members) => {
            if let Some(owner) = command.owner_id
                && !parent_members.contains(&owner)
            {
                return Ok(Err(Refusal::new(ReasonCode::OwnerNotParentMember)
                    .with("owner_id", owner)
                    .with("parent_thread_id", command.parent_thread_id)));
            }
        }
    }
    let resources: BTreeMap<String, Vec<String>> = command
        .resources
        .iter()
        .filter(|(_, ids)| !ids.is_empty())
        .map(|(key, ids)| (key.clone(), dedup(ids)))
        .collect();
    if let Some(blank) = resources.keys().find(|key| key.trim().is_empty()) {
        return Ok(Err(
            Refusal::new(ReasonCode::ResourceKeyInvalid).with("key", blank)
        ));
    }

    let mut thread = ThreadState {
        id: command.thread_id,
        owner_id: command.owner_id,
        title,
        origin,
        status: ThreadStatus::Open,
        resolution: None,
        created_at: now,
        last_activity_at: now,
        version: 1,
        members: users
            .iter()
            .map(|user| {
                (
                    *user,
                    Member {
                        display_name: None,
                        in_roster: true,
                    },
                )
            })
            .collect(),
        resources,
        read_marks: BTreeMap::new(),
        title_job: None,
        pending: Vec::new(),
    };
    let created = CreatedFact {
        owner_id: thread.owner_id,
        title: thread.title.as_ref().map(ThreadTitle::as_str),
        parent_thread_id: origin.map(|origin| origin.thread_id),
        origin_block_id: origin.map(|origin| origin.block_id),
        resources: &thread.resources,
    };
    let created = Fact::new(CREATED, &created, meta)?;
    thread.pending.push(ThreadChange::Recorded(created));
    if !users.is_empty() {
        record(
            &mut thread,
            MEMBERS_ADDED,
            &MembersFact { user_ids: &users },
            meta,
        )?;
    }
    Ok(Ok(thread))
}

fn dedup(ids: &[String]) -> Vec<String> {
    let mut kept: Vec<String> = Vec::with_capacity(ids.len());
    for id in ids {
        if !kept.contains(id) {
            kept.push(id.clone());
        }
    }
    kept
}

impl ThreadState {
    fn bump(&mut self) {
        self.version += 1;
    }

    /// `thread.add_members`: users already members change nothing; one user
    /// not in the roster refuses the whole list.
    pub fn add_members(
        &mut self,
        users: &[Uuid],
        in_roster: &BTreeSet<Uuid>,
        meta: &FactMeta,
    ) -> Result<Result<Applied, Refusal>, EngineError> {
        let unknown: Vec<Uuid> = users
            .iter()
            .copied()
            .filter(|user| !in_roster.contains(user))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if !unknown.is_empty() {
            return Ok(Err(
                Refusal::new(ReasonCode::UserUnknown).with("user_ids", unknown)
            ));
        }
        let mut added: Vec<Uuid> = Vec::new();
        for user in users {
            if !self.is_member(*user) && !added.contains(user) {
                added.push(*user);
            }
        }
        if added.is_empty() {
            return Ok(Ok(Applied::Unchanged));
        }
        for user in &added {
            self.members.insert(
                *user,
                Member {
                    display_name: None,
                    in_roster: true,
                },
            );
        }
        self.bump();
        self.pending.push(ThreadChange::MembersAdded(added.clone()));
        record(self, MEMBERS_ADDED, &MembersFact { user_ids: &added }, meta)?;
        Ok(Ok(Applied::Changed))
    }

    /// `thread.remove_members`: the owner is never removed (the whole list is
    /// refused); users not members change nothing; a removed member's read
    /// mark goes with them.
    pub fn remove_members(
        &mut self,
        users: &[Uuid],
        meta: &FactMeta,
    ) -> Result<Result<Applied, Refusal>, EngineError> {
        if let Some(owner) = users.iter().find(|user| self.is_owner(**user)) {
            return Ok(Err(
                Refusal::new(ReasonCode::OwnerNotRemovable).with("user_id", owner)
            ));
        }
        let mut removed: Vec<Uuid> = Vec::new();
        for user in users {
            if self.is_member(*user) && !removed.contains(user) {
                removed.push(*user);
            }
        }
        if removed.is_empty() {
            return Ok(Ok(Applied::Unchanged));
        }
        for user in &removed {
            self.members.remove(user);
            self.read_marks.remove(user);
        }
        self.bump();
        self.pending
            .push(ThreadChange::MembersRemoved(removed.clone()));
        record(
            self,
            MEMBERS_REMOVED,
            &MembersFact { user_ids: &removed },
            meta,
        )?;
        Ok(Ok(Applied::Changed))
    }

    /// `thread.add_resource_ids`: ids already under the key change nothing.
    pub fn add_resource_ids(
        &mut self,
        key: &str,
        ids: &[String],
        meta: &FactMeta,
    ) -> Result<Result<Applied, Refusal>, EngineError> {
        if key.trim().is_empty() {
            return Ok(Err(
                Refusal::new(ReasonCode::ResourceKeyInvalid).with("key", key)
            ));
        }
        let held = self.resources.get(key).cloned().unwrap_or_default();
        let added: Vec<String> = dedup(ids)
            .into_iter()
            .filter(|id| !held.contains(id))
            .collect();
        if added.is_empty() {
            return Ok(Ok(Applied::Unchanged));
        }
        let mut list = held;
        list.extend(added.iter().cloned());
        self.resources.insert(key.to_string(), list.clone());
        self.bump();
        self.pending.push(ThreadChange::ResourceSet {
            key: key.to_string(),
            ids: list,
        });
        record(
            self,
            RESOURCE_IDS_ADDED,
            &ResourceFact { key, ids: &added },
            meta,
        )?;
        Ok(Ok(Applied::Changed))
    }

    /// `thread.remove_resource_ids`: absent ids change nothing; an emptied key
    /// disappears.
    pub fn remove_resource_ids(
        &mut self,
        key: &str,
        ids: &[String],
        meta: &FactMeta,
    ) -> Result<Result<Applied, Refusal>, EngineError> {
        let Some(held) = self.resources.get(key).cloned() else {
            return Ok(Ok(Applied::Unchanged));
        };
        let removed: Vec<String> = dedup(ids)
            .into_iter()
            .filter(|id| held.contains(id))
            .collect();
        if removed.is_empty() {
            return Ok(Ok(Applied::Unchanged));
        }
        let list: Vec<String> = held
            .into_iter()
            .filter(|id| !removed.contains(id))
            .collect();
        if list.is_empty() {
            self.resources.remove(key);
        } else {
            self.resources.insert(key.to_string(), list.clone());
        }
        self.bump();
        self.pending.push(ThreadChange::ResourceSet {
            key: key.to_string(),
            ids: list,
        });
        record(
            self,
            RESOURCE_IDS_REMOVED,
            &ResourceFact { key, ids: &removed },
            meta,
        )?;
        Ok(Ok(Applied::Changed))
    }

    /// Sets the title (`thread.set_title`, or the title Job's runner): 1 to 200
    /// characters; the same title changes nothing.
    pub fn set_title(
        &mut self,
        raw: &str,
        source: TitleSource,
        meta: &FactMeta,
    ) -> Result<Result<Applied, Refusal>, EngineError> {
        let title = match ThreadTitle::parse(raw) {
            Ok(title) => title,
            Err(error) => return Ok(Err(title_refused(error))),
        };
        if self.title.as_ref() == Some(&title) {
            return Ok(Ok(Applied::Unchanged));
        }
        self.title = Some(title);
        self.bump();
        record(
            self,
            TITLE_CHANGED,
            &TitleFact {
                title: raw,
                by: source,
            },
            meta,
        )?;
        Ok(Ok(Applied::Changed))
    }

    /// `thread.close`: an `OPEN` thread takes a terminal status, for good.
    pub fn close(
        &mut self,
        status: ClosingStatus,
        meta: &FactMeta,
    ) -> Result<Result<Applied, Refusal>, EngineError> {
        if let Some(refusal) = self.open_refusal() {
            return Ok(Err(refusal));
        }
        self.status = status.into();
        self.bump();
        let fact = match status {
            ClosingStatus::Completed => COMPLETED,
            ClosingStatus::Failed => FAILED,
            ClosingStatus::Cancelled => CANCELLED,
        };
        record(self, fact, &NoPayload {}, meta)?;
        Ok(Ok(Applied::Changed))
    }

    /// `THREAD_NOT_OPEN` when the thread is closed: messages are accepted only
    /// while it is `OPEN`, and a terminal status never changes.
    pub fn open_refusal(&self) -> Option<Refusal> {
        self.status
            .is_terminal()
            .then(|| Refusal::new(ReasonCode::ThreadNotOpen).with("status", self.status.as_str()))
    }

    /// The `Deleted` fact of a thread, at the version after its last change.
    /// The fact covers its members, resources, messages, items, blocks,
    /// artifacts and read marks.
    pub fn deleted_fact(
        &self,
        reason: DeletionReason,
        meta: &FactMeta,
    ) -> Result<Fact, EngineError> {
        Fact::new(DELETED, &DeletedFact { reason }, meta)
    }

    /// A message was stored: the thread's activity moves to its date (the
    /// order of its members' lists). Not a change of the thread: no fact, no
    /// version.
    pub fn record_activity(&mut self, at: DateTime<Utc>) {
        if at > self.last_activity_at {
            self.last_activity_at = at;
        }
    }

    /// A member sets their own read mark: no fact, no version.
    pub fn set_read_mark(&mut self, user: Uuid, message: Uuid) {
        self.read_marks.insert(user, message);
        self.pending
            .push(ThreadChange::ReadMarkSet { user, message });
    }
}

#[cfg(test)]
#[path = "transitions_tests.rs"]
mod tests;
