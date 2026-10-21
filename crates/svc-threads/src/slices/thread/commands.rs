//! The thread commands, one reaction each: `integration.cmd.threads.thread.
//! {create, add_members, remove_members, add_resource_ids, remove_resource_ids,
//! set_title, close, resolve, reopen, delete}.v1`.
//!
//! Each reaction loads and locks what the command names, calls the command's
//! transition, writes only what the transition changed, stages the impacts of
//! the change, and answers the command once — `thread.confirmed` or
//! `thread.refused` — in the same transaction. A refusal writes nothing.

use std::collections::BTreeSet;

use contract_threads::{
    AddMembers, AddResourceIds, CloseThread, CreateThread, DeleteThread, ReasonCode, RemoveMembers,
    RemoveResourceIds, ReopenThread, ResolveThread, SetTitle,
};
use futures_util::future::BoxFuture;
use service_engine::pipeline::{Ops, Reaction};
use uuid::Uuid;

use super::state::{Thread, ThreadState};
use super::transitions::{self, Applied, DeletionReason, ParentLookup, TitleSource};
use super::{ThreadCause, title_job};
use crate::command::{Call, Inbound, Refusal, answer_thread};
use crate::directory;
use crate::facts::{self, FactMeta, FactNoun};
use crate::fault::Fault;
use crate::principal::MEMBERSHIP_DEPS;
use crate::slices::artifact;
use crate::slices::message::closing;

fn not_found() -> Refusal {
    Refusal::new(ReasonCode::ThreadNotFound)
}

/// What a thread command does once its thread is loaded: the transition's
/// verdict, `Applied` when it may have changed the thread.
type Verdict = Result<Applied, Refusal>;

/// Saves a changed thread and stages the impact of the change: the thread
/// itself for its viewers, and the principal facts of every user whose
/// membership moved, so their windows over the thread follow at once.
async fn save_changed(
    ops: &mut Ops<'_>,
    thread: &ThreadState,
    cause: ThreadCause,
    membership_moved: &[Uuid],
) -> Result<(), Fault> {
    ops.save(thread).await?;
    ops.impact_caused::<Thread, _>(&thread.id, cause)?;
    for user in membership_moved {
        ops.impact_principal_facts((*user).into(), MEMBERSHIP_DEPS);
    }
    Ok(())
}

pub fn create<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<CreateThread>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let verdict = create_thread(cx, &call, &command).await?;
        answer_thread(cx, &call, command.thread_id, verdict)
    })
}

async fn create_thread(
    cx: &mut Reaction<'_>,
    call: &Call,
    command: &CreateThread,
) -> Result<Result<(), Refusal>, Fault> {
    if let Err(refusal) = crate::command::minted("thread_id", command.thread_id) {
        return Ok(Err(refusal));
    }
    let exists = cx.load::<ThreadState>(&command.thread_id).await?.is_some();
    let parent = match command.parent_thread_id {
        None => ParentLookup::NotNamed,
        Some(parent) => match parent_members(cx, parent).await? {
            Some(members) => ParentLookup::Found(members),
            None => ParentLookup::Missing,
        },
    };
    let named: Vec<Uuid> = command
        .owner_id
        .iter()
        .chain(command.user_ids.iter())
        .copied()
        .collect();
    let in_roster = directory::in_roster(cx.connection(), &named).await?;
    let thread = match transitions::create(
        command,
        exists,
        &in_roster,
        &parent,
        &call.meta,
        call.meta.occurred_at,
    )? {
        Ok(thread) => thread,
        Err(refusal) => return Ok(Err(refusal)),
    };
    cx.create(&thread).await?;
    cx.impact_caused::<Thread, _>(&thread.id, ThreadCause::Created)?;
    for user in thread.members.keys() {
        cx.impact_principal_facts((*user).into(), MEMBERSHIP_DEPS);
    }
    Ok(Ok(()))
}

/// The members of a parent thread at a child's creation, if it exists: read,
/// not locked — a membership that moves meanwhile is a later change.
async fn parent_members(
    cx: &mut Reaction<'_>,
    thread: Uuid,
) -> Result<Option<BTreeSet<Uuid>>, Fault> {
    use service_engine::PersistenceExt;
    Ok(super::store::ThreadStore::load(cx.connection(), &thread)
        .await?
        .map(|parent| parent.member_ids()))
}

/// Runs a command whose transition acts on one existing thread: load and lock
/// it (`THREAD_NOT_FOUND` when absent), let `act` decide and change it, then
/// save what changed and answer.
macro_rules! on_thread {
    ($cx:ident, $command:ident, |$thread:ident, $call:ident| $act:expr) => {{
        let $call = Call::of($cx, &$command)?;
        let verdict = match $cx.load::<ThreadState>(&$command.thread_id).await? {
            None => Err(not_found()),
            Some(mut $thread) => {
                let outcome: Result<(Verdict, ThreadCause, Vec<Uuid>), Fault> = $act;
                let (verdict, cause, moved) = outcome?;
                if verdict == Ok(Applied::Changed) {
                    save_changed($cx, &$thread, cause, &moved).await?;
                }
                verdict.map(|_| ())
            }
        };
        answer_thread($cx, &$call, $command.thread_id, verdict)
    }};
}

pub fn add_members<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<AddMembers>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        on_thread!(cx, command, |thread, call| {
            let in_roster = directory::in_roster(cx.connection(), &command.user_ids).await?;
            let before = thread.member_ids();
            let verdict = thread.add_members(&command.user_ids, &in_roster, &call.meta)?;
            let added = thread.member_ids().difference(&before).copied().collect();
            Ok((verdict, ThreadCause::MembersChanged, added))
        })
    })
}

pub fn remove_members<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<RemoveMembers>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        on_thread!(cx, command, |thread, call| {
            let before = thread.member_ids();
            let verdict = thread.remove_members(&command.user_ids, &call.meta)?;
            let removed = before.difference(&thread.member_ids()).copied().collect();
            Ok((verdict, ThreadCause::MembersChanged, removed))
        })
    })
}

pub fn add_resource_ids<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<AddResourceIds>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        on_thread!(cx, command, |thread, call| {
            let verdict = thread.add_resource_ids(&command.key, &command.ids, &call.meta)?;
            Ok((verdict, ThreadCause::ResourcesChanged, Vec::new()))
        })
    })
}

pub fn remove_resource_ids<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<RemoveResourceIds>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        on_thread!(cx, command, |thread, call| {
            let verdict = thread.remove_resource_ids(&command.key, &command.ids, &call.meta)?;
            Ok((verdict, ThreadCause::ResourcesChanged, Vec::new()))
        })
    })
}

/// A title set by command while the title Job runs wins: the Job is cancelled
/// at Jobs in the same transaction.
pub fn set_title<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<SetTitle>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        on_thread!(cx, command, |thread, call| {
            let verdict = thread.set_title(&command.title, TitleSource::Command, &call.meta)?;
            if verdict == Ok(Applied::Changed)
                && let Some(job) = title_job::cancel_active(&mut thread, call.meta.occurred_at)
            {
                title_job::publish_cancel(cx, job)?;
            }
            Ok((verdict, ThreadCause::TitleChanged, Vec::new()))
        })
    })
}

pub fn close<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<CloseThread>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        on_thread!(cx, command, |thread, call| {
            let verdict = thread.close(command.status, &call.meta)?;
            Ok((verdict, ThreadCause::Closed, Vec::new()))
        })
    })
}

pub fn resolve<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<ResolveThread>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        on_thread!(cx, command, |thread, call| {
            let verdict = thread.resolve(command.acting_user_id, &call.meta)?;
            Ok((verdict, ThreadCause::Resolved, Vec::new()))
        })
    })
}

pub fn reopen<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<ReopenThread>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        on_thread!(cx, command, |thread, call| {
            let verdict = thread.reopen(&call.meta)?;
            Ok((verdict, ThreadCause::Reopened, Vec::new()))
        })
    })
}

pub fn delete<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<DeleteThread>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let verdict = match cx.load::<ThreadState>(&command.thread_id).await? {
            None => Err(not_found()),
            Some(thread) => {
                delete_thread(cx, thread, DeletionReason::Command, &call.meta).await?;
                Ok(())
            }
        };
        answer_thread(cx, &call, command.thread_id, verdict)
    })
}

/// Deletes a loaded and locked thread with everything it holds: its running
/// title Job is cancelled at Jobs, its artifacts deleted through their store
/// (their bytes released), the streams of its streaming messages sealed, its
/// `Deleted` fact recorded, its members entry
/// retracted (the offer's dirty key, staged by the delete), its rows deleted.
/// Its members' windows over it are repopulated.
pub async fn delete_thread(
    ops: &mut Ops<'_>,
    mut thread: ThreadState,
    reason: DeletionReason,
    meta: &FactMeta,
) -> Result<(), Fault> {
    if let Some(job) = title_job::cancel_active(&mut thread, meta.occurred_at) {
        title_job::publish_cancel(ops, job)?;
    }
    artifact::delete_all_of_thread(ops, thread.id).await?;
    closing::seal_streams_of_thread(ops, thread.id).await?;
    let deleted = thread.deleted_fact(reason, meta)?;
    facts::append(
        ops.connection(),
        FactNoun::Thread,
        thread.id,
        thread.version + 1,
        &[deleted],
    )
    .await?;
    ops.delete(&thread).await?;
    ops.impact_caused::<Thread, _>(&thread.id, ThreadCause::Deleted)?;
    for user in thread.members.keys() {
        ops.impact_principal_facts((*user).into(), MEMBERSHIP_DEPS);
    }
    Ok(())
}
