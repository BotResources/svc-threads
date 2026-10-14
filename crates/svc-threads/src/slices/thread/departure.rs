//! When an owner leaves the roster, Threads deletes the threads they own;
//! ownerless threads, and the user's memberships, messages and read marks
//! elsewhere, stay.
//!
//! ENGINE GAP (br-service-engine v0.4.1): a mirror projection can write rows
//! and stage impacts, but cannot run pipeline work — `cx.delete` (which retracts
//! the members entry and releases artifact bytes), an outbox command (the title
//! Job's `job.cancel`), or a scheduled reaction. The directory mirror therefore
//! only marks the user out of the roster, and this module carries the
//! consequence through the write pipeline: a cron job on the engine beat finds
//! the threads whose owner the roster copy holds out of the roster — the
//! canonical predicate itself, so nothing is lost if a pod dies, and a user
//! retracted while Threads was down is caught once the mirror's reconciliation
//! marks them — and deletes each through a registered mutation run by the
//! engine's executor, as Threads itself. The engine would need a mirror
//! projection hook that can stage pipeline work (or a post-commit reaction on a
//! known row) to make this event-driven.

use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::cron::{CronJob, Schedule};
use service_engine::error::CronError;
use service_engine::name::JobName;
use service_engine::pipeline::{Mutation, MutationExecutor, MutationInput};
use sqlx::PgPool;
use uuid::Uuid;

use super::commands::delete_thread;
use super::state::ThreadState;
use super::transitions::DeletionReason;
use crate::directory::KNOWN_USERS_TABLE;
use crate::facts::FactMeta;
use crate::fault::Fault;
use crate::principal::ThreadsPrincipal;

/// How many threads one sweep deletes at most; the next sweep takes the rest.
const SWEEP_BATCH: i64 = 64;

/// Deletes one thread whose owner left the roster, if that still holds.
#[derive(Debug, Deserialize)]
pub struct DeleteDepartedOwnersThread {
    pub thread_id: Uuid,
    pub owner_id: Uuid,
}

impl MutationInput for DeleteDepartedOwnersThread {
    type Output = ();
    type Error = Fault;
    const NAME: &'static str = "delete_departed_owners_thread";
}

pub fn delete_departed_owners_thread<'m>(
    cx: &'m mut Mutation<'m, ThreadsPrincipal>,
    input: DeleteDepartedOwnersThread,
) -> BoxFuture<'m, Result<(), Fault>> {
    Box::pin(async move {
        // Only Threads itself sweeps; the mutation has no GraphQL root.
        if !cx.principal().is_threads_itself() {
            return Err(crate::access::THREAD_NOT_FOUND.into());
        }
        let Some(thread) = cx.load::<ThreadState>(&input.thread_id).await? else {
            return Ok(());
        };
        let still_departed: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM {KNOWN_USERS_TABLE} WHERE user_id = $1 AND NOT in_roster)"
        ))
        .bind(input.owner_id)
        .fetch_one(cx.connection())
        .await?;
        if thread.owner_id != Some(input.owner_id) || !still_departed {
            return Ok(());
        }
        let meta = FactMeta::of_principal(cx.principal(), cx.now().as_datetime());
        delete_thread(cx, thread, DeletionReason::OwnerLeftRoster, &meta).await
    })
}

/// The sweep, on the engine's cron (one pod per fire).
pub struct DepartedOwners {
    executor: MutationExecutor<ThreadsPrincipal>,
}

impl DepartedOwners {
    pub fn new(executor: MutationExecutor<ThreadsPrincipal>) -> Self {
        Self { executor }
    }
}

impl CronJob for DepartedOwners {
    fn name(&self) -> JobName {
        JobName::from_static("departed_owners")
    }

    fn schedule(&self) -> Schedule {
        Schedule::EveryBeats(2)
    }

    fn run<'a>(&'a self, pg: &'a PgPool) -> BoxFuture<'a, Result<(), CronError>> {
        Box::pin(async move {
            let owned: Vec<(Uuid, Uuid)> = sqlx::query_as(&format!(
                "SELECT t.id, t.owner_id FROM threads.thread t \
                 JOIN {KNOWN_USERS_TABLE} k ON k.user_id = t.owner_id \
                 WHERE NOT k.in_roster ORDER BY t.id LIMIT $1"
            ))
            .bind(SWEEP_BATCH)
            .fetch_all(pg)
            .await?;
            for (thread_id, owner_id) in owned {
                // One thread per transaction: the impacts of a deletion grow
                // with its members, never with the batch.
                self.executor
                    .run::<DeleteDepartedOwnersThread>(
                        ThreadsPrincipal::threads_itself(),
                        DeleteDepartedOwnersThread {
                            thread_id,
                            owner_id,
                        },
                    )
                    .await
                    .map_err(|error| CronError::Job(Box::new(error)))?;
            }
            Ok(())
        })
    }
}
