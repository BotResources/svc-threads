//! The resolution of a thread: `thread.resolve` settles its discussion,
//! `thread.reopen` lifts it. A resolution is not a status — a resolved thread
//! stays `OPEN` — and while it holds, the message commands that would change the
//! conversation (`store`, `edit`, `delete`) are refused `THREAD_RESOLVED`. A
//! message already `STREAMING` still closes, and everything else of the thread
//! (its title, members, resources, read marks, its closing, its deletion) still
//! changes. Closing a resolved thread keeps its resolution as it was.
//!
//! Who may resolve or reopen is the commanding service's decision: the
//! transitions judge the thread's state only.

use contract_threads::ReasonCode;
use serde::Serialize;
use service_engine::error::EngineError;
use uuid::Uuid;

use super::state::{Resolution, ThreadChange, ThreadState};
use super::transitions::Applied;
use crate::command::Refusal;
use crate::facts::{Fact, FactMeta};

// The fact types of the resolution, noun `thread`.
pub const RESOLVED: &str = "Resolved";
pub const REOPENED: &str = "Reopened";

#[derive(Serialize)]
struct ResolvedFact {
    resolved_by: Uuid,
}

#[derive(Serialize)]
struct NoPayload {}

impl ThreadState {
    /// `THREAD_RESOLVED` while the thread is resolved: no message is stored,
    /// edited or deleted until it is reopened.
    pub fn resolved_refusal(&self) -> Option<Refusal> {
        self.resolution.map(|resolution| {
            Refusal::new(ReasonCode::ThreadResolved)
                .with("resolved_by", resolution.by)
                .with("resolved_at", resolution.at)
        })
    }

    /// `thread.resolve`, by `user`, at the command's date: judged on an `OPEN`
    /// thread that is not resolved yet.
    pub fn resolve(
        &mut self,
        user: Uuid,
        meta: &FactMeta,
    ) -> Result<Result<Applied, Refusal>, EngineError> {
        if let Some(refusal) = self.open_refusal() {
            return Ok(Err(refusal));
        }
        if let Some(resolution) = self.resolution {
            return Ok(Err(Refusal::new(ReasonCode::ThreadAlreadyResolved)
                .with("resolved_by", resolution.by)
                .with("resolved_at", resolution.at)));
        }
        self.resolution = Some(Resolution {
            by: user,
            at: meta.occurred_at,
        });
        self.version += 1;
        let fact = Fact::new(RESOLVED, &ResolvedFact { resolved_by: user }, meta)?;
        self.pending.push(ThreadChange::Recorded(fact));
        Ok(Ok(Applied::Changed))
    }

    /// `thread.reopen`: judged on an `OPEN` thread that is resolved; its
    /// resolution is cleared.
    pub fn reopen(&mut self, meta: &FactMeta) -> Result<Result<Applied, Refusal>, EngineError> {
        if let Some(refusal) = self.open_refusal() {
            return Ok(Err(refusal));
        }
        if self.resolution.is_none() {
            return Ok(Err(Refusal::new(ReasonCode::ThreadNotResolved)));
        }
        self.resolution = None;
        self.version += 1;
        let fact = Fact::new(REOPENED, &NoPayload {}, meta)?;
        self.pending.push(ThreadChange::Recorded(fact));
        Ok(Ok(Applied::Changed))
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use contract_threads::{ClosingStatus, ThreadStatus};

    use super::*;
    use crate::slices::thread::state::tests::thread_with;

    fn meta() -> FactMeta {
        FactMeta {
            actor: br_core_integration::Actor::Service(Uuid::now_v7().into()),
            acting_user_id: None,
            impersonator_id: None,
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            occurred_at: Utc::now(),
        }
    }

    fn code(verdict: Result<Result<Applied, Refusal>, EngineError>) -> ReasonCode {
        verdict.unwrap().unwrap_err().code
    }

    #[test]
    fn a_resolved_thread_stays_open_until_it_is_reopened() {
        let user = Uuid::now_v7();
        let mut thread = thread_with(&[user]);
        let at = meta();
        assert_eq!(thread.resolve(user, &at).unwrap(), Ok(Applied::Changed));
        assert_eq!(thread.status, ThreadStatus::Open);
        assert_eq!(
            thread.resolution,
            Some(Resolution {
                by: user,
                at: at.occurred_at
            })
        );
        assert_eq!(thread.version, 2);
        assert_eq!(
            thread.resolved_refusal().unwrap().code,
            ReasonCode::ThreadResolved
        );
        assert_eq!(
            code(thread.resolve(user, &meta())),
            ReasonCode::ThreadAlreadyResolved
        );
        assert_eq!(thread.version, 2, "a refusal changes nothing");

        assert_eq!(thread.reopen(&meta()).unwrap(), Ok(Applied::Changed));
        assert_eq!(thread.resolution, None);
        assert_eq!(thread.version, 3);
        assert!(thread.resolved_refusal().is_none());
        assert_eq!(code(thread.reopen(&meta())), ReasonCode::ThreadNotResolved);
        let types: Vec<&str> = thread
            .pending_facts()
            .iter()
            .map(|fact| fact.event_type)
            .collect();
        assert_eq!(types, [RESOLVED, REOPENED]);
    }

    #[test]
    fn a_closed_thread_keeps_its_resolution_and_is_neither_resolved_nor_reopened() {
        let user = Uuid::now_v7();
        let mut resolved = thread_with(&[user]);
        resolved.resolve(user, &meta()).unwrap().unwrap();
        resolved
            .close(ClosingStatus::Completed, &meta())
            .unwrap()
            .unwrap();
        assert!(resolved.resolution.is_some(), "closing keeps the resolution");
        assert_eq!(code(resolved.reopen(&meta())), ReasonCode::ThreadNotOpen);

        let mut closed = thread_with(&[user]);
        closed
            .close(ClosingStatus::Cancelled, &meta())
            .unwrap()
            .unwrap();
        assert_eq!(code(closed.resolve(user, &meta())), ReasonCode::ThreadNotOpen);
    }
}
