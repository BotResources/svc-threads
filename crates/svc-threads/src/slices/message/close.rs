use contract_threads::llm::{
    ClosureReason, DeltaOp, DraftPhase, EntryId, ResponseDelta, TurnClosure, TurnEntry,
};
use contract_threads::{MessageInvalidError, MessageStatus};
use service_engine::error::EngineError;
use uuid::Uuid;

use super::closure::{Closing, Kind};
use super::content::Content;
use super::draft::{Judged, JudgedItem};
use crate::fault::corrupt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseKind {
    Complete,
    Fail,
    Cancel,
    Watch,
}

pub mod reason {
    pub const FAILED_BY_WRITER: &str = "failed_by_writer";
    pub const STREAM_IDLE: &str = "stream_idle";
    pub const RESPONSE_STILL_PRODUCING: &str = "response_still_producing";
}

#[derive(Debug, Clone, PartialEq)]
pub struct Closed {
    pub status: MessageStatus,
    pub content: Content,
    pub reason: Option<&'static str>,
    pub closure: Kind,
}

struct Decided {
    reason: ClosureReason,
    threads_code: Option<&'static str>,
}

fn failed_by(code: &'static str) -> Result<Decided, EngineError> {
    Ok(Decided {
        reason: ClosureReason::failed(code).map_err(corrupt)?,
        threads_code: Some(code),
    })
}

fn requested(closing: &Closing) -> Result<Decided, EngineError> {
    if let Some(named) = &closing.named {
        return Ok(Decided {
            reason: named.clone(),
            threads_code: None,
        });
    }
    match closing.kind {
        CloseKind::Complete => Ok(Decided {
            reason: ClosureReason::completed(),
            threads_code: None,
        }),
        CloseKind::Cancel => Ok(Decided {
            reason: ClosureReason::cancelled(),
            threads_code: None,
        }),
        CloseKind::Watch => Ok(Decided {
            reason: ClosureReason::interrupted(),
            threads_code: Some(reason::STREAM_IDLE),
        }),
        CloseKind::Fail => failed_by(reason::FAILED_BY_WRITER),
    }
}

pub fn close(mut judged: Judged, closing: &Closing) -> Result<Closed, EngineError> {
    let closure_id = Uuid::now_v7();
    let interruption = closing.requested().interruption();
    let mut was_producing = false;
    for item in &mut judged.items {
        if let JudgedItem::Response(draft) = item
            && matches!(draft.phase(), DraftPhase::Producing { .. })
        {
            was_producing = true;
            let stop = ResponseDelta::new(
                draft.turn(),
                draft.entry(),
                DeltaOp::response_interrupted(interruption.clone()),
            );
            draft.apply(&stop).map_err(corrupt)?;
        }
    }
    let wanted = requested(closing)?;
    let decided = if let Some(refusal) = &judged.refusal {
        failed_by(refusal.code)?
    } else if closing.requested() == Kind::Completed {
        if was_producing {
            failed_by(reason::RESPONSE_STILL_PRODUCING)?
        } else {
            let completed = closure_entry(closure_id, wanted.reason.clone())?;
            match judged.turn.would_refuse(&completed) {
                None => wanted,
                Some(error) => failed_by(MessageInvalidError::of_library(error.code()).as_str())?,
            }
        }
    } else {
        wanted
    };
    let closure = Kind::of(&decided.reason).unwrap_or(Kind::Failed);
    judged
        .turn
        .push_entry(closure_entry(closure_id, decided.reason)?)
        .map_err(corrupt)?;
    Ok(Closed {
        status: closure.status(),
        content: judged.content()?,
        reason: decided.threads_code,
        closure,
    })
}

pub fn closure_entry(id: Uuid, reason: ClosureReason) -> Result<TurnEntry, EngineError> {
    Ok(TurnClosure::new(EntryId::from_uuid(id).map_err(corrupt)?, reason).into())
}

#[cfg(test)]
#[path = "close_tests.rs"]
mod tests;
