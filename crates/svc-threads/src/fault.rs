//! What a Threads gesture or reaction answers when it does not complete: a
//! refusal carrying its reason code, or a fault of the store or the engine,
//! logged with its cause and answered `INTERNAL` (a reaction retries it).

use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::inbound::{Disposition, ReactionError, sqlx_is_terminal};
use service_engine::pipeline::MutationFault;

#[derive(Debug, thiserror::Error)]
pub enum Fault {
    #[error("{}", .0.code())]
    Refused(Reason),
    /// A command whose envelope names no service account: Threads answers the
    /// commanding services only, and dead-letters anything else.
    #[error("a command reached Threads from something other than a service account")]
    NotACommandingService,
    #[error("the store or the engine failed")]
    Engine(#[source] EngineError),
}

impl MutationFault for Fault {
    fn reason(&self) -> Option<Reason> {
        match self {
            Self::Refused(reason) => Some(*reason),
            Self::NotACommandingService | Self::Engine(_) => None,
        }
    }
}

impl ReactionError for Fault {
    fn disposition(&self) -> Disposition {
        match self {
            Self::Refused(_) | Self::NotACommandingService => Disposition::Terminal,
            Self::Engine(EngineError::Db(db)) if sqlx_is_terminal(db) => Disposition::Terminal,
            Self::Engine(_) => Disposition::Retry,
        }
    }
}

impl From<Reason> for Fault {
    fn from(reason: Reason) -> Self {
        Self::Refused(reason)
    }
}

impl From<EngineError> for Fault {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

impl From<sqlx::Error> for Fault {
    fn from(error: sqlx::Error) -> Self {
        Self::Engine(EngineError::from(error))
    }
}

/// A stored value outside its typed domain: read back, never trusted. A fault
/// of the store, answered `INTERNAL`, its cause logged.
pub fn corrupt<E: std::error::Error + Send + Sync + 'static>(error: E) -> EngineError {
    EngineError::Service(Box::new(error))
}

/// A value Threads could not encode: a fault, never a refusal.
pub fn encode(what: &'static str, source: serde_json::Error) -> EngineError {
    EngineError::Encode { what, source }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_carries_its_code_and_a_fault_none() {
        let refused = Fault::from(crate::access::THREAD_NOT_FOUND);
        assert_eq!(refused.reason(), Some(crate::access::THREAD_NOT_FOUND));
        assert_eq!(refused.to_string(), "THREAD_NOT_FOUND");
        assert_eq!(refused.disposition(), Disposition::Terminal);
        let fault = Fault::Engine(EngineError::Config("absent".into()));
        assert_eq!(fault.reason(), None);
        assert_eq!(fault.disposition(), Disposition::Retry);
        assert_eq!(
            Fault::NotACommandingService.disposition(),
            Disposition::Terminal
        );
    }
}
