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
    /// A command whose envelope carries no metadata, hence no actor to
    /// attribute its facts to: not a contract command, dead-lettered.
    #[error("COMMAND_UNATTRIBUTED")]
    Unattributed,
    #[error("the store or the engine failed")]
    Engine(#[source] EngineError),
}

impl MutationFault for Fault {
    fn reason(&self) -> Option<Reason> {
        match self {
            Self::Refused(reason) => Some(*reason),
            Self::Unattributed | Self::Engine(_) => None,
        }
    }
}

/// The SQLSTATE of a unique violation.
const UNIQUE_VIOLATION: &str = "23505";

/// Whether a database fault is a key two transactions raced for: a command
/// judged a key free, and a concurrent one took it before the commit.
fn raced_for_a_key(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db) if db.code().as_deref() == Some(UNIQUE_VIOLATION))
}

impl ReactionError for Fault {
    /// A refusal, a command without attribution and a database fault the data
    /// itself causes are terminal — but a unique violation is retried: it is
    /// the loser of a race past a `KEY_REUSED` check (two stores of one
    /// message, two contents declaring one id), and the redelivery is judged
    /// anew on the winner's committed state, so that it is answered
    /// `KEY_REUSED` instead of dead-lettered unanswered.
    fn disposition(&self) -> Disposition {
        match self {
            Self::Refused(_) | Self::Unattributed => Disposition::Terminal,
            Self::Engine(EngineError::Db(db)) if raced_for_a_key(db) => Disposition::Retry,
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
        assert_eq!(Fault::Unattributed.disposition(), Disposition::Terminal);
    }

    /// A database error as Postgres reports one, by its SQLSTATE.
    #[derive(Debug)]
    struct Reported(&'static str);

    impl std::fmt::Display for Reported {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SQLSTATE {}", self.0)
        }
    }

    impl std::error::Error for Reported {}

    impl sqlx::error::DatabaseError for Reported {
        fn message(&self) -> &str {
            self.0
        }

        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(self.0.into())
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn db(code: &'static str) -> Fault {
        Fault::from(sqlx::Error::Database(Box::new(Reported(code))))
    }

    #[test]
    fn a_race_for_a_key_is_retried_and_any_other_data_fault_is_terminal() {
        assert_eq!(db("23505").disposition(), Disposition::Retry);
        assert_eq!(db("23514").disposition(), Disposition::Terminal);
        assert_eq!(db("22P05").disposition(), Disposition::Terminal);
        assert_eq!(db("40001").disposition(), Disposition::Retry);
    }
}
