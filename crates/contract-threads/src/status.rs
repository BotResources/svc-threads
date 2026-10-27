//! The statuses and kinds the published language names.

use crate::wire::wire_enum;

wire_enum! {
    /// A thread's status. A thread is born `OPEN`; a terminal status never
    /// changes.
    pub enum ThreadStatus {
        Open => "OPEN",
        Completed => "COMPLETED",
        Failed => "FAILED",
        Cancelled => "CANCELLED",
    }
}

impl ThreadStatus {
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Open)
    }
}

wire_enum! {
    /// The terminal status a `thread.close` command asks for.
    pub enum ClosingStatus {
        Completed => "COMPLETED",
        Failed => "FAILED",
        Cancelled => "CANCELLED",
    }
}

impl From<ClosingStatus> for ThreadStatus {
    fn from(status: ClosingStatus) -> Self {
        match status {
            ClosingStatus::Completed => Self::Completed,
            ClosingStatus::Failed => Self::Failed,
            ClosingStatus::Cancelled => Self::Cancelled,
        }
    }
}

wire_enum! {
    /// A message's status. A user message is always stored `COMPLETED`; an AI
    /// message may be opened `STREAMING` and is then completed, failed or
    /// cancelled once, its turn closed with the library's closure of the same
    /// name. A `COMPLETED`, `FAILED` or `CANCELLED` message never changes,
    /// except that the author of a `COMPLETED` user message may edit it.
    pub enum MessageStatus {
        Streaming => "STREAMING",
        Completed => "COMPLETED",
        Failed => "FAILED",
        Cancelled => "CANCELLED",
    }
}

impl MessageStatus {
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Streaming)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_literal_maps_back_to_its_variant() {
        for status in ThreadStatus::ALL {
            assert_eq!(ThreadStatus::parse(status.as_str()), Ok(*status));
        }
        for status in MessageStatus::ALL {
            assert_eq!(MessageStatus::parse(status.as_str()), Ok(*status));
        }
    }

    #[test]
    fn an_unknown_literal_is_refused_never_defaulted() {
        assert!(ThreadStatus::parse("open").is_err());
        assert!(serde_json::from_str::<MessageStatus>("\"DONE\"").is_err());
        assert_eq!(
            MessageStatus::parse("DONE").unwrap_err().to_string(),
            "`DONE` is not a MessageStatus literal"
        );
    }

    #[test]
    fn statuses_travel_as_their_literal() {
        assert_eq!(
            serde_json::to_value(ThreadStatus::Cancelled).unwrap(),
            serde_json::json!("CANCELLED")
        );
        assert_eq!(
            ThreadStatus::from(ClosingStatus::Failed),
            ThreadStatus::Failed
        );
        assert!(!ThreadStatus::Open.is_terminal());
        assert!(MessageStatus::Cancelled.is_terminal());
        assert!(!MessageStatus::Streaming.is_terminal());
    }
}
