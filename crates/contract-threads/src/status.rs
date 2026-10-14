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
    /// A message's status, and an item's, which follows its message's. A user
    /// message is always stored `COMPLETED`; an AI message may be opened
    /// `STREAMING` and is then completed, failed or cancelled once. A
    /// `COMPLETED`, `FAILED` or `CANCELLED` message never changes.
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

wire_enum! {
    /// The kind of a block. The first six are the library's blocks, whose
    /// content is the library's JSON; the last six are Threads' own, which show
    /// what happens around the AI and which the library's rules ignore.
    pub enum BlockKind {
        Text => "TEXT",
        Thinking => "THINKING",
        RedactedThinking => "REDACTED_THINKING",
        Structured => "STRUCTURED",
        ToolCall => "TOOL_CALL",
        ToolResult => "TOOL_RESULT",
        Artifact => "ARTIFACT",
        ThreadReference => "THREAD_REFERENCE",
        JobReference => "JOB_REFERENCE",
        Citation => "CITATION",
        Error => "ERROR",
        Warning => "WARNING",
    }
}

impl BlockKind {
    /// Whether the block belongs to the library's conversation: a runner
    /// receives it, the library's rules judge it.
    pub const fn is_library(self) -> bool {
        matches!(
            self,
            Self::Text
                | Self::Thinking
                | Self::RedactedThinking
                | Self::Structured
                | Self::ToolCall
                | Self::ToolResult
        )
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
        for kind in BlockKind::ALL {
            assert_eq!(BlockKind::parse(kind.as_str()), Ok(*kind));
        }
    }

    #[test]
    fn an_unknown_literal_is_refused_never_defaulted() {
        assert!(ThreadStatus::parse("open").is_err());
        assert!(serde_json::from_str::<MessageStatus>("\"DONE\"").is_err());
        assert_eq!(
            BlockKind::parse("IMAGE").unwrap_err().to_string(),
            "`IMAGE` is not a BlockKind literal"
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

    #[test]
    fn six_kinds_are_the_librarys_and_six_are_threads_own() {
        let library = BlockKind::ALL
            .iter()
            .filter(|kind| kind.is_library())
            .count();
        assert_eq!(library, 6);
        assert!(!BlockKind::Artifact.is_library());
        assert!(BlockKind::ToolResult.is_library());
    }
}
