//! The vocabulary of a `MESSAGE_INVALID` refusal's `error` param: why Threads
//! refused a message's content. Most codes are the conversation model's own
//! refusals ([`MessageInvalidError::of_library`]), one per `MessageError` of
//! `br-llm-messages`; the others are Threads' rules on its decomposition of a
//! message (a block kind its place does not admit, a parent of another thread,
//! …). Structured detail rides beside it in the refusal's `params`
//! (`block_id`, `item_index`, `tool_call_id`, `entry_id`, `field`).

use br_llm_messages::MessageError;

use crate::wire::wire_enum;

wire_enum! {
    /// Why a message's content was refused. The vocabulary grows with the
    /// library: a consumer matching it keeps a fallback arm.
    #[non_exhaustive]
    pub enum MessageInvalidError {
        // ── the conversation model's refusals ───────────────────────────────
        Blank => "blank",
        InvalidToolName => "invalid_tool_name",
        InvalidBase64 => "invalid_base64",
        EmptyStep => "empty_step",
        ToolCallNotAtTail => "tool_call_not_at_tail",
        AwaitingToolResultsWithoutCalls => "awaiting_tool_results_without_calls",
        ToolCallsWithoutAwaiting => "tool_calls_without_awaiting",
        DuplicateToolCallId => "duplicate_tool_call_id",
        DuplicateToolCallIdInTurn => "duplicate_tool_call_id_in_turn",
        DuplicateToolCallIdInConversation => "duplicate_tool_call_id_in_conversation",
        EmptyUserContent => "empty_user_content",
        DuplicateToolResultId => "duplicate_tool_result_id",
        ToolResultNotPending => "tool_result_not_pending",
        ToolNameMismatch => "tool_name_mismatch",
        EmptyToolResults => "empty_tool_results",
        TurnNotAwaitingResults => "turn_not_awaiting_results",
        TurnNotAwaitingStep => "turn_not_awaiting_step",
        TurnMustStartWithStep => "turn_must_start_with_step",
        DuplicateEntryId => "duplicate_entry_id",
        TurnNotFound => "turn_not_found",
        SchemaMismatch => "schema_mismatch",
        TurnAwaitingResults => "turn_awaiting_results",
        RenderStartsWithAssistant => "render_starts_with_assistant",
        EmptyRender => "empty_render",
        NoOpenBlock => "no_open_block",
        BlockStillOpen => "block_still_open",
        UnexpectedBlockIndex => "unexpected_block_index",
        BlockKindMismatch => "block_kind_mismatch",
        ToolCallNeedsStart => "tool_call_needs_start",
        EventAfterFinish => "event_after_finish",
        MissingFinish => "missing_finish",
        InvalidJson => "invalid_json",
        WireListEmpty => "wire_list_empty",
        WireListNotStartingWithUser => "wire_list_not_starting_with_user",
        WireToolCallUnanswered => "wire_tool_call_unanswered",
        WireToolCallAnsweredTwice => "wire_tool_call_answered_twice",
        WireToolResultUnmatched => "wire_tool_result_unmatched",
        WireToolCallIdRepeated => "wire_tool_call_id_repeated",
        /// A refusal of a later library version, not named here yet.
        LibraryRefusal => "library_refusal",
        // ── Threads' own rules ──────────────────────────────────────────────
        /// A block of a kind its place does not admit (a tool result in a user
        /// input, a thinking in a tool results item, …).
        BlockKindNotAllowed => "block_kind_not_allowed",
        /// A library block whose content is not the library's JSON for its
        /// kind, nor a value the library names a refusal for.
        BlockContentInvalid => "block_content_invalid",
        /// An `ARTIFACT` block in a user message: only runners and AIs produce
        /// artifacts.
        ArtifactInUserMessage => "artifact_in_user_message",
        /// An AI turn given whole left awaiting tool results.
        TurnAwaitingToolResults => "turn_awaiting_tool_results",
        /// An AI author whose id or name is blank.
        AuthorInvalid => "author_invalid",
        /// The parent is not a message of the thread.
        ParentNotInThread => "parent_not_in_thread",
    }
}

impl MessageInvalidError {
    /// The code of a refusal of the conversation model. A refusal a later
    /// library adds is [`Self::LibraryRefusal`] until it is named here: on
    /// every library upgrade, its changelog's new `MessageError` variants get
    /// a code.
    pub fn of_library(error: &MessageError) -> Self {
        match error {
            MessageError::Blank { .. } => Self::Blank,
            MessageError::InvalidToolName => Self::InvalidToolName,
            MessageError::InvalidBase64 => Self::InvalidBase64,
            MessageError::EmptyStep => Self::EmptyStep,
            MessageError::ToolCallNotAtTail => Self::ToolCallNotAtTail,
            MessageError::AwaitingToolResultsWithoutCalls => Self::AwaitingToolResultsWithoutCalls,
            MessageError::ToolCallsWithoutAwaiting => Self::ToolCallsWithoutAwaiting,
            MessageError::DuplicateToolCallId { .. } => Self::DuplicateToolCallId,
            MessageError::DuplicateToolCallIdInTurn { .. } => Self::DuplicateToolCallIdInTurn,
            MessageError::DuplicateToolCallIdInConversation { .. } => {
                Self::DuplicateToolCallIdInConversation
            }
            MessageError::EmptyUserContent => Self::EmptyUserContent,
            MessageError::DuplicateToolResultId { .. } => Self::DuplicateToolResultId,
            MessageError::ToolResultNotPending { .. } => Self::ToolResultNotPending,
            MessageError::ToolNameMismatch { .. } => Self::ToolNameMismatch,
            MessageError::EmptyToolResults => Self::EmptyToolResults,
            MessageError::TurnNotAwaitingResults { .. } => Self::TurnNotAwaitingResults,
            MessageError::TurnNotAwaitingStep { .. } => Self::TurnNotAwaitingStep,
            MessageError::TurnMustStartWithStep => Self::TurnMustStartWithStep,
            MessageError::DuplicateEntryId { .. } => Self::DuplicateEntryId,
            MessageError::TurnNotFound { .. } => Self::TurnNotFound,
            MessageError::SchemaMismatch { .. } => Self::SchemaMismatch,
            MessageError::TurnAwaitingResults => Self::TurnAwaitingResults,
            MessageError::RenderStartsWithAssistant => Self::RenderStartsWithAssistant,
            MessageError::EmptyRender => Self::EmptyRender,
            MessageError::NoOpenBlock { .. } => Self::NoOpenBlock,
            MessageError::BlockStillOpen { .. } => Self::BlockStillOpen,
            MessageError::UnexpectedBlockIndex { .. } => Self::UnexpectedBlockIndex,
            MessageError::BlockKindMismatch { .. } => Self::BlockKindMismatch,
            MessageError::ToolCallNeedsStart => Self::ToolCallNeedsStart,
            MessageError::EventAfterFinish => Self::EventAfterFinish,
            MessageError::MissingFinish => Self::MissingFinish,
            MessageError::InvalidJson { .. } => Self::InvalidJson,
            MessageError::WireListEmpty => Self::WireListEmpty,
            MessageError::WireListNotStartingWithUser { .. } => Self::WireListNotStartingWithUser,
            MessageError::WireToolCallUnanswered { .. } => Self::WireToolCallUnanswered,
            MessageError::WireToolCallAnsweredTwice { .. } => Self::WireToolCallAnsweredTwice,
            MessageError::WireToolResultUnmatched { .. } => Self::WireToolResultUnmatched,
            MessageError::WireToolCallIdRepeated { .. } => Self::WireToolCallIdRepeated,
            _ => Self::LibraryRefusal,
        }
    }
}

#[cfg(test)]
mod tests {
    use br_llm_messages::{ToolCallId, ToolName, TurnId, TurnStateLabel};

    use super::*;

    /// Every refusal of `br-llm-messages` v0.5.0, each with the code it
    /// publishes: the table is the contract.
    #[test]
    fn every_refusal_of_the_library_has_its_published_code() {
        let id = || ToolCallId::new("c1").unwrap();
        let name = |name: &str| ToolName::new(name).unwrap();
        let table: Vec<(MessageError, &str)> = vec![
            (MessageError::Blank { field: "text" }, "blank"),
            (MessageError::InvalidToolName, "invalid_tool_name"),
            (MessageError::InvalidBase64, "invalid_base64"),
            (MessageError::EmptyStep, "empty_step"),
            (MessageError::ToolCallNotAtTail, "tool_call_not_at_tail"),
            (
                MessageError::AwaitingToolResultsWithoutCalls,
                "awaiting_tool_results_without_calls",
            ),
            (
                MessageError::ToolCallsWithoutAwaiting,
                "tool_calls_without_awaiting",
            ),
            (
                MessageError::DuplicateToolCallId { id: id() },
                "duplicate_tool_call_id",
            ),
            (
                MessageError::DuplicateToolCallIdInTurn { id: id() },
                "duplicate_tool_call_id_in_turn",
            ),
            (
                MessageError::DuplicateToolCallIdInConversation { id: id() },
                "duplicate_tool_call_id_in_conversation",
            ),
            (MessageError::EmptyUserContent, "empty_user_content"),
            (
                MessageError::DuplicateToolResultId { id: id() },
                "duplicate_tool_result_id",
            ),
            (
                MessageError::ToolResultNotPending {
                    given: id(),
                    pending: Vec::new(),
                },
                "tool_result_not_pending",
            ),
            (
                MessageError::ToolNameMismatch {
                    id: id(),
                    call: name("search"),
                    result: name("fetch"),
                },
                "tool_name_mismatch",
            ),
            (MessageError::EmptyToolResults, "empty_tool_results"),
            (
                MessageError::TurnNotAwaitingResults {
                    state: TurnStateLabel::Finished,
                },
                "turn_not_awaiting_results",
            ),
            (
                MessageError::TurnNotAwaitingStep {
                    state: TurnStateLabel::Finished,
                },
                "turn_not_awaiting_step",
            ),
            (
                MessageError::TurnMustStartWithStep,
                "turn_must_start_with_step",
            ),
            (
                MessageError::DuplicateEntryId { id: "m1".into() },
                "duplicate_entry_id",
            ),
            (
                MessageError::TurnNotFound {
                    id: TurnId::new("t1").unwrap(),
                },
                "turn_not_found",
            ),
            (
                MessageError::SchemaMismatch {
                    found: "br-llm-messages/2".into(),
                },
                "schema_mismatch",
            ),
            (MessageError::TurnAwaitingResults, "turn_awaiting_results"),
            (
                MessageError::RenderStartsWithAssistant,
                "render_starts_with_assistant",
            ),
            (MessageError::EmptyRender, "empty_render"),
            (MessageError::NoOpenBlock { index: 0 }, "no_open_block"),
            (
                MessageError::BlockStillOpen { index: 0 },
                "block_still_open",
            ),
            (
                MessageError::UnexpectedBlockIndex {
                    expected: 0,
                    given: 1,
                },
                "unexpected_block_index",
            ),
            (
                MessageError::BlockKindMismatch { index: 0 },
                "block_kind_mismatch",
            ),
            (MessageError::ToolCallNeedsStart, "tool_call_needs_start"),
            (MessageError::EventAfterFinish, "event_after_finish"),
            (MessageError::MissingFinish, "missing_finish"),
            (
                MessageError::InvalidJson {
                    field: "structured",
                    message: "eof".into(),
                },
                "invalid_json",
            ),
            (MessageError::WireListEmpty, "wire_list_empty"),
            (
                MessageError::WireListNotStartingWithUser { message: 0 },
                "wire_list_not_starting_with_user",
            ),
            (
                MessageError::WireToolCallUnanswered {
                    message: 1,
                    block: 0,
                    id: id(),
                },
                "wire_tool_call_unanswered",
            ),
            (
                MessageError::WireToolCallAnsweredTwice {
                    message: 2,
                    block: 1,
                    id: id(),
                },
                "wire_tool_call_answered_twice",
            ),
            (
                MessageError::WireToolResultUnmatched {
                    message: 2,
                    block: 0,
                    id: id(),
                },
                "wire_tool_result_unmatched",
            ),
            (
                MessageError::WireToolCallIdRepeated {
                    message: 3,
                    block: 0,
                    id: id(),
                },
                "wire_tool_call_id_repeated",
            ),
        ];
        assert_eq!(table.len(), 38, "the 38 refusals of br-llm-messages v0.5.0");
        let mut codes = std::collections::BTreeSet::new();
        for (error, code) in &table {
            let published = MessageInvalidError::of_library(error);
            assert_eq!(published.as_str(), *code, "{error:?}");
            assert_ne!(published, MessageInvalidError::LibraryRefusal, "{error:?}");
            assert!(codes.insert(*code), "{code} is published once");
        }
    }

    #[test]
    fn every_code_is_snake_case_and_maps_back() {
        for code in MessageInvalidError::ALL {
            let literal = code.as_str();
            assert!(
                literal
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                "{literal}"
            );
            assert_eq!(MessageInvalidError::parse(literal), Ok(*code));
            assert_eq!(
                serde_json::to_value(code).unwrap(),
                serde_json::Value::String(literal.to_owned())
            );
        }
        assert!(MessageInvalidError::parse("BLANK").is_err());
    }
}
