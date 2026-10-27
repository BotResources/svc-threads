//! The vocabulary of a `MESSAGE_INVALID` refusal's `error` param, and of the
//! reason a stream failed: why Threads refused a message's content.
//!
//! Most codes are the conversation model's own (`br-llm-messages` 2.0
//! [`ErrorCode`]), published under the library's name: the refusal then also
//! names the library's `object_id` and `location` when the library gives them.
//! The others are Threads' rules on a message (a user message holding an
//! annotation, a parent of another thread, …) and on its stream.

use br_llm_messages::ErrorCode;

use crate::wire::wire_enum;

wire_enum! {
    /// Why a message's content was refused. The vocabulary grows with the
    /// library: a consumer matching it keeps a fallback arm.
    #[non_exhaustive]
    pub enum MessageInvalidError {
        // ── the conversation model's refusals (br-llm-messages 2.0) ─────────
        InvalidIdentity => "invalid_identity",
        ReusedIdentity => "reused_identity",
        InvalidAuthor => "invalid_author",
        AuthorForbidden => "author_forbidden",
        InvalidContent => "invalid_content",
        DisallowedKind => "disallowed_kind",
        UnknownReference => "unknown_reference",
        DuplicateResult => "duplicate_result",
        MismatchedResult => "mismatched_result",
        EntryOutOfOrder => "entry_out_of_order",
        ClosedTurn => "closed_turn",
        UnresolvedExecution => "unresolved_execution",
        InvalidRecordStructure => "invalid_record_structure",
        MalformedJson => "malformed_json",
        SchemaMismatch => "schema_mismatch",
        VariantMismatch => "variant_mismatch",
        StreamTargetError => "stream_target_error",
        StreamPhaseError => "stream_phase_error",
        InconsistentFinalization => "inconsistent_finalization",
        /// A refusal of a later library version, not named here yet.
        LibraryRefusal => "library_refusal",
        // ── Threads' own rules on a message ─────────────────────────────────
        /// The turn's id is not the message's.
        TurnIdMismatch => "turn_id_mismatch",
        /// A user message whose turn is not an input turn holding one human
        /// message.
        NotAUserTurn => "not_a_user_turn",
        /// A user message holding an annotation.
        AnnotationInUserMessage => "annotation_in_user_message",
        /// An annotation after an entry the message does not hold.
        AnnotationEntryUnknown => "annotation_entry_unknown",
        /// An AI turn whose agent or author is not the one the command names.
        AuthorMismatch => "author_mismatch",
        /// An AI message whose turn is an input turn holding a human message:
        /// a human message travels in a user message only, whose author is a
        /// member.
        HumanMessageInAiMessage => "human_message_in_ai_message",
        /// A string holding U+0000, which the store cannot keep (Postgres
        /// refuses it in `text` and `jsonb`).
        NulCharacter => "nul_character",
        /// The parent is not a message of the thread.
        ParentNotInThread => "parent_not_in_thread",
        // ── Threads' own rules on a stream ──────────────────────────────────
        /// A chunk the contract cannot read.
        ChunkUnreadable => "chunk_unreadable",
        /// A model response pushed as a whole entry: a response streams as
        /// deltas.
        ResponseInEntryChunk => "response_in_entry_chunk",
        /// An entry, or another response, pushed while a response is still
        /// produced.
        ResponseStillProducing => "response_still_producing",
        /// A closure pushed on the stream: closing the turn is Threads'.
        ClosureByWriter => "closure_by_writer",
        // ── the deprecated v1 forms ─────────────────────────────────────────
        /// A `STREAMING` opening by `message.store` v1: stream with v2.
        StreamingV1 => "streaming_v1",
        /// Content of the 0.4 form the 2.0 model has no place for, or JSON
        /// that is not 0.4's.
        V1NotConvertible => "v1_not_convertible",
    }
}

impl MessageInvalidError {
    /// The code of a refusal of the conversation model. A code a later library
    /// adds is [`Self::LibraryRefusal`] until it is named here: on every
    /// library upgrade, its changelog's new `ErrorCode` variants get a code.
    pub fn of_library(code: ErrorCode) -> Self {
        match code {
            ErrorCode::InvalidIdentity => Self::InvalidIdentity,
            ErrorCode::ReusedIdentity => Self::ReusedIdentity,
            ErrorCode::InvalidAuthor => Self::InvalidAuthor,
            ErrorCode::AuthorForbidden => Self::AuthorForbidden,
            ErrorCode::InvalidContent => Self::InvalidContent,
            ErrorCode::DisallowedKind => Self::DisallowedKind,
            ErrorCode::UnknownReference => Self::UnknownReference,
            ErrorCode::DuplicateResult => Self::DuplicateResult,
            ErrorCode::MismatchedResult => Self::MismatchedResult,
            ErrorCode::EntryOutOfOrder => Self::EntryOutOfOrder,
            ErrorCode::ClosedTurn => Self::ClosedTurn,
            ErrorCode::UnresolvedExecution => Self::UnresolvedExecution,
            ErrorCode::InvalidRecordStructure => Self::InvalidRecordStructure,
            ErrorCode::MalformedJson => Self::MalformedJson,
            ErrorCode::SchemaMismatch => Self::SchemaMismatch,
            ErrorCode::VariantMismatch => Self::VariantMismatch,
            ErrorCode::StreamTargetError => Self::StreamTargetError,
            ErrorCode::StreamPhaseError => Self::StreamPhaseError,
            ErrorCode::InconsistentFinalization => Self::InconsistentFinalization,
            // `ErrorCode` is non-exhaustive: a code of a later library.
            _ => Self::LibraryRefusal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every error code of `br-llm-messages` v2.0.0 is published under the
    /// library's own name: the table is the contract.
    #[test]
    fn every_code_of_the_library_is_published_under_its_own_name() {
        assert_eq!(
            ErrorCode::ALL.len(),
            19,
            "the 19 codes of br-llm-messages v2.0.0"
        );
        let mut codes = std::collections::BTreeSet::new();
        for code in ErrorCode::ALL {
            let published = MessageInvalidError::of_library(*code);
            assert_eq!(published.as_str(), code.as_str(), "{code:?}");
            assert!(codes.insert(published), "{code:?} is published once");
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
