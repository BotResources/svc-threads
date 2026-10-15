//! The reason codes a refusal event carries. A code is the contract: the
//! commanding service localizes it, Threads renders no sentence. Structured
//! detail rides beside it in the refusal's `params`.

use crate::wire::wire_enum;

wire_enum! {
    /// Why a command was refused. Codes marked *proposed* are not named by the
    /// functional specification (it describes the refusal without a code) and
    /// await ratification.
    pub enum ReasonCode {
        /// The id a creation names is already held.
        KeyReused => "KEY_REUSED",
        /// An owner or a member is not a known user still in the roster.
        UserUnknown => "USER_UNKNOWN",
        /// The owner cannot be removed from the members.
        OwnerNotRemovable => "OWNER_NOT_REMOVABLE",
        /// A child thread's owner is not a member of the parent thread.
        OwnerNotParentMember => "OWNER_NOT_PARENT_MEMBER",
        /// *Proposed*: a child thread names its parent without the block it was
        /// started from.
        OriginBlockRequired => "ORIGIN_BLOCK_REQUIRED",
        /// *Proposed*: an id the caller mints that is not a UUIDv7.
        IdNotUuidV7 => "ID_NOT_UUIDV7",
        /// *Proposed*: a resource key that is blank.
        ResourceKeyInvalid => "RESOURCE_KEY_INVALID",
        /// The thread is completed, failed or cancelled.
        ThreadNotOpen => "THREAD_NOT_OPEN",
        /// No thread holds the id the command names.
        ThreadNotFound => "THREAD_NOT_FOUND",
        /// A title of no character.
        TitleEmpty => "TITLE_EMPTY",
        /// A title longer than 200 characters.
        TitleTooLong => "TITLE_TOO_LONG",
        /// No message of the thread holds the id the command names.
        MessageNotFound => "MESSAGE_NOT_FOUND",
        /// The conversation model refuses the message, or a step of its stream.
        MessageInvalid => "MESSAGE_INVALID",
        /// An `ARTIFACT` block names an artifact of another thread, or one
        /// whose bytes are absent or of another digest.
        ArtifactNotReady => "ARTIFACT_NOT_READY",
        /// The acting user of a deletion is not the message's author.
        NotTheAuthor => "NOT_THE_AUTHOR",
        /// A user author who is not a member of the thread.
        NotAMember => "NOT_A_MEMBER",
        /// Completing, failing or cancelling a message that is not `STREAMING`.
        MessageNotStreaming => "MESSAGE_NOT_STREAMING",
        /// A seal whose last chunk or digest differs from what Threads received.
        StreamMismatch => "STREAM_MISMATCH",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_is_screaming_snake_case_and_maps_back() {
        for code in ReasonCode::ALL {
            let literal = code.as_str();
            assert!(
                literal
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
                "{literal}"
            );
            assert_eq!(ReasonCode::parse(literal), Ok(*code));
        }
    }

    #[test]
    fn the_codes_the_specification_names_are_all_there() {
        for named in [
            "KEY_REUSED",
            "USER_UNKNOWN",
            "OWNER_NOT_REMOVABLE",
            "OWNER_NOT_PARENT_MEMBER",
            "THREAD_NOT_OPEN",
            "THREAD_NOT_FOUND",
            "TITLE_EMPTY",
            "TITLE_TOO_LONG",
            "MESSAGE_NOT_FOUND",
            "MESSAGE_INVALID",
            "ARTIFACT_NOT_READY",
            "NOT_THE_AUTHOR",
            "NOT_A_MEMBER",
            "MESSAGE_NOT_STREAMING",
            "STREAM_MISMATCH",
        ] {
            assert!(ReasonCode::parse(named).is_ok(), "{named}");
        }
    }
}
