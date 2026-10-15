//! The value objects whose rule the published language states. Commands carry
//! the raw value (so that a value out of its domain is answered with a refusal,
//! never dead-lettered); Threads, and a commander that wants to check before it
//! sends, parse it here.

use std::fmt;

use crate::ReasonCode;

/// A thread's title: from [`ThreadTitle::MIN_CHARS`] to
/// [`ThreadTitle::MAX_CHARS`] characters (`TITLE_EMPTY`, `TITLE_TOO_LONG`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ThreadTitle(String);

/// Why a raw title is not a [`ThreadTitle`]; it displays as its reason code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ThreadTitleError {
    /// A title of no character at all.
    #[error("TITLE_EMPTY")]
    Empty,
    /// A title longer than [`ThreadTitle::MAX_CHARS`] characters.
    #[error("TITLE_TOO_LONG")]
    TooLong(#[from] TitleTooLong),
}

impl ThreadTitleError {
    /// The reason code a refusal of this title carries.
    pub const fn reason_code(self) -> ReasonCode {
        match self {
            Self::Empty => ReasonCode::TitleEmpty,
            Self::TooLong(_) => ReasonCode::TitleTooLong,
        }
    }
}

/// A title longer than [`ThreadTitle::MAX_CHARS`] characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a title holds at most {} characters, {chars} given",
    ThreadTitle::MAX_CHARS
)]
pub struct TitleTooLong {
    pub chars: usize,
}

impl ThreadTitle {
    /// Both bounds count Unicode scalar values, as the specification counts
    /// characters.
    pub const MIN_CHARS: usize = 1;
    /// See [`ThreadTitle::MIN_CHARS`].
    pub const MAX_CHARS: usize = 200;

    /// The raw value as given: whitespace counts as characters and is kept.
    pub fn parse(raw: impl Into<String>) -> Result<Self, ThreadTitleError> {
        let raw = raw.into();
        let chars = raw.chars().count();
        if chars < Self::MIN_CHARS {
            return Err(ThreadTitleError::Empty);
        }
        if chars > Self::MAX_CHARS {
            return Err(TitleTooLong { chars }.into());
        }
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for ThreadTitle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The digest that seals a stream: the lowercase hex SHA-256 the engine's
/// accumulated lane computes over a stream's chunks ([`crate::seal_hash`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SealDigest(String);

/// A digest that is not 64 lowercase hexadecimal characters.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a seal digest is 64 lowercase hexadecimal characters")]
pub struct SealDigestError;

impl SealDigest {
    pub const LEN: usize = 64;

    pub fn parse(raw: &str) -> Result<Self, SealDigestError> {
        let well_formed = raw.len() == Self::LEN
            && raw
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if well_formed {
            Ok(Self(raw.to_owned()))
        } else {
            Err(SealDigestError)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SealDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_title_of_200_characters_is_kept_and_one_of_201_refused() {
        let at_bound = "é".repeat(200);
        assert_eq!(
            ThreadTitle::parse(at_bound.clone()).unwrap().as_str(),
            at_bound
        );
        assert_eq!(
            ThreadTitle::parse("é".repeat(201)),
            Err(ThreadTitleError::TooLong(TitleTooLong { chars: 201 }))
        );
        assert_eq!(
            ThreadTitle::parse("é".repeat(201))
                .unwrap_err()
                .reason_code(),
            ReasonCode::TitleTooLong
        );
    }

    #[test]
    fn an_empty_title_is_refused_title_empty_and_one_character_kept() {
        assert_eq!(ThreadTitle::parse(""), Err(ThreadTitleError::Empty));
        assert_eq!(
            ThreadTitleError::Empty.reason_code(),
            ReasonCode::TitleEmpty
        );
        assert_eq!(ThreadTitle::parse("é").unwrap().as_str(), "é");
        assert_eq!(ThreadTitle::parse(" ").unwrap().as_str(), " ");
    }

    #[test]
    fn a_digest_is_64_lowercase_hex_characters() {
        let digest = "a".repeat(64);
        assert_eq!(SealDigest::parse(&digest).unwrap().as_str(), digest);
        assert!(SealDigest::parse(&"A".repeat(64)).is_err());
        assert!(SealDigest::parse(&"a".repeat(63)).is_err());
        assert!(SealDigest::parse(&"g".repeat(64)).is_err());
    }
}
