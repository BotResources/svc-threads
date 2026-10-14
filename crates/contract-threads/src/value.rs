//! The value objects whose rule the published language states. Commands carry
//! the raw value (so that a value out of its domain is answered with a refusal,
//! never dead-lettered); Threads, and a commander that wants to check before it
//! sends, parse it here.

use std::fmt;

/// A thread's title: at most [`ThreadTitle::MAX_CHARS`] characters
/// (`TITLE_TOO_LONG`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ThreadTitle(String);

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
    /// Counted in Unicode scalar values, as the specification counts
    /// characters.
    pub const MAX_CHARS: usize = 200;

    pub fn parse(raw: impl Into<String>) -> Result<Self, TitleTooLong> {
        let raw = raw.into();
        let chars = raw.chars().count();
        if chars > Self::MAX_CHARS {
            return Err(TitleTooLong { chars });
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
            Err(TitleTooLong { chars: 201 })
        );
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
