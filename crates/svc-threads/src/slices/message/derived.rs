//! The ids a conversion from the 0.4 form creates — the entries and blocks the
//! 0.4 form had no id for (`message.store` v1, and the upgrade), and the
//! closure of a message the upgrade converts — derived deterministically, so
//! that a v1 command received again, or an upgrade resumed after a crash,
//! derives the same id and never a second object.
//!
//! Only a conversion derives. The closure a close or a whole AI turn gets at
//! runtime is minted fresh ([`super::draft::close`]).
//!
//! A derived id is a UUIDv7 that keeps its source id's 48-bit timestamp (so it
//! sorts beside its source) and takes its random bits from
//! `SHA-256(source id ‖ role)`, its version and variant bits set.

use sha2::{Digest, Sha256};
use uuid::Uuid;

/// The role of the closure of a message the upgrade converts.
pub const CLOSURE: &str = "closure";
/// The role of the human message of a 0.4 user message.
pub const HUMAN_MESSAGE: &str = "human_message";

/// The role of the entry made of the 0.4 item at `index`.
pub fn item(index: usize) -> String {
    format!("item:{index}")
}

/// The role of the child block at `index` of a 0.4 tool result.
pub fn result_content(index: usize) -> String {
    format!("content:{index}")
}

/// The UUIDv7 derived from `source` for `role`.
pub fn derive(source: Uuid, role: &str) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(source.as_bytes());
    hasher.update(role.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&source.as_bytes()[..6]);
    bytes[6..].copy_from_slice(&digest[..10]);
    bytes[6] = 0x70 | (bytes[6] & 0x0f);
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_derived_id_is_a_uuidv7_keeping_its_sources_timestamp() {
        let source = Uuid::now_v7();
        let derived = derive(source, CLOSURE);
        assert_eq!(derived.get_version_num(), 7);
        assert_eq!(derived.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(&derived.as_bytes()[..6], &source.as_bytes()[..6]);
        contract_threads::llm::EntryId::from_uuid(derived).expect("the library accepts it");
    }

    #[test]
    fn the_same_source_and_role_always_derive_the_same_id_and_any_other_another() {
        let source = Uuid::now_v7();
        assert_eq!(derive(source, CLOSURE), derive(source, CLOSURE));
        assert_ne!(derive(source, CLOSURE), derive(source, HUMAN_MESSAGE));
        assert_ne!(derive(source, &item(0)), derive(source, &item(1)));
        assert_ne!(derive(source, CLOSURE), derive(Uuid::now_v7(), CLOSURE));
        assert_ne!(derive(source, CLOSURE), source);
    }

    #[test]
    fn the_derivation_is_pinned() {
        let source = Uuid::parse_str("0190f0a2-1c2b-7d3e-8f40-123456789abc").unwrap();
        let derived = derive(source, CLOSURE);
        assert_eq!(&derived.to_string()[..13], "0190f0a2-1c2b");
        assert_eq!(derived, derive(source, "closure"));
    }
}
