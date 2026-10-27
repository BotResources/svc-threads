//! Library content as it travels: the library's own JSON, carried as such and
//! judged by Threads with the library's rules when it arrives.
//!
//! A command or a chunk whose library part the library refuses still decodes,
//! so that Threads answers it (`MESSAGE_INVALID` naming the library's error
//! code, or a stream failed there) instead of dead-lettering it.

use br_llm_messages::{Error, OpaqueJson, PlainBlock, ResponseDelta, Turn, TurnEntry};
use serde::{Deserialize, Serialize};

/// The library's JSON for one of its values: a turn, an entry, a plain block,
/// a response delta.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LibraryJson(serde_json::Value);

impl LibraryJson {
    /// The JSON the library writes for `value`.
    pub fn of<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        serde_json::to_value(value).map(Self)
    }

    /// Raw JSON, as a sender that does not use the library writes it.
    pub fn from_json(json: serde_json::Value) -> Self {
        Self(json)
    }

    pub fn as_json(&self) -> &serde_json::Value {
        &self.0
    }

    pub fn into_json(self) -> serde_json::Value {
        self.0
    }

    /// The text the library parses: the library reads its values from text, so
    /// that its own rules on the JSON it is given (an explicit `null`, a value
    /// of the wrong shape) are the ones applied.
    ///
    /// What the library judges is the JSON value this crate decoded, not the
    /// sender's bytes: a key the sender wrote twice is already collapsed to its
    /// last value, and a number `serde_json` cannot hold already failed the
    /// decode of the whole command or chunk. Encoding a JSON value never fails.
    fn text(&self) -> String {
        serde_json::to_string(&self.0).unwrap_or_default()
    }

    /// Read as a turn, by the library's rules.
    pub fn to_turn(&self) -> Result<Turn, Error> {
        Turn::from_json(&self.text())
    }

    /// Read as an entry of an AI turn, by the library's rules.
    pub fn to_entry(&self) -> Result<TurnEntry, Error> {
        TurnEntry::from_json(&self.text())
    }

    /// Read as a plain block (text, image, structured content), by the
    /// library's rules.
    pub fn to_plain_block(&self) -> Result<PlainBlock, Error> {
        PlainBlock::from_json(&self.text())
    }

    /// Read as a response delta, by the library's rules.
    pub fn to_delta(&self) -> Result<ResponseDelta, Error> {
        ResponseDelta::from_json(&self.text())
    }

    /// The library's opaque form of the JSON.
    pub fn to_opaque(&self) -> Result<OpaqueJson, Error> {
        OpaqueJson::parse(&self.text())
    }
}

#[cfg(test)]
mod tests {
    use br_llm_messages::{EntryId, HumanId, HumanMessage, InputTurn, TextBlock, TurnId, Uuid};

    use super::*;

    fn v7() -> Uuid {
        // A fixed UUIDv7: the test owns no clock.
        Uuid::parse_str("0190f0a2-1c2b-7d3e-8f40-123456789abc").unwrap()
    }

    #[test]
    fn a_turn_travels_as_the_librarys_json_and_reads_back() {
        let block = TextBlock::new(
            br_llm_messages::BlockId::from_uuid(Uuid::from_u128(v7().as_u128() + 1)).unwrap(),
            "hello",
        )
        .unwrap();
        let human = HumanMessage::new(
            EntryId::from_uuid(Uuid::from_u128(v7().as_u128() + 2)).unwrap(),
            HumanId::from_uuid(Uuid::from_u128(v7().as_u128() + 3)).unwrap(),
            vec![PlainBlock::Text(block)],
        )
        .unwrap();
        let turn: Turn = InputTurn::new(TurnId::from_uuid(v7()).unwrap(), vec![human.into()])
            .unwrap()
            .into();
        let json = LibraryJson::of(&turn).unwrap();
        assert_eq!(json.as_json()["type"], "input_turn");
        assert_eq!(json.to_turn().unwrap(), turn);
    }

    #[test]
    fn json_the_library_refuses_still_travels_and_is_refused_on_reading() {
        let json = LibraryJson::from_json(serde_json::json!({
            "type": "text", "id": v7(), "text": "hi", "observed_at": null
        }));
        let refused = json.to_plain_block().unwrap_err();
        assert_eq!(refused.code(), br_llm_messages::ErrorCode::InvalidContent);
    }
}
