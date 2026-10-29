use br_llm_messages::{
    ClosureReason, Error, OpaqueJson, PlainBlock, ResponseDelta, Turn, TurnEntry,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LibraryJson(serde_json::Value);

impl LibraryJson {
    pub fn of<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        serde_json::to_value(value).map(Self)
    }

    pub fn from_json(json: serde_json::Value) -> Self {
        Self(json)
    }

    pub fn as_json(&self) -> &serde_json::Value {
        &self.0
    }

    pub fn into_json(self) -> serde_json::Value {
        self.0
    }

    fn text(&self) -> String {
        serde_json::to_string(&self.0).unwrap_or_default()
    }

    pub fn to_turn(&self) -> Result<Turn, Error> {
        Turn::from_json(&self.text())
    }

    pub fn to_entry(&self) -> Result<TurnEntry, Error> {
        TurnEntry::from_json(&self.text())
    }

    pub fn to_closure_reason(&self) -> Result<ClosureReason, Error> {
        let entry = serde_json::json!({
            "type": "turn_closure",
            "id": uuid::Uuid::now_v7(),
            "reason": self.0,
        });
        match LibraryJson::from_json(entry).to_entry()? {
            TurnEntry::TurnClosure(closure) => Ok(closure.reason().clone()),
            _ => ClosureReason::failed(""),
        }
    }

    pub fn to_plain_block(&self) -> Result<PlainBlock, Error> {
        PlainBlock::from_json(&self.text())
    }

    pub fn to_delta(&self) -> Result<ResponseDelta, Error> {
        ResponseDelta::from_json(&self.text())
    }

    pub fn to_opaque(&self) -> Result<OpaqueJson, Error> {
        OpaqueJson::parse(&self.text())
    }
}

#[cfg(test)]
mod tests {
    use br_llm_messages::{EntryId, HumanId, HumanMessage, InputTurn, TextBlock, TurnId, Uuid};

    use super::*;

    fn v7() -> Uuid {
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
