//! A message's content as Threads holds it: its turn's entries and blocks as
//! the library's storage records (`br-llm-messages-records/1`), in the order the
//! library decomposes the turn, and its annotations after their entries.
//!
//! Threads keeps no decomposition of its own: a turn becomes rows through
//! `RecordSet::from_turn` and comes back through `RecordSet::into_turns`, so
//! what Threads stores is exactly what the library loads. The library judges
//! every turn before it is stored; its refusals are answered with its own error
//! code ([`refusal_of`]).

use std::collections::HashSet;

use contract_threads::llm::{
    BlockRecord, ContentBlockKind, EntryRecord, Error, ErrorCode, RecordSet, RepresentationState,
    Turn, TurnEntryKind, TurnRecord,
};
use contract_threads::{MessageInvalidError, PlacedAnnotation, ReasonCode};
use uuid::Uuid;

use crate::command::Refusal;

/// A message's content: its entries and blocks as records, its annotations.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Content {
    /// In the order of the turn.
    pub entries: Vec<EntryRecord>,
    /// In the order the library decomposes the turn: each root block followed
    /// by its children.
    pub blocks: Vec<BlockRecord>,
    /// In order: by the entry they follow, then their place after it.
    pub annotations: Vec<PlacedAnnotation>,
}

impl Content {
    /// The records of a whole turn, with its annotations.
    pub fn of_turn(turn: &Turn, annotations: Vec<PlacedAnnotation>) -> Result<Self, Error> {
        let set = RecordSet::from_turn(turn)?;
        Ok(Self {
            entries: set.entries().to_vec(),
            blocks: set.blocks().to_vec(),
            annotations,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.blocks.is_empty() && self.annotations.is_empty()
    }

    /// Whether the turn holds an entry other than its closure: a message failed
    /// before its first entry is left out of a runner's conversation.
    pub fn has_an_entry(&self) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.kind() != TurnEntryKind::TurnClosure)
    }

    /// The rows of the turn the library's conversation holds: every row but
    /// those of an interrupted response, which members see and a runner never.
    pub fn complete_rows(&self) -> (Vec<EntryRecord>, Vec<BlockRecord>) {
        let interrupted: HashSet<_> = self
            .entries
            .iter()
            .filter(|entry| entry.state() != RepresentationState::Complete)
            .map(EntryRecord::id)
            .collect();
        let entries = self
            .entries
            .iter()
            .filter(|entry| !interrupted.contains(&entry.id()))
            .cloned()
            .collect();
        let blocks = self
            .blocks
            .iter()
            .filter(|block| !interrupted.contains(&block.entry()))
            .cloned()
            .collect();
        (entries, blocks)
    }

    /// The library's turn of the complete rows, under its turn row.
    pub fn to_turn(&self, record: TurnRecord) -> Result<Turn, Error> {
        let (entries, blocks) = self.complete_rows();
        let mut turns = RecordSet::new()
            .with_turns(vec![record])
            .with_entries(entries)
            .with_blocks(blocks)
            .into_turns()?;
        Ok(turns.remove(0))
    }

    /// Every id the content declares: its entries', its blocks' (children
    /// included), its text blocks' citations' and its annotations'.
    pub fn identities(&self) -> Vec<Uuid> {
        let mut ids: Vec<Uuid> = self.entries.iter().map(|e| e.id().as_uuid()).collect();
        for block in &self.blocks {
            ids.push(block.id().as_uuid());
            ids.extend(citation_ids(block));
        }
        ids.extend(self.annotations.iter().map(|placed| placed.annotation.id));
        ids
    }

    /// The artifacts its `ARTIFACT` annotations name.
    pub fn artifacts(&self) -> Vec<Uuid> {
        self.annotations
            .iter()
            .filter_map(|placed| placed.annotation.body.artifact())
            .collect()
    }

    /// Whether `entry` is one of its entries.
    pub fn holds_entry(&self, entry: Uuid) -> bool {
        self.entries.iter().any(|row| row.id().as_uuid() == entry)
    }
}

/// The citation ids of a text block's record: a citation is the library's
/// citation of a text block, its id in the record's payload.
pub fn citation_ids(block: &BlockRecord) -> Vec<Uuid> {
    if block.kind() != ContentBlockKind::Text {
        return Vec::new();
    }
    serde_json::to_value(block.payload())
        .ok()
        .and_then(|payload| payload.get("citations").cloned())
        .and_then(|citations| citations.as_array().cloned())
        .into_iter()
        .flatten()
        .filter_map(|citation| {
            citation
                .get("id")
                .and_then(serde_json::Value::as_str)
                .and_then(|id| Uuid::parse_str(id).ok())
        })
        .collect()
}

/// The first id named twice in `ids`.
pub fn first_repeated(ids: impl IntoIterator<Item = Uuid>) -> Option<Uuid> {
    let mut seen = HashSet::new();
    ids.into_iter().find(|id| !seen.insert(*id))
}

/// A `MESSAGE_INVALID` refusal under one of Threads' own codes.
pub fn invalid(code: MessageInvalidError) -> Refusal {
    Refusal::new(ReasonCode::MessageInvalid).with("error", code.as_str())
}

/// The refusal of what the library refuses: `MESSAGE_INVALID` under the
/// library's code, naming the object and the place the library names.
pub fn refusal_of(error: &Error) -> Refusal {
    let refusal = invalid(MessageInvalidError::of_library(error.code()));
    let refusal = match error.object_id() {
        Some(id) => refusal.with("object_id", id),
        None => refusal,
    };
    match error.location() {
        Some(location) => refusal.with("location", location),
        None => refusal,
    }
}

/// The refusal of content whose ids the caller minted for new objects: an id
/// the library refuses as no UUIDv7 is `ID_NOT_UUIDV7`, anything else the
/// library's `MESSAGE_INVALID`.
pub fn minted_refusal_of(error: &Error) -> Refusal {
    match error.code() {
        ErrorCode::InvalidIdentity => {
            let refusal = Refusal::new(ReasonCode::IdNotUuidV7);
            let refusal = match error.object_id() {
                Some(id) => refusal.with("id", id),
                None => refusal,
            };
            match error.location() {
                Some(location) => refusal.with("location", location),
                None => refusal,
            }
        }
        _ => refusal_of(error),
    }
}

#[cfg(test)]
mod tests {
    use contract_threads::llm::{
        AITurn, AgentId, BlockId, Citation, CitationId, CitationSource, ClosureReason, EntryId,
        HumanId, HumanMessage, InputTurn, ModelResponse, PlainBlock, ResponseEnding,
        ResponseMetadata, TextBlock, TurnClosure, TurnId, TurnKind,
    };
    use contract_threads::{AnnotationBody, Notice};

    use super::*;

    fn v7() -> Uuid {
        Uuid::now_v7()
    }

    fn cited_text(text: &str) -> (TextBlock, Uuid) {
        let citation = v7();
        let block = TextBlock::new(BlockId::from_uuid(v7()).unwrap(), text)
            .unwrap()
            .with_citations(vec![Citation::new(
                CitationId::from_uuid(citation).unwrap(),
                CitationSource::new("kb:1").unwrap(),
            )])
            .unwrap();
        (block, citation)
    }

    #[test]
    fn a_turn_becomes_records_and_comes_back_whole() {
        let turn_id = TurnId::from_uuid(v7()).unwrap();
        let (text, _) = cited_text("hello");
        let human = HumanMessage::new(
            EntryId::from_uuid(v7()).unwrap(),
            HumanId::from_uuid(v7()).unwrap(),
            vec![PlainBlock::try_from(contract_threads::llm::ContentBlock::Text(text)).unwrap()],
        )
        .unwrap();
        let turn: Turn = InputTurn::new(turn_id, vec![human.into()]).unwrap().into();
        let content = Content::of_turn(&turn, Vec::new()).unwrap();
        assert_eq!(content.entries.len(), 1);
        assert_eq!(content.blocks.len(), 1);
        let record = TurnRecord::new(turn_id, TurnKind::InputTurn);
        assert_eq!(content.to_turn(record).unwrap(), turn);
        assert!(content.has_an_entry());
    }

    #[test]
    fn the_identities_of_a_content_name_its_entries_blocks_citations_and_annotations() {
        let turn_id = TurnId::from_uuid(v7()).unwrap();
        let agent = AgentId::from_uuid(v7()).unwrap();
        let (text, citation) = cited_text("answer");
        let response_id = EntryId::from_uuid(v7()).unwrap();
        let response = ModelResponse::new(
            response_id,
            vec![text.clone().into()],
            ResponseEnding::finished(),
            ResponseMetadata::new(),
        )
        .unwrap();
        let mut ai = AITurn::open(
            turn_id,
            agent,
            contract_threads::llm::Author::new("Analyst").unwrap(),
        );
        ai.push_entry(response.into()).unwrap();
        ai.push_entry(
            TurnClosure::new(
                EntryId::from_uuid(v7()).unwrap(),
                ClosureReason::completed(),
            )
            .into(),
        )
        .unwrap();
        let annotation = PlacedAnnotation::new(
            response_id.as_uuid(),
            contract_threads::Annotation::new(
                v7(),
                AnnotationBody::Warning(Notice {
                    code: "PARTIAL".into(),
                    text: None,
                }),
            ),
        );
        let content = Content::of_turn(&ai.into(), vec![annotation.clone()]).unwrap();
        let ids = content.identities();
        assert!(ids.contains(&response_id.as_uuid()));
        assert!(ids.contains(&text.id().as_uuid()));
        assert!(ids.contains(&citation));
        assert!(ids.contains(&annotation.annotation.id));
        assert_eq!(first_repeated(ids), None);
        assert!(content.holds_entry(response_id.as_uuid()));
    }

    #[test]
    fn a_library_refusal_names_the_librarys_code_and_a_minted_id_refused_is_id_not_uuidv7() {
        let error =
            PlainBlock::from_json(r#"{"type":"text","id":"not-an-id","text":"hi"}"#).unwrap_err();
        assert_eq!(minted_refusal_of(&error).code, ReasonCode::IdNotUuidV7);
        let refusal = refusal_of(&error);
        assert_eq!(refusal.code, ReasonCode::MessageInvalid);
        assert_eq!(refusal.params["error"], "invalid_identity");
        let error =
            PlainBlock::from_json(&format!(r#"{{"type":"text","id":"{}","text":"  "}}"#, v7()))
                .unwrap_err();
        let refusal = refusal_of(&error);
        assert_eq!(refusal.code, ReasonCode::MessageInvalid);
        assert_eq!(refusal.params["error"], "invalid_content");
    }
}
