use std::collections::{HashMap, HashSet};

use contract_threads::llm::{
    AITurn, AgentId, Author, BlockId, DeltaOutcome, DraftPhase, EntryId, Error, RecordSet,
    ResponseDelta, ResponseDraft, Turn, TurnEntry, TurnEntryKind, TurnId,
};
use contract_threads::{Chunk, MessageInvalidError, PlacedAnnotation};
use service_engine::error::EngineError;
use uuid::Uuid;

use super::content::{Content, citation_ids};
use crate::fault::corrupt;

const PROBE_TURN: Uuid = Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0001);
const PROBE_AGENT: Uuid = Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0002);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRefusal {
    pub seq: Option<u64>,
    pub code: &'static str,
    pub object_id: Option<Uuid>,
    pub location: Option<String>,
}

impl StreamRefusal {
    fn library(error: &Error) -> Self {
        Self {
            seq: None,
            code: MessageInvalidError::of_library(error.code()).as_str(),
            object_id: error.object_id(),
            location: error.location().map(str::to_owned),
        }
    }

    fn threads(code: MessageInvalidError, object_id: Option<Uuid>) -> Self {
        Self {
            seq: None,
            code: code.as_str(),
            object_id,
            location: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Item {
    Entry { entry: TurnEntry, streamed: bool },
    Response(Box<ResponseDraft>),
    Annotation(PlacedAnnotation),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct TurnDraft {
    probe: Option<AITurn>,
    items: Vec<Item>,
    producing: Option<usize>,
    delta_turn: Option<TurnId>,
    refusal: Option<StreamRefusal>,
}

fn probe_turn() -> AITurn {
    AITurn::open(
        TurnId::from_uuid(PROBE_TURN).expect("a constant UUIDv7"),
        AgentId::from_uuid(PROBE_AGENT).expect("a constant UUIDv7"),
        Author::new("probe").expect("a constant author"),
    )
}

pub fn fold(draft: &mut TurnDraft, seq: u64, raw: serde_json::Value) {
    if draft.refusal.is_some() {
        return;
    }
    let folded = if super::content::holds_nul(&raw) {
        Err(StreamRefusal::threads(
            MessageInvalidError::NulCharacter,
            None,
        ))
    } else {
        match serde_json::from_value::<Chunk>(raw) {
            Ok(chunk) => draft.fold_chunk(chunk),
            Err(_) => Err(StreamRefusal::threads(
                MessageInvalidError::ChunkUnreadable,
                None,
            )),
        }
    };
    if let Err(mut refusal) = folded {
        refusal.seq = Some(seq);
        draft.refusal = Some(refusal);
    }
}

impl TurnDraft {
    pub fn refusal(&self) -> Option<&StreamRefusal> {
        self.refusal.as_ref()
    }

    pub fn refuse(&mut self, refusal: StreamRefusal) {
        self.refusal.get_or_insert(refusal);
    }

    fn probe_mut(&mut self) -> &mut AITurn {
        self.probe.get_or_insert_with(probe_turn)
    }

    fn holds_entry(&self, entry: Uuid) -> bool {
        self.items.iter().any(|item| match item {
            Item::Entry { entry: held, .. } => held.id().as_uuid() == entry,
            Item::Response(draft) => draft.entry().as_uuid() == entry,
            Item::Annotation(_) => false,
        })
    }

    fn fold_chunk(&mut self, chunk: Chunk) -> Result<(), StreamRefusal> {
        match chunk {
            Chunk::Keepalive => Ok(()),
            Chunk::Delta { delta } => {
                let delta = delta
                    .to_delta()
                    .map_err(|error| StreamRefusal::library(&error))?;
                self.fold_delta(&delta)
            }
            Chunk::Entry { entry } => {
                let entry = entry
                    .to_entry()
                    .map_err(|error| StreamRefusal::library(&error))?;
                let id = Some(entry.id().as_uuid());
                if self.producing.is_some() {
                    return Err(StreamRefusal::threads(
                        MessageInvalidError::ResponseStillProducing,
                        id,
                    ));
                }
                match entry.kind() {
                    TurnEntryKind::ModelResponse => {
                        return Err(StreamRefusal::threads(
                            MessageInvalidError::ResponseInEntryChunk,
                            id,
                        ));
                    }
                    TurnEntryKind::TurnClosure => {
                        return Err(StreamRefusal::threads(
                            MessageInvalidError::ClosureByWriter,
                            id,
                        ));
                    }
                    _ => {}
                }
                self.probe_mut()
                    .push_entry(entry.clone())
                    .map_err(|error| StreamRefusal::library(&error))?;
                self.items.push(Item::Entry {
                    entry,
                    streamed: false,
                });
                Ok(())
            }
            Chunk::Annotation {
                after_entry_id,
                annotation,
            } => {
                BlockId::from_uuid(annotation.id)
                    .map_err(|error| StreamRefusal::library(&error))?;
                if !self.holds_entry(after_entry_id) {
                    return Err(StreamRefusal::threads(
                        MessageInvalidError::AnnotationEntryUnknown,
                        Some(after_entry_id),
                    ));
                }
                self.items.push(Item::Annotation(PlacedAnnotation::new(
                    after_entry_id,
                    annotation,
                )));
                Ok(())
            }
        }
    }

    fn fold_delta(&mut self, delta: &ResponseDelta) -> Result<(), StreamRefusal> {
        let at = match self.producing {
            Some(at) => {
                let Some(Item::Response(draft)) = self.items.get_mut(at) else {
                    return Err(StreamRefusal::threads(
                        MessageInvalidError::ResponseStillProducing,
                        Some(delta.entry().as_uuid()),
                    ));
                };
                if draft.entry() != delta.entry() {
                    return Err(StreamRefusal::threads(
                        MessageInvalidError::ResponseStillProducing,
                        Some(delta.entry().as_uuid()),
                    ));
                }
                let outcome = draft
                    .apply(delta)
                    .map_err(|error| StreamRefusal::library(&error))?;
                return self.settle(outcome, at);
            }
            None => self.items.len(),
        };
        let turn = *self.delta_turn.get_or_insert(delta.turn());
        let mut draft = ResponseDraft::new(turn, delta.entry());
        let outcome = draft
            .apply(delta)
            .map_err(|error| StreamRefusal::library(&error))?;
        self.items.push(Item::Response(Box::new(draft)));
        self.producing = Some(at);
        self.settle(outcome, at)
    }

    fn settle(&mut self, outcome: DeltaOutcome, at: usize) -> Result<(), StreamRefusal> {
        match outcome {
            DeltaOutcome::Completed { .. } => {
                self.producing = None;
                let Some(Item::Response(draft)) = self.items.get(at) else {
                    return Ok(());
                };
                let Some(response) = draft.response().cloned() else {
                    return Ok(());
                };
                let entry = TurnEntry::ModelResponse(response);
                match self.probe_mut().push_entry(entry.clone()) {
                    Ok(()) => {
                        self.items[at] = Item::Entry {
                            entry,
                            streamed: true,
                        };
                        Ok(())
                    }
                    Err(error) => {
                        self.items.truncate(at);
                        Err(StreamRefusal::library(&error))
                    }
                }
            }
            DeltaOutcome::Interrupted { .. } => {
                self.producing = None;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn judge(
        &self,
        turn: TurnId,
        agent: AgentId,
        author: &Author,
        held: &HashSet<Uuid>,
    ) -> Result<Judged, EngineError> {
        let mut judged = Judged {
            turn: AITurn::open(turn, agent, author.clone()),
            items: Vec::with_capacity(self.items.len()),
            refusal: self.refusal.clone(),
        };
        let mut seen: HashSet<Uuid> = HashSet::from([turn.as_uuid()]);
        let mut entries: HashSet<Uuid> = HashSet::new();
        let declared = self.declared_by_entry()?;
        for item in &self.items {
            let ids = item_ids(item, &declared)?;
            if let Some(clash) = ids.iter().find(|id| seen.contains(id) || held.contains(id)) {
                judged.cut(StreamRefusal {
                    seq: None,
                    code: MessageInvalidError::ReusedIdentity.as_str(),
                    object_id: Some(*clash),
                    location: None,
                });
                break;
            }
            let kept = match item {
                Item::Entry {
                    entry,
                    streamed: true,
                } if self.delta_turn != Some(turn) => Err(StreamRefusal {
                    seq: None,
                    code: MessageInvalidError::StreamTargetError.as_str(),
                    object_id: Some(entry.id().as_uuid()),
                    location: Some("turn".to_owned()),
                }),
                Item::Entry { entry, .. } => judged
                    .turn
                    .push_entry(entry.clone())
                    .map(|()| JudgedItem::Entry(entry.id().as_uuid()))
                    .map_err(|error| StreamRefusal::library(&error)),
                Item::Response(draft) if draft.turn() != turn => Err(StreamRefusal {
                    seq: None,
                    code: MessageInvalidError::StreamTargetError.as_str(),
                    object_id: Some(draft.entry().as_uuid()),
                    location: Some("turn".to_owned()),
                }),
                Item::Response(draft) => Ok(JudgedItem::Response(draft.clone())),
                Item::Annotation(placed) if !entries.contains(&placed.after_entry_id) => {
                    Err(StreamRefusal::threads(
                        MessageInvalidError::AnnotationEntryUnknown,
                        Some(placed.after_entry_id),
                    ))
                }
                Item::Annotation(placed) => Ok(JudgedItem::Annotation(placed.clone())),
            };
            match kept {
                Ok(kept) => {
                    match &kept {
                        JudgedItem::Entry(id) => {
                            entries.insert(*id);
                        }
                        JudgedItem::Response(draft) => {
                            entries.insert(draft.entry().as_uuid());
                        }
                        JudgedItem::Annotation(_) => {}
                    }
                    seen.extend(ids);
                    judged.items.push(kept);
                }
                Err(refusal) => {
                    judged.cut(refusal);
                    break;
                }
            }
        }
        Ok(judged)
    }
}

impl TurnDraft {
    fn declared_by_entry(&self) -> Result<HashMap<Uuid, Vec<Uuid>>, EngineError> {
        let Some(probe) = &self.probe else {
            return Ok(HashMap::new());
        };
        let content =
            Content::of_turn(&Turn::AITurn(probe.clone()), Vec::new()).map_err(corrupt)?;
        Ok(content.identities_by_entry())
    }
}

fn item_ids(item: &Item, declared: &HashMap<Uuid, Vec<Uuid>>) -> Result<Vec<Uuid>, EngineError> {
    Ok(match item {
        Item::Entry { entry, .. } => declared
            .get(&entry.id().as_uuid())
            .cloned()
            .unwrap_or_else(|| vec![entry.id().as_uuid()]),
        Item::Response(draft) => {
            let snapshot = draft.to_snapshot().map_err(corrupt)?;
            let mut ids = vec![snapshot.entry().id().as_uuid()];
            for block in snapshot.blocks() {
                ids.push(block.id().as_uuid());
                ids.extend(citation_ids(block));
            }
            ids
        }
        Item::Annotation(placed) => vec![placed.annotation.id],
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum JudgedItem {
    Entry(Uuid),
    Response(Box<ResponseDraft>),
    Annotation(PlacedAnnotation),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Judged {
    pub turn: AITurn,
    pub items: Vec<JudgedItem>,
    pub refusal: Option<StreamRefusal>,
}

impl Judged {
    fn cut(&mut self, refusal: StreamRefusal) {
        self.refusal.get_or_insert(refusal);
    }

    pub fn producing(&self) -> Option<&ResponseDraft> {
        self.items.iter().find_map(|item| match item {
            JudgedItem::Response(draft)
                if matches!(draft.phase(), DraftPhase::Producing { .. }) =>
            {
                Some(draft.as_ref())
            }
            _ => None,
        })
    }

    pub fn identities(&self) -> Result<Vec<Uuid>, EngineError> {
        Ok(self.content()?.identities())
    }

    pub fn content(&self) -> Result<Content, EngineError> {
        let set = RecordSet::from_turn(&self.turn.clone().into()).map_err(corrupt)?;
        let mut content = Content::default();
        let mut entries = set.entries().iter();
        let blocks_of = |entry: EntryId| {
            set.blocks()
                .iter()
                .filter(move |block| block.entry() == entry)
                .cloned()
        };
        for item in &self.items {
            match item {
                JudgedItem::Entry(_) => {
                    if let Some(entry) = entries.next() {
                        content.blocks.extend(blocks_of(entry.id()));
                        content.entries.push(entry.clone());
                    }
                }
                JudgedItem::Response(draft) => {
                    let snapshot = draft.to_snapshot().map_err(corrupt)?;
                    content.entries.push(snapshot.entry().clone());
                    content.blocks.extend(snapshot.blocks().iter().cloned());
                }
                JudgedItem::Annotation(placed) => content.annotations.push(placed.clone()),
            }
        }
        for entry in entries {
            content.blocks.extend(blocks_of(entry.id()));
            content.entries.push(entry.clone());
        }
        Ok(content)
    }
}

#[cfg(test)]
#[path = "draft_tests.rs"]
pub(crate) mod tests;
