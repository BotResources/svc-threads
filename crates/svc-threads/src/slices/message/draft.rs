//! A `STREAMING` message's turn as its chunks fold, and the content it is
//! stored with when its stream closes. Pure: the accumulator calls [`fold`] for
//! every chunk, in sequence; the view, the watch, the close commands and the
//! agent context [`TurnDraft::judge`] what folded.
//!
//! The library has the last word on every chunk: a response delta is applied to
//! the library's `ResponseDraft` of its entry, and the response it completes —
//! like every whole entry — is pushed into the library's `AITurn`, whose rules
//! (order, results answering their calls, ids used once) refuse what does not
//! fit. The first chunk refused is recorded and nothing after it folds: the
//! stream watch then fails the message ([`TurnDraft::refusal`]).
//!
//! A fold does not know its key — the engine hands it the state, the sequence
//! and the chunk, never the message — so it judges the turn under a probe id
//! ([`PROBE_TURN`]). [`TurnDraft::judge`] rebuilds the turn under the message's
//! own id, agent and author, and with the ids other messages already hold:
//! what the probe could not see (an id equal to the message's, a delta naming
//! another turn, an id another message holds) cuts the turn there, as a refused
//! chunk does. The view, the watch, the close and the agent context all judge
//! the same way, so what members see is what the message closes to.
//!
//! Closing the turn is Threads': a closure pushed on the stream is refused,
//! and the closure a close appends is a new entry under a UUIDv7 Threads
//! mints.

use std::collections::HashSet;

use contract_threads::llm::{
    AITurn, AgentId, Author, BlockId, ClosureReason, ContentBlock, DeltaOp, DeltaOutcome,
    DraftPhase, EntryId, Error, InterruptionReason, PlainBlock, RecordSet, ResponseDelta,
    ResponseDraft, TurnClosure, TurnEntry, TurnEntryKind, TurnId,
};
use contract_threads::{Chunk, MessageInvalidError, MessageStatus, PlacedAnnotation};
use service_engine::error::EngineError;
use uuid::Uuid;

use super::content::{Content, citation_ids};
use crate::fault::corrupt;

/// The turn id a fold judges under: a fold does not know its message.
const PROBE_TURN: Uuid = Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0001);
/// The agent a fold judges under.
const PROBE_AGENT: Uuid = Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0002);

/// Why a stream was cut: a code — the library's ([`Error::code`]) or one of
/// Threads' ([`MessageInvalidError`]) — and the object and place the library
/// names, at the chunk it fell on when a chunk was refused.
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

/// What a stream holds, in arrival order.
#[derive(Debug, Clone, PartialEq)]
enum Item {
    /// A whole entry, or — `streamed` — a response the library completed from
    /// the stream's deltas.
    Entry {
        entry: TurnEntry,
        streamed: bool,
    },
    /// A response still produced, or one its writer interrupted.
    Response(Box<ResponseDraft>),
    Annotation(PlacedAnnotation),
}

/// What the stream of one message folded so far.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TurnDraft {
    /// The entries the library accepted, under the probe id.
    probe: Option<AITurn>,
    items: Vec<Item>,
    /// The item of the response being produced.
    producing: Option<usize>,
    /// The turn the stream's deltas name.
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

/// Folds one raw chunk, in sequence. Never fails: a chunk the contract or the
/// library refuses is recorded as the stream's refusal, and nothing after it
/// folds.
pub fn fold(draft: &mut TurnDraft, seq: u64, raw: serde_json::Value) {
    if draft.refusal.is_some() {
        return;
    }
    let folded = match serde_json::from_value::<Chunk>(raw) {
        Ok(chunk) => draft.fold_chunk(chunk),
        Err(_) => Err(StreamRefusal::threads(
            MessageInvalidError::ChunkUnreadable,
            None,
        )),
    };
    if let Err(mut refusal) = folded {
        refusal.seq = Some(seq);
        draft.refusal = Some(refusal);
    }
}

impl TurnDraft {
    /// The first refused chunk, if any: the message has failed there.
    pub fn refusal(&self) -> Option<&StreamRefusal> {
        self.refusal.as_ref()
    }

    /// The stream refused from outside its fold — a frame under the
    /// accumulator name of Threads 0.4: nothing it holds after now folds.
    pub fn refuse(&mut self, refusal: StreamRefusal) {
        self.refusal.get_or_insert(refusal);
    }

    fn probe_mut(&mut self) -> &mut AITurn {
        self.probe.get_or_insert_with(probe_turn)
    }

    /// The entries seen so far, by id: those an annotation may follow.
    fn holds_entry(&self, entry: Uuid) -> bool {
        self.items.iter().any(|item| match item {
            Item::Entry { entry: held, .. } => held.id().as_uuid() == entry,
            Item::Response(draft) => draft.entry().as_uuid() == entry,
            Item::Annotation(_) => false,
        })
    }

    fn fold_chunk(&mut self, chunk: Chunk) -> Result<(), StreamRefusal> {
        match chunk {
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
                // An annotation's id is minted like a block's: a UUIDv7.
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
        // Every delta of a stream names one turn: a draft opened under the
        // first one refuses a delta naming another (the library's target
        // error).
        let turn = *self.delta_turn.get_or_insert(delta.turn());
        let mut draft = ResponseDraft::new(turn, delta.entry());
        let outcome = draft
            .apply(delta)
            .map_err(|error| StreamRefusal::library(&error))?;
        self.items.push(Item::Response(Box::new(draft)));
        self.producing = Some(at);
        self.settle(outcome, at)
    }

    /// A response the library completed joins the turn — or, refused there,
    /// leaves the stream; one its writer interrupted stays, interrupted.
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

    /// The stream judged under the message's own id, agent and author, the ids
    /// in `held` belonging to other messages: the turn the library accepts, and
    /// what the stream holds around it, cut at the first item that does not fit.
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
        for item in &self.items {
            let ids = item_ids(item)?;
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
                // The deltas name the turn they fold into: under another
                // turn than the message's, nothing they built is the
                // message's.
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

/// The ids an item declares.
fn item_ids(item: &Item) -> Result<Vec<Uuid>, EngineError> {
    Ok(match item {
        Item::Entry { entry, .. } => entry_ids(entry),
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

/// The ids an entry declares: its own, its blocks' (children included) and its
/// text blocks' citations'.
pub fn entry_ids(entry: &TurnEntry) -> Vec<Uuid> {
    let mut ids = vec![entry.id().as_uuid()];
    match entry {
        TurnEntry::HumanMessage(message) => plain_ids(message.content(), &mut ids),
        TurnEntry::AgentRequest(request) => plain_ids(request.content(), &mut ids),
        TurnEntry::AgentMail(mail) => plain_ids(mail.content(), &mut ids),
        TurnEntry::ModelResponse(response) => {
            for block in response.blocks() {
                ids.push(block.id().as_uuid());
                if let ContentBlock::Text(text) = block {
                    ids.extend(text.citations().iter().map(|c| c.id().as_uuid()));
                }
                plain_ids(block.children(), &mut ids);
            }
        }
        TurnEntry::RuntimeToolResults(results) => {
            for result in results.results() {
                ids.push(result.id().as_uuid());
                plain_ids(result.content(), &mut ids);
            }
        }
        _ => {}
    }
    ids
}

fn plain_ids(blocks: &[PlainBlock], ids: &mut Vec<Uuid>) {
    for block in blocks {
        ids.push(block.id().as_uuid());
        if let PlainBlock::Text(text) = block {
            ids.extend(text.citations().iter().map(|c| c.id().as_uuid()));
        }
    }
}

/// An item of a judged stream.
#[derive(Debug, Clone, PartialEq)]
pub enum JudgedItem {
    /// An entry of the turn, by id.
    Entry(Uuid),
    /// A response still produced, or interrupted.
    Response(Box<ResponseDraft>),
    Annotation(PlacedAnnotation),
}

/// A stream judged under its message: the library's turn of what it accepted,
/// and its items around it.
#[derive(Debug, Clone, PartialEq)]
pub struct Judged {
    pub turn: AITurn,
    pub items: Vec<JudgedItem>,
    /// The stream's refusal, or where the judgement cut it.
    pub refusal: Option<StreamRefusal>,
}

impl Judged {
    fn cut(&mut self, refusal: StreamRefusal) {
        self.refusal.get_or_insert(refusal);
    }

    /// The response still being produced, if any.
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

    /// Every id the judged stream declares.
    pub fn identities(&self) -> Result<Vec<Uuid>, EngineError> {
        Ok(self.content()?.identities())
    }

    /// The records of what the stream holds, in arrival order: the turn's
    /// entries as the library decomposes them, a response being produced or
    /// interrupted as its draft's snapshot, the annotations after their
    /// entries.
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
        // What the turn holds past the items: its closure.
        for entry in entries {
            content.blocks.extend(blocks_of(entry.id()));
            content.entries.push(entry.clone());
        }
        Ok(content)
    }
}

/// What closes a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseKind {
    /// `message.complete`: the writer says the stream is whole.
    Complete,
    /// `message.fail`: the writer failed it.
    Fail,
    /// `message.cancel`, typically a user's stop.
    Cancel,
    /// The stream watch: the writer went silent, or a chunk was refused.
    Watch,
}

/// Why Threads failed a message whose stream gave no reason of its own.
pub mod reason {
    /// The writer failed it (`message.fail`).
    pub const FAILED_BY_WRITER: &str = "failed_by_writer";
    /// Its writer pushed nothing for the idle window.
    pub const STREAM_IDLE: &str = "stream_idle";
    /// A completion while a response was still produced.
    pub const RESPONSE_STILL_PRODUCING: &str = "response_still_producing";
}

/// The content a stream is stored with when it closes, its status, and why it
/// failed when it did.
#[derive(Debug, Clone, PartialEq)]
pub struct Closed {
    pub status: MessageStatus,
    pub content: Content,
    pub reason: Option<&'static str>,
}

/// Closes a judged stream: a response still produced is interrupted (kept for
/// members, never sent to a runner), then the turn is closed with the
/// library's closure under its derived id. A completion the library accepts —
/// nothing refused, no response still produced, the turn closable completed
/// (no tool call without its result) — is `COMPLETED`; a cancel `CANCELLED`;
/// anything else `FAILED`, closed failed — or interrupted, when its writer
/// went silent. A tool call left without a result stays unanswered, as the
/// library allows on an abnormal closure.
pub fn close(mut judged: Judged, kind: CloseKind) -> Result<Closed, EngineError> {
    // The closure is a new entry of the turn: Threads mints its UUIDv7.
    let closure_id = Uuid::now_v7();
    let interruption = match kind {
        CloseKind::Complete | CloseKind::Fail => InterruptionReason::failed(),
        CloseKind::Cancel => InterruptionReason::cancelled(),
        CloseKind::Watch => InterruptionReason::interrupted(),
    };
    let mut was_producing = false;
    for item in &mut judged.items {
        if let JudgedItem::Response(draft) = item
            && matches!(draft.phase(), DraftPhase::Producing { .. })
        {
            was_producing = true;
            let stop = ResponseDelta::new(
                draft.turn(),
                draft.entry(),
                DeltaOp::response_interrupted(interruption.clone()),
            );
            draft.apply(&stop).map_err(corrupt)?;
        }
    }
    let refusal = judged.refusal.as_ref().map(|refusal| refusal.code);
    let failed = |detail: &'static str| -> Result<_, EngineError> {
        Ok((
            MessageStatus::Failed,
            ClosureReason::failed(detail).map_err(corrupt)?,
            Some(detail),
        ))
    };
    let (status, closure, reason) = match (kind, refusal) {
        (CloseKind::Watch, None) => (
            MessageStatus::Failed,
            ClosureReason::interrupted(),
            Some(reason::STREAM_IDLE),
        ),
        (CloseKind::Cancel, None) => (MessageStatus::Cancelled, ClosureReason::cancelled(), None),
        (CloseKind::Fail, None) => failed(reason::FAILED_BY_WRITER)?,
        (_, Some(code)) => failed(code)?,
        (CloseKind::Complete, None) if was_producing => failed(reason::RESPONSE_STILL_PRODUCING)?,
        (CloseKind::Complete, None) => {
            let mut trial = judged.turn.clone();
            match trial.push_entry(closure_entry(closure_id, ClosureReason::completed())?) {
                Ok(()) => (MessageStatus::Completed, ClosureReason::completed(), None),
                Err(error) => failed(MessageInvalidError::of_library(error.code()).as_str())?,
            }
        }
    };
    let closure = closure_entry(closure_id, closure)?;
    judged.turn.push_entry(closure).map_err(corrupt)?;
    Ok(Closed {
        status,
        content: judged.content()?,
        reason,
    })
}

/// A turn's closure under `id`.
pub fn closure_entry(id: Uuid, reason: ClosureReason) -> Result<TurnEntry, EngineError> {
    Ok(TurnClosure::new(EntryId::from_uuid(id).map_err(corrupt)?, reason).into())
}

#[cfg(test)]
#[path = "draft_tests.rs"]
mod tests;
