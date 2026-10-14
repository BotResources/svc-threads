//! A `STREAMING` message's content as its chunks fold, and the content it is
//! stored with when its stream closes. Pure: the accumulator calls [`fold`] for
//! every chunk, in sequence; the view renders [`MessageDraft::preview`]; the
//! close commands and the stream watch store [`close`].
//!
//! Every chunk names its item. A step arrives as the library's stream events,
//! judged by the library's step draft as they come; a tool results item and a
//! Threads block arrive whole. The library has the last word on every event:
//! the draft keeps a mirror of what the library accepted (the blocks so far,
//! their text, which one is open) and, for each new event, rebuilds a library
//! `StepDraft` from that mirror — one start, one accumulated delta and one end
//! per block, which the library treats exactly as the original events, since a
//! delta only appends to the open block — then applies the event. The mirror is
//! what the view shows of a step still streaming.
//!
//! LIBRARY GAP (br-llm-messages v0.3.0): `StepDraft` is not `Clone` (an
//! accumulator state must be), exposes no partial blocks, and cannot close an
//! unfinished draft. The rebuild above covers the first two; [`close`] closes an
//! unfinished step itself, with the library's own events (the open block ended,
//! a `finish` whose stop reason is `awaiting_tool_results` when the step holds
//! tool calls, `other("interrupted")` otherwise), and keeps it only when the
//! library then finishes it.
//!
//! The first chunk the library or the chunk format refuses is recorded and
//! nothing after it folds: the message fails there ([`MessageDraft::refusal`]).

use contract_threads::llm::{
    BlockKind as LibraryKind, MessageError, ModelId, Step, StepDraft, StopReason, StreamEvent,
    Text, ToolCallId, ToolName, ToolResult, ToolResultBlock, ToolResults, Turn, TurnId, TurnState,
    Usage,
};
use contract_threads::{Block, BlockKind, Chunk, ChunkPart, MessageStatus};
use uuid::Uuid;

use super::content::{self, library_error_code, threads_block, tool_result_block};
use super::state::{ItemKind, StoredBlock, StoredItem};

/// The text of the error result that answers a tool call its message's close
/// left without a result.
pub const INTERRUPTED: &str = "interrupted";

/// The first chunk of a stream Threads refused: where, and why — a library
/// error code ([`library_error_code`]) or one of the chunk format's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRefusal {
    pub seq: u64,
    pub item_index: Option<u32>,
    pub code: &'static str,
}

/// The chunk format's own refusals.
pub mod chunk_error {
    pub const UNREADABLE: &str = "chunk_unreadable";
    pub const ITEM_OUT_OF_ORDER: &str = "item_out_of_order";
    pub const ITEM_COMPLETE: &str = "item_complete";
    pub const BLOCK_ID_REQUIRED: &str = "block_id_required";
    pub const BLOCK_ID_NOT_ALLOWED: &str = "block_id_not_allowed";
    pub const BLOCK_ID_REUSED: &str = "block_id_reused";
    pub const BLOCK_ID_NOT_UUIDV7: &str = "block_id_not_uuidv7";
    pub const MODEL_NOT_FIRST: &str = "model_not_first";
    pub const NOT_A_THREADS_BLOCK: &str = "not_a_threads_block";
    pub const NOT_A_TOOL_RESULTS_BLOCK: &str = "not_a_tool_results_block";
}

/// One library block of a step as streamed so far: what the library accepted.
#[derive(Debug, Clone, PartialEq)]
enum LibraryBlock {
    Text {
        text: String,
    },
    Thinking {
        text: String,
        signature: Option<String>,
    },
    RedactedThinking {
        data: String,
    },
    Structured {
        json: String,
    },
    ToolCall {
        id: ToolCallId,
        name: ToolName,
        arguments: String,
    },
}

impl LibraryBlock {
    fn kind(&self) -> BlockKind {
        match self {
            Self::Text { .. } => BlockKind::Text,
            Self::Thinking { .. } => BlockKind::Thinking,
            Self::RedactedThinking { .. } => BlockKind::RedactedThinking,
            Self::Structured { .. } => BlockKind::Structured,
            Self::ToolCall { .. } => BlockKind::ToolCall,
        }
    }

    /// What the view shows of the block while it streams: the library's shape,
    /// its text so far; a structured value or tool arguments still partial are
    /// shown as the raw text received.
    fn preview(&self) -> serde_json::Value {
        match self {
            Self::Text { text } => serde_json::json!({ "type": "text", "text": text }),
            Self::Thinking { text, signature } => match signature {
                Some(signature) => serde_json::json!({
                    "type": "thinking", "text": text, "signature": signature
                }),
                None => serde_json::json!({ "type": "thinking", "text": text }),
            },
            Self::RedactedThinking { data } => {
                serde_json::json!({ "type": "redacted_thinking", "data": data })
            }
            Self::Structured { json } => serde_json::json!({ "type": "structured", "json": json }),
            Self::ToolCall {
                id,
                name,
                arguments,
            } => serde_json::json!({
                "type": "tool_call", "id": id, "name": name, "arguments": arguments
            }),
        }
    }
}

/// A place in a step: a library block (its index among the step's library
/// blocks, and its Threads block id) or a Threads block, in push order.
#[derive(Debug, Clone, PartialEq)]
enum StepEntry {
    Library { index: usize, id: Uuid },
    Threads(StoredBlock),
}

#[derive(Debug, Clone, PartialEq, Default)]
struct DraftStep {
    entries: Vec<StepEntry>,
    library: Vec<LibraryBlock>,
    open: Option<usize>,
    finish: Option<(StopReason, Option<Usage>)>,
    model: Option<String>,
    /// The library's step, once its `finish` arrived and the library finished
    /// it.
    step: Option<Step>,
}

impl DraftStep {
    /// A library draft holding exactly what this step accepted so far.
    fn rebuild(&self) -> Result<StepDraft, MessageError> {
        let mut draft = StepDraft::new();
        for (index, block) in self.library.iter().enumerate() {
            match block {
                LibraryBlock::Text { text } => {
                    draft.apply(StreamEvent::BlockStart {
                        index,
                        kind: LibraryKind::Text,
                    })?;
                    if !text.is_empty() {
                        draft.apply(StreamEvent::TextDelta {
                            index,
                            text: text.clone(),
                        })?;
                    }
                }
                LibraryBlock::Thinking { text, signature } => {
                    draft.apply(StreamEvent::BlockStart {
                        index,
                        kind: LibraryKind::Thinking,
                    })?;
                    if !text.is_empty() {
                        draft.apply(StreamEvent::ThinkingDelta {
                            index,
                            text: text.clone(),
                        })?;
                    }
                    if let Some(signature) = signature {
                        draft.apply(StreamEvent::SignatureDelta {
                            index,
                            signature: signature.clone(),
                        })?;
                    }
                }
                LibraryBlock::RedactedThinking { data } => {
                    draft.apply(StreamEvent::BlockStart {
                        index,
                        kind: LibraryKind::RedactedThinking,
                    })?;
                    if !data.is_empty() {
                        draft.apply(StreamEvent::RedactedThinkingData {
                            index,
                            data: data.clone(),
                        })?;
                    }
                }
                LibraryBlock::Structured { json } => {
                    draft.apply(StreamEvent::BlockStart {
                        index,
                        kind: LibraryKind::Structured,
                    })?;
                    if !json.is_empty() {
                        draft.apply(StreamEvent::StructuredDelta {
                            index,
                            json_fragment: json.clone(),
                        })?;
                    }
                }
                LibraryBlock::ToolCall {
                    id,
                    name,
                    arguments,
                } => {
                    draft.apply(StreamEvent::ToolCallStart {
                        index,
                        id: id.clone(),
                        name: name.clone(),
                    })?;
                    if !arguments.is_empty() {
                        draft.apply(StreamEvent::ToolCallArgumentsDelta {
                            index,
                            json_fragment: arguments.clone(),
                        })?;
                    }
                }
            }
            if self.open != Some(index) {
                draft.apply(StreamEvent::BlockEnd { index })?;
            }
        }
        if let Some((stop_reason, usage)) = &self.finish {
            draft.apply(StreamEvent::Finish {
                stop_reason: stop_reason.clone(),
                usage: usage.clone(),
            })?;
        }
        Ok(draft)
    }

    /// Mirrors an event the library accepted.
    fn record(&mut self, event: StreamEvent, block_id: Option<Uuid>) {
        let push_open = |step: &mut Self, block: LibraryBlock| {
            let index = step.library.len();
            step.library.push(block);
            step.open = Some(index);
            if let Some(id) = block_id {
                step.entries.push(StepEntry::Library { index, id });
            }
        };
        match event {
            StreamEvent::BlockStart { kind, .. } => {
                let block = match kind {
                    LibraryKind::Text => LibraryBlock::Text {
                        text: String::new(),
                    },
                    LibraryKind::Thinking => LibraryBlock::Thinking {
                        text: String::new(),
                        signature: None,
                    },
                    LibraryKind::RedactedThinking => LibraryBlock::RedactedThinking {
                        data: String::new(),
                    },
                    LibraryKind::Structured => LibraryBlock::Structured {
                        json: String::new(),
                    },
                    // The library refuses it: never recorded.
                    LibraryKind::ToolCall => return,
                };
                push_open(self, block);
            }
            StreamEvent::ToolCallStart { id, name, .. } => push_open(
                self,
                LibraryBlock::ToolCall {
                    id,
                    name,
                    arguments: String::new(),
                },
            ),
            StreamEvent::TextDelta { text: more, .. } => {
                if let Some(LibraryBlock::Text { text }) = self.open_block() {
                    text.push_str(&more);
                }
            }
            StreamEvent::ThinkingDelta { text: more, .. } => {
                if let Some(LibraryBlock::Thinking { text, .. }) = self.open_block() {
                    text.push_str(&more);
                }
            }
            StreamEvent::SignatureDelta {
                signature: more, ..
            } => {
                if let Some(LibraryBlock::Thinking { signature, .. }) = self.open_block() {
                    signature.get_or_insert_with(String::new).push_str(&more);
                }
            }
            StreamEvent::RedactedThinkingData { data: more, .. } => {
                if let Some(LibraryBlock::RedactedThinking { data }) = self.open_block() {
                    data.push_str(&more);
                }
            }
            StreamEvent::StructuredDelta { json_fragment, .. } => {
                if let Some(LibraryBlock::Structured { json }) = self.open_block() {
                    json.push_str(&json_fragment);
                }
            }
            StreamEvent::ToolCallArgumentsDelta { json_fragment, .. } => {
                if let Some(LibraryBlock::ToolCall { arguments, .. }) = self.open_block() {
                    arguments.push_str(&json_fragment);
                }
            }
            StreamEvent::BlockEnd { .. } => self.open = None,
            StreamEvent::Finish { stop_reason, usage } => self.finish = Some((stop_reason, usage)),
        }
    }

    fn open_block(&mut self) -> Option<&mut LibraryBlock> {
        self.open.and_then(|open| self.library.get_mut(open))
    }

    fn model_id(&self) -> Result<Option<ModelId>, MessageError> {
        self.model.clone().map(ModelId::new).transpose()
    }

    /// Closes the step as the library would finish it: the open block ended,
    /// then a `finish`. `None` when the library cannot finish it.
    fn close_unfinished(&self) -> Option<Step> {
        let mut closing = self.clone();
        closing.open = None;
        let has_tool_call = closing
            .library
            .iter()
            .any(|block| matches!(block, LibraryBlock::ToolCall { .. }));
        closing.finish = Some((
            if has_tool_call {
                StopReason::AwaitingToolResults
            } else {
                StopReason::Other {
                    reason: INTERRUPTED.to_string(),
                }
            },
            None,
        ));
        closing.rebuild().ok()?.finish(self.model_id().ok()?).ok()
    }

    /// The step's blocks in push order: the library's JSON of a finished
    /// step's blocks, the partial preview of a streaming one's.
    fn blocks(&self, step: Option<&Step>) -> Vec<StoredBlock> {
        self.entries
            .iter()
            .enumerate()
            .map(|(position, entry)| match entry {
                StepEntry::Library { index, id } => StoredBlock {
                    id: *id,
                    index: position as i32,
                    kind: self.library[*index].kind(),
                    content: match step.and_then(|step| step.content().get(*index)) {
                        Some(block) => serde_json::to_value(block)
                            .unwrap_or_else(|_| self.library[*index].preview()),
                        None => self.library[*index].preview(),
                    },
                },
                StepEntry::Threads(block) => StoredBlock {
                    index: position as i32,
                    ..block.clone()
                },
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct DraftResults {
    blocks: Vec<StoredBlock>,
    results: Vec<ToolResult>,
}

#[derive(Debug, Clone, PartialEq)]
enum DraftItem {
    Step(Box<DraftStep>),
    ToolResults(DraftResults),
}

impl DraftItem {
    fn is_complete(&self) -> bool {
        match self {
            Self::Step(step) => step.step.is_some(),
            Self::ToolResults(_) => true,
        }
    }
}

/// What the stream of one message folded so far.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MessageDraft {
    items: Vec<DraftItem>,
    block_ids: Vec<Uuid>,
    refusal: Option<StreamRefusal>,
}

impl MessageDraft {
    /// The first refused chunk, if any: the message has failed there.
    pub fn refusal(&self) -> Option<&StreamRefusal> {
        self.refusal.as_ref()
    }

    /// The items as the stream holds them now: complete items `COMPLETED`, the
    /// item still streaming `STREAMING` with its blocks so far.
    pub fn preview(&self) -> Vec<StoredItem> {
        self.items
            .iter()
            .enumerate()
            .map(|(index, item)| stored_item(index, item, None))
            .collect()
    }

    fn refuse(&mut self, seq: u64, item_index: Option<u32>, code: &'static str) {
        self.refusal = Some(StreamRefusal {
            seq,
            item_index,
            code,
        });
    }

    /// Claims a block id for the message; `Err` names why it cannot be.
    fn claim(&mut self, id: Uuid) -> Result<(), &'static str> {
        if id.get_version_num() != 7 {
            return Err(chunk_error::BLOCK_ID_NOT_UUIDV7);
        }
        if self.claim_block_id(id) {
            Ok(())
        } else {
            Err(chunk_error::BLOCK_ID_REUSED)
        }
    }

    fn claim_block_id(&mut self, id: Uuid) -> bool {
        if self.block_ids.contains(&id) {
            return false;
        }
        self.block_ids.push(id);
        true
    }

    /// The library turn of the complete items, which must hold.
    fn check_turn(&self) -> Result<(), MessageError> {
        let mut turn: Option<Turn> = None;
        for item in &self.items {
            match item {
                DraftItem::Step(draft) => {
                    let Some(step) = &draft.step else { continue };
                    match &mut turn {
                        None => turn = Some(Turn::new(TurnId::new("draft")?, None, step.clone())),
                        Some(turn) => turn.push_step(step.clone())?,
                    }
                }
                DraftItem::ToolResults(results) => match &mut turn {
                    None => return Err(MessageError::TurnMustStartWithStep),
                    Some(turn) => turn.push_results(results.results.iter().cloned())?,
                },
            }
        }
        Ok(())
    }
}

fn stored_item(
    index: usize,
    item: &DraftItem,
    closing: Option<(MessageStatus, Option<&Step>)>,
) -> StoredItem {
    match item {
        DraftItem::Step(draft) => {
            let step = draft.step.as_ref().or(closing.and_then(|(_, step)| step));
            let status = match (draft.step.is_some(), closing) {
                (true, _) => MessageStatus::Completed,
                (false, Some((status, _))) => status,
                (false, None) => MessageStatus::Streaming,
            };
            StoredItem {
                index: index as i32,
                kind: ItemKind::Step,
                status,
                stop_reason: step.and_then(|step| serde_json::to_value(step.stop_reason()).ok()),
                usage: step
                    .and_then(|step| step.usage())
                    .and_then(|usage| serde_json::to_value(usage).ok()),
                model: draft.model.clone(),
                blocks: draft.blocks(step),
            }
        }
        DraftItem::ToolResults(results) => StoredItem {
            index: index as i32,
            kind: ItemKind::ToolResults,
            status: MessageStatus::Completed,
            stop_reason: None,
            usage: None,
            model: None,
            blocks: results.blocks.clone(),
        },
    }
}

/// Folds one raw chunk, in sequence. Never fails: a chunk the contract or the
/// library refuses is recorded as the stream's refusal, and nothing after it
/// folds.
pub fn fold(draft: &mut MessageDraft, seq: u64, raw: serde_json::Value) {
    if draft.refusal.is_some() {
        return;
    }
    let Ok(chunk) = serde_json::from_value::<Chunk>(raw) else {
        draft.refuse(seq, None, chunk_error::UNREADABLE);
        return;
    };
    let item_index = chunk.item_index;
    if let Err(code) = fold_chunk(draft, chunk) {
        draft.refuse(seq, Some(item_index), code);
    }
}

fn fold_chunk(draft: &mut MessageDraft, chunk: Chunk) -> Result<(), &'static str> {
    let index = chunk.item_index as usize;
    let latest = draft.items.len().checked_sub(1);
    let opens_item =
        index == draft.items.len() && draft.items.last().is_none_or(DraftItem::is_complete);
    match chunk.part {
        ChunkPart::StepEvent {
            event,
            block_id,
            model,
        } => {
            if opens_item {
                draft.items.push(DraftItem::Step(Box::new(DraftStep {
                    model,
                    ..DraftStep::default()
                })));
            } else if Some(index) != latest {
                return Err(chunk_error::ITEM_OUT_OF_ORDER);
            } else if model.is_some() {
                return Err(chunk_error::MODEL_NOT_FIRST);
            }
            let opens_block = matches!(
                event,
                StreamEvent::BlockStart { .. } | StreamEvent::ToolCallStart { .. }
            );
            match (opens_block, block_id) {
                (true, None) => return Err(chunk_error::BLOCK_ID_REQUIRED),
                (false, Some(_)) => return Err(chunk_error::BLOCK_ID_NOT_ALLOWED),
                (true, Some(id)) => draft.claim(id)?,
                _ => {}
            }
            let DraftItem::Step(step) = &mut draft.items[index] else {
                return Err(chunk_error::ITEM_COMPLETE);
            };
            if step.step.is_some() {
                return Err(chunk_error::ITEM_COMPLETE);
            }
            let mut library = step.rebuild().map_err(|e| library_error_code(&e))?;
            library
                .apply(event.clone())
                .map_err(|e| library_error_code(&e))?;
            let finishing = matches!(event, StreamEvent::Finish { .. });
            step.record(event, block_id);
            if finishing {
                let model = step.model_id().map_err(|e| library_error_code(&e))?;
                let finished = library.finish(model).map_err(|e| library_error_code(&e))?;
                step.step = Some(finished);
                draft.check_turn().map_err(|e| library_error_code(&e))?;
            }
            Ok(())
        }
        ChunkPart::ToolResults { blocks } => {
            if !opens_item {
                return Err(chunk_error::ITEM_OUT_OF_ORDER);
            }
            let mut results = DraftResults::default();
            let mut collected = ToolResults::empty();
            for (position, block) in blocks.iter().enumerate() {
                draft.claim(block.id)?;
                if block.kind().is_library() {
                    let (result, json) = tool_result_block(block)
                        .map_err(|_| chunk_error::NOT_A_TOOL_RESULTS_BLOCK)?;
                    collected
                        .push(result.clone())
                        .map_err(|e| library_error_code(&e))?;
                    results.results.push(result);
                    results.blocks.push(StoredBlock {
                        id: block.id,
                        index: position as i32,
                        kind: BlockKind::ToolResult,
                        content: json,
                    });
                } else {
                    results.blocks.push(
                        threads_block(block, position)
                            .map_err(|_| chunk_error::NOT_A_TOOL_RESULTS_BLOCK)?,
                    );
                }
            }
            draft.items.push(DraftItem::ToolResults(results));
            draft.check_turn().map_err(|e| library_error_code(&e))
        }
        ChunkPart::Block { block } => append_threads_block(draft, index, latest, &block),
    }
}

/// A Threads block takes the next index of its item, which must be the latest.
fn append_threads_block(
    draft: &mut MessageDraft,
    index: usize,
    latest: Option<usize>,
    block: &Block,
) -> Result<(), &'static str> {
    if block.kind().is_library() {
        return Err(chunk_error::NOT_A_THREADS_BLOCK);
    }
    if Some(index) != latest {
        return Err(chunk_error::ITEM_OUT_OF_ORDER);
    }
    draft.claim(block.id)?;
    match &mut draft.items[index] {
        DraftItem::Step(step) => {
            let stored = threads_block(block, step.entries.len())
                .map_err(|_| chunk_error::NOT_A_THREADS_BLOCK)?;
            step.entries.push(StepEntry::Threads(stored));
        }
        DraftItem::ToolResults(results) => {
            let stored = threads_block(block, results.blocks.len())
                .map_err(|_| chunk_error::NOT_A_THREADS_BLOCK)?;
            results.blocks.push(stored);
        }
    }
    Ok(())
}

/// What closes a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseKind {
    /// `message.complete`: the writer says the stream is whole.
    Complete,
    /// `message.fail`, or the watch: the writer, or its silence, failed it.
    Fail,
    /// `message.cancel`, typically a user's stop.
    Cancel,
}

/// The content a stream is stored with when it closes, and the status it
/// closes with.
#[derive(Debug, Clone, PartialEq)]
pub struct Closed {
    pub status: MessageStatus,
    pub items: Vec<StoredItem>,
    /// Why a completion failed instead: the stream's refusal, or the library's
    /// verdict on what the writer called whole.
    pub failure: Option<&'static str>,
}

/// Closes a stream.
///
/// A completion of a stream the library accepted whole — every item complete,
/// the turn starting with a step and not left awaiting tool results — is
/// `COMPLETED`. Anything else is `FAILED` (`CANCELLED` for a cancel of a stream
/// with no refusal): every complete item is kept; the step still streaming is
/// kept when the library can finish it, dropped otherwise; and every tool call
/// still without a result is answered with an error result saying it was
/// interrupted, so that the conversation always loads and renders.
pub fn close(draft: &MessageDraft, kind: CloseKind) -> Closed {
    let all_complete = draft.items.iter().all(DraftItem::is_complete);
    if kind == CloseKind::Complete && draft.refusal.is_none() && all_complete {
        let items: Vec<StoredItem> = draft
            .items
            .iter()
            .enumerate()
            .map(|(index, item)| stored_item(index, item, None))
            .collect();
        match content::turn_of(Uuid::nil(), None, &items) {
            Ok(turn) if !matches!(turn.state(), TurnState::AwaitingToolResults { .. }) => {
                return Closed {
                    status: MessageStatus::Completed,
                    items,
                    failure: None,
                };
            }
            Ok(_) => {
                return salvage(
                    draft,
                    MessageStatus::Failed,
                    Some(content::reason::TURN_AWAITING_TOOL_RESULTS),
                );
            }
            Err(error) => {
                return salvage(
                    draft,
                    MessageStatus::Failed,
                    Some(library_error_code(&error)),
                );
            }
        }
    }
    let refusal = draft.refusal.as_ref().map(|refusal| refusal.code);
    let status = match (kind, refusal) {
        (CloseKind::Cancel, None) => MessageStatus::Cancelled,
        _ => MessageStatus::Failed,
    };
    let failure = match kind {
        CloseKind::Complete => Some(refusal.unwrap_or("missing_finish")),
        CloseKind::Fail | CloseKind::Cancel => refusal,
    };
    salvage(draft, status, failure)
}

fn salvage(draft: &MessageDraft, status: MessageStatus, failure: Option<&'static str>) -> Closed {
    let mut items: Vec<StoredItem> = Vec::with_capacity(draft.items.len() + 1);
    for (index, item) in draft.items.iter().enumerate() {
        let candidate = match item {
            DraftItem::Step(step) if step.step.is_none() => match step.close_unfinished() {
                Some(finished) => stored_item(index, item, Some((status, Some(&finished)))),
                None => break,
            },
            _ => stored_item(index, item, None),
        };
        // An item the stream's refusal fell on may break the turn: it is
        // dropped, with everything after it, so that what is kept loads.
        items.push(candidate);
        if content::turn_of(Uuid::nil(), None, &items).is_err() {
            items.pop();
            break;
        }
    }
    answer_interrupted_calls(&mut items, status);
    Closed {
        status,
        items,
        failure,
    }
}

/// Every tool call of the turn's last step still without a result receives an
/// error result saying it was interrupted, in the trailing tool results item
/// (opened when there is none).
fn answer_interrupted_calls(items: &mut Vec<StoredItem>, status: MessageStatus) {
    let Ok(turn) = content::turn_of(Uuid::nil(), None, items) else {
        return;
    };
    let TurnState::AwaitingToolResults { pending } = turn.state() else {
        return;
    };
    let Some(step) = turn.items().iter().rev().find_map(|item| match item {
        contract_threads::llm::TurnItem::Step(step) => Some(step),
        contract_threads::llm::TurnItem::ToolResults(_) => None,
    }) else {
        return;
    };
    let text = Text::new(INTERRUPTED).expect("a constant, non-blank text");
    let interrupted: Vec<ToolResult> = step
        .tool_calls()
        .filter(|call| pending.contains(&call.id))
        .map(|call| {
            ToolResult::new(
                call.id.clone(),
                call.name.clone(),
                vec![ToolResultBlock::text(text.clone())],
                true,
            )
        })
        .collect();
    if items
        .last()
        .is_none_or(|item| item.kind != ItemKind::ToolResults)
    {
        items.push(StoredItem {
            index: items.len() as i32,
            kind: ItemKind::ToolResults,
            status,
            stop_reason: None,
            usage: None,
            model: None,
            blocks: Vec::new(),
        });
    }
    let trailing = items
        .last_mut()
        .expect("a tool results item was just ensured");
    trailing.status = status;
    for result in interrupted {
        trailing.blocks.push(StoredBlock {
            id: Uuid::now_v7(),
            index: trailing.blocks.len() as i32,
            kind: BlockKind::ToolResult,
            content: serde_json::to_value(&result).unwrap_or(serde_json::Value::Null),
        });
    }
}

/// Whether a closed message ever reached its first step: a message failed
/// before it is left out of a runner's conversation.
pub fn has_a_step(items: &[StoredItem]) -> bool {
    items.iter().any(|item| item.kind == ItemKind::Step)
}

#[cfg(test)]
#[path = "draft_tests.rs"]
mod tests;
