//! The content of a message, judged by the conversation model of
//! `br-llm-messages` and stored in Threads' decomposition: a user message's
//! blocks, an AI message's items and their blocks.
//!
//! A library block is stored as the library's own JSON for it — re-serialized
//! from the parsed value, so that what Threads keeps is exactly what the library
//! loads, its strings (a thinking, its signature, a redacted thinking) byte for
//! byte. A Threads block keeps its typed content. The library's rules ignore
//! Threads blocks, and a runner never receives them.
//!
//! The same module turns stored content back into the library's model, for the
//! conversation a runner reads: a user message is the input, and an AI message
//! the turn, whose id is the message's id.
//!
//! The library judges one conversation; Threads holds a tree of them, one per
//! branch. The rule that spans a conversation — a tool call id is used once in
//! it — is held for every branch through a message ([`check_calls_unused`]),
//! so that whichever branch a runner reads loads.

use std::collections::{BTreeSet, HashSet};

use contract_threads::llm::{
    AssistantBlock, Author, InputId, MessageError, ModelId, RedactedThinking, Signature, Step,
    StopReason, Text, ToolCallId, ToolName, ToolResult, ToolResults, Turn, TurnId, TurnState,
    Usage, UserBlock, UserInput, UserSource,
};
use contract_threads::{
    Block, BlockBody, BlockKind, Item, MessageInvalidError, MessageStatus, ReasonCode,
};
use uuid::Uuid;

use super::state::{ItemKind, StoredBlock, StoredItem};
use crate::command::Refusal;

/// The code of a refusal of the library's rules: what a `MESSAGE_INVALID`
/// names in its `error` param, the vocabulary `contract-threads` publishes.
pub fn library_error_code(error: &MessageError) -> &'static str {
    MessageInvalidError::of_library(error).as_str()
}

/// What library JSON is expected to be where Threads reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    UserBlock,
    AssistantBlock,
    ToolResult,
    StreamEvent,
}

/// The library's refusal of a value named by library JSON of `shape` that
/// does not load: each value the JSON names is built with the library's own
/// constructor, so that the refusal — an invalid tool name, a blank text, an
/// empty signature — is the library's. `None` when every value builds: the
/// JSON is then not the library's shape.
pub fn value_refusal(json: &serde_json::Value, shape: Shape) -> Option<MessageError> {
    type Build = fn(String) -> Result<(), MessageError>;
    let built = |field: &str, build: Build| {
        json.get(field)
            .and_then(serde_json::Value::as_str)
            .map(|value| build(value.to_owned()))
    };
    let text: Build = |value| Text::new(value).map(drop);
    let call_id: Build = |value| ToolCallId::new(value).map(drop);
    let tool_name: Build = |value| ToolName::new(value).map(drop);
    let signature: Build = |value| Signature::new(value).map(drop);
    let redacted: Build = |value| RedactedThinking::new(value).map(drop);
    let kind = json.get("type").and_then(serde_json::Value::as_str);
    let checks: Vec<Option<Result<(), MessageError>>> = match (shape, kind) {
        (Shape::UserBlock | Shape::AssistantBlock, Some("text")) => vec![built("text", text)],
        (Shape::AssistantBlock, Some("thinking")) => vec![built("signature", signature)],
        (Shape::AssistantBlock, Some("redacted_thinking")) => vec![built("data", redacted)],
        (Shape::AssistantBlock, Some("tool_call"))
        | (Shape::StreamEvent, Some("tool_call_start")) => {
            vec![built("id", call_id), built("name", tool_name)]
        }
        // A tool result's content blocks are text blocks.
        (Shape::ToolResult, _) => {
            let content = json
                .get("content")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .map(|block| value_refusal(block, Shape::UserBlock).map(Err));
            [
                built("tool_call_id", call_id),
                built("tool_name", tool_name),
            ]
            .into_iter()
            .chain(content)
            .collect()
        }
        _ => Vec::new(),
    };
    checks.into_iter().flatten().find_map(Result::err)
}

/// Why content was refused: one of Threads' own rules, or the library's.
#[derive(Debug, Clone, PartialEq)]
pub enum ContentFault {
    Threads(MessageInvalidError),
    Library(MessageError),
}

impl ContentFault {
    /// Library JSON of `shape` that does not load: the library's refusal of a
    /// value it names, `block_content_invalid` otherwise.
    pub fn unreadable(json: &serde_json::Value, shape: Shape) -> Self {
        match value_refusal(json, shape) {
            Some(error) => Self::Library(error),
            None => Self::Threads(MessageInvalidError::BlockContentInvalid),
        }
    }

    pub fn code(&self) -> MessageInvalidError {
        match self {
            Self::Threads(code) => *code,
            Self::Library(error) => MessageInvalidError::of_library(error),
        }
    }

    /// The `MESSAGE_INVALID` refusal it is.
    pub fn refusal(&self) -> Refusal {
        match self {
            Self::Threads(code) => invalid(*code),
            Self::Library(error) => invalid_by_library(error),
        }
    }
}

/// The tool call ids already used on a branch through a message: by the
/// messages before it and after it that a conversation holds.
pub type UsedCallIds = HashSet<ToolCallId>;

/// The tool call ids of stored items, in order.
pub fn call_ids(items: &[StoredItem]) -> Vec<ToolCallId> {
    items
        .iter()
        .flat_map(|item| item.blocks.iter())
        .filter(|block| block.kind == BlockKind::ToolCall)
        .filter_map(|block| serde_json::from_value::<AssistantBlock>(block.content.clone()).ok())
        .filter_map(|block| block.as_tool_call().map(|call| call.id.clone()))
        .collect()
}

/// The library's conversation rule, across the branches through a message: none
/// of its tool call ids is used by a message before or after it.
pub fn check_calls_unused(items: &[StoredItem], used: &UsedCallIds) -> Result<(), MessageError> {
    match call_ids(items).into_iter().find(|id| used.contains(id)) {
        Some(id) => Err(MessageError::DuplicateToolCallIdInConversation { id }),
        None => Ok(()),
    }
}

/// A `MESSAGE_INVALID` refusal for a library error, with the detail the
/// error names: the id it is about, the field it refuses.
pub fn invalid_by_library(error: &MessageError) -> Refusal {
    let refusal = invalid(MessageInvalidError::of_library(error));
    match error {
        MessageError::DuplicateToolCallId { id }
        | MessageError::DuplicateToolCallIdInTurn { id }
        | MessageError::DuplicateToolCallIdInConversation { id }
        | MessageError::DuplicateToolResultId { id }
        | MessageError::ToolNameMismatch { id, .. } => refusal.with("tool_call_id", id.as_str()),
        MessageError::DuplicateEntryId { id } => refusal.with("entry_id", id),
        MessageError::Blank { field } | MessageError::InvalidJson { field, .. } => {
            refusal.with("field", field)
        }
        _ => refusal,
    }
}

/// A `MESSAGE_INVALID` refusal under `code`.
pub fn invalid(code: MessageInvalidError) -> Refusal {
    Refusal::new(ReasonCode::MessageInvalid).with("error", code.as_str())
}

/// The library block of a contract block, by the library's rules, as the
/// library re-serializes it. Its kind must be the one its content says.
pub fn assistant_block(block: &Block) -> Result<(AssistantBlock, serde_json::Value), Refusal> {
    let Some(raw) = block.body.library() else {
        return Err(invalid(MessageInvalidError::BlockKindNotAllowed).with("block_id", block.id));
    };
    let parsed = raw.to_assistant().map_err(|_| {
        ContentFault::unreadable(raw.as_json(), Shape::AssistantBlock)
            .refusal()
            .with("block_id", block.id)
    })?;
    let expected = match &parsed {
        AssistantBlock::Text { .. } => Some(BlockKind::Text),
        AssistantBlock::Thinking(_) => Some(BlockKind::Thinking),
        AssistantBlock::RedactedThinking(_) => Some(BlockKind::RedactedThinking),
        AssistantBlock::Structured { .. } => Some(BlockKind::Structured),
        AssistantBlock::ToolCall(_) => Some(BlockKind::ToolCall),
        // A block a later library adds has no Threads kind.
        _ => None,
    };
    let content_invalid =
        || invalid(MessageInvalidError::BlockContentInvalid).with("block_id", block.id);
    if expected != Some(block.kind()) {
        return Err(content_invalid());
    }
    let json = serde_json::to_value(&parsed).map_err(|_| content_invalid())?;
    Ok((parsed, json))
}

/// The library tool result of a `TOOL_RESULT` block.
pub fn tool_result_block(block: &Block) -> Result<(ToolResult, serde_json::Value), Refusal> {
    tool_result_of(block).map_err(|fault| fault.refusal().with("block_id", block.id))
}

/// The library tool result of a `TOOL_RESULT` block, or why it is not one.
pub fn tool_result_of(block: &Block) -> Result<(ToolResult, serde_json::Value), ContentFault> {
    let Some(raw) = block
        .body
        .library()
        .filter(|_| block.kind() == BlockKind::ToolResult)
    else {
        return Err(ContentFault::Threads(
            MessageInvalidError::BlockKindNotAllowed,
        ));
    };
    let parsed = raw
        .to_tool_result()
        .map_err(|_| ContentFault::unreadable(raw.as_json(), Shape::ToolResult))?;
    let json = serde_json::to_value(&parsed)
        .map_err(|_| ContentFault::Threads(MessageInvalidError::BlockContentInvalid))?;
    Ok((parsed, json))
}

/// The stored form of a Threads block.
pub fn threads_block(block: &Block, index: usize) -> Result<StoredBlock, Refusal> {
    let refuse = |code: MessageInvalidError| invalid(code).with("block_id", block.id);
    let content = match &block.body {
        BlockBody::Artifact(content) => serde_json::to_value(content),
        BlockBody::ThreadReference(content) => serde_json::to_value(content),
        BlockBody::JobReference(content) => serde_json::to_value(content),
        BlockBody::Citation(content) => serde_json::to_value(content),
        BlockBody::Error(content) | BlockBody::Warning(content) => serde_json::to_value(content),
        _ => return Err(refuse(MessageInvalidError::BlockKindNotAllowed)),
    }
    .map_err(|_| refuse(MessageInvalidError::BlockContentInvalid))?;
    Ok(StoredBlock {
        id: block.id,
        index: index as i32,
        kind: block.kind(),
        content,
    })
}

/// The artifacts a list of blocks names in its `ARTIFACT` blocks.
pub fn named_artifacts<'a>(blocks: impl IntoIterator<Item = &'a StoredBlock>) -> Vec<Uuid> {
    blocks
        .into_iter()
        .filter(|block| block.kind == BlockKind::Artifact)
        .filter_map(|block| {
            block
                .content
                .get("artifact_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|raw| Uuid::parse_str(raw).ok())
        })
        .collect()
}

/// Every block id of a list of blocks appears once.
fn unique_ids<'a>(ids: impl IntoIterator<Item = &'a Uuid>) -> Result<(), Refusal> {
    let mut seen = BTreeSet::new();
    for id in ids {
        crate::command::minted("block_id", *id)?;
        if !seen.insert(*id) {
            return Err(Refusal::new(ReasonCode::KeyReused).with("block_id", id));
        }
    }
    Ok(())
}

/// The input id of a user message: its own id.
pub fn input_id(message: Uuid) -> InputId {
    InputId::new(message.to_string()).expect("a UUID's text is never empty")
}

/// The turn id of an AI message: its own id.
pub fn turn_id(message: Uuid) -> TurnId {
    TurnId::new(message.to_string()).expect("a UUID's text is never empty")
}

/// The user input of message `message`, judged: text blocks by the library's
/// rules, Threads blocks other than `ARTIFACT`; at least one text.
pub fn user_blocks(message: Uuid, blocks: &[Block]) -> Result<Vec<StoredBlock>, Refusal> {
    unique_ids(blocks.iter().map(|block| &block.id))?;
    let mut stored = Vec::with_capacity(blocks.len());
    let mut library = Vec::new();
    for (index, block) in blocks.iter().enumerate() {
        match block.kind() {
            BlockKind::Text => {
                let raw = block.body.library().expect("TEXT is a library kind");
                let parsed = raw.to_user().map_err(|_| {
                    ContentFault::unreadable(raw.as_json(), Shape::UserBlock)
                        .refusal()
                        .with("block_id", block.id)
                })?;
                if !matches!(parsed, UserBlock::Text { .. }) {
                    return Err(invalid(MessageInvalidError::BlockContentInvalid)
                        .with("block_id", block.id));
                }
                stored.push(StoredBlock {
                    id: block.id,
                    index: index as i32,
                    kind: BlockKind::Text,
                    content: serde_json::to_value(&parsed).map_err(|_| {
                        invalid(MessageInvalidError::BlockContentInvalid).with("block_id", block.id)
                    })?,
                });
                library.push(parsed);
            }
            BlockKind::Artifact => {
                return Err(
                    invalid(MessageInvalidError::ArtifactInUserMessage).with("block_id", block.id)
                );
            }
            kind if kind.is_library() => {
                return Err(invalid(MessageInvalidError::BlockKindNotAllowed)
                    .with("block_id", block.id)
                    .with("kind", kind.as_str()));
            }
            _ => stored.push(threads_block(block, index)?),
        }
    }
    UserInput::new(input_id(message), UserSource::Human, None, library)
        .map_err(|e| invalid_by_library(&e))?;
    Ok(stored)
}

/// The library step of a stored step item.
fn library_step(item: &StoredItem) -> Result<Step, MessageError> {
    let stop_reason: StopReason = item
        .stop_reason
        .clone()
        .and_then(|json| serde_json::from_value(json).ok())
        .ok_or(MessageError::MissingFinish)?;
    let usage: Option<Usage> = item
        .usage
        .clone()
        .and_then(|json| serde_json::from_value(json).ok());
    let content: Vec<AssistantBlock> = item
        .blocks
        .iter()
        .filter(|block| block.kind.is_library())
        .filter_map(|block| serde_json::from_value(block.content.clone()).ok())
        .collect();
    let model = item.model.clone().map(ModelId::new).transpose()?;
    Step::new(content, stop_reason, usage, model)
}

fn library_results(item: &StoredItem) -> Vec<ToolResult> {
    item.blocks
        .iter()
        .filter(|block| block.kind == BlockKind::ToolResult)
        .filter_map(|block| serde_json::from_value(block.content.clone()).ok())
        .collect()
}

/// The library turn of stored items, by the library's rules — those of a turn
/// loaded from JSON: a tool results item holds at least one result.
pub fn turn_of(
    id: Uuid,
    author: Option<Author>,
    items: &[StoredItem],
) -> Result<Turn, MessageError> {
    let mut items = items.iter();
    let first = match items.next() {
        Some(item) if item.kind == ItemKind::Step => library_step(item)?,
        _ => return Err(MessageError::TurnMustStartWithStep),
    };
    let mut turn = Turn::new(turn_id(id), author, first);
    for item in items {
        match item.kind {
            ItemKind::Step => turn.push_step(library_step(item)?)?,
            ItemKind::ToolResults => {
                let results = library_results(item);
                if results.is_empty() {
                    return Err(MessageError::EmptyToolResults);
                }
                turn.push_results(results)?
            }
        }
    }
    Ok(turn)
}

/// The library user input of message `message`'s stored blocks, under its
/// author.
pub fn user_input_of(
    message: Uuid,
    blocks: &[StoredBlock],
    author: Author,
) -> Result<UserInput, MessageError> {
    let content: Vec<UserBlock> = blocks
        .iter()
        .filter(|block| block.kind.is_library())
        .filter_map(|block| serde_json::from_value(block.content.clone()).ok())
        .collect();
    UserInput::new(input_id(message), UserSource::Human, Some(author), content)
}

/// An AI turn given whole, judged: every step and every tool results item by
/// the library's rules, in order, the turn starting with a step and not left
/// awaiting tool results. Its items are stored `COMPLETED`.
pub fn ai_items(message_id: Uuid, items: &[Item]) -> Result<Vec<StoredItem>, Refusal> {
    let all_ids = items.iter().flat_map(|item| match item {
        Item::Step(step) => step.blocks.iter(),
        Item::ToolResults(results) => results.blocks.iter(),
    });
    unique_ids(all_ids.map(|block| &block.id))?;
    let mut stored = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        stored.push(match item {
            Item::Step(step) => {
                let mut blocks = Vec::with_capacity(step.blocks.len());
                let mut content = Vec::new();
                for (position, block) in step.blocks.iter().enumerate() {
                    if block.kind().is_library() {
                        let (parsed, json) = assistant_block(block)?;
                        content.push(parsed);
                        blocks.push(StoredBlock {
                            id: block.id,
                            index: position as i32,
                            kind: block.kind(),
                            content: json,
                        });
                    } else {
                        blocks.push(threads_block(block, position)?);
                    }
                }
                let model = match &step.model {
                    Some(model) => Some(
                        ModelId::new(model.clone())
                            .map_err(|e| invalid_by_library(&e).with("item_index", index))?,
                    ),
                    None => None,
                };
                let library =
                    Step::new(content, step.stop_reason.clone(), step.usage.clone(), model)
                        .map_err(|e| invalid_by_library(&e).with("item_index", index))?;
                StoredItem {
                    index: index as i32,
                    kind: ItemKind::Step,
                    status: MessageStatus::Completed,
                    stop_reason: serde_json::to_value(library.stop_reason()).ok(),
                    usage: library
                        .usage()
                        .and_then(|usage| serde_json::to_value(usage).ok()),
                    model: library.model().map(|model| model.as_str().to_owned()),
                    blocks,
                }
            }
            Item::ToolResults(results) => {
                let mut blocks = Vec::with_capacity(results.blocks.len());
                let mut collected = ToolResults::empty();
                for (position, block) in results.blocks.iter().enumerate() {
                    if block.kind().is_library() {
                        let (result, json) = tool_result_block(block)?;
                        collected
                            .push(result)
                            .map_err(|e| invalid_by_library(&e).with("item_index", index))?;
                        blocks.push(StoredBlock {
                            id: block.id,
                            index: position as i32,
                            kind: BlockKind::ToolResult,
                            content: json,
                        });
                    } else {
                        blocks.push(threads_block(block, position)?);
                    }
                }
                StoredItem {
                    index: index as i32,
                    kind: ItemKind::ToolResults,
                    status: MessageStatus::Completed,
                    stop_reason: None,
                    usage: None,
                    model: None,
                    blocks,
                }
            }
        });
    }
    let turn = turn_of(message_id, None, &stored).map_err(|e| invalid_by_library(&e))?;
    if matches!(turn.state(), TurnState::AwaitingToolResults { .. }) {
        return Err(invalid(MessageInvalidError::TurnAwaitingToolResults));
    }
    Ok(stored)
}

#[cfg(test)]
#[path = "content_tests.rs"]
mod tests;
