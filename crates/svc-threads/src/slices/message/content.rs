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
use contract_threads::{Block, BlockBody, BlockKind, Item, MessageStatus, ReasonCode};
use uuid::Uuid;

use super::state::{ItemKind, StoredBlock, StoredItem};
use crate::command::Refusal;

/// The code of a library refusal Threads does not know: the library may grow
/// its refusals in a later version (`MessageError` is non-exhaustive).
pub const UNKNOWN_LIBRARY_ERROR: &str = "library_refusal";

/// The stable code of a refusal of the library's rules: what a
/// `MESSAGE_INVALID` names in its `error` param. A refusal a later library
/// adds maps to [`UNKNOWN_LIBRARY_ERROR`] until it is named here: on every
/// library upgrade, its changelog's new `MessageError` variants get a code.
pub fn library_error_code(error: &MessageError) -> &'static str {
    match error {
        MessageError::Blank { .. } => "blank",
        MessageError::InvalidToolName => "invalid_tool_name",
        MessageError::InvalidBase64 => "invalid_base64",
        MessageError::EmptyStep => "empty_step",
        MessageError::ToolCallNotAtTail => "tool_call_not_at_tail",
        MessageError::AwaitingToolResultsWithoutCalls => "awaiting_tool_results_without_calls",
        MessageError::ToolCallsWithoutAwaiting => "tool_calls_without_awaiting",
        MessageError::DuplicateToolCallId { .. } => "duplicate_tool_call_id",
        MessageError::DuplicateToolCallIdInTurn { .. } => "duplicate_tool_call_id_in_turn",
        MessageError::DuplicateToolCallIdInConversation { .. } => {
            "duplicate_tool_call_id_in_conversation"
        }
        MessageError::EmptyUserContent => "empty_user_content",
        MessageError::DuplicateToolResultId { .. } => "duplicate_tool_result_id",
        MessageError::ToolResultNotPending { .. } => "tool_result_not_pending",
        MessageError::ToolNameMismatch { .. } => "tool_name_mismatch",
        MessageError::EmptyToolResults => "empty_tool_results",
        MessageError::TurnNotAwaitingResults { .. } => "turn_not_awaiting_results",
        MessageError::TurnNotAwaitingStep { .. } => "turn_not_awaiting_step",
        MessageError::TurnMustStartWithStep => "turn_must_start_with_step",
        MessageError::DuplicateEntryId { .. } => "duplicate_entry_id",
        MessageError::TurnNotFound { .. } => "turn_not_found",
        MessageError::SchemaMismatch { .. } => "schema_mismatch",
        MessageError::TurnAwaitingResults => "turn_awaiting_results",
        MessageError::RenderStartsWithAssistant => "render_starts_with_assistant",
        MessageError::EmptyRender => "empty_render",
        MessageError::NoOpenBlock { .. } => "no_open_block",
        MessageError::BlockStillOpen { .. } => "block_still_open",
        MessageError::UnexpectedBlockIndex { .. } => "unexpected_block_index",
        MessageError::BlockKindMismatch { .. } => "block_kind_mismatch",
        MessageError::ToolCallNeedsStart => "tool_call_needs_start",
        MessageError::EventAfterFinish => "event_after_finish",
        MessageError::MissingFinish => "missing_finish",
        MessageError::InvalidJson { .. } => "invalid_json",
        MessageError::WireListEmpty => "wire_list_empty",
        MessageError::WireListNotStartingWithUser { .. } => "wire_list_not_starting_with_user",
        MessageError::WireToolCallUnanswered { .. } => "wire_tool_call_unanswered",
        MessageError::WireToolCallAnsweredTwice { .. } => "wire_tool_call_answered_twice",
        MessageError::WireToolResultUnmatched { .. } => "wire_tool_result_unmatched",
        MessageError::WireToolCallIdRepeated { .. } => "wire_tool_call_id_repeated",
        _ => UNKNOWN_LIBRARY_ERROR,
    }
}

/// The library's refusal of a value named by library JSON that does not load
/// (a block, a tool result, a stream event): each value the JSON names is
/// built with the library's own constructor, so that the refusal — an invalid
/// tool name, a blank text, an empty signature — is the library's.
/// `None` when every value builds: the JSON is then not the library's shape.
pub fn value_refusal(json: &serde_json::Value) -> Option<MessageError> {
    type Build = fn(String) -> Result<(), MessageError>;
    let built = |field: &str, build: Build| {
        json.get(field)
            .and_then(serde_json::Value::as_str)
            .map(|value| build(value.to_owned()))
    };
    let text: Build = |value| Text::new(value).map(drop);
    let call_id: Build = |value| ToolCallId::new(value).map(drop);
    let tool_name: Build = |value| ToolName::new(value).map(drop);
    let checks: Vec<Option<Result<(), MessageError>>> =
        match json.get("type").and_then(serde_json::Value::as_str) {
            Some("text") => vec![built("text", text)],
            Some("thinking") => vec![built("signature", |value| Signature::new(value).map(drop))],
            Some("redacted_thinking") => {
                vec![built("data", |value| {
                    RedactedThinking::new(value).map(drop)
                })]
            }
            Some("tool_call" | "tool_call_start") => {
                vec![built("id", call_id), built("name", tool_name)]
            }
            // A tool result is the one library value without a `type`; its
            // content blocks are judged as blocks.
            None => {
                let content = json
                    .get("content")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|block| value_refusal(block).map(Err));
                [
                    built("tool_call_id", call_id),
                    built("tool_name", tool_name),
                ]
                .into_iter()
                .chain(content)
                .collect()
            }
            Some(_) => Vec::new(),
        };
    checks.into_iter().flatten().find_map(Result::err)
}

/// Why library JSON does not load, as a `MESSAGE_INVALID` names it: the
/// library's code for a value it refuses, `block_content_invalid` otherwise.
pub fn unreadable_code(json: &serde_json::Value) -> &'static str {
    value_refusal(json)
        .map(|error| library_error_code(&error))
        .unwrap_or(reason::BLOCK_CONTENT_INVALID)
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

/// Threads' own reasons to refuse content the library would not judge.
pub mod reason {
    /// A block of a kind its place does not admit (a tool result in a user
    /// input, a thinking in a tool results item, …).
    pub const BLOCK_KIND_NOT_ALLOWED: &str = "block_kind_not_allowed";
    /// A library block whose content is not the library's JSON for its kind.
    pub const BLOCK_CONTENT_INVALID: &str = "block_content_invalid";
    /// An `ARTIFACT` block in a user message: only runners and AIs produce
    /// artifacts.
    pub const ARTIFACT_IN_USER_MESSAGE: &str = "artifact_in_user_message";
    /// An AI turn left awaiting tool results: no perspective could render it.
    pub const TURN_AWAITING_TOOL_RESULTS: &str = "turn_awaiting_tool_results";
    /// An AI author whose id or name is blank.
    pub const AUTHOR_INVALID: &str = "author_invalid";
    /// The parent is not a message of the thread.
    pub const PARENT_NOT_IN_THREAD: &str = "parent_not_in_thread";
}

/// A `MESSAGE_INVALID` refusal for a library error, naming the id it is about
/// when it names one.
pub fn invalid_by_library(error: &MessageError) -> Refusal {
    let refusal = Refusal::new(ReasonCode::MessageInvalid).with("error", library_error_code(error));
    match error {
        MessageError::DuplicateToolCallId { id }
        | MessageError::DuplicateToolCallIdInTurn { id }
        | MessageError::DuplicateToolCallIdInConversation { id }
        | MessageError::DuplicateToolResultId { id }
        | MessageError::ToolNameMismatch { id, .. } => refusal.with("tool_call_id", id.as_str()),
        MessageError::DuplicateEntryId { id } => refusal.with("entry_id", id),
        _ => refusal,
    }
}

/// A `MESSAGE_INVALID` refusal for one of Threads' own reasons.
pub fn invalid(reason: &'static str) -> Refusal {
    Refusal::new(ReasonCode::MessageInvalid).with("error", reason)
}

/// The library block of a contract block, by the library's rules, as the
/// library re-serializes it. Its kind must be the one its content says.
pub fn assistant_block(block: &Block) -> Result<(AssistantBlock, serde_json::Value), Refusal> {
    let Some(raw) = block.body.library() else {
        return Err(invalid(reason::BLOCK_KIND_NOT_ALLOWED).with("block_id", block.id));
    };
    let parsed = raw
        .to_assistant()
        .map_err(|_| invalid(unreadable_code(raw.as_json())).with("block_id", block.id))?;
    let expected = match &parsed {
        AssistantBlock::Text { .. } => Some(BlockKind::Text),
        AssistantBlock::Thinking(_) => Some(BlockKind::Thinking),
        AssistantBlock::RedactedThinking(_) => Some(BlockKind::RedactedThinking),
        AssistantBlock::Structured { .. } => Some(BlockKind::Structured),
        AssistantBlock::ToolCall(_) => Some(BlockKind::ToolCall),
        // A block a later library adds has no Threads kind.
        _ => None,
    };
    if expected != Some(block.kind()) {
        return Err(invalid(reason::BLOCK_CONTENT_INVALID).with("block_id", block.id));
    }
    let json = serde_json::to_value(&parsed)
        .map_err(|_| invalid(reason::BLOCK_CONTENT_INVALID).with("block_id", block.id))?;
    Ok((parsed, json))
}

/// The library tool result of a `TOOL_RESULT` block.
pub fn tool_result_block(block: &Block) -> Result<(ToolResult, serde_json::Value), Refusal> {
    tool_result_of(block).map_err(|code| invalid(code).with("block_id", block.id))
}

/// The library tool result of a `TOOL_RESULT` block, or the code of why it is
/// not one.
pub fn tool_result_of(block: &Block) -> Result<(ToolResult, serde_json::Value), &'static str> {
    let Some(raw) = block
        .body
        .library()
        .filter(|_| block.kind() == BlockKind::ToolResult)
    else {
        return Err(reason::BLOCK_KIND_NOT_ALLOWED);
    };
    let parsed = raw
        .to_tool_result()
        .map_err(|_| unreadable_code(raw.as_json()))?;
    let json = serde_json::to_value(&parsed).map_err(|_| reason::BLOCK_CONTENT_INVALID)?;
    Ok((parsed, json))
}

/// The stored form of a Threads block.
pub fn threads_block(block: &Block, index: usize) -> Result<StoredBlock, Refusal> {
    let content = match &block.body {
        BlockBody::Artifact(content) => serde_json::to_value(content),
        BlockBody::ThreadReference(content) => serde_json::to_value(content),
        BlockBody::JobReference(content) => serde_json::to_value(content),
        BlockBody::Citation(content) => serde_json::to_value(content),
        BlockBody::Error(content) | BlockBody::Warning(content) => serde_json::to_value(content),
        _ => return Err(invalid(reason::BLOCK_KIND_NOT_ALLOWED).with("block_id", block.id)),
    }
    .map_err(|_| invalid(reason::BLOCK_CONTENT_INVALID).with("block_id", block.id))?;
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
                    invalid(unreadable_code(raw.as_json())).with("block_id", block.id)
                })?;
                if !matches!(parsed, UserBlock::Text { .. }) {
                    return Err(invalid(reason::BLOCK_CONTENT_INVALID).with("block_id", block.id));
                }
                stored.push(StoredBlock {
                    id: block.id,
                    index: index as i32,
                    kind: BlockKind::Text,
                    content: serde_json::to_value(&parsed).map_err(|_| {
                        invalid(reason::BLOCK_CONTENT_INVALID).with("block_id", block.id)
                    })?,
                });
                library.push(parsed);
            }
            BlockKind::Artifact => {
                return Err(invalid(reason::ARTIFACT_IN_USER_MESSAGE).with("block_id", block.id));
            }
            kind if kind.is_library() => {
                return Err(invalid(reason::BLOCK_KIND_NOT_ALLOWED)
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
        return Err(invalid(reason::TURN_AWAITING_TOOL_RESULTS));
    }
    Ok(stored)
}

#[cfg(test)]
#[path = "content_tests.rs"]
mod tests;
