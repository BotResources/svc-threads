//! The conversation a scenario stores, built through the contract's and the
//! library's own types (`contract-threads`, `br-llm-messages/2`): blocks under
//! caller-minted UUIDv7 ids, the items of an AI turn, and the chunks of a
//! stream. A builder never decides what is valid — Threads does, with the
//! library's rules; the raw builders exist to send what the library refuses.

use contract_threads::llm::{
    AssistantBlock, BlockKind as StreamBlockKind, Signature, StopReason, StreamEvent, Text,
    Thinking, ToolCall, ToolCallId, ToolName, ToolResult, ToolResultBlock,
};
use contract_threads::{
    ArtifactRef, Block, BlockBody, Chunk, ChunkPart, Item, LibraryBlock, StepItem, ToolResultsItem,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ── blocks ──────────────────────────────────────────────────────────────────

/// A user input's text block.
pub fn user_text(text: &str) -> Block {
    Block::new(
        Uuid::now_v7(),
        BlockBody::user_text(&Text::new(text).expect("a scenario's text is not blank"))
            .expect("a text block serializes"),
    )
}

/// A `TEXT` block carrying raw JSON the library may refuse (an empty text).
pub fn raw_text(json: Value) -> Block {
    Block::new(
        Uuid::now_v7(),
        BlockBody::Text(LibraryBlock::from_json(json)),
    )
}

/// A step's text block.
pub fn ai_text(text: &str) -> Block {
    assistant(&AssistantBlock::Text {
        text: Text::new(text).expect("a scenario's text is not blank"),
    })
}

/// A step's thinking, with its signature, kept byte for byte.
pub fn thinking(text: &str, signature: &str) -> Block {
    assistant(&AssistantBlock::Thinking(Thinking {
        text: text.to_string(),
        signature: Some(Signature::new(signature).expect("a valid signature")),
    }))
}

/// A step's tool call.
pub fn tool_call(id: &str, name: &str, arguments: Value) -> Block {
    assistant(&AssistantBlock::ToolCall(ToolCall {
        id: ToolCallId::new(id).expect("a tool call id"),
        name: ToolName::new(name).expect("a tool name"),
        arguments,
    }))
}

fn assistant(block: &AssistantBlock) -> Block {
    Block::new(
        Uuid::now_v7(),
        BlockBody::assistant(block).expect("a step block serializes"),
    )
}

/// The library's tool result answering `call`.
pub fn result_of(call: &str, name: &str, text: &str, is_error: bool) -> ToolResult {
    ToolResult::new(
        ToolCallId::new(call).expect("a tool call id"),
        ToolName::new(name).expect("a tool name"),
        vec![ToolResultBlock::text(
            Text::new(text).expect("a result text"),
        )],
        is_error,
    )
}

/// A `TOOL_RESULT` block.
pub fn tool_result(call: &str, name: &str, text: &str) -> Block {
    Block::new(
        Uuid::now_v7(),
        BlockBody::tool_result(&result_of(call, name, text, false))
            .expect("a tool result serializes"),
    )
}

/// A Threads `ARTIFACT` block naming `artifact`.
pub fn artifact_block(artifact: Uuid) -> Block {
    Block::new(
        Uuid::now_v7(),
        BlockBody::Artifact(ArtifactRef {
            artifact_id: artifact,
        }),
    )
}

// ── items ───────────────────────────────────────────────────────────────────

pub fn step(stop_reason: StopReason, model: Option<&str>, blocks: Vec<Block>) -> Item {
    Item::Step(StepItem {
        stop_reason,
        usage: None,
        model: model.map(str::to_string),
        blocks,
    })
}

/// One step answering in text, ending the turn.
pub fn answer(text: &str) -> Vec<Item> {
    vec![step(StopReason::EndTurn, None, vec![ai_text(text)])]
}

pub fn tool_results(blocks: Vec<Block>) -> Item {
    Item::ToolResults(ToolResultsItem { blocks })
}

// ── chunks of a stream ──────────────────────────────────────────────────────

/// A step event of item `item`.
pub fn event(item: u32, event: StreamEvent) -> Chunk {
    Chunk {
        item_index: item,
        part: ChunkPart::StepEvent {
            event,
            block_id: None,
            model: None,
        },
    }
}

/// A `block_start` (or `tool_call_start`) of item `item`, naming its block —
/// and the step's model when it is the item's first chunk.
pub fn opening(item: u32, event: StreamEvent, block: Uuid, model: Option<&str>) -> Chunk {
    Chunk {
        item_index: item,
        part: ChunkPart::StepEvent {
            event,
            block_id: Some(block),
            model: model.map(str::to_string),
        },
    }
}

pub fn text_start(index: usize) -> StreamEvent {
    StreamEvent::BlockStart {
        index,
        kind: StreamBlockKind::Text,
    }
}

pub fn text_delta(index: usize, text: &str) -> StreamEvent {
    StreamEvent::TextDelta {
        index,
        text: text.to_string(),
    }
}

pub fn block_end(index: usize) -> StreamEvent {
    StreamEvent::BlockEnd { index }
}

pub fn call_start(index: usize, id: &str, name: &str) -> StreamEvent {
    StreamEvent::ToolCallStart {
        index,
        id: ToolCallId::new(id).expect("a tool call id"),
        name: ToolName::new(name).expect("a tool name"),
    }
}

pub fn arguments_delta(index: usize, fragment: &str) -> StreamEvent {
    StreamEvent::ToolCallArgumentsDelta {
        index,
        json_fragment: fragment.to_string(),
    }
}

pub fn finish(stop_reason: StopReason) -> StreamEvent {
    StreamEvent::Finish {
        stop_reason,
        usage: None,
    }
}

/// A whole tool results item, as one chunk.
pub fn results_chunk(item: u32, blocks: Vec<Block>) -> Chunk {
    Chunk {
        item_index: item,
        part: ChunkPart::ToolResults { blocks },
    }
}

/// A whole Threads block, as one chunk.
pub fn block_chunk(item: u32, block: Block) -> Chunk {
    Chunk {
        item_index: item,
        part: ChunkPart::Block { block },
    }
}

/// The chunks of a step of item `item` whose single text block grows from
/// `deltas`, ending with `stop_reason`.
pub fn text_step(item: u32, model: Option<&str>, deltas: &[&str], stop: StopReason) -> Vec<Chunk> {
    let mut chunks = vec![opening(item, text_start(0), Uuid::now_v7(), model)];
    chunks.extend(deltas.iter().map(|delta| event(item, text_delta(0, delta))));
    chunks.push(event(item, block_end(0)));
    chunks.push(event(item, finish(stop)));
    chunks
}

/// The chunks of a step of item `item` that ends calling `calls` (id, name,
/// JSON arguments), awaiting their results.
pub fn calling_step(item: u32, calls: &[(&str, &str, Value)]) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    for (index, (id, name, arguments)) in calls.iter().enumerate() {
        chunks.push(opening(
            item,
            call_start(index, id, name),
            Uuid::now_v7(),
            None,
        ));
        chunks.push(event(item, arguments_delta(index, &arguments.to_string())));
        chunks.push(event(item, block_end(index)));
    }
    chunks.push(event(item, finish(StopReason::AwaitingToolResults)));
    chunks
}

// ── reading a message view ──────────────────────────────────────────────────

/// The library JSON of a text block: what a `TEXT` block's content reads.
pub fn text_content(text: &str) -> Value {
    json!({ "type": "text", "text": text })
}

/// The text of every `TEXT` block of a message view, in order (user blocks,
/// then every item's).
pub fn texts_of(view: &Value) -> Vec<String> {
    let mut blocks: Vec<&Value> = view["blocks"]
        .as_array()
        .map(|blocks| blocks.iter().collect())
        .unwrap_or_default();
    for item in view["items"].as_array().into_iter().flatten() {
        blocks.extend(item["blocks"].as_array().into_iter().flatten());
    }
    blocks
        .into_iter()
        .filter(|block| block["kind"] == "TEXT")
        .filter_map(|block| block["content"]["text"].as_str().map(str::to_string))
        .collect()
}

/// The library tool results of every `TOOL_RESULTS` item of a message view.
pub fn tool_results_of(view: &Value) -> Vec<ToolResult> {
    view["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| item["kind"] == "TOOL_RESULTS")
        .flat_map(|item| item["blocks"].as_array().cloned().unwrap_or_default())
        .filter(|block| block["kind"] == "TOOL_RESULT")
        .map(|block| {
            serde_json::from_value(block["content"].clone())
                .unwrap_or_else(|e| panic!("a TOOL_RESULT block is the library's ({e}): {block}"))
        })
        .collect()
}
