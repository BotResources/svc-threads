//! The 0.4 form of a message, converted to the library's 2.0 model with the
//! library's own constructors: what `message.store` v1 still sends, the legacy
//! `blocks` of a `message.edit`, and what the upgrade finds in the `_v3`
//! tables ([`crate::upgrade`]).
//!
//! The mapping keeps every id the 0.4 form had — the message's (its turn's) and
//! every block's — and derives the ones it had not ([`super::derived`]):
//!
//! - a user message: an input turn holding one human message of its author
//!   (entry `derive(message, "human_message")`), its text blocks as plain text
//!   blocks;
//! - an AI message: an AI turn of its agent (the 0.4 author id, which must be a
//!   UUIDv7) under its name; each step a model response (entry
//!   `derive(message, "item:{index}")`) — its stop reason as the library's
//!   ending, its usage under the 2.0 names, a thinking's signature as replay
//!   material in the `anthropic.signature` format, a redacted thinking's data in
//!   the `anthropic.redacted_thinking` format, a tool call as a runtime tool call
//!   whose provider id is its external id; each tool results item a runtime
//!   tool results entry, each result answering the runtime call that carries its
//!   tool call id, its text blocks under `derive(result, "content:{index}")`;
//! - Threads' blocks become annotations after their item's entry. A 0.4
//!   `CITATION` block has no place in the 2.0 model (a citation is the
//!   library's citation of a text block) and cannot be converted.
//!
//! The turn is returned open: closing it is the caller's.

use std::collections::HashMap;

use contract_threads::llm::{
    AITurn, AgentId, Author, BlockId, ContentBlock, EntryId, Error, ExternalId, HumanId,
    HumanMessage, InputTurn, ModelName, ModelResponse, OpaqueJson, PlainBlock,
    RedactedThinkingBlock, ReplayFormat, ReplayMaterial, ReplayPayload, ResponseEnding,
    ResponseMetadata, RuntimeToolCall, RuntimeToolResult, RuntimeToolResults, StructuredBlock,
    TextBlock, ThinkingBlock, ToolName, TurnEntry, TurnId, Usage,
};
use contract_threads::v1::{Block, BlockBody, Item};
use contract_threads::{Annotation, AnnotationBody, MessageInvalidError, PlacedAnnotation};
use serde::Deserialize;
use uuid::Uuid;

use super::content::{invalid, refusal_of};
use super::derived;
use crate::command::Refusal;

/// The replay format of a 0.4 thinking's signature.
pub const SIGNATURE_FORMAT: &str = "anthropic.signature";
/// The replay format of a 0.4 redacted thinking's data.
pub const REDACTED_FORMAT: &str = "anthropic.redacted_thinking";

/// Why a 0.4 message cannot be converted: the library refuses what it becomes,
/// or the 2.0 model has no place for it.
#[derive(Debug, Clone, PartialEq)]
pub enum V1Fault {
    Library(Error),
    NotConvertible { object_id: Option<Uuid> },
}

impl V1Fault {
    fn not_convertible(object_id: Uuid) -> Self {
        Self::NotConvertible {
            object_id: Some(object_id),
        }
    }

    /// The refusal a command meets for it.
    pub fn refusal(&self) -> Refusal {
        match self {
            Self::Library(error) => refusal_of(error),
            Self::NotConvertible { object_id } => {
                let refusal = invalid(MessageInvalidError::V1NotConvertible);
                match object_id {
                    Some(id) => refusal.with("object_id", id),
                    None => refusal,
                }
            }
        }
    }
}

impl std::fmt::Display for V1Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Library(error) => write!(f, "{error}"),
            Self::NotConvertible {
                object_id: Some(id),
            } => write!(f, "v1_not_convertible id={id}"),
            Self::NotConvertible { object_id: None } => f.write_str("v1_not_convertible"),
        }
    }
}

impl From<Error> for V1Fault {
    fn from(error: Error) -> Self {
        Self::Library(error)
    }
}

// ── the JSON the 0.4 library wrote ──────────────────────────────────────────

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum V1Text {
    Text { text: String },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum V1Assistant {
    Text {
        text: String,
    },
    Thinking {
        text: String,
        #[serde(default)]
        signature: Option<String>,
    },
    RedactedThinking {
        data: String,
    },
    Structured {
        value: serde_json::Value,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: serde_json::Value,
    },
}

#[derive(Deserialize)]
struct V1ToolResult {
    tool_call_id: String,
    tool_name: String,
    #[serde(default)]
    content: Vec<V1Text>,
    is_error: bool,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum V1Stop {
    EndTurn,
    AwaitingToolResults,
    MaxTokens,
    StopSequence,
    Refusal,
    ContentFilter,
    Other { reason: String },
}

#[derive(Deserialize)]
struct V1Usage {
    input_tokens: u64,
    output_tokens: u64,
    #[serde(default)]
    cache_read_tokens: Option<u64>,
    #[serde(default)]
    cache_write_tokens: Option<u64>,
    #[serde(default)]
    thinking_tokens: Option<u64>,
}

fn read<T: for<'de> Deserialize<'de>>(json: &serde_json::Value, id: Uuid) -> Result<T, V1Fault> {
    T::deserialize(json).map_err(|_| V1Fault::not_convertible(id))
}

fn opaque(json: &serde_json::Value) -> Result<OpaqueJson, V1Fault> {
    Ok(OpaqueJson::parse(&json.to_string())?)
}

fn block_id(id: Uuid) -> Result<BlockId, V1Fault> {
    Ok(BlockId::from_uuid(id)?)
}

fn entry_id(id: Uuid) -> Result<EntryId, V1Fault> {
    Ok(EntryId::from_uuid(id)?)
}

/// The library ending of a 0.4 stop reason.
fn ending_of(stop: V1Stop) -> Result<ResponseEnding, V1Fault> {
    Ok(match stop {
        V1Stop::EndTurn => ResponseEnding::finished(),
        V1Stop::AwaitingToolResults => ResponseEnding::tool_use(),
        V1Stop::MaxTokens => ResponseEnding::output_exhaustion(),
        V1Stop::StopSequence => ResponseEnding::stop_sequence(),
        V1Stop::Refusal => ResponseEnding::refusal(),
        V1Stop::ContentFilter => ResponseEnding::filtering(),
        V1Stop::Other { reason } => ResponseEnding::unclassified(reason)?,
    })
}

/// The library usage of a 0.4 usage, under the 2.0 names.
fn usage_of(usage: V1Usage) -> Usage {
    let mut converted =
        Usage::of_input_tokens(usage.input_tokens).with_output_tokens(usage.output_tokens);
    if let Some(count) = usage.cache_read_tokens {
        converted = converted.with_cache_read_input_tokens(count);
    }
    if let Some(count) = usage.cache_write_tokens {
        converted = converted.with_cache_creation_input_tokens(count);
    }
    if let Some(count) = usage.thinking_tokens {
        converted = converted.with_thinking_tokens(count);
    }
    converted
}

fn replay(format: &str, payload: String) -> Result<ReplayMaterial, V1Fault> {
    Ok(ReplayMaterial::new(
        ReplayFormat::new(format)?,
        ReplayPayload::text(payload),
    ))
}

/// The annotation a 0.4 Threads block becomes, after `entry`.
fn annotation_of(block: &Block, entry: Uuid) -> Result<PlacedAnnotation, V1Fault> {
    let body = match &block.body {
        BlockBody::Artifact(content) => AnnotationBody::Artifact(content.clone()),
        BlockBody::ThreadReference(content) => AnnotationBody::ThreadReference(content.clone()),
        BlockBody::JobReference(content) => AnnotationBody::JobReference(content.clone()),
        BlockBody::Error(content) => AnnotationBody::Error(content.clone()),
        BlockBody::Warning(content) => AnnotationBody::Warning(content.clone()),
        BlockBody::Citation(_)
        | BlockBody::Text(_)
        | BlockBody::Thinking(_)
        | BlockBody::RedactedThinking(_)
        | BlockBody::Structured(_)
        | BlockBody::ToolCall(_)
        | BlockBody::ToolResult(_) => return Err(V1Fault::not_convertible(block.id)),
    };
    block_id(block.id)?;
    Ok(PlacedAnnotation::new(
        entry,
        Annotation::new(block.id, body),
    ))
}

/// The plain text block of a 0.4 user or tool result text.
fn text_block(id: Uuid, text: String) -> Result<PlainBlock, V1Fault> {
    Ok(PlainBlock::try_from(TextBlock::new(block_id(id)?, text)?)?)
}

/// A 0.4 user message's blocks: its text blocks as plain blocks, its Threads
/// blocks as annotations after `entry`.
pub fn user_content(
    blocks: &[Block],
    entry: Uuid,
) -> Result<(Vec<PlainBlock>, Vec<PlacedAnnotation>), V1Fault> {
    let mut content = Vec::with_capacity(blocks.len());
    let mut annotations = Vec::new();
    for block in blocks {
        match &block.body {
            BlockBody::Text(json) => {
                let V1Text::Text { text } = read(json, block.id)?;
                content.push(text_block(block.id, text)?);
            }
            body if body.library().is_some() => return Err(V1Fault::not_convertible(block.id)),
            _ => annotations.push(annotation_of(block, entry)?),
        }
    }
    Ok((content, annotations))
}

/// A 0.4 user message of `author`: an input turn under the message's id holding
/// its human message, and the annotations its Threads blocks become.
pub fn user_turn(
    message: Uuid,
    author: Uuid,
    blocks: &[Block],
) -> Result<(InputTurn, Vec<PlacedAnnotation>), V1Fault> {
    let entry = derived::derive(message, derived::HUMAN_MESSAGE);
    let (content, annotations) = user_content(blocks, entry)?;
    let human = HumanMessage::new(entry_id(entry)?, HumanId::from_uuid(author)?, content)?;
    let turn = InputTurn::new(TurnId::from_uuid(message)?, vec![human.into()])?;
    Ok((turn, annotations))
}

/// A 0.4 AI message of `agent` under `author`: its AI turn — open — and the
/// annotations its Threads blocks become.
pub fn ai_turn(
    message: Uuid,
    agent: AgentId,
    author: Author,
    items: &[Item],
) -> Result<(AITurn, Vec<PlacedAnnotation>), V1Fault> {
    let mut turn = AITurn::open(TurnId::from_uuid(message)?, agent, author);
    let mut annotations = Vec::new();
    // The runtime calls of the turn so far, by their 0.4 (provider) id.
    let mut calls: HashMap<String, BlockId> = HashMap::new();
    for (index, item) in items.iter().enumerate() {
        let entry = derived::derive(message, &derived::item(index));
        let converted = match item {
            Item::Step(step) => {
                let mut blocks = Vec::with_capacity(step.blocks.len());
                for block in &step.blocks {
                    let Some(json) = block.body.library() else {
                        annotations.push(annotation_of(block, entry)?);
                        continue;
                    };
                    blocks.push(match read::<V1Assistant>(json, block.id)? {
                        V1Assistant::Text { text } => {
                            ContentBlock::Text(TextBlock::new(block_id(block.id)?, text)?)
                        }
                        V1Assistant::Thinking { text, signature } => {
                            let id = block_id(block.id)?;
                            let thinking = match signature {
                                Some(signature) if text.is_empty() => ThinkingBlock::replay_only(
                                    id,
                                    replay(SIGNATURE_FORMAT, signature)?,
                                ),
                                Some(signature) => ThinkingBlock::new(id, text)?
                                    .with_replay(replay(SIGNATURE_FORMAT, signature)?),
                                None => ThinkingBlock::new(id, text)?,
                            };
                            thinking.into()
                        }
                        V1Assistant::RedactedThinking { data } => RedactedThinkingBlock::new(
                            block_id(block.id)?,
                            replay(REDACTED_FORMAT, data)?,
                        )
                        .into(),
                        V1Assistant::Structured { value } => {
                            StructuredBlock::new(block_id(block.id)?, opaque(&value)?)?.into()
                        }
                        V1Assistant::ToolCall {
                            id,
                            name,
                            arguments,
                        } => {
                            let call = RuntimeToolCall::new(
                                block_id(block.id)?,
                                ToolName::new(name)?,
                                opaque(&arguments)?,
                            )?
                            .with_external_id(ExternalId::new(id.clone())?);
                            calls.insert(id, call.id());
                            call.into()
                        }
                    });
                }
                let ending = ending_of(read(&step.stop_reason, entry)?)?;
                let mut metadata = ResponseMetadata::new();
                if let Some(usage) = &step.usage {
                    metadata = metadata.with_usage(usage_of(read(usage, entry)?));
                }
                if let Some(model) = &step.model {
                    metadata = metadata.with_model(ModelName::new(model.clone())?);
                }
                TurnEntry::from(ModelResponse::new(
                    entry_id(entry)?,
                    blocks,
                    ending,
                    metadata,
                )?)
            }
            Item::ToolResults(results) => {
                let mut converted = Vec::with_capacity(results.blocks.len());
                for block in &results.blocks {
                    let Some(json) = block.body.library() else {
                        annotations.push(annotation_of(block, entry)?);
                        continue;
                    };
                    let BlockBody::ToolResult(_) = &block.body else {
                        return Err(V1Fault::not_convertible(block.id));
                    };
                    let result: V1ToolResult = read(json, block.id)?;
                    let call = *calls
                        .get(&result.tool_call_id)
                        .ok_or(V1Fault::not_convertible(block.id))?;
                    let mut content = Vec::with_capacity(result.content.len());
                    for (position, V1Text::Text { text }) in result.content.into_iter().enumerate()
                    {
                        let id = derived::derive(block.id, &derived::result_content(position));
                        content.push(text_block(id, text)?);
                    }
                    converted.push(RuntimeToolResult::new(
                        block_id(block.id)?,
                        call,
                        ToolName::new(result.tool_name)?,
                        result.is_error,
                        content,
                    )?);
                }
                TurnEntry::from(RuntimeToolResults::new(entry_id(entry)?, converted)?)
            }
        };
        turn.push_entry(converted)?;
    }
    Ok((turn, annotations))
}

#[cfg(test)]
mod tests {
    use contract_threads::llm::{ContentBlockKind, Turn, TurnEntryKind};
    use contract_threads::v1::{StepItem, ToolResultsItem};
    use contract_threads::{ArtifactRef, Notice};
    use serde_json::json;

    use super::*;

    fn block(kind: &str, content: serde_json::Value) -> Block {
        serde_json::from_value(json!({ "id": Uuid::now_v7(), "kind": kind, "content": content }))
            .unwrap()
    }

    fn agent() -> AgentId {
        AgentId::from_uuid(Uuid::now_v7()).unwrap()
    }

    fn analyst() -> Author {
        Author::new("Analyst").unwrap()
    }

    #[test]
    fn a_user_message_becomes_an_input_turn_keeping_its_block_ids() {
        let (message, author) = (Uuid::now_v7(), Uuid::now_v7());
        let text = block("TEXT", json!({ "type": "text", "text": "Hello" }));
        let warning = Block::new(
            Uuid::now_v7(),
            BlockBody::Warning(Notice {
                code: "W".into(),
                text: None,
            }),
        );
        let (turn, annotations) =
            user_turn(message, author, &[text.clone(), warning.clone()]).unwrap();
        assert_eq!(turn.id().as_uuid(), message);
        let entry = &turn.entries()[0];
        assert_eq!(
            entry.id().as_uuid(),
            derived::derive(message, derived::HUMAN_MESSAGE)
        );
        let contract_threads::llm::IncomingEntry::HumanMessage(human) = entry else {
            panic!("a human message");
        };
        assert_eq!(human.human().as_uuid(), author);
        assert_eq!(human.content()[0].id().as_uuid(), text.id);
        assert_eq!(annotations[0].annotation.id, warning.id);
        assert_eq!(annotations[0].after_entry_id, entry.id().as_uuid());
    }

    #[test]
    fn an_ai_message_keeps_its_steps_calls_results_and_replay_material() {
        let message = Uuid::now_v7();
        let thinking = block(
            "THINKING",
            json!({ "type": "thinking", "text": " let me think ", "signature": "c2ln" }),
        );
        let redacted = block(
            "REDACTED_THINKING",
            json!({ "type": "redacted_thinking", "data": "b3Bx" }),
        );
        let call = block(
            "TOOL_CALL",
            json!({ "type": "tool_call", "id": "call_1", "name": "search", "arguments": { "q": "x" } }),
        );
        let result = block(
            "TOOL_RESULT",
            json!({ "tool_call_id": "call_1", "tool_name": "search", "is_error": false,
                    "content": [{ "type": "text", "text": "found" }] }),
        );
        let answer = block("TEXT", json!({ "type": "text", "text": "done" }));
        let artifact = Block::new(
            Uuid::now_v7(),
            BlockBody::Artifact(ArtifactRef {
                artifact_id: Uuid::now_v7(),
            }),
        );
        let items = vec![
            Item::Step(StepItem {
                stop_reason: json!({ "type": "awaiting_tool_results" }),
                usage: Some(
                    json!({ "input_tokens": 3, "output_tokens": 5, "cache_read_tokens": 1 }),
                ),
                model: Some("m-1".into()),
                blocks: vec![thinking.clone(), redacted, call.clone()],
            }),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![result.clone()],
            }),
            Item::Step(StepItem {
                stop_reason: json!({ "type": "end_turn" }),
                usage: None,
                model: None,
                blocks: vec![answer, artifact.clone()],
            }),
        ];
        let (turn, annotations) = ai_turn(message, agent(), analyst(), &items).unwrap();
        let kinds: Vec<TurnEntryKind> = turn.entries().iter().map(TurnEntry::kind).collect();
        assert_eq!(
            kinds,
            [
                TurnEntryKind::ModelResponse,
                TurnEntryKind::RuntimeToolResults,
                TurnEntryKind::ModelResponse
            ]
        );
        let TurnEntry::ModelResponse(first) = &turn.entries()[0] else {
            panic!("a response");
        };
        assert_eq!(first.ending(), &ResponseEnding::tool_use());
        assert_eq!(first.usage().unwrap().cache_read_input_tokens(), Some(1));
        assert_eq!(first.model().unwrap().as_str(), "m-1");
        let ContentBlock::Thinking(kept) = &first.blocks()[0] else {
            panic!("a thinking");
        };
        assert_eq!(kept.text(), " let me think ");
        assert_eq!(kept.replay().unwrap().format().as_str(), SIGNATURE_FORMAT);
        let ContentBlock::RuntimeToolCall(runtime) = &first.blocks()[2] else {
            panic!("a runtime call");
        };
        assert_eq!(runtime.id().as_uuid(), call.id);
        assert_eq!(runtime.external_id().unwrap().as_str(), "call_1");
        let TurnEntry::RuntimeToolResults(results) = &turn.entries()[1] else {
            panic!("results");
        };
        assert_eq!(results.results()[0].call(), runtime.id());
        assert_eq!(results.results()[0].id().as_uuid(), result.id);
        assert_eq!(annotations[0].annotation.id, artifact.id);
        assert_eq!(
            annotations[0].after_entry_id,
            turn.entries()[2].id().as_uuid()
        );
        let content =
            super::super::content::Content::of_turn(&Turn::AITurn(turn), annotations).unwrap();
        assert!(
            content
                .blocks
                .iter()
                .any(|b| b.kind() == ContentBlockKind::RedactedThinking)
        );
    }

    #[test]
    fn a_citation_and_a_result_answering_no_call_cannot_be_converted() {
        let citation = block(
            "CITATION",
            json!({ "text": "a quote", "source_ids": ["s1"] }),
        );
        assert_eq!(
            user_turn(
                Uuid::now_v7(),
                Uuid::now_v7(),
                std::slice::from_ref(&citation)
            )
            .unwrap_err(),
            V1Fault::not_convertible(citation.id)
        );
        let orphan = block(
            "TOOL_RESULT",
            json!({ "tool_call_id": "nobody", "tool_name": "search", "is_error": false }),
        );
        let items = vec![
            Item::Step(StepItem {
                stop_reason: json!({ "type": "awaiting_tool_results" }),
                usage: None,
                model: None,
                blocks: vec![block(
                    "TOOL_CALL",
                    json!({ "type": "tool_call", "id": "call_1", "name": "search", "arguments": {} }),
                )],
            }),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![orphan.clone()],
            }),
        ];
        assert_eq!(
            ai_turn(Uuid::now_v7(), agent(), analyst(), &items).unwrap_err(),
            V1Fault::not_convertible(orphan.id)
        );
    }

    #[test]
    fn what_the_library_refuses_is_refused_with_its_code() {
        let blank = block("TEXT", json!({ "type": "text", "text": "  " }));
        let refusal = user_turn(Uuid::now_v7(), Uuid::now_v7(), &[blank])
            .unwrap_err()
            .refusal();
        assert_eq!(refusal.code, contract_threads::ReasonCode::MessageInvalid);
        assert_eq!(refusal.params["error"], "invalid_content");
    }
}
