//! The content of a message: blocks, and for an AI message the items (steps and
//! tool results) that hold them, in the library's order.
//!
//! A block is `{ "id": <UUIDv7>, "kind": <BlockKind>, "content": <JSON> }`.
//! A library block's content is the library's own JSON for it — a thinking, its
//! signature and a redacted thinking are kept byte for byte, so that they can be
//! replayed to the provider that produced them — and is judged by Threads with
//! the library's rules (`MESSAGE_INVALID`). A Threads block's content is typed
//! by its kind.

use br_llm_messages::{AssistantBlock, StopReason, Text, ToolResult, Usage, UserBlock};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::status::BlockKind;

/// One block, under the caller-minted UUIDv7 it keeps for its life.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Block {
    pub id: Uuid,
    #[serde(flatten)]
    pub body: BlockBody,
}

impl Block {
    pub fn new(id: Uuid, body: BlockBody) -> Self {
        Self { id, body }
    }

    pub fn kind(&self) -> BlockKind {
        self.body.kind()
    }
}

/// A block's kind and its content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "content")]
pub enum BlockBody {
    #[serde(rename = "TEXT")]
    Text(LibraryBlock),
    #[serde(rename = "THINKING")]
    Thinking(LibraryBlock),
    #[serde(rename = "REDACTED_THINKING")]
    RedactedThinking(LibraryBlock),
    #[serde(rename = "STRUCTURED")]
    Structured(LibraryBlock),
    #[serde(rename = "TOOL_CALL")]
    ToolCall(LibraryBlock),
    #[serde(rename = "TOOL_RESULT")]
    ToolResult(LibraryBlock),
    #[serde(rename = "ARTIFACT")]
    Artifact(ArtifactRef),
    #[serde(rename = "THREAD_REFERENCE")]
    ThreadReference(ThreadRef),
    #[serde(rename = "JOB_REFERENCE")]
    JobReference(JobRef),
    #[serde(rename = "CITATION")]
    Citation(Citation),
    #[serde(rename = "ERROR")]
    Error(Notice),
    #[serde(rename = "WARNING")]
    Warning(Notice),
}

impl BlockBody {
    pub fn kind(&self) -> BlockKind {
        match self {
            Self::Text(_) => BlockKind::Text,
            Self::Thinking(_) => BlockKind::Thinking,
            Self::RedactedThinking(_) => BlockKind::RedactedThinking,
            Self::Structured(_) => BlockKind::Structured,
            Self::ToolCall(_) => BlockKind::ToolCall,
            Self::ToolResult(_) => BlockKind::ToolResult,
            Self::Artifact(_) => BlockKind::Artifact,
            Self::ThreadReference(_) => BlockKind::ThreadReference,
            Self::JobReference(_) => BlockKind::JobReference,
            Self::Citation(_) => BlockKind::Citation,
            Self::Error(_) => BlockKind::Error,
            Self::Warning(_) => BlockKind::Warning,
        }
    }

    /// The library's JSON of a library block; `None` for a Threads block.
    pub fn library(&self) -> Option<&LibraryBlock> {
        match self {
            Self::Text(block)
            | Self::Thinking(block)
            | Self::RedactedThinking(block)
            | Self::Structured(block)
            | Self::ToolCall(block)
            | Self::ToolResult(block) => Some(block),
            Self::Artifact(_)
            | Self::ThreadReference(_)
            | Self::JobReference(_)
            | Self::Citation(_)
            | Self::Error(_)
            | Self::Warning(_) => None,
        }
    }

    /// The body of a user input's text block. A user input holds text only:
    /// the block kinds Threads stores name no image.
    pub fn user_text(text: &Text) -> Result<Self, serde_json::Error> {
        Ok(Self::Text(LibraryBlock::of(&UserBlock::text(
            text.clone(),
        ))?))
    }

    /// The body of a step's block, under the kind the library gives it. A
    /// block kind a later library adds has no Threads kind yet: it is refused
    /// ([`LibraryValueError::UnsupportedAssistantBlock`]) until the contract
    /// names it.
    pub fn assistant(block: &AssistantBlock) -> Result<Self, LibraryValueError> {
        let json = LibraryBlock::of(block)?;
        Ok(match block {
            AssistantBlock::Text { .. } => Self::Text(json),
            AssistantBlock::Thinking(_) => Self::Thinking(json),
            AssistantBlock::RedactedThinking(_) => Self::RedactedThinking(json),
            AssistantBlock::Structured { .. } => Self::Structured(json),
            AssistantBlock::ToolCall(_) => Self::ToolCall(json),
            _ => return Err(LibraryValueError::UnsupportedAssistantBlock),
        })
    }

    /// The body of one tool result of a tool results item.
    pub fn tool_result(result: &ToolResult) -> Result<Self, serde_json::Error> {
        Ok(Self::ToolResult(LibraryBlock::of(result)?))
    }
}

/// Why the contract cannot write a library value as its JSON.
#[derive(Debug, thiserror::Error)]
pub enum LibraryValueError {
    /// An assistant block of a later library, which has no Threads kind yet.
    #[error("unsupported_assistant_block")]
    UnsupportedAssistantBlock,
    /// A stream event of a later library, which the contract does not write
    /// yet.
    #[error("unsupported_stream_event")]
    UnsupportedStreamEvent,
    #[error("encode")]
    Encode(#[from] serde_json::Error),
}

/// The library's JSON for one block, carried as such and judged by Threads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LibraryBlock(serde_json::Value);

impl LibraryBlock {
    /// The JSON the library gives `block`.
    pub fn of<B: Serialize>(block: &B) -> Result<Self, serde_json::Error> {
        serde_json::to_value(block).map(Self)
    }

    /// Raw JSON, as a commander that does not use the library sends it.
    pub fn from_json(json: serde_json::Value) -> Self {
        Self(json)
    }

    pub fn as_json(&self) -> &serde_json::Value {
        &self.0
    }

    /// Read as a user input's block, by the library's rules.
    pub fn to_user(&self) -> Result<UserBlock, serde_json::Error> {
        UserBlock::deserialize(&self.0)
    }

    /// Read as a step's block, by the library's rules.
    pub fn to_assistant(&self) -> Result<AssistantBlock, serde_json::Error> {
        AssistantBlock::deserialize(&self.0)
    }

    /// Read as a tool result, by the library's rules.
    pub fn to_tool_result(&self) -> Result<ToolResult, serde_json::Error> {
        ToolResult::deserialize(&self.0)
    }
}

/// `ARTIFACT`: an artifact of the same thread, produced by a runner. Naming it
/// attaches it; it must be uploaded with its declared digest
/// (`ARTIFACT_NOT_READY`). No foreign key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: Uuid,
}

/// `THREAD_REFERENCE`: a thread, typically a child started from this block.
/// No foreign key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadRef {
    pub thread_id: Uuid,
}

/// `JOB_REFERENCE`: a Job of the Jobs service. No foreign key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRef {
    pub job_id: Uuid,
}

/// `CITATION`: a quoted text and the ids of its optional sources, opaque to
/// Threads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Citation {
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_ids: Vec<String>,
}

/// `ERROR` or `WARNING`: a code, and an optional text for the people reading the
/// thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notice {
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

/// One item of an AI message: a step (one model response) or a tool results
/// item, in the library's order. Its index is its position in the message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Item {
    #[serde(rename = "STEP")]
    Step(StepItem),
    #[serde(rename = "TOOL_RESULTS")]
    ToolResults(ToolResultsItem),
}

/// A step, as the library gives it: its blocks, its stop reason, its optional
/// usage and the optional name of the model that produced it (which Threads
/// does not read). Threads blocks may follow, each taking the next index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepItem {
    pub stop_reason: StopReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub blocks: Vec<Block>,
}

/// A tool results item: `TOOL_RESULT` blocks answering the previous step's tool
/// calls, and Threads blocks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResultsItem {
    pub blocks: Vec<Block>,
}

#[cfg(test)]
mod tests {
    use br_llm_messages::{Thinking, ToolCall, ToolCallId, ToolName};
    use serde_json::json;

    use super::*;

    #[test]
    fn a_library_block_travels_as_the_librarys_json_under_its_kind() {
        let id = Uuid::now_v7();
        let block = Block::new(
            id,
            BlockBody::assistant(&AssistantBlock::Text {
                text: Text::new("hello").unwrap(),
            })
            .unwrap(),
        );
        let wire = serde_json::to_value(&block).unwrap();
        assert_eq!(
            wire,
            json!({
                "id": id,
                "kind": "TEXT",
                "content": { "type": "text", "text": "hello" }
            })
        );
        let back: Block = serde_json::from_value(wire).unwrap();
        assert_eq!(back, block);
        assert_eq!(back.kind(), BlockKind::Text);
    }

    #[test]
    fn a_thinking_and_its_signature_come_back_byte_for_byte() {
        let thinking = AssistantBlock::Thinking(Thinking {
            text: "  let me think\n\twith whitespace ".to_owned(),
            signature: Some(br_llm_messages::Signature::new("c2lnPT0=").unwrap()),
        });
        let body = BlockBody::assistant(&thinking).unwrap();
        let wire = serde_json::to_string(&Block::new(Uuid::now_v7(), body)).unwrap();
        let back: Block = serde_json::from_str(&wire).unwrap();
        assert_eq!(
            back.body.library().unwrap().to_assistant().unwrap(),
            thinking
        );
    }

    #[test]
    fn invalid_library_content_still_decodes_so_that_threads_can_refuse_it() {
        let wire = json!({
            "id": Uuid::now_v7(),
            "kind": "TEXT",
            "content": { "type": "text", "text": "   " }
        });
        let block: Block = serde_json::from_value(wire).unwrap();
        assert!(block.body.library().unwrap().to_user().is_err());
    }

    #[test]
    fn a_threads_block_carries_typed_content() {
        let artifact_id = Uuid::now_v7();
        let wire = json!({
            "id": Uuid::now_v7(),
            "kind": "ARTIFACT",
            "content": { "artifact_id": artifact_id }
        });
        let block: Block = serde_json::from_value(wire).unwrap();
        assert_eq!(block.body, BlockBody::Artifact(ArtifactRef { artifact_id }));
        assert!(block.body.library().is_none());

        let notice: Block = serde_json::from_value(json!({
            "id": Uuid::now_v7(),
            "kind": "WARNING",
            "content": { "code": "PARTIAL_ANSWER" }
        }))
        .unwrap();
        assert_eq!(
            notice.body,
            BlockBody::Warning(Notice {
                code: "PARTIAL_ANSWER".into(),
                text: None
            })
        );
    }

    #[test]
    fn an_unknown_kind_does_not_decode() {
        let wire = json!({ "id": Uuid::now_v7(), "kind": "IMAGE", "content": {} });
        assert!(serde_json::from_value::<Block>(wire).is_err());
    }

    #[test]
    fn a_step_item_keeps_its_stop_reason_usage_and_model() {
        let call = AssistantBlock::ToolCall(ToolCall {
            id: ToolCallId::new("c1").unwrap(),
            name: ToolName::new("search").unwrap(),
            arguments: json!({ "q": "rust" }),
        });
        let item = Item::Step(StepItem {
            stop_reason: StopReason::AwaitingToolResults,
            usage: Some(Usage {
                input_tokens: 3,
                output_tokens: 5,
                ..Usage::default()
            }),
            model: Some("m-1".into()),
            blocks: vec![Block::new(
                Uuid::now_v7(),
                BlockBody::assistant(&call).unwrap(),
            )],
        });
        let wire = serde_json::to_value(&item).unwrap();
        assert_eq!(wire["kind"], "STEP");
        assert_eq!(
            wire["stop_reason"],
            json!({ "type": "awaiting_tool_results" })
        );
        assert_eq!(wire["model"], "m-1");
        assert_eq!(serde_json::from_value::<Item>(wire).unwrap(), item);
    }
}
