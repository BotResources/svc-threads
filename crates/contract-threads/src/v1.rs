//! The 0.4 form of a message's content, still read in two places: the `blocks`
//! of a `message.edit` in the 0.4 form (deprecated, converted when it
//! arrives), and the rows the upgrade converts from what Threads 0.4 stored.
//! `message.store` v1, which carried a whole message in this form, is retired:
//! Threads answers it `COMMAND_VERSION_RETIRED`.
//!
//! The library of 0.4 (`br-llm-messages/3`) is gone: its blocks, stop reasons
//! and usages travel here as the raw JSON 0.4 wrote, read by Threads' conversion.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::annotation::{ArtifactRef, JobRef, Notice, ThreadRef};
use crate::wire::wire_enum;

/// One item of an AI message: a step or a tool results item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Item {
    #[serde(rename = "STEP")]
    Step(StepItem),
    #[serde(rename = "TOOL_RESULTS")]
    ToolResults(ToolResultsItem),
}

/// A step: its blocks, its stop reason and optional usage as the JSON 0.4
/// wrote them, its optional model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepItem {
    pub stop_reason: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub blocks: Vec<Block>,
}

/// A tool results item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResultsItem {
    pub blocks: Vec<Block>,
}

/// One block, under its caller-minted UUIDv7.
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

/// A block's kind and content: the 0.4 library's JSON for its six kinds, the
/// typed content of Threads' own.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "content")]
pub enum BlockBody {
    #[serde(rename = "TEXT")]
    Text(serde_json::Value),
    #[serde(rename = "THINKING")]
    Thinking(serde_json::Value),
    #[serde(rename = "REDACTED_THINKING")]
    RedactedThinking(serde_json::Value),
    #[serde(rename = "STRUCTURED")]
    Structured(serde_json::Value),
    #[serde(rename = "TOOL_CALL")]
    ToolCall(serde_json::Value),
    #[serde(rename = "TOOL_RESULT")]
    ToolResult(serde_json::Value),
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

    /// The 0.4 library's JSON of a library block; `None` for a Threads block.
    pub fn library(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Text(json)
            | Self::Thinking(json)
            | Self::RedactedThinking(json)
            | Self::Structured(json)
            | Self::ToolCall(json)
            | Self::ToolResult(json) => Some(json),
            Self::Artifact(_)
            | Self::ThreadReference(_)
            | Self::JobReference(_)
            | Self::Citation(_)
            | Self::Error(_)
            | Self::Warning(_) => None,
        }
    }

    /// A user input's text block, as 0.4 wrote it.
    pub fn text(text: &str) -> Self {
        Self::Text(serde_json::json!({ "type": "text", "text": text }))
    }
}

/// `CITATION`: a quoted text and the ids of its optional sources. The 2.0 model
/// has no such block: a citation is the library's citation of a text block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Citation {
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_ids: Vec<String>,
}

wire_enum! {
    /// The kind of a 0.4 block: the first six were the library's, the last six
    /// Threads' own.
    pub enum BlockKind {
        Text => "TEXT",
        Thinking => "THINKING",
        RedactedThinking => "REDACTED_THINKING",
        Structured => "STRUCTURED",
        ToolCall => "TOOL_CALL",
        ToolResult => "TOOL_RESULT",
        Artifact => "ARTIFACT",
        ThreadReference => "THREAD_REFERENCE",
        JobReference => "JOB_REFERENCE",
        Citation => "CITATION",
        Error => "ERROR",
        Warning => "WARNING",
    }
}

impl BlockKind {
    /// Whether the block was one of the 0.4 library's.
    pub const fn is_library(self) -> bool {
        matches!(
            self,
            Self::Text
                | Self::Thinking
                | Self::RedactedThinking
                | Self::Structured
                | Self::ToolCall
                | Self::ToolResult
        )
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_0_4_block_and_item_still_decode_as_0_4_wrote_them() {
        let block = json!({ "id": Uuid::now_v7(), "kind": "TEXT", "content": { "type": "text", "text": "hi" } });
        let parsed: Block = serde_json::from_value(block.clone()).unwrap();
        assert_eq!(parsed.body, BlockBody::text("hi"));
        assert_eq!(serde_json::to_value(&parsed).unwrap(), block);
        let item = json!({
            "kind": "STEP",
            "stop_reason": { "type": "end_turn" },
            "usage": { "input_tokens": 3, "output_tokens": 5 },
            "blocks": [block]
        });
        let parsed: Item = serde_json::from_value(item.clone()).unwrap();
        assert_eq!(serde_json::to_value(&parsed).unwrap(), item);
    }

    #[test]
    fn every_v1_literal_maps_back() {
        for kind in BlockKind::ALL {
            assert_eq!(BlockKind::parse(kind.as_str()), Ok(*kind));
        }
        assert_eq!(BlockKind::ALL.iter().filter(|k| k.is_library()).count(), 6);
    }
}
