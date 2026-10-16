//! The content of a `STREAMING` message, as its writer pushes it on the
//! engine's accumulated lane.
//!
//! A frame is published on [`chunk_subject`]`(message_id)` of the
//! [`STREAMING_STREAM`] stream, with `Nats-Msg-Id` = [`ChunkFrame::msg_id`], no
//! integration envelope:
//!
//! ```json
//! { "accumulator": "message_content", "key": "<message_id>", "seq": 0, "chunk": { … } }
//! ```
//!
//! `seq` starts at 0 and is contiguous. Every chunk names the item it belongs
//! to and is one of three parts:
//!
//! - `STEP_EVENT`: one stream event of the library (`br-llm-messages/3`), as
//!   the library's JSON for it ([`LibraryEvent`]), folded with the library's
//!   step draft for that item. `block_id` names the
//!   block a `block_start` or `tool_call_start` opens (the library event carries
//!   no id) and is refused elsewhere; `model` is allowed on the item's first
//!   chunk only. The `finish` event completes the item.
//! - `TOOL_RESULTS`: a whole tool results item, completing its item.
//! - `BLOCK`: a whole Threads block, appended to its item at its next index.
//!
//! A chunk the library refuses fails the message (`MESSAGE_INVALID`); a chunk
//! after the seal is refused on every pod; a chunk is never a fact. The writer
//! closes the stream with `message.complete` (or `fail` / `cancel`) carrying
//! the [`seal_hash`] of every chunk it pushed.

use br_llm_messages::StreamEvent;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::content::{Block, LibraryValueError};

/// The stream the engine binds for Threads (`STREAMING_{service}`), declared out
/// of band like every stream.
pub const STREAMING_STREAM: &str = "STREAMING_threads";

/// The accumulator a message's content folds into.
pub const MESSAGE_CONTENT_ACCUMULATOR: &str = "message_content";

/// `stream.threads.{message_id}`: the engine's accumulated-lane subject for one
/// message. A UUID is a token-safe key, so the token is the id itself.
pub fn chunk_subject(message_id: Uuid) -> String {
    format!("stream.{}.{message_id}", crate::SERVICE)
}

/// One chunk of a message's content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    /// The index of the item of the message the chunk belongs to.
    pub item_index: u32,
    #[serde(flatten)]
    pub part: ChunkPart,
}

/// What a chunk carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ChunkPart {
    /// One library stream event of a step.
    #[serde(rename = "STEP_EVENT")]
    StepEvent {
        event: LibraryEvent,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        block_id: Option<Uuid>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },
    /// A whole tool results item: its `TOOL_RESULT` blocks.
    #[serde(rename = "TOOL_RESULTS")]
    ToolResults { blocks: Vec<Block> },
    /// A whole Threads block.
    #[serde(rename = "BLOCK")]
    Block { block: Block },
}

/// One stream event of the library, carried as the library's JSON for it and
/// judged by Threads when it folds — as a [`crate::LibraryBlock`] is: a chunk
/// whose event the library refuses still decodes, so that Threads can fail its
/// message with the library's reason instead of losing the chunk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LibraryEvent(serde_json::Value);

impl LibraryEvent {
    /// The JSON `event` loads from.
    ///
    /// The library loads stream events from JSON but never serializes them, so
    /// the contract writes that JSON itself: one arm per event, every field
    /// under the name the library reads it by (`to_event` of the result gives
    /// `event` back, and the JSON is the one `br-llm-messages` 0.3 wrote — the
    /// contract's tests hold both for every event). An event a later library
    /// adds is refused ([`LibraryValueError::UnsupportedStreamEvent`]) until
    /// the contract names it.
    pub fn of(event: &StreamEvent) -> Result<Self, LibraryValueError> {
        let json = match event {
            StreamEvent::BlockStart { index, kind } => {
                json!({ "type": "block_start", "index": index, "kind": kind })
            }
            StreamEvent::TextDelta { index, text } => {
                json!({ "type": "text_delta", "index": index, "text": text })
            }
            StreamEvent::ThinkingDelta { index, text } => {
                json!({ "type": "thinking_delta", "index": index, "text": text })
            }
            StreamEvent::SignatureDelta { index, signature } => {
                json!({ "type": "signature_delta", "index": index, "signature": signature })
            }
            StreamEvent::RedactedThinkingData { index, data } => {
                json!({ "type": "redacted_thinking_data", "index": index, "data": data })
            }
            StreamEvent::StructuredDelta {
                index,
                json_fragment,
            } => json!({
                "type": "structured_delta", "index": index, "json_fragment": json_fragment
            }),
            StreamEvent::ToolCallStart { index, id, name } => {
                json!({ "type": "tool_call_start", "index": index, "id": id, "name": name })
            }
            StreamEvent::ToolCallArgumentsDelta {
                index,
                json_fragment,
            } => json!({
                "type": "tool_call_arguments_delta", "index": index, "json_fragment": json_fragment
            }),
            StreamEvent::BlockEnd { index } => json!({ "type": "block_end", "index": index }),
            StreamEvent::Finish { stop_reason, usage } => match usage {
                Some(usage) => {
                    json!({ "type": "finish", "stop_reason": stop_reason, "usage": usage })
                }
                None => json!({ "type": "finish", "stop_reason": stop_reason }),
            },
            _ => return Err(LibraryValueError::UnsupportedStreamEvent),
        };
        Ok(Self(json))
    }

    /// Raw JSON, as a writer that does not use the library sends it.
    pub fn from_json(json: serde_json::Value) -> Self {
        Self(json)
    }

    pub fn as_json(&self) -> &serde_json::Value {
        &self.0
    }

    /// Read as the library's stream event, by the library's rules, without
    /// copying the JSON.
    pub fn to_event(&self) -> Result<StreamEvent, serde_json::Error> {
        StreamEvent::deserialize(&self.0)
    }

    /// Read as the library's stream event, by the library's rules, consuming
    /// the JSON.
    pub fn into_event(self) -> Result<StreamEvent, serde_json::Error> {
        serde_json::from_value(self.0)
    }
}

/// The lane-A frame a writer publishes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkFrame {
    pub accumulator: String,
    pub key: Uuid,
    pub seq: u64,
    pub chunk: Chunk,
}

impl ChunkFrame {
    pub fn new(message_id: Uuid, seq: u64, chunk: Chunk) -> Self {
        Self {
            accumulator: MESSAGE_CONTENT_ACCUMULATOR.to_owned(),
            key: message_id,
            seq,
            chunk,
        }
    }

    pub fn subject(&self) -> String {
        chunk_subject(self.key)
    }

    /// The `Nats-Msg-Id` header: `{message_id}:{seq}`, the broker's duplicate
    /// window keyed on the chunk's place in its stream.
    pub fn msg_id(&self) -> String {
        format!("{}:{}", self.key, self.seq)
    }
}

/// The digest a seal declares: lowercase hex SHA-256 over every chunk, in
/// `seq` order, each serialized as compact JSON with object keys in byte order.
///
/// That is the form the engine reads a chunk back in: a stored chunk is
/// `jsonb`, which keeps no key order, and the engine hashes the decoded value.
/// The keys are therefore ordered here, whatever the serializer of the writer
/// does — never hash the bytes as published.
pub fn seal_hash<'a>(
    chunks: impl IntoIterator<Item = &'a Chunk>,
) -> Result<String, serde_json::Error> {
    let mut hasher = Sha256::new();
    let mut bytes = Vec::new();
    for chunk in chunks {
        bytes.clear();
        write_canonical(&mut bytes, &serde_json::to_value(chunk)?)?;
        hasher.update(&bytes);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn write_canonical(out: &mut Vec<u8>, value: &serde_json::Value) -> Result<(), serde_json::Error> {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            out.push(b'{');
            for (position, (key, value)) in entries.into_iter().enumerate() {
                if position > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, key)?;
                out.push(b':');
                write_canonical(out, value)?;
            }
            out.push(b'}');
        }
        serde_json::Value::Array(values) => {
            out.push(b'[');
            for (position, value) in values.iter().enumerate() {
                if position > 0 {
                    out.push(b',');
                }
                write_canonical(out, value)?;
            }
            out.push(b']');
        }
        scalar => serde_json::to_writer(&mut *out, scalar)?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use br_llm_messages::{BlockKind as StreamBlockKind, StopReason, ToolCallId, ToolName, Usage};

    use super::*;
    use crate::content::{ArtifactRef, BlockBody};

    fn library(event: StreamEvent) -> LibraryEvent {
        LibraryEvent::of(&event).unwrap()
    }

    fn text_start(block_id: Uuid) -> Chunk {
        Chunk {
            item_index: 0,
            part: ChunkPart::StepEvent {
                event: library(StreamEvent::BlockStart {
                    index: 0,
                    kind: StreamBlockKind::Text,
                }),
                block_id: Some(block_id),
                model: Some("m-1".into()),
            },
        }
    }

    #[test]
    fn a_step_event_chunk_names_its_item_its_block_and_its_model() {
        let block_id = Uuid::now_v7();
        let wire = serde_json::to_value(text_start(block_id)).unwrap();
        assert_eq!(
            wire,
            json!({
                "item_index": 0,
                "kind": "STEP_EVENT",
                "event": { "type": "block_start", "index": 0, "kind": "text" },
                "block_id": block_id,
                "model": "m-1"
            })
        );
        assert_eq!(
            serde_json::from_value::<Chunk>(wire).unwrap(),
            text_start(block_id)
        );
    }

    /// Every stream event, with the JSON `br-llm-messages` 0.3 serialized it
    /// to — the wire of a 0.2 chunk, generated from that version's own
    /// `Serialize` — which the contract writes as the same JSON value and the
    /// library loads back to the same event.
    #[test]
    fn every_stream_event_is_written_as_the_json_of_a_0_2_chunk() {
        let id = || ToolCallId::new("call_1").unwrap();
        let golden: Vec<(StreamEvent, &str)> = vec![
            (
                StreamEvent::BlockStart {
                    index: 0,
                    kind: StreamBlockKind::Text,
                },
                r#"{"type":"block_start","index":0,"kind":"text"}"#,
            ),
            (
                StreamEvent::BlockStart {
                    index: 1,
                    kind: StreamBlockKind::Thinking,
                },
                r#"{"type":"block_start","index":1,"kind":"thinking"}"#,
            ),
            (
                StreamEvent::BlockStart {
                    index: 2,
                    kind: StreamBlockKind::RedactedThinking,
                },
                r#"{"type":"block_start","index":2,"kind":"redacted_thinking"}"#,
            ),
            (
                StreamEvent::BlockStart {
                    index: 3,
                    kind: StreamBlockKind::Structured,
                },
                r#"{"type":"block_start","index":3,"kind":"structured"}"#,
            ),
            (
                StreamEvent::BlockStart {
                    index: 4,
                    kind: StreamBlockKind::ToolCall,
                },
                r#"{"type":"block_start","index":4,"kind":"tool_call"}"#,
            ),
            (
                StreamEvent::TextDelta {
                    index: 0,
                    text: " hello ".into(),
                },
                r#"{"type":"text_delta","index":0,"text":" hello "}"#,
            ),
            (
                StreamEvent::ThinkingDelta {
                    index: 1,
                    text: "\tlet me think".into(),
                },
                r#"{"type":"thinking_delta","index":1,"text":"\tlet me think"}"#,
            ),
            (
                StreamEvent::SignatureDelta {
                    index: 1,
                    signature: "c2ln".into(),
                },
                r#"{"type":"signature_delta","index":1,"signature":"c2ln"}"#,
            ),
            (
                StreamEvent::RedactedThinkingData {
                    index: 2,
                    data: "b3BhcXVl".into(),
                },
                r#"{"type":"redacted_thinking_data","index":2,"data":"b3BhcXVl"}"#,
            ),
            (
                StreamEvent::StructuredDelta {
                    index: 3,
                    json_fragment: "{\"a\":".into(),
                },
                r#"{"type":"structured_delta","index":3,"json_fragment":"{\"a\":"}"#,
            ),
            (
                StreamEvent::ToolCallStart {
                    index: 4,
                    id: id(),
                    name: ToolName::new("search").unwrap(),
                },
                r#"{"type":"tool_call_start","index":4,"id":"call_1","name":"search"}"#,
            ),
            (
                StreamEvent::ToolCallArgumentsDelta {
                    index: 4,
                    json_fragment: "{\"q\":1}".into(),
                },
                r#"{"type":"tool_call_arguments_delta","index":4,"json_fragment":"{\"q\":1}"}"#,
            ),
            (
                StreamEvent::BlockEnd { index: 4 },
                r#"{"type":"block_end","index":4}"#,
            ),
            (
                StreamEvent::Finish {
                    stop_reason: StopReason::AwaitingToolResults,
                    usage: Some(Usage {
                        input_tokens: 3,
                        output_tokens: 5,
                        cache_read_tokens: Some(7),
                        cache_write_tokens: None,
                        thinking_tokens: Some(11),
                    }),
                },
                r#"{"type":"finish","stop_reason":{"type":"awaiting_tool_results"},"usage":{"input_tokens":3,"output_tokens":5,"cache_read_tokens":7,"thinking_tokens":11}}"#,
            ),
            (
                StreamEvent::Finish {
                    stop_reason: StopReason::Other {
                        reason: "length".into(),
                    },
                    usage: None,
                },
                r#"{"type":"finish","stop_reason":{"type":"other","reason":"length"}}"#,
            ),
        ];
        for (event, wire) in golden {
            let written = LibraryEvent::of(&event).unwrap();
            // The same JSON value: key order is no part of the wire (a chunk
            // is stored as `jsonb`, and `seal_hash` orders keys).
            assert_eq!(
                written.as_json(),
                &serde_json::from_str::<serde_json::Value>(wire).unwrap(),
                "{event:?}"
            );
            assert_eq!(written.to_event().unwrap(), event, "{wire}");
            let read: LibraryEvent = serde_json::from_str(wire).unwrap();
            assert_eq!(read.into_event().unwrap(), event, "{wire}");
        }
    }

    #[test]
    fn an_event_the_library_refuses_still_decodes_so_that_threads_can_judge_it() {
        let wire = json!({
            "item_index": 0,
            "kind": "STEP_EVENT",
            "event": { "type": "tool_call_start", "index": 0, "id": "c1", "name": "web.search" },
            "block_id": Uuid::now_v7()
        });
        let chunk: Chunk = serde_json::from_value(wire).unwrap();
        let ChunkPart::StepEvent { event, .. } = chunk.part else {
            panic!("a step event chunk");
        };
        assert!(event.to_event().is_err());
    }

    #[test]
    fn a_threads_block_chunk_carries_the_whole_block() {
        let chunk = Chunk {
            item_index: 2,
            part: ChunkPart::Block {
                block: Block::new(
                    Uuid::now_v7(),
                    BlockBody::Artifact(ArtifactRef {
                        artifact_id: Uuid::now_v7(),
                    }),
                ),
            },
        };
        let wire = serde_json::to_value(&chunk).unwrap();
        assert_eq!(wire["kind"], "BLOCK");
        assert_eq!(wire["block"]["kind"], "ARTIFACT");
        assert_eq!(serde_json::from_value::<Chunk>(wire).unwrap(), chunk);
    }

    #[test]
    fn a_frame_is_published_on_the_messages_subject_under_its_place() {
        let message_id = Uuid::now_v7();
        let frame = ChunkFrame::new(message_id, 7, text_start(Uuid::now_v7()));
        assert_eq!(frame.subject(), format!("stream.threads.{message_id}"));
        assert_eq!(frame.msg_id(), format!("{message_id}:7"));
        let wire = serde_json::to_value(&frame).unwrap();
        assert_eq!(wire["accumulator"], "message_content");
        assert_eq!(wire["key"], json!(message_id));
        assert_eq!(wire["seq"], 7);
    }

    #[test]
    fn the_seal_hash_ignores_the_writers_key_order() {
        let block_id = Uuid::now_v7();
        let chunk = text_start(block_id);
        let reordered = json!({
            "model": "m-1",
            "block_id": block_id,
            "event": { "kind": "text", "index": 0, "type": "block_start" },
            "kind": "STEP_EVENT",
            "item_index": 0
        });
        let decoded: Chunk = serde_json::from_value(reordered).unwrap();
        assert_eq!(seal_hash([&chunk]).unwrap(), seal_hash([&decoded]).unwrap());
    }

    #[test]
    fn the_seal_hash_is_the_sha256_of_each_chunk_with_sorted_keys_in_order() {
        let finish = Chunk {
            item_index: 0,
            part: ChunkPart::StepEvent {
                event: library(StreamEvent::Finish {
                    stop_reason: StopReason::EndTurn,
                    usage: None,
                }),
                block_id: None,
                model: None,
            },
        };
        let expected = {
            let mut hasher = Sha256::new();
            hasher.update(
                br#"{"event":{"stop_reason":{"type":"end_turn"},"type":"finish"},"item_index":0,"kind":"STEP_EVENT"}"#,
            );
            hasher
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(seal_hash([&finish]).unwrap(), expected);

        let two = [text_start(Uuid::now_v7()), finish.clone()];
        let swapped = [finish, two[0].clone()];
        assert_ne!(seal_hash(&two).unwrap(), seal_hash(&swapped).unwrap());
        assert_eq!(seal_hash(&two).unwrap().len(), 64);
    }
}
