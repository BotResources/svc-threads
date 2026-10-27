//! The content of a `STREAMING` message, as its writer pushes it on the
//! engine's accumulated lane.
//!
//! A frame is published on [`chunk_subject`]`(message_id)` of the
//! [`STREAMING_STREAM`] stream, with `Nats-Msg-Id` = [`ChunkFrame::msg_id`], no
//! integration envelope:
//!
//! ```json
//! { "accumulator": "turn_content", "key": "<message_id>", "seq": 0, "chunk": { … } }
//! ```
//!
//! `seq` starts at 0 and is contiguous. A chunk is one of three kinds:
//!
//! - `DELTA`: one response delta of the library (`br-llm-messages-delta/1`),
//!   folded with the library's response draft of its entry. A response opens
//!   with its `response_started` delta and ends with its `response_completed`
//!   (or `response_interrupted`); one response is produced at a time.
//! - `ENTRY`: one whole entry of the turn other than a model response (runtime
//!   tool results, an incoming entry, a task notice). The closure is Threads':
//!   `message.complete`, `fail` or `cancel` closes the turn.
//! - `ANNOTATION`: one whole annotation, after an entry of the turn
//!   (`after_entry_id`).
//!
//! A chunk the library refuses fails the message; a chunk after the seal is
//! refused on every pod; a chunk is never a fact. The writer closes the stream
//! with `message.complete` (or `fail` / `cancel`) carrying the [`seal_hash`]
//! of every chunk it pushed.

use br_llm_messages::{ResponseDelta, TurnEntry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::annotation::Annotation;
use crate::library::LibraryJson;

/// The stream the engine binds for Threads (`STREAMING_{service}`), declared out
/// of band like every stream.
pub const STREAMING_STREAM: &str = "STREAMING_threads";

/// The accumulator a message's turn folds into. (Threads 0.4 named it
/// `message_content`: a frame under that name is refused.)
pub const TURN_CONTENT_ACCUMULATOR: &str = "turn_content";

/// `stream.threads.{message_id}`: the engine's accumulated-lane subject for one
/// message. A UUID is a token-safe key, so the token is the id itself.
pub fn chunk_subject(message_id: Uuid) -> String {
    format!("stream.{}.{message_id}", crate::SERVICE)
}

/// One chunk of a message's turn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Chunk {
    /// One response delta, the library's JSON for it.
    #[serde(rename = "DELTA")]
    Delta { delta: LibraryJson },
    /// One whole entry other than a model response, the library's JSON for it.
    #[serde(rename = "ENTRY")]
    Entry { entry: LibraryJson },
    /// One whole annotation, after an entry of the turn.
    #[serde(rename = "ANNOTATION")]
    Annotation {
        after_entry_id: Uuid,
        annotation: Annotation,
    },
}

impl Chunk {
    /// The chunk of a library response delta.
    pub fn delta(delta: &ResponseDelta) -> Result<Self, serde_json::Error> {
        Ok(Self::Delta {
            delta: LibraryJson::of(delta)?,
        })
    }

    /// The chunk of a whole library entry.
    pub fn entry(entry: &TurnEntry) -> Result<Self, serde_json::Error> {
        Ok(Self::Entry {
            entry: LibraryJson::of(entry)?,
        })
    }

    pub fn annotation(after_entry_id: Uuid, annotation: Annotation) -> Self {
        Self::Annotation {
            after_entry_id,
            annotation,
        }
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
            accumulator: TURN_CONTENT_ACCUMULATOR.to_owned(),
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
    use br_llm_messages::{DeltaOp, EntryId, ResponseMetadata, TurnId};
    use serde_json::json;

    use super::*;
    use crate::annotation::{AnnotationBody, ArtifactRef};

    fn started(turn: Uuid, entry: Uuid) -> Chunk {
        Chunk::delta(&ResponseDelta::new(
            TurnId::from_uuid(turn).unwrap(),
            EntryId::from_uuid(entry).unwrap(),
            DeltaOp::response_started(ResponseMetadata::new()),
        ))
        .unwrap()
    }

    #[test]
    fn a_delta_chunk_carries_the_librarys_delta_json() {
        let (turn, entry) = (Uuid::now_v7(), Uuid::now_v7());
        let chunk = started(turn, entry);
        let wire = serde_json::to_value(&chunk).unwrap();
        assert_eq!(wire["kind"], "DELTA");
        assert_eq!(wire["delta"]["type"], "response_started");
        assert_eq!(wire["delta"]["turn"], json!(turn));
        let back: Chunk = serde_json::from_value(wire).unwrap();
        let Chunk::Delta { delta } = back else {
            panic!("a delta chunk");
        };
        assert_eq!(delta.to_delta().unwrap().entry().as_uuid(), entry);
    }

    #[test]
    fn a_delta_the_library_refuses_still_decodes_so_that_threads_can_judge_it() {
        let chunk: Chunk = serde_json::from_value(json!({
            "kind": "DELTA",
            "delta": { "type": "text_appended", "turn": "not-a-uuid", "entry": "x", "block": "y", "text": "hi" }
        }))
        .unwrap();
        let Chunk::Delta { delta } = chunk else {
            panic!("a delta chunk");
        };
        assert!(delta.to_delta().is_err());
    }

    #[test]
    fn an_annotation_chunk_carries_the_whole_annotation() {
        let entry = Uuid::now_v7();
        let chunk = Chunk::annotation(
            entry,
            Annotation::new(
                Uuid::now_v7(),
                AnnotationBody::Artifact(ArtifactRef {
                    artifact_id: Uuid::now_v7(),
                }),
            ),
        );
        let wire = serde_json::to_value(&chunk).unwrap();
        assert_eq!(wire["kind"], "ANNOTATION");
        assert_eq!(wire["after_entry_id"], json!(entry));
        assert_eq!(wire["annotation"]["kind"], "ARTIFACT");
        assert_eq!(serde_json::from_value::<Chunk>(wire).unwrap(), chunk);
    }

    #[test]
    fn a_frame_is_published_on_the_messages_subject_under_its_place() {
        let message_id = Uuid::now_v7();
        let frame = ChunkFrame::new(message_id, 7, started(message_id, Uuid::now_v7()));
        assert_eq!(frame.subject(), format!("stream.threads.{message_id}"));
        assert_eq!(frame.msg_id(), format!("{message_id}:7"));
        let wire = serde_json::to_value(&frame).unwrap();
        assert_eq!(wire["accumulator"], "turn_content");
        assert_eq!(wire["key"], json!(message_id));
        assert_eq!(wire["seq"], 7);
    }

    #[test]
    fn the_seal_hash_ignores_the_writers_key_order() {
        let chunk = started(Uuid::now_v7(), Uuid::now_v7());
        let mut reordered = serde_json::Map::new();
        let wire = serde_json::to_value(&chunk).unwrap();
        for (key, value) in wire.as_object().unwrap().iter().rev() {
            reordered.insert(key.clone(), value.clone());
        }
        let decoded: Chunk = serde_json::from_value(serde_json::Value::Object(reordered)).unwrap();
        assert_eq!(seal_hash([&chunk]).unwrap(), seal_hash([&decoded]).unwrap());
    }

    #[test]
    fn the_seal_hash_is_the_sha256_of_each_chunk_with_sorted_keys_in_order() {
        let entry = Chunk::Entry {
            entry: LibraryJson::from_json(json!({ "type": "task_started", "b": 1, "a": 2 })),
        };
        let expected = {
            let mut hasher = Sha256::new();
            hasher.update(br#"{"entry":{"a":2,"b":1,"type":"task_started"},"kind":"ENTRY"}"#);
            hasher
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(seal_hash([&entry]).unwrap(), expected);
        let other = started(Uuid::now_v7(), Uuid::now_v7());
        let two = [entry.clone(), other.clone()];
        let swapped = [other, entry];
        assert_ne!(seal_hash(&two).unwrap(), seal_hash(&swapped).unwrap());
        assert_eq!(seal_hash(&two).unwrap().len(), 64);
    }
}
