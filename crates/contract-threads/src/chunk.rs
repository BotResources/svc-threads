use br_llm_messages::{ResponseDelta, TurnEntry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::annotation::Annotation;
use crate::library::LibraryJson;

pub const STREAMING_STREAM: &str = "STREAMING_threads";

pub const TURN_CONTENT_ACCUMULATOR: &str = "turn_content";

pub fn chunk_subject(message_id: Uuid) -> String {
    format!("stream.{}.{message_id}", crate::SERVICE)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Chunk {
    #[serde(rename = "DELTA")]
    Delta { delta: LibraryJson },
    #[serde(rename = "ENTRY")]
    Entry { entry: LibraryJson },
    #[serde(rename = "ANNOTATION")]
    Annotation {
        after_entry_id: Uuid,
        annotation: Annotation,
    },
    #[serde(rename = "KEEPALIVE")]
    Keepalive,
}

impl Chunk {
    pub fn delta(delta: &ResponseDelta) -> Result<Self, serde_json::Error> {
        Ok(Self::Delta {
            delta: LibraryJson::of(delta)?,
        })
    }

    pub fn entry(entry: &TurnEntry) -> Result<Self, serde_json::Error> {
        Ok(Self::Entry {
            entry: LibraryJson::of(entry)?,
        })
    }

    pub fn keepalive() -> Self {
        Self::Keepalive
    }

    pub fn annotation(after_entry_id: Uuid, annotation: Annotation) -> Self {
        Self::Annotation {
            after_entry_id,
            annotation,
        }
    }
}

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

    pub fn msg_id(&self) -> String {
        format!("{}:{}", self.key, self.seq)
    }
}

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
    fn a_keepalive_chunk_has_no_field_but_its_kind() {
        let wire = serde_json::to_value(Chunk::keepalive()).unwrap();
        assert_eq!(wire, json!({ "kind": "KEEPALIVE" }));
        assert_eq!(
            serde_json::from_value::<Chunk>(wire).unwrap(),
            Chunk::Keepalive
        );
    }

    #[test]
    fn a_keepalive_enters_the_seal_hash_like_any_chunk() {
        let delta = started(Uuid::now_v7(), Uuid::now_v7());
        let without = seal_hash([&delta]).unwrap();
        let with = seal_hash([&delta, &Chunk::Keepalive]).unwrap();
        assert_ne!(without, with);
        let expected = {
            let mut hasher = Sha256::new();
            hasher.update(br#"{"kind":"KEEPALIVE"}"#);
            hasher
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(seal_hash([&Chunk::Keepalive]).unwrap(), expected);
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
