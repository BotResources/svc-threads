//! The content of a `STREAMING` message, on the engine's accumulated lane: the
//! writer publishes ordered chunks on `stream.threads.{message_id}` of the
//! `STREAMING_threads` stream ([`contract_threads::chunk`]); every pod's
//! ephemeral consumer folds them into the engine's Postgres-backed accumulator
//! and stages an impact on the message, so its members see the content live
//! and a late joiner replays it from the store; `message.complete` (or `fail`,
//! `cancel`) seals the stream with the writer's digest, and a chunk after the
//! seal is refused on every pod.
//!
//! The engine stores a chunk as it arrives, before any decode, and replays
//! every stored chunk at each fold: a chunk this accumulator could not decode
//! would fail every fold of its message — every render, every seal — for good.
//! The accumulator's chunk is therefore the raw JSON ([`RawChunk`]), read with
//! the contract inside the fold, and a chunk the contract cannot read is
//! recorded in the state ([`MessageDraft::refused_at`]) rather than raised.
//!
//! The fold keeps the decoded chunks in order. Folding them with the library —
//! a step draft per item, the item completing on its `finish`, a refused event
//! failing the message — is the implementation pass's. It depends on what
//! `br-llm-messages` v0.3.0 does not offer yet: a `StepDraft` that is `Clone`
//! (an accumulator state must be), a snapshot of a draft's partial blocks, and
//! a way to close an unfinished draft.

use contract_threads::{Chunk, MESSAGE_CONTENT_ACCUMULATOR};
use serde::{Deserialize, Serialize};
use service_engine::accumulator::{Accumulator, ChunkSeq};
use service_engine::name::AccumulatorName;

use super::state::Message;

/// A writer that pushes nothing for this long before closing its stream has
/// its message failed.
pub const STREAM_IDLE_WINDOW: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// One chunk as the engine stores it: raw JSON, read with the contract by the
/// fold.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RawChunk(pub serde_json::Value);

/// What the stream of one message folded so far.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MessageDraft {
    /// The chunks read, in sequence order, up to the first refused one.
    pub chunks: Vec<Chunk>,
    /// The sequence of the first chunk the contract could not read: the message
    /// fails on it, and nothing after it is folded.
    pub refused_at: Option<u64>,
}

/// The accumulator of a message's content.
pub struct MessageContent;

impl MessageContent {
    pub const NAME: AccumulatorName = AccumulatorName::from_static(MESSAGE_CONTENT_ACCUMULATOR);
}

impl Accumulator for MessageContent {
    type Noun = Message;
    type Chunk = RawChunk;
    type State = MessageDraft;

    fn name(&self) -> AccumulatorName {
        Self::NAME
    }

    fn fold(&self, state: &mut MessageDraft, seq: ChunkSeq, chunk: RawChunk) {
        if state.refused_at.is_some() {
            return;
        }
        match serde_json::from_value::<Chunk>(chunk.0) {
            Ok(chunk) => state.chunks.push(chunk),
            Err(_) => state.refused_at = Some(seq.get()),
        }
    }
}

#[cfg(test)]
mod tests {
    use contract_threads::ChunkPart;
    use contract_threads::llm::{BlockKind, StreamEvent};
    use uuid::Uuid;

    use super::*;

    fn seq(value: u64) -> ChunkSeq {
        ChunkSeq::new(value).unwrap()
    }

    fn start() -> RawChunk {
        RawChunk(
            serde_json::to_value(Chunk {
                item_index: 0,
                part: ChunkPart::StepEvent {
                    event: StreamEvent::BlockStart {
                        index: 0,
                        kind: BlockKind::Text,
                    },
                    block_id: Some(Uuid::now_v7()),
                    model: None,
                },
            })
            .unwrap(),
        )
    }

    #[test]
    fn the_accumulator_is_the_contracts() {
        assert_eq!(MessageContent.name().as_str(), "message_content");
    }

    /// The engine verifies a seal over the chunks it reads back from `jsonb`,
    /// decoded as JSON values: the digest a writer computes with the contract
    /// must be that one. It holds only while this build keeps a JSON object's
    /// keys in order (`serde_json` without `preserve_order`): a dependency that
    /// turned it on would make every seal of the service mismatch.
    #[test]
    fn the_contracts_seal_digest_is_the_one_the_engine_verifies() {
        let chunks: Vec<Chunk> = (0..3)
            .map(|_| serde_json::from_value(start().0).unwrap())
            .collect();
        let as_read_back: Vec<serde_json::Value> = chunks
            .iter()
            .map(|chunk| serde_json::to_value(chunk).unwrap())
            .collect();
        assert_eq!(
            contract_threads::seal_hash(&chunks).unwrap(),
            service_engine::SealHash::of_chunks(as_read_back)
                .unwrap()
                .to_hex()
        );
    }

    #[test]
    fn readable_chunks_fold_in_order() {
        let mut draft = MessageDraft::default();
        MessageContent.fold(&mut draft, seq(0), start());
        MessageContent.fold(&mut draft, seq(1), start());
        assert_eq!(draft.chunks.len(), 2);
        assert_eq!(draft.refused_at, None);
    }

    #[test]
    fn an_unreadable_chunk_is_recorded_and_nothing_after_it_folds() {
        let mut draft = MessageDraft::default();
        MessageContent.fold(&mut draft, seq(0), start());
        MessageContent.fold(
            &mut draft,
            seq(1),
            RawChunk(serde_json::json!({ "item_index": 0, "kind": "NOT_A_PART" })),
        );
        MessageContent.fold(&mut draft, seq(2), start());
        assert_eq!(draft.chunks.len(), 1);
        assert_eq!(draft.refused_at, Some(1));
    }
}
