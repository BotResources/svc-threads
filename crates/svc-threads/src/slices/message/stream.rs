//! The content of a `STREAMING` message, on the engine's accumulated lane: the
//! writer publishes ordered chunks on `stream.threads.{message_id}` of the
//! `STREAMING_threads` stream ([`contract_threads::chunk`]); every pod's
//! ephemeral consumer stores them in the engine's Postgres-backed accumulator
//! and stages an impact on the message, so its members see the content live
//! and a late joiner replays it from the store; `message.complete` (or `fail`,
//! `cancel`, or the stream watch) seals the stream, and a chunk after the seal
//! is refused on every pod.
//!
//! The engine stores a chunk as it arrives, before any decode, and replays
//! every stored chunk at each fold: the accumulator's chunk is therefore the raw
//! JSON ([`RawChunk`]), read with the contract inside the fold
//! ([`super::draft::fold`]), and a chunk the contract or the library refuses is
//! recorded in the state rather than raised.

use serde::{Deserialize, Serialize};
use service_engine::accumulator::{Accumulator, ChunkSeq};
use service_engine::name::AccumulatorName;

use service_engine::name::NounName;
use service_engine::wire::Noun;
use uuid::Uuid;

use super::draft::{self, MessageDraft};

/// One chunk as the engine stores it: raw JSON, read with the contract by the
/// fold.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RawChunk(pub serde_json::Value);

/// The streamed content of a message, keyed by the message: the noun its
/// folded chunks are impacts on. Apart from the `message` noun so that a fold
/// re-renders the message where it is shown, and never repopulates a window
/// (only a message's own change can move a branch).
pub struct MessageStream;

impl Noun for MessageStream {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("message_stream");
}

/// The accumulator of a message's content.
pub struct MessageContent;

impl MessageContent {
    pub const NAME: AccumulatorName =
        AccumulatorName::from_static(contract_threads::MESSAGE_CONTENT_ACCUMULATOR);
}

impl Accumulator for MessageContent {
    type Noun = MessageStream;
    type Chunk = RawChunk;
    type State = MessageDraft;

    fn name(&self) -> AccumulatorName {
        Self::NAME
    }

    fn fold(&self, state: &mut MessageDraft, seq: ChunkSeq, chunk: RawChunk) {
        draft::fold(state, seq.get(), chunk.0);
    }
}

#[cfg(test)]
mod tests {
    use contract_threads::llm::{BlockKind, StreamEvent};
    use contract_threads::{Chunk, ChunkPart, LibraryEvent};
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
                    event: LibraryEvent::of(&StreamEvent::BlockStart {
                        index: 0,
                        kind: BlockKind::Text,
                    })
                    .unwrap(),
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
    fn an_unreadable_chunk_is_the_streams_refusal_and_nothing_after_it_folds() {
        let mut state = MessageDraft::default();
        MessageContent.fold(&mut state, seq(0), start());
        MessageContent.fold(
            &mut state,
            seq(1),
            RawChunk(serde_json::json!({ "item_index": 0, "kind": "NOT_A_PART" })),
        );
        let refusal = state.refusal().cloned().expect("the stream is refused");
        assert_eq!(refusal.seq, 1);
        assert_eq!(refusal.code, super::draft::chunk_error::UNREADABLE);
        let before = state.clone();
        MessageContent.fold(&mut state, seq(2), start());
        assert_eq!(state, before);
    }
}
