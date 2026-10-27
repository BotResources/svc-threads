//! The turn of a `STREAMING` message, on the engine's accumulated lane: the
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

use super::draft::{self, TurnDraft};

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

/// The accumulator of a message's turn (`turn_content`).
pub struct TurnContent;

impl TurnContent {
    pub const NAME: AccumulatorName =
        AccumulatorName::from_static(contract_threads::TURN_CONTENT_ACCUMULATOR);
}

impl Accumulator for TurnContent {
    type Noun = MessageStream;
    type Chunk = RawChunk;
    type State = TurnDraft;

    fn name(&self) -> AccumulatorName {
        Self::NAME
    }

    fn fold(&self, state: &mut TurnDraft, seq: ChunkSeq, chunk: RawChunk) {
        draft::fold(state, seq.get(), chunk.0);
    }
}

/// The accumulator name of Threads 0.4 (`message_content`). A writer still on
/// 0.4 pushes its chunks under it: they are received here and never folded into
/// a turn — the stream is refused, and its message fails, closed failed
/// ([`super::closing`], [`super::watch`]).
pub struct LegacyContent;

impl LegacyContent {
    pub const NAME: AccumulatorName = AccumulatorName::from_static("message_content");
}

/// Whether a 0.4 writer pushed under [`LegacyContent`]: the first such chunk.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LegacyFrames {
    pub first: Option<u64>,
}

impl Accumulator for LegacyContent {
    type Noun = MessageStream;
    type Chunk = RawChunk;
    type State = LegacyFrames;

    fn name(&self) -> AccumulatorName {
        Self::NAME
    }

    fn fold(&self, state: &mut LegacyFrames, seq: ChunkSeq, _chunk: RawChunk) {
        state.first.get_or_insert(seq.get());
    }
}

#[cfg(test)]
mod tests {
    use contract_threads::llm::{DeltaOp, EntryId, ResponseDelta, ResponseMetadata, TurnId};
    use contract_threads::{Chunk, MessageInvalidError};
    use uuid::Uuid;

    use super::*;

    fn seq(value: u64) -> ChunkSeq {
        ChunkSeq::new(value).unwrap()
    }

    fn started() -> RawChunk {
        let delta = ResponseDelta::new(
            TurnId::from_uuid(Uuid::now_v7()).unwrap(),
            EntryId::from_uuid(Uuid::now_v7()).unwrap(),
            DeltaOp::response_started(ResponseMetadata::new()),
        );
        RawChunk(serde_json::to_value(Chunk::delta(&delta).unwrap()).unwrap())
    }

    #[test]
    fn the_accumulator_is_the_contracts() {
        assert_eq!(TurnContent.name().as_str(), "turn_content");
    }

    #[test]
    fn a_frame_under_the_0_4_name_is_received_and_never_folded_into_a_turn() {
        let mut frames = LegacyFrames::default();
        LegacyContent.fold(&mut frames, seq(3), started());
        LegacyContent.fold(&mut frames, seq(4), started());
        assert_eq!(frames.first, Some(3));
        assert_eq!(LegacyContent.name().as_str(), "message_content");
    }

    /// The engine verifies a seal over the chunks it reads back from `jsonb`,
    /// decoded as JSON values: the digest a writer computes with the contract
    /// must be that one. It holds only while this build keeps a JSON object's
    /// keys in order (`serde_json` without `preserve_order`): a dependency that
    /// turned it on would make every seal of the service mismatch.
    #[test]
    fn the_contracts_seal_digest_is_the_one_the_engine_verifies() {
        let chunks: Vec<Chunk> = (0..3)
            .map(|_| serde_json::from_value(started().0).unwrap())
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
        let mut state = TurnDraft::default();
        TurnContent.fold(&mut state, seq(0), started());
        TurnContent.fold(
            &mut state,
            seq(1),
            RawChunk(serde_json::json!({ "kind": "NOT_A_KIND" })),
        );
        let refusal = state.refusal().cloned().expect("the stream is refused");
        assert_eq!(refusal.seq, Some(1));
        assert_eq!(refusal.code, MessageInvalidError::ChunkUnreadable.as_str());
        let before = state.clone();
        TurnContent.fold(&mut state, seq(2), started());
        assert_eq!(state, before);
    }
}
