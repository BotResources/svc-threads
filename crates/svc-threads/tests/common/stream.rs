//! Playing a writer on Threads' message stream: the lane-A frames of a
//! `STREAMING` message, published on `stream.threads.{message_id}` of the
//! `STREAMING_threads` stream with their `Nats-Msg-Id`, exactly as
//! `contract_threads::ChunkFrame` describes them.
//!
//! Neither the Fabric nor the harness offers a lane-A producer (a harness and
//! library gap), and `async_nats` is banned from a suite: the frames go out
//! through the `nats` CLI, as the streams themselves are declared
//! (`super::infra`).

use contract_threads::{Chunk, ChunkFrame, seal_hash};
use uuid::Uuid;

use super::infra::nats_cli;

/// Publishes `chunks` for `message_id`, from `first_seq` on, each acknowledged
/// by JetStream before the next. Answers the seal a completion of exactly these
/// chunks declares when `first_seq` is 0: the last sequence and the digest.
pub async fn push(
    nats_url: &str,
    message_id: Uuid,
    first_seq: u64,
    chunks: &[Chunk],
) -> (u64, String) {
    for (offset, chunk) in chunks.iter().enumerate() {
        let frame = ChunkFrame::new(message_id, first_seq + offset as u64, chunk.clone());
        push_frame(nats_url, &frame).await;
    }
    let last_seq = first_seq + chunks.len() as u64 - 1;
    (
        last_seq,
        seal_hash(chunks).expect("the chunks of a scenario serialize"),
    )
}

/// Publishes one frame as it is, under its subject and `Nats-Msg-Id`.
pub async fn push_frame(nats_url: &str, frame: &ChunkFrame) {
    let body = serde_json::to_string(frame).expect("a frame serializes");
    push_raw(nats_url, &frame.subject(), &frame.msg_id(), &body).await;
}

/// Publishes any body on a message's subject: the adversarial path (a frame the
/// contract cannot read).
pub async fn push_raw(nats_url: &str, subject: &str, msg_id: &str, body: &str) {
    let header = format!("Nats-Msg-Id:{msg_id}");
    nats_cli(
        nats_url,
        &[
            "pub",
            "--jetstream",
            "--no-templates",
            "--header",
            &header,
            subject,
            body,
        ],
    )
    .await;
}
