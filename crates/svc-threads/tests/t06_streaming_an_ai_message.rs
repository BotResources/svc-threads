//! **T06 — Streaming an AI message: deltas folded live, sealed by last chunk
//! and hash.**
//!
//! > Playing the agents service, the harness stores AI message M2 (agent a1,
//! > name "Analyst", job_id J) STREAMING, parent M1: confirmed with status
//! > STREAMING, no content. Playing the runner, it publishes on
//! > stream.threads.{M2} chunks seq 0..n: the response deltas of a first model
//! > response R1 by model m-1, opening a text block and a runtime tool call,
//! > growing them and ending R1 on tool use; a whole entry of runtime tool
//! > results answering the call; the response deltas of a second response R2
//! > holding a text, ending it; then an ARTIFACT annotation after R2 naming an
//! > artifact a runner uploaded for the thread. A member following T sees R1's
//! > draft grow and each entry complete on its own while M2 still streams; a
//! > member joining mid-stream sees everything already folded, the current
//! > draft included. A completion with a wrong hash, or with last_seq below a
//! > chunk that arrived, is refused STREAM_MISMATCH and the message stays
//! > STREAMING. The corrected completion, last_seq n and the SHA-256 of the
//! > chunks, is confirmed COMPLETED: M2 reads as an AI turn of agent a1 holding
//! > R1 with model m-1, the tool results, R2 and a completed closure, the
//! > ARTIFACT annotation after R2, the artifact attached, and the library loads
//! > the turn. The facts record M2's opening, each entry and its end, never a
//! > chunk. A chunk published after the seal is ignored on every pod: M2 reads
//! > exactly as sealed on two pods and nothing is pushed. Completing M2 again
//! > is refused MESSAGE_NOT_STREAMING.
//!
//! What Threads must show is what the library's own draft folds from the same
//! deltas (`common::content::Streamed`): a draft while R1 is produced, the
//! library's response once its draft completes it. The closure is Threads'
//! (its id minted by Threads); a fact for each entry means R1, the tool
//! results and R2 — the closure is the `Completed` fact itself.
//!
//! Channels: the answers (4); the live feed of a member over SSE and of a late
//! joiner over WebSocket (3); the message as members read it, on two pods (2);
//! the fact log; the artifact the annotation attaches.

mod common;

use common::artifacts::{File, download_url, fetch, upload};
use common::content::{
    Streamed, annotation_chunk, annotation_view, artifact, closure_kind, complete_entry,
    draft_entry, draft_text, entries_of, entry_chunk, entry_view, result_of, results, to_json,
    turn_of,
};
use common::facts;
use common::feed::{Live, THREAD_FEED, in_reset, thread_feed, upsert_of};
use common::flows::{
    ai, command, completion, create_thread, message_confirmed, message_refused, message_view, say,
    store, streaming_message, uuid_of,
};
use common::{Q_MESSAGES, TestContext, gql, stream};
use contract_threads::llm::{ResponseEnding, TurnEntry};
use contract_threads::{MessageStatus, ReasonCode, seal_hash};
use serde_json::{Value, json};
use uuid::Uuid;

/// The draft text of `entry` in a feed's view of M2, if it is still produced.
fn producing_text(view: &Value, entry: Uuid) -> Option<String> {
    entries_of(view)
        .into_iter()
        .find(|read| read["id"] == json!(entry) && read["state"] == "PRODUCING")
        .map(|read| draft_text(&draft_entry(&read)))
}

/// Whether `entry` reads COMPLETE in a feed's view of M2.
fn complete_in(view: &Value, entry: Uuid) -> bool {
    entries_of(view)
        .iter()
        .any(|read| read["id"] == json!(entry) && read["state"] == "COMPLETE")
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t06_a_streaming_ai_message_is_folded_live_and_sealed_by_its_last_chunk_and_digest() {
    // Given T with members O, A and B, A's question M1, an artifact X a runner
    // uploaded for T, a second pod of the deployment, and A following T.
    let mut ctx = TestContext::setup().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b]).await;
    let m1 = say(&ctx, t, None, &a, "Please analyse Q3").await;
    let runner = ctx.service_account(true);
    let file = File::csv("quarter,revenue\nQ3,42\n");
    let x = upload(&ctx, &runner.passport, t, &file).await;
    let pod2 = ctx.spawn_pod().await;
    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;
    let analyst = ai("a1", "Analyst");

    // When the agents service opens M2 STREAMING, by a1 "Analyst", for Job J.
    let job = Uuid::now_v7();
    let mut open = streaming_message(t, Some(m1), &analyst);
    open.job_id = Some(job);
    let m2 = store(&ctx, open, MessageStatus::Streaming).await;

    // Then (2) M2 reads STREAMING, an AI turn of a1, for J, with no content.
    let view = message_view(&ctx, &a.passport, t, m2).await;
    assert_eq!(view["status"], "STREAMING");
    assert_eq!(view["turnKind"], "AI_TURN");
    assert_eq!(
        view["author"],
        json!({ "kind": "AI", "userId": null, "aiId": analyst.uuid(), "name": "Analyst" })
    );
    assert_eq!(view["jobId"], Value::String(job.to_string()));
    assert_eq!(view["parentMessageId"], Value::String(m1.to_string()));
    assert_eq!(view["entries"], json!([]));
    // And (3) A's feed receives it.
    a_feed
        .until("M2 enters A's feed", |delta| upsert_of(delta, m2).is_some())
        .await;

    // R1, by m-1: a text block growing, a runtime tool call, ending on tool use.
    let mut r1 = Streamed::start(m2, Some("m-1"));
    r1.text(&["Hel", "lo"], true);
    let call = r1.call("search", &["{\"q\":", "\"rust\"}"]);
    r1.complete(ResponseEnding::tool_use());
    // The whole entry of runtime tool results answering the call.
    let answered = results(vec![result_of(&r1.runtime_call(call), "found", false)]);
    // R2: a text, ending the response.
    let mut r2 = Streamed::start(m2, None);
    r2.text(&["Done."], true);
    r2.complete(ResponseEnding::finished());
    // Then the ARTIFACT annotation after R2.
    let attached = artifact(x);
    let r1_chunks = r1.chunks();
    let chunks: Vec<_> = [
        r1_chunks.clone(),
        vec![entry_chunk(&answered)],
        r2.chunks(),
        vec![annotation_chunk(r2.entry, attached.clone())],
    ]
    .concat();
    let n = (chunks.len() - 1) as u64;
    let hash = seal_hash(&chunks).expect("the chunks hash");
    let nats = ctx.nats.url();

    // When the runner pushes R1's opening, its text block and "Hel".
    stream::push(&nats, m2, 0, &chunks[0..3]).await;
    // Then (3) A sees R1's draft hold "Hel".
    let r1_entry = r1.entry.as_uuid();
    a_feed
        .until("A sees R1's draft hold `Hel`", |delta| {
            upsert_of(delta, m2)
                .and_then(|view| producing_text(view, r1_entry))
                .as_deref()
                == Some("Hel")
        })
        .await;

    // When it pushes "lo".
    stream::push(&nats, m2, 3, &chunks[3..4]).await;
    // Then (3) A sees the draft grow, as the library's draft folds it.
    let grown = draft_text(&r1.draft_after(4));
    assert_eq!(grown, "Hello");
    a_feed
        .until("A sees R1's draft grow to `Hello`", |delta| {
            upsert_of(delta, m2)
                .and_then(|view| producing_text(view, r1_entry))
                .as_deref()
                == Some(grown.as_str())
        })
        .await;

    // When B joins mid-stream, over WebSocket.
    let b_feed = Live::ws(&ctx, &b.passport, THREAD_FEED, &thread_feed(t, 20)).await;
    // Then B's Reset holds everything folded so far, the current draft included.
    let joined = in_reset(&b_feed.reset, m2).expect("M2 is in B's Reset");
    assert_eq!(joined["status"], "STREAMING");
    assert_eq!(producing_text(&joined, r1_entry).as_deref(), Some("Hello"));

    // When the rest of R1 arrives.
    stream::push(&nats, m2, 4, &chunks[4..r1_chunks.len()]).await;
    // Then (3) R1 completes on its own while M2 still streams.
    a_feed
        .until("R1 completes on its own", |delta| {
            upsert_of(delta, m2)
                .is_some_and(|view| view["status"] == "STREAMING" && complete_in(view, r1_entry))
        })
        .await;

    // When the tool results, R2 and the annotation arrive.
    stream::push(
        &nats,
        m2,
        r1_chunks.len() as u64,
        &chunks[r1_chunks.len()..],
    )
    .await;
    // Then (3) each entry completes on its own, M2 still streaming.
    let (results_entry, r2_entry) = (answered.id().as_uuid(), r2.entry.as_uuid());
    a_feed
        .until(
            "the tool results, then R2, complete while M2 streams",
            |delta| {
                upsert_of(delta, m2).is_some_and(|view| {
                    view["status"] == "STREAMING"
                        && complete_in(view, results_entry)
                        && complete_in(view, r2_entry)
                        && entry_view(view, r2.entry)["annotations"]
                            .as_array()
                            .map(Vec::len)
                            == Some(1)
                })
            },
        )
        .await;

    // When the writer completes M2 with a wrong digest, then with a last_seq
    // below a chunk that arrived.
    let wrong_hash = command(&ctx, &completion(t, m2, n, &"0".repeat(64))).await;
    message_refused(
        &ctx,
        wrong_hash,
        ReasonCode::StreamMismatch,
        "a wrong digest",
    )
    .await;
    let short = command(
        &ctx,
        &completion(t, m2, n - 1, &seal_hash(&chunks[..n as usize]).unwrap()),
    )
    .await;
    message_refused(
        &ctx,
        short,
        ReasonCode::StreamMismatch,
        "a last_seq too low",
    )
    .await;
    // Then M2 stays STREAMING.
    assert_eq!(
        message_view(&ctx, &a.passport, t, m2).await["status"],
        "STREAMING"
    );

    // When the corrected completion arrives: last_seq n, the chunks' digest.
    let completed = command(&ctx, &completion(t, m2, n, &hash)).await;

    // Then (4) confirmed COMPLETED.
    let confirmed =
        message_confirmed(&ctx, completed, MessageStatus::Completed, "the completion").await;
    assert_eq!(confirmed.message_id, m2);

    // And (2) M2 reads as an AI turn of a1 holding R1 (model m-1), the tool
    // results, R2 and a completed closure — each entry as the library gives it.
    let sealed = message_view(&ctx, &a.passport, t, m2).await;
    assert_eq!(sealed["status"], "COMPLETED");
    let read = entries_of(&sealed);
    assert_eq!(read.len(), 4, "{sealed}");
    assert!(
        read.iter().all(|entry| entry["state"] == "COMPLETE"),
        "{sealed}"
    );
    let r1_done = TurnEntry::from(r1.response());
    assert_eq!(
        r1.response().model().map(|model| model.as_str()),
        Some("m-1")
    );
    for (index, expected) in [r1_done, answered.clone(), TurnEntry::from(r2.response())]
        .iter()
        .enumerate()
    {
        assert_eq!(
            to_json(&complete_entry(&read[index])),
            to_json(expected),
            "entry {index} of M2 is the library's"
        );
    }
    assert_eq!(
        closure_kind(&sealed).as_deref(),
        Some("completed"),
        "{sealed}"
    );
    let closure_id = uuid_of(&read[3]["id"]);
    assert_eq!(
        closure_id.get_version_num(),
        7,
        "the closure's id is a UUIDv7"
    );
    // And the library loads the turn.
    let turn = turn_of(&sealed);
    let ai_turn = turn.as_ai().expect("an AI turn");
    assert_eq!(ai_turn.agent(), analyst.id);
    assert!(ai_turn.is_closed());
    // And the ARTIFACT annotation reads after R2.
    assert_eq!(
        entry_view(&sealed, r2.entry)["annotations"],
        json!([{
            "index": 0,
            "id": annotation_view(&attached)["id"],
            "kind": "ARTIFACT",
            "content": { "artifact_id": x },
        }])
    );
    // And the artifact it names is attached: members read its bytes.
    let url = download_url(&ctx, &b.passport, t, x)
        .await
        .expect("a member reads the attached artifact");
    assert_eq!(fetch(&url).await, (200, file.bytes.clone()));
    // And (3) A's feed carries the completion.
    a_feed
        .until("A sees M2 COMPLETED", |delta| {
            upsert_of(delta, m2).is_some_and(|view| view["status"] == "COMPLETED")
        })
        .await;

    // And the facts recorded M2's opening, each entry, and its end — never a
    // chunk.
    let m2_facts = facts::of(&ctx.pool, "message", m2).await;
    assert_eq!(
        facts::types(&m2_facts),
        vec![
            "Created",
            "EntryAdded",
            "EntryAdded",
            "EntryAdded",
            "Completed"
        ],
        "one EntryAdded per entry, none per chunk"
    );
    facts::assert_no_fact_carries(&ctx.pool, "Hello", "a streamed text").await;

    // Given B following T on the second pod.
    let mut b_on_pod2 = Live::sse_on(
        &pod2.base_url,
        &b.passport,
        THREAD_FEED,
        &thread_feed(t, 20),
    )
    .await;
    assert_eq!(
        in_reset(&b_on_pod2.reset, m2).expect("M2 on pod 2")["status"],
        "COMPLETED"
    );

    // When a chunk is published after the seal: the opening of a response R3.
    let r3 = Streamed::start(m2, None);
    stream::push(&nats, m2, n + 1, &r3.chunks()).await;

    // Then it is ignored on every pod: M2 reads exactly as sealed on both, and
    // nothing is pushed.
    a_feed.silence("a chunk after the seal").await;
    b_on_pod2.silence("a chunk after the seal, on pod 2").await;
    assert_eq!(message_view(&ctx, &a.passport, t, m2).await, sealed);
    let on_pod2 = pod2
        .run(
            &a.passport,
            Q_MESSAGES,
            json!({ "thread": t, "size": 1, "before": null, "tip": m2 }),
        )
        .await;
    assert_eq!(gql::data(&on_pod2, "threadsMessages")[0], sealed);

    // When M2 is completed again.
    let again = command(&ctx, &completion(t, m2, n, &hash)).await;
    // Then refused MESSAGE_NOT_STREAMING, and M2 never changes.
    message_refused(
        &ctx,
        again,
        ReasonCode::MessageNotStreaming,
        "completing twice",
    )
    .await;
    assert_eq!(message_view(&ctx, &a.passport, t, m2).await, sealed);
    assert_eq!(
        facts::of(&ctx.pool, "message", m2).await.len(),
        m2_facts.len()
    );

    ctx.teardown().await;
}
