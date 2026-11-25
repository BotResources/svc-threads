//! **T31 — A restarted runner closes its left-open turn, finds its stored mail
//! and retries.**
//!
//! > Threads runs with a short stream window. Thread T has user message U1.
//! > Agent g's runner, a service account holding threads:runner, opens G1, an
//! > AI turn of g, STREAMING. It pushes a response calling runtime tool c1 and
//! > c1's result, then an agent's mail m1 as an incoming entry whose entry id
//! > is the mail's id, then part of a text response, and then pushes nothing
//! > more. Before the window ends, a restarted run of the same agent reads T's
//! > context naming g: G1 is there as g's own turn, with its response, c1's
//! > result and m1, found by its id. The run fails G1 with no seal, naming
//! > interrupted: confirmed FAILED; G1's turn is closed interrupted; the half
//! > text is kept as the library's interrupted response (members read it, a
//! > runner never receives it); the Failed fact names interrupted. The run
//! > stores G2, a retry of G1, under new message, entry and block ids, streams
//! > it with a WARNING annotation and completes it: confirmed COMPLETED. G1
//! > stays readable before G2, and the context naming g holds U1, G1 closed
//! > interrupted without its half text, and G2. A retry G3 stored whole that
//! > reuses an entry id or a block id of G1, or an annotation id of G2, is
//! > refused KEY_REUSED (each tried alone); a streamed G3 that reuses one is
//! > stored FAILED with the code reused_identity. The run then stores I1, an
//! > input turn holding mail m2, and stops before answering. A restarted run
//! > reads the context naming g and finds I1 last, with m2 whole and found by
//! > its id, and no response after it. On thread V, G4 is left open and nothing
//! > arrives for longer than the window: Threads fails it and closes its turn
//! > interrupted. The restarted run's fail naming interrupted is then refused
//! > MESSAGE_NOT_STREAMING and changes nothing.
//!
//! The oracle of each agent context is the library itself, as in T11: the
//! turns as members read them (`content::turn_of`, the complete entries only),
//! U1's carrying the time the context shows, checked as an instant against
//! U1's `createdAt` (`content::carries_its_created_at`, 0.13), projected for g
//! by `Perspective::of(g)`; an own turn still `STREAMING` comes with its
//! complete entries, so no pending response is passed for it.
//! m1 is a mail of another agent, h, received inside g's turn; m2 is the mail
//! g itself stored, alone in an input turn.
//!
//! "Short" is one window for the whole scenario, long enough for the restarted
//! run to read and fail G1 before it ends, short enough to wait out on V.
//!
//! Channels: the answers (4); the messages as a member reads them (2); the
//! runner's agent context loaded by the library; the fact log and the store.

mod common;

use std::time::Duration;

use common::content::{
    Streamed, after, ai_turn, annotation_chunk, carries_its_created_at, closed, closure_kind,
    conversation_id, draft_entry, draft_text, entries_of, entry_chunk, input_turn, kinds_of,
    mail_as, observed_as, result_of, results, said, text, to_json, turn_of, warning,
};
use common::facts;
use common::flows::{
    ai, await_message, command, completion, context, create_thread, message_confirmed, message_ids,
    message_refused, message_view, open_stream, projected_of, say, store, whole_message,
};
use common::{PUSH_TIMEOUT, TestContext, Windows, eventually, store as stored, stream};
use contract_threads::llm::{
    AITurn, BlockId, ClosureReason, ContentBlock, ModelResponse, Perspective, ResponseEnding,
    ResponseMetadata, TextBlock, Turn, TurnEntry,
};
use contract_threads::{FailMessageV2, LibraryJson, MessageStatus, ReasonCode};
use uuid::Uuid;

/// The stream window Threads runs with in this scenario.
const WINDOW: Duration = Duration::from_secs(8);

/// `fail.v2` of `message` with no seal, naming `closure`.
fn fail_naming(thread: Uuid, message: Uuid, closure: &ClosureReason) -> FailMessageV2 {
    FailMessageV2 {
        message_id: message,
        thread_id: thread,
        seal: None,
        closure: Some(LibraryJson::from_json(to_json(closure))),
        acting_user_id: None,
    }
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t31_a_restarted_runner_closes_its_left_open_turn_finds_its_stored_mail_and_retries() {
    // Given Threads with a short stream window; T with member A and U1; agent
    // g, agent h who writes to it, and g's runner.
    let ctx = TestContext::setup_with(Windows {
        stream_idle: Some(WINDOW),
        ..Windows::default()
    })
    .await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let (g, h) = (ai("g", "Analyst"), ai("h", "Critic"));
    let runner = ctx.service_account(true);
    let nats = ctx.nats.url();
    let u1 = say(&ctx, t, &a, "Review this").await;
    let u1_view = message_view(&ctx, &a.passport, t, u1).await;

    // When g's runner opens G1 STREAMING and pushes a response calling c1,
    // c1's result, h's mail m1 under the mail's id, then part of a text
    // response — and then nothing more.
    let g1 = open_stream(&ctx, t, g.clone()).await;
    let mut calling = Streamed::start(g1, None);
    let c1 = calling.call("search", &["{\"q\":\"q3\"}"]);
    calling.complete(ResponseEnding::tool_use());
    let c1_result = results(vec![result_of(&calling.runtime_call(c1), "q3 rows", false)]);
    let m1 = Uuid::now_v7();
    let m1_mail = mail_as(m1, &h, Uuid::now_v7(), vec![text("Analyst, which rows?")]);
    const HALF: &str = "half of a";
    let mut half = Streamed::start(g1, None);
    half.text(&[HALF], false);
    let mut chunks = calling.chunks();
    chunks.push(entry_chunk(&c1_result));
    chunks.push(entry_chunk(&m1_mail));
    chunks.extend(half.chunks());
    stream::push(&nats, g1, 0, &chunks).await;
    await_message(&ctx, &a.passport, t, g1, "G1's half text folded", |view| {
        entries_of(view).last().is_some_and(|entry| {
            entry["state"] == "PRODUCING" && draft_text(&draft_entry(entry)) == HALF
        })
    })
    .await;
    let own_entries = vec![
        TurnEntry::from(calling.response()),
        c1_result.clone(),
        m1_mail.clone(),
    ];

    // When, before the window ends, a restarted run of g reads T's context
    // naming g.
    let read = context(&ctx, &runner.passport, t, Some(&g)).await;

    // Then G1 is there as g's own turn, open, with its response, c1's result
    // and m1 — the library's agent context for g, value for value; U1's turn
    // carries the time Threads stored it, its createdAt (0.13), checked as an
    // instant and expected of the library as the context wrote it.
    let found = projected_of(&read);
    let u1_turn = observed_as(
        turn_of(&u1_view),
        carries_its_created_at(&read, &u1_view, "U1's turn"),
    );
    let g1_open = Turn::from(
        AITurn::from_parts(common::content::turn_id(g1), g.id, g.author(), own_entries)
            .expect("an open AI turn the library accepts"),
    );
    let expected = Perspective::of(g.id)
        .keeping(|_| false)
        .project_pending(conversation_id(t), &[u1_turn.clone(), g1_open], &[])
        .expect("the library projects T for g");
    assert_eq!(found, expected, "the agent context is the library's, for g");
    let own = found.turns()[1].as_ai().expect("G1, g's own AI turn");
    assert_eq!(own.id().as_uuid(), g1);
    assert!(!own.is_closed(), "G1 is still open: {own:?}");
    // And m1 is found by its id, whole.
    let by_id = own
        .entries()
        .iter()
        .find(|entry| entry.id().as_uuid() == m1)
        .expect("m1, found by the mail's id");
    assert_eq!(to_json(by_id), to_json(&m1_mail));

    // When the run fails G1 with no seal, naming interrupted.
    let interrupted = ClosureReason::interrupted()
        .with_detail("the runner died")
        .expect("a detail");
    let sent = command(&ctx, &fail_naming(t, g1, &interrupted)).await;

    // Then (4) confirmed FAILED.
    message_confirmed(
        &ctx,
        sent,
        MessageStatus::Failed,
        "failing the left-open G1",
    )
    .await;
    // And (2) G1's turn is closed interrupted, its half text kept as the
    // library's interrupted response, which members read.
    let g1_view = message_view(&ctx, &a.passport, t, g1).await;
    assert_eq!(g1_view["status"], "FAILED");
    assert_eq!(
        kinds_of(&g1_view),
        vec![
            "model_response",
            "runtime_tool_results",
            "agent_mail",
            "model_response",
            "turn_closure"
        ],
        "{g1_view}"
    );
    let kept = &entries_of(&g1_view)[3];
    assert_eq!(kept["state"], "INTERRUPTED", "{g1_view}");
    assert_eq!(draft_text(&draft_entry(kept)), HALF);
    assert_eq!(closure_kind(&g1_view).as_deref(), Some("interrupted"));
    // And the Failed fact names interrupted, never the detail.
    let g1_facts = facts::of(&ctx.pool, "message", g1).await;
    assert_eq!(
        facts::the(&g1_facts, "Failed").payload["closure"],
        "interrupted"
    );
    facts::assert_no_fact_carries(&ctx.pool, "the runner died", "a closure's detail").await;

    // When the run stores G2, a retry of G1, under new message, entry and
    // block ids, streams it with a WARNING annotation and completes it.
    let g2 = open_stream(&ctx, t, g.clone()).await;
    let mut retry = Streamed::start(g2, None);
    retry.text(&["the answer, again"], true);
    retry.complete(ResponseEnding::finished());
    let note = warning("RETRIED", "an earlier attempt was interrupted");
    let note_id = note.id;
    let mut chunks = retry.chunks();
    chunks.push(annotation_chunk(retry.entry, note));
    let (last_seq, hash) = stream::push(&nats, g2, 0, &chunks).await;
    let sent = command(&ctx, &completion(t, g2, last_seq, &hash)).await;

    // Then (4) confirmed COMPLETED.
    message_confirmed(
        &ctx,
        sent,
        MessageStatus::Completed,
        "completing the retry G2",
    )
    .await;
    // And (2) G1 stays readable before G2.
    assert_eq!(message_ids(&ctx, &a.passport, t).await, vec![u1, g1, g2]);
    // And the context naming g holds U1, G1 closed interrupted without its
    // half text, and G2 — the library's projection of the three turns as
    // members read them, the interrupted response not in them.
    let g1_closed = turn_of(&message_view(&ctx, &a.passport, t, g1).await);
    let g2_turn = turn_of(&message_view(&ctx, &a.passport, t, g2).await);
    let read = context(&ctx, &runner.passport, t, Some(&g)).await;
    let found = projected_of(&read);
    let expected = Perspective::of(g.id)
        .keeping(|_| false)
        .project_pending(
            conversation_id(t),
            &[u1_turn.clone(), g1_closed, g2_turn],
            &[],
        )
        .expect("the library projects T for g");
    assert_eq!(found, expected, "the agent context is the library's, for g");
    let ids: Vec<Uuid> = found
        .turns()
        .iter()
        .map(|turn| turn.id().as_uuid())
        .collect();
    assert_eq!(ids, vec![u1, g1, g2]);
    let g1_read = found.turns()[1].as_ai().expect("G1, g's own turn");
    assert!(g1_read.is_closed());
    let raw = read["projectedThread"].to_string();
    assert!(
        !raw.contains(HALF),
        "a runner never receives the half text: {raw}"
    );
    assert!(
        !raw.contains("RETRIED"),
        "no annotation in a conversation: {raw}"
    );

    // When a retry G3 stored whole reuses — each alone — an entry id of G1, a
    // block id of G1, or an annotation id of G2.
    let reuse_entry = {
        let g3 = Uuid::now_v7();
        let response = ModelResponse::new(
            calling.entry,
            vec![said("again")],
            ResponseEnding::finished(),
            ResponseMetadata::new(),
        )
        .expect("a model response on its own");
        (
            g3,
            Turn::from(ai_turn(g3, &g, closed(vec![TurnEntry::from(response)]))),
            Vec::new(),
            "an entry id of G1",
        )
    };
    let reuse_block = {
        let g3 = Uuid::now_v7();
        let block = ContentBlock::from(
            TextBlock::new(BlockId::from_uuid(c1.as_uuid()).expect("a UUIDv7"), "again")
                .expect("a text"),
        );
        let response = common::content::response(vec![block], ResponseEnding::finished(), None);
        (
            g3,
            Turn::from(ai_turn(g3, &g, closed(vec![response]))),
            Vec::new(),
            "a block id of G1",
        )
    };
    let reuse_annotation = {
        let g3 = Uuid::now_v7();
        let answered = common::content::reply("again, annotated");
        let mut reused = warning("RETRIED", "once more");
        reused.id = note_id;
        (
            g3,
            Turn::from(ai_turn(g3, &g, closed(answered.clone()))),
            vec![after(&answered[0], reused)],
            "an annotation id of G2",
        )
    };
    for (g3, turn, annotations, what) in [reuse_entry, reuse_block, reuse_annotation] {
        let sent = command(&ctx, &whole_message(t, g3, &g, &turn, annotations)).await;
        // Then each is refused KEY_REUSED, and nothing is stored.
        message_refused(&ctx, sent, ReasonCode::KeyReused, what).await;
        assert!(
            facts::of(&ctx.pool, "message", g3).await.is_empty(),
            "{what}"
        );
        assert_eq!(
            message_ids(&ctx, &a.passport, t).await,
            vec![u1, g1, g2],
            "{what}"
        );
    }

    // When a streamed G3 reuses one: m1's id, held by G1, as the id of an
    // entry of its stream.
    let g3 = open_stream(&ctx, t, g.clone()).await;
    let reusing = mail_as(m1, &h, Uuid::now_v7(), vec![text("the same mail, again")]);
    let mut answer = Streamed::start(g3, None);
    answer.text(&["an answer"], true);
    answer.complete(ResponseEnding::finished());
    let mut chunks = vec![entry_chunk(&reusing)];
    chunks.extend(answer.chunks());
    let (last_seq, hash) = stream::push(&nats, g3, 0, &chunks).await;
    let sent = command(&ctx, &completion(t, g3, last_seq, &hash)).await;
    // Then G3 is stored FAILED, with the code reused_identity.
    message_confirmed(
        &ctx,
        sent,
        MessageStatus::Failed,
        "completing the streamed G3",
    )
    .await;
    let g3_view = message_view(&ctx, &a.passport, t, g3).await;
    assert_eq!(g3_view["status"], "FAILED", "{g3_view}");
    let g3_facts = facts::of(&ctx.pool, "message", g3).await;
    assert_eq!(
        facts::the(&g3_facts, "Failed").payload["reason"],
        "reused_identity"
    );

    // When the run stores I1, an input turn holding g's mail m2, and stops
    // before answering.
    let (i1, m2) = (Uuid::now_v7(), Uuid::now_v7());
    let m2_mail = mail_as(m2, &g, t, vec![text("Critic, a second question")]);
    let i1_turn = Turn::from(input_turn(i1, vec![m2_mail.clone()]));
    store(
        &ctx,
        whole_message(t, i1, &g, &i1_turn, Vec::new()),
        MessageStatus::Completed,
    )
    .await;

    // Then a restarted run reading the context naming g finds I1 last, with
    // m2 whole and found by its id, and no response after it.
    let found = projected_of(&context(&ctx, &runner.passport, t, Some(&g)).await);
    let last = found.turns().last().expect("a turn in g's context");
    assert_eq!(last.id().as_uuid(), i1, "I1 is last: no response after it");
    let input = last.as_input().expect("I1, an input turn");
    assert_eq!(input.entries().len(), 1, "{input:?}");
    assert_eq!(to_json(&input.entries()[0]), to_json(&m2_mail), "m2 whole");
    assert_eq!(
        to_json(&input.entries()[0])["id"],
        serde_json::json!(m2),
        "m2, found by the mail's id"
    );

    // Given thread V, where g's G4 is left open after part of a text.
    let v = create_thread(&ctx, Some(&a), &[]).await;
    say(&ctx, v, &a, "Review that").await;
    let g4 = open_stream(&ctx, v, g.clone()).await;
    let mut left = Streamed::start(g4, None);
    left.text(&["half"], false);
    stream::push(&nats, g4, 0, &left.chunks()).await;

    // When nothing arrives for longer than the window.
    // Then Threads fails G4 — within the window and the time its watch takes
    // to see it — and closes its turn interrupted.
    eventually("G4 failed by silence", WINDOW + PUSH_TIMEOUT, || async {
        message_view(&ctx, &a.passport, v, g4).await["status"] == "FAILED"
    })
    .await;
    let g4_view = message_view(&ctx, &a.passport, v, g4).await;
    assert_eq!(
        closure_kind(&g4_view).as_deref(),
        Some("interrupted"),
        "{g4_view}"
    );

    // When the restarted run fails G4, naming interrupted.
    let facts_before = facts::count(&ctx.pool).await;
    let snapshot = stored::snapshot(&ctx.pool).await;
    let sent = command(&ctx, &fail_naming(v, g4, &ClosureReason::interrupted())).await;
    // Then refused MESSAGE_NOT_STREAMING, and nothing changes.
    message_refused(
        &ctx,
        sent,
        ReasonCode::MessageNotStreaming,
        "failing a turn silence already closed",
    )
    .await;
    assert_eq!(message_view(&ctx, &a.passport, v, g4).await, g4_view);
    assert_eq!(facts::count(&ctx.pool).await, facts_before);
    assert_eq!(stored::snapshot(&ctx.pool).await, snapshot);

    ctx.teardown().await;
}
