//! **T26 — Messages stored before the upgrade are converted.**
//!
//! > The harness runs the previous version of Threads and stores through its
//! > commands: thread T holding a user message, an AI message COMPLETED with a
//! > step carrying a thinking with its signature, a text and a tool call, a tool
//! > results item and an ARTIFACT block, an AI message FAILED with an
//! > interrupted tool result, a deleted user message, an edited one, a fork and
//! > a STREAMING message; and a resolved thread T2. It then starts the new
//! > version on the same database: it becomes ready; every message keeps its
//! > id, its parent, its status, its version and edited_at, every block its id;
//! > the AI messages read as turns of the library with their entries and
//! > closures, the thinking's signature as replay material byte for byte, the
//! > ARTIFACT block as an annotation after its entry, the interrupted tool
//! > result as a result; the STREAMING message reads FAILED with its turn
//! > closed interrupted; a runner's context of T loads in the library; T2 is
//! > still resolved; members, resources, read marks and artifacts are
//! > unchanged; no fact is added and no version is raised. Starting the new
//! > version again changes nothing. With a stored message that cannot be
//! > converted (an AI author id that is not a UUIDv7), the new version does not
//! > become ready and names the message, and no message is converted half-way.
//!
//! The previous version is the release `svc-threads` 0.4.0 itself, built from
//! its tag (`common::previous`), commanded with `contract-threads` 0.4.0: the
//! suite writes no row. "Keeps its status" holds for every message but the
//! STREAMING one, which the gesture itself names FAILED; its version is kept
//! all the same, as every message's.
//!
//! "No message is converted half-way" is read on the store, table by table:
//! every row the previous version stored keeps, column for column, what it
//! held, and no table the new version adds holds a row of these messages.
//!
//! Channels: the answers (4); the messages, threads, read marks and artifacts
//! as members read them, before and after (2); the fact log and the store; a
//! runner's context; the readiness of the upgraded boot and what it says.

mod common;

use std::collections::BTreeSet;

use common::artifacts::{File, download_url, fetch, upload};
use common::content::{closure_kind, complete_entry, entries_of, kinds_of, turn_of};
use common::facts;
use common::flows::{
    command, context, conversation_of, create, creation, message_confirmed, message_view,
    thread_confirmed, thread_view, turn_ids, uuid_of,
};
use common::previous::{
    self, ai_message, ai_text, artifact_block, author, block_ids_0_4, calling_step, command_v1,
    edit_of, message_view_0_4, step, streaming_message, thinking, tool_call, tool_result,
    tool_results, user_message, user_text,
};
use common::{M_SET_READ_MARK, Person, TestContext, store};
use contract_threads::llm::{ContentBlock, ReplayPayload, TurnEntry};
use contract_threads::{FailMessage, MessageStatus};
use contract_threads_v0_4::llm::StopReason;
use serde_json::{Value, json};
use uuid::Uuid;

/// Every id a 0.5 message view holds: its entries', their blocks' (children
/// included) and its annotations'.
fn ids_held(view: &Value) -> BTreeSet<Uuid> {
    fn walk(value: &Value, ids: &mut BTreeSet<Uuid>) {
        match value {
            Value::Object(map) => {
                for (key, inner) in map {
                    if key == "id"
                        && let Some(id) = inner.as_str().and_then(|s| Uuid::parse_str(s).ok())
                    {
                        ids.insert(id);
                    }
                    walk(inner, ids);
                }
            }
            Value::Array(items) => items.iter().for_each(|item| walk(item, ids)),
            _ => {}
        }
    }
    let mut ids = BTreeSet::new();
    walk(&view["entries"], &mut ids);
    ids
}

/// A stored v1 command, confirmed with `status`.
async fn stored_v1(
    ctx: &TestContext,
    store: &contract_threads_v0_4::StoreMessage,
    status: MessageStatus,
    what: &str,
) -> Uuid {
    let sent = command_v1(ctx, store).await;
    message_confirmed(ctx, sent, status, what).await;
    store.message_id
}

async fn set_read_mark(ctx: &TestContext, person: &Person, thread: Uuid, message: Uuid) {
    let answer = ctx
        .run(
            &person.passport,
            M_SET_READ_MARK,
            json!({ "thread": thread, "message": message }),
        )
        .await;
    assert_eq!(
        common::gql::data(&answer, "threadsSetReadMark"),
        &json!({ "success": true })
    );
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH, and builds the previous release"]
async fn t26_messages_stored_by_the_previous_version_are_converted_once_keeping_their_ids() {
    // Given the previous version running, and through its commands: thread T
    // (owner O, members A and B, resources {kms: [k1]}) and thread T2.
    let mut ctx = TestContext::setup_previous().await;
    let (o, a, b) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
    );
    let mut born = creation(Some(o.id), &[a.id, b.id]);
    born.title = Some("before the upgrade".to_string());
    born.resources =
        std::collections::BTreeMap::from([("kms".to_string(), vec!["k1".to_string()])]);
    let t = create(&ctx, born).await;
    let mut born = creation(Some(o.id), &[a.id]);
    born.title = Some("resolved before the upgrade".to_string());
    let t2 = create(&ctx, born).await;
    let runner = ctx.service_account(true);
    let file = File::csv("quarter,revenue\nQ3,42\n");
    let x = upload(&ctx, &runner.passport, t, &file).await;
    let agent = Uuid::now_v7();
    let analyst = || author(&agent.to_string(), "Analyst");
    const THOUGHT: &str = "  weighing it\n\tcarefully ";
    const SIGNATURE: &str = "c2lnbmF0dXJlPT0=";

    // M1, A's user message;
    let m1 = stored_v1(
        &ctx,
        &user_message(t, None, a.id, vec![user_text("what were the Q3 figures?")]),
        MessageStatus::Completed,
        "M1",
    )
    .await;
    // M2, an AI message COMPLETED: a step with a thinking and its signature, a
    // text and a tool call; a tool results item; a step with a text and an
    // ARTIFACT block;
    let (thought, said, called) = (
        thinking(THOUGHT, SIGNATURE),
        ai_text("let me look"),
        tool_call("c1", "search", json!({ "q": "q3" })),
    );
    let (thought_id, called_id) = (thought.id, called.id);
    let answered = tool_result("c1", "search", "42");
    let answered_id = answered.id;
    let attaching = artifact_block(x);
    let attaching_id = attaching.id;
    let m2 = stored_v1(
        &ctx,
        &ai_message(
            t,
            Some(m1),
            analyst(),
            vec![
                step(
                    StopReason::AwaitingToolResults,
                    Some("m-1"),
                    vec![thought, said, called],
                ),
                tool_results(vec![answered]),
                step(
                    StopReason::EndTurn,
                    None,
                    vec![ai_text("Q3 was 42."), attaching],
                ),
            ],
        ),
        MessageStatus::Completed,
        "M2",
    )
    .await;
    // M3, an AI message FAILED with an interrupted tool result;
    let open = streaming_message(t, Some(m2), analyst());
    let m3 = stored_v1(&ctx, &open, MessageStatus::Streaming, "M3 opened").await;
    previous::push(&ctx.nats.url(), m3, &calling_step(0, "c9", "search")).await;
    previous::await_message_0_4(&ctx, &o.passport, t, m3, "M3's step folded", |view| {
        view["items"][0]["status"] == "COMPLETED"
    })
    .await;
    let failed = command(
        &ctx,
        &FailMessage {
            message_id: m3,
            thread_id: t,
            seal: None,
            acting_user_id: None,
        },
    )
    .await;
    message_confirmed(&ctx, failed, MessageStatus::Failed, "M3 failed").await;
    let m3_call = {
        let view = message_view_0_4(&ctx, &o.passport, t, m3).await;
        assert_eq!(
            view["items"][1]["blocks"][0]["content"]["is_error"], true,
            "the previous version answered c9 with an interrupted result: {view}"
        );
        uuid_of(&view["items"][0]["blocks"][0]["id"])
    };
    // M4, A's user message, deleted;
    let m4 = stored_v1(
        &ctx,
        &user_message(t, Some(m3), a.id, vec![user_text("scratch that")]),
        MessageStatus::Completed,
        "M4",
    )
    .await;
    let deleted = command_v1(
        &ctx,
        &contract_threads_v0_4::DeleteMessage {
            message_id: m4,
            thread_id: t,
            acting_user_id: a.id,
        },
    )
    .await;
    message_confirmed(&ctx, deleted, MessageStatus::Completed, "M4 deleted").await;
    // M5, B's user message, edited;
    let m5 = stored_v1(
        &ctx,
        &user_message(t, Some(m4), b.id, vec![user_text("first wording")]),
        MessageStatus::Completed,
        "M5",
    )
    .await;
    let edited = command_v1(
        &ctx,
        &edit_of(t, m5, b.id, 1, vec![user_text("second wording")]),
    )
    .await;
    message_confirmed(&ctx, edited, MessageStatus::Completed, "M5 edited").await;
    // M6, a fork: another answer to M1;
    let m6 = stored_v1(
        &ctx,
        &ai_message(
            t,
            Some(m1),
            analyst(),
            vec![step(
                StopReason::EndTurn,
                None,
                vec![ai_text("another take")],
            )],
        ),
        MessageStatus::Completed,
        "M6",
    )
    .await;
    // M7, a STREAMING message;
    let m7 = stored_v1(
        &ctx,
        &streaming_message(t, Some(m5), analyst()),
        MessageStatus::Streaming,
        "M7 opened",
    )
    .await;
    // A's read mark on T at M2; T2 holding A's message, resolved by O.
    set_read_mark(&ctx, &a, t, m2).await;
    stored_v1(
        &ctx,
        &user_message(t2, None, a.id, vec![user_text("settled")]),
        MessageStatus::Completed,
        "T2's message",
    )
    .await;
    let resolved = command(
        &ctx,
        &contract_threads::ResolveThread {
            thread_id: t2,
            acting_user_id: o.id,
        },
    )
    .await;
    thread_confirmed(&ctx, resolved, "resolving T2").await;

    // And what the previous version serves, as its members read it.
    let messages = [m1, m2, m3, m4, m5, m6, m7];
    let mut before = Vec::new();
    for message in messages {
        before.push(message_view_0_4(&ctx, &o.passport, t, message).await);
    }
    let threads_before = (
        thread_view(&ctx, &a.passport, t).await,
        thread_view(&ctx, &a.passport, t2).await,
    );
    let facts_before = facts::count(&ctx.pool).await;

    // When the new version starts on the same database.
    let upgrade = ctx.upgrade().await;

    // Then it becomes ready.
    assert!(
        upgrade.migrated && upgrade.ready,
        "the new version becomes ready on what the previous one stored:\n{}",
        upgrade.output
    );

    // And every message keeps its id, its parent, its version, its edited_at
    // and its deletion, every block its id, every status but the STREAMING
    // message's.
    let mut after = Vec::new();
    for (message, old) in messages.iter().zip(&before) {
        let view = message_view(&ctx, &o.passport, t, *message).await;
        for field in [
            "id",
            "threadId",
            "parentMessageId",
            "version",
            "editedAt",
            "deleted",
            "createdAt",
            "jobId",
        ] {
            assert_eq!(view[field], old[field], "{field} of {message}: {view}");
        }
        if *message != m7 {
            assert_eq!(view["status"], old["status"], "the status of {message}");
        }
        let held = ids_held(&view);
        for block in block_ids_0_4(old) {
            assert!(held.contains(&block), "block {block} keeps its id: {view}");
        }
        after.push(view);
    }
    let [v1, v2, v3, v4, v5, v6, v7] = after.clone().try_into().expect("seven views");

    // And the AI messages read as turns of the library, with their entries and
    // closures.
    assert_eq!(v1["turnKind"], "INPUT_TURN");
    assert_eq!(kinds_of(&v1), vec!["human_message"]);
    turn_of(&v1);
    assert_eq!(v2["turnKind"], "AI_TURN");
    assert_eq!(
        v2["author"]["aiId"],
        json!(agent),
        "the v1 author id is the agent id"
    );
    assert_eq!(
        kinds_of(&v2),
        vec![
            "model_response",
            "runtime_tool_results",
            "model_response",
            "turn_closure"
        ],
        "{v2}"
    );
    assert_eq!(closure_kind(&v2).as_deref(), Some("completed"));
    turn_of(&v2);
    // The thinking's signature as replay material, byte for byte.
    let TurnEntry::ModelResponse(first) = complete_entry(&entries_of(&v2)[0]) else {
        panic!("M2 opens on a model response: {v2}");
    };
    assert_eq!(first.model().map(|model| model.as_str()), Some("m-1"));
    let ContentBlock::Thinking(kept) = &first.blocks()[0] else {
        panic!("M2's first block is its thinking: {v2}");
    };
    assert_eq!(kept.id().as_uuid(), thought_id);
    assert_eq!(kept.text(), THOUGHT);
    let replay = kept
        .replay()
        .expect("the signature is kept as replay material");
    assert!(
        matches!(replay.payload(), ReplayPayload::Text { value, .. } if value == SIGNATURE),
        "the signature, byte for byte"
    );
    // The tool result answers the call by the call's block id.
    let TurnEntry::RuntimeToolResults(results) = complete_entry(&entries_of(&v2)[1]) else {
        panic!("M2's tool results: {v2}");
    };
    assert_eq!(results.results()[0].id().as_uuid(), answered_id);
    assert_eq!(results.results()[0].call().as_uuid(), called_id);
    // The ARTIFACT block, an annotation after its entry, under its id.
    assert_eq!(
        entries_of(&v2)[2]["annotations"],
        json!([{ "id": attaching_id, "index": 0, "kind": "ARTIFACT", "content": { "artifact_id": x } }]),
        "{v2}"
    );
    assert_eq!(entries_of(&v2)[0]["annotations"], json!([]));
    // M3: the interrupted tool result as a result, the turn closed failed.
    assert_eq!(
        kinds_of(&v3),
        vec!["model_response", "runtime_tool_results", "turn_closure"],
        "{v3}"
    );
    assert_eq!(closure_kind(&v3).as_deref(), Some("failed"));
    let TurnEntry::RuntimeToolResults(interrupted) = complete_entry(&entries_of(&v3)[1]) else {
        panic!("M3's tool results: {v3}");
    };
    assert_eq!(interrupted.results()[0].call().as_uuid(), m3_call);
    assert!(interrupted.results()[0].is_error());
    turn_of(&v3);
    // M4, deleted: no content. M5, edited: its new content. M6, the fork.
    assert_eq!(v4["entries"], json!([]));
    assert_eq!(
        common::content::texts_of(&v5),
        vec!["second wording".to_string()]
    );
    assert_eq!(kinds_of(&v6), vec!["model_response", "turn_closure"]);
    turn_of(&v6);
    // M7, STREAMING before: FAILED, its turn closed interrupted.
    assert_eq!(v7["status"], "FAILED", "{v7}");
    assert_eq!(kinds_of(&v7), vec!["turn_closure"], "{v7}");
    assert_eq!(closure_kind(&v7).as_deref(), Some("interrupted"));

    // And a runner's context of T loads in the library: the active branch,
    // ending at M7, without the deleted M4 and M7 failed before an entry.
    let conversation = conversation_of(&context(&ctx, &runner.passport, t, None).await);
    assert_eq!(turn_ids(&conversation), vec![m1, m2, m3, m5]);

    // And T2 is still resolved; members, resources, read marks and artifacts
    // are unchanged; no fact is added and no version is raised.
    let threads_after = (
        thread_view(&ctx, &a.passport, t).await,
        thread_view(&ctx, &a.passport, t2).await,
    );
    for (old, new) in [
        (&threads_before.0, &threads_after.0),
        (&threads_before.1, &threads_after.1),
    ] {
        for field in [
            "id",
            "ownerId",
            "title",
            "status",
            "resolvedBy",
            "resolvedAt",
            "createdAt",
            "version",
            "readMark",
            "members",
            "resources",
        ] {
            assert_eq!(new[field], old[field], "{field} of a thread: {new}");
        }
    }
    assert!(
        threads_after.1["resolvedBy"].is_string(),
        "T2 is still resolved"
    );
    let url = download_url(&ctx, &b.passport, t, x)
        .await
        .expect("the artifact is still attached");
    assert_eq!(fetch(&url).await, (200, file.bytes.clone()));
    assert_eq!(
        facts::count(&ctx.pool).await,
        facts_before,
        "no fact is added"
    );

    // When the new version starts again.
    let snapshot = store::snapshot(&ctx.pool).await;
    let again = ctx.restart_current().await;

    // Then it is ready, and nothing changes: not a row of the store, not a
    // message, not a fact.
    assert!(
        again.ready,
        "the new version starts again:\n{}",
        again.output
    );
    assert_eq!(
        store::snapshot(&ctx.pool).await,
        snapshot,
        "the second start converts nothing"
    );
    for (message, view) in messages.iter().zip(&after) {
        assert_eq!(&message_view(&ctx, &o.passport, t, *message).await, view);
    }
    assert_eq!(facts::count(&ctx.pool).await, facts_before);

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH, and builds the previous release"]
async fn t26_a_message_that_cannot_be_converted_keeps_the_new_version_from_becoming_ready() {
    // Given the previous version running, and through its commands: T holding
    // A's message M1 and an AI message M2 whose author id is not a UUIDv7.
    let mut ctx = TestContext::setup_previous().await;
    let a = ctx.enrol("ada").await;
    let mut born = creation(Some(a.id), &[]);
    born.title = Some("before the upgrade".to_string());
    let t = create(&ctx, born).await;
    let m1 = stored_v1(
        &ctx,
        &user_message(t, None, a.id, vec![user_text("convertible")]),
        MessageStatus::Completed,
        "M1",
    )
    .await;
    let m2 = stored_v1(
        &ctx,
        &ai_message(
            t,
            Some(m1),
            author("a1", "Analyst"),
            vec![step(
                StopReason::EndTurn,
                None,
                vec![ai_text("not convertible")],
            )],
        ),
        MessageStatus::Completed,
        "M2",
    )
    .await;
    let before = store::snapshot_json(&ctx.pool).await;
    let ids: Vec<String> = [m1, m2].iter().map(Uuid::to_string).collect();

    // When the new version starts on the same database.
    let upgrade = ctx.upgrade().await;

    // Then it does not become ready, and names the message.
    assert!(
        !upgrade.ready,
        "an unconvertible message keeps the new version from becoming ready"
    );
    assert!(
        upgrade.output.contains(&m2.to_string()),
        "the new version names the message it cannot convert:\n{}",
        upgrade.output
    );

    // And no message is converted half-way: every row the previous version
    // stored keeps what it held, and no table the new version adds holds a row
    // of these messages.
    let now = store::snapshot_json(&ctx.pool).await;
    assert_eq!(
        store::restricted_to(&before, &now),
        store::restricted_to(&before, &before),
        "the rows of the previous version are untouched"
    );
    for (table, rows) in now.iter().filter(|(table, _)| !before.contains_key(*table)) {
        for row in rows {
            let text = row.to_string();
            assert!(
                ids.iter().all(|id| !text.contains(id)),
                "{table} holds a row of a message not converted: {text}"
            );
        }
    }

    ctx.teardown().await;
}
