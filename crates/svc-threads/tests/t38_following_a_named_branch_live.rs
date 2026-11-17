//! **T38 — Following live the branch a named message ends.**
//!
//! > Two agents answer A's question M1 at the same time: the agents service
//! > opens M2 (agent a1) then M3 (agent a2) STREAMING, both answering M1. M3
//! > is the thread's most recent message, so the active branch is M1, M3 and
//! > M2 is on another branch. A member following T sees M1 and M3. A member
//! > following T naming M2 as the branch tip opens on M1 and M2, STREAMING,
//! > without M3; they see M2's draft grow as its runner writes it, then M2
//! > COMPLETED, and never M3. The member following the active branch sees M3
//! > grow, and never M2. Naming a message no thread holds, or a message of
//! > another thread, the member opens on the thread and no message, as
//! > `threadsMessages` reads such a tip.
//!
//! Channels: the answers (4); the live feeds of a member — the active branch
//! over SSE, the named branch over WebSocket (3).

mod common;

use common::content::{Streamed, draft_entry, draft_text, entries_of};
use common::feed::{Live, THREAD_FEED, in_reset, thread_feed_on, upsert_of, views};
use common::flows::{
    ai, command, completion, create_thread, message_confirmed, say, store, streaming_message,
};
use common::{TestContext, stream};
use contract_threads::llm::ResponseEnding;
use contract_threads::{MessageStatus, seal_hash};
use serde_json::{Value, json};
use uuid::Uuid;

/// The draft text of `entry` in a feed's view of a message, while produced.
fn producing_text(view: &Value, entry: Uuid) -> Option<String> {
    entries_of(view)
        .into_iter()
        .find(|read| read["id"] == json!(entry) && read["state"] == "PRODUCING")
        .map(|read| draft_text(&draft_entry(&read)))
}

/// The messages a `Reset` holds.
fn messages_in(reset: &Value) -> Vec<Value> {
    views(reset)
        .into_iter()
        .filter(|view| view["__typename"] == "MessageView")
        .collect()
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t38_a_member_follows_live_the_branch_a_named_message_ends() {
    // Given T, member A, A's question M1, and another thread U with a message.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let m1 = say(&ctx, t, None, &a, "What do you both think?").await;
    let u = create_thread(&ctx, Some(&a), &[]).await;
    let elsewhere = say(&ctx, u, None, &a, "Another conversation").await;
    // And two agents answering M1 at the same time: M2, then M3, STREAMING.
    let (analyst, critic) = (ai("a1", "Analyst"), ai("a2", "Critic"));
    let m2 = store(
        &ctx,
        streaming_message(t, Some(m1), &analyst),
        MessageStatus::Streaming,
    )
    .await;
    let m3 = store(
        &ctx,
        streaming_message(t, Some(m1), &critic),
        MessageStatus::Streaming,
    )
    .await;

    // When A follows T, and follows T naming M2 as the branch tip.
    let mut active = Live::thread(&ctx, &a.passport, t).await;
    let mut named = Live::ws(
        &ctx,
        &a.passport,
        THREAD_FEED,
        &thread_feed_on(t, 20, Some(m2)),
    )
    .await;

    // Then (3) the active branch is M1, M3: M2 is not in it.
    assert!(in_reset(&active.reset, m1).is_some(), "{}", active.reset);
    assert!(in_reset(&active.reset, m3).is_some(), "{}", active.reset);
    assert!(in_reset(&active.reset, m2).is_none(), "{}", active.reset);
    // And the named branch opens on the thread, M1 and M2 STREAMING, not M3.
    assert!(in_reset(&named.reset, t).is_some(), "{}", named.reset);
    assert!(in_reset(&named.reset, m1).is_some(), "{}", named.reset);
    let opened = in_reset(&named.reset, m2).expect("M2 is in the named branch's Reset");
    assert_eq!(opened["status"], "STREAMING");
    assert!(in_reset(&named.reset, m3).is_none(), "{}", named.reset);

    // When M2's runner writes the start of its answer.
    let mut r2 = Streamed::start(m2, Some("m-1"));
    r2.text(&["Hel", "lo"], true);
    r2.complete(ResponseEnding::finished());
    let chunks = r2.chunks();
    let nats = ctx.nats.url();
    stream::push(&nats, m2, 0, &chunks[0..3]).await;
    // Then (3) the member following the named branch sees M2's draft hold it.
    let entry = r2.entry.as_uuid();
    named
        .until("M2's draft holds `Hel` on the named branch", |delta| {
            upsert_of(delta, m2)
                .and_then(|view| producing_text(view, entry))
                .as_deref()
                == Some("Hel")
        })
        .await;

    // When the rest arrives and the writer completes M2.
    stream::push(&nats, m2, 3, &chunks[3..]).await;
    let n = (chunks.len() - 1) as u64;
    let hash = seal_hash(&chunks).expect("the chunks hash");
    let completed = command(&ctx, &completion(t, m2, n, &hash)).await;
    message_confirmed(&ctx, completed, MessageStatus::Completed, "completing M2").await;
    // Then (3) the named branch shows M2 COMPLETED.
    named
        .until("M2 is COMPLETED on the named branch", |delta| {
            upsert_of(delta, m2).is_some_and(|view| view["status"] == "COMPLETED")
        })
        .await;

    // When M3's runner writes its answer.
    let mut r3 = Streamed::start(m3, Some("m-1"));
    r3.text(&["Well"], true);
    r3.complete(ResponseEnding::finished());
    let chunks = r3.chunks();
    stream::push(&nats, m3, 0, &chunks[0..3]).await;
    // Then (3) the member following the active branch sees M3's draft hold it.
    let entry = r3.entry.as_uuid();
    let seen = active
        .collect_until("M3's draft holds `Well` on the active branch", |seen| {
            seen.last()
                .and_then(|delta| upsert_of(delta, m3))
                .and_then(|view| producing_text(view, entry))
                .as_deref()
                == Some("Well")
        })
        .await;
    // And the active branch never told of M2, nor the named branch of M3.
    assert!(
        seen.iter().all(|delta| upsert_of(delta, m2).is_none()),
        "{seen:?}"
    );
    let rest = named.rest().await;
    assert!(
        rest.iter().all(|delta| upsert_of(delta, m3).is_none()),
        "{rest:?}"
    );

    // When A follows T naming a message no thread holds, then a message of U.
    for (tip, what) in [
        (Uuid::now_v7(), "a message no thread holds"),
        (elsewhere, "a message of another thread"),
    ] {
        let feed = Live::ws(
            &ctx,
            &a.passport,
            THREAD_FEED,
            &thread_feed_on(t, 20, Some(tip)),
        )
        .await;
        // Then (3) the feed opens on the thread and no message.
        assert!(in_reset(&feed.reset, t).is_some(), "{what}: {}", feed.reset);
        assert_eq!(messages_in(&feed.reset), Vec::<Value>::new(), "{what}");
    }

    // When A follows T naming M4 before it is stored, and a third agent then
    // opens M4 answering M1.
    let opening = streaming_message(t, Some(m1), &ai("a3", "Reviewer"));
    let m4 = opening.message_id;
    let mut early = Live::ws(
        &ctx,
        &a.passport,
        THREAD_FEED,
        &thread_feed_on(t, 20, Some(m4)),
    )
    .await;
    assert_eq!(messages_in(&early.reset), Vec::<Value>::new());
    store(&ctx, opening, MessageStatus::Streaming).await;
    // Then (3) M4 and the question it answers enter the feed opened before it.
    let entered = early
        .collect_until("M4's branch enters the feed opened before it", |seen| {
            [m1, m4]
                .iter()
                .all(|id| seen.iter().any(|delta| upsert_of(delta, *id).is_some()))
        })
        .await;
    assert!(
        entered.iter().all(|delta| upsert_of(delta, m3).is_none()),
        "{entered:?}"
    );
}
