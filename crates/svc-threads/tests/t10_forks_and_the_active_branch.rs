//! **T10 — Forks and the active branch.**
//!
//! > On thread T: user message M1, AI answer M2, user message M3 answering M2.
//! > The harness regenerates M2 as AI message M2b with parent M1, then stores
//! > M3b answering M2b: a member's live view switches branch (M2b enters, M2
//! > and M3 leave it). A member reads T's active branch: M1, M2b, M3b, and a
//! > runner's conversation follows it. Reading the branch ending at M3 returns
//! > M1, M2, M3. A retry of M1 stored as M1b without parent, being the most
//! > recent, starts and ends the active branch; M1's branch stays readable. A
//! > branch of twelve messages read page by page, five at a time, each page
//! > starting before the oldest message of the previous one: the pages follow
//! > each other with no gap and no duplicate.
//!
//! A retry is a new message, a fork of M1: the in-place edit of a message is
//! T19's.
//!
//! Channels: the branch reads (2) — the active one and a named one, page by
//! page — and the runner's conversation, which follows the active branch.

mod common;

use std::collections::BTreeSet;

use common::content::reply as text_answer;
use common::feed::{Live, removes, upsert_of};
use common::flows::{
    active_branch, ai, answer, branch, branch_answer, context, conversation_of, create_thread, say,
    turn_ids, uuid_of,
};
use common::{TestContext, gql};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t10_the_active_branch_ends_at_the_most_recent_message_and_every_branch_stays_readable() {
    // Given T: M1 by A, M2 answering it, M3 by A answering M2.
    let ctx = TestContext::setup().await;
    let a = ctx.enrol("ada").await;
    let t = create_thread(&ctx, Some(&a), &[]).await;
    let analyst = ai("a1", "Analyst");
    let m1 = say(&ctx, t, None, &a, "first question").await;
    let m2 = answer(
        &ctx,
        t,
        Some(m1),
        analyst.clone(),
        text_answer("first answer"),
    )
    .await;
    let m3 = say(&ctx, t, Some(m2), &a, "follow-up").await;
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1, m2, m3]);

    let mut a_feed = Live::thread(&ctx, &a.passport, t).await;

    // When M2 is regenerated as M2b (same parent M1), and M3b answers M2b.
    let m2b = answer(
        &ctx,
        t,
        Some(m1),
        analyst.clone(),
        text_answer("better answer"),
    )
    .await;
    // (3) A's live view switches branch: M2b enters, M2 and M3 — now off the
    // active branch — leave it.
    a_feed
        .collect_until("A's view switches to M2b's branch", |seen| {
            seen.iter().any(|delta| upsert_of(delta, m2b).is_some())
                && seen.iter().any(|delta| removes(delta, m2))
                && seen.iter().any(|delta| removes(delta, m3))
        })
        .await;
    let m3b = say(&ctx, t, Some(m2b), &a, "follow-up, again").await;

    // Then the active branch is M1, M2b, M3b.
    assert_eq!(
        active_branch(&ctx, &a.passport, t).await,
        vec![m1, m2b, m3b]
    );
    // And the branch ending at M3 reads M1, M2, M3, newest first.
    let tip_m3: Vec<Uuid> = branch(&ctx, &a.passport, t, 10, Some(m3))
        .await
        .iter()
        .map(|view| uuid_of(&view["id"]))
        .collect();
    assert_eq!(tip_m3, vec![m3, m2, m1]);
    // And a runner's conversation follows the active branch.
    let runner = ctx.service_account(true);
    let read = context(&ctx, &runner.passport, t, None).await;
    let conversation = conversation_of(&read);
    assert_eq!(turn_ids(&conversation), vec![m1, m2b, m3b]);
    assert!(
        conversation.turns()[2].as_input().is_some(),
        "M3b, A's input"
    );
    let raw = read["conversation"].to_string();
    assert!(
        raw.contains("better answer") && !raw.contains("first answer"),
        "the runner reads M2b, not M2: {raw}"
    );

    // When M1 is retried: M1b, without parent, is the most recent.
    let m1b = say(&ctx, t, None, &a, "first question, rephrased").await;

    // Then the active branch starts — and ends — at M1b.
    assert_eq!(active_branch(&ctx, &a.passport, t).await, vec![m1b]);
    // And M1's branch stays readable.
    let tip_m3b: Vec<Uuid> = branch(&ctx, &a.passport, t, 10, Some(m3b))
        .await
        .iter()
        .map(|view| uuid_of(&view["id"]))
        .collect();
    assert_eq!(tip_m3b, vec![m3b, m2b, m1]);

    // Given a long branch: twelve messages, each answering the previous.
    let long = create_thread(&ctx, Some(&a), &[]).await;
    let mut chain = Vec::new();
    let mut parent = None;
    for n in 0..12 {
        let id = if n % 2 == 0 {
            say(&ctx, long, parent, &a, &format!("question {n}")).await
        } else {
            answer(
                &ctx,
                long,
                parent,
                analyst.clone(),
                text_answer(&format!("answer {n}")),
            )
            .await
        };
        chain.push(id);
        parent = Some(id);
    }

    // When a member reads it page by page, five at a time, each page starting
    // before the oldest message of the previous one.
    let mut read = Vec::new();
    let mut before = None;
    loop {
        let answer = branch_answer(&ctx, &a.passport, long, 5, before, None).await;
        let page: Vec<Uuid> = gql::data(&answer, "threadsMessages")
            .as_array()
            .expect("a page")
            .iter()
            .map(|view| uuid_of(&view["id"]))
            .collect();
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 5);
        before = page.last().copied();
        read.extend(page);
        assert!(
            read.len() <= chain.len(),
            "the pages never run past the branch"
        );
    }

    // Then the pages follow each other with no gap and no duplicate.
    let mut newest_first = chain.clone();
    newest_first.reverse();
    assert_eq!(read, newest_first);
    assert_eq!(read.iter().collect::<BTreeSet<_>>().len(), chain.len());

    ctx.teardown().await;
}
