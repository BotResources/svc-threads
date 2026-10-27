//! **T05 — Storing a complete user message.**
//!
//! > Playing the agents service, the harness stores on OPEN thread T, with
//! > message.store v2, a COMPLETED user message M1: an input turn under M1's id
//! > holding a human message of member A under a minted entry id, with a text
//! > block and an image block under minted ids, and no parent: message.confirmed
//! > names the command, M1 and status COMPLETED; every member reads M1 as that
//! > turn, with A's current display name, and a member following T receives it.
//! > A user message holding a structured content block is stored too. The
//! > message's Created fact carries kinds, positions, author and status, and no
//! > text. The bus redelivers the store under its own envelope id: no second
//! > distinct answer, M1 stored once. A message the library refuses (an empty
//! > text, an explicit null, an entry id equal to its turn's id) is refused
//! > MESSAGE_INVALID naming the library's error code, and nothing is stored; so
//! > is a user message holding an annotation. A human who is not a member is
//! > refused NOT_A_MEMBER. A parent from another thread is refused
//! > MESSAGE_INVALID; a parent nobody stored, MESSAGE_NOT_FOUND; a thread nobody
//! > created, THREAD_NOT_FOUND. Reusing M1's id is refused KEY_REUSED. An
//! > impersonated author's store carries the impersonator: it is in the Created
//! > fact, never on the message. Renaming A in Identity shows the new name on M1
//! > everywhere members read it and in the members of a runner's context, while
//! > no fact and no stored message carries a name. The thread is closed
//! > COMPLETED: storing on it is refused THREAD_NOT_OPEN, as is any later close.
//!
//! The library's error code a refusal names is computed by the library itself
//! on the very input sent (`common::content::entry_refusal`, `InputTurn::new`).
//!
//! Channels: the answers (4), M1 as each member reads it with its affordance
//! (2), B's live feed of T (3), the fact log and the store, a runner's context.
//! Adversarial: a non-member human, a foreign parent, a reused id, an
//! impersonator, a redelivered command, content the library refuses.

mod common;

use common::content::{
    self, assert_same_turn, entry_refusal, human, image, input_turn, structured, text, to_json,
    turn_of, warning,
};
use common::facts;
use common::feed::{Live, upsert_of};
use common::flows::{
    active_branch, command, context, conversation_of, create_thread, message_confirmed,
    message_refused, message_view, say, thread_confirmed, thread_refused, user_entry_message,
    user_message,
};
use common::{TestContext, identity, store};
use contract_threads::llm::{EntryId, HumanMessage, IncomingEntry, InputTurn, Turn, TurnEntry};
use contract_threads::{CloseThread, ClosingStatus, MessageStatus, NewTurn, ReasonCode};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t05_a_complete_user_message_is_stored_checked_by_the_library_and_never_leaks_content() {
    // Given OPEN thread T with members O, A and B; X in the roster, not a
    // member; B following T.
    let ctx = TestContext::setup().await;
    let (o, a, b, x) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("bob").await,
        ctx.enrol("xan").await,
    );
    let t = create_thread(&ctx, Some(&o), &[&a, &b]).await;
    let mut b_feed = Live::thread(&ctx, &b.passport, t).await;

    // When the agents service stores, with message.store v2, M1: an input turn
    // under M1's id holding A's human message (a minted entry id) with a text
    // block and an image block, no parent.
    const SECRET: &str = "the quarterly figures are confidential";
    let entry = human(a.id, vec![text(SECRET), image()]);
    let store_m1 = user_entry_message(t, None, a.id, &entry);
    let m1 = store_m1.message_id;
    let store_command = command(&ctx, &store_m1).await;

    // Then (4) message.confirmed names the command, M1 and COMPLETED.
    let confirmed =
        message_confirmed(&ctx, store_command, MessageStatus::Completed, "storing M1").await;
    assert_eq!(
        (confirmed.thread_id, confirmed.message_id, confirmed.deleted),
        (t, m1, false)
    );
    assert_eq!(confirmed.command_type, "message.store");
    common::flows::answered_once(&ctx, store_command, "storing M1").await;

    // And (2) every member reads M1 as that turn — the library's input turn,
    // entry for entry — under A's current name, with the read mark it offers.
    let stored_as = Turn::from(input_turn(m1, vec![entry.clone()]));
    for person in [&o, &a, &b] {
        let view = message_view(&ctx, &person.passport, t, m1).await;
        assert_eq!(view["status"], "COMPLETED", "{view}");
        assert_eq!(view["deleted"], false);
        assert_eq!(view["parentMessageId"], Value::Null);
        assert_eq!(view["turnKind"], "INPUT_TURN", "{view}");
        assert_eq!(
            view["author"],
            json!({
                "kind": "USER",
                "userId": a.id,
                "aiId": null,
                "name": identity::display_name("ada"),
            })
        );
        let read = &view["entries"][0];
        assert_eq!(
            (
                read["id"].clone(),
                read["index"].clone(),
                read["kind"].clone()
            ),
            (
                json!(entry.id().as_uuid()),
                json!(0),
                json!("human_message")
            ),
            "{view}"
        );
        assert_eq!(read["annotations"], json!([]));
        assert_same_turn(&turn_of(&view), &stored_as, "M1");
        common::gql::assert_allowed(&view, "markRead");
    }

    // And (3) B's feed receives M1, carrying its content.
    let pushed = b_feed
        .until("M1 reaches B's feed", |delta| {
            upsert_of(delta, m1).is_some()
        })
        .await;
    assert_same_turn(
        &turn_of(upsert_of(&pushed, m1).expect("an upsert of M1")),
        &stored_as,
        "M1 as B's feed carries it",
    );

    // And a user message holding a structured content block is stored too.
    let figures = structured(json!({ "quarter": "Q3", "revenue": 42 }));
    let structured_entry = human(a.id, vec![figures]);
    let with_structure = user_entry_message(t, Some(m1), a.id, &structured_entry);
    let m_structured = with_structure.message_id;
    common::flows::store(&ctx, with_structure, MessageStatus::Completed).await;
    assert_same_turn(
        &turn_of(&message_view(&ctx, &b.passport, t, m_structured).await),
        &Turn::from(input_turn(m_structured, vec![structured_entry])),
        "a structured content block",
    );

    // And the Created fact names the author, the kinds and the status — never
    // the text.
    let m1_facts = facts::of(&ctx.pool, "message", m1).await;
    let created = facts::the(&m1_facts, "Created");
    let payload = created.payload.to_string();
    for needle in [
        a.id.to_string(),
        "human_message".to_string(),
        "COMPLETED".to_string(),
    ] {
        assert!(
            payload.contains(&needle),
            "Created carries {needle}: {payload}"
        );
    }
    facts::assert_no_fact_carries(&ctx.pool, SECRET, "a message's text").await;

    // When the bus redelivers M1's store, under its own envelope id.
    common::flows::redeliver(&ctx, store_command, &store_m1).await;
    // Then no second distinct answer is published, and M1 is stored once.
    common::flows::replayed_alike(&ctx, store_command, "the redelivered store").await;
    assert_eq!(
        active_branch(&ctx, &a.passport, t).await,
        vec![m1, m_structured]
    );
    assert_eq!(
        facts::of(&ctx.pool, "message", m1).await.len(),
        m1_facts.len()
    );

    // When the agents service stores messages the library refuses: an empty
    // text, an explicit null, an entry id equal to its turn's id.
    let valid = to_json(&human(a.id, vec![text("hello")]));
    let mut empty_text = valid.clone();
    empty_text["content"][0]["text"] = json!("");
    let mut explicit_null = valid.clone();
    explicit_null["observed_at"] = Value::Null;
    let same_id = Uuid::now_v7();
    let entry_as_turn = TurnEntry::from(
        HumanMessage::new(
            EntryId::from_uuid(same_id).expect("a UUIDv7"),
            content::human_id(a.id),
            vec![text("hello")],
        )
        .expect("a human message on its own"),
    );
    let same_id_code = InputTurn::new(
        content::turn_id(same_id),
        vec![IncomingEntry::try_from(entry_as_turn.clone()).expect("an incoming entry")],
    )
    .expect_err("the library refuses an entry id equal to its turn's id")
    .code()
    .as_str()
    .to_string();
    let cases: Vec<(Uuid, Value, String, &str)> = vec![
        (
            Uuid::now_v7(),
            empty_text.clone(),
            entry_refusal(&empty_text).expect("the library refuses an empty text"),
            "an empty text",
        ),
        (
            Uuid::now_v7(),
            explicit_null.clone(),
            entry_refusal(&explicit_null).expect("the library refuses an explicit null"),
            "an explicit null",
        ),
        (
            same_id,
            to_json(&entry_as_turn),
            same_id_code,
            "an entry id equal to its turn's id",
        ),
    ];
    for (message_id, entry_json, error, what) in cases {
        let mut refused = user_message(t, Some(m1), a.id, vec![text("placeholder")]);
        refused.message_id = message_id;
        let NewTurn::User(user) = &mut refused.message else {
            unreachable!("a user message")
        };
        user.entry = entry_json;
        let refused_command = command(&ctx, &refused).await;

        // Then each is refused MESSAGE_INVALID, naming the library's code, and
        // nothing is stored.
        let answer = message_refused(&ctx, refused_command, ReasonCode::MessageInvalid, what).await;
        assert_eq!(answer.params["error"], json!(error), "{what}: {answer:?}");
        assert!(
            facts::of(&ctx.pool, "message", message_id).await.is_empty(),
            "{what}"
        );
        assert_eq!(
            active_branch(&ctx, &a.passport, t).await,
            vec![m1, m_structured],
            "{what}"
        );
    }

    // When a user message holds an annotation.
    let mut annotated = user_message(t, Some(m1), a.id, vec![text("noted")]);
    let annotated_id = annotated.message_id;
    if let NewTurn::User(user) = &mut annotated.message {
        let after: TurnEntry =
            serde_json::from_value(user.entry.clone()).expect("the library's entry");
        user.annotations = vec![content::after(&after, warning("NOTE", "a user's note"))];
    }
    let annotated_command = command(&ctx, &annotated).await;
    // Then refused MESSAGE_INVALID, and nothing is stored.
    let answer = message_refused(
        &ctx,
        annotated_command,
        ReasonCode::MessageInvalid,
        "a user message holding an annotation",
    )
    .await;
    assert_eq!(
        answer.params["error"], "annotation_in_user_message",
        "{answer:?}"
    );
    assert!(
        facts::of(&ctx.pool, "message", annotated_id)
            .await
            .is_empty()
    );

    // When X, not a member, is the human of the message.
    let outsider = command(&ctx, &user_message(t, Some(m1), x.id, vec![text("hello")])).await;
    // Then refused NOT_A_MEMBER.
    message_refused(&ctx, outsider, ReasonCode::NotAMember, "a non-member human").await;

    // When a message of T names a parent of another thread.
    let elsewhere = create_thread(&ctx, Some(&a), &[]).await;
    let foreign = say(&ctx, elsewhere, None, &a, "elsewhere").await;
    let foreign_parent = command(
        &ctx,
        &user_message(t, Some(foreign), a.id, vec![text("hi")]),
    )
    .await;
    // Then refused MESSAGE_INVALID.
    message_refused(
        &ctx,
        foreign_parent,
        ReasonCode::MessageInvalid,
        "a foreign parent",
    )
    .await;

    // When a message of T names a parent nobody stored.
    let unknown_parent = command(
        &ctx,
        &user_message(t, Some(Uuid::now_v7()), a.id, vec![text("hi")]),
    )
    .await;
    // Then refused MESSAGE_NOT_FOUND.
    message_refused(
        &ctx,
        unknown_parent,
        ReasonCode::MessageNotFound,
        "an unknown parent",
    )
    .await;

    // When a message is stored on a thread nobody created.
    let nowhere = command(
        &ctx,
        &user_message(Uuid::now_v7(), None, a.id, vec![text("hi")]),
    )
    .await;
    // Then refused THREAD_NOT_FOUND.
    message_refused(
        &ctx,
        nowhere,
        ReasonCode::ThreadNotFound,
        "an unknown thread",
    )
    .await;

    // When a new store reuses M1's id.
    let mut reuse = user_message(t, None, b.id, vec![text("mine")]);
    reuse.message_id = m1;
    let reuse_command = command(&ctx, &reuse).await;
    // Then refused KEY_REUSED, M1 untouched.
    message_refused(
        &ctx,
        reuse_command,
        ReasonCode::KeyReused,
        "reusing M1's id",
    )
    .await;
    assert_same_turn(
        &turn_of(&message_view(&ctx, &a.passport, t, m1).await),
        &stored_as,
        "M1 after a reuse of its id",
    );

    // When A's message is stored through impersonation by I.
    let impersonator = Uuid::now_v7();
    let mut impersonated = user_message(t, Some(m1), a.id, vec![text("on A's behalf")]);
    impersonated.impersonator_id = Some(impersonator);
    let m2 = impersonated.message_id;
    let impersonated_command = command(&ctx, &impersonated).await;
    message_confirmed(
        &ctx,
        impersonated_command,
        MessageStatus::Completed,
        "impersonated",
    )
    .await;

    // Then the Created fact carries the impersonator, and the message never.
    let created = facts::of(&ctx.pool, "message", m2).await;
    assert_eq!(
        facts::the(&created, "Created").impersonator_id,
        Some(impersonator)
    );
    let view = message_view(&ctx, &a.passport, t, m2).await;
    assert!(
        !view.to_string().contains(&impersonator.to_string()),
        "the impersonator is never on the message: {view}"
    );

    // When Identity renames A.
    ctx.rename(&a, "adele").await;

    // Then the new name shows on M1 everywhere members read it, and in the
    // members of a runner's context — the conversation itself names nobody.
    for person in [&o, &b] {
        let view = message_view(&ctx, &person.passport, t, m1).await;
        assert_eq!(view["author"]["name"], identity::display_name("adele"));
    }
    let runner = ctx.service_account(true);
    let read = context(&ctx, &runner.passport, t, None).await;
    let a_member = read["members"]
        .as_array()
        .expect("the context's members")
        .iter()
        .find(|member| member["userId"] == json!(a.id))
        .cloned()
        .expect("A among the context's members");
    assert_eq!(a_member["displayName"], identity::display_name("adele"));
    let conversation = conversation_of(&read);
    let first = conversation.turns()[0]
        .as_input()
        .expect("the conversation opens on M1, an input turn");
    assert_eq!(
        first.entries()[0].kind().map(|kind| kind.as_str()),
        Some("human_message")
    );
    let IncomingEntry::HumanMessage(message) = &first.entries()[0] else {
        panic!("M1 holds A's human message: {conversation:?}");
    };
    assert_eq!(message.human().as_uuid(), a.id, "the human is A, by id");
    assert_eq!(
        to_json(&first.entries()[0]),
        to_json(&entry),
        "M1's human message, whole"
    );
    // And no fact and no stored message carries a name, the old nor the new.
    for name in [
        identity::display_name("ada"),
        identity::display_name("adele"),
    ] {
        facts::assert_no_fact_carries(&ctx.pool, &name, "a name").await;
        assert!(
            !read["conversation"].to_string().contains(&name),
            "the conversation names nobody"
        );
        store::assert_nowhere(&ctx.pool, &name, &["known_users"], "a stored message").await;
    }

    // When the thread is closed COMPLETED.
    let close = |status| CloseThread {
        thread_id: t,
        status,
        acting_user_id: Some(o.id),
    };
    let closed = command(&ctx, &close(ClosingStatus::Completed)).await;
    thread_confirmed(&ctx, closed, "closing T").await;

    // Then storing on it is refused THREAD_NOT_OPEN, as is any later close.
    let late = command(&ctx, &user_message(t, Some(m2), a.id, vec![text("late")])).await;
    message_refused(
        &ctx,
        late,
        ReasonCode::ThreadNotOpen,
        "a message on a closed thread",
    )
    .await;
    let reclose = command(&ctx, &close(ClosingStatus::Failed)).await;
    thread_refused(&ctx, reclose, ReasonCode::ThreadNotOpen, "closing again").await;

    ctx.teardown().await;
}
