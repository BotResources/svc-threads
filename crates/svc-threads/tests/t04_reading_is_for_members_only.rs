//! **T04 — Reading is for members only, super users included.**
//!
//! > Thread T has members O and A; user U is not a member and S is a super user
//! > who is not a member. A reads T, its members, resources and messages and
//! > subscribes to it. U and S asking for T by id, for its messages, for a read
//! > access to one of its artifacts, or subscribing to it each get the same not
//! > found as for a random id; U's and S's list of threads does not contain T.
//! > The harness adds S as member: S now reads T. An anonymous call is refused;
//! > a user absent from the roster, a deactivated user and a user the roster
//! > marked out are refused everything with ACTIVE_USER_REQUIRED; reactivated,
//! > the user reads again. A service account without threads:runner is refused
//! > the human surface. U's list of threads shows only threads U is a member
//! > of, the most recently active first: a thread's activity is its latest
//! > message's date, else its creation date.
//!
//! Channels: every read root of the human surface (2), the live feed of a
//! thread and its refusal (3), the read access to an artifact. The adversary
//! is a valid, admitted Passport asking for what it must not see: it is
//! answered exactly as for an id nobody created — no existence leak.

mod common;

use br_core_auth::Passport;
use common::artifacts::{File, download_answer, download_url, fetch, upload};
use common::content::artifact_block;
use common::feed::{Live, in_reset, thread_feed, ws_opening};
use common::flows::{
    ai, answer, branch_answer, command, create_thread, listed, member_ids, say, sorted,
    thread_confirmed, thread_view,
};
use common::{
    ACTIVE_USER_REQUIRED, M_SET_READ_MARK, Person, Q_THREAD, Q_THREADS, TestContext, gql,
    person_passport,
};
use contract_threads::AddMembers;
use contract_threads::llm::StopReason;
use serde_json::{Value, json};
use uuid::Uuid;

/// Every read of the human surface `passport` may attempt on `thread`, as
/// answered — the code and the data, ids blanked.
async fn reads(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    artifact: Uuid,
) -> Vec<String> {
    let blank = |answer: Value| {
        format!("{:?} {}", gql::code(&answer), answer["data"])
            .replace(&thread.to_string(), "<id>")
            .replace(&artifact.to_string(), "<artifact>")
    };
    vec![
        blank(ctx.run(passport, Q_THREAD, json!({ "id": thread })).await),
        blank(branch_answer(ctx, passport, thread, 10, None, None).await),
        blank(download_answer(ctx, passport, thread, artifact).await),
    ]
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t04_a_thread_is_read_by_its_members_only_and_a_stranger_gets_not_found() {
    // Given T with members O and A, a message and an attached artifact; U, not
    // a member; S, a super user, not a member.
    let ctx = TestContext::setup().await;
    let (o, a, u) = (
        ctx.enrol("olga").await,
        ctx.enrol("ada").await,
        ctx.enrol("ugo").await,
    );
    let s = ctx.enrol_super_user("sam").await;
    let t = create_thread(&ctx, Some(&o), &[&a]).await;
    let m1 = say(&ctx, t, None, &o, "members only").await;
    let runner = ctx.service_account(true);
    let file = File::csv("q,n\n1,2\n");
    let x = upload(&ctx, &runner.passport, t, &file).await;
    answer(
        &ctx,
        t,
        Some(m1),
        ai("a1", "Analyst"),
        vec![common::content::step(
            StopReason::EndTurn,
            None,
            vec![common::content::ai_text("here"), artifact_block(x)],
        )],
    )
    .await;

    // When A reads T, its messages, and follows it.
    let view = thread_view(&ctx, &a.passport, t).await;
    let messages = gql::data(
        &branch_answer(&ctx, &a.passport, t, 10, None, None).await,
        "threadsMessages",
    )
    .clone();
    let live = Live::thread(&ctx, &a.passport, t).await;

    // Then A reads it all: the thread with its members, its messages, the
    // artifact's bytes, and a live feed opening on T.
    assert_eq!(member_ids(&view), sorted(&[o.id, a.id]));
    assert_eq!(messages.as_array().map(Vec::len), Some(2), "{messages}");
    let url = download_url(&ctx, &a.passport, t, x)
        .await
        .expect("A obtains a read access");
    assert_eq!(fetch(&url).await, (200, file.bytes.clone()));
    assert!(in_reset(&live.reset, t).is_some(), "{}", live.reset);

    // When U and S ask for T, its messages, its artifact, or follow it.
    let random = Uuid::now_v7();
    let unknown_artifact = Uuid::now_v7();
    let as_unknown_u = reads(&ctx, &u.passport, random, unknown_artifact).await;
    for stranger in [&u, &s] {
        let what = &stranger.label;
        // Then each gets exactly what an id nobody created gets.
        assert_eq!(
            reads(&ctx, &stranger.passport, t, x).await,
            as_unknown_u,
            "{what}: a non-member is answered as for an unknown id"
        );
        assert_eq!(
            thread_view(&ctx, &stranger.passport, t).await,
            Value::Null,
            "{what}: not found"
        );
        assert_eq!(
            ws_opening(&ctx, &stranger.passport, &thread_feed(t, 10), &[t]).await,
            ws_opening(
                &ctx,
                &stranger.passport,
                &thread_feed(random, 10),
                &[random]
            )
            .await,
            "{what}: following T opens as following an unknown id"
        );
        // And T is not in their list.
        assert!(
            !listed(&ctx, &stranger.passport).await.contains(&t),
            "{what}: T is not listed"
        );
    }

    // When the harness adds S as a member.
    let added = command(
        &ctx,
        &AddMembers {
            thread_id: t,
            user_ids: vec![s.id],
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, added, "adding S").await;

    // Then S reads T — as a member, not as a super user.
    assert!(thread_view(&ctx, &s.passport, t).await.is_object());
    assert!(listed(&ctx, &s.passport).await.contains(&t));

    // When nobody is named at all.
    let (status, _) = ctx.run_anonymous(Q_THREADS, json!({ "size": 10 })).await;
    // Then the call is refused.
    assert_eq!(status, 401, "an anonymous call is refused");

    // When a person absent from the roster, a deactivated member and a member
    // the roster marked out call anything of the human surface.
    let absent = person_passport(Uuid::now_v7(), false);
    let deactivated: Person = ctx.enrol("dee").await;
    let marked_out: Person = ctx.enrol("max").await;
    let add_both = command(
        &ctx,
        &AddMembers {
            thread_id: t,
            user_ids: vec![deactivated.id, marked_out.id],
            acting_user_id: Some(o.id),
        },
    )
    .await;
    thread_confirmed(&ctx, add_both, "adding D and M").await;
    ctx.deactivate(&deactivated).await;
    ctx.retract(&marked_out).await;
    let plain_service = ctx.service_account(false);
    for (passport, what) in [
        (&absent, "a person absent from the roster"),
        (&deactivated.passport, "a deactivated member"),
        (&marked_out.passport, "a member out of the roster"),
        (
            &plain_service.passport,
            "a service account without threads:runner",
        ),
    ] {
        // Then each is refused everything with ACTIVE_USER_REQUIRED.
        for answer in [
            ctx.run(passport, Q_THREADS, json!({ "size": 10 })).await,
            ctx.run(passport, Q_THREAD, json!({ "id": t })).await,
            branch_answer(&ctx, passport, t, 10, None, None).await,
            download_answer(&ctx, passport, t, x).await,
            ctx.run(
                passport,
                M_SET_READ_MARK,
                json!({ "thread": t, "message": m1 }),
            )
            .await,
        ] {
            gql::assert_refused(&answer, ACTIVE_USER_REQUIRED);
            assert!(
                answer["data"].is_null()
                    || answer["data"]
                        .as_object()
                        .is_some_and(|data| data.values().all(Value::is_null)),
                "{what}: a refusal carries no data: {answer}"
            );
        }
    }

    // When the deactivated member is reactivated.
    ctx.reactivate(&deactivated).await;
    // Then they read T again.
    assert!(
        thread_view(&ctx, &deactivated.passport, t)
            .await
            .is_object()
    );

    ctx.teardown().await;
}

#[tokio::test]
#[ignore = "real-infra e2e: needs Postgres (E2E_PG_ADMIN_URL), nats-server, the nats CLI and minio on PATH"]
async fn t04_a_users_list_holds_their_threads_only_the_most_recently_active_first() {
    // Given U a member of Ta, Tb and Tc, created in that order, and of no
    // other; Tz, U is not a member of.
    let ctx = TestContext::setup().await;
    let (u, v) = (ctx.enrol("ugo").await, ctx.enrol("val").await);
    let ta = create_thread(&ctx, None, &[&u]).await;
    let tb = create_thread(&ctx, None, &[&u, &v]).await;
    let tc = create_thread(&ctx, Some(&u), &[]).await;
    let tz = create_thread(&ctx, Some(&v), &[]).await;

    // Then U's list holds Ta, Tb and Tc only, the latest created first: their
    // activity is their creation date while they have no message.
    assert_eq!(listed(&ctx, &u.passport).await, vec![tc, tb, ta]);
    assert!(!listed(&ctx, &u.passport).await.contains(&tz));

    // When a message is stored on Ta.
    let m = say(&ctx, ta, None, &u, "bumping Ta").await;

    // Then Ta is the most recently active: its activity is its latest
    // message's date.
    assert_eq!(listed(&ctx, &u.passport).await, vec![ta, tc, tb]);
    let views = common::flows::thread_list(&ctx, &u.passport).await;
    let message = common::flows::message_view(&ctx, &u.passport, ta, m).await;
    assert_eq!(views[0]["lastActivityAt"], message["createdAt"]);

    ctx.teardown().await;
}
