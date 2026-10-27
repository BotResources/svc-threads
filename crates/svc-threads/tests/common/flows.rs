//! The gestures every scenario repeats, each asserting what the specification
//! demands of it: a command answered exactly once by its id, a thread created
//! and published, a message stored, and the reads a member or a runner makes.
//!
//! State is reached through the service's own surfaces only.

use std::collections::BTreeMap;

use br_core_auth::Passport;
use contract_threads::llm::{Conversation, PlainBlock, ProjectedThread, Turn, TurnEntry};
use contract_threads::{
    AiDelivery, AiTurn, CompleteMessage, CreateThread, DeleteMessage, EditMessage, LibraryJson,
    MessageCommandConfirmed, MessageCommandRefused, MessageStatus, MessageTurn, PlacedAnnotation,
    ReasonCode, ReopenThread, ResolveThread, StoreMessageV2, StreamSeal, ThreadCommandConfirmed,
    ThreadCommandRefused, ThreadsCommand, UserTurn,
};
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::commands::{Answer, commanding_service, send, send_as};
use super::content::{self, Agent};
use super::{
    PUSH_TIMEOUT, Person, Q_CONTEXT_FOR, Q_MESSAGES, Q_THREAD, Q_THREADS, QUIET_WINDOW,
    TestContext, eventually, gql,
};

// ── commands and their one answer ───────────────────────────────────────────

/// Sends `command` as the commanding service; answers its id. The id is
/// entered in the ledger the teardown checks: one answer per command.
pub async fn command<C: ThreadsCommand + Serialize>(ctx: &TestContext, command: &C) -> Uuid {
    let command_id = send(&ctx.nats, commanding_service(), command).await;
    ctx.sent.lock().expect("the ledger").push(command_id);
    command_id
}

/// Sends `command` under an envelope naming `actor` — a person, as a commanding
/// service's GraphQL mutation stages it under its caller; answers its id,
/// entered in the ledger like [`command`]'s.
pub async fn command_as<C: ThreadsCommand + Serialize>(
    ctx: &TestContext,
    actor: br_core_integration::Actor,
    command: &C,
) -> Uuid {
    let command_id = send(&ctx.nats, actor, command).await;
    ctx.sent.lock().expect("the ledger").push(command_id);
    command_id
}

/// The bus delivers `command` again under its own id.
pub async fn redeliver<C: ThreadsCommand + Serialize>(
    ctx: &TestContext,
    command_id: Uuid,
    command: &C,
) {
    ctx.redelivered.lock().expect("the ledger").push(command_id);
    send_as(&ctx.nats, commanding_service(), command_id, command).await;
}

pub async fn thread_confirmed(
    ctx: &TestContext,
    command_id: Uuid,
    what: &str,
) -> ThreadCommandConfirmed {
    match ctx.answers.await_answer(command_id, what).await {
        Answer::ThreadConfirmed(confirmed) => {
            assert_eq!(confirmed.command_id, command_id, "{what}: {confirmed:?}");
            confirmed
        }
        other => panic!(
            "{what}: thread.confirmed expected, got {other:?}\n{}",
            ctx.logs()
        ),
    }
}

pub async fn thread_refused(
    ctx: &TestContext,
    command_id: Uuid,
    code: ReasonCode,
    what: &str,
) -> ThreadCommandRefused {
    match ctx.answers.await_answer(command_id, what).await {
        Answer::ThreadRefused(refused) => {
            assert_eq!(refused.command_id, command_id, "{what}: {refused:?}");
            assert_eq!(refused.reason_code, code, "{what}: {refused:?}");
            refused
        }
        other => panic!(
            "{what}: thread.refused {code} expected, got {other:?}\n{}",
            ctx.logs()
        ),
    }
}

pub async fn message_confirmed(
    ctx: &TestContext,
    command_id: Uuid,
    status: MessageStatus,
    what: &str,
) -> MessageCommandConfirmed {
    match ctx.answers.await_answer(command_id, what).await {
        Answer::MessageConfirmed(confirmed) => {
            assert_eq!(confirmed.command_id, command_id, "{what}: {confirmed:?}");
            assert_eq!(confirmed.status, status, "{what}: {confirmed:?}");
            confirmed
        }
        other => panic!(
            "{what}: message.confirmed {status} expected, got {other:?}\n{}",
            ctx.logs()
        ),
    }
}

pub async fn message_refused(
    ctx: &TestContext,
    command_id: Uuid,
    code: ReasonCode,
    what: &str,
) -> MessageCommandRefused {
    match ctx.answers.await_answer(command_id, what).await {
        Answer::MessageRefused(refused) => {
            assert_eq!(refused.command_id, command_id, "{what}: {refused:?}");
            assert_eq!(refused.reason_code, code, "{what}: {refused:?}");
            refused
        }
        other => panic!(
            "{what}: message.refused {code} expected, got {other:?}\n{}",
            ctx.logs()
        ),
    }
}

/// The command was answered exactly once, held over the quiet window.
pub async fn answered_once(ctx: &TestContext, command_id: Uuid, what: &str) {
    tokio::time::sleep(QUIET_WINDOW).await;
    let answers = ctx.answers.for_command(command_id);
    assert_eq!(
        answers.len(),
        1,
        "{what}: one answer per command, got {answers:?}"
    );
}

/// After a redelivery of `command_id`, held over the quiet window: no second
/// distinct answer. The replayed answer may never arrive (it is re-published
/// under its first message id, which the broker deduplicates); any that does
/// is identical to the first.
pub async fn replayed_alike(ctx: &TestContext, command_id: Uuid, what: &str) {
    tokio::time::sleep(QUIET_WINDOW).await;
    let answers = ctx.answers.for_command(command_id);
    assert!(!answers.is_empty(), "{what}: the command was answered");
    assert!(
        answers.iter().all(|answer| *answer == answers[0]),
        "{what}: a redelivery answers alike, never anew: {answers:?}"
    );
}

// ── threads ─────────────────────────────────────────────────────────────────

/// A creation under a fresh UUIDv7.
pub fn creation(owner: Option<Uuid>, users: &[Uuid]) -> CreateThread {
    CreateThread {
        thread_id: Uuid::now_v7(),
        owner_id: owner,
        user_ids: users.to_vec(),
        title: None,
        resources: BTreeMap::new(),
        parent_thread_id: None,
        origin_block_id: None,
        acting_user_id: owner,
    }
}

/// A thread created, confirmed and published: `owner` (if any) and `members`.
pub async fn create_thread(ctx: &TestContext, owner: Option<&Person>, members: &[&Person]) -> Uuid {
    let users: Vec<Uuid> = members.iter().map(|person| person.id).collect();
    create(ctx, creation(owner.map(|o| o.id), &users)).await
}

/// `creation` sent, confirmed, and its members entry published.
pub async fn create(ctx: &TestContext, creation: CreateThread) -> Uuid {
    let thread = creation.thread_id;
    let command_id = command(ctx, &creation).await;
    thread_confirmed(ctx, command_id, "the creation").await;
    await_members(ctx, thread, "the creation's members entry").await;
    thread
}

/// Waits until `threads.members.{thread}` lists exactly `users` (and
/// `owner`): the entry leaves by its own relay, not with the confirmation.
pub async fn await_members_equal(
    ctx: &TestContext,
    thread: Uuid,
    owner: Option<Uuid>,
    users: &[Uuid],
    what: &str,
) {
    let expected = sorted(users);
    let deadline = tokio::time::Instant::now() + PUSH_TIMEOUT;
    loop {
        let entry = ctx.published_members(thread).await;
        if entry
            .as_ref()
            .is_some_and(|entry| entry.owner_id == owner && sorted(&entry.users) == expected)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: threads.members.{thread} never listed {owner:?} / {expected:?}; last {entry:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// The members entry's revision once the quiet window has passed: a
/// comparison of two such revisions proves no rewrite was in flight.
pub async fn settled_revision(ctx: &TestContext, thread: Uuid) -> br_util_nats_fabric::Revision {
    tokio::time::sleep(QUIET_WINDOW).await;
    ctx.members_entry(thread)
        .await
        .unwrap_or_else(|| panic!("threads.members.{thread} is published"))
        .1
}

/// Waits until `threads.members.{thread}` is published, and answers it.
pub async fn await_members(
    ctx: &TestContext,
    thread: Uuid,
    what: &str,
) -> contract_threads::PublishedThreadMembers {
    eventually(what, PUSH_TIMEOUT, || async {
        ctx.published_members(thread).await.is_some()
    })
    .await;
    ctx.published_members(thread).await.expect("waited above")
}

// ── messages ────────────────────────────────────────────────────────────────

/// The agent `label` named `name` (see [`content::agent`]).
pub fn ai(label: &str, name: &str) -> Agent {
    content::agent(label, name)
}

/// A store under a fresh UUIDv7, of `message`.
fn storing(
    thread: Uuid,
    parent: Option<Uuid>,
    message_id: Uuid,
    message: MessageTurn,
) -> StoreMessageV2 {
    StoreMessageV2 {
        message_id,
        thread_id: thread,
        parent_message_id: parent,
        job_id: None,
        acting_user_id: None,
        impersonator_id: None,
        message,
    }
}

/// A complete user message by `author`: an input turn holding one human
/// message of `author` with `content`.
pub fn user_message(
    thread: Uuid,
    parent: Option<Uuid>,
    author: Uuid,
    content: Vec<PlainBlock>,
) -> StoreMessageV2 {
    user_entry_message(thread, parent, author, &content::human(author, content))
}

/// A complete user message holding `entry`, as given.
pub fn user_entry_message(
    thread: Uuid,
    parent: Option<Uuid>,
    author: Uuid,
    entry: &TurnEntry,
) -> StoreMessageV2 {
    let mut store = storing(
        thread,
        parent,
        Uuid::now_v7(),
        MessageTurn::User(UserTurn {
            entry: LibraryJson::from_json(content::to_json(entry)),
            annotations: Vec::new(),
        }),
    );
    store.acting_user_id = Some(author);
    store
}

/// A complete AI message: the AI turn of `agent` holding `entries`, closed
/// completed, and no annotation.
pub fn ai_message(
    thread: Uuid,
    parent: Option<Uuid>,
    agent: &Agent,
    entries: Vec<TurnEntry>,
) -> StoreMessageV2 {
    let message = Uuid::now_v7();
    let turn = content::ai_turn(message, agent, content::closed(entries));
    whole_message(
        thread,
        parent,
        message,
        agent,
        &Turn::from(turn),
        Vec::new(),
    )
}

/// A complete AI message under `message`'s id: `turn` as given (an AI turn or
/// an input turn of incoming entries) and `annotations`.
pub fn whole_message(
    thread: Uuid,
    parent: Option<Uuid>,
    message: Uuid,
    agent: &Agent,
    turn: &Turn,
    annotations: Vec<PlacedAnnotation>,
) -> StoreMessageV2 {
    raw_whole_message(
        thread,
        parent,
        message,
        agent,
        content::to_json(turn),
        annotations,
    )
}

/// A complete AI message carrying `turn` as raw JSON: the adversarial path.
pub fn raw_whole_message(
    thread: Uuid,
    parent: Option<Uuid>,
    message: Uuid,
    agent: &Agent,
    turn: Value,
    annotations: Vec<PlacedAnnotation>,
) -> StoreMessageV2 {
    storing(
        thread,
        parent,
        message,
        MessageTurn::Ai(AiTurn {
            agent_id: agent.uuid(),
            author_name: agent.name.clone(),
            delivery: AiDelivery::Completed {
                turn: LibraryJson::from_json(turn),
                annotations,
            },
        }),
    )
}

/// An AI message opened `STREAMING`, empty.
pub fn streaming_message(thread: Uuid, parent: Option<Uuid>, agent: &Agent) -> StoreMessageV2 {
    storing(
        thread,
        parent,
        Uuid::now_v7(),
        MessageTurn::Ai(AiTurn {
            agent_id: agent.uuid(),
            author_name: agent.name.clone(),
            delivery: AiDelivery::Streaming,
        }),
    )
}

/// `store` sent and confirmed with `status`; answers the message id.
pub async fn store(ctx: &TestContext, store: StoreMessageV2, status: MessageStatus) -> Uuid {
    let message = store.message_id;
    let command_id = command(ctx, &store).await;
    let confirmed = message_confirmed(ctx, command_id, status, "the message's storage").await;
    assert_eq!(confirmed.message_id, message);
    assert!(!confirmed.deleted);
    message
}

/// `author` says `text` on `thread`, answering `parent`.
pub async fn say(
    ctx: &TestContext,
    thread: Uuid,
    parent: Option<Uuid>,
    author: &Person,
    text: &str,
) -> Uuid {
    store(
        ctx,
        user_message(thread, parent, author.id, vec![content::text(text)]),
        MessageStatus::Completed,
    )
    .await
}

/// `agent` answers `parent` with `entries`, its turn closed completed.
pub async fn answer(
    ctx: &TestContext,
    thread: Uuid,
    parent: Option<Uuid>,
    agent: Agent,
    entries: Vec<TurnEntry>,
) -> Uuid {
    store(
        ctx,
        ai_message(thread, parent, &agent, entries),
        MessageStatus::Completed,
    )
    .await
}

/// `agent` answers `parent` in `text`, attaching `artifact` in an ARTIFACT
/// annotation after its answer; its turn closed completed.
pub async fn answer_attaching(
    ctx: &TestContext,
    thread: Uuid,
    parent: Option<Uuid>,
    agent: Agent,
    text: &str,
    artifact: Uuid,
) -> Uuid {
    let replied = content::reply(text);
    let message = Uuid::now_v7();
    let turn = Turn::from(content::ai_turn(
        message,
        &agent,
        content::closed(replied.clone()),
    ));
    store(
        ctx,
        whole_message(
            thread,
            parent,
            message,
            &agent,
            &turn,
            vec![content::after(&replied[0], content::artifact(artifact))],
        ),
        MessageStatus::Completed,
    )
    .await
}

/// An AI message opened `STREAMING` and confirmed.
pub async fn open_stream(
    ctx: &TestContext,
    thread: Uuid,
    parent: Option<Uuid>,
    agent: Agent,
) -> Uuid {
    store(
        ctx,
        streaming_message(thread, parent, &agent),
        MessageStatus::Streaming,
    )
    .await
}

pub fn completion(thread: Uuid, message: Uuid, last_seq: u64, hash: &str) -> CompleteMessage {
    CompleteMessage {
        message_id: message,
        thread_id: thread,
        seal: StreamSeal {
            last_seq,
            hash: hash.to_string(),
        },
        acting_user_id: None,
    }
}

/// `acting`'s edit of `message`, made from `expected_version`, with `content`.
pub fn edit_of(
    thread: Uuid,
    message: Uuid,
    acting: Uuid,
    expected_version: u64,
    content: Vec<PlainBlock>,
) -> EditMessage {
    raw_edit_of(
        thread,
        message,
        acting,
        expected_version,
        content.iter().map(content::to_json).collect(),
    )
}

/// An edit carrying raw JSON content: the adversarial path.
pub fn raw_edit_of(
    thread: Uuid,
    message: Uuid,
    acting: Uuid,
    expected_version: u64,
    content: Vec<Value>,
) -> EditMessage {
    EditMessage {
        message_id: message,
        thread_id: thread,
        acting_user_id: acting,
        expected_version,
        content: content.into_iter().map(LibraryJson::from_json).collect(),
    }
}

/// `acting`'s deletion of `message`.
pub fn deletion_of(thread: Uuid, message: Uuid, acting: Uuid) -> DeleteMessage {
    DeleteMessage {
        message_id: message,
        thread_id: thread,
        acting_user_id: acting,
    }
}

/// `acting` resolves `thread`.
pub fn resolution_of(thread: Uuid, acting: Uuid) -> ResolveThread {
    ResolveThread {
        thread_id: thread,
        acting_user_id: acting,
    }
}

/// `acting` reopens `thread`.
pub fn reopening_of(thread: Uuid, acting: Uuid) -> ReopenThread {
    ReopenThread {
        thread_id: thread,
        acting_user_id: Some(acting),
    }
}

/// The message's version as a member reads it.
pub async fn message_version(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    message: Uuid,
) -> u64 {
    message_view(ctx, passport, thread, message).await["version"]
        .as_u64()
        .unwrap_or_else(|| panic!("message {message} has a version"))
}

// ── reads ───────────────────────────────────────────────────────────────────

/// One thread as `passport` sees it: its view, or `null`.
pub async fn thread_view(ctx: &TestContext, passport: &Passport, thread: Uuid) -> Value {
    let answer = ctx.run(passport, Q_THREAD, json!({ "id": thread })).await;
    gql::data(&answer, "threadsThread").clone()
}

/// The first page of `passport`'s list.
pub async fn thread_list(ctx: &TestContext, passport: &Passport) -> Vec<Value> {
    let answer = ctx.run(passport, Q_THREADS, json!({ "size": 50 })).await;
    gql::data(&answer, "threadsThreads")
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// The ids of `passport`'s list, in order.
pub async fn listed(ctx: &TestContext, passport: &Passport) -> Vec<Uuid> {
    thread_list(ctx, passport)
        .await
        .iter()
        .map(|view| uuid_of(&view["id"]))
        .collect()
}

/// One page of a branch, the newest first: the raw answer.
pub async fn branch_answer(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    size: u32,
    before: Option<Uuid>,
    tip: Option<Uuid>,
) -> Value {
    ctx.run(
        passport,
        Q_MESSAGES,
        json!({ "thread": thread, "size": size, "before": before, "tip": tip }),
    )
    .await
}

/// One page of a branch, the newest first.
pub async fn branch(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    size: u32,
    tip: Option<Uuid>,
) -> Vec<Value> {
    let answer = branch_answer(ctx, passport, thread, size, None, tip).await;
    gql::data(&answer, "threadsMessages")
        .as_array()
        .cloned()
        .unwrap_or_else(|| panic!("a page of messages: {answer}"))
}

/// The ids of the active branch, oldest first.
pub async fn active_branch(ctx: &TestContext, passport: &Passport, thread: Uuid) -> Vec<Uuid> {
    let mut ids: Vec<Uuid> = branch(ctx, passport, thread, 50, None)
        .await
        .iter()
        .map(|view| uuid_of(&view["id"]))
        .collect();
    ids.reverse();
    ids
}

/// One message, read as the tip of its own branch.
pub async fn message_view(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    message: Uuid,
) -> Value {
    let page = branch(ctx, passport, thread, 1, Some(message)).await;
    let view = page
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("message {message} reads as the tip of its branch"));
    assert_eq!(uuid_of(&view["id"]), message);
    view
}

/// Waits until `message` reads as `holds` demands; answers that view.
pub async fn await_message<F>(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    message: Uuid,
    what: &str,
    holds: F,
) -> Value
where
    F: Fn(&Value) -> bool,
{
    let deadline = tokio::time::Instant::now() + PUSH_TIMEOUT;
    loop {
        let view = message_view(ctx, passport, thread, message).await;
        if holds(&view) {
            return view;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: never held; last read {view}\n{}",
            ctx.logs()
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// A runner's read of a thread's context, whole (`agent` absent) or as the
/// agent context of the agent id named — any string, the adversarial path
/// included: the raw answer.
pub async fn context_answer(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    agent: Option<&str>,
) -> Value {
    ctx.run(
        passport,
        Q_CONTEXT_FOR,
        json!({ "thread": thread, "agent": agent }),
    )
    .await
}

/// A runner's read of a thread's context, whole or for `agent`.
pub async fn context(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    agent: Option<&Agent>,
) -> Value {
    let agent = agent.map(|agent| agent.uuid().to_string());
    let answer = context_answer(ctx, passport, thread, agent.as_deref()).await;
    gql::data(&answer, "threadsContext").clone()
}

/// The whole conversation of a context, loaded by the library — a
/// conversation it cannot load fails here. Its id is the thread's.
pub fn conversation_of(context: &Value) -> Conversation {
    assert!(
        context["projectedThread"].is_null(),
        "a context read naming no agent holds no projected thread: {context}"
    );
    let conversation: Conversation = serde_json::from_value(context["conversation"].clone())
        .unwrap_or_else(|e| {
            panic!(
                "the context's conversation is one the library loads ({e}): {}",
                context["conversation"]
            )
        });
    assert_eq!(
        conversation.id().as_uuid(),
        uuid_of(&context["threadId"]),
        "the conversation is the thread's"
    );
    conversation
}

/// The agent context of a context read naming an agent, loaded by the library.
pub fn projected_of(context: &Value) -> ProjectedThread {
    assert!(
        context["conversation"].is_null(),
        "a context read naming an agent holds no whole conversation: {context}"
    );
    serde_json::from_value(context["projectedThread"].clone()).unwrap_or_else(|e| {
        panic!(
            "the agent context is a projected thread the library loads ({e}): {}",
            context["projectedThread"]
        )
    })
}

/// The ids of the turns of a whole conversation, in order.
pub fn turn_ids(conversation: &Conversation) -> Vec<Uuid> {
    conversation
        .turns()
        .iter()
        .map(|turn| turn.id().as_uuid())
        .collect()
}

/// The thread's version as a member reads it.
pub async fn version(ctx: &TestContext, passport: &Passport, thread: Uuid) -> i64 {
    thread_view(ctx, passport, thread).await["version"]
        .as_i64()
        .unwrap_or_else(|| panic!("thread {thread} has a version"))
}

/// `text` with every UUID it holds replaced by `<uuid>`: two payloads of the
/// same shape then compare on what is not an identity.
pub fn mask_uuids(text: &str) -> String {
    const LEN: usize = 36;
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while !rest.is_empty() {
        let candidate = rest.get(..LEN);
        if let Some(candidate) = candidate
            && candidate.as_bytes()[8] == b'-'
            && Uuid::parse_str(candidate).is_ok()
        {
            out.push_str("<uuid>");
            rest = &rest[LEN..];
        } else {
            let next = rest.chars().next().expect("not empty");
            out.push(next);
            rest = &rest[next.len_utf8()..];
        }
    }
    out
}

pub fn uuid_of(value: &Value) -> Uuid {
    value
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(|| panic!("a UUID: {value}"))
}

/// The user ids of a view's members, sorted.
pub fn member_ids(view: &Value) -> Vec<Uuid> {
    let mut ids: Vec<Uuid> = view["members"]
        .as_array()
        .unwrap_or_else(|| panic!("a view with members: {view}"))
        .iter()
        .map(|member| uuid_of(&member["userId"]))
        .collect();
    ids.sort();
    ids
}

pub fn sorted(ids: &[Uuid]) -> Vec<Uuid> {
    let mut ids = ids.to_vec();
    ids.sort();
    ids
}

/// The resources of a view, as `{ key: [ids] }`.
pub fn resources_of(view: &Value) -> BTreeMap<String, Vec<String>> {
    view["resources"]
        .as_array()
        .unwrap_or_else(|| panic!("a view with resources: {view}"))
        .iter()
        .map(|resource| {
            let ids = resource["ids"]
                .as_array()
                .expect("a resource's ids")
                .iter()
                .map(|id| id.as_str().expect("a string id").to_string())
                .collect();
            (resource["key"].as_str().expect("a key").to_string(), ids)
        })
        .collect()
}
