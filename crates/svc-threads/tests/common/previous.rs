//! The previous releases a deployment ran before upgrading to this version,
//! and their published languages — how the messages an upgrade converts were
//! stored, through those releases' own commands and stream, never a row
//! written by the suite:
//!
//! - `svc-threads` 0.4.0, commanded with `contract-threads` 0.4.0 (every
//!   command v1): the messages the conversion to the library's model reads
//!   (T26), and the `message.store` v1 a consumer still on it sends, now
//!   retired (T40). Taken from `THREADS_PREVIOUS_BIN`, or built once.
//! - `svc-threads` 0.10.1: the threads stored before the resources were
//!   published (T38). Its thread commands are this version's. Taken from
//!   `THREADS_LAST_RELEASE_BIN`, or built once.
//! - `svc-threads` 0.11.0, the last release with branches, commanded with
//!   `contract-threads` 0.9.0 (`message.store` v2 naming a parent): the threads
//!   that held branches when they upgrade to the list, and the
//!   `message.store` v2 a consumer still on it sends, now retired (T40). Taken
//!   from `THREADS_BRANCHES_RELEASE_BIN`, or built once.
//!
//! A release's binary is built once from its tag (`cargo install --git`, the
//! repository of this crate).

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use contract_threads_v0_4::llm::{
    AssistantBlock, Signature, StopReason, Text, Thinking, ToolCall, ToolCallId, ToolName,
    ToolResult, ToolResultBlock,
};
use contract_threads_v0_4::{
    AiAuthor, AiDelivery, AiMessage, ArtifactRef, Block, BlockBody, Chunk, ChunkFrame, ChunkPart,
    EditMessage, Item, LibraryEvent, NewMessage, StepItem, StoreMessage, ThreadsCommand,
    ToolResultsItem, UserMessage, seal_hash,
};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use super::commands::{commanding_service, send_v1, send_v2};
use super::stream::push_raw;
use super::{PUSH_TIMEOUT, TestContext, gql};

/// The tag of the release the upgrade starts from.
pub const PREVIOUS_RELEASE_TAG: &str = "svc-threads/v0.4.0";

/// The tag of the last release before this version: the one a deployment runs
/// when it upgrades to this one (T38, the threads it stored before the
/// resources were published).
pub const LAST_RELEASE_TAG: &str = "svc-threads/v0.10.1";

/// The tag of the last release with branches: the one whose threads upgrade
/// to the list (T40).
pub const BRANCHES_RELEASE_TAG: &str = "svc-threads/v0.11.0";

/// The binary of the previous release.
pub fn binary() -> PathBuf {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY
        .get_or_init(|| {
            release_binary(
                PREVIOUS_RELEASE_TAG,
                "svc-threads-0.4.0",
                "THREADS_PREVIOUS_BIN",
            )
        })
        .clone()
}

/// The binary of the last release, built once from its tag, or taken from
/// `THREADS_LAST_RELEASE_BIN`.
pub fn last_release_binary() -> PathBuf {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY
        .get_or_init(|| {
            release_binary(
                LAST_RELEASE_TAG,
                "svc-threads-0.10.1",
                "THREADS_LAST_RELEASE_BIN",
            )
        })
        .clone()
}

/// The binary of the last release with branches, built once from its tag, or
/// taken from `THREADS_BRANCHES_RELEASE_BIN`.
pub fn branches_release_binary() -> PathBuf {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY
        .get_or_init(|| {
            release_binary(
                BRANCHES_RELEASE_TAG,
                "svc-threads-0.11.0",
                "THREADS_BRANCHES_RELEASE_BIN",
            )
        })
        .clone()
}

/// A release's binary: the file `env_var` names, or the release built once
/// from `tag` under the target's temporary directory `dir`.
fn release_binary(tag: &str, dir: &str, env_var: &str) -> PathBuf {
    if let Ok(path) = std::env::var(env_var) {
        let path = PathBuf::from(path);
        assert!(path.is_file(), "{env_var} names no file: {path:?}");
        return path;
    }
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(dir);
    let bin = root.join("bin").join("svc-threads");
    if !bin.is_file() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let built = Command::new(cargo)
            .args([
                "install",
                "--locked",
                "--git",
                env!("CARGO_PKG_REPOSITORY"),
                "--tag",
                tag,
                "--root",
            ])
            .arg(&root)
            .arg("svc-threads")
            .env("CARGO_TARGET_DIR", root.join("target"))
            .status()
            .expect("run cargo install for a release");
        assert!(built.success(), "the release {tag} builds from its tag");
    }
    bin
}

// ── its commands ────────────────────────────────────────────────────────────

/// Sends a v1 command as the commanding service; answers its id, entered in
/// the ledger the teardown checks.
pub async fn command_v1<C: ThreadsCommand + Serialize>(ctx: &TestContext, command: &C) -> Uuid {
    let command_id = send_v1(&ctx.nats, commanding_service(), command).await;
    ctx.sent.lock().expect("the ledger").push(command_id);
    command_id
}

/// Sends a command of 0.11.0's published language (`contract-threads` 0.9.0)
/// as the commanding service; answers its id, entered in the ledger the
/// teardown checks.
pub async fn command_v2<C>(ctx: &TestContext, command: &C) -> Uuid
where
    C: contract_threads_v0_9::ThreadsCommand + Serialize,
{
    let command_id = send_v2(&ctx.nats, commanding_service(), command).await;
    ctx.sent.lock().expect("the ledger").push(command_id);
    command_id
}

/// A `message.store` v2 of 0.11.0 answering `parent`: a complete user message
/// of `author` saying `text`.
pub fn user_message_v2(
    thread: Uuid,
    parent: Option<Uuid>,
    author: Uuid,
    text: &str,
) -> contract_threads_v0_9::StoreMessageV2 {
    let entry = super::content::human(author, vec![super::content::text(text)]);
    contract_threads_v0_9::StoreMessageV2 {
        message_id: Uuid::now_v7(),
        thread_id: thread,
        parent_message_id: parent,
        job_id: None,
        acting_user_id: Some(author),
        impersonator_id: None,
        message: contract_threads_v0_9::MessageTurn::User(contract_threads_v0_9::UserTurn {
            entry: contract_threads_v0_9::LibraryJson::from_json(super::content::to_json(&entry)),
            annotations: Vec::new(),
        }),
    }
}

/// A `message.store` v2 of 0.11.0 answering `parent`: a complete AI message
/// of `agent` answering in `text`, its turn closed completed.
pub fn ai_message_v2(
    thread: Uuid,
    parent: Option<Uuid>,
    agent: &super::content::Agent,
    text: &str,
) -> contract_threads_v0_9::StoreMessageV2 {
    ai_message_v2_attaching(thread, parent, agent, text, None)
}

/// The same, attaching `artifact`, when named, in an ARTIFACT annotation
/// after its answer.
pub fn ai_message_v2_attaching(
    thread: Uuid,
    parent: Option<Uuid>,
    agent: &super::content::Agent,
    text: &str,
    artifact: Option<Uuid>,
) -> contract_threads_v0_9::StoreMessageV2 {
    let message = Uuid::now_v7();
    let replied = super::content::reply(text);
    let annotations = artifact
        .map(|artifact| {
            let placed = super::content::after(&replied[0], super::content::artifact(artifact));
            vec![
                serde_json::from_value(super::content::to_json(&placed))
                    .expect("0.11.0 reads the same placed annotation"),
            ]
        })
        .unwrap_or_default();
    let turn = contract_threads::llm::Turn::from(super::content::ai_turn(
        message,
        agent,
        super::content::closed(replied),
    ));
    contract_threads_v0_9::StoreMessageV2 {
        message_id: message,
        thread_id: thread,
        parent_message_id: parent,
        job_id: None,
        acting_user_id: None,
        impersonator_id: None,
        message: contract_threads_v0_9::MessageTurn::Ai(contract_threads_v0_9::AiTurn {
            agent_id: agent.uuid(),
            author_name: agent.name.clone(),
            delivery: contract_threads_v0_9::AiDelivery::Completed {
                turn: contract_threads_v0_9::LibraryJson::from_json(super::content::to_json(&turn)),
                annotations,
            },
        }),
    }
}

/// The v1 AI author: an opaque id and a name.
pub fn author(id: &str, name: &str) -> AiAuthor {
    AiAuthor {
        id: id.to_string(),
        name: name.to_string(),
    }
}

fn storing(thread: Uuid, parent: Option<Uuid>, message: NewMessage) -> StoreMessage {
    StoreMessage {
        message_id: Uuid::now_v7(),
        thread_id: thread,
        parent_message_id: parent,
        job_id: None,
        acting_user_id: None,
        impersonator_id: None,
        message,
    }
}

/// A v1 user message by `author`, holding `blocks`.
pub fn user_message(
    thread: Uuid,
    parent: Option<Uuid>,
    author: Uuid,
    blocks: Vec<Block>,
) -> StoreMessage {
    let mut store = storing(
        thread,
        parent,
        NewMessage::User(UserMessage {
            author_user_id: author,
            blocks,
        }),
    );
    store.acting_user_id = Some(author);
    store
}

/// A v1 AI message stored COMPLETED, holding `items`.
pub fn ai_message(
    thread: Uuid,
    parent: Option<Uuid>,
    author: AiAuthor,
    items: Vec<Item>,
) -> StoreMessage {
    storing(
        thread,
        parent,
        NewMessage::Ai(AiMessage {
            author,
            delivery: AiDelivery::Completed { items },
        }),
    )
}

/// A v1 AI message opened STREAMING.
pub fn streaming_message(thread: Uuid, parent: Option<Uuid>, author: AiAuthor) -> StoreMessage {
    storing(
        thread,
        parent,
        NewMessage::Ai(AiMessage {
            author,
            delivery: AiDelivery::Streaming,
        }),
    )
}

/// A v1 edit of `message` by `acting`, from `expected_version`, with `blocks`.
pub fn edit_of(
    thread: Uuid,
    message: Uuid,
    acting: Uuid,
    expected_version: u64,
    blocks: Vec<Block>,
) -> EditMessage {
    EditMessage {
        message_id: message,
        thread_id: thread,
        acting_user_id: acting,
        expected_version,
        blocks,
    }
}

// ── its content (`br-llm-messages` 0.5, schema /3) ──────────────────────────

/// A user's text block under `id`.
pub fn user_text_as(id: Uuid, text: &str) -> Block {
    Block::new(
        id,
        BlockBody::user_text(&Text::new(text).expect("a scenario's text is not blank"))
            .expect("a text block serializes"),
    )
}

pub fn user_text(text: &str) -> Block {
    user_text_as(Uuid::now_v7(), text)
}

fn assistant(block: &AssistantBlock) -> Block {
    Block::new(
        Uuid::now_v7(),
        BlockBody::assistant(block).expect("a step block serializes"),
    )
}

pub fn ai_text(text: &str) -> Block {
    assistant(&AssistantBlock::Text {
        text: Text::new(text).expect("a scenario's text is not blank"),
    })
}

/// A step's thinking with its signature.
pub fn thinking(text: &str, signature: &str) -> Block {
    assistant(&AssistantBlock::Thinking(Thinking {
        text: text.to_string(),
        signature: Some(Signature::new(signature).expect("a valid signature")),
    }))
}

/// A step's tool call `id` of `name`.
pub fn tool_call(id: &str, name: &str, arguments: Value) -> Block {
    assistant(&AssistantBlock::ToolCall(ToolCall {
        id: ToolCallId::new(id).expect("a tool call id"),
        name: ToolName::new(name).expect("a tool name"),
        arguments,
    }))
}

/// A `TOOL_RESULT` block answering `call`.
pub fn tool_result(call: &str, name: &str, text: &str) -> Block {
    Block::new(
        Uuid::now_v7(),
        BlockBody::tool_result(&ToolResult::new(
            ToolCallId::new(call).expect("a tool call id"),
            ToolName::new(name).expect("a tool name"),
            vec![ToolResultBlock::text(
                Text::new(text).expect("a result text"),
            )],
            false,
        ))
        .expect("a tool result serializes"),
    )
}

/// An `ARTIFACT` block naming `artifact`.
pub fn artifact_block(artifact: Uuid) -> Block {
    Block::new(
        Uuid::now_v7(),
        BlockBody::Artifact(ArtifactRef {
            artifact_id: artifact,
        }),
    )
}

pub fn step(stop_reason: StopReason, model: Option<&str>, blocks: Vec<Block>) -> Item {
    Item::Step(StepItem {
        stop_reason,
        usage: None,
        model: model.map(str::to_string),
        blocks,
    })
}

pub fn tool_results(blocks: Vec<Block>) -> Item {
    Item::ToolResults(ToolResultsItem { blocks })
}

// ── its stream (`message_content`, step events) ─────────────────────────────

/// The chunks of a v1 step of item `item` that calls `call` of `name`, awaiting
/// its result.
pub fn calling_step(item: u32, call: &str, name: &str) -> Vec<Chunk> {
    use contract_threads_v0_4::llm::StreamEvent;
    let event = |event: StreamEvent, block_id: Option<Uuid>| Chunk {
        item_index: item,
        part: ChunkPart::StepEvent {
            event: LibraryEvent::of(&event).expect("every event of this library is written"),
            block_id,
            model: None,
        },
    };
    vec![
        event(
            StreamEvent::ToolCallStart {
                index: 0,
                id: ToolCallId::new(call).expect("a tool call id"),
                name: ToolName::new(name).expect("a tool name"),
            },
            Some(Uuid::now_v7()),
        ),
        event(
            StreamEvent::ToolCallArgumentsDelta {
                index: 0,
                json_fragment: "{}".to_string(),
            },
            None,
        ),
        event(StreamEvent::BlockEnd { index: 0 }, None),
        event(
            StreamEvent::Finish {
                stop_reason: StopReason::AwaitingToolResults,
                usage: None,
            },
            None,
        ),
    ]
}

/// Publishes v1 `chunks` for `message`, from seq 0; answers the seal.
pub async fn push(nats_url: &str, message: Uuid, chunks: &[Chunk]) -> (u64, String) {
    for (seq, chunk) in chunks.iter().enumerate() {
        let frame = ChunkFrame::new(message, seq as u64, chunk.clone());
        let body = serde_json::to_string(&frame).expect("a frame serializes");
        push_raw(nats_url, &frame.subject(), &frame.msg_id(), &body).await;
    }
    (
        chunks.len() as u64 - 1,
        seal_hash(chunks).expect("the chunks of a scenario serialize"),
    )
}

// ── its reads (0.4.0's SDL) ─────────────────────────────────────────────────

/// One page of a branch, as the previous release served it.
pub const Q_MESSAGES_0_4: &str = "query($thread: UUID!, $size: Int!, $before: UUID, $tip: UUID) { \
    threadsMessages(threadId: $thread, size: $size, before: $before, branchTip: $tip) { \
        id threadId parentMessageId jobId status deleted createdAt editedAt version \
        author { kind userId aiId name } \
        blocks { id index kind content } \
        items { index kind status stopReason usage model blocks { id index kind content } } } }";

/// One page of a branch, as 0.11.0 served it: `branchTip` names the message
/// another branch ends at.
pub const Q_MESSAGES_0_11: &str = "query($thread: UUID!, $size: Int!, $before: UUID, $tip: UUID) { \
    threadsMessages(threadId: $thread, size: $size, before: $before, branchTip: $tip) { \
        id threadId parentMessageId jobId status deleted createdAt editedAt version \
        author { kind userId aiId name } turnKind \
        entries { id index kind state content annotations { id index kind content } } } }";

/// One message, as 0.11.0 served it: the tip of its own branch.
pub async fn message_view_0_11(
    ctx: &TestContext,
    passport: &br_core_auth::Passport,
    thread: Uuid,
    message: Uuid,
) -> Value {
    let answer = ctx
        .run(
            passport,
            Q_MESSAGES_0_11,
            serde_json::json!({ "thread": thread, "size": 1, "before": null, "tip": message }),
        )
        .await;
    let view = gql::data(&answer, "threadsMessages")[0].clone();
    assert_eq!(
        view["id"],
        serde_json::json!(message),
        "0.11.0 reads {message} as the tip of its branch: {answer}"
    );
    view
}

/// One message, as the previous release served it.
pub async fn message_view_0_4(
    ctx: &TestContext,
    passport: &br_core_auth::Passport,
    thread: Uuid,
    message: Uuid,
) -> Value {
    let answer = ctx
        .run(
            passport,
            Q_MESSAGES_0_4,
            serde_json::json!({ "thread": thread, "size": 1, "before": null, "tip": message }),
        )
        .await;
    gql::data(&answer, "threadsMessages")[0].clone()
}

/// Waits until the previous release reads `message` as `holds` demands.
pub async fn await_message_0_4<F>(
    ctx: &TestContext,
    passport: &br_core_auth::Passport,
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
        let view = message_view_0_4(ctx, passport, thread, message).await;
        if holds(&view) {
            return view;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: never held; last read {view}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Every block id of a 0.4 message view, user blocks then every item's.
pub fn block_ids_0_4(view: &Value) -> Vec<Uuid> {
    let mut ids: Vec<Uuid> = view["blocks"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|block| super::flows::uuid_of(&block["id"]))
        .collect();
    for item in view["items"].as_array().into_iter().flatten() {
        ids.extend(
            item["blocks"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|block| super::flows::uuid_of(&block["id"])),
        );
    }
    ids
}
