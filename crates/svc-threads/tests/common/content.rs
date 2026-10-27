//! The conversation a scenario stores, built with `br-llm-messages` 2.0 itself
//! (re-exported by the contract as `contract_threads::llm`): turns of entries,
//! entries of blocks, every id a caller-minted UUIDv7, and the response deltas
//! a writer streams, folded by the library's own draft to know what Threads
//! must show. A builder never decides what is valid — Threads does, with the
//! library's rules; the adversarial paths mutate the library's own JSON.
//!
//! The readers at the end load what a member reads back with the library: a
//! complete entry as the library's entry, a response still produced or
//! interrupted as the library's draft snapshot.

use contract_threads::llm::{
    AITurn, AgentId, AgentMail, AgentRequest, Author, Base64Data, BlockId, BlockStart, Citation,
    CitationId, CitationSource, ClosureReason, ContentBlock, ConversationId, DeltaOp,
    DraftSnapshot, EntryId, Finalization, HumanId, HumanMessage, ImageBlock, ImageSource,
    IncomingEntry, InputTurn, MediaType, ModelName, ModelResponse, OpaqueJson, PlainBlock,
    QueuePosition, RedactedThinkingBlock, ReplayFormat, ReplayMaterial, ReplayPayload, RequestId,
    RequestOrigin, ResponseDelta, ResponseDraft, ResponseEnding, ResponseMetadata, RuntimeToolCall,
    RuntimeToolResult, RuntimeToolResults, StructuredBlock, TaskCompleted, TaskId, TaskStarted,
    TextBlock, ThinkingBlock, ToolName, Turn, TurnClosure, TurnEntry, TurnId,
};
use contract_threads::{
    Annotation, AnnotationBody, ArtifactRef, Chunk, Notice, PlacedAnnotation, ThreadRef,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

// ── ids ─────────────────────────────────────────────────────────────────────

pub fn turn_id(message: Uuid) -> TurnId {
    TurnId::from_uuid(message).expect("a message id is a UUIDv7")
}

pub fn entry_id() -> EntryId {
    EntryId::from_uuid(Uuid::now_v7()).expect("a fresh UUIDv7")
}

pub fn block_id() -> BlockId {
    BlockId::from_uuid(Uuid::now_v7()).expect("a fresh UUIDv7")
}

pub fn human_id(user: Uuid) -> HumanId {
    HumanId::from_uuid(user).expect("a user id is a UUIDv7")
}

pub fn conversation_id(thread: Uuid) -> ConversationId {
    ConversationId::from_uuid(thread).expect("a thread id is a UUIDv7")
}

/// An agent of a scenario: the library's agent id — one stable UUIDv7 per
/// label, so that every turn of "a1" names the same agent — and its name.
#[derive(Debug, Clone, PartialEq)]
pub struct Agent {
    pub id: AgentId,
    pub name: String,
}

impl Agent {
    pub fn uuid(&self) -> Uuid {
        self.id.as_uuid()
    }

    pub fn author(&self) -> Author {
        Author::new(self.name.clone()).expect("a scenario's agent has a name")
    }
}

/// The agent `label`, named `name`.
pub fn agent(label: &str, name: &str) -> Agent {
    let digest = Sha256::digest(label.as_bytes());
    let mut random = [0u8; 10];
    random.copy_from_slice(&digest[..10]);
    let id = uuid::Builder::from_unix_timestamp_millis(1_760_000_000_000, &random).into_uuid();
    Agent {
        id: AgentId::from_uuid(id).expect("a UUIDv7 agent id"),
        name: name.to_string(),
    }
}

// ── plain blocks: what a human message holds ────────────────────────────────

pub fn text(text: &str) -> PlainBlock {
    text_as(Uuid::now_v7(), text)
}

/// A text block under a chosen id.
pub fn text_as(id: Uuid, text: &str) -> PlainBlock {
    let block = TextBlock::new(BlockId::from_uuid(id).expect("a UUIDv7"), text)
        .expect("a scenario's text is not blank");
    PlainBlock::try_from(block).expect("a plain text block")
}

/// A one-pixel PNG, inline.
pub fn image() -> PlainBlock {
    PlainBlock::from(ImageBlock::new(block_id(), png()))
}

fn png() -> ImageSource {
    ImageSource::inline(
        MediaType::Png,
        Base64Data::new(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
        )
        .expect("a standard padded base64"),
    )
}

pub fn structured(value: Value) -> PlainBlock {
    PlainBlock::from(StructuredBlock::new(block_id(), opaque(&value)).expect("a structured value"))
}

// ── content blocks: what a model response holds ─────────────────────────────

pub fn said(text: &str) -> ContentBlock {
    ContentBlock::from(TextBlock::new(block_id(), text).expect("a scenario's text is not blank"))
}

/// A text block carrying a citation of `source`.
pub fn cited(text: &str, source: &str) -> ContentBlock {
    let citation = Citation::new(
        CitationId::from_uuid(Uuid::now_v7()).expect("a UUIDv7"),
        CitationSource::new(source).expect("a citation source"),
    );
    ContentBlock::from(
        TextBlock::new(block_id(), text)
            .expect("a scenario's text is not blank")
            .with_citations(vec![citation])
            .expect("one citation"),
    )
}

pub fn response_image() -> ContentBlock {
    ContentBlock::from(ImageBlock::new(block_id(), png()))
}

/// The replay material a provider signs its thinking with, kept byte for byte.
pub fn signature(value: &str) -> ReplayMaterial {
    ReplayMaterial::new(
        ReplayFormat::new("anthropic.signature").expect("a replay format"),
        ReplayPayload::text(value),
    )
}

/// A thinking and its replay material.
pub fn thinking(text: &str, signed: &str) -> ContentBlock {
    ContentBlock::from(
        ThinkingBlock::new(block_id(), text)
            .expect("a thinking")
            .with_replay(signature(signed)),
    )
}

pub fn redacted_thinking(data: &str) -> ContentBlock {
    ContentBlock::from(RedactedThinkingBlock::new(
        block_id(),
        ReplayMaterial::new(
            ReplayFormat::new("anthropic.redacted_thinking").expect("a replay format"),
            ReplayPayload::text(data),
        ),
    ))
}

/// A runtime tool call of `name` with `arguments`.
pub fn call(name: &str, arguments: Value) -> RuntimeToolCall {
    RuntimeToolCall::new(
        block_id(),
        ToolName::new(name).expect("a tool name"),
        opaque(&arguments),
    )
    .expect("a runtime tool call")
}

/// The runtime tool result answering `call`.
pub fn result_of(call: &RuntimeToolCall, text: &str, is_error: bool) -> RuntimeToolResult {
    RuntimeToolResult::new(
        block_id(),
        call.id(),
        call.name().clone(),
        is_error,
        vec![self::text(text)],
    )
    .expect("a runtime tool result")
}

// ── entries ─────────────────────────────────────────────────────────────────

/// A human message of `user`.
pub fn human(user: Uuid, content: Vec<PlainBlock>) -> TurnEntry {
    TurnEntry::from(
        HumanMessage::new(entry_id(), human_id(user), content).expect("a human message"),
    )
}

/// A model response holding `blocks`, ending `ending`, by `model`.
pub fn response(
    blocks: Vec<ContentBlock>,
    ending: ResponseEnding,
    model: Option<&str>,
) -> TurnEntry {
    TurnEntry::from(model_response(blocks, ending, model))
}

pub fn model_response(
    blocks: Vec<ContentBlock>,
    ending: ResponseEnding,
    model: Option<&str>,
) -> ModelResponse {
    let mut metadata = ResponseMetadata::new();
    if let Some(model) = model {
        metadata = metadata.with_model(ModelName::new(model).expect("a model name"));
    }
    ModelResponse::new(entry_id(), blocks, ending, metadata).expect("a model response")
}

/// A response that calls `calls`, awaiting their results.
pub fn calling(calls: &[&RuntimeToolCall]) -> TurnEntry {
    response(
        calls
            .iter()
            .map(|call| ContentBlock::from((*call).clone()))
            .collect(),
        ResponseEnding::tool_use(),
        None,
    )
}

pub fn results(results: Vec<RuntimeToolResult>) -> TurnEntry {
    TurnEntry::from(RuntimeToolResults::new(entry_id(), results).expect("runtime tool results"))
}

pub fn closure(reason: ClosureReason) -> TurnEntry {
    TurnEntry::from(TurnClosure::new(entry_id(), reason))
}

/// One response answering in `text`, ending the turn — its closure follows.
pub fn reply(text: &str) -> Vec<TurnEntry> {
    vec![response(vec![said(text)], ResponseEnding::finished(), None)]
}

/// An agent's mail, sent by `from` from conversation `source`.
pub fn mail(from: &Agent, source: Uuid, content: Vec<PlainBlock>) -> TurnEntry {
    TurnEntry::from(
        AgentMail::new(
            entry_id(),
            from.id,
            from.author(),
            conversation_id(source),
            content,
        )
        .expect("an agent's mail"),
    )
}

/// A request of `from`, from conversation `source`.
pub fn request(from: &Agent, source: Uuid, content: Vec<PlainBlock>) -> TurnEntry {
    TurnEntry::from(
        AgentRequest::new(
            entry_id(),
            from.id,
            from.author(),
            conversation_id(source),
            RequestOrigin::request(RequestId::from_uuid(Uuid::now_v7()).expect("a UUIDv7")),
            content,
        )
        .expect("an agent's request"),
    )
}

pub fn task_started(task: TaskId) -> TurnEntry {
    TurnEntry::from(TaskStarted::new(
        entry_id(),
        RequestId::from_uuid(Uuid::now_v7()).expect("a UUIDv7"),
        task,
    ))
}

pub fn task_completed(task: TaskId, summary: &str) -> TurnEntry {
    TurnEntry::from(
        TaskCompleted::new(entry_id(), task)
            .with_summary(summary)
            .expect("a task summary"),
    )
}

pub fn queue_position(position: u32) -> TurnEntry {
    TurnEntry::from(
        QueuePosition::new(
            entry_id(),
            RequestId::from_uuid(Uuid::now_v7()).expect("a UUIDv7"),
            position,
        )
        .expect("a queue position"),
    )
}

pub fn task() -> TaskId {
    TaskId::from_uuid(Uuid::now_v7()).expect("a UUIDv7")
}

// ── turns ───────────────────────────────────────────────────────────────────

/// The AI turn of `agent` under `message`'s id, holding `entries` as given.
pub fn ai_turn(message: Uuid, agent: &Agent, entries: Vec<TurnEntry>) -> AITurn {
    AITurn::from_parts(turn_id(message), agent.id, agent.author(), entries)
        .expect("a turn the library accepts")
}

/// `entries` closed completed: the turn a writer stores whole.
pub fn closed(mut entries: Vec<TurnEntry>) -> Vec<TurnEntry> {
    entries.push(closure(ClosureReason::completed()));
    entries
}

/// The input turn under `message`'s id holding `entries`.
pub fn input_turn(message: Uuid, entries: Vec<TurnEntry>) -> InputTurn {
    let incoming = entries
        .into_iter()
        .map(|entry| IncomingEntry::try_from(entry).expect("an incoming entry"))
        .collect();
    InputTurn::new(turn_id(message), incoming).expect("an input turn the library accepts")
}

// ── annotations ─────────────────────────────────────────────────────────────

pub fn artifact(artifact: Uuid) -> Annotation {
    Annotation {
        id: Uuid::now_v7(),
        body: AnnotationBody::Artifact(ArtifactRef {
            artifact_id: artifact,
        }),
    }
}

pub fn warning(code: &str, text: &str) -> Annotation {
    Annotation {
        id: Uuid::now_v7(),
        body: AnnotationBody::Warning(Notice {
            code: code.to_string(),
            text: Some(text.to_string()),
        }),
    }
}

pub fn error_note(code: &str) -> Annotation {
    Annotation {
        id: Uuid::now_v7(),
        body: AnnotationBody::Error(Notice {
            code: code.to_string(),
            text: None,
        }),
    }
}

pub fn thread_reference(thread: Uuid) -> Annotation {
    Annotation {
        id: Uuid::now_v7(),
        body: AnnotationBody::ThreadReference(ThreadRef { thread_id: thread }),
    }
}

pub fn after(entry: &TurnEntry, annotation: Annotation) -> PlacedAnnotation {
    PlacedAnnotation {
        after_entry_id: entry.id().as_uuid(),
        annotation,
    }
}

// ── the stream: response deltas, folded by the library's draft ──────────────

/// One model response a writer streams into `message`: the deltas it pushes,
/// and the library's draft folding them — what Threads must show.
pub struct Streamed {
    pub turn: TurnId,
    pub entry: EntryId,
    deltas: Vec<ResponseDelta>,
    draft: ResponseDraft,
}

impl Streamed {
    /// A response opened under a fresh entry id, by `model`.
    pub fn start(message: Uuid, model: Option<&str>) -> Self {
        let (turn, entry) = (turn_id(message), entry_id());
        let mut streamed = Self {
            turn,
            entry,
            deltas: Vec::new(),
            draft: ResponseDraft::new(turn, entry),
        };
        let mut metadata = ResponseMetadata::new();
        if let Some(model) = model {
            metadata = metadata.with_model(ModelName::new(model).expect("a model name"));
        }
        streamed.push(DeltaOp::response_started(metadata));
        streamed
    }

    /// Appends one delta; the library's draft must accept it.
    pub fn push(&mut self, op: DeltaOp) -> ResponseDelta {
        let delta = ResponseDelta::new(self.turn, self.entry, op);
        self.draft
            .apply(&delta)
            .unwrap_or_else(|e| panic!("the scenario's delta folds in the library: {e:?}"));
        self.deltas.push(delta.clone());
        delta
    }

    /// A delta the library refuses: kept out of the draft, sent all the same.
    pub fn refused(&self, op: DeltaOp) -> ResponseDelta {
        let delta = ResponseDelta::new(self.turn, self.entry, op);
        assert!(
            self.draft.clone().apply(&delta).is_err(),
            "the library refuses this delta"
        );
        delta
    }

    /// Opens a text block and grows it with `parts`; finalizes it when `close`.
    pub fn text(&mut self, parts: &[&str], close: bool) -> BlockId {
        let block = block_id();
        self.push(DeltaOp::block_started(block, BlockStart::text()));
        for part in parts {
            self.push(DeltaOp::text_appended(block, *part));
        }
        if close {
            self.push(DeltaOp::block_finalized(block, Finalization::accumulated()));
        }
        block
    }

    /// A runtime tool call of `name`, its arguments in `fragments`, finalized.
    pub fn call(&mut self, name: &str, fragments: &[&str]) -> BlockId {
        let block = block_id();
        self.push(DeltaOp::block_started(
            block,
            BlockStart::runtime_tool_call(Some(ToolName::new(name).expect("a tool name")), None),
        ));
        for fragment in fragments {
            self.push(DeltaOp::json_appended(block, *fragment));
        }
        self.push(DeltaOp::block_finalized(block, Finalization::accumulated()));
        block
    }

    pub fn complete(&mut self, ending: ResponseEnding) {
        self.push(DeltaOp::response_completed(ending));
    }

    pub fn deltas(&self) -> &[ResponseDelta] {
        &self.deltas
    }

    /// Every delta, as the chunks a writer pushes.
    pub fn chunks(&self) -> Vec<Chunk> {
        self.deltas.iter().map(delta_chunk).collect()
    }

    /// The library's draft after every delta.
    pub fn draft(&self) -> &ResponseDraft {
        &self.draft
    }

    /// The library's draft after the first `count` deltas.
    pub fn draft_after(&self, count: usize) -> ResponseDraft {
        let mut draft = ResponseDraft::new(self.turn, self.entry);
        for delta in &self.deltas[..count] {
            draft.apply(delta).expect("folded once already");
        }
        draft
    }

    /// The completed response, as the library folded it.
    pub fn response(&self) -> ModelResponse {
        self.draft
            .response()
            .cloned()
            .expect("the scenario completed this response")
    }

    /// The runtime call `block` of the completed response.
    pub fn runtime_call(&self, block: BlockId) -> RuntimeToolCall {
        self.response()
            .runtime_calls()
            .find(|call| call.id() == block)
            .cloned()
            .expect("the response holds that call")
    }
}

pub fn delta_chunk(delta: &ResponseDelta) -> Chunk {
    Chunk::Delta {
        delta: to_json(delta),
    }
}

/// A whole entry, as one chunk.
pub fn entry_chunk(entry: &TurnEntry) -> Chunk {
    Chunk::Entry {
        entry: to_json(entry),
    }
}

/// A whole annotation after `entry`, as one chunk.
pub fn annotation_chunk(entry: EntryId, annotation: Annotation) -> Chunk {
    Chunk::Annotation {
        after_entry_id: entry.as_uuid(),
        annotation,
    }
}

// ── JSON ────────────────────────────────────────────────────────────────────

/// The library's JSON for `value`, by the library's `Serialize`.
pub fn to_json<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("a library value serializes")
}

pub fn opaque(value: &Value) -> OpaqueJson {
    OpaqueJson::from_serialize(value).expect("a JSON value")
}

/// The library's error code for `json` loaded as a `/5` turn, if it refuses
/// it: what a `MESSAGE_INVALID` refusal of that turn names.
pub fn turn_refusal(json: &Value) -> Option<String> {
    Turn::from_value(opaque(json))
        .err()
        .map(|error| error.code().as_str().to_string())
}

/// The library's error code for `json` loaded as a turn entry, if it refuses it.
pub fn entry_refusal(json: &Value) -> Option<String> {
    TurnEntry::from_value(opaque(json))
        .err()
        .map(|error| error.code().as_str().to_string())
}

// ── reading what a member reads ─────────────────────────────────────────────

/// The entries of a message view, in order.
pub fn entries_of(view: &Value) -> Vec<Value> {
    view["entries"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| panic!("a message view with entries: {view}"))
}

/// The entry `id` of a message view.
pub fn entry_view(view: &Value, id: EntryId) -> Value {
    entries_of(view)
        .into_iter()
        .find(|entry| entry["id"] == json!(id.as_uuid()))
        .unwrap_or_else(|| panic!("entry {} in {view}", id.as_uuid()))
}

/// A complete entry, loaded by the library.
pub fn complete_entry(entry: &Value) -> TurnEntry {
    assert_eq!(entry["state"], "COMPLETE", "a complete entry: {entry}");
    serde_json::from_value(entry["content"].clone())
        .unwrap_or_else(|e| panic!("the library loads the entry ({e}): {entry}"))
}

/// A response still produced or interrupted, loaded by the library as the
/// draft its snapshot restores.
pub fn draft_entry(entry: &Value) -> ResponseDraft {
    assert!(
        entry["state"] == "PRODUCING" || entry["state"] == "INTERRUPTED",
        "a response draft: {entry}"
    );
    assert_eq!(entry["kind"], "model_response", "{entry}");
    let snapshot: DraftSnapshot = serde_json::from_value(entry["content"].clone())
        .unwrap_or_else(|e| panic!("the library loads the draft snapshot ({e}): {entry}"));
    ResponseDraft::from_snapshot(&snapshot)
        .unwrap_or_else(|e| panic!("the library restores the draft ({e:?}): {entry}"))
}

/// The text a draft holds so far in its first text slot.
pub fn draft_text(draft: &ResponseDraft) -> String {
    draft
        .view()
        .slots()
        .iter()
        .find_map(|slot| slot.text().map(str::to_string))
        .unwrap_or_default()
}

/// The library's entry kinds of a message view, in order.
pub fn kinds_of(view: &Value) -> Vec<String> {
    entries_of(view)
        .iter()
        .map(|entry| entry["kind"].as_str().expect("an entry kind").to_string())
        .collect()
}

/// The message as the library's turn, rebuilt from its complete entries by the
/// library's own rules — a message the library cannot load fails here.
pub fn turn_of(view: &Value) -> Turn {
    let id = turn_id(super::flows::uuid_of(&view["id"]));
    let entries: Vec<TurnEntry> = entries_of(view)
        .iter()
        .filter(|entry| entry["state"] == "COMPLETE")
        .map(complete_entry)
        .collect();
    match view["turnKind"].as_str() {
        Some("INPUT_TURN") => Turn::from(
            InputTurn::new(
                id,
                entries
                    .into_iter()
                    .map(|entry| IncomingEntry::try_from(entry).expect("an incoming entry"))
                    .collect(),
            )
            .unwrap_or_else(|e| panic!("the library loads the input turn ({e:?}): {view}")),
        ),
        Some("AI_TURN") => {
            let agent = AgentId::parse(view["author"]["aiId"].as_str().expect("an agent id"))
                .expect("the agent id is the library's");
            let author = Author::new(view["author"]["name"].as_str().expect("an agent name"))
                .expect("the agent's name");
            Turn::from(
                AITurn::from_parts(id, agent, author, entries)
                    .unwrap_or_else(|e| panic!("the library loads the AI turn ({e:?}): {view}")),
            )
        }
        other => panic!("a turn kind, got {other:?}: {view}"),
    }
}

/// The closure of a message view's turn, if closed.
pub fn closure_of(view: &Value) -> Option<ClosureReason> {
    entries_of(view)
        .iter()
        .filter(|entry| entry["kind"] == "turn_closure")
        .map(complete_entry)
        .find_map(|entry| match entry {
            TurnEntry::TurnClosure(closure) => Some(closure.reason().clone()),
            _ => None,
        })
}

/// The closure's reason type: `completed`, `failed`, `cancelled`,
/// `interrupted`, ...
pub fn closure_kind(view: &Value) -> Option<String> {
    closure_of(view).map(|reason| to_json(&reason)["type"].as_str().unwrap_or("").to_string())
}

/// Every text of a message view's complete entries, in order: a human
/// message's content, a response's text blocks.
pub fn texts_of(view: &Value) -> Vec<String> {
    let mut texts = Vec::new();
    for entry in entries_of(view)
        .iter()
        .filter(|entry| entry["state"] == "COMPLETE")
    {
        let json = &entry["content"];
        let blocks = json["content"].as_array().cloned().unwrap_or_default();
        texts.extend(
            blocks
                .iter()
                .filter(|block| block["type"] == "text")
                .filter_map(|block| block["text"].as_str().map(str::to_string)),
        );
    }
    texts
}

/// The annotations of a message view, in order, as `(entry, annotation)`.
pub fn annotations_of(view: &Value) -> Vec<(Uuid, Value)> {
    entries_of(view)
        .iter()
        .flat_map(|entry| {
            let after = super::flows::uuid_of(&entry["id"]);
            entry["annotations"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(move |annotation| (after, annotation))
        })
        .collect()
}

/// An annotation as a member reads it.
pub fn annotation_view(annotation: &Annotation) -> Value {
    let wire = to_json(annotation);
    json!({ "id": wire["id"], "kind": wire["kind"], "content": wire["content"] })
}

/// Whether `turn` is the library's: equal to `expected`, entry for entry.
pub fn assert_same_turn(read: &Turn, expected: &Turn, what: &str) {
    assert_eq!(
        to_json(read),
        to_json(expected),
        "{what}: the turn reads as the library's turn it was stored as"
    );
}
