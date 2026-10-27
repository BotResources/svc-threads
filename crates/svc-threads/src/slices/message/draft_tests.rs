use contract_threads::llm::{
    BlockStart, ClosureReason, ContentBlock, DeltaOp, Finalization, HumanId, HumanMessage,
    PlainBlock, ResponseEnding, ResponseMetadata, RuntimeToolResult, RuntimeToolResults, TextBlock,
    ToolName, TurnEntry,
};
use contract_threads::{Annotation, AnnotationBody, ArtifactRef, LibraryJson};

use contract_threads::llm::ModelResponse;

use super::*;

fn v7() -> Uuid {
    Uuid::now_v7()
}

/// A writer pushing chunks on one message's stream, numbered from 0.
struct Writer {
    draft: TurnDraft,
    seq: u64,
    turn: TurnId,
}

impl Writer {
    fn new() -> Self {
        Self {
            draft: TurnDraft::default(),
            seq: 0,
            turn: TurnId::from_uuid(v7()).unwrap(),
        }
    }

    fn push(&mut self, chunk: Chunk) -> &mut Self {
        fold(
            &mut self.draft,
            self.seq,
            serde_json::to_value(chunk).unwrap(),
        );
        self.seq += 1;
        self
    }

    fn delta(&mut self, entry: EntryId, op: DeltaOp) -> &mut Self {
        let chunk = Chunk::delta(&ResponseDelta::new(self.turn, entry, op)).unwrap();
        self.push(chunk)
    }

    fn entry(&mut self, entry: TurnEntry) -> &mut Self {
        self.push(Chunk::entry(&entry).unwrap())
    }

    /// A response whose one text block grows from `parts`, completed when
    /// `ending` is given.
    fn text_response(&mut self, parts: &[&str], ending: Option<ResponseEnding>) -> EntryId {
        let entry = EntryId::from_uuid(v7()).unwrap();
        let block = BlockId::from_uuid(v7()).unwrap();
        self.delta(entry, DeltaOp::response_started(ResponseMetadata::new()));
        self.delta(entry, DeltaOp::block_started(block, BlockStart::text()));
        for part in parts {
            self.delta(entry, DeltaOp::text_appended(block, *part));
        }
        if let Some(ending) = ending {
            self.delta(
                entry,
                DeltaOp::block_finalized(block, Finalization::accumulated()),
            );
            self.delta(entry, DeltaOp::response_completed(ending));
        }
        entry
    }

    /// A response ending on a runtime call of `search`; answers the call's id.
    fn calling_response(&mut self) -> BlockId {
        let entry = EntryId::from_uuid(v7()).unwrap();
        let call = BlockId::from_uuid(v7()).unwrap();
        self.delta(entry, DeltaOp::response_started(ResponseMetadata::new()));
        self.delta(
            entry,
            DeltaOp::block_started(
                call,
                BlockStart::runtime_tool_call(Some(ToolName::new("search").unwrap()), None),
            ),
        );
        self.delta(entry, DeltaOp::json_appended(call, r#"{"q":"rust"}"#));
        self.delta(
            entry,
            DeltaOp::block_finalized(call, Finalization::accumulated()),
        );
        self.delta(
            entry,
            DeltaOp::response_completed(ResponseEnding::tool_use()),
        );
        call
    }

    fn results(&mut self, call: BlockId) -> &mut Self {
        let result = RuntimeToolResult::new(
            BlockId::from_uuid(v7()).unwrap(),
            call,
            ToolName::new("search").unwrap(),
            false,
            vec![
                PlainBlock::try_from(
                    TextBlock::new(BlockId::from_uuid(v7()).unwrap(), "found").unwrap(),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let entry =
            RuntimeToolResults::new(EntryId::from_uuid(v7()).unwrap(), vec![result]).unwrap();
        self.entry(entry.into())
    }

    fn judged(&self) -> Judged {
        self.judged_holding(&HashSet::new())
    }

    fn judged_holding(&self, held: &HashSet<Uuid>) -> Judged {
        self.draft
            .judge(
                self.turn,
                AgentId::from_uuid(v7()).unwrap(),
                &Author::new("Analyst").unwrap(),
                held,
            )
            .unwrap()
    }

    fn closed(&self, kind: CloseKind) -> Closed {
        close(self.judged(), kind).unwrap()
    }
}

fn kinds(content: &Content) -> Vec<(&'static str, &'static str)> {
    content
        .entries
        .iter()
        .map(|entry| (entry.kind().as_str(), entry.state().as_str()))
        .collect()
}

fn closure_of(content: &Content) -> serde_json::Value {
    let closure = content
        .entries
        .last()
        .expect("a closed turn holds its closure");
    serde_json::to_value(closure.payload()).unwrap()["reason"]["type"].clone()
}

#[test]
fn a_streamed_response_folds_into_the_turn_and_a_completion_closes_it_completed() {
    let mut writer = Writer::new();
    writer.text_response(&["Hel", "lo"], Some(ResponseEnding::finished()));
    assert_eq!(writer.draft.refusal(), None);
    let judged = writer.judged();
    assert_eq!(judged.turn.entries().len(), 1);
    assert_eq!(judged.producing(), None);
    let closed = writer.closed(CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Completed);
    assert_eq!(closed.reason, None);
    assert_eq!(
        kinds(&closed.content),
        [("model_response", "complete"), ("turn_closure", "complete")]
    );
    assert_eq!(closure_of(&closed.content), "completed");
    let closure = closed.content.entries.last().unwrap();
    assert_eq!(
        closure.id().as_uuid().get_version_num(),
        7,
        "a UUIDv7 Threads mints"
    );
}

#[test]
fn a_tool_call_answered_by_a_whole_results_entry_then_an_answer_completes() {
    let mut writer = Writer::new();
    let call = writer.calling_response();
    writer.results(call);
    writer.text_response(&["done"], Some(ResponseEnding::finished()));
    assert_eq!(writer.draft.refusal(), None);
    let closed = writer.closed(CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Completed);
    assert_eq!(
        kinds(&closed.content),
        [
            ("model_response", "complete"),
            ("runtime_tool_results", "complete"),
            ("model_response", "complete"),
            ("turn_closure", "complete")
        ]
    );
}

#[test]
fn a_response_still_produced_is_shown_and_kept_interrupted_when_the_message_fails() {
    let mut writer = Writer::new();
    writer.text_response(&["half an ans"], None);
    let judged = writer.judged();
    assert!(judged.producing().is_some(), "members see the draft");
    let shown = judged.content().unwrap();
    assert_eq!(kinds(&shown), [("model_response", "producing")]);
    let closed = writer.closed(CloseKind::Fail);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(closed.reason, Some(reason::FAILED_BY_WRITER));
    assert_eq!(
        kinds(&closed.content),
        [
            ("model_response", "interrupted"),
            ("turn_closure", "complete")
        ]
    );
    assert_eq!(closure_of(&closed.content), "failed");
    let (entries, blocks) = closed.content.complete_rows();
    assert_eq!(
        entries.len(),
        1,
        "a runner never receives the interrupted response"
    );
    assert!(blocks.is_empty());
}

#[test]
fn a_silent_writer_closes_its_turn_interrupted() {
    let mut writer = Writer::new();
    writer.text_response(&["so"], None);
    let closed = writer.closed(CloseKind::Watch);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(closed.reason, Some(reason::STREAM_IDLE));
    assert_eq!(closure_of(&closed.content), "interrupted");
}

#[test]
fn a_delta_its_response_refuses_fails_the_stream_and_nothing_after_it_folds() {
    let mut writer = Writer::new();
    let entry = writer.text_response(&["a"], None);
    writer.delta(
        entry,
        DeltaOp::text_appended(BlockId::from_uuid(v7()).unwrap(), "never opened"),
    );
    let refusal = writer
        .draft
        .refusal()
        .cloned()
        .expect("the library refuses it");
    assert_eq!(refusal.code, "stream_target_error");
    assert_eq!(refusal.seq, Some(3));
    let before = writer.draft.clone();
    writer.text_response(&["more"], Some(ResponseEnding::finished()));
    assert_eq!(writer.draft, before);
    let closed = writer.closed(CloseKind::Watch);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(closed.reason, Some("stream_target_error"));
    assert_eq!(closure_of(&closed.content), "failed");
}

#[test]
fn a_completion_with_a_call_unanswered_fails_and_no_result_is_fabricated() {
    let mut writer = Writer::new();
    writer.calling_response();
    let closed = writer.closed(CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(closed.reason, Some("unresolved_execution"));
    assert_eq!(
        kinds(&closed.content),
        [("model_response", "complete"), ("turn_closure", "complete")]
    );
    let cancelled = writer.closed(CloseKind::Cancel);
    assert_eq!(cancelled.status, MessageStatus::Cancelled);
    assert_eq!(closure_of(&cancelled.content), "cancelled");
}

#[test]
fn a_completion_while_a_response_is_produced_fails() {
    let mut writer = Writer::new();
    writer.text_response(&["not yet"], None);
    let closed = writer.closed(CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(closed.reason, Some(reason::RESPONSE_STILL_PRODUCING));
    assert_eq!(closed.content.entries[0].state().as_str(), "interrupted");
}

#[test]
fn a_response_a_closure_or_an_entry_pushed_mid_response_are_refused() {
    let response = ModelResponse::new(
        EntryId::from_uuid(v7()).unwrap(),
        vec![ContentBlock::Text(
            TextBlock::new(BlockId::from_uuid(v7()).unwrap(), "whole").unwrap(),
        )],
        ResponseEnding::finished(),
        ResponseMetadata::new(),
    )
    .unwrap();
    let mut writer = Writer::new();
    writer.entry(response.into());
    assert_eq!(
        writer.draft.refusal().unwrap().code,
        "response_in_entry_chunk"
    );

    let mut writer = Writer::new();
    writer.entry(
        TurnClosure::new(
            EntryId::from_uuid(v7()).unwrap(),
            ClosureReason::completed(),
        )
        .into(),
    );
    assert_eq!(writer.draft.refusal().unwrap().code, "closure_by_writer");

    let mut writer = Writer::new();
    writer.text_response(&["mid"], None);
    let human = HumanMessage::new(
        EntryId::from_uuid(v7()).unwrap(),
        HumanId::from_uuid(v7()).unwrap(),
        vec![
            PlainBlock::try_from(TextBlock::new(BlockId::from_uuid(v7()).unwrap(), "hey").unwrap())
                .unwrap(),
        ],
    )
    .unwrap();
    writer.entry(human.into());
    assert_eq!(
        writer.draft.refusal().unwrap().code,
        "response_still_producing"
    );
}

#[test]
fn an_annotation_follows_an_entry_the_stream_holds() {
    let mut writer = Writer::new();
    let answer = writer.text_response(&["see the file"], Some(ResponseEnding::finished()));
    let artifact = Annotation::new(
        v7(),
        AnnotationBody::Artifact(ArtifactRef { artifact_id: v7() }),
    );
    writer.push(Chunk::annotation(answer.as_uuid(), artifact.clone()));
    assert_eq!(writer.draft.refusal(), None);
    let closed = writer.closed(CloseKind::Complete);
    assert_eq!(
        closed.content.annotations,
        vec![PlacedAnnotation::new(answer.as_uuid(), artifact.clone())]
    );
    assert_eq!(
        closed.content.artifacts(),
        vec![artifact.body.artifact().unwrap()]
    );

    let mut writer = Writer::new();
    writer.push(Chunk::annotation(
        v7(),
        Annotation::new(
            v7(),
            AnnotationBody::Artifact(ArtifactRef { artifact_id: v7() }),
        ),
    ));
    assert_eq!(
        writer.draft.refusal().unwrap().code,
        "annotation_entry_unknown"
    );
}

#[test]
fn the_judgement_under_the_message_cuts_at_an_id_another_message_holds() {
    let mut writer = Writer::new();
    let call = writer.calling_response();
    writer.results(call);
    let answer = writer.text_response(&["second"], Some(ResponseEnding::finished()));
    assert_eq!(writer.draft.refusal(), None);
    let held = HashSet::from([answer.as_uuid()]);
    let judged = writer.judged_holding(&held);
    assert_eq!(judged.turn.entries().len(), 2);
    let refusal = judged.refusal.clone().unwrap();
    assert_eq!(refusal.code, "reused_identity");
    assert_eq!(refusal.object_id, Some(answer.as_uuid()));
    let closed = close(judged, CloseKind::Complete).unwrap();
    assert_eq!(closed.status, MessageStatus::Failed);
}

#[test]
fn deltas_naming_another_turn_than_the_message_are_cut_by_the_judgement() {
    let mut writer = Writer::new();
    writer.text_response(&["elsewhere"], Some(ResponseEnding::finished()));
    let judged = writer
        .draft
        .judge(
            TurnId::from_uuid(v7()).unwrap(),
            AgentId::from_uuid(v7()).unwrap(),
            &Author::new("Analyst").unwrap(),
            &HashSet::new(),
        )
        .unwrap();
    assert!(judged.turn.entries().is_empty());
    assert!(judged.refusal.is_some());
}

#[test]
fn an_unreadable_chunk_is_refused() {
    let mut draft = TurnDraft::default();
    fold(&mut draft, 0, serde_json::json!({ "kind": "STEP_EVENT" }));
    assert_eq!(draft.refusal().unwrap().code, "chunk_unreadable");
    let mut draft = TurnDraft::default();
    let chunk = Chunk::Entry {
        entry: LibraryJson::from_json(serde_json::json!({ "type": "task_started" })),
    };
    fold(&mut draft, 0, serde_json::to_value(chunk).unwrap());
    assert_eq!(draft.refusal().unwrap().code, "invalid_content");
}

#[test]
fn a_cancel_keeps_the_results_of_one_call_of_two_and_answers_none_for_the_other() {
    let mut writer = Writer::new();
    let entry = EntryId::from_uuid(v7()).unwrap();
    writer.delta(entry, DeltaOp::response_started(ResponseMetadata::new()));
    let mut calls = Vec::new();
    for query in [r#"{"q":"one"}"#, r#"{"q":"two"}"#] {
        let call = BlockId::from_uuid(v7()).unwrap();
        writer.delta(
            entry,
            DeltaOp::block_started(
                call,
                BlockStart::runtime_tool_call(Some(ToolName::new("search").unwrap()), None),
            ),
        );
        writer.delta(entry, DeltaOp::json_appended(call, query));
        writer.delta(
            entry,
            DeltaOp::block_finalized(call, Finalization::accumulated()),
        );
        calls.push(call);
    }
    writer.delta(
        entry,
        DeltaOp::response_completed(ResponseEnding::tool_use()),
    );
    writer.results(calls[0]);
    assert_eq!(writer.draft.refusal(), None);
    let closed = writer.closed(CloseKind::Cancel);
    assert_eq!(closed.status, MessageStatus::Cancelled);
    assert_eq!(
        kinds(&closed.content),
        [
            ("model_response", "complete"),
            ("runtime_tool_results", "complete"),
            ("turn_closure", "complete")
        ]
    );
    assert_eq!(closure_of(&closed.content), "cancelled");
}

#[test]
fn an_id_a_whole_entry_declares_deep_in_its_blocks_is_read_off_its_records() {
    let mut writer = Writer::new();
    let call = writer.calling_response();
    let child = BlockId::from_uuid(v7()).unwrap();
    let result = RuntimeToolResult::new(
        BlockId::from_uuid(v7()).unwrap(),
        call,
        ToolName::new("search").unwrap(),
        false,
        vec![PlainBlock::try_from(TextBlock::new(child, "found").unwrap()).unwrap()],
    )
    .unwrap();
    let results = RuntimeToolResults::new(EntryId::from_uuid(v7()).unwrap(), vec![result]).unwrap();
    writer.entry(results.into());
    assert_eq!(writer.draft.refusal(), None);
    let judged = writer.judged_holding(&HashSet::from([child.as_uuid()]));
    assert_eq!(judged.turn.entries().len(), 1, "cut at the results entry");
    let refusal = judged.refusal.unwrap();
    assert_eq!(refusal.code, "reused_identity");
    assert_eq!(refusal.object_id, Some(child.as_uuid()));
}

#[test]
fn a_chunk_holding_nul_is_refused_and_nothing_after_it_folds() {
    let mut writer = Writer::new();
    writer.text_response(&["fine"], Some(ResponseEnding::finished()));
    let entry = EntryId::from_uuid(v7()).unwrap();
    let block = BlockId::from_uuid(v7()).unwrap();
    writer.delta(entry, DeltaOp::response_started(ResponseMetadata::new()));
    writer.delta(entry, DeltaOp::block_started(block, BlockStart::text()));
    writer.delta(entry, DeltaOp::text_appended(block, "a\u{0}b"));
    let refusal = writer.draft.refusal().cloned().unwrap();
    assert_eq!(refusal.code, "nul_character");
    assert_eq!(refusal.seq, Some(7), "the eighth chunk");
    let closed = writer.closed(CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert!(
        !serde_json::to_string(&closed.content.blocks)
            .unwrap()
            .contains("\\u0000"),
        "what the close stores holds no U+0000"
    );
}
