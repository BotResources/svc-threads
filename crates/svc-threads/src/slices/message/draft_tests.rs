use contract_threads::llm::{
    AssistantBlock, BlockKind as StreamKind, StopReason, StreamEvent, ToolCallId, ToolName,
    ToolResult, ToolResultBlock,
};
use contract_threads::{
    ArtifactRef, Block, BlockBody, Chunk, ChunkPart, LibraryEvent, MessageInvalidError,
    MessageStatus,
};

use super::*;

fn lib(event: StreamEvent) -> LibraryEvent {
    LibraryEvent::of(&event).unwrap()
}

/// Closes a stream alone on its branch.
fn close_alone(draft: &MessageDraft, kind: CloseKind) -> Closed {
    close(draft, kind, &UsedCallIds::new())
}

/// A writer pushing chunks, numbered from 0.
#[derive(Default)]
struct Writer {
    draft: MessageDraft,
    seq: u64,
}

impl Writer {
    fn push(&mut self, item_index: u32, part: ChunkPart) -> &mut Self {
        let chunk = Chunk { item_index, part };
        fold(
            &mut self.draft,
            self.seq,
            serde_json::to_value(chunk).unwrap(),
        );
        self.seq += 1;
        self
    }

    fn event(&mut self, item: u32, event: StreamEvent) -> &mut Self {
        let block_id = matches!(
            event,
            StreamEvent::BlockStart { .. } | StreamEvent::ToolCallStart { .. }
        )
        .then(Uuid::now_v7);
        self.push(
            item,
            ChunkPart::StepEvent {
                event: lib(event),
                block_id,
                model: None,
            },
        )
    }

    fn text_block(&mut self, item: u32, index: usize, parts: &[&str]) -> &mut Self {
        self.event(
            item,
            StreamEvent::BlockStart {
                index,
                kind: StreamKind::Text,
            },
        );
        for part in parts {
            self.event(
                item,
                StreamEvent::TextDelta {
                    index,
                    text: part.to_string(),
                },
            );
        }
        self.event(item, StreamEvent::BlockEnd { index })
    }

    fn tool_call(&mut self, item: u32, index: usize, id: &str, arguments: &str) -> &mut Self {
        self.event(
            item,
            StreamEvent::ToolCallStart {
                index,
                id: ToolCallId::new(id).unwrap(),
                name: ToolName::new("search").unwrap(),
            },
        );
        self.event(
            item,
            StreamEvent::ToolCallArgumentsDelta {
                index,
                json_fragment: arguments.to_string(),
            },
        );
        self.event(item, StreamEvent::BlockEnd { index })
    }

    fn finish(&mut self, item: u32, stop_reason: StopReason) -> &mut Self {
        self.event(
            item,
            StreamEvent::Finish {
                stop_reason,
                usage: None,
            },
        )
    }

    fn results(&mut self, item: u32, answered: &[&str]) -> &mut Self {
        let blocks = answered
            .iter()
            .map(|id| {
                Block::new(
                    Uuid::now_v7(),
                    BlockBody::tool_result(&ToolResult::new(
                        ToolCallId::new(*id).unwrap(),
                        ToolName::new("search").unwrap(),
                        vec![ToolResultBlock::text(Text::new("found").unwrap())],
                        false,
                    ))
                    .unwrap(),
                )
            })
            .collect();
        self.push(item, ChunkPart::ToolResults { blocks })
    }

    fn artifact(&mut self, item: u32) -> &mut Self {
        self.push(
            item,
            ChunkPart::Block {
                block: Block::new(
                    Uuid::now_v7(),
                    BlockBody::Artifact(ArtifactRef {
                        artifact_id: Uuid::now_v7(),
                    }),
                ),
            },
        )
    }
}

fn library_blocks(item: &StoredItem) -> Vec<AssistantBlock> {
    item.blocks
        .iter()
        .filter(|block| block.kind.is_library())
        .map(|block| serde_json::from_value(block.content.clone()).unwrap())
        .collect()
}

#[test]
fn a_step_folds_as_it_streams_and_completes_on_its_finish() {
    let mut writer = Writer::default();
    writer
        .event(
            0,
            StreamEvent::BlockStart {
                index: 0,
                kind: StreamKind::Text,
            },
        )
        .event(
            0,
            StreamEvent::TextDelta {
                index: 0,
                text: "Hel".into(),
            },
        );
    let preview = writer.draft.preview();
    assert_eq!(preview.len(), 1);
    assert_eq!(preview[0].status, MessageStatus::Streaming);
    assert_eq!(preview[0].blocks[0].content["text"], "Hel");

    writer
        .event(
            0,
            StreamEvent::TextDelta {
                index: 0,
                text: "lo".into(),
            },
        )
        .event(0, StreamEvent::BlockEnd { index: 0 })
        .finish(0, StopReason::EndTurn);
    assert_eq!(writer.draft.refusal(), None);
    let preview = writer.draft.preview();
    assert_eq!(preview[0].status, MessageStatus::Completed);
    assert_eq!(
        library_blocks(&preview[0]),
        vec![AssistantBlock::Text {
            text: Text::new("Hello").unwrap()
        }]
    );

    let closed = close_alone(&writer.draft, CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Completed);
    assert_eq!(closed.failure, None);
    assert_eq!(
        closed.items[0].stop_reason,
        Some(serde_json::json!({ "type": "end_turn" }))
    );
}

#[test]
fn a_turn_of_steps_and_tool_results_completes_with_threads_blocks_in_place() {
    let mut writer = Writer::default();
    writer
        .push(
            0,
            ChunkPart::StepEvent {
                event: lib(StreamEvent::BlockStart {
                    index: 0,
                    kind: StreamKind::Text,
                }),
                block_id: Some(Uuid::now_v7()),
                model: Some("m-1".into()),
            },
        )
        .event(
            0,
            StreamEvent::TextDelta {
                index: 0,
                text: "Let me look".into(),
            },
        )
        .event(0, StreamEvent::BlockEnd { index: 0 })
        .tool_call(0, 1, "c1", r#"{"q":"rust"}"#)
        .finish(0, StopReason::AwaitingToolResults)
        .results(1, &["c1"])
        .text_block(2, 0, &["Found ", "it"])
        .finish(2, StopReason::EndTurn)
        .artifact(2);
    assert_eq!(writer.draft.refusal(), None);
    let closed = close_alone(&writer.draft, CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Completed);
    assert_eq!(closed.items.len(), 3);
    assert_eq!(closed.items[0].model.as_deref(), Some("m-1"));
    assert_eq!(closed.items[1].kind, ItemKind::ToolResults);
    let last = &closed.items[2];
    assert_eq!(last.blocks.len(), 2);
    assert_eq!(last.blocks[1].kind, BlockKind::Artifact);
    assert_eq!(
        last.blocks[1].index, 1,
        "a Threads block takes the next index"
    );
}

#[test]
fn a_chunk_the_library_refuses_fails_the_stream_and_freezes_it() {
    let mut writer = Writer::default();
    writer.event(
        0,
        StreamEvent::TextDelta {
            index: 0,
            text: "orphan".into(),
        },
    );
    let refusal = writer.draft.refusal().cloned().expect("refused");
    assert_eq!(refusal.seq, 0);
    assert_eq!(refusal.code, "no_open_block");
    assert_eq!(refusal.item_index, Some(0));
    writer
        .text_block(0, 0, &["late"])
        .finish(0, StopReason::EndTurn);
    let closed = close_alone(&writer.draft, CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(closed.failure, Some("no_open_block"));
    assert!(closed.items.is_empty());
}

#[test]
fn the_chunk_format_has_its_own_rules() {
    let mut missing_id = Writer::default();
    missing_id.push(
        0,
        ChunkPart::StepEvent {
            event: lib(StreamEvent::BlockStart {
                index: 0,
                kind: StreamKind::Text,
            }),
            block_id: None,
            model: None,
        },
    );
    assert_eq!(
        missing_id.draft.refusal().map(|r| r.code),
        Some(chunk_error::BLOCK_ID_REQUIRED)
    );

    let mut late_model = Writer::default();
    late_model.text_block(0, 0, &["a"]).push(
        0,
        ChunkPart::StepEvent {
            event: lib(StreamEvent::Finish {
                stop_reason: StopReason::EndTurn,
                usage: None,
            }),
            block_id: None,
            model: Some("m-2".into()),
        },
    );
    assert_eq!(
        late_model.draft.refusal().map(|r| r.code),
        Some(chunk_error::MODEL_NOT_FIRST)
    );

    let mut skipping = Writer::default();
    skipping.text_block(1, 0, &["a"]);
    assert_eq!(
        skipping.draft.refusal().map(|r| r.code),
        Some(chunk_error::ITEM_OUT_OF_ORDER)
    );

    let mut before_finish = Writer::default();
    before_finish
        .text_block(0, 0, &["a"])
        .text_block(1, 0, &["b"]);
    assert_eq!(
        before_finish.draft.refusal().map(|r| r.code),
        Some(chunk_error::ITEM_OUT_OF_ORDER),
        "an item opens only once the previous one is complete"
    );

    let mut results_first = Writer::default();
    results_first.results(0, &["c1"]);
    assert_eq!(
        results_first.draft.refusal().map(|r| r.code),
        Some("turn_must_start_with_step")
    );

    let mut not_v7 = Writer::default();
    not_v7.push(
        0,
        ChunkPart::StepEvent {
            event: lib(StreamEvent::BlockStart {
                index: 0,
                kind: StreamKind::Text,
            }),
            block_id: Some(Uuid::new_v4()),
            model: None,
        },
    );
    assert_eq!(
        not_v7.draft.refusal().map(|r| r.code),
        Some(chunk_error::BLOCK_ID_NOT_UUIDV7)
    );

    let mut reused = Writer::default();
    let id = Uuid::now_v7();
    for index in 0..2 {
        reused.push(
            0,
            ChunkPart::StepEvent {
                event: lib(StreamEvent::BlockStart {
                    index,
                    kind: StreamKind::Text,
                }),
                block_id: Some(id),
                model: None,
            },
        );
        if index == 0 {
            reused.event(
                0,
                StreamEvent::TextDelta {
                    index: 0,
                    text: "a".into(),
                },
            );
            reused.event(0, StreamEvent::BlockEnd { index: 0 });
        }
    }
    assert_eq!(
        reused.draft.refusal().map(|r| r.code),
        Some(chunk_error::BLOCK_ID_REUSED)
    );
}

#[test]
fn an_unreadable_chunk_is_refused_without_a_decode_error_reaching_the_engine() {
    let mut draft = MessageDraft::default();
    fold(&mut draft, 0, serde_json::json!({ "item_index": "zero" }));
    assert_eq!(
        draft.refusal().map(|r| r.code),
        Some(chunk_error::UNREADABLE)
    );
}

#[test]
fn a_cancel_answers_every_open_tool_call_with_an_interrupted_error() {
    let mut writer = Writer::default();
    writer
        .tool_call(0, 0, "c1", "{}")
        .tool_call(0, 1, "c2", "{}")
        .finish(0, StopReason::AwaitingToolResults)
        .results(1, &["c1"]);
    let closed = close_alone(&writer.draft, CloseKind::Cancel);
    assert_eq!(closed.status, MessageStatus::Cancelled);
    assert_eq!(closed.items.len(), 2);
    let results = &closed.items[1];
    assert_eq!(results.kind, ItemKind::ToolResults);
    assert_eq!(results.blocks.len(), 2);
    let interrupted: ToolResult =
        serde_json::from_value(results.blocks[1].content.clone()).unwrap();
    assert_eq!(interrupted.tool_call_id.as_str(), "c2");
    assert!(interrupted.is_error);
    assert_eq!(
        interrupted.content,
        vec![ToolResultBlock::text(Text::new(INTERRUPTED).unwrap())]
    );
    let turn = content::turn_of(Uuid::now_v7(), None, &closed.items).expect("the turn loads");
    assert!(!matches!(
        turn.state(),
        TurnState::AwaitingToolResults { .. }
    ));
}

#[test]
fn a_failure_with_no_tool_results_yet_opens_the_results_item() {
    let mut writer = Writer::default();
    writer
        .tool_call(0, 0, "c1", "{}")
        .finish(0, StopReason::AwaitingToolResults);
    let closed = close_alone(&writer.draft, CloseKind::Fail);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(closed.items.len(), 2);
    assert_eq!(closed.items[1].status, MessageStatus::Failed);
}

#[test]
fn an_unfinished_step_is_kept_when_the_library_can_finish_it() {
    let mut writer = Writer::default();
    writer
        .event(
            0,
            StreamEvent::BlockStart {
                index: 0,
                kind: StreamKind::Text,
            },
        )
        .event(
            0,
            StreamEvent::TextDelta {
                index: 0,
                text: "half an ".into(),
            },
        )
        .event(
            0,
            StreamEvent::TextDelta {
                index: 0,
                text: "answer".into(),
            },
        );
    let closed = close_alone(&writer.draft, CloseKind::Fail);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(closed.items.len(), 1);
    assert_eq!(closed.items[0].status, MessageStatus::Failed);
    assert_eq!(
        closed.items[0].stop_reason,
        Some(serde_json::json!({ "type": "other", "reason": INTERRUPTED }))
    );
    assert_eq!(
        library_blocks(&closed.items[0]),
        vec![AssistantBlock::Text {
            text: Text::new("half an answer").unwrap()
        }]
    );
}

#[test]
fn an_unfinished_step_the_library_cannot_finish_is_dropped() {
    let mut writer = Writer::default();
    writer.event(
        0,
        StreamEvent::BlockStart {
            index: 0,
            kind: StreamKind::Text,
        },
    );
    let closed = close_alone(&writer.draft, CloseKind::Fail);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert!(closed.items.is_empty());
    assert!(!has_a_step(&closed.items));

    let mut broken_call = Writer::default();
    broken_call
        .text_block(0, 0, &["calling"])
        .event(
            0,
            StreamEvent::ToolCallStart {
                index: 1,
                id: ToolCallId::new("c1").unwrap(),
                name: ToolName::new("search").unwrap(),
            },
        )
        .event(
            0,
            StreamEvent::ToolCallArgumentsDelta {
                index: 1,
                json_fragment: "{\"q\":".into(),
            },
        );
    assert!(
        close_alone(&broken_call.draft, CloseKind::Cancel)
            .items
            .is_empty()
    );
}

#[test]
fn a_completion_of_an_unfinished_stream_fails_it_instead() {
    let mut writer = Writer::default();
    writer.text_block(0, 0, &["almost"]);
    let closed = close_alone(&writer.draft, CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(closed.failure, Some("missing_finish"));
    assert_eq!(
        closed.items.len(),
        1,
        "the step the library can finish is kept"
    );

    let empty = close_alone(&MessageDraft::default(), CloseKind::Complete);
    assert_eq!(empty.status, MessageStatus::Failed);
    assert!(empty.items.is_empty());

    let mut pending = Writer::default();
    pending
        .tool_call(0, 0, "c1", "{}")
        .finish(0, StopReason::AwaitingToolResults);
    let awaiting = close_alone(&pending.draft, CloseKind::Complete);
    assert_eq!(awaiting.status, MessageStatus::Failed);
    assert_eq!(
        awaiting.failure,
        Some(MessageInvalidError::TurnAwaitingToolResults.as_str())
    );
    assert_eq!(awaiting.items.len(), 2, "its open call is answered");
}

#[test]
fn a_cancel_of_a_refused_stream_is_a_failure() {
    let mut writer = Writer::default();
    writer.event(0, StreamEvent::BlockEnd { index: 0 });
    assert_eq!(
        close_alone(&writer.draft, CloseKind::Cancel).status,
        MessageStatus::Failed
    );
}

#[test]
fn a_thinking_and_its_signature_fold_byte_for_byte() {
    let mut writer = Writer::default();
    writer
        .event(
            0,
            StreamEvent::BlockStart {
                index: 0,
                kind: StreamKind::Thinking,
            },
        )
        .event(
            0,
            StreamEvent::ThinkingDelta {
                index: 0,
                text: "  let me\n".into(),
            },
        )
        .event(
            0,
            StreamEvent::ThinkingDelta {
                index: 0,
                text: "\tthink ".into(),
            },
        )
        .event(
            0,
            StreamEvent::SignatureDelta {
                index: 0,
                signature: "c2ln".into(),
            },
        )
        .event(
            0,
            StreamEvent::SignatureDelta {
                index: 0,
                signature: "PT0=".into(),
            },
        )
        .event(0, StreamEvent::BlockEnd { index: 0 })
        .text_block(0, 1, &["answer"])
        .finish(0, StopReason::EndTurn);
    let closed = close_alone(&writer.draft, CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Completed);
    match &library_blocks(&closed.items[0])[0] {
        AssistantBlock::Thinking(thinking) => {
            assert_eq!(thinking.text, "  let me\n\tthink ");
            assert_eq!(thinking.signature.as_ref().unwrap().as_str(), "c2lnPT0=");
        }
        other => panic!("a thinking block, not {other:?}"),
    }
}

#[test]
fn a_threads_block_needs_an_item_and_is_never_a_library_block() {
    let mut first = Writer::default();
    first.artifact(0);
    assert_eq!(
        first.draft.refusal().map(|r| r.code),
        Some(chunk_error::ITEM_OUT_OF_ORDER)
    );
}

fn used(ids: &[&str]) -> UsedCallIds {
    ids.iter().map(|id| ToolCallId::new(*id).unwrap()).collect()
}

#[test]
fn a_blank_text_the_library_drops_is_not_stored_with_the_finished_step() {
    let mut writer = Writer::default();
    writer
        .text_block(0, 0, &["  "])
        .tool_call(0, 1, "c1", "{}")
        .text_block(0, 2, &["\n"])
        .finish(0, StopReason::AwaitingToolResults)
        .results(1, &["c1"])
        .text_block(2, 0, &[""])
        .text_block(2, 1, &["Found it"])
        .finish(2, StopReason::EndTurn);
    assert_eq!(writer.draft.refusal(), None);
    let streaming = writer.draft.preview();
    assert_eq!(
        streaming[0].blocks.len(),
        1,
        "a finished step shows what it keeps"
    );

    let closed = close_alone(&writer.draft, CloseKind::Complete);
    assert_eq!(closed.status, MessageStatus::Completed);
    let calling = &closed.items[0];
    assert_eq!(calling.blocks.len(), 1);
    assert_eq!(calling.blocks[0].kind, BlockKind::ToolCall);
    assert_eq!(calling.blocks[0].index, 0);
    let answer = &closed.items[2];
    assert_eq!(
        library_blocks(answer),
        vec![AssistantBlock::Text {
            text: Text::new("Found it").unwrap()
        }]
    );
    assert_eq!(answer.blocks[0].kind, BlockKind::Text);
    content::turn_of(Uuid::now_v7(), None, &closed.items).expect("what is stored loads");
}

#[test]
fn a_step_still_streaming_shows_its_blank_text() {
    let mut writer = Writer::default();
    writer.event(
        0,
        StreamEvent::BlockStart {
            index: 0,
            kind: StreamKind::Text,
        },
    );
    let preview = writer.draft.preview();
    assert_eq!(preview[0].blocks.len(), 1);
    assert_eq!(preview[0].blocks[0].content["text"], "");
}

#[test]
fn a_tool_call_id_used_on_the_branch_fails_the_completion_at_its_step() {
    let mut writer = Writer::default();
    writer
        .tool_call(0, 0, "c1", "{}")
        .finish(0, StopReason::AwaitingToolResults)
        .results(1, &["c1"])
        .tool_call(2, 0, "c2", "{}")
        .finish(2, StopReason::AwaitingToolResults)
        .results(3, &["c2"])
        .text_block(4, 0, &["done"])
        .finish(4, StopReason::EndTurn);
    assert_eq!(writer.draft.refusal(), None, "a fold cannot see the branch");

    let alone = close_alone(&writer.draft, CloseKind::Complete);
    assert_eq!(alone.status, MessageStatus::Completed);

    let closed = close(&writer.draft, CloseKind::Complete, &used(&["c2"]));
    assert_eq!(closed.status, MessageStatus::Failed);
    assert_eq!(
        closed.failure,
        Some("duplicate_tool_call_id_in_conversation")
    );
    assert_eq!(
        closed.items.len(),
        2,
        "the steps before the reused call are kept, the rest dropped"
    );
    assert!(content::check_calls_unused(&closed.items, &used(&["c2"])).is_ok());

    let failed = close(&writer.draft, CloseKind::Fail, &used(&["c1"]));
    assert_eq!(failed.status, MessageStatus::Failed);
    assert!(failed.items.is_empty(), "its first step reuses c1");
}

#[test]
fn the_stricter_rules_of_the_library_refuse_a_chunk_with_its_codes() {
    let mut dotted = Writer::default();
    dotted.push(
        0,
        ChunkPart::StepEvent {
            event: LibraryEvent::from_json(serde_json::json!({
                "type": "tool_call_start", "index": 0, "id": "c1", "name": "web.search"
            })),
            block_id: Some(Uuid::now_v7()),
            model: None,
        },
    );
    let refusal = dotted.draft.refusal().cloned().expect("refused");
    assert_eq!(refusal.code, "invalid_tool_name");
    assert_eq!(refusal.item_index, Some(0));
    assert!(dotted.draft.preview().is_empty(), "nothing folds from it");

    let mut unknown = Writer::default();
    unknown.push(
        0,
        ChunkPart::StepEvent {
            event: LibraryEvent::from_json(serde_json::json!({ "type": "audio_delta" })),
            block_id: None,
            model: None,
        },
    );
    assert_eq!(
        unknown.draft.refusal().map(|r| r.code),
        Some(chunk_error::UNREADABLE)
    );

    let mut mismatch = Writer::default();
    mismatch
        .tool_call(0, 0, "c1", "{}")
        .finish(0, StopReason::AwaitingToolResults)
        .push(
            1,
            ChunkPart::ToolResults {
                blocks: vec![Block::new(
                    Uuid::now_v7(),
                    BlockBody::tool_result(&ToolResult::new(
                        ToolCallId::new("c1").unwrap(),
                        ToolName::new("fetch").unwrap(),
                        Vec::new(),
                        false,
                    ))
                    .unwrap(),
                )],
            },
        );
    assert_eq!(
        mismatch.draft.refusal().map(|r| r.code),
        Some("tool_name_mismatch")
    );

    let mut reused = Writer::default();
    reused
        .tool_call(0, 0, "c1", "{}")
        .finish(0, StopReason::AwaitingToolResults)
        .results(1, &["c1"])
        .tool_call(2, 0, "c1", "{}")
        .finish(2, StopReason::AwaitingToolResults);
    assert_eq!(
        reused.draft.refusal().map(|r| r.code),
        Some("duplicate_tool_call_id_in_turn")
    );

    let mut no_result = Writer::default();
    no_result
        .tool_call(0, 0, "c1", "{}")
        .finish(0, StopReason::AwaitingToolResults)
        .push(
            1,
            ChunkPart::ToolResults {
                blocks: vec![Block::new(
                    Uuid::now_v7(),
                    BlockBody::Artifact(ArtifactRef {
                        artifact_id: Uuid::now_v7(),
                    }),
                )],
            },
        );
    assert_eq!(
        no_result.draft.refusal().map(|r| r.code),
        Some("empty_tool_results")
    );
}

#[test]
fn a_cancel_is_sealed_as_received_whatever_the_branch_uses() {
    let mut writer = Writer::default();
    writer
        .tool_call(0, 0, "c-used", "{}")
        .finish(0, StopReason::AwaitingToolResults)
        .results(1, &["c-used"])
        .text_block(2, 0, &["answered"])
        .finish(2, StopReason::EndTurn);
    let cancelled = close(&writer.draft, CloseKind::Cancel, &used(&["c-used"]));
    assert_eq!(cancelled.status, MessageStatus::Cancelled);
    assert_eq!(
        cancelled.items.len(),
        3,
        "a cancelled message is no conversation's"
    );
    assert_eq!((cancelled.failure, cancelled.cut), (None, None));
}

#[test]
fn a_failure_names_the_cut_its_close_made() {
    let mut answered = Writer::default();
    answered
        .text_block(0, 0, &["first"])
        .finish(0, StopReason::EndTurn);
    let mut calling = Writer::default();
    calling
        .tool_call(0, 0, "c-used", "{}")
        .finish(0, StopReason::AwaitingToolResults);
    let failed = close(&calling.draft, CloseKind::Fail, &used(&["c-used"]));
    assert_eq!(failed.status, MessageStatus::Failed);
    assert_eq!(failed.failure, None, "the stream gave no reason of its own");
    assert_eq!(failed.cut, Some("duplicate_tool_call_id_in_conversation"));
    assert!(failed.items.is_empty());

    let mut opened = Writer::default();
    opened.event(
        0,
        StreamEvent::BlockStart {
            index: 0,
            kind: StreamKind::Text,
        },
    );
    let failed = close_alone(&opened.draft, CloseKind::Fail);
    assert_eq!(failed.cut, Some(UNFINISHABLE_STEP));

    let whole = close_alone(&answered.draft, CloseKind::Fail);
    assert_eq!(whole.cut, None, "nothing was cut");
}

#[test]
fn a_completion_whose_content_is_dropped_whole_fails() {
    let mut writer = Writer::default();
    writer
        .text_block(0, 0, &["done"])
        .finish(0, StopReason::EndTurn);
    let completed = close_alone(&writer.draft, CloseKind::Complete);
    assert_eq!(completed.status, MessageStatus::Completed);
    let dropped = completed.without_content();
    assert_eq!(dropped.status, MessageStatus::Failed);
    assert_eq!(dropped.failure, Some(chunk_error::BLOCK_ID_REUSED));
    assert!(dropped.items.is_empty());

    let failed = close_alone(&writer.draft, CloseKind::Fail).without_content();
    assert_eq!(failed.status, MessageStatus::Failed);
    assert_eq!(failed.cut, Some(chunk_error::BLOCK_ID_REUSED));
    let cancelled = close_alone(&writer.draft, CloseKind::Cancel).without_content();
    assert_eq!(cancelled.status, MessageStatus::Cancelled);
}

#[test]
fn a_finished_step_must_line_up_with_what_streamed() {
    let mut step = DraftStep::default();
    step.library.push(LibraryBlock::Text { text: " ".into() });
    step.library.push(LibraryBlock::Text {
        text: "kept".into(),
    });
    let text = |text: &str| AssistantBlock::Text {
        text: Text::new(text).unwrap(),
    };
    let finished = |blocks| Step::new(blocks, StopReason::EndTurn, None, None).unwrap();
    assert!(step.aligned(&finished(vec![text("kept")])));
    assert!(!step.aligned(&finished(vec![text("kept"), text("more")])));
    let thinking = AssistantBlock::Thinking(contract_threads::llm::Thinking {
        text: "x".into(),
        signature: None,
    });
    assert!(!step.aligned(&finished(vec![thinking])));
}
