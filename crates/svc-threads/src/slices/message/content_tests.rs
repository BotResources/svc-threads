use contract_threads::llm::{
    AssistantBlock, StopReason, Text, Thinking, ToolCall, ToolCallId, ToolName, UserBlock,
};
use contract_threads::{ArtifactRef, Citation, LibraryBlock, StepItem, ToolResultsItem};
use serde_json::json;

use super::*;

fn block(body: BlockBody) -> Block {
    Block::new(Uuid::now_v7(), body)
}

fn user_text(text: &str) -> Block {
    block(BlockBody::user_text(&Text::new(text).unwrap()).unwrap())
}

fn ai_text(text: &str) -> Block {
    block(
        BlockBody::assistant(&AssistantBlock::Text {
            text: Text::new(text).unwrap(),
        })
        .unwrap(),
    )
}

fn call(id: &str) -> Block {
    block(
        BlockBody::assistant(&AssistantBlock::ToolCall(ToolCall {
            id: ToolCallId::new(id).unwrap(),
            name: ToolName::new("search").unwrap(),
            arguments: json!({ "q": "rust" }),
        }))
        .unwrap(),
    )
}

fn result(id: &str) -> Block {
    block(
        BlockBody::tool_result(&ToolResult::new(
            ToolCallId::new(id).unwrap(),
            ToolName::new("search").unwrap(),
            Vec::new(),
            false,
        ))
        .unwrap(),
    )
}

fn step(stop_reason: StopReason, blocks: Vec<Block>) -> Item {
    Item::Step(StepItem {
        stop_reason,
        usage: None,
        model: Some("m-1".into()),
        blocks,
    })
}

fn error_of(refusal: &Refusal) -> &str {
    refusal.params["error"].as_str().unwrap_or_default()
}

#[test]
fn a_user_input_holds_text_and_threads_blocks() {
    let citation = block(BlockBody::Citation(Citation {
        text: "a quote".into(),
        source_ids: vec!["s1".into()],
    }));
    let stored = user_blocks(Uuid::now_v7(), &[user_text("Hello"), citation]).unwrap();
    assert_eq!(stored.len(), 2);
    assert_eq!(
        stored[0].content,
        json!({ "type": "text", "text": "Hello" })
    );
    assert_eq!(stored[1].kind, BlockKind::Citation);
    assert_eq!(stored[1].index, 1);
}

#[test]
fn a_user_input_the_library_refuses_is_message_invalid() {
    let blank = Block::new(
        Uuid::now_v7(),
        BlockBody::Text(LibraryBlock::from_json(
            json!({ "type": "text", "text": "  " }),
        )),
    );
    let refused = user_blocks(Uuid::now_v7(), &[blank]).unwrap_err();
    assert_eq!(refused.code, ReasonCode::MessageInvalid);
    assert_eq!(error_of(&refused), "blank", "the library's own refusal");

    let shapeless = Block::new(
        Uuid::now_v7(),
        BlockBody::Text(LibraryBlock::from_json(json!({ "text": 42 }))),
    );
    let refused = user_blocks(Uuid::now_v7(), &[shapeless]).unwrap_err();
    assert_eq!(error_of(&refused), reason::BLOCK_CONTENT_INVALID);

    let refused = user_blocks(Uuid::now_v7(), &[result("c1")]).unwrap_err();
    assert_eq!(error_of(&refused), reason::BLOCK_KIND_NOT_ALLOWED);

    let artifact = block(BlockBody::Artifact(ArtifactRef {
        artifact_id: Uuid::now_v7(),
    }));
    let refused = user_blocks(Uuid::now_v7(), &[user_text("see"), artifact]).unwrap_err();
    assert_eq!(error_of(&refused), reason::ARTIFACT_IN_USER_MESSAGE);

    let only_threads = block(BlockBody::Citation(Citation {
        text: "q".into(),
        source_ids: Vec::new(),
    }));
    let refused = user_blocks(Uuid::now_v7(), &[only_threads]).unwrap_err();
    assert_eq!(error_of(&refused), "empty_user_content");
}

#[test]
fn a_block_id_appears_once_in_a_message() {
    let text = user_text("twice");
    let refused = user_blocks(Uuid::now_v7(), &[text.clone(), text]).unwrap_err();
    assert_eq!(refused.code, ReasonCode::KeyReused);
}

#[test]
fn a_kind_must_match_the_librarys_content() {
    let lying = Block::new(
        Uuid::now_v7(),
        BlockBody::Thinking(LibraryBlock::from_json(
            json!({ "type": "text", "text": "x" }),
        )),
    );
    let refused = ai_items(Uuid::now_v7(), &[step(StopReason::EndTurn, vec![lying])]).unwrap_err();
    assert_eq!(error_of(&refused), reason::BLOCK_CONTENT_INVALID);
}

#[test]
fn an_ai_turn_given_whole_is_judged_item_by_item() {
    let message = Uuid::now_v7();
    let artifact = block(BlockBody::Artifact(ArtifactRef {
        artifact_id: Uuid::now_v7(),
    }));
    let items = vec![
        step(
            StopReason::AwaitingToolResults,
            vec![ai_text("Looking"), call("c1")],
        ),
        Item::ToolResults(ToolResultsItem {
            blocks: vec![result("c1")],
        }),
        step(StopReason::EndTurn, vec![ai_text("Found"), artifact]),
    ];
    let stored = ai_items(message, &items).unwrap();
    assert_eq!(stored.len(), 3);
    assert!(
        stored
            .iter()
            .all(|item| item.status == MessageStatus::Completed)
    );
    assert_eq!(stored[0].model.as_deref(), Some("m-1"));
    assert_eq!(stored[2].blocks[1].kind, BlockKind::Artifact);
    assert_eq!(
        named_artifacts(stored.iter().flat_map(|item| item.blocks.iter())).len(),
        1
    );
    let turn = turn_of(message, None, &stored).unwrap();
    assert!(matches!(turn.state(), TurnState::Finished { .. }));
}

#[test]
fn an_ai_turn_the_library_refuses_is_message_invalid() {
    let at = |items: Vec<Item>| error_of(&ai_items(Uuid::now_v7(), &items).unwrap_err()).to_owned();
    assert_eq!(at(Vec::new()), "turn_must_start_with_step");
    assert_eq!(
        at(vec![step(StopReason::EndTurn, vec![call("c1")])]),
        "tool_calls_without_awaiting"
    );
    assert_eq!(
        at(vec![Item::ToolResults(ToolResultsItem {
            blocks: vec![result("c1")]
        })]),
        "turn_must_start_with_step"
    );
    assert_eq!(
        at(vec![
            step(StopReason::AwaitingToolResults, vec![call("c1")]),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![result("c9")]
            }),
        ]),
        "tool_result_not_pending"
    );
    assert_eq!(
        at(vec![step(
            StopReason::AwaitingToolResults,
            vec![call("c1")]
        )]),
        reason::TURN_AWAITING_TOOL_RESULTS
    );
    assert_eq!(
        at(vec![step(StopReason::EndTurn, Vec::new())]),
        "empty_step"
    );
}

#[test]
fn a_thinking_is_kept_byte_for_byte_and_read_back_by_the_library() {
    let thinking = AssistantBlock::Thinking(Thinking {
        text: "  let me think\n\twith whitespace ".to_owned(),
        signature: Some(contract_threads::llm::Signature::new("c2lnPT0=").unwrap()),
    });
    let message = Uuid::now_v7();
    let stored = ai_items(
        message,
        &[step(
            StopReason::EndTurn,
            vec![
                block(BlockBody::assistant(&thinking).unwrap()),
                ai_text("Analysis"),
            ],
        )],
    )
    .unwrap();
    let back: AssistantBlock = serde_json::from_value(stored[0].blocks[0].content.clone()).unwrap();
    assert_eq!(back, thinking);
    let author = Author::new("a1", "Analyst").unwrap();
    let turn = turn_of(message, Some(author), &stored).unwrap();
    assert_eq!(turn.items().len(), 1);
}

#[test]
fn a_user_input_reads_back_under_its_author() {
    let message = Uuid::now_v7();
    let stored = user_blocks(message, &[user_text("Hi")]).unwrap();
    let input = user_input_of(message, &stored, Author::new("u1", "Ada").unwrap()).unwrap();
    assert_eq!(
        input.id().as_str(),
        message.to_string(),
        "the input's id is the message's"
    );
    assert_eq!(
        input.content(),
        &[UserBlock::text(Text::new("Hi").unwrap())]
    );
    assert_eq!(
        input.author().map(|author| author.name().as_str()),
        Some("Ada")
    );
}

#[test]
fn every_library_error_has_a_stable_code() {
    let id = || ToolCallId::new("c1").unwrap();
    let name = |name: &str| ToolName::new(name).unwrap();
    let errors = [
        (MessageError::EmptyStep, "empty_step"),
        (MessageError::MissingFinish, "missing_finish"),
        (MessageError::Blank { field: "text" }, "blank"),
        (
            MessageError::InvalidJson {
                field: "tool_arguments",
                message: "eof".into(),
            },
            "invalid_json",
        ),
        (MessageError::InvalidToolName, "invalid_tool_name"),
        (
            MessageError::DuplicateToolCallIdInTurn { id: id() },
            "duplicate_tool_call_id_in_turn",
        ),
        (
            MessageError::DuplicateToolCallIdInConversation { id: id() },
            "duplicate_tool_call_id_in_conversation",
        ),
        (
            MessageError::ToolNameMismatch {
                id: id(),
                call: name("search"),
                result: name("fetch"),
            },
            "tool_name_mismatch",
        ),
        (MessageError::EmptyToolResults, "empty_tool_results"),
        (
            MessageError::DuplicateEntryId { id: "m1".into() },
            "duplicate_entry_id",
        ),
        (MessageError::WireListEmpty, "wire_list_empty"),
        (
            MessageError::WireToolCallIdRepeated {
                message: 1,
                block: 0,
                id: id(),
            },
            "wire_tool_call_id_repeated",
        ),
    ];
    for (error, expected) in errors {
        let code = library_error_code(&error);
        assert_eq!(code, expected, "{error:?}");
        assert!(
            code.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
            "{code}"
        );
    }
    let refused = invalid_by_library(&MessageError::DuplicateToolCallIdInConversation { id: id() });
    assert_eq!(refused.code, ReasonCode::MessageInvalid);
    assert_eq!(refused.params["tool_call_id"], "c1");
}

fn raw(kind: &str, json: serde_json::Value) -> Block {
    let raw = LibraryBlock::from_json(json);
    let body = match kind {
        "TOOL_CALL" => BlockBody::ToolCall(raw),
        "TOOL_RESULT" => BlockBody::ToolResult(raw),
        _ => BlockBody::Text(raw),
    };
    Block::new(Uuid::now_v7(), body)
}

fn result_named(id: &str, tool: &str) -> Block {
    block(
        BlockBody::tool_result(&ToolResult::new(
            ToolCallId::new(id).unwrap(),
            ToolName::new(tool).unwrap(),
            Vec::new(),
            false,
        ))
        .unwrap(),
    )
}

#[test]
fn the_stricter_rules_of_the_library_refuse_with_its_codes() {
    let at = |items: Vec<Item>| error_of(&ai_items(Uuid::now_v7(), &items).unwrap_err()).to_owned();

    // A tool name no provider accepts, in a call and in a result.
    let dotted_call = raw(
        "TOOL_CALL",
        json!({ "type": "tool_call", "id": "c1", "name": "web.search", "arguments": {} }),
    );
    assert_eq!(
        at(vec![step(
            StopReason::AwaitingToolResults,
            vec![dotted_call]
        )]),
        "invalid_tool_name"
    );
    let dotted_result = raw(
        "TOOL_RESULT",
        json!({ "tool_call_id": "c1", "tool_name": "web.search", "content": [], "is_error": false }),
    );
    assert_eq!(
        at(vec![
            step(StopReason::AwaitingToolResults, vec![call("c1")]),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![dotted_result]
            }),
        ]),
        "invalid_tool_name"
    );

    // A result naming another tool than its call.
    assert_eq!(
        at(vec![
            step(StopReason::AwaitingToolResults, vec![call("c1")]),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![result_named("c1", "fetch")]
            }),
        ]),
        "tool_name_mismatch"
    );

    // A call id reused by a later step of the same turn.
    assert_eq!(
        at(vec![
            step(StopReason::AwaitingToolResults, vec![call("c1")]),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![result("c1")]
            }),
            step(StopReason::AwaitingToolResults, vec![call("c1")]),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![result("c1")]
            }),
            step(StopReason::EndTurn, vec![ai_text("done")]),
        ]),
        "duplicate_tool_call_id_in_turn"
    );

    // A tool results item holding no result: Threads blocks alone are none.
    let citation = block(BlockBody::Citation(Citation {
        text: "q".into(),
        source_ids: Vec::new(),
    }));
    assert_eq!(
        at(vec![
            step(StopReason::AwaitingToolResults, vec![call("c1")]),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![result("c1")]
            }),
            step(StopReason::AwaitingToolResults, vec![call("c2")]),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![citation]
            }),
        ]),
        "empty_tool_results"
    );
}

#[test]
fn a_tool_call_id_is_used_once_on_every_branch_through_a_message() {
    let stored = ai_items(
        Uuid::now_v7(),
        &[
            step(StopReason::AwaitingToolResults, vec![call("c1")]),
            Item::ToolResults(ToolResultsItem {
                blocks: vec![result("c1")],
            }),
            step(StopReason::EndTurn, vec![ai_text("done")]),
        ],
    )
    .unwrap();
    assert_eq!(call_ids(&stored), vec![ToolCallId::new("c1").unwrap()]);
    assert_eq!(check_calls_unused(&stored, &UsedCallIds::new()), Ok(()));
    let used: UsedCallIds = [ToolCallId::new("c9").unwrap()].into_iter().collect();
    assert_eq!(check_calls_unused(&stored, &used), Ok(()));
    let used: UsedCallIds = [ToolCallId::new("c1").unwrap()].into_iter().collect();
    assert_eq!(
        check_calls_unused(&stored, &used),
        Err(MessageError::DuplicateToolCallIdInConversation {
            id: ToolCallId::new("c1").unwrap()
        })
    );
}

#[test]
fn a_value_the_library_refuses_is_named_by_its_own_constructor() {
    let refusal = |json: serde_json::Value| value_refusal(&json).map(|e| library_error_code(&e));
    assert_eq!(
        refusal(json!({ "type": "tool_call_start", "index": 0, "id": "c1", "name": "a b" })),
        Some("invalid_tool_name")
    );
    assert_eq!(
        refusal(json!({ "type": "tool_call", "id": "", "name": "search", "arguments": {} })),
        Some("blank")
    );
    assert_eq!(
        refusal(json!({ "type": "thinking", "text": "", "signature": "" })),
        Some("blank")
    );
    assert_eq!(
        refusal(json!({
            "tool_call_id": "c1", "tool_name": "search",
            "content": [{ "type": "text", "text": " " }], "is_error": false
        })),
        Some("blank")
    );
    assert_eq!(refusal(json!({ "type": "text", "text": "fine" })), None);
    assert_eq!(refusal(json!({ "type": "audio" })), None);
}
