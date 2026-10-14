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
    let stored = user_blocks(&[user_text("Hello"), citation]).unwrap();
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
    let refused = user_blocks(&[blank]).unwrap_err();
    assert_eq!(refused.code, ReasonCode::MessageInvalid);
    assert_eq!(error_of(&refused), reason::BLOCK_CONTENT_INVALID);

    let refused = user_blocks(&[result("c1")]).unwrap_err();
    assert_eq!(error_of(&refused), reason::BLOCK_KIND_NOT_ALLOWED);

    let artifact = block(BlockBody::Artifact(ArtifactRef {
        artifact_id: Uuid::now_v7(),
    }));
    let refused = user_blocks(&[user_text("see"), artifact]).unwrap_err();
    assert_eq!(error_of(&refused), reason::ARTIFACT_IN_USER_MESSAGE);

    let only_threads = block(BlockBody::Citation(Citation {
        text: "q".into(),
        source_ids: Vec::new(),
    }));
    let refused = user_blocks(&[only_threads]).unwrap_err();
    assert_eq!(error_of(&refused), "empty_user_content");
}

#[test]
fn a_block_id_appears_once_in_a_message() {
    let text = user_text("twice");
    let refused = user_blocks(&[text.clone(), text]).unwrap_err();
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
    let stored = user_blocks(&[user_text("Hi")]).unwrap();
    let input = user_input_of(&stored, Author::new("u1", "Ada").unwrap()).unwrap();
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
    let errors = [
        MessageError::EmptyStep,
        MessageError::MissingFinish,
        MessageError::Blank { field: "text" },
        MessageError::InvalidJson {
            field: "tool_arguments",
            message: "eof".into(),
        },
    ];
    for error in errors {
        let code = library_error_code(&error);
        assert!(
            code.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
            "{code}"
        );
    }
}
