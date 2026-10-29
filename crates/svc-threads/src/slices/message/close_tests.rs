use contract_threads::MessageStatus;
use contract_threads::llm::{ClosureReason, ResponseEnding};

use super::super::closure::Named;
use super::*;
use crate::slices::message::draft::tests::{Writer, closure_of, kinds, v7};

fn named(kind: CloseKind, reason: ClosureReason) -> Closing {
    Closing::naming(kind, Some(Named::of(reason).unwrap()))
}

fn closed(writer: &Writer, closing: &Closing) -> Closed {
    close(writer.judged(), closing).unwrap()
}

fn closure_json(closed: &Closed) -> serde_json::Value {
    let closure = closed.content.entries.last().expect("a closure");
    serde_json::to_value(closure.payload()).unwrap()["reason"].clone()
}

fn budget(detail: &str) -> ClosureReason {
    ClosureReason::budget_exhausted()
        .with_detail(detail)
        .unwrap()
}

#[test]
fn budget_exhausted_completes_a_turn_with_a_response_still_produced_and_keeps_it_interrupted() {
    let mut writer = Writer::new();
    writer.text_response(&["half"], None);
    let closed = closed(&writer, &named(CloseKind::Complete, budget("limit")));
    assert_eq!(closed.status, MessageStatus::Completed);
    assert_eq!(closed.closure, Kind::BudgetExhausted);
    assert_eq!(closed.reason, None);
    assert_eq!(
        kinds(&closed.content),
        [
            ("model_response", "interrupted"),
            ("turn_closure", "complete")
        ]
    );
    assert_eq!(closure_json(&closed)["type"], "budget_exhausted");
    assert_eq!(closure_json(&closed)["detail"], "limit");
}

#[test]
fn budget_exhausted_completes_a_turn_with_a_call_left_unanswered() {
    let mut writer = Writer::new();
    writer.calling_response();
    let closed = closed(&writer, &named(CloseKind::Complete, budget("limit")));
    assert_eq!(closed.status, MessageStatus::Completed);
    assert_eq!(
        kinds(&closed.content),
        [("model_response", "complete"), ("turn_closure", "complete")]
    );
}

#[test]
fn completed_still_needs_a_whole_turn_whether_it_is_named_or_not() {
    let mut calling = Writer::new();
    calling.calling_response();
    let mut producing = Writer::new();
    producing.text_response(&["half"], None);
    for closing in [
        Closing::of(CloseKind::Complete),
        named(CloseKind::Complete, ClosureReason::completed()),
        named(CloseKind::Fail, ClosureReason::completed()),
    ] {
        let unanswered = closed(&calling, &closing);
        assert_eq!(unanswered.status, MessageStatus::Failed, "{closing:?}");
        assert_eq!(unanswered.reason, Some("unresolved_execution"));
        assert_eq!(unanswered.closure, Kind::Failed);
        let unfinished = closed(&producing, &closing);
        assert_eq!(unfinished.status, MessageStatus::Failed, "{closing:?}");
        assert_eq!(unfinished.reason, Some(reason::RESPONSE_STILL_PRODUCING));
        assert_eq!(
            kinds(&unfinished.content),
            [
                ("model_response", "interrupted"),
                ("turn_closure", "complete")
            ]
        );
    }
}

#[test]
fn a_failure_naming_completed_on_a_whole_turn_is_completed() {
    let mut writer = Writer::new();
    writer.text_response(&["done"], Some(ResponseEnding::finished()));
    let closed = closed(&writer, &named(CloseKind::Fail, ClosureReason::completed()));
    assert_eq!(closed.status, MessageStatus::Completed);
    assert_eq!(closure_of(&closed.content), "completed");
}

#[test]
fn the_status_follows_the_reason_on_both_commands() {
    let mut writer = Writer::new();
    writer.text_response(&["done"], Some(ResponseEnding::finished()));
    let failed = ClosureReason::failed("tool crashed").unwrap();
    let table = [
        (failed, MessageStatus::Failed, "failed"),
        (
            ClosureReason::interrupted(),
            MessageStatus::Failed,
            "interrupted",
        ),
        (
            ClosureReason::cancelled(),
            MessageStatus::Cancelled,
            "cancelled",
        ),
        (
            budget("limit"),
            MessageStatus::Completed,
            "budget_exhausted",
        ),
    ];
    for kind in [CloseKind::Complete, CloseKind::Fail] {
        for (reason, status, name) in &table {
            let closed = closed(&writer, &named(kind, reason.clone()));
            assert_eq!(closed.status, *status, "{kind:?} {name}");
            assert_eq!(closure_of(&closed.content), *name);
            assert_eq!(closed.closure.as_str(), *name);
            assert_eq!(closed.reason, None, "the writer decided, not Threads");
        }
    }
}

#[test]
fn the_detail_of_a_failure_is_on_the_closure_and_in_no_reason() {
    let mut writer = Writer::new();
    writer.text_response(&["done"], Some(ResponseEnding::finished()));
    let closed = closed(
        &writer,
        &named(
            CloseKind::Fail,
            ClosureReason::failed("a secret detail").unwrap(),
        ),
    );
    assert_eq!(closure_json(&closed)["detail"], "a secret detail");
    assert_eq!(closed.reason, None);
}

#[test]
fn a_response_produced_is_interrupted_with_the_kind_of_the_reason_named() {
    let mut writer = Writer::new();
    writer.text_response(&["half"], None);
    let table = [
        (ClosureReason::interrupted(), "interrupted"),
        (ClosureReason::failed("x").unwrap(), "failed"),
        (ClosureReason::cancelled(), "cancelled"),
        (budget("x"), "interrupted"),
    ];
    for (reason, expected) in table {
        let closed = closed(&writer, &named(CloseKind::Fail, reason));
        let response = closed.content.entries.first().unwrap();
        assert_eq!(response.state().as_str(), "interrupted");
        let payload = serde_json::to_value(response.payload()).unwrap();
        assert_eq!(payload["interruption"]["kind"], expected);
    }
}

#[test]
fn a_stream_the_library_refused_fails_whatever_reason_is_named() {
    let mut writer = Writer::new();
    let entry = writer.text_response(&["a"], None);
    writer.delta(
        entry,
        contract_threads::llm::DeltaOp::text_appended(
            contract_threads::llm::BlockId::from_uuid(v7()).unwrap(),
            "never opened",
        ),
    );
    for (reason, interruption) in [
        (budget("x"), "interrupted"),
        (ClosureReason::completed(), "failed"),
        (ClosureReason::cancelled(), "cancelled"),
    ] {
        let closed = closed(&writer, &named(CloseKind::Complete, reason));
        assert_eq!(closed.status, MessageStatus::Failed);
        assert_eq!(closed.reason, Some("stream_target_error"));
        assert_eq!(closed.closure, Kind::Failed);
        let response = closed.content.entries.first().unwrap();
        let payload = serde_json::to_value(response.payload()).unwrap();
        assert_eq!(payload["interruption"]["kind"], interruption);
    }
}

#[test]
fn a_stream_the_library_refused_is_interrupted_as_each_closer_did_in_0_5() {
    let mut writer = Writer::new();
    let entry = writer.text_response(&["a"], None);
    writer.delta(
        entry,
        contract_threads::llm::DeltaOp::text_appended(
            contract_threads::llm::BlockId::from_uuid(v7()).unwrap(),
            "never opened",
        ),
    );
    for (kind, interruption) in [
        (CloseKind::Complete, "failed"),
        (CloseKind::Fail, "failed"),
        (CloseKind::Cancel, "cancelled"),
        (CloseKind::Watch, "interrupted"),
    ] {
        let closed = closed(&writer, &Closing::of(kind));
        assert_eq!(closed.status, MessageStatus::Failed, "{kind:?}");
        assert_eq!(closed.reason, Some("stream_target_error"), "{kind:?}");
        assert_eq!(closed.closure, Kind::Failed, "{kind:?}");
        let response = closed.content.entries.first().unwrap();
        let payload = serde_json::to_value(response.payload()).unwrap();
        assert_eq!(payload["interruption"]["kind"], interruption, "{kind:?}");
    }
}

#[test]
fn without_a_reason_the_closers_behave_as_in_0_5() {
    let mut writer = Writer::new();
    writer.text_response(&["done"], Some(ResponseEnding::finished()));
    let fail = closed(&writer, &Closing::of(CloseKind::Fail));
    assert_eq!(fail.reason, Some(reason::FAILED_BY_WRITER));
    assert_eq!(fail.closure, Kind::Failed);
    assert_eq!(closure_json(&fail)["detail"], reason::FAILED_BY_WRITER);
    let watch = closed(&writer, &Closing::of(CloseKind::Watch));
    assert_eq!(watch.reason, Some(reason::STREAM_IDLE));
    assert_eq!(watch.closure, Kind::Interrupted);
    let complete = closed(&writer, &Closing::of(CloseKind::Complete));
    assert_eq!(complete.closure, Kind::Completed);
    let cancel = closed(&writer, &Closing::of(CloseKind::Cancel));
    assert_eq!(cancel.closure, Kind::Cancelled);
}
