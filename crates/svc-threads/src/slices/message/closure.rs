use contract_threads::llm::{ClosureReason, InterruptionReason};
use contract_threads::{LibraryJson, MessageInvalidError, MessageStatus};

use super::close::CloseKind;
use super::content::{invalid, refusal_of};
use crate::command::Refusal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Completed,
    BudgetExhausted,
    Cancelled,
    Interrupted,
    Failed,
}

impl Kind {
    pub fn of(reason: &ClosureReason) -> Option<Self> {
        Some(match reason {
            ClosureReason::Completed { .. } => Kind::Completed,
            ClosureReason::BudgetExhausted { .. } => Kind::BudgetExhausted,
            ClosureReason::Cancelled { .. } => Kind::Cancelled,
            ClosureReason::Interrupted { .. } => Kind::Interrupted,
            ClosureReason::Failed { .. } => Kind::Failed,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Completed => "completed",
            Kind::BudgetExhausted => "budget_exhausted",
            Kind::Cancelled => "cancelled",
            Kind::Interrupted => "interrupted",
            Kind::Failed => "failed",
        }
    }

    pub fn parse(wire: &str) -> Option<Self> {
        [
            Kind::Completed,
            Kind::BudgetExhausted,
            Kind::Cancelled,
            Kind::Interrupted,
            Kind::Failed,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == wire)
    }

    pub fn status(self) -> MessageStatus {
        match self {
            Kind::Completed | Kind::BudgetExhausted => MessageStatus::Completed,
            Kind::Cancelled => MessageStatus::Cancelled,
            Kind::Interrupted | Kind::Failed => MessageStatus::Failed,
        }
    }

    pub fn gives_completed(self) -> bool {
        self.status() == MessageStatus::Completed
    }

    pub fn interruption(self) -> InterruptionReason {
        match self {
            Kind::Completed | Kind::Failed => InterruptionReason::failed(),
            Kind::Cancelled => InterruptionReason::cancelled(),
            Kind::BudgetExhausted | Kind::Interrupted => InterruptionReason::interrupted(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Named {
    reason: ClosureReason,
    kind: Kind,
}

impl Named {
    pub fn of(reason: ClosureReason) -> Result<Self, Refusal> {
        match Kind::of(&reason) {
            Some(kind) => Ok(Self { reason, kind }),
            None => Err(invalid(MessageInvalidError::LibraryRefusal)),
        }
    }

    pub fn reason(&self) -> &ClosureReason {
        &self.reason
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }
}

pub fn parse(json: &LibraryJson) -> Result<Named, Refusal> {
    Named::of(
        json.to_closure_reason()
            .map_err(|error| refusal_of(&error))?,
    )
}

#[derive(Debug, Clone, PartialEq)]
pub struct Closing {
    pub kind: CloseKind,
    pub named: Option<Named>,
}

impl Closing {
    pub fn of(kind: CloseKind) -> Self {
        Self { kind, named: None }
    }

    pub fn naming(kind: CloseKind, named: Option<Named>) -> Self {
        Self { kind, named }
    }

    pub fn requested(&self) -> Kind {
        match (&self.named, self.kind) {
            (Some(named), _) => named.kind(),
            (None, CloseKind::Complete) => Kind::Completed,
            (None, CloseKind::Fail) => Kind::Failed,
            (None, CloseKind::Cancel) => Kind::Cancelled,
            (None, CloseKind::Watch) => Kind::Interrupted,
        }
    }

    pub fn strict(&self) -> bool {
        match self.kind {
            CloseKind::Complete => true,
            CloseKind::Fail => self.requested().gives_completed(),
            CloseKind::Cancel | CloseKind::Watch => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use contract_threads::llm::InterruptionKind;
    use serde_json::json;

    use super::*;

    fn named(json: serde_json::Value) -> ClosureReason {
        parse(&LibraryJson::from_json(json)).unwrap().reason
    }

    #[test]
    fn the_status_follows_the_reason() {
        let table = [
            (Kind::Completed, MessageStatus::Completed),
            (Kind::BudgetExhausted, MessageStatus::Completed),
            (Kind::Cancelled, MessageStatus::Cancelled),
            (Kind::Interrupted, MessageStatus::Failed),
            (Kind::Failed, MessageStatus::Failed),
        ];
        for (kind, status) in table {
            assert_eq!(kind.status(), status, "{kind:?}");
            assert_eq!(kind.gives_completed(), status == MessageStatus::Completed);
        }
    }

    #[test]
    fn a_response_still_produced_is_interrupted_with_the_kind_of_its_reason() {
        let table = [
            (Kind::Completed, InterruptionKind::Failed),
            (Kind::Failed, InterruptionKind::Failed),
            (Kind::Cancelled, InterruptionKind::Cancelled),
            (Kind::BudgetExhausted, InterruptionKind::Interrupted),
            (Kind::Interrupted, InterruptionKind::Interrupted),
        ];
        for (kind, interruption) in table {
            assert_eq!(kind.interruption().kind(), interruption, "{kind:?}");
        }
    }

    #[test]
    fn the_five_reasons_of_the_library_are_all_known() {
        for (wire, kind) in [
            (json!({ "type": "completed" }), Kind::Completed),
            (
                json!({ "type": "budget_exhausted", "detail": "limit" }),
                Kind::BudgetExhausted,
            ),
            (json!({ "type": "cancelled" }), Kind::Cancelled),
            (json!({ "type": "interrupted" }), Kind::Interrupted),
            (json!({ "type": "failed", "detail": "boom" }), Kind::Failed),
        ] {
            assert_eq!(Kind::of(&named(wire)), Some(kind));
            assert_eq!(Kind::parse(kind.as_str()), Some(kind));
            assert_eq!(
                kind.as_str(),
                serde_json::to_value(kind_reason(kind)).unwrap()["type"]
            );
        }
    }

    fn kind_reason(kind: Kind) -> ClosureReason {
        match kind {
            Kind::Completed => ClosureReason::completed(),
            Kind::BudgetExhausted => ClosureReason::budget_exhausted(),
            Kind::Cancelled => ClosureReason::cancelled(),
            Kind::Interrupted => ClosureReason::interrupted(),
            Kind::Failed => ClosureReason::failed("x").unwrap(),
        }
    }

    #[test]
    fn a_closure_the_library_cannot_read_is_message_invalid_with_its_code() {
        for wire in [
            json!({ "type": "failed" }),
            json!({ "type": "failed", "detail": "  " }),
            json!({ "type": "no_such_reason" }),
        ] {
            let refusal = parse(&LibraryJson::from_json(wire)).unwrap_err();
            assert_eq!(refusal.code, contract_threads::ReasonCode::MessageInvalid);
        }
    }

    #[test]
    fn only_a_completion_or_a_failure_that_gives_completed_refuses_an_unready_artifact() {
        let strict = |kind, named| Closing::naming(kind, named).strict();
        assert!(strict(CloseKind::Complete, None));
        let reason = |reason| Some(Named::of(reason).unwrap());
        assert!(strict(
            CloseKind::Complete,
            reason(ClosureReason::cancelled())
        ));
        assert!(strict(CloseKind::Fail, reason(ClosureReason::completed())));
        assert!(strict(
            CloseKind::Fail,
            reason(ClosureReason::budget_exhausted())
        ));
        assert!(!strict(CloseKind::Fail, None));
        assert!(!strict(
            CloseKind::Fail,
            reason(ClosureReason::interrupted())
        ));
        assert!(!strict(CloseKind::Cancel, None));
        assert!(!strict(CloseKind::Watch, None));
    }

    #[test]
    fn the_requested_reason_defaults_by_command() {
        assert_eq!(
            Closing::of(CloseKind::Complete).requested(),
            Kind::Completed
        );
        assert_eq!(Closing::of(CloseKind::Fail).requested(), Kind::Failed);
        assert_eq!(Closing::of(CloseKind::Cancel).requested(), Kind::Cancelled);
        assert_eq!(Closing::of(CloseKind::Watch).requested(), Kind::Interrupted);
    }
}
