//! Reading the GraphQL answers: a refusal's code, an affordance, the data of a
//! root.

use serde_json::Value;

/// The code of the first error of an answer, if it carries one.
pub fn code(answer: &Value) -> Option<String> {
    answer["errors"][0]["extensions"]["code"]
        .as_str()
        .map(str::to_string)
}

/// The answer's data under `root`, asserting it carries no error.
pub fn data<'a>(answer: &'a Value, root: &str) -> &'a Value {
    assert!(
        answer["errors"].is_null(),
        "{root} answered an error: {answer}"
    );
    &answer["data"][root]
}

/// The answer is refused with `expected`.
pub fn assert_refused(answer: &Value, expected: &str) {
    assert_eq!(
        code(answer).as_deref(),
        Some(expected),
        "expected a refusal {expected}: {answer}"
    );
}

/// An affordance of a view: `{ allowed, reason? }`.
pub fn affordance<'a>(view: &'a Value, action: &str) -> &'a Value {
    &view["affordances"][action]
}

pub fn assert_allowed(view: &Value, action: &str) {
    assert_eq!(
        affordance(view, action)["allowed"],
        Value::Bool(true),
        "{action} should be allowed: {view}"
    );
}

pub fn assert_blocked(view: &Value, action: &str, reason: &str) {
    let gate = affordance(view, action);
    assert_eq!(gate["allowed"], Value::Bool(false), "{action}: {view}");
    assert_eq!(
        gate["reason"],
        Value::String(reason.to_string()),
        "{action}: {view}"
    );
}
