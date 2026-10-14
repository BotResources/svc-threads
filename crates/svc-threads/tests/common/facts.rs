//! `threads.fact` — the service's one append-only fact table (nouns `thread`,
//! `message`, `artifact`), which the specification names as an observable in
//! its own right: every change of an object lands there in the transaction of
//! the gesture that made it, with who sent it, the user whose gesture led to it
//! and, for a message, the impersonator of its author. A fact never carries a
//! message's content, nor a name.
//!
//! Read-only, on the app role. The suite never writes a fact.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Fact {
    pub noun: String,
    pub key: Value,
    pub seq: i64,
    pub version: i32,
    pub event_type: String,
    pub payload: Value,
    pub actor_id: Uuid,
    pub actor_kind: String,
    pub acting_user_id: Option<Uuid>,
    pub impersonator_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub occurred_at: DateTime<Utc>,
}

const COLUMNS: &str = "noun, key, seq, version, event_type, payload, actor_id, actor_kind, \
     acting_user_id, impersonator_id, correlation_id, causation_id, occurred_at";

/// Every fact, in insertion order (the id is a UUIDv7 minted at insert).
pub async fn all(pool: &PgPool) -> Vec<Fact> {
    sqlx::query_as(&format!("SELECT {COLUMNS} FROM threads.fact ORDER BY id"))
        .fetch_all(pool)
        .await
        .expect("read threads.fact")
}

/// The facts of one UUID-keyed object, in `seq` order.
pub async fn of(pool: &PgPool, noun: &str, id: Uuid) -> Vec<Fact> {
    sqlx::query_as(&format!(
        "SELECT {COLUMNS} FROM threads.fact WHERE noun = $1 AND key = $2 ORDER BY seq"
    ))
    .bind(noun)
    .bind(Value::String(id.to_string()))
    .fetch_all(pool)
    .await
    .expect("read an object's facts")
}

pub async fn count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM threads.fact")
        .fetch_one(pool)
        .await
        .expect("count threads.fact")
}

/// The event types of `facts`, in order.
pub fn types(facts: &[Fact]) -> Vec<&str> {
    facts.iter().map(|fact| fact.event_type.as_str()).collect()
}

/// The one fact of `event_type` among `facts`.
pub fn the<'a>(facts: &'a [Fact], event_type: &str) -> &'a Fact {
    let found: Vec<&Fact> = facts
        .iter()
        .filter(|fact| fact.event_type == event_type)
        .collect();
    assert_eq!(
        found.len(),
        1,
        "exactly one {event_type} fact expected among {:?}",
        types(facts)
    );
    found[0]
}

/// No fact, of any object, carries `needle` anywhere in its payload: a
/// message's text, a person's name.
pub async fn assert_no_fact_carries(pool: &PgPool, needle: &str, what: &str) {
    for fact in all(pool).await {
        assert!(
            !fact.payload.to_string().contains(needle),
            "{what}: the {} fact of {} {} carries {needle:?}: {}",
            fact.event_type,
            fact.noun,
            fact.key,
            fact.payload
        );
    }
}
