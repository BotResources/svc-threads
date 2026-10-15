//! `threads.fact`: the one fact log of the service, soft EDA. Every change of a
//! thread, a message or an artifact appends its facts in the transaction that
//! writes its state row. A fact names who acted and never carries the content of
//! a message nor a display name.
//!
//! Each object numbers its facts `seq` = 1, 2, … and stamps each with the
//! object's `version` after the change that recorded it: one change may record
//! several facts (a creation records `Created` then `MembersAdded`), all under
//! one version. The unique `(noun, key, seq)` makes two writers of one object
//! collide loudly instead of losing a fact.

use br_core_integration::Actor;
use chrono::{DateTime, Utc};
use service_engine::error::EngineError;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::principal::ThreadsPrincipal;

/// The object a fact is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactNoun {
    Thread,
    Message,
    Artifact,
}

impl FactNoun {
    pub const fn as_db(self) -> &'static str {
        match self {
            Self::Thread => "thread",
            Self::Message => "message",
            Self::Artifact => "artifact",
        }
    }
}

/// Who and what caused a change: the frontier's actor (a commanding service, a
/// runner, Threads itself), the user whose gesture led to it, the impersonator
/// of a message's author, and the message that caused it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactMeta {
    pub actor: Actor,
    pub acting_user_id: Option<Uuid>,
    pub impersonator_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub occurred_at: DateTime<Utc>,
}

impl FactMeta {
    /// A change a principal made through a GraphQL gesture.
    pub fn of_principal(principal: &ThreadsPrincipal, occurred_at: DateTime<Utc>) -> Self {
        use service_engine::principal::Principal;
        Self {
            actor: principal.passport().to_actor(),
            acting_user_id: None,
            impersonator_id: None,
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            occurred_at,
        }
    }

    pub fn with_acting_user(mut self, acting_user_id: Option<Uuid>) -> Self {
        self.acting_user_id = acting_user_id;
        self
    }

    pub fn with_impersonator(mut self, impersonator_id: Option<Uuid>) -> Self {
        self.impersonator_id = impersonator_id;
        self
    }
}

/// One fact to append: its type (the stable `{Fact}` name) and its typed
/// payload, already serialized.
#[derive(Debug, Clone, PartialEq)]
pub struct Fact {
    pub event_type: &'static str,
    pub payload: serde_json::Value,
    pub meta: FactMeta,
}

impl Fact {
    pub fn new<P: serde::Serialize>(
        event_type: &'static str,
        payload: &P,
        meta: &FactMeta,
    ) -> Result<Self, EngineError> {
        Ok(Self {
            event_type,
            payload: serde_json::to_value(payload).map_err(|source| EngineError::Encode {
                what: "a fact payload",
                source,
            })?,
            meta: meta.clone(),
        })
    }
}

/// Appends `facts` to the log of `(noun, key)`, each at `version`, numbered
/// after the last fact the object holds. Two statements, whatever the count.
pub async fn append(
    conn: &mut PgConnection,
    noun: FactNoun,
    key: Uuid,
    version: i64,
    facts: &[Fact],
) -> Result<(), EngineError> {
    if facts.is_empty() {
        return Ok(());
    }
    let key_json = serde_json::Value::String(key.to_string());
    let last: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(seq), 0) FROM threads.fact WHERE noun = $1 AND key = $2",
    )
    .bind(noun.as_db())
    .bind(&key_json)
    .fetch_one(&mut *conn)
    .await?;
    let version = i32::try_from(version).map_err(|_| {
        EngineError::Service(
            format!("the version {version} of a {} overflows", noun.as_db()).into(),
        )
    })?;

    let count = facts.len();
    let mut ids = Vec::with_capacity(count);
    let mut seqs = Vec::with_capacity(count);
    let mut types = Vec::with_capacity(count);
    let mut payloads = Vec::with_capacity(count);
    let mut actors = Vec::with_capacity(count);
    let mut kinds = Vec::with_capacity(count);
    let mut acting = Vec::with_capacity(count);
    let mut impersonators = Vec::with_capacity(count);
    let mut correlations = Vec::with_capacity(count);
    let mut causations = Vec::with_capacity(count);
    let mut at = Vec::with_capacity(count);
    for (offset, fact) in facts.iter().enumerate() {
        ids.push(Uuid::now_v7());
        seqs.push(last + offset as i64 + 1);
        types.push(fact.event_type);
        payloads.push(fact.payload.clone());
        actors.push(fact.meta.actor.id());
        kinds.push(if fact.meta.actor.is_human() {
            "human"
        } else {
            "service"
        });
        acting.push(fact.meta.acting_user_id);
        impersonators.push(fact.meta.impersonator_id);
        correlations.push(fact.meta.correlation_id);
        causations.push(fact.meta.causation_id);
        at.push(fact.meta.occurred_at);
    }
    sqlx::query(
        "INSERT INTO threads.fact (id, noun, key, seq, version, event_type, payload, actor_id, \
             actor_kind, acting_user_id, impersonator_id, correlation_id, causation_id, \
             occurred_at) \
         SELECT f.id, $2, $3, f.seq, $4, f.event_type, f.payload, f.actor_id, f.actor_kind, \
                f.acting_user_id, f.impersonator_id, f.correlation_id, f.causation_id, \
                f.occurred_at \
         FROM unnest($1::uuid[], $5::bigint[], $6::text[], $7::jsonb[], $8::uuid[], $9::text[], \
                     $10::uuid[], $11::uuid[], $12::uuid[], $13::uuid[], $14::timestamptz[]) \
              AS f(id, seq, event_type, payload, actor_id, actor_kind, acting_user_id, \
                   impersonator_id, correlation_id, causation_id, occurred_at)",
    )
    .bind(&ids)
    .bind(noun.as_db())
    .bind(&key_json)
    .bind(version)
    .bind(&seqs)
    .bind(&types)
    .bind(&payloads)
    .bind(&actors)
    .bind(&kinds)
    .bind(&acting)
    .bind(&impersonators)
    .bind(&correlations)
    .bind(&causations)
    .bind(&at)
    .execute(conn)
    .await?;
    Ok(())
}
