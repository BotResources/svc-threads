//! The upgrade of the messages an earlier Threads stored (`br-llm-messages/3`,
//! in the 0.4 decomposition kept under `message_item` / `message_block`) to the
//! library's 2.0 model, once, at boot, before the service is ready.
//!
//! - Multi-pod: the pods serialize on one Postgres session advisory lock
//!   ([`LOCK`]); a pod that waited finds nothing left to convert.
//! - Judged first, then written: every message still in the 0.4 form is read
//!   and converted in memory, one at a time; a message that cannot be converted
//!   stops the upgrade before anything is written, so that no message is
//!   converted while another cannot be.
//! - Then one transaction per message, the message's row locked and its marker
//!   (`content_schema`) re-read under the lock: a message is converted whole or
//!   not at all, and a crash resumes where it stopped. Idempotent: a converted
//!   message is never converted again.
//! - The mapping is [`super::v1`]'s, with the library's constructors: message
//!   ids (= turn ids) and block ids kept, entry and closure ids derived. The
//!   message's status becomes its turn's closure; a message still `STREAMING`
//!   (its 0.4 chunks, under the accumulator `message_content`, are never read
//!   again) is closed interrupted and becomes `FAILED`.
//! - No fact is added and no version raised: the conversion changes no
//!   conversation.
//! - A message that cannot be converted is logged by its id and left as it
//!   was; the upgrade then fails, and the service with it: it never becomes
//!   ready while a message is left unconverted.

use contract_threads::llm::{
    AITurn, AgentId, Author, ClosureReason, HumanId, Turn, TurnId, TurnKind,
};
use contract_threads::v1::{Block, Item, StepItem, ToolResultsItem};
use contract_threads::{MessageStatus, ReasonCode};
use service_engine::error::EngineError;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::content::Content;
use super::derived;
use super::draft::closure_entry;
use super::store::{self, CONTENT_SCHEMA};
use super::v1::{self, V1Fault};

/// The schema of the content of a message an earlier Threads stored.
pub const LEGACY_SCHEMA: &str = "br-llm-messages/3";

/// The session advisory lock the pods of Threads take to upgrade, one at a
/// time: `threads/upgrade/5` as an integer, a value no engine lock domain uses
/// (the engine's keys are FNV hashes under their own domain tag).
pub const LOCK: i64 = 0x7468_7265_6164_7305;

/// How many messages one read of the pending ones names.
const BATCH: i64 = 256;

/// What an upgrade did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Upgraded {
    pub converted: usize,
}

/// A 0.4 message, as its rows read.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacyMessage {
    pub id: Uuid,
    pub author_user_id: Option<Uuid>,
    pub author_ai_id: Option<String>,
    pub author_ai_name: Option<String>,
    pub status: MessageStatus,
    pub deleted: bool,
    /// A user message's blocks.
    pub blocks: Vec<Block>,
    /// An AI message's items.
    pub items: Vec<Item>,
}

/// What a 0.4 message becomes.
#[derive(Debug, Clone, PartialEq)]
pub struct Converted {
    pub turn_kind: TurnKind,
    pub agent: Option<AgentId>,
    pub status: MessageStatus,
    pub content: Content,
}

/// The 2.0 form of a 0.4 message. Pure.
pub fn convert(message: &LegacyMessage) -> Result<Converted, V1Fault> {
    match (
        &message.author_user_id,
        &message.author_ai_id,
        &message.author_ai_name,
    ) {
        (Some(user), None, None) => {
            if message.deleted {
                return Ok(Converted {
                    turn_kind: TurnKind::InputTurn,
                    agent: None,
                    status: message.status,
                    content: Content::default(),
                });
            }
            HumanId::from_uuid(*user)?;
            let (turn, annotations) = v1::user_turn(message.id, *user, &message.blocks)?;
            Ok(Converted {
                turn_kind: TurnKind::InputTurn,
                agent: None,
                status: message.status,
                content: Content::of_turn(&turn.into(), annotations)?,
            })
        }
        (None, Some(agent), Some(name)) => {
            let agent = AgentId::parse(agent)?;
            let author = Author::new(name.clone())?;
            let (mut turn, annotations, status, closure) = match message.status {
                MessageStatus::Streaming => (
                    AITurn::open(TurnId::from_uuid(message.id)?, agent, author),
                    Vec::new(),
                    MessageStatus::Failed,
                    ClosureReason::interrupted(),
                ),
                status => {
                    let (turn, annotations) =
                        v1::ai_turn(message.id, agent, author, &message.items)?;
                    let closure = match status {
                        MessageStatus::Completed => ClosureReason::completed(),
                        MessageStatus::Cancelled => ClosureReason::cancelled(),
                        MessageStatus::Failed | MessageStatus::Streaming => {
                            ClosureReason::failed("failed")?
                        }
                    };
                    (turn, annotations, status, closure)
                }
            };
            let closure_id = derived::derive(message.id, derived::CLOSURE);
            let closure =
                closure_entry(closure_id, closure).map_err(|_| V1Fault::NotConvertible {
                    object_id: Some(closure_id),
                })?;
            turn.push_entry(closure)?;
            Ok(Converted {
                turn_kind: TurnKind::AITurn,
                agent: Some(agent),
                status,
                content: Content::of_turn(&Turn::AITurn(turn), annotations)?,
            })
        }
        _ => Err(V1Fault::NotConvertible {
            object_id: Some(message.id),
        }),
    }
}

/// Converts every message still in the 0.4 form, under the upgrade lock.
pub async fn run(pool: &PgPool) -> Result<Upgraded, EngineError> {
    let mut conn = pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(LOCK)
        .execute(&mut *conn)
        .await?;
    let outcome = convert_all(&mut conn).await;
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(LOCK)
        .execute(&mut *conn)
        .await?;
    outcome
}

async fn convert_all(conn: &mut PgConnection) -> Result<Upgraded, EngineError> {
    let refused = judge_all(conn).await?;
    if !refused.is_empty() {
        for (id, fault) in &refused {
            tracing::error!(
                message_id = %id,
                error = %fault,
                "a message stored by an earlier Threads cannot be converted to \
                 br-llm-messages/5; no message is converted and the service stays not ready"
            );
        }
        let ids: Vec<String> = refused.iter().map(|(id, _)| id.to_string()).collect();
        return Err(EngineError::Service(
            format!(
                "{} message(s) stored by an earlier Threads cannot be converted to \
                 br-llm-messages/5: {}",
                refused.len(),
                ids.join(", ")
            )
            .into(),
        ));
    }
    let mut upgraded = Upgraded::default();
    let mut after = Uuid::nil();
    while let Some(pending) = next_pending(conn, after).await? {
        after = *pending.last().unwrap_or(&after);
        for id in pending {
            match convert_one(conn, id).await? {
                Ok(true) => upgraded.converted += 1,
                Ok(false) => {}
                Err(fault) => {
                    return Err(EngineError::Service(
                        format!("message {id} cannot be converted to br-llm-messages/5: {fault}")
                            .into(),
                    ));
                }
            }
        }
    }
    if upgraded.converted > 0 {
        tracing::info!(
            converted = upgraded.converted,
            "the messages stored by an earlier Threads are converted to br-llm-messages/5"
        );
    }
    Ok(upgraded)
}

/// The next batch of messages still in the 0.4 form after `after`, by id.
async fn next_pending(
    conn: &mut PgConnection,
    after: Uuid,
) -> Result<Option<Vec<Uuid>>, EngineError> {
    let pending: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM threads.message WHERE content_schema = $1 AND id > $2 \
         ORDER BY id LIMIT $3",
    )
    .bind(LEGACY_SCHEMA)
    .bind(after)
    .bind(BATCH)
    .fetch_all(&mut *conn)
    .await?;
    Ok((!pending.is_empty()).then_some(pending))
}

/// Every message still in the 0.4 form converted in memory, one at a time,
/// nothing written: those that cannot be, with why.
async fn judge_all(conn: &mut PgConnection) -> Result<Vec<(Uuid, String)>, EngineError> {
    let mut refused = Vec::new();
    let mut after = Uuid::nil();
    while let Some(pending) = next_pending(conn, after).await? {
        after = *pending.last().unwrap_or(&after);
        for id in pending {
            let mut tx = sqlx::Connection::begin(&mut *conn).await?;
            let loaded = load(&mut tx, id, false).await?;
            tx.rollback().await?;
            let verdict = match loaded {
                None => continue,
                Some(Ok(message)) => convert(&message)
                    .map(drop)
                    .map_err(|fault| fault.to_string()),
                Some(Err(unreadable)) => Err(unreadable),
            };
            if let Err(fault) = verdict {
                refused.push((id, fault));
            }
        }
    }
    Ok(refused)
}

/// Converts one message in its own transaction: `Ok(true)` when it converted
/// it, `Ok(false)` when another pod already had, `Err` naming why it cannot —
/// the transaction then rolled back.
async fn convert_one(
    conn: &mut PgConnection,
    id: Uuid,
) -> Result<Result<bool, String>, EngineError> {
    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
    let Some(message) = load(&mut tx, id, true).await? else {
        tx.rollback().await?;
        return Ok(Ok(false));
    };
    let converted = match message {
        Ok(message) => convert(&message).map_err(|fault| fault.to_string()),
        Err(unreadable) => Err(unreadable),
    };
    let converted = match converted {
        Ok(converted) => converted,
        Err(fault) => {
            tx.rollback().await?;
            return Ok(Err(fault));
        }
    };
    match write(&mut tx, id, &converted).await {
        Ok(()) => {
            tx.commit().await?;
            Ok(Ok(true))
        }
        // An id of the converted content another message already holds, a row
        // the schema refuses: this message cannot be converted.
        Err(EngineError::Db(sqlx::Error::Database(error))) => {
            tx.rollback().await?;
            Ok(Err(format!(
                "{} {}",
                ReasonCode::KeyReused,
                error.message()
            )))
        }
        Err(other) => Err(other),
    }
}

/// The message's 0.4 rows, its row locked when `lock`; `None` when it is
/// converted already; `Some(Err)` when its rows are not the 0.4 form.
async fn load(
    conn: &mut PgConnection,
    id: Uuid,
    lock: bool,
) -> Result<Option<Result<LegacyMessage, String>>, EngineError> {
    let lock = if lock { " FOR UPDATE" } else { "" };
    let Some(row) = sqlx::query(&format!(
        "SELECT id, author_user_id, author_ai_id, author_ai_name, status, deleted \
         FROM threads.message WHERE id = $1 AND content_schema = $2{lock}"
    ))
    .bind(id)
    .bind(LEGACY_SCHEMA)
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    let status: String = row.try_get("status")?;
    let Ok(status) = MessageStatus::parse(&status) else {
        return Ok(Some(Err(format!("unknown status `{status}`"))));
    };
    let items = sqlx::query(
        "SELECT index, kind, stop_reason, usage, model FROM threads.message_item \
         WHERE message_id = $1 ORDER BY index",
    )
    .bind(id)
    .fetch_all(&mut *conn)
    .await?;
    let blocks = sqlx::query(
        "SELECT id, item_index, kind, content FROM threads.message_block \
         WHERE message_id = $1 ORDER BY item_index NULLS FIRST, index",
    )
    .bind(id)
    .fetch_all(&mut *conn)
    .await?;
    let mut user_blocks = Vec::new();
    let mut by_item: std::collections::BTreeMap<i32, Vec<Block>> = Default::default();
    for row in &blocks {
        let block_id: Uuid = row.try_get("id")?;
        let kind: String = row.try_get("kind")?;
        let content: serde_json::Value = row.try_get("content")?;
        let Ok(block) = serde_json::from_value::<Block>(
            serde_json::json!({ "id": block_id, "kind": kind, "content": content }),
        ) else {
            return Ok(Some(Err(format!(
                "block {block_id} is not of the 0.4 form"
            ))));
        };
        let item: Option<i32> = row.try_get("item_index")?;
        match item {
            None => user_blocks.push(block),
            Some(item) => by_item.entry(item).or_default().push(block),
        }
    }
    let mut converted_items = Vec::with_capacity(items.len());
    for row in &items {
        let index: i32 = row.try_get("index")?;
        let kind: String = row.try_get("kind")?;
        let blocks = by_item.remove(&index).unwrap_or_default();
        converted_items.push(match kind.as_str() {
            "STEP" => {
                let stop_reason: Option<serde_json::Value> = row.try_get("stop_reason")?;
                let Some(stop_reason) = stop_reason else {
                    return Ok(Some(Err(format!("step {index} has no stop reason"))));
                };
                Item::Step(StepItem {
                    stop_reason,
                    usage: row.try_get("usage")?,
                    model: row.try_get("model")?,
                    blocks,
                })
            }
            "TOOL_RESULTS" => Item::ToolResults(ToolResultsItem { blocks }),
            other => return Ok(Some(Err(format!("unknown item kind `{other}`")))),
        });
    }
    Ok(Some(Ok(LegacyMessage {
        id,
        author_user_id: row.try_get("author_user_id")?,
        author_ai_id: row.try_get("author_ai_id")?,
        author_ai_name: row.try_get("author_ai_name")?,
        status,
        deleted: row.try_get("deleted")?,
        blocks: user_blocks,
        items: converted_items,
    })))
}

/// Writes what the message became: its turn row marked converted (the 0.4 id
/// of its AI author cleared, its agent id set), its records, annotations and
/// identities; a stream left open is no longer watched.
async fn write(
    conn: &mut PgConnection,
    id: Uuid,
    converted: &Converted,
) -> Result<(), EngineError> {
    sqlx::query(
        "UPDATE threads.message SET content_schema = $2, turn_kind = $3, \
             author_agent_id = $4, author_ai_id = NULL, status = $5 \
         WHERE id = $1",
    )
    .bind(id)
    .bind(CONTENT_SCHEMA)
    .bind(converted.turn_kind.as_str())
    .bind(converted.agent.map(|agent| agent.as_uuid()))
    .bind(converted.status.as_str())
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM threads.stream_watch WHERE message_id = $1")
        .bind(id)
        .execute(&mut *conn)
        .await?;
    store::insert_identities(conn, id, &[id]).await?;
    store::write_content(conn, id, &converted.content).await
}

#[cfg(test)]
mod tests {
    use contract_threads::v1::BlockBody;
    use contract_threads::{AnnotationKind, Notice};
    use serde_json::json;

    use super::*;

    fn block(kind: &str, content: serde_json::Value) -> Block {
        serde_json::from_value(json!({ "id": Uuid::now_v7(), "kind": kind, "content": content }))
            .unwrap()
    }

    fn legacy(status: MessageStatus) -> LegacyMessage {
        LegacyMessage {
            id: Uuid::now_v7(),
            author_user_id: None,
            author_ai_id: Some(Uuid::now_v7().to_string()),
            author_ai_name: Some("Analyst".into()),
            status,
            deleted: false,
            blocks: Vec::new(),
            items: vec![Item::Step(StepItem {
                stop_reason: json!({ "type": "end_turn" }),
                usage: None,
                model: Some("m-1".into()),
                blocks: vec![
                    block("TEXT", json!({ "type": "text", "text": "done" })),
                    Block::new(
                        Uuid::now_v7(),
                        BlockBody::Warning(Notice {
                            code: "W".into(),
                            text: None,
                        }),
                    ),
                ],
            })],
        }
    }

    fn kinds(content: &Content) -> Vec<&'static str> {
        content.entries.iter().map(|e| e.kind().as_str()).collect()
    }

    #[test]
    fn a_completed_ai_message_becomes_its_turn_closed_completed() {
        let message = legacy(MessageStatus::Completed);
        let converted = convert(&message).unwrap();
        assert_eq!(converted.status, MessageStatus::Completed);
        assert_eq!(converted.turn_kind, TurnKind::AITurn);
        assert_eq!(
            kinds(&converted.content),
            ["model_response", "turn_closure"]
        );
        assert_eq!(
            converted.content.annotations[0].annotation.kind(),
            AnnotationKind::Warning
        );
        assert_eq!(
            converted.content.entries[1].id().as_uuid(),
            derived::derive(message.id, derived::CLOSURE)
        );
        assert_eq!(
            convert(&message).unwrap(),
            converted,
            "the conversion is deterministic"
        );
    }

    #[test]
    fn a_message_still_streaming_becomes_failed_closed_interrupted_and_empty() {
        let converted = convert(&legacy(MessageStatus::Streaming)).unwrap();
        assert_eq!(converted.status, MessageStatus::Failed);
        assert_eq!(kinds(&converted.content), ["turn_closure"]);
        assert!(!converted.content.has_an_entry());
    }

    #[test]
    fn a_cancelled_or_failed_message_keeps_its_content_under_its_closure() {
        for status in [MessageStatus::Cancelled, MessageStatus::Failed] {
            let converted = convert(&legacy(status)).unwrap();
            assert_eq!(converted.status, status);
            assert_eq!(
                kinds(&converted.content),
                ["model_response", "turn_closure"]
            );
        }
    }

    #[test]
    fn a_user_message_becomes_its_human_message_and_a_deleted_one_stays_empty() {
        let user = Uuid::now_v7();
        let text = block("TEXT", json!({ "type": "text", "text": "Hello" }));
        let mut message = LegacyMessage {
            id: Uuid::now_v7(),
            author_user_id: Some(user),
            author_ai_id: None,
            author_ai_name: None,
            status: MessageStatus::Completed,
            deleted: false,
            blocks: vec![text.clone()],
            items: Vec::new(),
        };
        let converted = convert(&message).unwrap();
        assert_eq!(converted.turn_kind, TurnKind::InputTurn);
        assert_eq!(kinds(&converted.content), ["human_message"]);
        assert_eq!(converted.content.blocks[0].id().as_uuid(), text.id);
        message.deleted = true;
        message.blocks.clear();
        assert!(convert(&message).unwrap().content.is_empty());
    }

    #[test]
    fn an_ai_author_whose_id_is_no_uuidv7_or_a_citation_cannot_be_converted() {
        let mut message = legacy(MessageStatus::Completed);
        message.author_ai_id = Some("a1".into());
        assert!(convert(&message).is_err());
        let mut message = legacy(MessageStatus::Completed);
        let Item::Step(step) = &mut message.items[0] else {
            unreachable!()
        };
        step.blocks.push(block(
            "CITATION",
            json!({ "text": "a quote", "source_ids": [] }),
        ));
        assert!(convert(&message).is_err());
    }

    #[test]
    fn the_lock_is_its_own() {
        assert_ne!(LOCK, 0);
    }
}
