//! The upgrade of the messages an earlier Threads stored (`br-llm-messages/3`,
//! in the 0.4 decomposition kept under `message_item` / `message_block`) to the
//! library's 2.0 model, once, at boot, before the service is ready.
//!
//! - Multi-pod: the pods serialize on one Postgres session advisory lock
//!   ([`LOCK`]); a pod that waited finds nothing left to convert.
//! - Judged first, then written: every message still in the 0.4 form is read
//!   and converted in memory, a batch of [`BATCH`] at a time (one statement per
//!   family — the messages, their items, their blocks — per batch), and every id
//!   the conversions declare is checked once in the thread's sense — named once
//!   across them, and held by no message already in the 2.0 form (one
//!   statement per batch). A message that cannot be converted, or whose ids
//!   clash, stops the upgrade before anything is written, so that no message is
//!   converted while another cannot be and the writes cannot stop half-way.
//!   The ids judged are kept in memory until the writes start: about 50 bytes
//!   per id.
//! - Then one transaction per batch, its rows locked and their marker
//!   (`content_schema`) re-read under the lock, read the same way: a batch is
//!   converted whole or not at all, and a crash resumes where it stopped.
//!   Idempotent: a converted message is never converted again.
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

use std::collections::{BTreeMap, HashMap};

use contract_threads::llm::{
    AITurn, AgentId, Author, ClosureReason, HumanId, Turn, TurnId, TurnKind,
};
use contract_threads::v1::{Block, Item, StepItem, ToolResultsItem};
use contract_threads::{MessageStatus, ReasonCode};
use service_engine::error::EngineError;
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::content::{Content, first_repeated};
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

impl Converted {
    /// Every id the message declares once converted: its own (its turn's) and
    /// its content's.
    fn identities(&self, message: Uuid) -> Vec<Uuid> {
        let mut ids = vec![message];
        ids.extend(self.content.identities());
        ids
    }
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
        upgraded.converted += convert_batch(conn, &pending).await?;
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

/// Every message still in the 0.4 form converted in memory, batch by batch,
/// nothing written: those that cannot be, with why — a conversion refused, or
/// an id the converted messages name twice or another message already holds.
async fn judge_all(conn: &mut PgConnection) -> Result<Vec<(Uuid, String)>, EngineError> {
    let mut refused = Vec::new();
    // Every id the conversions declare, with the message declaring it.
    let mut declared: HashMap<Uuid, Uuid> = HashMap::new();
    let mut after = Uuid::nil();
    while let Some(pending) = next_pending(conn, after).await? {
        after = *pending.last().unwrap_or(&after);
        let mut batch = Vec::new();
        for (id, loaded) in load_batch(conn, &pending, false).await? {
            let converted = match loaded
                .and_then(|message| convert(&message).map_err(|fault| fault.to_string()))
            {
                Ok(converted) => converted,
                Err(fault) => {
                    refused.push((id, fault));
                    continue;
                }
            };
            let ids = converted.identities(id);
            if let Some(twice) = first_repeated(ids.iter().copied()) {
                refused.push((
                    id,
                    format!("{} id={twice} named twice", ReasonCode::KeyReused),
                ));
                continue;
            }
            for declared_id in ids {
                if let Some(other) = declared.insert(declared_id, id) {
                    refused.push((
                        id,
                        format!(
                            "{} id={declared_id} also declared by message {other}",
                            ReasonCode::KeyReused
                        ),
                    ));
                    break;
                }
                batch.push(declared_id);
            }
        }
        // An id a message already in the 2.0 form holds.
        for (held, holder) in store::holders(conn, &batch).await? {
            let claimant = declared.get(&held).copied().unwrap_or(holder);
            if claimant != holder {
                refused.push((
                    claimant,
                    format!(
                        "{} id={held} held by message {holder}",
                        ReasonCode::KeyReused
                    ),
                ));
            }
        }
    }
    Ok(refused)
}

/// Converts one batch in its own transaction, its rows locked and re-read: how
/// many it converted (none when another pod already had). A fault rolls the
/// batch back and stops the upgrade: the judgement made every message of it
/// convertible and every id free, so a fault here is the store's, logged with
/// its SQLSTATE.
async fn convert_batch(conn: &mut PgConnection, ids: &[Uuid]) -> Result<usize, EngineError> {
    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
    let mut converted = 0;
    for (id, loaded) in load_batch(&mut tx, ids, true).await? {
        let message = loaded.map_err(|fault| unconvertible(id, &fault))?;
        let into = convert(&message).map_err(|fault| unconvertible(id, &fault.to_string()))?;
        if let Err(error) = write(&mut tx, id, &into).await {
            return Err(store_fault(id, error));
        }
        converted += 1;
    }
    tx.commit().await?;
    Ok(converted)
}

fn unconvertible(id: Uuid, fault: &str) -> EngineError {
    EngineError::Service(
        format!("message {id} cannot be converted to br-llm-messages/5: {fault}").into(),
    )
}

/// A write the store refused, named by its SQLSTATE and constraint.
fn store_fault(id: Uuid, error: EngineError) -> EngineError {
    if let EngineError::Db(sqlx::Error::Database(db)) = &error {
        tracing::error!(
            message_id = %id,
            sqlstate = db.code().as_deref().unwrap_or("none"),
            constraint = db.constraint().unwrap_or("none"),
            error = db.message(),
            "the store refused a converted message; its batch is rolled back and the \
             service stays not ready"
        );
    }
    error
}

/// A 0.4 message's turn row, as `load_batch` reads it.
struct LegacyRow {
    id: Uuid,
    author_user_id: Option<Uuid>,
    author_ai_id: Option<String>,
    author_ai_name: Option<String>,
    status: String,
    deleted: bool,
}

/// The 0.4 rows of the messages of `ids` still in the 0.4 form, by id, their
/// rows locked when `lock`: three statements whatever the batch — the
/// messages, their items, their blocks. `Err` for a message whose rows are not
/// the 0.4 form.
async fn load_batch(
    conn: &mut PgConnection,
    ids: &[Uuid],
    lock: bool,
) -> Result<Vec<(Uuid, Result<LegacyMessage, String>)>, EngineError> {
    let lock = if lock { " FOR UPDATE" } else { "" };
    let rows = sqlx::query(&format!(
        "SELECT id, author_user_id, author_ai_id, author_ai_name, status, deleted \
         FROM threads.message WHERE id = ANY($1) AND content_schema = $2 ORDER BY id{lock}"
    ))
    .bind(ids)
    .bind(LEGACY_SCHEMA)
    .fetch_all(&mut *conn)
    .await?;
    let rows: Vec<LegacyRow> = rows
        .iter()
        .map(|row| {
            Ok(LegacyRow {
                id: row.try_get("id")?,
                author_user_id: row.try_get("author_user_id")?,
                author_ai_id: row.try_get("author_ai_id")?,
                author_ai_name: row.try_get("author_ai_name")?,
                status: row.try_get("status")?,
                deleted: row.try_get("deleted")?,
            })
        })
        .collect::<Result<_, sqlx::Error>>()?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let found: Vec<Uuid> = rows.iter().map(|row| row.id).collect();
    let items = sqlx::query(
        "SELECT message_id, index, kind, stop_reason, usage, model FROM threads.message_item \
         WHERE message_id = ANY($1) ORDER BY message_id, index",
    )
    .bind(&found)
    .fetch_all(&mut *conn)
    .await?;
    let blocks = sqlx::query(
        "SELECT id, message_id, item_index, kind, content FROM threads.message_block \
         WHERE message_id = ANY($1) ORDER BY message_id, item_index NULLS FIRST, index",
    )
    .bind(&found)
    .fetch_all(&mut *conn)
    .await?;
    let mut items_of: HashMap<Uuid, Vec<&PgRow>> = HashMap::new();
    for row in &items {
        items_of
            .entry(row.try_get("message_id")?)
            .or_default()
            .push(row);
    }
    let mut blocks_of: HashMap<Uuid, Vec<&PgRow>> = HashMap::new();
    for row in &blocks {
        blocks_of
            .entry(row.try_get("message_id")?)
            .or_default()
            .push(row);
    }
    rows.into_iter()
        .map(|row| {
            let id = row.id;
            let items = items_of.remove(&id).unwrap_or_default();
            let blocks = blocks_of.remove(&id).unwrap_or_default();
            Ok((id, assemble(row, &items, &blocks)?))
        })
        .collect()
}

/// One 0.4 message from its rows; `Err` when they are not the 0.4 form.
fn assemble(
    row: LegacyRow,
    items: &[&PgRow],
    blocks: &[&PgRow],
) -> Result<Result<LegacyMessage, String>, EngineError> {
    let Ok(status) = MessageStatus::parse(&row.status) else {
        return Ok(Err(format!("unknown status `{}`", row.status)));
    };
    let mut user_blocks = Vec::new();
    let mut by_item: BTreeMap<i32, Vec<Block>> = BTreeMap::new();
    for block in blocks {
        let block_id: Uuid = block.try_get("id")?;
        let kind: String = block.try_get("kind")?;
        let content: serde_json::Value = block.try_get("content")?;
        let Ok(parsed) = serde_json::from_value::<Block>(
            serde_json::json!({ "id": block_id, "kind": kind, "content": content }),
        ) else {
            return Ok(Err(format!("block {block_id} is not of the 0.4 form")));
        };
        let item: Option<i32> = block.try_get("item_index")?;
        match item {
            None => user_blocks.push(parsed),
            Some(item) => by_item.entry(item).or_default().push(parsed),
        }
    }
    let mut converted_items = Vec::with_capacity(items.len());
    for item in items {
        let index: i32 = item.try_get("index")?;
        let kind: String = item.try_get("kind")?;
        let blocks = by_item.remove(&index).unwrap_or_default();
        converted_items.push(match kind.as_str() {
            "STEP" => {
                let stop_reason: Option<serde_json::Value> = item.try_get("stop_reason")?;
                let Some(stop_reason) = stop_reason else {
                    return Ok(Err(format!("step {index} has no stop reason")));
                };
                Item::Step(StepItem {
                    stop_reason,
                    usage: item.try_get("usage")?,
                    model: item.try_get("model")?,
                    blocks,
                })
            }
            "TOOL_RESULTS" => Item::ToolResults(ToolResultsItem { blocks }),
            other => return Ok(Err(format!("unknown item kind `{other}`"))),
        });
    }
    Ok(Ok(LegacyMessage {
        id: row.id,
        author_user_id: row.author_user_id,
        author_ai_id: row.author_ai_id,
        author_ai_name: row.author_ai_name,
        status,
        deleted: row.deleted,
        blocks: user_blocks,
        items: converted_items,
    }))
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
    fn an_id_a_converted_message_declares_twice_is_seen_before_anything_is_written() {
        let mut message = legacy(MessageStatus::Completed);
        let Item::Step(step) = &mut message.items[0] else {
            unreachable!()
        };
        // A 0.4 Threads block under the id of a library block: the library
        // never sees the annotation it becomes, the identities do.
        let twice = step.blocks[0].id;
        step.blocks[1].id = twice;
        let converted = convert(&message).expect("the library accepts the turn");
        let ids = converted.identities(message.id);
        assert_eq!(ids[0], message.id, "the message's own id is declared");
        assert_eq!(first_repeated(ids), Some(twice));
        let fresh = convert(&legacy(MessageStatus::Completed)).unwrap();
        assert_eq!(first_repeated(fresh.identities(Uuid::now_v7())), None);
    }

    #[test]
    fn the_lock_is_its_own() {
        assert_ne!(LOCK, 0);
    }
}
