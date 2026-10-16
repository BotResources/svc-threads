//! `threads.message` and the rows that are part of a message's state —
//! `message_item`, `message_block` — through the engine's `Persistence` seam,
//! soft EDA.
//!
//! A batch read is four statements whatever its size: the messages with their
//! user author's current name, their items, their blocks, and the members of
//! their threads. A save writes the message row, the content its transition
//! wrote or removed (items before their blocks), and the facts of the change,
//! in the pipeline's transaction.

use std::collections::{BTreeSet, HashMap};

use contract_threads::{BlockKind, MessageStatus};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{
    Aggregate, CohortIndex, Persistence, PersistenceStyle, RowBatch,
};
use service_engine::{Cohort, KeyCeiling};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::content::UsedCallIds;
use super::state::{
    ItemKind, MessageAuthor, MessageChange, MessageState, StoredBlock, StoredItem, in_conversation,
};
use crate::directory::KNOWN_USERS_TABLE;
use crate::facts::{self, FactNoun};
use crate::fault::corrupt;
use crate::slices::thread::state::{MEMBER_DIM, UnknownStoredValue};

pub const TABLE: &str = "threads.message";

pub struct MessageStore;

impl Persistence for MessageStore {
    type Aggregate = MessageState;
    type Key = Uuid;
    type Event = MessageChange;

    const STYLE: PersistenceStyle = PersistenceStyle::SoftEda;

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, TABLE, key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> RowBatch<'a, Uuid, MessageState> {
        Box::pin(async move { read_many(conn, keys).await })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        message: &'a MessageState,
        changes: &'a [MessageChange],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "UPDATE {TABLE} SET status = $2, deleted = $3, version = $4 WHERE id = $1"
            ))
            .bind(message.id)
            .bind(message.status.as_str())
            .bind(message.deleted)
            .bind(message.version)
            .execute(&mut *conn)
            .await?;
            write_changes(conn, message, changes).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        message: &'a MessageState,
        changes: &'a [MessageChange],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let (user, ai_id, ai_name) = match &message.author {
                MessageAuthor::User { user_id, .. } => (Some(*user_id), None, None),
                MessageAuthor::Ai { id, name } => (None, Some(id.as_str()), Some(name.as_str())),
            };
            sqlx::query(&format!(
                "INSERT INTO {TABLE} (id, thread_id, parent_message_id, author_user_id, \
                     author_ai_id, author_ai_name, job_id, status, deleted, created_at, version) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
            ))
            .bind(message.id)
            .bind(message.thread_id)
            .bind(message.parent_message_id)
            .bind(user)
            .bind(ai_id)
            .bind(ai_name)
            .bind(message.job_id)
            .bind(message.status.as_str())
            .bind(message.deleted)
            .bind(message.created_at)
            .bind(message.version)
            .execute(&mut *conn)
            .await?;
            write_changes(conn, message, changes).await
        })
    }
}

impl Aggregate for MessageState {
    type Store = MessageStore;

    fn key(&self) -> Uuid {
        self.id
    }

    fn pending_events(&self) -> &[MessageChange] {
        &self.pending
    }
}

async fn write_changes(
    conn: &mut PgConnection,
    message: &MessageState,
    changes: &[MessageChange],
) -> Result<(), EngineError> {
    for change in changes {
        match change {
            MessageChange::ContentWritten => write_content(conn, message).await?,
            MessageChange::ContentRemoved => remove_content(conn, message.id).await?,
            MessageChange::Recorded(_) => {}
        }
    }
    facts::append(
        conn,
        FactNoun::Message,
        message.id,
        message.version,
        &message.pending_facts(),
    )
    .await
}

async fn remove_content(conn: &mut PgConnection, message: Uuid) -> Result<(), EngineError> {
    sqlx::query("DELETE FROM threads.message_block WHERE message_id = $1")
        .bind(message)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM threads.message_item WHERE message_id = $1")
        .bind(message)
        .execute(conn)
        .await?;
    Ok(())
}

/// The message's content, whole: its items, then every block, in two inserts.
async fn write_content(conn: &mut PgConnection, message: &MessageState) -> Result<(), EngineError> {
    remove_content(conn, message.id).await?;
    if !message.items.is_empty() {
        let items = &message.items;
        sqlx::query(
            "INSERT INTO threads.message_item \
                 (message_id, index, kind, status, stop_reason, usage, model) \
             SELECT $1, i.index, i.kind, i.status, i.stop_reason, i.usage, i.model \
             FROM unnest($2::int[], $3::text[], $4::text[], $5::jsonb[], $6::jsonb[], $7::text[]) \
                  AS i(index, kind, status, stop_reason, usage, model)",
        )
        .bind(message.id)
        .bind(items.iter().map(|item| item.index).collect::<Vec<_>>())
        .bind(
            items
                .iter()
                .map(|item| item.kind.as_db())
                .collect::<Vec<_>>(),
        )
        .bind(
            items
                .iter()
                .map(|item| item.status.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(
            items
                .iter()
                .map(|item| item.stop_reason.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            items
                .iter()
                .map(|item| item.usage.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            items
                .iter()
                .map(|item| item.model.clone())
                .collect::<Vec<_>>(),
        )
        .execute(&mut *conn)
        .await?;
    }
    let blocks: Vec<(Option<i32>, &StoredBlock)> = message
        .blocks
        .iter()
        .map(|block| (None, block))
        .chain(message.items.iter().flat_map(|item| {
            item.blocks
                .iter()
                .map(move |block| (Some(item.index), block))
        }))
        .collect();
    if blocks.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO threads.message_block (id, message_id, item_index, index, kind, content) \
         SELECT b.id, $1, b.item_index, b.index, b.kind, b.content \
         FROM unnest($2::uuid[], $3::int[], $4::int[], $5::text[], $6::jsonb[]) \
              AS b(id, item_index, index, kind, content)",
    )
    .bind(message.id)
    .bind(blocks.iter().map(|(_, block)| block.id).collect::<Vec<_>>())
    .bind(blocks.iter().map(|(item, _)| *item).collect::<Vec<_>>())
    .bind(
        blocks
            .iter()
            .map(|(_, block)| block.index)
            .collect::<Vec<_>>(),
    )
    .bind(
        blocks
            .iter()
            .map(|(_, block)| block.kind.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        blocks
            .iter()
            .map(|(_, block)| block.content.clone())
            .collect::<Vec<_>>(),
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Of `ids`, those some block already holds: a block id is used once.
pub async fn held_block_ids(
    conn: &mut PgConnection,
    ids: &[Uuid],
) -> Result<Vec<Uuid>, EngineError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(
        sqlx::query_scalar("SELECT id FROM threads.message_block WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(conn)
            .await?,
    )
}

/// The tool call ids already used on the branches through message `message`
/// (whose parent is `parent`): by its ancestors and by its descendants, those a
/// conversation holds. One statement: the walk reads the branch's messages by
/// key, and each one's tool calls and first step are probed by index
/// (`message_block_message_idx`, `message_item_pkey`). The planner cannot
/// estimate a recursive walk and would rather scan every block of the
/// database; `OFFSET 0` keeps the block probe a per-message subquery. A tool
/// call is a step's block, never a user message's.
///
/// Every message command locks the message's thread first, so the content of
/// a thread is written one command at a time: of two messages on one branch,
/// the one written second reads the other's calls here. A message streaming
/// has no content in its row yet; it is judged when its stream closes.
pub async fn used_call_ids(
    conn: &mut PgConnection,
    parent: Option<Uuid>,
    message: Uuid,
) -> Result<UsedCallIds, EngineError> {
    let rows = sqlx::query(
        "WITH RECURSIVE ancestors(id, parent, status, deleted) AS ( \
             SELECT id, parent_message_id, status, deleted FROM threads.message WHERE id = $1 \
             UNION ALL \
             SELECT m.id, m.parent_message_id, m.status, m.deleted \
             FROM threads.message m JOIN ancestors a ON m.id = a.parent \
         ), descendants(id, status, deleted) AS ( \
             SELECT id, status, deleted FROM threads.message WHERE parent_message_id = $2 \
             UNION ALL \
             SELECT m.id, m.status, m.deleted \
             FROM threads.message m JOIN descendants d ON m.parent_message_id = d.id \
         ), around(id, status, deleted) AS ( \
             SELECT id, status, deleted FROM ancestors \
             UNION SELECT id, status, deleted FROM descendants \
         ) \
         SELECT around.status, around.deleted, call.content, \
                step.found IS NOT NULL AS has_a_step \
         FROM around \
         CROSS JOIN LATERAL ( \
             SELECT b.content FROM threads.message_block b \
             WHERE b.message_id = around.id AND b.item_index IS NOT NULL \
               AND b.kind = 'TOOL_CALL' \
             OFFSET 0 \
         ) call \
         LEFT JOIN LATERAL ( \
             SELECT 1 AS found FROM threads.message_item i \
             WHERE i.message_id = around.id AND i.kind = 'STEP' LIMIT 1 \
         ) step ON true",
    )
    .bind(parent)
    .bind(message)
    .fetch_all(conn)
    .await?;
    let mut used = UsedCallIds::new();
    for row in rows {
        let status: String = row.try_get("status")?;
        let status = MessageStatus::parse(&status).map_err(corrupt)?;
        if !in_conversation(status, row.try_get("deleted")?, row.try_get("has_a_step")?) {
            continue;
        }
        let block: contract_threads::llm::AssistantBlock =
            serde_json::from_value(row.try_get("content")?).map_err(corrupt)?;
        if let Some(call) = block.as_tool_call() {
            used.insert(call.id.clone());
        }
    }
    Ok(used)
}

/// The thread a message belongs to, if it exists.
pub async fn thread_of(
    conn: &mut PgConnection,
    message: Uuid,
) -> Result<Option<Uuid>, EngineError> {
    Ok(
        sqlx::query_scalar("SELECT thread_id FROM threads.message WHERE id = $1")
            .bind(message)
            .fetch_optional(conn)
            .await?,
    )
}

/// The messages of the threads of the members named, one read bounded by the
/// engine's ceiling.
impl CohortIndex for MessageStore {
    fn keys_in_cohorts<'a>(
        conn: &'a mut PgConnection,
        cohorts: &'a [Cohort],
        ceiling: KeyCeiling,
    ) -> BoxFuture<'a, Result<Vec<Uuid>, EngineError>> {
        Box::pin(async move {
            let members = Cohort::uuids(cohorts, MEMBER_DIM);
            let keys = sqlx::query_scalar(
                "SELECT m.id FROM threads.message m \
                 WHERE EXISTS (SELECT 1 FROM threads.thread_member t \
                               WHERE t.thread_id = m.thread_id AND t.user_id = ANY($1)) \
                 ORDER BY m.id LIMIT $2",
            )
            .bind(&members)
            .bind(ceiling.limit())
            .fetch_all(conn)
            .await?;
            Ok(keys)
        })
    }
}

/// One page of a branch of `thread`, the newest first, read for `member`
/// (nothing when they are not a member of it): the branch ending at `tip`, or
/// the active branch — the one ending at the thread's most recent message —
/// when no tip is named; the page holds the messages older than `cursor` when
/// one is given. One statement.
pub async fn branch_page(
    pg: &PgPool,
    member: Uuid,
    thread: Uuid,
    tip: Option<Uuid>,
    cursor: Option<Uuid>,
    limit: i64,
) -> Result<Vec<Uuid>, EngineError> {
    Ok(sqlx::query_scalar(
        "WITH RECURSIVE tip AS ( \
             SELECT m.id FROM threads.message m \
             WHERE m.thread_id = $2 \
               AND EXISTS (SELECT 1 FROM threads.thread_member t \
                           WHERE t.thread_id = $2 AND t.user_id = $1) \
               AND ($3::uuid IS NULL OR m.id = $3) \
             ORDER BY m.created_at DESC, m.id DESC LIMIT 1 \
         ), branch AS ( \
             SELECT m.id, m.parent_message_id, 0 AS depth \
             FROM threads.message m JOIN tip ON m.id = tip.id \
             UNION ALL \
             SELECT p.id, p.parent_message_id, b.depth + 1 \
             FROM threads.message p JOIN branch b ON p.id = b.parent_message_id \
         ) \
         SELECT id FROM branch \
         WHERE $4::uuid IS NULL OR depth > (SELECT c.depth FROM branch c WHERE c.id = $4) \
         ORDER BY depth LIMIT $5",
    )
    .bind(member)
    .bind(thread)
    .bind(tip)
    .bind(cursor)
    .bind(limit)
    .fetch_all(pg)
    .await?)
}

async fn read_many(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<(Uuid, MessageState)>, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT m.id, m.thread_id, m.parent_message_id, m.author_user_id, m.author_ai_id, \
                m.author_ai_name, m.job_id, m.status, m.deleted, m.created_at, m.version, \
                k.display_name, COALESCE(k.in_roster, false) AS in_roster \
         FROM {TABLE} m LEFT JOIN {KNOWN_USERS_TABLE} k ON k.user_id = m.author_user_id \
         WHERE m.id = ANY($1)"
    ))
    .bind(keys)
    .fetch_all(&mut *conn)
    .await?;
    let mut messages = rows
        .iter()
        .map(message_of_row)
        .collect::<Result<Vec<_>, _>>()?;
    if messages.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<Uuid> = messages.iter().map(|message| message.id).collect();
    let threads: Vec<Uuid> = messages
        .iter()
        .map(|message| message.thread_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut items = items_of(conn, &ids).await?;
    let mut blocks = blocks_of(conn, &ids).await?;
    let members = members_of(conn, &threads).await?;
    for message in &mut messages {
        let mut own_blocks = blocks.remove(&message.id).unwrap_or_default();
        let mut own_items = items.remove(&message.id).unwrap_or_default();
        for item in &mut own_items {
            item.blocks = own_blocks.remove(&Some(item.index)).unwrap_or_default();
        }
        message.items = own_items;
        message.blocks = own_blocks.remove(&None).unwrap_or_default();
        message.members = members.get(&message.thread_id).cloned().unwrap_or_default();
    }
    Ok(messages
        .into_iter()
        .map(|message| (message.id, message))
        .collect())
}

/// A row read back is validated, never trusted.
fn message_of_row(row: &PgRow) -> Result<MessageState, EngineError> {
    let status: String = row.try_get("status")?;
    let user: Option<Uuid> = row.try_get("author_user_id")?;
    let ai_id: Option<String> = row.try_get("author_ai_id")?;
    let ai_name: Option<String> = row.try_get("author_ai_name")?;
    let author = match (user, ai_id, ai_name) {
        (Some(user_id), None, None) => MessageAuthor::User {
            user_id,
            display_name: row.try_get("display_name")?,
            in_roster: row.try_get("in_roster")?,
        },
        (None, Some(id), Some(name)) => MessageAuthor::Ai { id, name },
        _ => {
            return Err(corrupt(UnknownStoredValue {
                column: "message.author",
                value: "not exactly one author".to_owned(),
            }));
        }
    };
    Ok(MessageState {
        id: row.try_get("id")?,
        thread_id: row.try_get("thread_id")?,
        parent_message_id: row.try_get("parent_message_id")?,
        author,
        job_id: row.try_get("job_id")?,
        status: MessageStatus::parse(&status).map_err(corrupt)?,
        deleted: row.try_get("deleted")?,
        created_at: row.try_get("created_at")?,
        version: row.try_get("version")?,
        blocks: Vec::new(),
        items: Vec::new(),
        members: BTreeSet::new(),
        pending: Vec::new(),
    })
}

async fn items_of(
    conn: &mut PgConnection,
    messages: &[Uuid],
) -> Result<HashMap<Uuid, Vec<StoredItem>>, EngineError> {
    let rows = sqlx::query(
        "SELECT message_id, index, kind, status, stop_reason, usage, model \
         FROM threads.message_item WHERE message_id = ANY($1) ORDER BY message_id, index",
    )
    .bind(messages)
    .fetch_all(conn)
    .await?;
    let mut by_message: HashMap<Uuid, Vec<StoredItem>> = HashMap::new();
    for row in &rows {
        let kind: String = row.try_get("kind")?;
        let status: String = row.try_get("status")?;
        by_message
            .entry(row.try_get("message_id")?)
            .or_default()
            .push(StoredItem {
                index: row.try_get("index")?,
                kind: ItemKind::from_db(&kind).map_err(corrupt)?,
                status: MessageStatus::parse(&status).map_err(corrupt)?,
                stop_reason: row.try_get("stop_reason")?,
                usage: row.try_get("usage")?,
                model: row.try_get("model")?,
                blocks: Vec::new(),
            });
    }
    Ok(by_message)
}

/// Each message's blocks, by the item they belong to (`None` for a user
/// message's own blocks), in order.
async fn blocks_of(
    conn: &mut PgConnection,
    messages: &[Uuid],
) -> Result<HashMap<Uuid, HashMap<Option<i32>, Vec<StoredBlock>>>, EngineError> {
    let rows = sqlx::query(
        "SELECT id, message_id, item_index, index, kind, content \
         FROM threads.message_block WHERE message_id = ANY($1) \
         ORDER BY message_id, item_index NULLS FIRST, index",
    )
    .bind(messages)
    .fetch_all(conn)
    .await?;
    let mut by_message: HashMap<Uuid, HashMap<Option<i32>, Vec<StoredBlock>>> = HashMap::new();
    for row in &rows {
        let kind: String = row.try_get("kind")?;
        by_message
            .entry(row.try_get("message_id")?)
            .or_default()
            .entry(row.try_get("item_index")?)
            .or_default()
            .push(StoredBlock {
                id: row.try_get("id")?,
                index: row.try_get("index")?,
                kind: BlockKind::parse(&kind).map_err(corrupt)?,
                content: row.try_get("content")?,
            });
    }
    Ok(by_message)
}

async fn members_of(
    conn: &mut PgConnection,
    threads: &[Uuid],
) -> Result<HashMap<Uuid, BTreeSet<Uuid>>, EngineError> {
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT thread_id, user_id FROM threads.thread_member WHERE thread_id = ANY($1)",
    )
    .bind(threads)
    .fetch_all(conn)
    .await?;
    let mut by_thread: HashMap<Uuid, BTreeSet<Uuid>> = HashMap::new();
    for (thread, user) in rows {
        by_thread.entry(thread).or_default().insert(user);
    }
    Ok(by_thread)
}

/// The active branch of `thread` — the one ending at its most recent message —
/// from its first message to its last. One statement.
pub async fn active_branch(
    conn: &mut PgConnection,
    thread: Uuid,
) -> Result<Vec<Uuid>, EngineError> {
    Ok(sqlx::query_scalar(
        "WITH RECURSIVE tip AS ( \
             SELECT m.id FROM threads.message m WHERE m.thread_id = $1 \
             ORDER BY m.created_at DESC, m.id DESC LIMIT 1 \
         ), branch AS ( \
             SELECT m.id, m.parent_message_id, 0 AS depth \
             FROM threads.message m JOIN tip ON m.id = tip.id \
             UNION ALL \
             SELECT p.id, p.parent_message_id, b.depth + 1 \
             FROM threads.message p JOIN branch b ON p.id = b.parent_message_id \
         ) \
         SELECT id FROM branch ORDER BY depth DESC",
    )
    .bind(thread)
    .fetch_all(conn)
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_block_kind_is_read_through_the_published_literal() {
        assert_eq!(BlockKind::parse("TOOL_RESULT"), Ok(BlockKind::ToolResult));
        assert!(BlockKind::parse("tool_result").is_err());
    }
}
