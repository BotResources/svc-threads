//! `threads.message` and the rows that are part of a message's state —
//! `message_entry`, `entry_block` (the library's records), `message_annotation`
//! and `message_identity` — through the engine's `Persistence` seam, soft EDA.
//!
//! A batch read is five statements whatever its size: the messages with their
//! user author's current name, their entries, their blocks, their annotations,
//! and the members of their threads. A record read back is checked by the
//! library (`check_local`), never trusted. A save writes the message row, the
//! content its transition wrote or removed (entries before their blocks, both
//! before the annotations, the identities with them), and the facts of the
//! change, in the pipeline's transaction.

use std::collections::{BTreeSet, HashMap};

use contract_threads::llm::{
    ActorRef, AgentId, Author, BlockId, BlockRecord, ContentBlockKind, EntryId, EntryRecord,
    HumanId, ObservedAt, OpaqueJson, RepresentationState, TurnEntryKind, TurnId,
};
use contract_threads::{
    Annotation, AnnotationBody, AnnotationKind, MessageStatus, PlacedAnnotation,
};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{
    Aggregate, CohortIndex, Persistence, PersistenceStyle, RowBatch,
};
use service_engine::{Cohort, KeyCeiling};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::content::Content;
use super::state::{MessageAuthor, MessageChange, MessageState, turn_kind_of_db};
use crate::directory::KNOWN_USERS_TABLE;
use crate::facts::{self, FactNoun};
use crate::fault::{corrupt, encode};
use crate::slices::thread::state::{MEMBER_DIM, UnknownStoredValue};

pub const TABLE: &str = "threads.message";

/// The schema of the content of a message Threads 0.5 writes.
pub const CONTENT_SCHEMA: &str = contract_threads::llm::CONVERSATION_SCHEMA;

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
                "UPDATE {TABLE} SET status = $2, deleted = $3, version = $4, edited_at = $5 \
                 WHERE id = $1"
            ))
            .bind(message.id)
            .bind(message.status.as_str())
            .bind(message.deleted)
            .bind(message.version)
            .bind(message.edited_at)
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
            let (user, agent, name) = match &message.author {
                MessageAuthor::User { user_id, .. } => (Some(*user_id), None, None),
                MessageAuthor::Agent { agent, name } => {
                    (None, Some(agent.as_uuid()), Some(name.as_str()))
                }
            };
            sqlx::query(&format!(
                "INSERT INTO {TABLE} (id, thread_id, parent_message_id, author_user_id, \
                     author_agent_id, author_ai_name, job_id, status, deleted, created_at, \
                     version, content_schema, turn_kind, observed_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"
            ))
            .bind(message.id)
            .bind(message.thread_id)
            .bind(message.parent_message_id)
            .bind(user)
            .bind(agent)
            .bind(name)
            .bind(message.job_id)
            .bind(message.status.as_str())
            .bind(message.deleted)
            .bind(message.created_at)
            .bind(message.version)
            .bind(CONTENT_SCHEMA)
            .bind(message.turn_kind.as_str())
            .bind(message.observed_at.as_ref().map(ObservedAt::as_str))
            .execute(&mut *conn)
            .await?;
            insert_identities(conn, message.id, &[message.id]).await?;
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
            MessageChange::ContentWritten => {
                remove_content(conn, message.id).await?;
                write_content(conn, message.id, &message.content).await?;
            }
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

/// Removes a message's content, its identities but its own, and whatever
/// Threads 0.4 kept of it: no previous content survives anywhere.
async fn remove_content(conn: &mut PgConnection, message: Uuid) -> Result<(), EngineError> {
    for statement in [
        "DELETE FROM threads.message_annotation WHERE message_id = $1",
        "DELETE FROM threads.entry_block WHERE message_id = $1",
        "DELETE FROM threads.message_entry WHERE message_id = $1",
        "DELETE FROM threads.message_identity WHERE message_id = $1 AND id <> $1",
        // What Threads 0.4 kept of the message (read by the upgrade only).
        "DELETE FROM threads.message_block WHERE message_id = $1",
        "DELETE FROM threads.message_item WHERE message_id = $1",
    ] {
        sqlx::query(statement)
            .bind(message)
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

/// A message's content, whole: its entries, its blocks, its annotations and
/// its identities, one insert each.
pub async fn write_content(
    conn: &mut PgConnection,
    message: Uuid,
    content: &Content,
) -> Result<(), EngineError> {
    insert_entries(conn, message, &content.entries).await?;
    insert_blocks(conn, message, &content.blocks).await?;
    insert_annotations(conn, message, &content.annotations).await?;
    insert_identities(conn, message, &content.identities()).await
}

fn json_of(payload: &OpaqueJson) -> Result<serde_json::Value, EngineError> {
    serde_json::to_value(payload).map_err(|source| encode("a record payload", source))
}

fn sender_of(entry: &EntryRecord) -> Result<(Option<&'static str>, Option<Uuid>), EngineError> {
    Ok(match entry.sender() {
        None => (None, None),
        Some(ActorRef::Human { id, .. }) => (Some("human"), Some(id.as_uuid())),
        Some(ActorRef::Agent { id, .. }) => (Some("agent"), Some(id.as_uuid())),
        Some(_) => {
            return Err(corrupt(UnknownStoredValue {
                column: "message_entry.sender_kind",
                value: "a sender of a later library".to_owned(),
            }));
        }
    })
}

async fn insert_entries(
    conn: &mut PgConnection,
    message: Uuid,
    entries: &[EntryRecord],
) -> Result<(), EngineError> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut ids = Vec::with_capacity(entries.len());
    let mut positions = Vec::with_capacity(entries.len());
    let mut kinds = Vec::with_capacity(entries.len());
    let mut states = Vec::with_capacity(entries.len());
    let mut sender_kinds = Vec::with_capacity(entries.len());
    let mut sender_ids = Vec::with_capacity(entries.len());
    let mut authors = Vec::with_capacity(entries.len());
    let mut observed = Vec::with_capacity(entries.len());
    let mut payloads = Vec::with_capacity(entries.len());
    for (position, entry) in entries.iter().enumerate() {
        let (sender_kind, sender_id) = sender_of(entry)?;
        ids.push(entry.id().as_uuid());
        positions.push(position as i32);
        kinds.push(entry.kind().as_str());
        states.push(entry.state().as_str());
        sender_kinds.push(sender_kind);
        sender_ids.push(sender_id);
        authors.push(entry.author().map(|author| author.as_str().to_owned()));
        observed.push(entry.observed_at().map(|at| at.as_str().to_owned()));
        payloads.push(json_of(entry.payload())?);
    }
    sqlx::query(
        "INSERT INTO threads.message_entry \
             (id, message_id, position, kind, state, sender_kind, sender_id, author, \
              observed_at, payload) \
         SELECT e.id, $1, e.position, e.kind, e.state, e.sender_kind, e.sender_id, e.author, \
                e.observed_at, e.payload \
         FROM unnest($2::uuid[], $3::int[], $4::text[], $5::text[], $6::text[], $7::uuid[], \
                     $8::text[], $9::text[], $10::jsonb[]) \
              AS e(id, position, kind, state, sender_kind, sender_id, author, observed_at, payload)",
    )
    .bind(message)
    .bind(&ids)
    .bind(&positions)
    .bind(&kinds)
    .bind(&states)
    .bind(&sender_kinds)
    .bind(&sender_ids)
    .bind(&authors)
    .bind(&observed)
    .bind(&payloads)
    .execute(conn)
    .await?;
    Ok(())
}

async fn insert_blocks(
    conn: &mut PgConnection,
    message: Uuid,
    blocks: &[BlockRecord],
) -> Result<(), EngineError> {
    if blocks.is_empty() {
        return Ok(());
    }
    let mut ids = Vec::with_capacity(blocks.len());
    let mut entries = Vec::with_capacity(blocks.len());
    let mut parents = Vec::with_capacity(blocks.len());
    let mut positions = Vec::with_capacity(blocks.len());
    let mut kinds = Vec::with_capacity(blocks.len());
    let mut states = Vec::with_capacity(blocks.len());
    let mut calls = Vec::with_capacity(blocks.len());
    let mut observed = Vec::with_capacity(blocks.len());
    let mut payloads = Vec::with_capacity(blocks.len());
    for (position, block) in blocks.iter().enumerate() {
        ids.push(block.id().as_uuid());
        entries.push(block.entry().as_uuid());
        parents.push(block.parent().map(|parent| parent.as_uuid()));
        positions.push(position as i32);
        kinds.push(block.kind().as_str());
        states.push(block.state().as_str());
        calls.push(block.call().map(|call| call.as_uuid()));
        observed.push(block.observed_at().map(|at| at.as_str().to_owned()));
        payloads.push(json_of(block.payload())?);
    }
    sqlx::query(
        "INSERT INTO threads.entry_block \
             (id, message_id, entry_id, parent_id, position, kind, state, call_id, observed_at, \
              payload) \
         SELECT b.id, $1, b.entry_id, b.parent_id, b.position, b.kind, b.state, b.call_id, \
                b.observed_at, b.payload \
         FROM unnest($2::uuid[], $3::uuid[], $4::uuid[], $5::int[], $6::text[], $7::text[], \
                     $8::uuid[], $9::text[], $10::jsonb[]) \
              AS b(id, entry_id, parent_id, position, kind, state, call_id, observed_at, payload)",
    )
    .bind(message)
    .bind(&ids)
    .bind(&entries)
    .bind(&parents)
    .bind(&positions)
    .bind(&kinds)
    .bind(&states)
    .bind(&calls)
    .bind(&observed)
    .bind(&payloads)
    .execute(conn)
    .await?;
    Ok(())
}

/// The place of each annotation among those after the same entry.
pub fn positions_after_their_entry(annotations: &[PlacedAnnotation]) -> Vec<usize> {
    let mut seen: HashMap<Uuid, usize> = HashMap::new();
    annotations
        .iter()
        .map(|placed| {
            let next = seen.entry(placed.after_entry_id).or_default();
            let position = *next;
            *next += 1;
            position
        })
        .collect()
}

async fn insert_annotations(
    conn: &mut PgConnection,
    message: Uuid,
    annotations: &[PlacedAnnotation],
) -> Result<(), EngineError> {
    if annotations.is_empty() {
        return Ok(());
    }
    let mut ids = Vec::with_capacity(annotations.len());
    let mut after = Vec::with_capacity(annotations.len());
    let mut positions = Vec::with_capacity(annotations.len());
    let mut kinds = Vec::with_capacity(annotations.len());
    let mut contents = Vec::with_capacity(annotations.len());
    for (placed, position) in annotations
        .iter()
        .zip(positions_after_their_entry(annotations))
    {
        ids.push(placed.annotation.id);
        after.push(placed.after_entry_id);
        positions.push(position as i32);
        kinds.push(placed.annotation.kind().as_str());
        contents.push(annotation_content(&placed.annotation.body)?);
    }
    sqlx::query(
        "INSERT INTO threads.message_annotation \
             (id, message_id, after_entry_id, position, kind, content) \
         SELECT a.id, $1, a.after_entry_id, a.position, a.kind, a.content \
         FROM unnest($2::uuid[], $3::uuid[], $4::int[], $5::text[], $6::jsonb[]) \
              AS a(id, after_entry_id, position, kind, content)",
    )
    .bind(message)
    .bind(&ids)
    .bind(&after)
    .bind(&positions)
    .bind(&kinds)
    .bind(&contents)
    .execute(conn)
    .await?;
    Ok(())
}

/// An annotation's content, as its kind types it.
fn annotation_content(body: &AnnotationBody) -> Result<serde_json::Value, EngineError> {
    let mut value = serde_json::to_value(body).map_err(|source| encode("an annotation", source))?;
    Ok(value
        .get_mut("content")
        .map(serde_json::Value::take)
        .unwrap_or(serde_json::Value::Null))
}

/// An annotation's body, read back through its kind's typed content.
fn annotation_body(kind: &str, content: serde_json::Value) -> Result<AnnotationBody, EngineError> {
    let kind = AnnotationKind::parse(kind).map_err(corrupt)?;
    serde_json::from_value(serde_json::json!({ "kind": kind.as_str(), "content": content }))
        .map_err(corrupt)
}

pub async fn insert_identities(
    conn: &mut PgConnection,
    message: Uuid,
    ids: &[Uuid],
) -> Result<(), EngineError> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO threads.message_identity (id, message_id) SELECT unnest($2::uuid[]), $1",
    )
    .bind(message)
    .bind(ids)
    .execute(conn)
    .await?;
    Ok(())
}

/// Of `ids`, those a message already declared: an id is used once. The
/// identities of `except` do not count — an edit keeps its own message's.
pub async fn held_ids(
    conn: &mut PgConnection,
    ids: &[Uuid],
    except: Option<Uuid>,
) -> Result<Vec<Uuid>, EngineError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(sqlx::query_scalar(
        "SELECT id FROM threads.message_identity \
         WHERE id = ANY($1) AND ($2::uuid IS NULL OR message_id <> $2) ORDER BY id",
    )
    .bind(ids)
    .bind(except)
    .fetch_all(conn)
    .await?)
}

/// Of `ids`, those a message already declared, each with the message that
/// declared it: one statement, whatever the number of ids.
pub async fn holders(
    conn: &mut PgConnection,
    ids: &[Uuid],
) -> Result<Vec<(Uuid, Uuid)>, EngineError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(sqlx::query_as(
        "SELECT id, message_id FROM threads.message_identity WHERE id = ANY($1) ORDER BY id",
    )
    .bind(ids)
    .fetch_all(conn)
    .await?)
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
        "SELECT m.id, m.thread_id, m.parent_message_id, m.author_user_id, m.author_agent_id, \
                m.author_ai_name, m.job_id, m.status, m.deleted, m.created_at, m.edited_at, \
                m.version, m.content_schema, m.turn_kind, m.observed_at, \
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
    let mut entries = entries_of(conn, &ids).await?;
    let mut blocks = blocks_of(conn, &ids).await?;
    let mut annotations = annotations_of(conn, &ids).await?;
    let members = members_of(conn, &threads).await?;
    for message in &mut messages {
        message.content = Content {
            entries: entries.remove(&message.id).unwrap_or_default(),
            blocks: blocks.remove(&message.id).unwrap_or_default(),
            annotations: annotations.remove(&message.id).unwrap_or_default(),
        };
        message.members = members.get(&message.thread_id).cloned().unwrap_or_default();
    }
    Ok(messages
        .into_iter()
        .map(|message| (message.id, message))
        .collect())
}

/// A row read back is validated, never trusted.
fn message_of_row(row: &PgRow) -> Result<MessageState, EngineError> {
    let unknown = |column: &'static str, value: &str| {
        corrupt(UnknownStoredValue {
            column,
            value: value.to_owned(),
        })
    };
    let schema: String = row.try_get("content_schema")?;
    if schema != CONTENT_SCHEMA {
        return Err(unknown("message.content_schema", &schema));
    }
    let status: String = row.try_get("status")?;
    let turn_kind: Option<String> = row.try_get("turn_kind")?;
    let turn_kind = turn_kind.ok_or_else(|| unknown("message.turn_kind", "NULL"))?;
    let user: Option<Uuid> = row.try_get("author_user_id")?;
    let agent: Option<Uuid> = row.try_get("author_agent_id")?;
    let name: Option<String> = row.try_get("author_ai_name")?;
    let author = match (user, agent, name) {
        (Some(user_id), None, None) => MessageAuthor::User {
            user_id,
            display_name: row.try_get("display_name")?,
            in_roster: row.try_get("in_roster")?,
        },
        (None, Some(agent), Some(name)) => MessageAuthor::Agent {
            agent: AgentId::from_uuid(agent).map_err(corrupt)?,
            name: Author::new(name).map_err(corrupt)?,
        },
        _ => return Err(unknown("message.author", "not exactly one author")),
    };
    let observed_at: Option<String> = row.try_get("observed_at")?;
    Ok(MessageState {
        id: row.try_get("id")?,
        thread_id: row.try_get("thread_id")?,
        parent_message_id: row.try_get("parent_message_id")?,
        author,
        job_id: row.try_get("job_id")?,
        status: MessageStatus::parse(&status).map_err(corrupt)?,
        deleted: row.try_get("deleted")?,
        created_at: row.try_get("created_at")?,
        edited_at: row.try_get("edited_at")?,
        version: row.try_get("version")?,
        turn_kind: turn_kind_of_db(&turn_kind).map_err(corrupt)?,
        observed_at: observed_at
            .map(ObservedAt::new)
            .transpose()
            .map_err(corrupt)?,
        content: Content::default(),
        members: BTreeSet::new(),
        pending: Vec::new(),
    })
}

fn payload_of(row: &PgRow) -> Result<OpaqueJson, EngineError> {
    let payload: serde_json::Value = row.try_get("payload")?;
    serde_json::from_value(payload).map_err(corrupt)
}

/// One entry row, as the library's record, checked by the library.
pub fn entry_of_row(row: &PgRow) -> Result<EntryRecord, EngineError> {
    let kind: String = row.try_get("kind")?;
    let state: String = row.try_get("state")?;
    let mut record = EntryRecord::new(
        EntryId::from_uuid(row.try_get("id")?).map_err(corrupt)?,
        TurnId::from_uuid(row.try_get("message_id")?).map_err(corrupt)?,
        kind.parse::<TurnEntryKind>().map_err(corrupt)?,
        state.parse::<RepresentationState>().map_err(corrupt)?,
        payload_of(row)?,
    );
    let sender_kind: Option<String> = row.try_get("sender_kind")?;
    let sender_id: Option<Uuid> = row.try_get("sender_id")?;
    record = match (sender_kind.as_deref(), sender_id) {
        (None, None) => record,
        (Some("human"), Some(id)) => {
            record.with_sender(ActorRef::human(HumanId::from_uuid(id).map_err(corrupt)?))
        }
        (Some("agent"), Some(id)) => {
            record.with_sender(ActorRef::agent(AgentId::from_uuid(id).map_err(corrupt)?))
        }
        (other, _) => {
            return Err(corrupt(UnknownStoredValue {
                column: "message_entry.sender_kind",
                value: other.unwrap_or("NULL").to_owned(),
            }));
        }
    };
    let author: Option<String> = row.try_get("author")?;
    if let Some(author) = author {
        record = record.with_author(Author::new(author).map_err(corrupt)?);
    }
    let observed_at: Option<String> = row.try_get("observed_at")?;
    if let Some(observed_at) = observed_at {
        record = record.with_observed_at(ObservedAt::new(observed_at).map_err(corrupt)?);
    }
    Ok(record)
}

/// One block row, as the library's record.
pub fn block_of_row(row: &PgRow) -> Result<BlockRecord, EngineError> {
    let kind: String = row.try_get("kind")?;
    let state: String = row.try_get("state")?;
    let mut record = BlockRecord::new(
        BlockId::from_uuid(row.try_get("id")?).map_err(corrupt)?,
        EntryId::from_uuid(row.try_get("entry_id")?).map_err(corrupt)?,
        kind.parse::<ContentBlockKind>().map_err(corrupt)?,
        state.parse::<RepresentationState>().map_err(corrupt)?,
        payload_of(row)?,
    );
    let parent: Option<Uuid> = row.try_get("parent_id")?;
    if let Some(parent) = parent {
        record = record.with_parent(BlockId::from_uuid(parent).map_err(corrupt)?);
    }
    let call: Option<Uuid> = row.try_get("call_id")?;
    if let Some(call) = call {
        record = record.with_call(BlockId::from_uuid(call).map_err(corrupt)?);
    }
    let observed_at: Option<String> = row.try_get("observed_at")?;
    if let Some(observed_at) = observed_at {
        record = record.with_observed_at(ObservedAt::new(observed_at).map_err(corrupt)?);
    }
    Ok(record)
}

async fn entries_of(
    conn: &mut PgConnection,
    messages: &[Uuid],
) -> Result<HashMap<Uuid, Vec<EntryRecord>>, EngineError> {
    let rows = sqlx::query(
        "SELECT id, message_id, kind, state, sender_kind, sender_id, author, observed_at, \
                payload \
         FROM threads.message_entry WHERE message_id = ANY($1) ORDER BY message_id, position",
    )
    .bind(messages)
    .fetch_all(conn)
    .await?;
    let mut by_message: HashMap<Uuid, Vec<EntryRecord>> = HashMap::new();
    for row in &rows {
        let record = entry_of_row(row)?;
        record.check_local().map_err(corrupt)?;
        by_message
            .entry(row.try_get("message_id")?)
            .or_default()
            .push(record);
    }
    Ok(by_message)
}

async fn blocks_of(
    conn: &mut PgConnection,
    messages: &[Uuid],
) -> Result<HashMap<Uuid, Vec<BlockRecord>>, EngineError> {
    let rows = sqlx::query(
        "SELECT id, message_id, entry_id, parent_id, kind, state, call_id, observed_at, payload \
         FROM threads.entry_block WHERE message_id = ANY($1) ORDER BY message_id, position",
    )
    .bind(messages)
    .fetch_all(conn)
    .await?;
    let mut by_message: HashMap<Uuid, Vec<BlockRecord>> = HashMap::new();
    for row in &rows {
        let record = block_of_row(row)?;
        record.check_local().map_err(corrupt)?;
        by_message
            .entry(row.try_get("message_id")?)
            .or_default()
            .push(record);
    }
    Ok(by_message)
}

async fn annotations_of(
    conn: &mut PgConnection,
    messages: &[Uuid],
) -> Result<HashMap<Uuid, Vec<PlacedAnnotation>>, EngineError> {
    let rows = sqlx::query(
        "SELECT a.id, a.message_id, a.after_entry_id, a.kind, a.content \
         FROM threads.message_annotation a \
         JOIN threads.message_entry e ON e.id = a.after_entry_id \
         WHERE a.message_id = ANY($1) \
         ORDER BY a.message_id, e.position, a.position",
    )
    .bind(messages)
    .fetch_all(conn)
    .await?;
    let mut by_message: HashMap<Uuid, Vec<PlacedAnnotation>> = HashMap::new();
    for row in &rows {
        let kind: String = row.try_get("kind")?;
        let annotation = PlacedAnnotation::new(
            row.try_get("after_entry_id")?,
            Annotation::new(
                row.try_get("id")?,
                annotation_body(&kind, row.try_get("content")?)?,
            ),
        );
        by_message
            .entry(row.try_get("message_id")?)
            .or_default()
            .push(annotation);
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

#[cfg(test)]
mod tests {
    use contract_threads::{AnnotationBody, Notice};

    use super::*;

    #[test]
    fn an_annotation_content_is_stored_apart_from_its_kind_and_read_back() {
        let body = AnnotationBody::Error(Notice {
            code: "TOOL_DOWN".into(),
            text: Some("the search is down".into()),
        });
        let content = annotation_content(&body).unwrap();
        assert_eq!(
            content,
            serde_json::json!({ "code": "TOOL_DOWN", "text": "the search is down" })
        );
        assert_eq!(annotation_body("ERROR", content).unwrap(), body);
        assert!(annotation_body("error", serde_json::json!({})).is_err());
    }
}
