//! `threads.thread` and the rows that are part of a thread's state —
//! `thread_member`, `thread_resource`, `read_mark`, `title_job` — through the
//! engine's `Persistence` seam, soft EDA.
//!
//! A batch read is four statements whatever its size: the threads with their
//! title Job, their members with the roster copy's name, their resources, their
//! read marks. A save writes the thread row and its title Job whole, applies the
//! row changes its transition made (members, resources, read marks) and appends
//! the facts of the change, in the pipeline's transaction.
//!
//! Concurrency: the engine serialises commands on one key with a per-key
//! transaction advisory lock taken when the pipeline loads it; `lock` adds a row
//! lock that pins the row image. A gesture that holds a thread and its messages
//! locks the thread first, then the messages, then the artifacts — every
//! gesture takes them in that one order.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use contract_threads::{ThreadStatus, ThreadTitle};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{
    Aggregate, CohortIndex, Persistence, PersistenceStyle, RowBatch,
};
use service_engine::{Cohort, KeyCeiling};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::state::{
    MEMBER_DIM, Member, Resolution, ThreadChange, ThreadOrigin, ThreadState, TitleJob,
    TitleJobState, UnknownStoredValue,
};
use crate::directory::KNOWN_USERS_TABLE;
use crate::facts::{self, FactNoun};
use crate::fault::corrupt;

pub const TABLE: &str = "threads.thread";
const COLUMNS: &str = "t.id, t.owner_id, t.title, t.parent_thread_id, t.origin_block_id, \
                       t.status, t.resolved_by, t.resolved_at, t.created_at, \
                       t.last_activity_at, t.version, \
                       j.job_id AS title_job_id, j.state AS title_job_state, \
                       j.created_at AS title_job_created_at, j.ended_at AS title_job_ended_at";

pub struct ThreadStore;

impl Persistence for ThreadStore {
    type Aggregate = ThreadState;
    type Key = Uuid;
    type Event = ThreadChange;

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
    ) -> RowBatch<'a, Uuid, ThreadState> {
        Box::pin(async move { read_many(conn, keys).await })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        thread: &'a ThreadState,
        changes: &'a [ThreadChange],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "UPDATE {TABLE} SET title = $2, status = $3, last_activity_at = $4, version = $5, \
                     resolved_by = $6, resolved_at = $7 \
                 WHERE id = $1"
            ))
            .bind(thread.id)
            .bind(thread.title.as_ref().map(ThreadTitle::as_str))
            .bind(thread.status.as_str())
            .bind(thread.last_activity_at)
            .bind(thread.version)
            .bind(thread.resolution.map(|resolution| resolution.by))
            .bind(thread.resolution.map(|resolution| resolution.at))
            .execute(&mut *conn)
            .await?;
            write_title_job(conn, thread).await?;
            for change in changes {
                apply(conn, thread.id, change).await?;
            }
            facts::append(
                conn,
                FactNoun::Thread,
                thread.id,
                thread.version,
                &thread.pending_facts(),
            )
            .await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        thread: &'a ThreadState,
        _changes: &'a [ThreadChange],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO {TABLE} (id, owner_id, title, parent_thread_id, origin_block_id, \
                     status, created_at, last_activity_at, version) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
            ))
            .bind(thread.id)
            .bind(thread.owner_id)
            .bind(thread.title.as_ref().map(ThreadTitle::as_str))
            .bind(thread.origin.map(|origin| origin.thread_id))
            .bind(thread.origin.map(|origin| origin.block_id))
            .bind(thread.status.as_str())
            .bind(thread.created_at)
            .bind(thread.last_activity_at)
            .bind(thread.version)
            .execute(&mut *conn)
            .await?;
            let members: Vec<Uuid> = thread.members.keys().copied().collect();
            add_members(conn, thread.id, &members).await?;
            for (key, ids) in &thread.resources {
                set_resource(conn, thread.id, key, ids).await?;
            }
            write_title_job(conn, thread).await?;
            facts::append(
                conn,
                FactNoun::Thread,
                thread.id,
                thread.version,
                &thread.pending_facts(),
            )
            .await
        })
    }

    /// The thread row; its members, resources, messages, items, blocks, read
    /// marks and title Job go with it by cascade. Its artifacts do not: they are
    /// deleted first, through their own store, so that their bytes are
    /// released.
    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!("DELETE FROM {TABLE} WHERE id = $1"))
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl Aggregate for ThreadState {
    type Store = ThreadStore;

    fn key(&self) -> Uuid {
        self.id
    }

    fn pending_events(&self) -> &[ThreadChange] {
        &self.pending
    }
}

async fn apply(
    conn: &mut PgConnection,
    thread: Uuid,
    change: &ThreadChange,
) -> Result<(), EngineError> {
    match change {
        ThreadChange::MembersAdded(users) => add_members(conn, thread, users).await,
        ThreadChange::MembersRemoved(users) => {
            // Their read marks go by cascade.
            sqlx::query(
                "DELETE FROM threads.thread_member WHERE thread_id = $1 AND user_id = ANY($2)",
            )
            .bind(thread)
            .bind(users)
            .execute(conn)
            .await?;
            Ok(())
        }
        ThreadChange::ResourceSet { key, ids } => set_resource(conn, thread, key, ids).await,
        ThreadChange::ReadMarkSet { user, message } => {
            sqlx::query(
                "INSERT INTO threads.read_mark (thread_id, user_id, last_read_message_id) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (thread_id, user_id) \
                 DO UPDATE SET last_read_message_id = EXCLUDED.last_read_message_id",
            )
            .bind(thread)
            .bind(user)
            .bind(message)
            .execute(conn)
            .await?;
            Ok(())
        }
        ThreadChange::Recorded(_) => Ok(()),
    }
}

async fn add_members(
    conn: &mut PgConnection,
    thread: Uuid,
    users: &[Uuid],
) -> Result<(), EngineError> {
    if users.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO threads.thread_member (thread_id, user_id) \
         SELECT $1, unnest($2::uuid[]) ON CONFLICT DO NOTHING",
    )
    .bind(thread)
    .bind(users)
    .execute(conn)
    .await?;
    Ok(())
}

async fn set_resource(
    conn: &mut PgConnection,
    thread: Uuid,
    key: &str,
    ids: &[String],
) -> Result<(), EngineError> {
    if ids.is_empty() {
        sqlx::query("DELETE FROM threads.thread_resource WHERE thread_id = $1 AND key = $2")
            .bind(thread)
            .bind(key)
            .execute(conn)
            .await?;
    } else {
        sqlx::query(
            "INSERT INTO threads.thread_resource (thread_id, key, ids) VALUES ($1, $2, $3) \
             ON CONFLICT (thread_id, key) DO UPDATE SET ids = EXCLUDED.ids",
        )
        .bind(thread)
        .bind(key)
        .bind(ids)
        .execute(conn)
        .await?;
    }
    Ok(())
}

async fn write_title_job(conn: &mut PgConnection, thread: &ThreadState) -> Result<(), EngineError> {
    let Some(job) = &thread.title_job else {
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO threads.title_job (thread_id, job_id, state, created_at, ended_at) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (thread_id) DO UPDATE SET state = EXCLUDED.state, ended_at = EXCLUDED.ended_at",
    )
    .bind(thread.id)
    .bind(job.job_id)
    .bind(job.state.as_db())
    .bind(job.created_at)
    .bind(job.ended_at)
    .execute(conn)
    .await?;
    Ok(())
}

/// The threads of the members named, one read bounded by the engine's ceiling.
impl CohortIndex for ThreadStore {
    fn keys_in_cohorts<'a>(
        conn: &'a mut PgConnection,
        cohorts: &'a [Cohort],
        ceiling: KeyCeiling,
    ) -> BoxFuture<'a, Result<Vec<Uuid>, EngineError>> {
        Box::pin(async move {
            let members = Cohort::uuids(cohorts, MEMBER_DIM);
            let keys = sqlx::query_scalar(
                "SELECT DISTINCT thread_id FROM threads.thread_member \
                 WHERE user_id = ANY($1) ORDER BY thread_id LIMIT $2",
            )
            .bind(&members)
            .bind(ceiling.limit())
            .fetch_all(conn)
            .await?;
            Ok(keys)
        })
    }
}

/// One page of `member`'s threads, the most recently active first: those
/// before `cursor` (a thread of the list) when one is given. One statement.
pub async fn page_of_member(
    pg: &PgPool,
    member: Uuid,
    cursor: Option<Uuid>,
    limit: i64,
) -> Result<Vec<Uuid>, EngineError> {
    Ok(sqlx::query_scalar(
        "SELECT t.id FROM threads.thread t \
         JOIN threads.thread_member m ON m.thread_id = t.id AND m.user_id = $1 \
         WHERE $2::uuid IS NULL \
            OR (t.last_activity_at, t.id) < \
               (SELECT c.last_activity_at, c.id FROM threads.thread c WHERE c.id = $2) \
         ORDER BY t.last_activity_at DESC, t.id DESC LIMIT $3",
    )
    .bind(member)
    .bind(cursor)
    .bind(limit)
    .fetch_all(pg)
    .await?)
}

/// Every thread: the members offer's reconcile. Five statements.
pub async fn all_threads(conn: &mut PgConnection) -> Result<Vec<ThreadState>, EngineError> {
    let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM threads.thread ORDER BY id")
        .fetch_all(&mut *conn)
        .await?;
    Ok(read_many(conn, &ids)
        .await?
        .into_iter()
        .map(|(_, thread)| thread)
        .collect())
}

async fn read_many(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<(Uuid, ThreadState)>, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM {TABLE} t \
         LEFT JOIN threads.title_job j ON j.thread_id = t.id \
         WHERE t.id = ANY($1)"
    ))
    .bind(keys)
    .fetch_all(&mut *conn)
    .await?;
    let mut threads = rows
        .iter()
        .map(thread_of_row)
        .collect::<Result<Vec<_>, _>>()?;
    if threads.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<Uuid> = threads.iter().map(|thread| thread.id).collect();
    let mut members = members_of(conn, &ids).await?;
    let mut resources = resources_of(conn, &ids).await?;
    let mut read_marks = read_marks_of(conn, &ids).await?;
    for thread in &mut threads {
        thread.members = members.remove(&thread.id).unwrap_or_default();
        thread.resources = resources.remove(&thread.id).unwrap_or_default();
        thread.read_marks = read_marks.remove(&thread.id).unwrap_or_default();
    }
    Ok(threads
        .into_iter()
        .map(|thread| (thread.id, thread))
        .collect())
}

/// A row read back is validated, never trusted.
fn thread_of_row(row: &PgRow) -> Result<ThreadState, EngineError> {
    let status: String = row.try_get("status")?;
    let title: Option<String> = row.try_get("title")?;
    let parent: Option<Uuid> = row.try_get("parent_thread_id")?;
    let origin_block: Option<Uuid> = row.try_get("origin_block_id")?;
    let origin = match (parent, origin_block) {
        (Some(thread_id), Some(block_id)) => Some(ThreadOrigin {
            thread_id,
            block_id,
        }),
        (None, None) => None,
        _ => {
            return Err(corrupt(UnknownStoredValue {
                column: "thread.origin_block_id",
                value: "set without its parent, or the reverse".to_owned(),
            }));
        }
    };
    let job_id: Option<Uuid> = row.try_get("title_job_id")?;
    let job_state: Option<String> = row.try_get("title_job_state")?;
    let job_created_at: Option<DateTime<Utc>> = row.try_get("title_job_created_at")?;
    let title_job = match (job_id, job_state, job_created_at) {
        (Some(job_id), Some(state), Some(created_at)) => Some(TitleJob {
            job_id,
            state: TitleJobState::from_db(&state).map_err(corrupt)?,
            created_at,
            ended_at: row.try_get("title_job_ended_at")?,
        }),
        _ => None,
    };
    let resolved_by: Option<Uuid> = row.try_get("resolved_by")?;
    let resolved_at: Option<DateTime<Utc>> = row.try_get("resolved_at")?;
    let resolution = match (resolved_by, resolved_at) {
        (Some(by), Some(at)) => Some(Resolution { by, at }),
        (None, None) => None,
        _ => {
            return Err(corrupt(UnknownStoredValue {
                column: "thread.resolved_at",
                value: "set without its resolver, or the reverse".to_owned(),
            }));
        }
    };
    let created_at: DateTime<Utc> = row.try_get("created_at")?;
    Ok(ThreadState {
        id: row.try_get("id")?,
        owner_id: row.try_get("owner_id")?,
        title: title.map(ThreadTitle::parse).transpose().map_err(corrupt)?,
        origin,
        status: ThreadStatus::parse(&status).map_err(corrupt)?,
        resolution,
        created_at,
        last_activity_at: row.try_get("last_activity_at")?,
        version: row.try_get("version")?,
        members: BTreeMap::new(),
        resources: BTreeMap::new(),
        read_marks: BTreeMap::new(),
        title_job,
        pending: Vec::new(),
    })
}

async fn members_of(
    conn: &mut PgConnection,
    threads: &[Uuid],
) -> Result<HashMap<Uuid, BTreeMap<Uuid, Member>>, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT m.thread_id, m.user_id, k.display_name, COALESCE(k.in_roster, false) AS in_roster \
         FROM threads.thread_member m LEFT JOIN {KNOWN_USERS_TABLE} k ON k.user_id = m.user_id \
         WHERE m.thread_id = ANY($1)"
    ))
    .bind(threads)
    .fetch_all(conn)
    .await?;
    let mut by_thread: HashMap<Uuid, BTreeMap<Uuid, Member>> = HashMap::new();
    for row in &rows {
        by_thread
            .entry(row.try_get("thread_id")?)
            .or_default()
            .insert(
                row.try_get("user_id")?,
                Member {
                    display_name: row.try_get("display_name")?,
                    in_roster: row.try_get("in_roster")?,
                },
            );
    }
    Ok(by_thread)
}

async fn resources_of(
    conn: &mut PgConnection,
    threads: &[Uuid],
) -> Result<HashMap<Uuid, BTreeMap<String, Vec<String>>>, EngineError> {
    let rows = sqlx::query(
        "SELECT thread_id, key, ids FROM threads.thread_resource WHERE thread_id = ANY($1)",
    )
    .bind(threads)
    .fetch_all(conn)
    .await?;
    let mut by_thread: HashMap<Uuid, BTreeMap<String, Vec<String>>> = HashMap::new();
    for row in &rows {
        by_thread
            .entry(row.try_get("thread_id")?)
            .or_default()
            .insert(row.try_get("key")?, row.try_get("ids")?);
    }
    Ok(by_thread)
}

async fn read_marks_of(
    conn: &mut PgConnection,
    threads: &[Uuid],
) -> Result<HashMap<Uuid, BTreeMap<Uuid, Uuid>>, EngineError> {
    let rows = sqlx::query(
        "SELECT thread_id, user_id, last_read_message_id FROM threads.read_mark \
         WHERE thread_id = ANY($1)",
    )
    .bind(threads)
    .fetch_all(conn)
    .await?;
    let mut by_thread: HashMap<Uuid, BTreeMap<Uuid, Uuid>> = HashMap::new();
    for row in &rows {
        by_thread
            .entry(row.try_get("thread_id")?)
            .or_default()
            .insert(
                row.try_get("user_id")?,
                row.try_get("last_read_message_id")?,
            );
    }
    Ok(by_thread)
}
