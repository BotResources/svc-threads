//! `threads.thread` and the rows that are part of a thread's state —
//! `thread_member`, `thread_resource`, `read_mark`, `title_job` — through the
//! engine's `Persistence` seam, soft EDA.
//!
//! A batch read is four statements whatever its size: the threads with their
//! title Job, their members with the roster copy's name, their resources, their
//! read marks. The write path (`save`, `create`, `delete`, each with the facts
//! it records in `threads.fact`) is the implementation pass's.
//!
//! Concurrency: the engine serialises commands on one key with a per-key
//! transaction advisory lock taken when the pipeline loads it; `lock` adds a row
//! lock that pins the row image.

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
    MEMBER_DIM, Member, ThreadOrigin, ThreadState, TitleJob, TitleJobState, UnknownStoredValue,
};
use crate::directory::KNOWN_USERS_TABLE;
use crate::fault::{self, corrupt};

pub const TABLE: &str = "threads.thread";
const COLUMNS: &str = "t.id, t.owner_id, t.title, t.parent_thread_id, t.origin_block_id, \
                       t.status, t.created_at, t.last_activity_at, t.version, \
                       j.job_id AS title_job_id, j.state AS title_job_state";

pub struct ThreadStore;

impl Persistence for ThreadStore {
    type Aggregate = ThreadState;
    type Key = Uuid;
    // The thread's facts are the implementation pass's, with its transitions.
    type Event = ();

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
        _conn: &'a mut PgConnection,
        _thread: &'a ThreadState,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(fault::write_path_pending("thread")) })
    }

    fn create<'a>(
        _conn: &'a mut PgConnection,
        _thread: &'a ThreadState,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(fault::write_path_pending("thread")) })
    }
}

impl Aggregate for ThreadState {
    type Store = ThreadStore;

    fn key(&self) -> Uuid {
        self.id
    }
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
    let title_job = match (job_id, job_state) {
        (Some(job_id), Some(state)) => Some(TitleJob {
            job_id,
            state: TitleJobState::from_db(&state).map_err(corrupt)?,
        }),
        _ => None,
    };
    let created_at: DateTime<Utc> = row.try_get("created_at")?;
    Ok(ThreadState {
        id: row.try_get("id")?,
        owner_id: row.try_get("owner_id")?,
        title: title.map(ThreadTitle::parse).transpose().map_err(corrupt)?,
        origin,
        status: ThreadStatus::parse(&status).map_err(corrupt)?,
        created_at,
        last_activity_at: row.try_get("last_activity_at")?,
        version: row.try_get("version")?,
        members: BTreeMap::new(),
        resources: BTreeMap::new(),
        read_marks: BTreeMap::new(),
        title_job,
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
