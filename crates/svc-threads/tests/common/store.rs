//! Threads' store as the app role reads it, table by table, without naming
//! one: the tables a message's content lives in are the implementation's to
//! choose, and what the specification demands of them is about every table —
//! "a deleted message keeps no content anywhere", "no stored message carries a
//! name", "no message is converted half-way".
//!
//! Read-only, `SELECT` on the app role, over every table of the `threads`
//! schema. The suite never writes a row.
//!
//! "Anywhere" is literal: [`rows_holding`] and [`assert_nowhere`] also read
//! the engine's tables a message's content can pass through in the
//! `service_engine` schema — the event log and its snapshots, the accumulator's
//! chunks, a dead letter's payload. The snapshots ([`snapshot`],
//! [`snapshot_json`]) stay on the `threads` schema: they compare Threads' own
//! state across a boot, which the engine's bookkeeping is free to move.

use std::collections::BTreeMap;

use sqlx::PgPool;

/// The tables of the `threads` schema, by name.
pub async fn tables(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'threads' AND c.relkind IN ('r', 'p') ORDER BY 1",
    )
    .fetch_all(pool)
    .await
    .expect("list the tables of the threads schema")
}

/// Every row of `table`, as its JSON text, sorted.
pub async fn rows(pool: &PgPool, table: &str) -> Vec<String> {
    let mut rows: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT row_to_json(t)::text FROM threads.\"{table}\" t"
    ))
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("the app role reads threads.{table}: {e}"));
    rows.sort();
    rows
}

/// The tables of the `service_engine` schema a message's content can pass
/// through, each with how a row of it is read as text: whole as its JSON, or —
/// for a dead letter — its payload alone, the bytes the engine could not
/// handle, read as text (`escape` keeps ASCII as is).
const ENGINE_TABLES: [(&str, &str); 4] = [
    ("event_log", "row_to_json(t)::text"),
    ("event_snapshot", "row_to_json(t)::text"),
    ("accumulator_chunk", "row_to_json(t)::text"),
    ("dead_letter", "encode(t.payload, 'escape')"),
];

/// Every row of the engine's `table`, read as `read`.
async fn engine_rows(pool: &PgPool, table: &str, read: &str) -> Vec<String> {
    sqlx::query_scalar(&format!("SELECT {read} FROM service_engine.\"{table}\" t"))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("the app role reads service_engine.{table}: {e}"))
}

/// Whether `except` names `table`: by its bare name for a table of the
/// `threads` schema, by its qualified name for any.
fn excepted(except: &[&str], schema: &str, table: &str) -> bool {
    let qualified = format!("{schema}.{table}");
    except
        .iter()
        .any(|name| *name == qualified || (schema == "threads" && *name == table))
}

/// Every row of every table, by table.
pub async fn snapshot(pool: &PgPool) -> BTreeMap<String, Vec<String>> {
    let mut snapshot = BTreeMap::new();
    for table in tables(pool).await {
        let rows = rows(pool, &table).await;
        snapshot.insert(table, rows);
    }
    snapshot
}

/// The rows, of any table of the `threads` schema and of the engine's tables
/// a content passes through, but those of `except`, whose text holds
/// `needle`: `(schema.table, row)`. A table of `except` is named bare
/// (`"fact"`) for the `threads` schema, qualified for either
/// (`"service_engine.event_log"`).
pub async fn rows_holding(pool: &PgPool, needle: &str, except: &[&str]) -> Vec<(String, String)> {
    let mut found = Vec::new();
    for table in tables(pool).await {
        if excepted(except, "threads", &table) {
            continue;
        }
        for row in rows(pool, &table).await {
            if row.contains(needle) {
                found.push((format!("threads.{table}"), row));
            }
        }
    }
    for (table, read) in ENGINE_TABLES {
        if excepted(except, "service_engine", table) {
            continue;
        }
        for row in engine_rows(pool, table, read).await {
            if row.contains(needle) {
                found.push((format!("service_engine.{table}"), row));
            }
        }
    }
    found
}

/// No row of the store, but those of the `except` tables, holds `needle`.
pub async fn assert_nowhere(pool: &PgPool, needle: &str, except: &[&str], what: &str) {
    let found = rows_holding(pool, needle, except).await;
    assert!(
        found.is_empty(),
        "{what}: {needle:?} is still stored in {found:?}"
    );
}

/// Every row of every table, as JSON objects, by table.
pub async fn snapshot_json(pool: &PgPool) -> BTreeMap<String, Vec<serde_json::Value>> {
    let mut snapshot = BTreeMap::new();
    for table in tables(pool).await {
        let rows: Vec<serde_json::Value> = rows(pool, &table)
            .await
            .iter()
            .map(|row| serde_json::from_str(row).expect("a row as JSON"))
            .collect();
        snapshot.insert(table, rows);
    }
    snapshot
}

/// The rows of `after` restricted to the columns `before` had, for every table
/// `before` had: what a schema migration that only adds tables or columns
/// leaves equal to `before`.
pub fn restricted_to(
    before: &BTreeMap<String, Vec<serde_json::Value>>,
    after: &BTreeMap<String, Vec<serde_json::Value>>,
) -> BTreeMap<String, Vec<String>> {
    let mut restricted = BTreeMap::new();
    for (table, rows) in before {
        let columns: Vec<String> = rows
            .first()
            .and_then(|row| row.as_object())
            .map(|row| row.keys().cloned().collect())
            .unwrap_or_default();
        let mut kept: Vec<String> = after
            .get(table)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|row| {
                let object = row.as_object().expect("a row is an object");
                let only: serde_json::Map<String, serde_json::Value> = columns
                    .iter()
                    .map(|column| {
                        (
                            column.clone(),
                            object
                                .get(column)
                                .cloned()
                                .unwrap_or(serde_json::Value::Null),
                        )
                    })
                    .collect();
                serde_json::Value::Object(only).to_string()
            })
            .collect();
        kept.sort();
        restricted.insert(table.clone(), kept);
    }
    restricted
}
