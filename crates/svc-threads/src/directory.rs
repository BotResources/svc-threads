//! `threads.known_users`: Threads' local copy of Identity's roster, projected
//! by the engine's directory mirror from the `identity/users/…` keys of the
//! `PUBLISHED_LANGUAGE` bucket. Admission, the owner and member checks of the
//! commands and every displayed name read it; Threads never asks Identity at
//! request time.
//!
//! A user who leaves the roster is not removed: the retraction keeps their row,
//! marked `in_roster = false` with its last name, so that the threads, messages
//! and read marks that refer to them stay whole; a key that comes back marks
//! them in the roster again. A user Threads never read stays unknown, so
//! refused (admission fails closed). A present entry Threads cannot read is
//! logged and ignored: its row, if any, stays as it was.
//!
//! The mirror holds readiness until its first read converges, reconciles
//! periodically, and at every start re-projects every id the table holds — so a
//! user retracted while the service was down is marked out of the roster at
//! reconciliation. Every row inserted or changed stages an impact on the
//! `threads.known_user` foreign namespace and, since the caller's standing is
//! loaded from this table, a principal-facts change for that user, so their open
//! sessions are re-evaluated (and ended, once they are no longer active).
//!
//! What a retraction does beyond the row — deleting the threads the user owns —
//! is not a projection's to do: a mirror projection can stage no pipeline work.
//! The departed-owner sweep carries it ([`crate::slices::thread::departure`]).

use br_core_directory::{PublishedUser, USERS_KEY_PREFIX, user_id_from_kv_key};
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{
    Change, Column, KnownRow, Mirror, MirrorReady, PrincipalColumn, Project, Projection, RowScope,
    col,
};
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use service_engine::{Consumed, Shadows};
use sqlx::PgPool;
use uuid::Uuid;

use crate::principal::ROSTER_DEPS;

pub const DIRECTORY_MIRROR: MirrorName = MirrorName::from_static("directory");
pub const KNOWN_USERS_TABLE: &str = "threads.known_users";
pub const KNOWN_USER_NAMESPACE: &str = "threads.known_user";

/// One roster entry as Identity publishes it: the `br-core-directory` core
/// fields, and the extensions Threads reads by name ([`ReadExtensions`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RosterUser(pub PublishedUser);

impl Consumed for RosterUser {
    const PREFIX: &'static str = USERS_KEY_PREFIX;
}

/// A row of `threads.known_users`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownUser {
    pub user_id: Uuid,
    pub display_name: String,
    /// Set when Identity deactivated the user.
    pub disabled_at: Option<DateTime<Utc>>,
    /// False once the user left the roster.
    pub in_roster: bool,
}

/// The two extensions of a roster entry Threads reads, both optional. Anything
/// else in the extensions is not Threads' concern; a present value of another
/// shape makes the entry unreadable.
#[derive(Debug, Default, Deserialize)]
struct ReadExtensions {
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    disabled_at: Option<DateTime<Utc>>,
}

impl KnownUser {
    /// The row a roster entry projects to, or `None` when Threads cannot read
    /// its extensions. The display name is the entry's `display_name`, else its
    /// first and last name, else its email.
    pub fn of(user_id: Uuid, entry: &PublishedUser) -> Option<Self> {
        let extensions = serde_json::Value::Object(
            entry
                .extensions()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        );
        let read: ReadExtensions = serde_json::from_value(extensions).ok()?;
        let display_name = read
            .display_name
            .filter(|name| !name.trim().is_empty())
            .or_else(|| full_name(entry))
            .unwrap_or_else(|| entry.email.clone());
        Some(Self {
            user_id,
            display_name,
            disabled_at: read.disabled_at,
            in_roster: true,
        })
    }

    /// The row kept for a user who left the roster: their last name, out of
    /// the roster.
    fn out_of_roster(mut self) -> Self {
        self.in_roster = false;
        self
    }
}

fn full_name(entry: &PublishedUser) -> Option<String> {
    let parts: Vec<&str> = [entry.first_name.as_deref(), entry.last_name.as_deref()]
        .into_iter()
        .flatten()
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect();
    (!parts.is_empty()).then(|| parts.join(" "))
}

impl KnownRow for KnownUser {
    const TABLE: &'static str = KNOWN_USERS_TABLE;
    const NAMESPACE: &'static str = KNOWN_USER_NAMESPACE;
    const KEY: &'static [&'static str] = &["user_id"];
    const PRINCIPAL: Option<PrincipalColumn> = Some(PrincipalColumn {
        column: "user_id",
        deps: ROSTER_DEPS,
    });

    fn key(&self) -> Vec<Column> {
        vec![col("user_id", self.user_id)]
    }

    fn values(&self) -> Vec<Column> {
        vec![
            col("display_name", self.display_name.clone()),
            col("disabled_at", self.disabled_at),
            col("in_roster", self.in_roster),
        ]
    }
}

/// Of `user_ids`, those the roster copy holds and still in the roster: the only
/// users a command may name as an owner or a member. One statement.
pub async fn in_roster(
    conn: &mut sqlx::PgConnection,
    user_ids: &[Uuid],
) -> Result<std::collections::BTreeSet<Uuid>, EngineError> {
    if user_ids.is_empty() {
        return Ok(Default::default());
    }
    let held: Vec<Uuid> = sqlx::query_scalar(&format!(
        "SELECT user_id FROM {KNOWN_USERS_TABLE} WHERE user_id = ANY($1) AND in_roster"
    ))
    .bind(user_ids)
    .fetch_all(conn)
    .await?;
    Ok(held.into_iter().collect())
}

/// The current display name of `user`, if the roster copy holds them.
pub async fn display_name(
    conn: &mut sqlx::PgConnection,
    user: Uuid,
) -> Result<Option<String>, EngineError> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT display_name FROM {KNOWN_USERS_TABLE} WHERE user_id = $1"
    ))
    .bind(user)
    .fetch_optional(conn)
    .await?)
}

fn user_key(user_id: Uuid) -> KvKey {
    KvKey::new(br_core_directory::user_kv_key(user_id))
        .expect("the roster key of a UUID is a valid KV key")
}

/// The rows the table holds for `user_ids`, in one read.
async fn held_rows(
    cx: &mut Projection<'_>,
    user_ids: &[Uuid],
) -> Result<Vec<KnownUser>, EngineError> {
    if user_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(Uuid, String, Option<DateTime<Utc>>, bool)> = sqlx::query_as(&format!(
        "SELECT user_id, display_name, disabled_at, in_roster FROM {KNOWN_USERS_TABLE} \
         WHERE user_id = ANY($1)"
    ))
    .bind(user_ids)
    .fetch_all(cx.conn())
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(user_id, display_name, disabled_at, in_roster)| KnownUser {
                user_id,
                display_name,
                disabled_at,
                in_roster,
            },
        )
        .collect())
}

struct KnownUsersProjection;

impl Project<Uuid> for KnownUsersProjection {
    type Error = EngineError;

    /// The whole batch in one read and one write. A key whose entry is present
    /// and readable becomes its row, in the roster. A key with no entry
    /// (retracted) keeps the row it had, marked out of the roster — or has none,
    /// when Threads never held it. A key whose entry is present but unreadable
    /// is left out of the scope: its row stays as it was.
    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        user_ids: Vec<Uuid>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let (mut rows, retracted, unreadable) = {
                let roster = cx.shadow::<RosterUser>();
                let mut rows = Vec::with_capacity(user_ids.len());
                let mut retracted = Vec::new();
                let mut unreadable = Vec::new();
                for user_id in &user_ids {
                    match roster.get(&user_key(*user_id)) {
                        None => retracted.push(*user_id),
                        Some(RosterUser(entry)) => match KnownUser::of(*user_id, entry) {
                            Some(known) => rows.push(known),
                            None => unreadable.push(*user_id),
                        },
                    }
                }
                (rows, retracted, unreadable)
            };
            rows.extend(
                held_rows(&mut cx, &retracted)
                    .await?
                    .into_iter()
                    .map(KnownUser::out_of_roster),
            );
            let replaced: Vec<Uuid> = user_ids
                .into_iter()
                .filter(|user_id| !unreadable.contains(user_id))
                .collect();
            cx.replace_rows(RowScope::any_of("user_id", replaced), rows)
                .await?;
            if !unreadable.is_empty() {
                tracing::warn!(
                    users = ?unreadable,
                    "roster entries whose extensions Threads cannot read: ignored, their rows \
                     left as they were"
                );
            }
            Ok(())
        })
    }
}

pub fn directory_mirror() -> MirrorReady<Uuid, impl Project<Uuid>> {
    Mirror::new(DIRECTORY_MIRROR)
        .consume::<RosterUser>()
        .keyed_by(|_shadows: &Shadows, change: &Change| {
            user_id_from_kv_key(change.key.as_str())
                .into_iter()
                .collect::<Vec<Uuid>>()
        })
        .project(KnownUsersProjection)
        .reconcile_keys(|pool: PgPool| {
            Box::pin(async move {
                let ids: Vec<Uuid> =
                    sqlx::query_scalar(&format!("SELECT user_id FROM {KNOWN_USERS_TABLE}"))
                        .fetch_all(&pool)
                        .await?;
                Ok(ids)
            }) as BoxFuture<'static, Result<Vec<Uuid>, EngineError>>
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn entry(
        first: Option<&str>,
        last: Option<&str>,
        extensions: BTreeMap<String, serde_json::Value>,
    ) -> PublishedUser {
        PublishedUser::new(
            "ada@example.test".to_string(),
            first.map(str::to_string),
            last.map(str::to_string),
            extensions,
        )
        .expect("a valid roster entry")
    }

    fn extensions(pairs: &[(&str, serde_json::Value)]) -> BTreeMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect()
    }

    #[test]
    fn the_display_name_is_the_published_one_else_the_full_name_else_the_email() {
        let user = Uuid::now_v7();
        let named = entry(
            Some("Ada"),
            Some("Lovelace"),
            extensions(&[("display_name", serde_json::json!("Countess"))]),
        );
        assert_eq!(
            KnownUser::of(user, &named).unwrap().display_name,
            "Countess"
        );

        let unnamed = entry(Some("Ada"), Some("Lovelace"), BTreeMap::new());
        assert_eq!(
            KnownUser::of(user, &unnamed).unwrap().display_name,
            "Ada Lovelace"
        );

        let blank = entry(
            None,
            Some(" "),
            extensions(&[("display_name", serde_json::json!("  "))]),
        );
        assert_eq!(
            KnownUser::of(user, &blank).unwrap().display_name,
            "ada@example.test"
        );
    }

    #[test]
    fn a_deactivation_is_read_from_the_extensions() {
        let at = Utc::now();
        let deactivated = entry(
            Some("Ada"),
            None,
            extensions(&[("disabled_at", serde_json::json!(at))]),
        );
        let known = KnownUser::of(Uuid::now_v7(), &deactivated).unwrap();
        assert_eq!(known.disabled_at, Some(at));
        assert!(known.in_roster);
    }

    #[test]
    fn an_entry_whose_extensions_threads_cannot_read_projects_nothing() {
        let garbled = entry(
            Some("Ada"),
            None,
            extensions(&[("display_name", serde_json::json!(42))]),
        );
        assert_eq!(KnownUser::of(Uuid::now_v7(), &garbled), None);
        let bad_date = entry(
            Some("Ada"),
            None,
            extensions(&[("disabled_at", serde_json::json!("yesterday"))]),
        );
        assert_eq!(KnownUser::of(Uuid::now_v7(), &bad_date), None);
    }

    #[test]
    fn extensions_threads_does_not_read_are_ignored() {
        let rich = entry(
            Some("Ada"),
            None,
            extensions(&[
                (
                    "relations",
                    serde_json::json!([{ "group_id": Uuid::nil() }]),
                ),
                ("next_version_field", serde_json::json!({})),
            ]),
        );
        assert_eq!(
            KnownUser::of(Uuid::now_v7(), &rich).unwrap().display_name,
            "Ada"
        );
    }

    #[test]
    fn a_user_who_left_the_roster_keeps_their_name() {
        let known = KnownUser {
            user_id: Uuid::now_v7(),
            display_name: "Ada".into(),
            disabled_at: None,
            in_roster: true,
        };
        let kept = known.clone().out_of_roster();
        assert!(!kept.in_roster);
        assert_eq!(kept.display_name, known.display_name);
    }

    #[test]
    fn the_mirror_consumes_the_users_only() {
        let mirror = directory_mirror();
        let prefixes: Vec<&str> = mirror.guards().iter().map(|guard| guard.prefix()).collect();
        assert_eq!(prefixes, vec![USERS_KEY_PREFIX]);
        mirror.validate().expect("a valid mirror");
    }
}
