//! The caller's facts, loaded by the engine's principal-fact loader on every
//! request, every subscription upgrade and every principal refresh: one
//! statement for a person, none for a service account (no roster entry names
//! one).

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use sqlx::PgPool;

use super::{CallerFacts, RosterStanding, ThreadsPrincipal};
use crate::directory::KNOWN_USERS_TABLE;

type StandingRow = (String, Option<DateTime<Utc>>, bool);

/// Fills `principal`'s facts.
pub fn loader<'a>(
    pg: &'a PgPool,
    principal: &'a mut ThreadsPrincipal,
) -> BoxFuture<'a, Result<(), EngineError>> {
    Box::pin(async move {
        let standing = match principal.user() {
            None => RosterStanding::Unknown,
            Some(user) => {
                let row: Option<StandingRow> = sqlx::query_as(&format!(
                    "SELECT display_name, disabled_at, in_roster FROM {KNOWN_USERS_TABLE} \
                     WHERE user_id = $1"
                ))
                .bind(user)
                .fetch_optional(pg)
                .await?;
                standing_of(row)
            }
        };
        *principal.facts_mut() = CallerFacts { standing };
        Ok(())
    })
}

pub(crate) fn standing_of(row: Option<StandingRow>) -> RosterStanding {
    match row {
        None => RosterStanding::Unknown,
        Some((_, _, false)) => RosterStanding::OutOfRoster,
        Some((_, Some(_), true)) => RosterStanding::Deactivated,
        Some((display_name, None, true)) => RosterStanding::Active { display_name },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_roster_row_reads_as_its_standing() {
        assert_eq!(standing_of(None), RosterStanding::Unknown);
        assert_eq!(
            standing_of(Some(("Ada".into(), None, true))),
            RosterStanding::Active {
                display_name: "Ada".into()
            }
        );
        assert_eq!(
            standing_of(Some(("Ada".into(), Some(Utc::now()), true))),
            RosterStanding::Deactivated
        );
        assert_eq!(
            standing_of(Some(("Ada".into(), None, false))),
            RosterStanding::OutOfRoster,
            "out of the roster wins over any deactivation"
        );
        assert_eq!(
            standing_of(Some(("Ada".into(), Some(Utc::now()), false))),
            RosterStanding::OutOfRoster
        );
    }
}
