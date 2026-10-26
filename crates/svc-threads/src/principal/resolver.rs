//! Who a live session's principal is now, asked by the engine whenever that
//! principal's facts may have moved (the directory mirror stages a
//! principal-facts change for every user whose roster row moved).
//!
//! A person the roster no longer holds as an active entry is answered `None`:
//! the engine then ends every live session of that person on every pod —
//! they are refused everything, streams included — and the client reconnects
//! to a fresh refusal. Anyone else keeps their Passport and the engine reloads
//! their facts. A service account holds no session on the human surface and
//! is kept as it is.

use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::principal::PrincipalResolver;
use sqlx::PgPool;

use super::ThreadsPrincipal;
use super::facts::standing_of;
use crate::directory::KNOWN_USERS_TABLE;

pub struct ThreadsPrincipalResolver;

impl PrincipalResolver<ThreadsPrincipal> for ThreadsPrincipalResolver {
    fn resolve<'a>(
        &'a self,
        pg: &'a PgPool,
        current: &'a ThreadsPrincipal,
    ) -> BoxFuture<'a, Result<Option<ThreadsPrincipal>, EngineError>> {
        Box::pin(async move {
            let Some(user) = current.user() else {
                return Ok(Some(current.clone()));
            };
            let row = sqlx::query_as(&format!(
                "SELECT display_name, disabled_at, in_roster FROM {KNOWN_USERS_TABLE} \
                 WHERE user_id = $1"
            ))
            .bind(user)
            .fetch_optional(pg)
            .await?;
            Ok(standing_of(row).is_active().then(|| current.clone()))
        })
    }
}
