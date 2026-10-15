//! The watch over a `STREAMING` message: a writer that pushes nothing for the
//! idle window before closing its stream has its message failed, and a stream
//! whose chunk the library refused has its failure stored.
//!
//! ENGINE GAP (br-service-engine v0.4.1): the accumulated lane keeps no
//! last-chunk time a service may read, and a fold can neither fail its message
//! nor trigger work after it commits. The watch therefore runs on Threads' own
//! scheduled message, every quarter of the idle window while the message
//! streams: it compares how far the stream has come (the folded state's
//! contiguous sequence, read through the engine's reader) with what it saw the
//! last time — kept in `threads.stream_watch`, so that a forged or repeated
//! message cannot cut a stream short — and fails the message once the stream
//! has not moved for the whole window, or as soon as it sees the stream's
//! refusal — within one period, a quarter of the window. The engine would need
//! a last-chunk time on the accumulated state and a post-fold hook that can run
//! pipeline work.

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::{AccumulatorError, EngineError};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::{Ops, Reaction};
use uuid::Uuid;

use super::closing;
use super::draft::CloseKind;
use super::state::MessageState;
use super::store;
use super::stream::MessageContent;
use crate::facts::FactMeta;
use crate::fault::Fault;
use crate::principal::ThreadsPrincipal;
use crate::slices::thread::state::ThreadState;

/// Why the watch failed a message, as its `Failed` fact says when the stream
/// itself was not refused.
pub const STREAM_IDLE: &str = "stream_idle";

/// Threads' own scheduled message: time to look at a streaming message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamWatchDue {
    pub message_id: Uuid,
    /// The look this message is for: only the one the watch last scheduled is
    /// acted on, so that a repeated or forged message starts no second chain.
    pub tick: Uuid,
}

impl ReactionMessage for StreamWatchDue {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(contract_threads::coords::command_coords(
            "message",
            "stream_watch_due",
        ))
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

fn window() -> TimeDelta {
    TimeDelta::from_std(crate::config::installed().stream_idle_window).unwrap_or(TimeDelta::MAX)
}

fn period() -> TimeDelta {
    TimeDelta::from_std(crate::config::installed().stream_watch_period()).unwrap_or(TimeDelta::MAX)
}

/// Schedules the next look, under a fresh tick the watch row remembers.
async fn schedule(ops: &mut Ops<'_>, message: Uuid, at: DateTime<Utc>) -> Result<(), Fault> {
    let tick = Uuid::now_v7();
    sqlx::query("UPDATE threads.stream_watch SET tick = $2 WHERE message_id = $1")
        .bind(message)
        .bind(tick)
        .execute(ops.connection())
        .await?;
    ops.schedule_at(
        service_engine::Timestamp::from_utc(at),
        StreamWatchDue {
            message_id: message,
            tick,
        },
    )?;
    Ok(())
}

/// Starts watching a message opened `STREAMING` at `now`.
pub async fn open(ops: &mut Ops<'_>, message: Uuid, now: DateTime<Utc>) -> Result<(), Fault> {
    sqlx::query(
        "INSERT INTO threads.stream_watch (message_id, seen_through, since, tick) \
         VALUES ($1, NULL, $2, $3)",
    )
    .bind(message)
    .bind(now)
    .bind(Uuid::nil())
    .execute(ops.connection())
    .await?;
    schedule(ops, message, now + period()).await
}

/// Ends the watch of a message whose stream closed.
pub async fn end(ops: &mut Ops<'_>, message: Uuid) -> Result<(), Fault> {
    sqlx::query("DELETE FROM threads.stream_watch WHERE message_id = $1")
        .bind(message)
        .execute(ops.connection())
        .await?;
    Ok(())
}

/// What the watch decides when it looks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The stream moved: remember how far, look again a period later.
    Moved,
    /// Still within the window: look again.
    Wait { next: DateTime<Utc> },
    /// Silent for the whole window, or refused: fail the message.
    Fail,
}

/// The watch's one decision, on what it saw last (`seen`, since `since`) and
/// what the stream holds now.
pub fn decide(
    seen: Option<u64>,
    since: DateTime<Utc>,
    now_through: Option<u64>,
    refused: bool,
    now: DateTime<Utc>,
) -> Verdict {
    if refused {
        return Verdict::Fail;
    }
    if now_through != seen {
        return Verdict::Moved;
    }
    let deadline = since + window();
    if deadline <= now {
        Verdict::Fail
    } else {
        Verdict::Wait {
            next: (now + period()).min(deadline),
        }
    }
}

pub fn due<'r>(
    cx: &'r mut Reaction<'r>,
    message: StreamWatchDue,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        // Threads schedules its looks itself, from a reaction: a service sends
        // it. Anything else is not the watch's.
        if !cx.actor().is_some_and(|actor| actor.is_service()) {
            return Ok(());
        }
        // The thread first, then the message: the one lock order of the
        // service (`slices::thread::store`), which a thread's deletion takes too.
        let Some(thread) = store::thread_of(cx.connection(), message.message_id).await? else {
            return Ok(());
        };
        cx.load::<ThreadState>(&thread).await?;
        let Some(state) = cx.load::<MessageState>(&message.message_id).await? else {
            return Ok(());
        };
        if state.not_streaming().is_some() {
            return Ok(());
        }
        let watched: Option<(Option<i64>, DateTime<Utc>, Uuid)> = sqlx::query_as(
            "SELECT seen_through, since, tick FROM threads.stream_watch WHERE message_id = $1",
        )
        .bind(state.id)
        .fetch_optional(cx.connection())
        .await?;
        let Some((seen, since, tick)) = watched else {
            return Ok(());
        };
        if tick != message.tick {
            return Ok(());
        }
        let now = cx.now().as_datetime();
        let held = cx.accumulated::<MessageContent>(&state.id).await?;
        let through = held.contiguous_to.map(|seq| seq.get());
        match decide(
            seen.map(|seq| seq as u64),
            since,
            through,
            held.state.refusal().is_some(),
            now,
        ) {
            Verdict::Moved => {
                sqlx::query(
                    "UPDATE threads.stream_watch SET seen_through = $2, since = $3 \
                     WHERE message_id = $1",
                )
                .bind(state.id)
                .bind(through.map(|seq| seq as i64))
                .bind(now)
                .execute(cx.connection())
                .await?;
                schedule(cx, state.id, now + period()).await
            }
            Verdict::Wait { next } => schedule(cx, state.id, next).await,
            Verdict::Fail => {
                let sealed = match cx.seal_current::<MessageContent>(&state.id).await {
                    Ok(sealed) => sealed,
                    Err(EngineError::Accumulator(AccumulatorError::AlreadySealed { .. })) => {
                        return Ok(());
                    }
                    Err(error) => return Err(error.into()),
                };
                let meta = FactMeta {
                    actor: ThreadsPrincipal::threads_itself().actor(),
                    acting_user_id: None,
                    impersonator_id: None,
                    correlation_id: cx.correlation_id().unwrap_or_else(|| cx.message_id()),
                    causation_id: Some(cx.message_id()),
                    occurred_at: now,
                };
                closing::store_closed(
                    cx,
                    state,
                    &sealed,
                    CloseKind::Fail,
                    Vec::new(),
                    Some(STREAM_IDLE),
                    &meta,
                )
                .await
                .map(|_| ())
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_that_moved_is_looked_at_again() {
        let now = Utc::now();
        assert_eq!(decide(None, now, Some(0), false, now), Verdict::Moved);
        assert_eq!(decide(Some(3), now, Some(4), false, now), Verdict::Moved);
    }

    #[test]
    fn a_silent_stream_fails_once_the_whole_window_passed() {
        let since = Utc::now();
        let within = since + window() - TimeDelta::seconds(1);
        assert!(matches!(
            decide(Some(3), since, Some(3), false, within),
            Verdict::Wait { next } if next <= since + window()
        ));
        assert_eq!(
            decide(Some(3), since, Some(3), false, since + window()),
            Verdict::Fail
        );
        assert_eq!(
            decide(None, since, None, false, since + window()),
            Verdict::Fail,
            "a writer that never pushed is silent too"
        );
    }

    #[test]
    fn a_refused_stream_fails_at_once() {
        let now = Utc::now();
        assert_eq!(decide(None, now, Some(1), true, now), Verdict::Fail);
    }
}
