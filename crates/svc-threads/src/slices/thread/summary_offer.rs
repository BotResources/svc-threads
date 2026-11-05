//! The summary of each thread, published as `threads.thread_summary.{thread_id}`
//! in the `PUBLISHED_LANGUAGE` bucket so that other services list
//! conversations without asking Threads: title, parent and origin, status,
//! resolution, creation and last activity.
//!
//! The engine's offer machinery owns the publication, as for the members
//! (`super::offer`): every save or deletion of a thread stages its dirty key in
//! the transaction of the change, and the offer's leader drains it (a put, or a
//! retraction for a deleted thread), reconciles the whole keyspace on its first
//! drain after boot and periodically, and publishes the manifest
//! `threads.thread_summary_manifest`.
//!
//! What moves the entry, and how often: a thread is saved by its commands
//! (title, close, resolve, members, resources, read marks), when a message is
//! stored — a complete message, or a streamed one when its stream opens — and
//! when its writer closes a streamed message (complete, fail or cancel), each
//! of which moves `last_activity_at` (`ThreadState::record_activity`). A chunk
//! of a stream and a message's edit or deletion never save the thread, so a
//! streamed answer rewrites the entry twice — at its opening and at its close —
//! never per chunk. Saves that leave the summary as it was (members, resources, read
//! marks) are staged too, but the relay finds the entry already holding that
//! value and writes nothing; staged keys coalesce per thread until drained.
//! No timer coalescing is needed on top.

use contract_threads::PublishedThreadSummary;
use contract_threads::summary::{SUMMARY_PREFIX, SUMMARY_VERSION, summary_key};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::nats::KvKey;
use service_engine::offer::Offer;
use sqlx::PgConnection;

use super::state::ThreadState;
use super::store;

pub struct SummaryOffer;

impl Offer for SummaryOffer {
    type Row = ThreadState;
    type Published = PublishedThreadSummary;

    const NAME: &'static str = "thread_summary";
    const PREFIX: &'static str = SUMMARY_PREFIX;
    const VERSION: u16 = SUMMARY_VERSION;

    fn key(thread: &ThreadState) -> Result<KvKey, EngineError> {
        KvKey::new(summary_key(thread.id))
            .map_err(|error| EngineError::Config(format!("summary offer key: {error}")))
    }

    fn publish(thread: &ThreadState) -> Option<PublishedThreadSummary> {
        Some(entry_of(thread))
    }

    fn all(conn: &mut PgConnection) -> BoxFuture<'_, Result<Vec<ThreadState>, EngineError>> {
        Box::pin(async move { store::all_threads(conn).await })
    }
}

/// The entry of one thread.
pub fn entry_of(thread: &ThreadState) -> PublishedThreadSummary {
    PublishedThreadSummary {
        thread_id: thread.id,
        title: thread.title.as_ref().map(|title| title.as_str().to_owned()),
        parent_thread_id: thread.origin.map(|origin| origin.thread_id),
        origin_block_id: thread.origin.map(|origin| origin.block_id),
        status: thread.status,
        resolved_at: thread.resolution.map(|resolution| resolution.at),
        created_at: thread.created_at,
        last_activity_at: thread.last_activity_at,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use contract_threads::{ThreadStatus, ThreadTitle};
    use uuid::Uuid;

    use super::*;
    use crate::slices::thread::state::tests::thread_with;
    use crate::slices::thread::state::{Resolution, ThreadOrigin};

    #[test]
    fn a_top_level_untitled_thread_publishes_no_title_no_parent_no_resolution() {
        let thread = thread_with(&[Uuid::now_v7()]);
        let entry = entry_of(&thread);
        assert_eq!(entry.thread_id, thread.id);
        assert_eq!(entry.title, None);
        assert_eq!(entry.parent_thread_id, None);
        assert_eq!(entry.origin_block_id, None);
        assert_eq!(entry.resolved_at, None);
        assert_eq!(entry.status, ThreadStatus::Open);
        assert_eq!(entry.created_at, thread.created_at);
        assert_eq!(entry.last_activity_at, thread.last_activity_at);
    }

    #[test]
    fn a_titled_resolved_child_publishes_its_title_origin_and_resolution() {
        let mut thread = thread_with(&[Uuid::now_v7()]);
        let (parent, block, resolver) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        thread.title = Some(ThreadTitle::parse("Q3 figures").unwrap());
        thread.origin = Some(ThreadOrigin {
            thread_id: parent,
            block_id: block,
        });
        let at = thread.created_at + Duration::minutes(5);
        thread.resolution = Some(Resolution { by: resolver, at });
        let entry = entry_of(&thread);
        assert_eq!(entry.title.as_deref(), Some("Q3 figures"));
        assert_eq!(entry.parent_thread_id, Some(parent));
        assert_eq!(entry.origin_block_id, Some(block));
        assert_eq!(entry.resolved_at, Some(at));
    }

    #[test]
    fn recorded_activity_moves_the_published_last_activity() {
        let mut thread = thread_with(&[Uuid::now_v7()]);
        let later = thread.last_activity_at + Duration::seconds(30);
        thread.record_activity(later);
        assert_eq!(entry_of(&thread).last_activity_at, later);
    }

    #[test]
    fn the_key_is_the_published_language_key() {
        let thread = thread_with(&[Uuid::now_v7()]);
        assert_eq!(
            SummaryOffer::key(&thread).unwrap().as_str(),
            format!("threads.thread_summary.{}", thread.id)
        );
    }
}
