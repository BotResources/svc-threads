//! The summary of each thread, published so that other services can list
//! conversations without asking Threads: one `threads.thread_summary.{thread_id}`
//! entry of the `PUBLISHED_LANGUAGE` bucket per thread, rewritten when a field
//! it carries changes and retracted when the thread is deleted (on command, or
//! because its owner left the roster).
//!
//! State-based: a consumer bootstraps wholesale, then watches; a retracted key
//! deletes its local rows. The wire version is the manifest's,
//! `threads.thread_summary_manifest` = `{"prefix": "threads.thread_summary.",
//! "version": 1}`. The members are not here — they are the
//! `threads.members.{thread_id}` entry (`super::members`), which this offer
//! leaves untouched — and neither are the resources, the messages or the read
//! marks.
//!
//! `last_activity_at` is the date of the thread's latest message activity (the
//! creation date while there is none), the very value Threads orders a member's
//! list by. It moves when a message is stored — a complete message, or a
//! streamed one when its stream opens — and when the writer of a streamed
//! message closes it (complete, fail or cancel); never on a chunk of a stream,
//! so the entry is rewritten twice per streamed answer, not per chunk.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::status::ThreadStatus;

/// The key prefix of the entries.
pub const SUMMARY_PREFIX: &str = "threads.thread_summary.";
/// The manifest key: the prefix with its trailing separator replaced by
/// `_manifest` (a sibling of the data, never under it).
pub const SUMMARY_MANIFEST_KEY: &str = "threads.thread_summary_manifest";
/// The wire version the manifest declares.
pub const SUMMARY_VERSION: u16 = 1;

/// `threads.thread_summary.{thread_id}`.
pub fn summary_key(thread_id: Uuid) -> String {
    format!("{SUMMARY_PREFIX}{thread_id}")
}

/// The thread a summary key names, if it is one.
pub fn thread_id_from_summary_key(key: &str) -> Option<Uuid> {
    key.strip_prefix(SUMMARY_PREFIX)
        .and_then(|rest| Uuid::parse_str(rest).ok())
}

/// The value of one entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedThreadSummary {
    pub thread_id: Uuid,
    /// The title; absent for an untitled thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The thread a child thread was started from; absent for a thread that is
    /// no one's child. Set at the creation, never changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<Uuid>,
    /// The block of the parent the child thread was started from; present
    /// exactly when `parent_thread_id` is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_block_id: Option<Uuid>,
    pub status: ThreadStatus,
    /// When the thread was resolved, while it is; absent otherwise. A resolved
    /// thread is still `OPEN`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    /// The date of the latest message stored or streamed message closed by its
    /// writer, or the creation date while there is none.
    pub last_activity_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_summary_key_names_its_thread() {
        let thread = Uuid::now_v7();
        let key = summary_key(thread);
        assert_eq!(key, format!("threads.thread_summary.{thread}"));
        assert_eq!(thread_id_from_summary_key(&key), Some(thread));
        assert_eq!(thread_id_from_summary_key(SUMMARY_MANIFEST_KEY), None);
        assert_eq!(
            thread_id_from_summary_key("threads.thread_summary.nope"),
            None
        );
        assert_eq!(
            thread_id_from_summary_key(&crate::members::members_key(thread)),
            None
        );
    }

    #[test]
    fn the_manifest_is_the_prefixs_sibling() {
        assert_eq!(
            SUMMARY_MANIFEST_KEY,
            format!("{}_manifest", SUMMARY_PREFIX.trim_end_matches('.'))
        );
    }

    #[test]
    fn an_untitled_top_level_thread_carries_no_optional_field() {
        let thread = Uuid::now_v7();
        let at: DateTime<Utc> = "2026-11-05T10:00:00Z".parse().unwrap();
        let entry = PublishedThreadSummary {
            thread_id: thread,
            title: None,
            parent_thread_id: None,
            origin_block_id: None,
            status: ThreadStatus::Open,
            resolved_at: None,
            created_at: at,
            last_activity_at: at,
        };
        assert_eq!(
            serde_json::to_value(&entry).unwrap(),
            json!({
                "thread_id": thread,
                "status": "OPEN",
                "created_at": "2026-11-05T10:00:00Z",
                "last_activity_at": "2026-11-05T10:00:00Z",
            })
        );
    }

    #[test]
    fn a_child_thread_reads_back_whole() {
        let (thread, parent, block) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let raw = json!({
            "thread_id": thread,
            "title": "Q3 figures",
            "parent_thread_id": parent,
            "origin_block_id": block,
            "status": "COMPLETED",
            "resolved_at": "2026-11-05T11:00:00Z",
            "created_at": "2026-11-05T10:00:00Z",
            "last_activity_at": "2026-11-05T10:30:00Z",
        });
        let entry: PublishedThreadSummary = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(entry.parent_thread_id, Some(parent));
        assert_eq!(entry.origin_block_id, Some(block));
        assert_eq!(entry.title.as_deref(), Some("Q3 figures"));
        assert_eq!(entry.status, ThreadStatus::Completed);
        assert_eq!(serde_json::to_value(&entry).unwrap(), raw);
    }
}
