//! The resources of each thread, published as `threads.resources.{thread_id}`
//! in the `PUBLISHED_LANGUAGE` bucket so that the service owning a key knows
//! locally which threads reference its objects: every key of the thread with
//! its ids, an empty object for a thread without resources.
//!
//! The engine's offer machinery owns the publication, as for the members
//! (`super::offer`) and the summary (`super::summary_offer`): every save or
//! deletion of a thread stages its dirty key in the transaction of the change,
//! and the offer's leader drains it (a put, or a retraction for a deleted
//! thread — on command or because its owner left the roster), reconciles the
//! whole keyspace on its first drain after boot and periodically (so the
//! threads that existed before this offer are published by the first pod that
//! leads it), and publishes the manifest `threads.resources_manifest`.
//!
//! What moves the entry: the creation, and `add_resource_ids` /
//! `remove_resource_ids` when they change a key. Every other save of the
//! thread (title, status, members, read marks, activity) is staged too, as is
//! a no-op resources command's, but the relay finds the entry already holding
//! that value and writes nothing; staged keys coalesce per thread until
//! drained.

use contract_threads::PublishedThreadResources;
use contract_threads::resources::{RESOURCES_PREFIX, RESOURCES_VERSION, resources_key};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::nats::KvKey;
use service_engine::offer::Offer;
use sqlx::PgConnection;

use super::state::ThreadState;
use super::store;

pub struct ResourcesOffer;

impl Offer for ResourcesOffer {
    type Row = ThreadState;
    type Published = PublishedThreadResources;

    const NAME: &'static str = "thread_resources";
    const PREFIX: &'static str = RESOURCES_PREFIX;
    const VERSION: u16 = RESOURCES_VERSION;

    fn key(thread: &ThreadState) -> Result<KvKey, EngineError> {
        KvKey::new(resources_key(thread.id))
            .map_err(|error| EngineError::Config(format!("resources offer key: {error}")))
    }

    fn publish(thread: &ThreadState) -> Option<PublishedThreadResources> {
        Some(entry_of(thread))
    }

    fn all(conn: &mut PgConnection) -> BoxFuture<'_, Result<Vec<ThreadState>, EngineError>> {
        Box::pin(async move { store::all_threads(conn).await })
    }
}

/// The entry of one thread: its keys, sorted, each with its ids in the order
/// they were added. A key is never empty in the thread, so none is published
/// empty.
pub fn entry_of(thread: &ThreadState) -> PublishedThreadResources {
    PublishedThreadResources {
        resources: thread.resources.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::slices::thread::state::tests::thread_with;

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|id| id.to_string()).collect()
    }

    #[test]
    fn a_thread_without_resources_publishes_an_empty_object() {
        let thread = thread_with(&[Uuid::now_v7()]);
        assert_eq!(
            serde_json::to_value(entry_of(&thread)).unwrap(),
            json!({ "resources": {} })
        );
    }

    #[test]
    fn the_entry_carries_every_key_with_its_ids_in_their_order() {
        let mut thread = thread_with(&[Uuid::now_v7()]);
        thread.resources.insert("kms".into(), ids(&["k2", "k1"]));
        thread.resources.insert("documents".into(), ids(&["d1"]));
        let entry = entry_of(&thread);
        assert_eq!(entry.ids_of("kms"), ["k2", "k1"]);
        assert_eq!(entry.ids_of("documents"), ["d1"]);
        assert_eq!(
            serde_json::to_string(&entry).unwrap(),
            r#"{"resources":{"documents":["d1"],"kms":["k2","k1"]}}"#
        );
    }

    #[test]
    fn a_change_outside_the_resources_leaves_the_entry_as_it_was() {
        let mut thread = thread_with(&[Uuid::now_v7()]);
        thread.resources.insert("kms".into(), ids(&["k1"]));
        let before = entry_of(&thread);
        thread.title = Some(contract_threads::ThreadTitle::parse("Q3").unwrap());
        thread.members.clear();
        thread.record_activity(thread.last_activity_at + chrono::Duration::seconds(1));
        assert_eq!(entry_of(&thread), before);
    }

    #[test]
    fn the_key_is_the_published_language_key() {
        let thread = thread_with(&[Uuid::now_v7()]);
        assert_eq!(
            ResourcesOffer::key(&thread).unwrap().as_str(),
            format!("threads.resources.{}", thread.id)
        );
    }
}
