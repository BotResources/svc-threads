//! The resources of each thread, published so that the service owning a key
//! knows locally which threads reference its objects: one
//! `threads.resources.{thread_id}` entry of the `PUBLISHED_LANGUAGE` bucket per
//! thread, rewritten when its resources change and retracted when the thread is
//! deleted (on command, or because its owner left the roster).
//!
//! State-based: a consumer bootstraps wholesale, then watches, reads only the
//! key it owns, and deletes its local rows on a retracted key. The wire version
//! is not in the entry: it is the manifest's, `threads.resources_manifest` =
//! `{"prefix": "threads.resources.", "version": 1}`. Members, titles, messages
//! and read marks are deliberately absent.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The key prefix of the entries.
pub const RESOURCES_PREFIX: &str = "threads.resources.";
/// The manifest key: the prefix with its trailing separator replaced by
/// `_manifest` (a sibling of the data, never under it).
pub const RESOURCES_MANIFEST_KEY: &str = "threads.resources_manifest";
/// The wire version the manifest declares.
pub const RESOURCES_VERSION: u16 = 1;

/// `threads.resources.{thread_id}`.
pub fn resources_key(thread_id: Uuid) -> String {
    format!("{RESOURCES_PREFIX}{thread_id}")
}

/// The thread a resources key names, if it is one.
pub fn thread_id_from_resources_key(key: &str) -> Option<Uuid> {
    key.strip_prefix(RESOURCES_PREFIX)
        .and_then(|rest| Uuid::parse_str(rest).ok())
}

/// The value of one entry: every key of the thread with its ids, the keys in
/// a stable (sorted) order; an empty map for a thread without resources. The
/// ids are opaque, as the commanding services gave them.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PublishedThreadResources {
    pub resources: BTreeMap<String, Vec<String>>,
}

impl PublishedThreadResources {
    /// The ids under `key`, empty when the thread holds no such key.
    pub fn ids_of(&self, key: &str) -> &[String] {
        self.resources.get(key).map(Vec::as_slice).unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_resources_key_names_its_thread() {
        let thread = Uuid::now_v7();
        let key = resources_key(thread);
        assert_eq!(key, format!("threads.resources.{thread}"));
        assert_eq!(thread_id_from_resources_key(&key), Some(thread));
        assert_eq!(thread_id_from_resources_key(RESOURCES_MANIFEST_KEY), None);
        assert_eq!(thread_id_from_resources_key("threads.resources.nope"), None);
        assert_eq!(
            thread_id_from_resources_key(&format!("threads.members.{thread}")),
            None
        );
    }

    #[test]
    fn the_manifest_is_the_prefixs_sibling() {
        assert_eq!(
            RESOURCES_MANIFEST_KEY,
            format!("{}_manifest", RESOURCES_PREFIX.trim_end_matches('.'))
        );
    }

    #[test]
    fn a_thread_without_resources_publishes_an_empty_object() {
        let entry = PublishedThreadResources::default();
        assert_eq!(
            serde_json::to_value(&entry).unwrap(),
            json!({ "resources": {} })
        );
        assert!(entry.ids_of("kms").is_empty());
    }

    #[test]
    fn the_keys_travel_sorted_with_their_ids() {
        let mut entry = PublishedThreadResources::default();
        entry
            .resources
            .insert("kms".into(), vec!["k1".into(), "k2".into()]);
        entry
            .resources
            .insert("documents".into(), vec!["d1".into()]);
        assert_eq!(
            serde_json::to_string(&entry).unwrap(),
            r#"{"resources":{"documents":["d1"],"kms":["k1","k2"]}}"#
        );
        let back: PublishedThreadResources =
            serde_json::from_value(json!({ "resources": { "kms": ["k1", "k2"] } })).unwrap();
        assert_eq!(back.ids_of("kms"), ["k1", "k2"]);
        assert!(back.ids_of("documents").is_empty());
    }
}
