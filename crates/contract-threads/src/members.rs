//! The members of each thread, published so that other services decide
//! locally: one `threads.members.{thread_id}` entry of the `PUBLISHED_LANGUAGE`
//! bucket per thread, rewritten at every membership change and retracted when
//! the thread is deleted (on command, or because its owner left the roster).
//!
//! State-based: a consumer bootstraps wholesale, then watches; a retracted key
//! deletes its local rows. The wire version is not in the entry: it is the
//! manifest's, `threads.members_manifest` = `{"prefix": "threads.members.",
//! "version": 1}`. Titles, resources, messages and read marks are deliberately
//! absent.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The key prefix of the entries.
pub const MEMBERS_PREFIX: &str = "threads.members.";
/// The manifest key: the prefix with its trailing separator replaced by
/// `_manifest` (a sibling of the data, never under it).
pub const MEMBERS_MANIFEST_KEY: &str = "threads.members_manifest";
/// The wire version the manifest declares.
pub const MEMBERS_VERSION: u16 = 1;

/// `threads.members.{thread_id}`.
pub fn members_key(thread_id: Uuid) -> String {
    format!("{MEMBERS_PREFIX}{thread_id}")
}

/// The thread a members key names, if it is one.
pub fn thread_id_from_members_key(key: &str) -> Option<Uuid> {
    key.strip_prefix(MEMBERS_PREFIX)
        .and_then(|rest| Uuid::parse_str(rest).ok())
}

/// The value of one entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedThreadMembers {
    /// The owning user; absent for a thread without owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<Uuid>,
    /// Every member, the owner included, each once.
    pub users: Vec<Uuid>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_members_key_names_its_thread() {
        let thread = Uuid::now_v7();
        let key = members_key(thread);
        assert_eq!(key, format!("threads.members.{thread}"));
        assert_eq!(thread_id_from_members_key(&key), Some(thread));
        assert_eq!(thread_id_from_members_key(MEMBERS_MANIFEST_KEY), None);
        assert_eq!(thread_id_from_members_key("threads.members.nope"), None);
    }

    #[test]
    fn the_manifest_is_the_prefixs_sibling() {
        assert_eq!(
            MEMBERS_MANIFEST_KEY,
            format!("{}_manifest", MEMBERS_PREFIX.trim_end_matches('.'))
        );
    }

    #[test]
    fn an_ownerless_entry_carries_no_owner_field() {
        let user = Uuid::now_v7();
        let entry = PublishedThreadMembers {
            owner_id: None,
            users: vec![user],
        };
        assert_eq!(
            serde_json::to_value(&entry).unwrap(),
            json!({ "users": [user] })
        );
        let owned: PublishedThreadMembers =
            serde_json::from_value(json!({ "owner_id": user, "users": [user] })).unwrap();
        assert_eq!(owned.owner_id, Some(user));
    }
}
