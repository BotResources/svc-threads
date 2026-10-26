//! The members of each thread, published as `threads.members.{thread_id}` in
//! the `PUBLISHED_LANGUAGE` bucket so that other services decide locally.
//!
//! The engine's offer machinery owns the publication: a saved or deleted
//! thread stages its dirty key in the transaction of the change, the offer's
//! leader drains it (a put, or a retraction for a deleted thread), reconciles
//! the whole keyspace on its first drain after boot and periodically, and
//! publishes the manifest `threads.members_manifest`.

use contract_threads::PublishedThreadMembers;
use contract_threads::members::{MEMBERS_PREFIX, MEMBERS_VERSION, members_key};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::nats::KvKey;
use service_engine::offer::Offer;
use sqlx::PgConnection;

use super::state::ThreadState;
use super::store;

pub struct MembersOffer;

impl Offer for MembersOffer {
    type Row = ThreadState;
    type Published = PublishedThreadMembers;

    const NAME: &'static str = "thread_members";
    const PREFIX: &'static str = MEMBERS_PREFIX;
    const VERSION: u16 = MEMBERS_VERSION;

    fn key(thread: &ThreadState) -> Result<KvKey, EngineError> {
        KvKey::new(members_key(thread.id))
            .map_err(|error| EngineError::Config(format!("members offer key: {error}")))
    }

    fn publish(thread: &ThreadState) -> Option<PublishedThreadMembers> {
        Some(entry_of(thread))
    }

    fn all(conn: &mut PgConnection) -> BoxFuture<'_, Result<Vec<ThreadState>, EngineError>> {
        Box::pin(async move { store::all_threads(conn).await })
    }
}

/// The entry of one thread: its owner, if any, and every member, the owner
/// included, each once.
pub fn entry_of(thread: &ThreadState) -> PublishedThreadMembers {
    PublishedThreadMembers {
        owner_id: thread.owner_id,
        users: thread.member_ids().into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::slices::thread::state::tests::thread_with;

    #[test]
    fn the_entry_carries_the_owner_and_every_member_once() {
        let (owner, member) = (Uuid::now_v7(), Uuid::now_v7());
        let thread = thread_with(&[owner, member]);
        let entry = entry_of(&thread);
        assert_eq!(entry.owner_id, Some(owner));
        let mut expected = vec![owner, member];
        expected.sort();
        assert_eq!(entry.users, expected);
    }

    #[test]
    fn an_ownerless_thread_publishes_no_owner() {
        let mut thread = thread_with(&[Uuid::now_v7()]);
        thread.owner_id = None;
        assert_eq!(entry_of(&thread).owner_id, None);
    }

    #[test]
    fn the_key_is_the_published_language_key() {
        let thread = thread_with(&[Uuid::now_v7()]);
        assert_eq!(
            MembersOffer::key(&thread).unwrap().as_str(),
            format!("threads.members.{}", thread.id)
        );
    }
}
