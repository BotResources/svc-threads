//! A member sets their own read mark: the reader's own state, with no fact and
//! no thread version. Removing a member removes their mark (by cascade).
//!
//! The gesture asks the two decisions its affordances show: the thread's
//! `setReadMark` (the caller is a member) and the message's `markRead` (the
//! message is one of that thread's, seen by the caller).

use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::PersistenceExt;
use service_engine::pipeline::{Mutation, MutationInput};
use uuid::Uuid;

use super::ThreadCause;
use super::decision;
use super::state::ThreadState;
use super::view::ThreadReader;
use crate::access::{self, MESSAGE_NOT_FOUND};
use crate::fault::Fault;
use crate::principal::ThreadsPrincipal;
use crate::slices::message::store::MessageStore;

#[derive(Debug, Deserialize)]
pub struct SetReadMark {
    pub thread_id: Uuid,
    pub message_id: Uuid,
}

impl MutationInput for SetReadMark {
    type Output = ();
    type Error = Fault;
    const NAME: &'static str = "set_read_mark";
}

pub fn set_read_mark<'m>(
    cx: &'m mut Mutation<'m, ThreadsPrincipal>,
    input: SetReadMark,
) -> BoxFuture<'m, Result<(), Fault>> {
    Box::pin(async move {
        access::human_surface(cx.principal()).require()?;
        let mut thread = cx
            .load::<ThreadState>(&input.thread_id)
            .await?
            .ok_or(access::THREAD_NOT_FOUND)?;
        decision::set_read_mark(&thread, cx.principal()).require()?;
        let message = MessageStore::load(cx.connection(), &input.message_id)
            .await?
            .filter(|message| message.thread_id == thread.id)
            .ok_or(MESSAGE_NOT_FOUND)?;
        crate::slices::message::decision::mark_read(&message, cx.principal()).require()?;
        let user = cx
            .principal()
            .active_user()
            .ok_or(access::ACTIVE_USER_REQUIRED)?;
        if thread.read_marks.get(&user) == Some(&message.id) {
            return Ok(());
        }
        thread.set_read_mark(user, message.id);
        cx.save(&thread).await?;
        cx.impact_caused::<ThreadReader, _>(&thread.id, ThreadCause::ReadMarkSet)?;
        Ok(())
    })
}
