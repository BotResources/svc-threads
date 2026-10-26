//! A thread's context, as a runner reads it: its active branch as a
//! conversation of the `br-llm-messages/3` model, whole or lightened for the AI
//! the runner answers for, with the thread's members and resources. A user
//! message is the input, an AI message the turn, under the message's id.
//!
//! The conversation leaves out the active branch's deleted messages, its
//! cancelled ones, those failed before their first step and those still
//! streaming, and every Threads block. Every message carries its author: the
//! AI's id and name, or the user's id and current display name.

use std::collections::{BTreeMap, BTreeSet};

use contract_threads::llm::{Author, Conversation, Perspective, lighten};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::persistence::Persistence;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::{Cohort, Deps};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::access;
use crate::fault::encode;
use crate::principal::ThreadsPrincipal;
use crate::slices::message::content::{turn_of, user_input_of};
use crate::slices::message::draft::has_a_step;
use crate::slices::message::state::{self, MessageAuthor, MessageState};
use crate::slices::message::store::{self as message_store, MessageStore};
use crate::slices::thread::state::{Thread, ThreadState};
use crate::slices::thread::store::ThreadStore;

const RUNNER_DIM: &str = "runner";

/// Any thread, seen by a runner: a service account holding `threads:runner`.
pub struct RunnerAccess;

impl Visibility for RunnerAccess {
    type Row = ThreadState;
    type Principal = ThreadsPrincipal;

    const DEPS: Deps = Deps::EMPTY;

    fn cohorts(_row: &ThreadState) -> Cohorts {
        vec![Cohort::flag(RUNNER_DIM, true)]
    }

    fn memberships(principal: &ThreadsPrincipal) -> Cohorts {
        if access::runner_surface(principal).is_allowed() {
            vec![Cohort::flag(RUNNER_DIM, true)]
        } else {
            Vec::new()
        }
    }
}

/// The gate a runner reads a thread behind (`Query::behind`). Never subscribed
/// to: it populates nothing.
pub struct RunnerThreads;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerThread {
    pub id: Uuid,
}

impl Projector for RunnerThreads {
    type Principal = ThreadsPrincipal;
    type Noun = Thread;
    type Store = ThreadStore;
    type Query = ();
    type Out = RunnerThread;
    type Visibility = RunnerAccess;

    const NAME: ProjectorName = ProjectorName::from_static("runner_threads");

    async fn populate(
        _cx: &Populate<'_, ThreadsPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(Population::Keys(BTreeSet::new()))
    }

    fn project(
        thread: &ThreadState,
        _principal: &ThreadsPrincipal,
    ) -> Result<RunnerThread, EngineError> {
        Ok(RunnerThread { id: thread.id })
    }
}

/// Whether a message of the active branch is part of the conversation.
pub fn in_conversation(message: &MessageState) -> bool {
    state::in_conversation(message.status, message.deleted, has_a_step(&message.items))
}

fn author_of(message: &MessageState) -> Result<Author, EngineError> {
    let (id, name) = match &message.author {
        MessageAuthor::User {
            user_id,
            display_name,
            ..
        } => (
            user_id.to_string(),
            display_name
                .clone()
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| user_id.to_string()),
        ),
        MessageAuthor::Ai { id, name } => (id.clone(), name.clone()),
    };
    Author::new(id, name).map_err(|error| EngineError::Service(error.to_string().into()))
}

/// The conversation of a list of messages, in order. A message the library
/// cannot read back is a fault of the store: Threads only stores what it can.
pub fn conversation_of(messages: &[MessageState]) -> Result<Conversation, EngineError> {
    let mut conversation = Conversation::new();
    for message in messages.iter().filter(|message| in_conversation(message)) {
        let author = author_of(message)?;
        let corrupt = |error: contract_threads::llm::MessageError| {
            EngineError::Service(format!("message {} does not load: {error}", message.id).into())
        };
        match message.author {
            MessageAuthor::User { .. } => conversation
                .push_input(user_input_of(message.id, &message.blocks, author).map_err(corrupt)?)
                .map_err(corrupt)?,
            MessageAuthor::Ai { .. } => conversation
                .push_turn(turn_of(message.id, Some(author), &message.items).map_err(corrupt)?)
                .map_err(corrupt)?,
        }
    }
    Ok(conversation)
}

/// The conversation of a thread's active branch, lightened for `ai` when one
/// is named, as the library's JSON. Two reads: the branch, then its messages.
pub async fn read(
    conn: &mut PgConnection,
    thread: Uuid,
    ai: Option<Author>,
) -> Result<serde_json::Value, EngineError> {
    let branch = message_store::active_branch(conn, thread).await?;
    let mut loaded: BTreeMap<Uuid, MessageState> = MessageStore::read_many(conn, &branch)
        .await?
        .into_iter()
        .collect();
    let messages: Vec<MessageState> = branch.iter().filter_map(|id| loaded.remove(id)).collect();
    let whole = conversation_of(&messages)?;
    let conversation = match ai {
        Some(ai) => lighten(&whole, &Perspective::new(ai)),
        None => whole,
    };
    serde_json::to_value(&conversation).map_err(|source| encode("a conversation", source))
}

#[cfg(test)]
mod tests {
    use contract_threads::MessageStatus;

    use super::*;
    use crate::principal::tests::{active, person, service};
    use crate::slices::message::state::tests::message_in;
    use crate::slices::thread::state::tests::thread_with;

    #[test]
    fn a_runner_sees_any_thread_and_nobody_else_does() {
        let thread = thread_with(&[Uuid::now_v7()]);
        assert!(RunnerAccess::visible(
            &thread,
            &service(&[crate::scopes::RUNNER])
        ));
        assert!(!RunnerAccess::visible(&thread, &service(&[])));
        assert!(!RunnerAccess::visible(&thread, &person(true, active())));
    }

    #[test]
    fn deleted_cancelled_streaming_and_stepless_failures_are_left_out() {
        let mut message = message_in(&[Uuid::now_v7()]);
        assert!(in_conversation(&message));
        message.deleted = true;
        assert!(!in_conversation(&message));
        message.deleted = false;
        for status in [MessageStatus::Cancelled, MessageStatus::Streaming] {
            message.status = status;
            assert!(!in_conversation(&message), "{status}");
        }
        message.status = MessageStatus::Failed;
        assert!(!in_conversation(&message), "failed before its first step");
    }
}
