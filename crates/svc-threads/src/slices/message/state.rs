//! A message's canonical state, as its store reads it back: the turn row (its
//! author — with the roster copy's name for a user author — its status and its
//! turn kind), its content as the library's records with its annotations, and
//! the members of its thread, which decide who sees it — plus the changes a
//! transition made and not yet saved ([`MessageChange`]).
//!
//! While a message streams, its content is the accumulated lane's
//! ([`super::draft`]): the rows hold none until the stream closes. The guarded
//! transitions live in [`super::transitions`].

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use contract_threads::MessageStatus;
use contract_threads::llm::{AgentId, Author, ObservedAt, TurnId, TurnKind, TurnRecord};
use service_engine::error::EngineError;
use service_engine::name::NounName;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use service_engine::{Cohort, Deps};
use uuid::Uuid;

use super::content::Content;
use crate::facts::Fact;
use crate::fault::corrupt;
use crate::principal::{MEMBERSHIP_DEPS, ROSTER_DEPS, ThreadsPrincipal};
use crate::slices::thread::state::{MEMBER_DIM, UnknownStoredValue};

/// The message noun: what a message's impacts name.
pub struct Message;

impl Noun for Message {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("message");
}

/// A message is seen by the members of its thread only, while they are active
/// users of the roster copy.
impl Visibility for Message {
    type Row = MessageState;
    type Principal = ThreadsPrincipal;

    const DEPS: Deps = ROSTER_DEPS.union(MEMBERSHIP_DEPS);

    fn cohorts(row: &MessageState) -> Cohorts {
        row.members
            .iter()
            .map(|user| Cohort::uuid(MEMBER_DIM, *user))
            .collect()
    }

    fn memberships(principal: &ThreadsPrincipal) -> Cohorts {
        principal
            .active_user()
            .map(|user| Cohort::uuid(MEMBER_DIM, user))
            .into_iter()
            .collect()
    }
}

/// Who wrote a message: exactly one of a user and an AI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageAuthor {
    /// A user, the human of the message's human message, shown under the
    /// roster copy's current name (never copied).
    User {
        user_id: Uuid,
        display_name: Option<String>,
        in_roster: bool,
    },
    /// An AI: the library's agent id and its name, as the storing service gave
    /// them. Threads does not read the id.
    Agent { agent: AgentId, name: Author },
}

/// The library's literal of a turn kind, read back by a total mapping.
pub fn turn_kind_of_db(stored: &str) -> Result<TurnKind, UnknownStoredValue> {
    match stored {
        "input_turn" => Ok(TurnKind::InputTurn),
        "ai_turn" => Ok(TurnKind::AITurn),
        other => Err(UnknownStoredValue {
            column: "message.turn_kind",
            value: other.to_owned(),
        }),
    }
}

/// What a transition changed and the store has yet to write beside the message
/// row.
#[derive(Debug, Clone, PartialEq)]
pub enum MessageChange {
    /// The message's content is written whole, its identities with it: at its
    /// creation when it arrives whole, at its close when it streamed, at an
    /// edit (replacing what was there).
    ContentWritten,
    /// Its author deleted it: its content goes, the node stays.
    ContentRemoved,
    Recorded(Fact),
}

/// One message, whole.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageState {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub parent_message_id: Option<Uuid>,
    pub author: MessageAuthor,
    pub job_id: Option<Uuid>,
    pub status: MessageStatus,
    pub deleted: bool,
    pub created_at: DateTime<Utc>,
    /// The date of its author's latest edit; `None` until the first.
    pub edited_at: Option<DateTime<Utc>>,
    pub version: i64,
    /// The library's kind of its turn: an input turn for a user message, an AI
    /// turn or an input turn for an AI message.
    pub turn_kind: TurnKind,
    /// The turn's own observed time, as the library writes it.
    pub observed_at: Option<ObservedAt>,
    /// Its content: empty while it streams, and once deleted.
    pub content: Content,
    /// The members of the message's thread.
    pub members: BTreeSet<Uuid>,
    /// Unsaved changes, in order.
    pub pending: Vec<MessageChange>,
}

impl MessageState {
    pub fn pending_facts(&self) -> Vec<Fact> {
        self.pending
            .iter()
            .filter_map(|change| match change {
                MessageChange::Recorded(fact) => Some(fact.clone()),
                _ => None,
            })
            .collect()
    }

    /// The user who wrote it, when a user did.
    pub fn author_user(&self) -> Option<Uuid> {
        match &self.author {
            MessageAuthor::User { user_id, .. } => Some(*user_id),
            MessageAuthor::Agent { .. } => None,
        }
    }

    /// The agent that wrote it, when an AI did.
    pub fn author_agent(&self) -> Option<AgentId> {
        match &self.author {
            MessageAuthor::Agent { agent, .. } => Some(*agent),
            MessageAuthor::User { .. } => None,
        }
    }

    /// Its turn's id: the message's own. Every message id is a UUIDv7, refused
    /// otherwise when it is stored.
    pub fn turn_id(&self) -> Result<TurnId, EngineError> {
        TurnId::from_uuid(self.id).map_err(corrupt)
    }

    /// Its turn's row, as the library's record: an AI turn names its agent and
    /// author, an input turn neither.
    pub fn turn_record(&self) -> Result<TurnRecord, EngineError> {
        let mut record = TurnRecord::new(self.turn_id()?, self.turn_kind);
        if self.turn_kind == TurnKind::AITurn {
            let MessageAuthor::Agent { agent, name } = &self.author else {
                return Err(corrupt(UnknownStoredValue {
                    column: "message.author",
                    value: "an AI turn without its agent".to_owned(),
                }));
            };
            record = record.with_agent(*agent).with_author(name.clone());
        }
        if let Some(observed_at) = &self.observed_at {
            record = record.with_observed_at(observed_at.clone());
        }
        Ok(record)
    }

    /// Whether it is part of the conversation a runner receives: not deleted,
    /// not cancelled, and not failed before its first entry. A message still
    /// streaming is part of it with what its stream holds.
    pub fn in_conversation(&self) -> bool {
        !self.deleted
            && match self.status {
                MessageStatus::Completed | MessageStatus::Streaming => true,
                MessageStatus::Failed => self.content.has_an_entry(),
                MessageStatus::Cancelled => false,
            }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn agent() -> MessageAuthor {
        MessageAuthor::Agent {
            agent: AgentId::from_uuid(Uuid::now_v7()).unwrap(),
            name: Author::new("Analyst").unwrap(),
        }
    }

    pub fn message_in(members: &[Uuid]) -> MessageState {
        MessageState {
            id: Uuid::now_v7(),
            thread_id: Uuid::now_v7(),
            parent_message_id: None,
            author: agent(),
            job_id: None,
            status: MessageStatus::Completed,
            deleted: false,
            created_at: Utc::now(),
            edited_at: None,
            version: 1,
            turn_kind: TurnKind::AITurn,
            observed_at: None,
            content: Content::default(),
            members: members.iter().copied().collect(),
            pending: Vec::new(),
        }
    }

    #[test]
    fn a_turn_kind_maps_to_and_from_its_column() {
        for kind in TurnKind::ALL {
            assert_eq!(turn_kind_of_db(kind.as_str()), Ok(*kind));
        }
        assert!(turn_kind_of_db("AI_TURN").is_err());
    }

    #[test]
    fn a_message_is_visible_to_the_active_members_of_its_thread() {
        use crate::principal::RosterStanding;
        use crate::principal::tests::{active, person};

        let member = person(false, active());
        let message = message_in(&[member.user().unwrap()]);
        assert!(<Message as Visibility>::visible(&message, &member));
        assert!(!<Message as Visibility>::visible(
            &message,
            &person(true, active())
        ));
        let gone = person(false, RosterStanding::Deactivated);
        assert!(!<Message as Visibility>::visible(
            &message_in(&[gone.user().unwrap()]),
            &gone
        ));
    }

    #[test]
    fn deleted_cancelled_and_failed_before_their_first_entry_are_left_out() {
        let mut message = message_in(&[Uuid::now_v7()]);
        assert!(message.in_conversation());
        message.deleted = true;
        assert!(!message.in_conversation());
        message.deleted = false;
        message.status = MessageStatus::Cancelled;
        assert!(!message.in_conversation());
        message.status = MessageStatus::Failed;
        assert!(!message.in_conversation(), "failed before its first entry");
        message.status = MessageStatus::Streaming;
        assert!(message.in_conversation(), "what its stream holds");
    }

    #[test]
    fn an_ai_turn_row_names_its_agent_and_author() {
        let message = message_in(&[]);
        let record = message.turn_record().unwrap();
        assert_eq!(record.kind(), TurnKind::AITurn);
        assert_eq!(record.agent(), message.author_agent());
        record.check_local().unwrap();
    }
}
