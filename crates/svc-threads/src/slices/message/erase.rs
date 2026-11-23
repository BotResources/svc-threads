//! `integration.cmd.threads.message.erase.v1`: the end of a thread's list
//! erased. Every message stored after the named one (by `created_at`, then
//! id), and the named one too when `including`, goes whoever wrote it and
//! whatever its status: its entries, blocks, annotations, artifacts (bytes
//! included), stream and the read marks set on it. Nothing of it can be read
//! again. Child threads started from an erased message survive; their origin
//! then leads nowhere.
//!
//! One transaction under the thread's row lock, the lock every message writer
//! takes first (store, close, edit, delete, the stream watch). A close, a chunk
//! fold or a watch tick that reaches an erased message afterwards finds no
//! message: the closers answer `MESSAGE_NOT_FOUND`, and the stream of an
//! erased `STREAMING` message is sealed in the same transaction, so a late
//! chunk is refused on every pod and never folded or shown.
//!
//! The decision is the thread's ([`ThreadState::erase_messages`]): it
//! refuses, in order, `THREAD_NOT_OPEN`, `THREAD_RESOLVED` and
//! `MESSAGE_NOT_FOUND` (after `THREAD_NOT_FOUND`, which the load decides). An
//! erasure that finds nothing to remove is confirmed and changes nothing.
//! Otherwise it records one thread fact, `MessagesErased` (the named message,
//! whether it went too, how many messages went, never content), and raises the
//! thread's version. It does not move `last_activity_at`. Erased messages
//! record no message fact: the thread's fact covers them. Their artifacts each
//! record `Removed` (reason `MESSAGE_ERASED`), as with a deleted message.

use contract_threads::{EraseMessages, MessageStatus, ReasonCode};
use futures_util::future::BoxFuture;
use serde::Serialize;
use service_engine::error::EngineError;
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::commands::{Verdict, refuse_on};
use super::state::Message;
use super::store;
use super::{MessageCause, closing, message_cause};
use crate::command::{Call, Inbound, MessageOutcome, Refusal, answer_message};
use crate::facts::{Fact, FactMeta};
use crate::fault::Fault;
use crate::slices::artifact;
use crate::slices::thread::ThreadCause;
use crate::slices::thread::state::{Thread, ThreadChange, ThreadState};
use crate::slices::thread::transitions::Applied;

/// The fact type of an erasure, noun `thread`.
pub const MESSAGES_ERASED: &str = "MessagesErased";

#[derive(Serialize)]
struct MessagesErasedFact {
    message_id: Uuid,
    including: bool,
    count: usize,
}

/// What an erasure found: the messages it removes, in list order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Erasure {
    pub message_id: Uuid,
    pub including: bool,
    pub erased: Vec<Uuid>,
}

impl ThreadState {
    /// `message.erase`: judged on an `OPEN` thread that is not resolved, then
    /// on the named message (`found` is `None` when the thread holds no such
    /// message). Removes the read marks set on the erased messages, raises the
    /// version and records `MessagesErased`; an erasure of nothing changes
    /// nothing.
    pub fn erase_messages(
        &mut self,
        message_id: Uuid,
        found: Option<&Erasure>,
        meta: &FactMeta,
    ) -> Result<Result<Applied, Refusal>, EngineError> {
        if let Some(refusal) = self.open_refusal() {
            return Ok(Err(refusal));
        }
        if let Some(refusal) = self.resolved_refusal() {
            return Ok(Err(refusal));
        }
        let Some(erasure) = found else {
            return Ok(Err(
                Refusal::new(ReasonCode::MessageNotFound).with("message_id", message_id)
            ));
        };
        if erasure.erased.is_empty() {
            return Ok(Ok(Applied::Unchanged));
        }
        self.read_marks
            .retain(|_, read| !erasure.erased.contains(read));
        self.version += 1;
        let fact = Fact::new(
            MESSAGES_ERASED,
            &MessagesErasedFact {
                message_id: erasure.message_id,
                including: erasure.including,
                count: erasure.erased.len(),
            },
            meta,
        )?;
        self.pending.push(ThreadChange::Recorded(fact));
        Ok(Ok(Applied::Changed))
    }
}

pub fn erase<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<EraseMessages>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let verdict = erase_messages(cx, &call, &command).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

async fn erase_messages(
    cx: &mut Reaction<'_>,
    call: &Call,
    command: &EraseMessages,
) -> Result<Verdict, Fault> {
    let Some(mut thread) = cx.load::<ThreadState>(&command.thread_id).await? else {
        return Ok(Err(Refusal::new(ReasonCode::ThreadNotFound)));
    };
    // Read under the thread's lock, before anything is written: a refusal
    // commits its answer alone.
    let tail = store::tail_from(cx.connection(), thread.id, command.message_id).await?;
    let found = tail.as_ref().map(|tail| Erasure {
        message_id: command.message_id,
        including: command.including,
        erased: ids(&tail.erased(command.including)),
    });
    let applied =
        refuse_on!(thread.erase_messages(command.message_id, found.as_ref(), &call.meta)?);
    let tail = tail.expect("an erasure the thread accepted found its named message");
    let outcome = MessageOutcome {
        status: tail.named_status,
        deleted: tail.named_deleted,
    };
    if applied == Applied::Unchanged {
        return Ok(Ok(outcome));
    }
    let erased = tail.erased(command.including);
    let gone = ids(&erased);
    let streaming: Vec<Uuid> = erased
        .iter()
        .filter(|(_, status)| *status == MessageStatus::Streaming)
        .map(|(id, _)| *id)
        .collect();
    artifact::delete_all_of_erased(cx, thread.id, &gone, &call.meta).await?;
    closing::seal_streams(cx, &streaming).await?;
    store::erase(cx.connection(), &gone).await?;
    cx.save(&thread).await?;
    for message in &gone {
        cx.impact_caused::<Message, _>(message, message_cause(MessageCause::Erased, thread.id))?;
    }
    cx.impact_caused::<Thread, _>(&thread.id, ThreadCause::MessagesErased)?;
    Ok(Ok(outcome))
}

fn ids(messages: &[(Uuid, MessageStatus)]) -> Vec<Uuid> {
    messages.iter().map(|(id, _)| *id).collect()
}

#[cfg(test)]
mod tests {
    use br_core_integration::{Actor, ServiceAccountId};
    use chrono::Utc;
    use contract_threads::ThreadStatus;

    use super::*;
    use crate::slices::message::store::Tail;
    use crate::slices::thread::state::Resolution;
    use crate::slices::thread::state::tests::thread_with;

    fn open_thread() -> ThreadState {
        thread_with(&[Uuid::now_v7()])
    }

    fn meta() -> FactMeta {
        FactMeta {
            actor: Actor::Service(ServiceAccountId::from(Uuid::now_v7())),
            acting_user_id: None,
            impersonator_id: None,
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            occurred_at: Utc::now(),
        }
    }

    fn erasure(named: Uuid, erased: &[Uuid]) -> Erasure {
        Erasure {
            message_id: named,
            including: erased.first() == Some(&named),
            erased: erased.to_vec(),
        }
    }

    #[test]
    fn the_refusals_come_in_their_order_and_change_nothing() {
        let named = Uuid::now_v7();
        let found = erasure(named, &[named]);
        let mut closed = open_thread();
        closed.status = ThreadStatus::Completed;
        closed.resolution = Some(Resolution {
            by: Uuid::now_v7(),
            at: Utc::now(),
        });
        let before = closed.clone();
        let refused = closed
            .erase_messages(named, Some(&found), &meta())
            .unwrap()
            .unwrap_err();
        assert_eq!(refused.code, ReasonCode::ThreadNotOpen);
        assert_eq!(closed, before);

        let mut resolved = open_thread();
        resolved.resolution = Some(Resolution {
            by: Uuid::now_v7(),
            at: Utc::now(),
        });
        let refused = resolved
            .erase_messages(named, None, &meta())
            .unwrap()
            .unwrap_err();
        assert_eq!(
            refused.code,
            ReasonCode::ThreadResolved,
            "before the message"
        );

        let mut open = open_thread();
        let before = open.clone();
        let refused = open
            .erase_messages(named, None, &meta())
            .unwrap()
            .unwrap_err();
        assert_eq!(refused.code, ReasonCode::MessageNotFound);
        assert_eq!(refused.params["message_id"], serde_json::json!(named));
        assert_eq!(open, before);
    }

    #[test]
    fn an_erasure_of_nothing_is_confirmed_and_changes_nothing() {
        let named = Uuid::now_v7();
        let mut thread = open_thread();
        let before = thread.clone();
        let applied = thread
            .erase_messages(named, Some(&erasure(named, &[])), &meta())
            .unwrap()
            .unwrap();
        assert_eq!(applied, Applied::Unchanged);
        assert_eq!(thread, before, "no version, no fact");
    }

    #[test]
    fn an_erasure_records_one_fact_without_content_raises_the_version_and_drops_the_marks_on_it() {
        let (named, after) = (Uuid::now_v7(), Uuid::now_v7());
        let (reader, other) = (Uuid::now_v7(), Uuid::now_v7());
        let mut thread = open_thread();
        thread.read_marks.insert(reader, after);
        thread.read_marks.insert(other, named);
        let activity = thread.last_activity_at;
        let version = thread.version;
        let applied = thread
            .erase_messages(
                named,
                Some(&Erasure {
                    message_id: named,
                    including: false,
                    erased: vec![after],
                }),
                &meta(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(applied, Applied::Changed);
        assert_eq!(thread.version, version + 1);
        assert_eq!(
            thread.last_activity_at, activity,
            "an erasure is no activity"
        );
        assert_eq!(thread.read_marks.get(&reader), None);
        assert_eq!(
            thread.read_marks.get(&other),
            Some(&named),
            "the named one stays"
        );
        let facts = thread.pending_facts();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].event_type, MESSAGES_ERASED);
        assert_eq!(
            facts[0].payload,
            serde_json::json!({ "message_id": named, "including": false, "count": 1 })
        );
    }

    #[test]
    fn the_tail_erases_after_the_named_message_and_the_named_one_when_asked() {
        let (named, a, b) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let tail = Tail {
            named_status: MessageStatus::Completed,
            named_deleted: false,
            from_named: vec![
                (named, MessageStatus::Completed),
                (a, MessageStatus::Failed),
                (b, MessageStatus::Streaming),
            ],
        };
        assert_eq!(ids(&tail.erased(false)), [a, b]);
        assert_eq!(ids(&tail.erased(true)), [named, a, b]);
        let last = Tail {
            from_named: vec![(named, MessageStatus::Completed)],
            ..tail
        };
        assert!(
            last.erased(false).is_empty(),
            "nothing after the last message"
        );
    }
}
