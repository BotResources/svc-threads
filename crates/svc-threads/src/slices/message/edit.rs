//! `integration.cmd.threads.message.edit.v1`: a `COMPLETED` user message that
//! is not deleted, rewritten in place by its author. Its blocks are replaced
//! whole; it keeps its id, its place in the tree and every descendant, even
//! those written on its previous content. Threads keeps no history of the
//! previous content: the `Edited` fact records the new blocks' kinds and
//! positions, never their text.
//!
//! The refusals come in one order: the thread (`THREAD_NOT_FOUND`,
//! `THREAD_NOT_OPEN`, `THREAD_RESOLVED`), the message (`MESSAGE_NOT_FOUND`,
//! `MESSAGE_NOT_EDITABLE`), its author (`NOT_THE_AUTHOR`), the version the
//! edit was made from (`VERSION_CONFLICT`), then the new content
//! (`ID_NOT_UUIDV7`, `KEY_REUSED`, `MESSAGE_INVALID`) — so that an edit from a
//! version the message has left never reaches its content.

use contract_threads::{EditMessage, MessageStatus, ReasonCode};
use futures_util::future::BoxFuture;
use serde::Serialize;
use service_engine::error::EngineError;
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::commands::{Verdict, message_of, refuse_on, writable_thread};
use super::state::{Message, MessageChange, MessageState, StoredBlock};
use super::transitions::{BlockShape, EDITED, shapes};
use super::{MessageCause, content, message_cause, store};
use crate::command::{Call, Inbound, MessageOutcome, Refusal, answer_message};
use crate::facts::{Fact, FactMeta};
use crate::fault::Fault;

#[derive(Serialize)]
struct EditedFact {
    blocks: Vec<BlockShape>,
}

impl MessageState {
    /// `MESSAGE_NOT_EDITABLE` unless the message is a user message,
    /// `COMPLETED` and not deleted.
    pub fn not_editable(&self) -> Option<Refusal> {
        let editable = self.author_user().is_some()
            && self.status == MessageStatus::Completed
            && !self.deleted;
        (!editable).then(|| {
            Refusal::new(ReasonCode::MessageNotEditable)
                .with("status", self.status.as_str())
                .with("deleted", self.deleted)
        })
    }

    /// Who may edit it, and from which version: `NOT_THE_AUTHOR` unless
    /// `acting_user` wrote it, then `VERSION_CONFLICT`, naming the current
    /// version, unless the edit was made from it.
    pub fn edit_refusal(&self, acting_user: Uuid, expected_version: u64) -> Option<Refusal> {
        if let Some(refusal) = self.not_editable() {
            return Some(refusal);
        }
        if self.author_user() != Some(acting_user) {
            return Some(
                Refusal::new(ReasonCode::NotTheAuthor).with("acting_user_id", acting_user),
            );
        }
        if i64::try_from(expected_version).ok() != Some(self.version) {
            return Some(
                Refusal::new(ReasonCode::VersionConflict)
                    .with("expected_version", expected_version)
                    .with("current_version", self.version),
            );
        }
        None
    }

    /// The edit itself, once judged: the blocks replaced whole, `edited_at`
    /// set, the version raised by one, `Edited` recorded.
    pub fn edit(&mut self, blocks: Vec<StoredBlock>, meta: &FactMeta) -> Result<(), EngineError> {
        let fact = Fact::new(
            EDITED,
            &EditedFact {
                blocks: shapes(&blocks),
            },
            meta,
        )?;
        self.blocks = blocks;
        self.edited_at = Some(meta.occurred_at);
        self.version += 1;
        self.pending.push(MessageChange::ContentWritten);
        self.pending.push(MessageChange::Recorded(fact));
        Ok(())
    }
}

pub fn edit<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<EditMessage>,
) -> BoxFuture<'r, Result<(), Fault>> {
    Box::pin(async move {
        let call = Call::of(cx, &command)?;
        let verdict = edit_message(cx, &call, &command).await?;
        answer_message(cx, &call, command.thread_id, command.message_id, verdict)
    })
}

async fn edit_message(
    cx: &mut Reaction<'_>,
    call: &Call,
    command: &EditMessage,
) -> Result<Verdict, Fault> {
    refuse_on!(writable_thread(cx, command.thread_id).await?);
    let mut state = refuse_on!(message_of(cx, command.thread_id, command.message_id).await?);
    if let Some(refusal) = state.edit_refusal(command.acting_user_id, command.expected_version) {
        return Ok(Err(refusal));
    }
    let blocks = refuse_on!(content::user_blocks(state.id, &command.blocks));
    // The message's own block ids may be kept; any other held id is reused.
    let ids: Vec<Uuid> = blocks.iter().map(|block| block.id).collect();
    let held = store::held_block_ids(cx.connection(), &ids, Some(state.id)).await?;
    if !held.is_empty() {
        return Ok(Err(
            Refusal::new(ReasonCode::KeyReused).with("block_ids", held)
        ));
    }
    state.edit(blocks, &call.meta)?;
    cx.save(&state).await?;
    cx.impact_caused::<Message, _>(
        &state.id,
        message_cause(MessageCause::Edited, state.thread_id),
    )?;
    Ok(Ok(MessageOutcome {
        status: state.status,
        deleted: state.deleted,
    }))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::slices::message::state::MessageAuthor;
    use crate::slices::message::state::tests::message_in;

    fn meta() -> FactMeta {
        FactMeta {
            actor: br_core_integration::Actor::Service(Uuid::now_v7().into()),
            acting_user_id: None,
            impersonator_id: None,
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            occurred_at: Utc::now(),
        }
    }

    fn by_user(user: Uuid) -> MessageState {
        let mut message = message_in(&[user]);
        message.author = MessageAuthor::User {
            user_id: user,
            display_name: Some("Ada".into()),
            in_roster: true,
        };
        message
    }

    fn text(words: &str) -> StoredBlock {
        StoredBlock {
            id: Uuid::now_v7(),
            index: 0,
            kind: contract_threads::BlockKind::Text,
            content: serde_json::json!({ "type": "text", "text": words }),
        }
    }

    fn code(refusal: Option<Refusal>) -> Option<ReasonCode> {
        refusal.map(|refusal| refusal.code)
    }

    #[test]
    fn only_a_completed_user_message_not_deleted_is_editable() {
        let author = Uuid::now_v7();
        assert_eq!(code(by_user(author).not_editable()), None);
        assert_eq!(
            code(message_in(&[author]).not_editable()),
            Some(ReasonCode::MessageNotEditable),
            "an AI message"
        );
        // A stored user message is always COMPLETED
        // (`message_user_input_is_complete`), so the status check is defensive:
        // pinned here on an in-memory user message in each other status.
        for status in [
            MessageStatus::Streaming,
            MessageStatus::Failed,
            MessageStatus::Cancelled,
        ] {
            let mut user = by_user(author);
            user.status = status;
            assert_eq!(
                code(user.not_editable()),
                Some(ReasonCode::MessageNotEditable),
                "{status:?}"
            );
        }
        let mut deleted = by_user(author);
        deleted.deleted = true;
        assert_eq!(
            code(deleted.edit_refusal(author, 1)),
            Some(ReasonCode::MessageNotEditable),
            "editability is judged before the author and the version"
        );
    }

    #[test]
    fn the_author_edits_from_the_current_version_only() {
        let author = Uuid::now_v7();
        let message = by_user(author);
        assert_eq!(
            code(message.edit_refusal(Uuid::now_v7(), 7)),
            Some(ReasonCode::NotTheAuthor),
            "the author is judged before the version"
        );
        let stale = message.edit_refusal(author, 7).unwrap();
        assert_eq!(stale.code, ReasonCode::VersionConflict);
        assert_eq!(stale.params["current_version"], 1);
        assert_eq!(code(message.edit_refusal(author, 1)), None);
    }

    #[test]
    fn an_edit_replaces_the_blocks_and_records_no_content() {
        let author = Uuid::now_v7();
        let mut message = by_user(author);
        message.blocks = vec![text("the old secret")];
        let at = meta();
        message.edit(vec![text("the new secret")], &at).unwrap();
        assert_eq!(message.version, 2);
        assert_eq!(message.edited_at, Some(at.occurred_at));
        assert_eq!(message.blocks[0].content["text"], "the new secret");
        let facts = message.pending_facts();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].event_type, EDITED);
        let payload = facts[0].payload.to_string();
        assert!(!payload.contains("secret"), "{payload}");
        assert_eq!(facts[0].payload["blocks"][0]["kind"], "TEXT");
    }
}
