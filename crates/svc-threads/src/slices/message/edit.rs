//! `integration.cmd.threads.message.edit.v1`: a `COMPLETED` user message that
//! is not deleted, rewritten in place by its author. Its human message's
//! content is replaced whole by the library's plain blocks; the message keeps
//! its id, its entry's id, its place in the tree and every descendant, even
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

use contract_threads::llm::{
    EntryId, HumanId, HumanMessage, IncomingEntry, InputTurn, PlainBlock, Turn,
};
use contract_threads::{
    LibraryJson, MessageInvalidError, MessageStatus, ReasonCode, ThreadsCommand, v1,
};
use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::commands::{Verdict, message_of, refuse_on, writable_thread};
use super::content::{Content, first_repeated, invalid, minted_refusal_of, refusal_of};
use super::state::{Message, MessageState};
use super::{MessageCause, message_cause, store};
use crate::command::{Call, Inbound, MessageOutcome, Refusal, answer_message};
use crate::fault::Fault;

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

    /// The id of its human message, which an edit keeps.
    fn human_entry(&self) -> Option<EntryId> {
        self.content.entries.first().map(|entry| entry.id())
    }
}

/// `message.edit` as it arrives: [`contract_threads::EditMessage`], or the 0.4
/// form of the same offer, whose `blocks` Threads converts — a sender still on
/// `contract-threads` 0.4 keeps editing.
#[derive(Debug, Deserialize)]
pub struct EditWire {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    pub acting_user_id: Uuid,
    pub expected_version: u64,
    #[serde(default)]
    pub content: Vec<LibraryJson>,
    #[serde(default)]
    pub blocks: Option<Vec<v1::Block>>,
}

impl ThreadsCommand for EditWire {
    const AGGREGATE: &'static str = <contract_threads::EditMessage as ThreadsCommand>::AGGREGATE;
    const VERB: &'static str = <contract_threads::EditMessage as ThreadsCommand>::VERB;

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        Some(self.acting_user_id)
    }
}

/// The new content of an edit: its plain blocks, read by the library — the 0.4
/// `blocks` converted when `content` is absent. A block id the library refuses
/// as no UUIDv7 is `ID_NOT_UUIDV7`.
fn new_blocks(command: &EditWire) -> Result<Vec<PlainBlock>, Refusal> {
    match (&command.blocks, command.content.is_empty()) {
        (Some(blocks), true) => {
            let (content, annotations) =
                super::v1::user_content(blocks, Uuid::nil()).map_err(|fault| fault.refusal())?;
            if !annotations.is_empty() {
                return Err(invalid(MessageInvalidError::AnnotationInUserMessage));
            }
            Ok(content)
        }
        _ => command
            .content
            .iter()
            .map(|block| {
                block
                    .to_plain_block()
                    .map_err(|error| minted_refusal_of(&error))
            })
            .collect(),
    }
}

/// The edited message's new content: its human message, under the ids it
/// keeps, holding `blocks` — judged by the library, a repeated id refused
/// `KEY_REUSED` first.
fn edited_content(message: &MessageState, blocks: Vec<PlainBlock>) -> Result<Content, Refusal> {
    let mut ids = Vec::new();
    for block in &blocks {
        ids.push(block.id().as_uuid());
        if let PlainBlock::Text(text) = block {
            ids.extend(text.citations().iter().map(|c| c.id().as_uuid()));
        }
    }
    if let Some(repeated) = first_repeated(ids) {
        return Err(Refusal::new(ReasonCode::KeyReused).with("ids", [repeated]));
    }
    let entry = message
        .human_entry()
        .ok_or_else(|| invalid(MessageInvalidError::NotAUserTurn))?;
    let human = message
        .author_user()
        .ok_or_else(|| invalid(MessageInvalidError::NotAUserTurn))?;
    let human = HumanId::from_uuid(human).map_err(|error| refusal_of(&error))?;
    let message_turn = message
        .turn_id()
        .map_err(|_| invalid(MessageInvalidError::TurnIdMismatch))?;
    let said = HumanMessage::new(entry, human, blocks).map_err(|error| refusal_of(&error))?;
    let turn: Turn = InputTurn::new(message_turn, vec![IncomingEntry::from(said)])
        .map_err(|error| refusal_of(&error))?
        .into();
    Content::of_turn(&turn, Vec::new()).map_err(|error| refusal_of(&error))
}

pub fn edit<'r>(
    cx: &'r mut Reaction<'r>,
    Inbound(command): Inbound<EditWire>,
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
    command: &EditWire,
) -> Result<Verdict, Fault> {
    let thread = refuse_on!(writable_thread(cx, command.thread_id).await?);
    let mut state = refuse_on!(message_of(cx, command.thread_id, command.message_id).await?);
    if let Some(refusal) = state.edit_refusal(command.acting_user_id, command.expected_version) {
        return Ok(Err(refusal));
    }
    let blocks = refuse_on!(new_blocks(command));
    let content = refuse_on!(edited_content(&state, blocks));
    // The message's own ids may be kept; any other held id is reused.
    let ids = content.identities();
    let mut held = store::held_ids(cx.connection(), &ids, Some(state.id)).await?;
    if ids.contains(&thread.id) {
        held.push(thread.id);
    }
    if !held.is_empty() {
        return Ok(Err(Refusal::new(ReasonCode::KeyReused).with("ids", held)));
    }
    state.edit(content, &call.meta)?;
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
    use contract_threads::llm::{BlockId, TextBlock, TurnKind};

    use super::*;
    use crate::slices::message::state::MessageAuthor;
    use crate::slices::message::state::tests::message_in;

    fn by_user(user: Uuid) -> MessageState {
        let mut message = message_in(&[user]);
        message.author = MessageAuthor::User {
            user_id: user,
            display_name: Some("Ada".into()),
            in_roster: true,
        };
        message.turn_kind = TurnKind::InputTurn;
        message
    }

    fn text(words: &str) -> PlainBlock {
        PlainBlock::try_from(
            TextBlock::new(BlockId::from_uuid(Uuid::now_v7()).unwrap(), words).unwrap(),
        )
        .unwrap()
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
    fn the_wire_of_the_0_4_edit_still_decodes() {
        let edit: EditWire = serde_json::from_value(serde_json::json!({
            "message_id": Uuid::now_v7(),
            "thread_id": Uuid::now_v7(),
            "acting_user_id": Uuid::now_v7(),
            "expected_version": 2,
            "blocks": [{ "id": Uuid::now_v7(), "kind": "TEXT", "content": { "type": "text", "text": "hi" } }]
        }))
        .unwrap();
        let blocks = new_blocks(&edit).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(EditWire::coords(), contract_threads::EditMessage::coords());
    }

    #[test]
    fn a_block_id_that_is_no_uuidv7_is_id_not_uuidv7() {
        let edit: EditWire = serde_json::from_value(serde_json::json!({
            "message_id": Uuid::now_v7(),
            "thread_id": Uuid::now_v7(),
            "acting_user_id": Uuid::now_v7(),
            "expected_version": 2,
            "content": [{ "type": "text", "id": Uuid::new_v4(), "text": "hi" }]
        }))
        .unwrap();
        assert_eq!(new_blocks(&edit).unwrap_err().code, ReasonCode::IdNotUuidV7);
    }

    #[test]
    fn an_edit_keeps_the_human_messages_id_and_refuses_an_id_named_twice() {
        let author = Uuid::now_v7();
        let mut message = by_user(author);
        let entry = Uuid::now_v7();
        let said = HumanMessage::new(
            EntryId::from_uuid(entry).unwrap(),
            HumanId::from_uuid(author).unwrap(),
            vec![text("before")],
        )
        .unwrap();
        let turn: Turn = InputTurn::new(message.turn_id().unwrap(), vec![said.into()])
            .unwrap()
            .into();
        message.content = Content::of_turn(&turn, Vec::new()).unwrap();
        let content = edited_content(&message, vec![text("after"), text("and more")]).unwrap();
        assert_eq!(content.entries[0].id().as_uuid(), entry);
        assert_eq!(content.blocks.len(), 2);
        let twice = text("twice");
        let refused = edited_content(&message, vec![twice.clone(), twice]).unwrap_err();
        assert_eq!(refused.code, ReasonCode::KeyReused);
        let refused = edited_content(&message, Vec::new()).unwrap_err();
        assert_eq!(refused.code, ReasonCode::MessageInvalid);
        assert_eq!(refused.params["error"], "invalid_content");
    }
}
