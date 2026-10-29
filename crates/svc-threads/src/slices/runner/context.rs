use std::collections::{BTreeSet, HashSet};

use contract_threads::MessageStatus;
use contract_threads::llm::{
    AgentId, BlockRecord, ContentBlockKind, ConversationId, EntryRecord, PendingResponse,
    RecordSet, RepresentationState, ResponseDraft, Turn, TurnEntryKind, TurnKind, TurnRecord,
};
use futures_util::future::{BoxFuture, try_join_all};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::Persistence;
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::{JsonScalar, KeyCeiling};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::access;
use crate::fault::{corrupt, encode};
use crate::principal::ThreadsPrincipal;
use crate::slices::message::closing::judge_streams;
use crate::slices::message::state::{MessageAuthor, MessageState};
use crate::slices::message::store::{block_of_row, entry_of_row};
use crate::slices::message::stream::TurnContent;
use crate::slices::thread::state::{ThreadState, UnknownStoredValue};
use crate::slices::thread::store::ThreadStore;
use crate::slices::thread::view::{MemberView, ResourceView};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct ThreadContext {
    pub thread_id: Uuid,
    pub conversation: Option<JsonScalar>,
    pub projected_thread: Option<JsonScalar>,
    pub members: Vec<MemberView>,
    pub resources: Vec<ResourceView>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContextKey {
    pub thread: Uuid,
    pub agent: Option<Uuid>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContextWindow {
    pub thread: Option<Uuid>,
    pub agent: Option<Uuid>,
}

#[derive(Default)]
pub struct ContextView;

impl ContextView {
    pub const NAME: ProjectorName = ProjectorName::from_static("runner_context");
}

pub struct ContextFacts {
    contexts: Vec<(ContextKey, ThreadContext)>,
}

impl Projector for ContextView {
    type Principal = ThreadsPrincipal;
    type Key = ContextKey;
    type Facts = ContextFacts;
    type View = ThreadContext;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        &[]
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        window: &'a WindowParams,
        _ceiling: KeyCeiling,
        principal: &'a ThreadsPrincipal,
    ) -> BoxFuture<'a, Result<Population<ContextKey>, EngineError>> {
        Box::pin(async move {
            let window: ContextWindow = if window.as_value().is_null() {
                ContextWindow::default()
            } else {
                window.decode()?
            };
            let Some(thread) = window.thread else {
                return Ok(Population::Keys(BTreeSet::new()));
            };
            if !access::runner_surface(principal).is_allowed() {
                return Ok(Population::Keys(BTreeSet::new()));
            }
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM threads.thread WHERE id = $1)")
                    .bind(thread)
                    .fetch_one(pg)
                    .await?;
            let keys = exists
                .then_some(ContextKey {
                    thread,
                    agent: window.agent,
                })
                .into_iter()
                .collect();
            Ok(Population::Keys(keys))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<ContextKey> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, ContextKey, ThreadsPrincipal>,
    ) -> BoxFuture<'a, Result<ContextFacts, EngineError>> {
        Box::pin(async move {
            let keys = scope.keys().to_vec();
            let chunks = scope.chunks();
            let mut contexts = Vec::with_capacity(keys.len());
            match scope {
                LoadScope::Bulk { pg, .. } => {
                    let mut conn = pg.acquire().await.map_err(EngineError::from)?;
                    for key in keys {
                        let context = read(&mut conn, chunks, &key).await?;
                        contexts.extend(context.map(|context| (key, context)));
                    }
                }
                LoadScope::PerPrincipal { conn, .. } => {
                    for key in keys {
                        let context = read(conn, chunks, &key).await?;
                        contexts.extend(context.map(|context| (key, context)));
                    }
                }
            }
            Ok(ContextFacts { contexts })
        })
    }

    fn project(
        &self,
        facts: &ContextFacts,
        key: &ContextKey,
        principal: &ThreadsPrincipal,
    ) -> Result<Option<ThreadContext>, EngineError> {
        if !access::runner_surface(principal).is_allowed() {
            return Ok(None);
        }
        Ok(facts
            .contexts
            .iter()
            .find(|(held, _)| held == key)
            .map(|(_, context)| context.clone()))
    }
}

fn plain_kinds() -> Vec<&'static str> {
    ContentBlockKind::ALL
        .iter()
        .filter(|kind| kind.is_plain())
        .map(|kind| kind.as_str())
        .collect()
}

pub fn context_query() -> String {
    let response = TurnEntryKind::ModelResponse.as_str();
    let closure = TurnEntryKind::TurnClosure.as_str();
    let complete = RepresentationState::Complete.as_str();
    let ai_turn = TurnKind::AITurn.as_str();
    let cancelled = MessageStatus::Cancelled.as_str();
    let failed = MessageStatus::Failed.as_str();
    format!(
        "WITH RECURSIVE tip AS ( \
             SELECT m.id FROM threads.message m WHERE m.thread_id = $1 \
             ORDER BY m.created_at DESC, m.id DESC LIMIT 1 \
         ), branch AS ( \
             SELECT m.id, m.parent_message_id, 0 AS depth \
             FROM threads.message m JOIN tip ON m.id = tip.id \
             UNION ALL \
             SELECT p.id, p.parent_message_id, b.depth + 1 \
             FROM threads.message p JOIN branch b ON p.id = b.parent_message_id \
         ), turn AS ( \
             SELECT m.id, b.depth, m.status, m.turn_kind, m.author_user_id, m.author_agent_id, \
                    m.author_ai_name, m.observed_at, \
                    (m.turn_kind = '{ai_turn}' AND $2::uuid IS NOT NULL \
                     AND m.author_agent_id <> $2) AS foreign_turn \
             FROM branch b JOIN threads.message m ON m.id = b.id \
             WHERE NOT m.deleted AND m.status <> '{cancelled}' \
               AND (m.status <> '{failed}' OR EXISTS ( \
                   SELECT 1 FROM threads.message_entry f \
                   WHERE f.message_id = m.id AND f.kind <> '{closure}')) \
         ), entry AS ( \
             SELECT e.id, e.message_id, e.position, e.kind, e.state, e.sender_kind, \
                    e.sender_id, e.author, e.observed_at, \
                    CASE WHEN t.foreign_turn AND e.kind = '{response}' THEN '{{}}'::jsonb \
                         ELSE e.payload END AS payload, \
                    t.depth, t.foreign_turn \
             FROM turn t JOIN threads.message_entry e ON e.message_id = t.id \
             WHERE e.state = '{complete}' \
               AND (NOT t.foreign_turn OR e.kind = '{response}') \
         ) \
         SELECT 0 AS row_kind, t.depth, t.id AS message_id, t.id, 0 AS position, \
                t.status AS kind, t.turn_kind AS state, NULL::text AS sender_kind, \
                t.author_user_id AS sender_id, t.author_ai_name AS author, t.observed_at, \
                NULL::jsonb AS payload, NULL::uuid AS entry_id, NULL::uuid AS parent_id, \
                t.author_agent_id AS call_id \
         FROM turn t \
         UNION ALL \
         SELECT 1, e.depth, e.message_id, e.id, e.position, e.kind, e.state, e.sender_kind, \
                e.sender_id, e.author, e.observed_at, e.payload, NULL, NULL, NULL \
         FROM entry e \
         UNION ALL \
         SELECT 2, e.depth, b.message_id, b.id, b.position, b.kind, b.state, NULL, NULL, NULL, \
                b.observed_at, b.payload, b.entry_id, b.parent_id, b.call_id \
         FROM entry e JOIN threads.entry_block b ON b.entry_id = e.id \
         WHERE b.state = '{complete}' \
           AND (NOT e.foreign_turn OR e.kind <> '{response}' \
                OR (b.parent_id IS NULL AND b.kind = ANY($3))) \
         ORDER BY 2 DESC, 1, 5"
    )
}

struct BranchTurn {
    message: MessageState,
    foreign: bool,
}

#[derive(Default)]
struct Branch {
    turns: Vec<BranchTurn>,
    entries: Vec<EntryRecord>,
    blocks: Vec<BlockRecord>,
    drafts: Vec<ResponseDraft>,
}

impl Branch {
    fn streaming(&self) -> Vec<uuid::Uuid> {
        self.turns
            .iter()
            .filter(|turn| turn.message.status == MessageStatus::Streaming)
            .map(|turn| turn.message.id)
            .collect()
    }
}

async fn read(
    conn: &mut PgConnection,
    chunks: &service_engine::accumulator::ChunkReader,
    key: &ContextKey,
) -> Result<Option<ThreadContext>, EngineError> {
    let Some(thread) = ThreadStore::read_many(conn, &[key.thread])
        .await?
        .into_iter()
        .map(|(_, thread)| thread)
        .next()
    else {
        return Ok(None);
    };
    let agent = key
        .agent
        .map(AgentId::from_uuid)
        .transpose()
        .map_err(corrupt)?;
    let mut branch = read_branch(conn, chunks, key, agent).await?;
    if closed_meanwhile(conn, &branch.streaming()).await? {
        branch = read_branch(conn, chunks, key, agent).await?;
    }
    let records = branch
        .turns
        .iter()
        .map(|turn| turn.message.turn_record())
        .collect::<Result<Vec<TurnRecord>, _>>()?;
    let set = RecordSet::new()
        .with_turns(records)
        .with_entries(branch.entries)
        .with_blocks(branch.blocks);
    let conversation = ConversationId::from_uuid(key.thread).map_err(corrupt)?;
    let read = match agent {
        Some(agent) => {
            let pending: Vec<PendingResponse<'_>> =
                branch.drafts.iter().map(PendingResponse::from).collect();
            let projected = set
                .into_projected_thread_pending(conversation, agent, &pending)
                .map_err(|error| store_fault(key.thread, &error))?;
            Read::Projected(
                serde_json::to_value(&projected)
                    .map_err(|source| encode("an agent context", source))?,
            )
        }
        None => {
            let whole = set
                .into_conversation(conversation)
                .map_err(|error| store_fault(key.thread, &error))?;
            Read::Whole(
                serde_json::to_value(&whole).map_err(|source| encode("a conversation", source))?,
            )
        }
    };
    Ok(Some(context_of(&thread, read)))
}

async fn closed_meanwhile(
    conn: &mut PgConnection,
    streaming: &[uuid::Uuid],
) -> Result<bool, EngineError> {
    if streaming.is_empty() {
        return Ok(false);
    }
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM threads.message WHERE id = ANY($1) AND status <> $2)",
    )
    .bind(streaming)
    .bind(MessageStatus::Streaming.as_str())
    .fetch_one(conn)
    .await?)
}

async fn read_branch(
    conn: &mut PgConnection,
    chunks: &service_engine::accumulator::ChunkReader,
    key: &ContextKey,
    agent: Option<AgentId>,
) -> Result<Branch, EngineError> {
    let rows = sqlx::query(&context_query())
        .bind(key.thread)
        .bind(key.agent)
        .bind(plain_kinds())
        .fetch_all(&mut *conn)
        .await?;
    let mut branch = Branch::default();
    for row in &rows {
        let row_kind: i32 = row.try_get("row_kind")?;
        match row_kind {
            0 => branch.turns.push(turn_of_row(row, agent)?),
            1 => branch.entries.push(entry_of_row(row)?),
            2 => branch.blocks.push(block_of_row(row)?),
            other => {
                return Err(corrupt(UnknownStoredValue {
                    column: "context.row_kind",
                    value: other.to_string(),
                }));
            }
        }
    }
    let streaming: Vec<&BranchTurn> = branch
        .turns
        .iter()
        .filter(|turn| turn.message.status == MessageStatus::Streaming)
        .collect();
    let streams = try_join_all(streaming.iter().map(|turn| async move {
        Ok::<_, EngineError>(chunks.state::<TurnContent>(&turn.message.id).await?.state)
    }))
    .await?;
    let pairs: Vec<_> = streaming
        .iter()
        .map(|turn| &turn.message)
        .zip(streams.iter())
        .collect();
    let judged = judge_streams(conn, &pairs).await?;
    let mut entries = Vec::new();
    let mut blocks = Vec::new();
    let mut drafts = Vec::new();
    for (turn, judged) in streaming.iter().zip(judged) {
        let set = RecordSet::from_turn(&Turn::AITurn(judged.turn.clone())).map_err(corrupt)?;
        let kept: HashSet<_> = set
            .entries()
            .iter()
            .filter(|entry| !turn.foreign || reduced_entry(entry.kind()))
            .map(EntryRecord::id)
            .collect();
        entries.extend(
            set.entries()
                .iter()
                .filter(|entry| kept.contains(&entry.id()))
                .cloned(),
        );
        blocks.extend(
            set.blocks()
                .iter()
                .filter(|block| {
                    kept.contains(&block.entry()) && (!turn.foreign || reduced_block(&set, block))
                })
                .cloned(),
        );
        if turn.foreign
            && let Some(producing) = judged.producing()
        {
            drafts.push(producing.clone());
        }
    }
    branch.entries.extend(entries);
    branch.blocks.extend(blocks);
    branch.drafts = drafts;
    Ok(branch)
}

fn reduced_entry(kind: TurnEntryKind) -> bool {
    kind == TurnEntryKind::ModelResponse
}

fn reduced_block(set: &RecordSet, block: &BlockRecord) -> bool {
    let of_response = set
        .entries()
        .iter()
        .any(|entry| entry.id() == block.entry() && entry.kind() == TurnEntryKind::ModelResponse);
    !of_response || (block.parent().is_none() && block.kind().is_plain())
}

fn store_fault(thread: Uuid, error: &contract_threads::llm::Error) -> EngineError {
    EngineError::Service(format!("the context of thread {thread} does not load: {error}").into())
}

fn turn_of_row(row: &PgRow, agent: Option<AgentId>) -> Result<BranchTurn, EngineError> {
    let unknown = |column: &'static str, value: &str| {
        corrupt(UnknownStoredValue {
            column,
            value: value.to_owned(),
        })
    };
    let status: String = row.try_get("kind")?;
    let turn_kind: Option<String> = row.try_get("state")?;
    let turn_kind = turn_kind.ok_or_else(|| unknown("message.turn_kind", "NULL"))?;
    let turn_kind = crate::slices::message::state::turn_kind_of_db(&turn_kind).map_err(corrupt)?;
    let user: Option<Uuid> = row.try_get("sender_id")?;
    let agent_id: Option<Uuid> = row.try_get("call_id")?;
    let name: Option<String> = row.try_get("author")?;
    let author = match (user, agent_id, name) {
        (Some(user_id), None, None) => MessageAuthor::User {
            user_id,
            display_name: None,
            in_roster: true,
        },
        (None, Some(agent_id), Some(name)) => MessageAuthor::Agent {
            agent: AgentId::from_uuid(agent_id).map_err(corrupt)?,
            name: contract_threads::llm::Author::new(name).map_err(corrupt)?,
        },
        _ => return Err(unknown("message.author", "not exactly one author")),
    };
    let observed_at: Option<String> = row.try_get("observed_at")?;
    let foreign = turn_kind == TurnKind::AITurn
        && agent.is_some_and(|named| Some(named) != author_agent(&author));
    let message = MessageState {
        id: row.try_get("id")?,
        thread_id: Uuid::nil(),
        parent_message_id: None,
        author,
        job_id: None,
        status: MessageStatus::parse(&status).map_err(corrupt)?,
        deleted: false,
        created_at: chrono::DateTime::<chrono::Utc>::MIN_UTC,
        edited_at: None,
        version: 1,
        turn_kind,
        observed_at: observed_at
            .map(contract_threads::llm::ObservedAt::new)
            .transpose()
            .map_err(corrupt)?,
        content: Default::default(),
        members: BTreeSet::new(),
        pending: Vec::new(),
    };
    Ok(BranchTurn { message, foreign })
}

fn author_agent(author: &MessageAuthor) -> Option<AgentId> {
    match author {
        MessageAuthor::Agent { agent, .. } => Some(*agent),
        MessageAuthor::User { .. } => None,
    }
}

enum Read {
    Whole(serde_json::Value),
    Projected(serde_json::Value),
}

fn context_of(thread: &ThreadState, read: Read) -> ThreadContext {
    let (conversation, projected_thread) = match read {
        Read::Whole(json) => (Some(async_graphql::Json(json)), None),
        Read::Projected(json) => (None, Some(async_graphql::Json(json))),
    };
    ThreadContext {
        thread_id: thread.id,
        conversation,
        projected_thread,
        members: thread
            .members
            .iter()
            .map(|(user_id, member)| MemberView {
                user_id: *user_id,
                display_name: member.display_name.clone(),
                in_roster: member.in_roster,
            })
            .collect(),
        resources: thread
            .resources
            .iter()
            .map(|(key, ids)| ResourceView {
                key: key.clone(),
                ids: ids.clone(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::principal::tests::{active, person, service};

    #[test]
    fn the_context_is_read_in_one_statement_over_the_active_branch() {
        let query = context_query();
        assert_eq!(query.matches("WITH RECURSIVE").count(), 1);
        assert!(
            query.contains("e.state = 'complete'"),
            "never an interrupted response"
        );
        assert!(
            query.contains("'{}'::jsonb"),
            "another agent's response payload is not read"
        );
        let selection = &query[query.find("entry AS").unwrap()..query.find("SELECT 0").unwrap()];
        assert!(
            !selection.contains("e.*") && selection.contains("CASE WHEN t.foreign_turn"),
            "the entry selection names its columns and blanks a foreign payload itself"
        );
        assert!(query.contains("ANY($3)") && !query.contains("$4"));
    }

    #[test]
    fn the_plain_kinds_the_context_keeps_are_the_librarys() {
        assert_eq!(plain_kinds(), ["text", "image", "structured_content"]);
    }

    #[test]
    fn a_runner_reads_any_thread_and_nobody_else_does() {
        let runner = service(&[crate::scopes::RUNNER]);
        assert!(access::runner_surface(&runner).is_allowed());
        assert!(!access::runner_surface(&service(&[])).is_allowed());
        assert!(!access::runner_surface(&person(true, active())).is_allowed());
    }

    #[test]
    fn another_agents_turn_keeps_only_its_responses() {
        assert!(reduced_entry(TurnEntryKind::ModelResponse));
        assert!(!reduced_entry(TurnEntryKind::AgentMail));
        assert!(!reduced_entry(TurnEntryKind::HumanMessage));
        assert!(!reduced_entry(TurnEntryKind::RuntimeNotice));
        assert!(!reduced_entry(TurnEntryKind::RuntimeToolResults));
        assert!(!reduced_entry(TurnEntryKind::TurnClosure));
    }

    #[test]
    fn a_foreign_turn_is_not_asked_for_what_it_received() {
        let query = context_query();
        let selection = &query[query.find("entry AS").unwrap()..query.find("SELECT 0").unwrap()];
        assert!(selection.contains("NOT t.foreign_turn OR e.kind = 'model_response'"));
        assert!(!selection.contains("ANY"));
    }
}
