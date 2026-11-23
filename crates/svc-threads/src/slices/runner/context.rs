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
use crate::slices::artifact::context::{
    self as artifact_context, AttachedArtifact, ContextArtifact,
};
use crate::slices::artifact::read_access::SigningPermits;
use crate::slices::message::closing::judge_streams;
use crate::slices::message::state::{MessageAuthor, MessageState};
use crate::slices::message::store::{block_of_row, entry_of_row};
use crate::slices::message::stream::TurnContent;
use crate::slices::thread::state::{ThreadState, UnknownStoredValue};
use crate::slices::thread::store::ThreadStore;
use crate::slices::thread::view::{MemberView, ResourceView};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct ThreadContext {
    pub thread_id: Uuid,
    pub conversation: Option<JsonScalar>,
    pub projected_thread: Option<JsonScalar>,
    pub members: Vec<MemberView>,
    pub resources: Vec<ResourceView>,
    #[graphql(skip)]
    pub artifacts: Vec<AttachedArtifact>,
    /// The signings this read may run at once, shared by all its artifacts.
    /// Never stored: every read decodes its context afresh, with its own.
    #[graphql(skip)]
    #[serde(skip)]
    pub signing: SigningPermits,
}

#[async_graphql::ComplexObject]
impl ThreadContext {
    /// The `ATTACHED` artifacts of the messages of the conversation, beside it.
    async fn artifacts(&self) -> Vec<ContextArtifact> {
        ContextArtifact::of_read(&self.artifacts, &self.signing)
    }
}

/// One context: a thread, read whole or for an agent, its whole list or the
/// list up to the message `up_to` names, that message included. The bound is
/// part of the key, so that a bounded context is never rendered for another
/// read; it is left out of the key's JSON when absent.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContextKey {
    pub thread: Uuid,
    pub agent: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub up_to: Option<Uuid>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContextWindow {
    pub thread: Option<Uuid>,
    pub agent: Option<Uuid>,
    /// The last message of the conversation read; none for the whole list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub up_to: Option<Uuid>,
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

    /// Never live: no noun and no inverse, so no impact reaches a context, and
    /// every read populates and renders afresh under its own key — a bounded
    /// one included. Making it live would need the message noun's impacts
    /// routed to every key of the thread, whatever its bound.
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
                    up_to: window.up_to,
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

/// The context of one thread in one statement: `$1` the thread, `$2` the agent
/// read for (or none), `$3` the plain block kinds a foreign response keeps, `$4`
/// the last message of the conversation (or none: the whole list).
///
/// The messages are the thread's list, in the order they were stored
/// (`created_at`, then id). The bound is looked for among the thread's own
/// messages only: a message unknown to the thread, or a message of another
/// thread, bounds nothing, and the conversation is empty. A bound that is
/// deleted, cancelled, or failed before its first entry still bounds the
/// conversation: the `turn` filter leaves it out, as it leaves out such a
/// message anywhere in the list, and the messages before it stay.
pub fn context_query() -> String {
    let response = TurnEntryKind::ModelResponse.as_str();
    let closure = TurnEntryKind::TurnClosure.as_str();
    let complete = RepresentationState::Complete.as_str();
    let ai_turn = TurnKind::AITurn.as_str();
    let cancelled = MessageStatus::Cancelled.as_str();
    let failed = MessageStatus::Failed.as_str();
    format!(
        "WITH bound AS ( \
             SELECT m.created_at, m.id FROM threads.message m \
             WHERE m.thread_id = $1 AND m.id = $4 \
         ), list AS ( \
             SELECT m.id, row_number() OVER (ORDER BY m.created_at, m.id) AS ordinal \
             FROM threads.message m WHERE m.thread_id = $1 \
               AND ($4::uuid IS NULL OR EXISTS ( \
                   SELECT 1 FROM bound b WHERE (m.created_at, m.id) <= (b.created_at, b.id))) \
         ), turn AS ( \
             SELECT m.id, l.ordinal, m.status, m.turn_kind, m.author_user_id, \
                    m.author_agent_id, m.author_ai_name, m.observed_at, \
                    (m.turn_kind = '{ai_turn}' AND $2::uuid IS NOT NULL \
                     AND m.author_agent_id <> $2) AS foreign_turn \
             FROM list l JOIN threads.message m ON m.id = l.id \
             WHERE NOT m.deleted AND m.status <> '{cancelled}' \
               AND (m.status <> '{failed}' OR EXISTS ( \
                   SELECT 1 FROM threads.message_entry f \
                   WHERE f.message_id = m.id AND f.kind <> '{closure}')) \
         ), entry AS ( \
             SELECT e.id, e.message_id, e.position, e.kind, e.state, e.sender_kind, \
                    e.sender_id, e.author, e.observed_at, \
                    CASE WHEN t.foreign_turn AND e.kind = '{response}' THEN '{{}}'::jsonb \
                         ELSE e.payload END AS payload, \
                    t.ordinal, t.foreign_turn \
             FROM turn t JOIN threads.message_entry e ON e.message_id = t.id \
             WHERE e.state = '{complete}' \
               AND (NOT t.foreign_turn OR e.kind = '{response}') \
         ) \
         SELECT 0 AS row_kind, t.ordinal, t.id AS message_id, t.id, 0 AS position, \
                t.status AS kind, t.turn_kind AS state, NULL::text AS sender_kind, \
                t.author_user_id AS sender_id, t.author_ai_name AS author, t.observed_at, \
                NULL::jsonb AS payload, NULL::uuid AS entry_id, NULL::uuid AS parent_id, \
                t.author_agent_id AS call_id \
         FROM turn t \
         UNION ALL \
         SELECT 1, e.ordinal, e.message_id, e.id, e.position, e.kind, e.state, e.sender_kind, \
                e.sender_id, e.author, e.observed_at, e.payload, NULL, NULL, NULL \
         FROM entry e \
         UNION ALL \
         SELECT 2, e.ordinal, b.message_id, b.id, b.position, b.kind, b.state, NULL, NULL, \
                NULL, b.observed_at, b.payload, b.entry_id, b.parent_id, b.call_id \
         FROM entry e JOIN threads.entry_block b ON b.entry_id = e.id \
         WHERE b.state = '{complete}' \
           AND (NOT e.foreign_turn OR e.kind <> '{response}' \
                OR (b.parent_id IS NULL AND b.kind = ANY($3))) \
         ORDER BY 2, 1, 5"
    )
}

struct ListedTurn {
    message: MessageState,
    foreign: bool,
}

#[derive(Default)]
struct Listed {
    turns: Vec<ListedTurn>,
    entries: Vec<EntryRecord>,
    blocks: Vec<BlockRecord>,
    drafts: Vec<ResponseDraft>,
}

impl Listed {
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
    let mut listed = read_list(conn, chunks, key, agent).await?;
    if closed_meanwhile(conn, &listed.streaming()).await? {
        listed = read_list(conn, chunks, key, agent).await?;
    }
    let messages: Vec<Uuid> = listed.turns.iter().map(|turn| turn.message.id).collect();
    let artifacts = artifact_context::of_messages(conn, &messages).await?;
    let records = listed
        .turns
        .iter()
        .map(|turn| turn.message.turn_record())
        .collect::<Result<Vec<TurnRecord>, _>>()?;
    let set = RecordSet::new()
        .with_turns(records)
        .with_entries(listed.entries)
        .with_blocks(listed.blocks);
    let conversation = ConversationId::from_uuid(key.thread).map_err(corrupt)?;
    let read = match agent {
        Some(agent) => {
            let pending: Vec<PendingResponse<'_>> =
                listed.drafts.iter().map(PendingResponse::from).collect();
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
    Ok(Some(context_of(&thread, read, artifacts)))
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

async fn read_list(
    conn: &mut PgConnection,
    chunks: &service_engine::accumulator::ChunkReader,
    key: &ContextKey,
    agent: Option<AgentId>,
) -> Result<Listed, EngineError> {
    let rows = sqlx::query(&context_query())
        .bind(key.thread)
        .bind(key.agent)
        .bind(plain_kinds())
        .bind(key.up_to)
        .fetch_all(&mut *conn)
        .await?;
    let mut listed = Listed::default();
    for row in &rows {
        let row_kind: i32 = row.try_get("row_kind")?;
        match row_kind {
            0 => listed.turns.push(turn_of_row(row, agent)?),
            1 => listed.entries.push(entry_of_row(row)?),
            2 => listed.blocks.push(block_of_row(row)?),
            other => {
                return Err(corrupt(UnknownStoredValue {
                    column: "context.row_kind",
                    value: other.to_string(),
                }));
            }
        }
    }
    let streaming: Vec<&ListedTurn> = listed
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
    listed.entries.extend(entries);
    listed.blocks.extend(blocks);
    listed.drafts = drafts;
    Ok(listed)
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

fn turn_of_row(row: &PgRow, agent: Option<AgentId>) -> Result<ListedTurn, EngineError> {
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
    Ok(ListedTurn { message, foreign })
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

fn context_of(thread: &ThreadState, read: Read, artifacts: Vec<AttachedArtifact>) -> ThreadContext {
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
        artifacts,
        signing: SigningPermits::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::principal::tests::{active, person, service};

    fn with_artifacts(count: usize) -> ThreadContext {
        ThreadContext {
            thread_id: Uuid::now_v7(),
            conversation: Some(async_graphql::Json(serde_json::json!({ "turns": [] }))),
            projected_thread: None,
            members: vec![MemberView {
                user_id: Uuid::now_v7(),
                display_name: Some("Ada".into()),
                in_roster: true,
            }],
            resources: Vec::new(),
            artifacts: (0..count)
                .map(|index| AttachedArtifact {
                    id: Uuid::now_v7(),
                    name: format!("file-{index}.csv"),
                    media_type: "text/csv".into(),
                    size: 3,
                    sha256: "a".repeat(64),
                    message_id: Uuid::now_v7(),
                    blob_ref: Uuid::now_v7(),
                })
                .collect(),
            signing: SigningPermits::default(),
        }
    }

    struct Probe(ThreadContext);

    #[async_graphql::Object]
    impl Probe {
        async fn context(&self) -> Option<ThreadContext> {
            Some(self.0.clone())
        }
    }

    /// No engine stands behind this schema, so every signing fails: each
    /// failure nulls its own access and is reported at its own path, and the
    /// context — its conversation, members and artifacts — stays whole.
    #[tokio::test]
    async fn a_failed_signing_nulls_its_own_access_and_leaves_the_context_whole() {
        let context = with_artifacts(2);
        let schema = async_graphql::Schema::build(
            Probe(context.clone()),
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        )
        .finish();
        let response = schema
            .execute(
                "{ context { threadId conversation members { userId } \
                   artifacts { id name downloadUrl } } }",
            )
            .await;
        let data = response.data.into_json().expect("JSON data");
        let read = &data["context"];
        assert_eq!(
            read["threadId"],
            serde_json::json!(context.thread_id),
            "{data}"
        );
        assert_eq!(read["conversation"], serde_json::json!({ "turns": [] }));
        assert_eq!(read["members"].as_array().map(Vec::len), Some(1));
        let artifacts = read["artifacts"].as_array().expect("the artifacts, whole");
        assert_eq!(artifacts.len(), 2, "{data}");
        for (read, held) in artifacts.iter().zip(&context.artifacts) {
            assert_eq!(read["id"], serde_json::json!(held.id));
            assert_eq!(read["name"], serde_json::json!(held.name));
            assert!(read["downloadUrl"].is_null(), "{read}");
        }
        let paths: Vec<String> = response
            .errors
            .iter()
            .map(|error| serde_json::to_string(&error.path).expect("a path"))
            .collect();
        assert_eq!(
            paths,
            [
                r#"["context","artifacts",0,"downloadUrl"]"#,
                r#"["context","artifacts",1,"downloadUrl"]"#,
            ],
            "one error per failed access, at its own path"
        );
    }

    #[test]
    fn the_artifacts_of_one_read_sign_under_its_permits_and_each_read_has_its_own() {
        let held = with_artifacts(3);
        let read: ThreadContext =
            serde_json::from_value(serde_json::to_value(&held).expect("encode"))
                .expect("a context decodes as the engine decodes each read");
        assert_eq!(read, held, "permits are not part of the view");
        assert!(!read.signing.shared_with(&held.signing));
        let artifacts = ContextArtifact::of_read(&read.artifacts, &read.signing);
        assert_eq!(artifacts.len(), 3);
        assert!(
            artifacts
                .iter()
                .all(|artifact| artifact.permits().shared_with(&read.signing))
        );
    }

    #[test]
    fn the_context_is_read_in_one_statement_over_the_list_up_to_its_bound() {
        let query = context_query();
        assert!(
            !query.contains("RECURSIVE"),
            "no message names another: {query}"
        );
        let bound = &query[query.find("bound AS").unwrap()..query.find("list AS").unwrap()];
        assert!(
            bound.contains("m.thread_id = $1") && bound.contains("m.id = $4"),
            "the bound, among the thread's own messages only: {bound}"
        );
        let list = &query[query.find("list AS").unwrap()..query.find("turn AS").unwrap()];
        assert!(
            list.contains("row_number() OVER (ORDER BY m.created_at, m.id)")
                && list.contains("($4::uuid IS NULL OR EXISTS (")
                && list.contains("(m.created_at, m.id) <= (b.created_at, b.id)"),
            "the list in the order it was stored, up to its bound included: {list}"
        );
        assert!(
            query.ends_with("ORDER BY 2, 1, 5"),
            "the oldest message first: {query}"
        );
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
        assert!(query.contains("ANY($3)") && !query.contains("$5"));
        assert_eq!(
            query.matches("$4").count(),
            2,
            "the bound is read by the bound and list CTEs only"
        );
    }

    /// The window and the key name the bound, so that a bounded read and a
    /// whole one are two contexts; a whole read encodes without it.
    #[test]
    fn the_bound_is_part_of_the_context_key_and_absent_when_none_is_named() {
        let (thread, agent, up_to) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let whole = ContextKey {
            thread,
            agent: Some(agent),
            up_to: None,
        };
        let bounded = ContextKey {
            up_to: Some(up_to),
            ..whole.clone()
        };
        assert_ne!(whole, bounded);
        assert_eq!(
            serde_json::to_value(&whole).unwrap(),
            serde_json::json!({ "thread": thread, "agent": agent })
        );
        assert_eq!(
            serde_json::to_value(&bounded).unwrap(),
            serde_json::json!({ "thread": thread, "agent": agent, "up_to": up_to })
        );
        let decoded: ContextKey =
            serde_json::from_value(serde_json::json!({ "thread": thread, "agent": null }))
                .expect("a key without a bound decodes");
        assert_eq!(decoded.up_to, None);

        let window = ContextWindow {
            thread: Some(thread),
            agent: None,
            up_to: None,
        };
        assert_eq!(
            WindowParams::encode(&window).unwrap().as_value(),
            &serde_json::json!({ "thread": thread, "agent": null })
        );
        let bounded: ContextWindow = WindowParams::encode(&ContextWindow {
            up_to: Some(up_to),
            ..window
        })
        .unwrap()
        .decode()
        .unwrap();
        assert_eq!(bounded.up_to, Some(up_to));
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
