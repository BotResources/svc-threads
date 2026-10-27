-- Threads 0.5: a message is a turn of the br-llm-messages 2.0 model (schema
-- `br-llm-messages/5`), stored as the library's records
-- (`br-llm-messages-records/1`): `threads.message` stays the turn row, beside it
-- one row per entry (`message_entry`) and one per block (`entry_block`),
-- mirroring the library's EntryRecord and BlockRecord, and Threads' own
-- annotations (`message_annotation`).
--
-- The 0.4 decomposition — `message_item`, and `message_block` under it — is
-- kept as it is, read only by the upgrade that runs at boot before the service
-- is ready (src/slices/message/upgrade.rs): it converts each message once, one
-- transaction per batch of messages, and marks it
-- `content_schema = 'br-llm-messages/5'`.
-- Threads never writes the 0.4 tables otherwise — but deleting or editing a
-- message removes its 0.4 rows, so that no previous content survives anywhere.
-- A later version drops them.

-- ── the turn row ────────────────────────────────────────────────────────────
-- `content_schema` is the conversion's marker: every message stored by 0.4 or
-- earlier is `br-llm-messages/3` until the upgrade converts it; a message
-- stored by 0.5 names `br-llm-messages/5`, written explicitly. The default
-- stays `br-llm-messages/3` on purpose: a 0.4 pod brought back after this
-- migration (a rollback) inserts without naming the column, so the messages it
-- stores are marked for conversion, and the next 0.5 boot converts them.
-- `turn_kind` is the library's turn kind, known once converted. The AI author
-- is the library's agent id (`author_agent_id`, a UUIDv7) and its name
-- (`author_ai_name`); `author_ai_id`, the 0.4 free-text id, is read by the
-- upgrade and cleared by it. `observed_at` is the turn's own, as the library
-- writes it.
ALTER TABLE threads.message
    ADD COLUMN content_schema text NOT NULL DEFAULT 'br-llm-messages/3'
        CHECK (content_schema IN ('br-llm-messages/3', 'br-llm-messages/5')),
    ADD COLUMN turn_kind text CHECK (turn_kind IN ('input_turn', 'ai_turn')),
    ADD COLUMN author_agent_id uuid,
    ADD COLUMN observed_at text;

ALTER TABLE threads.message DROP CONSTRAINT message_one_author;
ALTER TABLE threads.message ADD CONSTRAINT message_one_author CHECK (
    (author_user_id IS NOT NULL AND author_ai_id IS NULL AND author_agent_id IS NULL
        AND author_ai_name IS NULL)
    OR (author_user_id IS NULL AND author_ai_name <> '' AND (
        (content_schema = 'br-llm-messages/3' AND author_ai_id <> '' AND author_agent_id IS NULL)
        OR (content_schema = 'br-llm-messages/5' AND author_ai_id IS NULL
            AND author_agent_id IS NOT NULL)))
);
ALTER TABLE threads.message ADD CONSTRAINT message_turn_kind_once_converted
    CHECK ((content_schema = 'br-llm-messages/5') = (turn_kind IS NOT NULL));
-- A user message is an input turn holding a human message.
ALTER TABLE threads.message ADD CONSTRAINT message_user_turn_is_an_input_turn
    CHECK (author_user_id IS NULL OR turn_kind IS DISTINCT FROM 'ai_turn');

-- The messages the upgrade still has to convert.
CREATE INDEX message_to_convert_idx ON threads.message (id)
    WHERE content_schema = 'br-llm-messages/3';

-- ── entries ─────────────────────────────────────────────────────────────────
-- One row per entry of a message's turn: the library's EntryRecord (its kind
-- and record state as the library's literals, its sender and author columns,
-- its observed time and its payload) at its position in the turn. A response
-- interrupted when its message closed is kept `interrupted`, for members only.
CREATE TABLE threads.message_entry (
    id          uuid    PRIMARY KEY,
    message_id  uuid    NOT NULL REFERENCES threads.message (id) ON DELETE CASCADE,
    position    integer NOT NULL CHECK (position >= 0),
    kind        text    NOT NULL CHECK (kind IN (
                    'human_message', 'agent_request', 'agent_mail', 'request_accepted',
                    'queue_position', 'task_started', 'task_completed', 'task_failed',
                    'model_response', 'runtime_tool_results', 'turn_closure')),
    state       text    NOT NULL CHECK (state IN ('producing', 'complete', 'interrupted')),
    sender_kind text    CHECK (sender_kind IN ('human', 'agent')),
    sender_id   uuid,
    author      text,
    observed_at text,
    payload     jsonb   NOT NULL,
    CONSTRAINT message_entry_place_key UNIQUE (message_id, position),
    CONSTRAINT message_entry_sender_together CHECK ((sender_kind IS NULL) = (sender_id IS NULL))
);
-- What the agent context reads of another agent's turn: its model responses and
-- its incoming entries, by kind.
CREATE INDEX message_entry_kind_idx ON threads.message_entry (message_id, kind);

-- ── blocks ──────────────────────────────────────────────────────────────────
-- One row per block of an entry, root or child: the library's BlockRecord (its
-- entry, its parent block for a child, its kind and record state as the
-- library's literals, the call a result answers, its observed time and its
-- payload — a thinking's replay material and a redacted thinking byte for byte).
-- `position` orders the blocks of a message as the library decomposes them: each
-- root followed by its children.
CREATE TABLE threads.entry_block (
    id          uuid    PRIMARY KEY,
    message_id  uuid    NOT NULL REFERENCES threads.message (id) ON DELETE CASCADE,
    entry_id    uuid    NOT NULL REFERENCES threads.message_entry (id) ON DELETE CASCADE,
    parent_id   uuid    REFERENCES threads.entry_block (id) ON DELETE CASCADE,
    position    integer NOT NULL CHECK (position >= 0),
    kind        text    NOT NULL CHECK (kind IN (
                    'text', 'image', 'structured_content', 'thinking', 'redacted_thinking',
                    'runtime_tool_call', 'runtime_tool_result', 'provider_tool_call',
                    'provider_tool_result', 'provider_tool_execution_record')),
    state       text    NOT NULL CHECK (state IN ('producing', 'complete', 'interrupted')),
    call_id     uuid,
    observed_at text,
    payload     jsonb   NOT NULL,
    CONSTRAINT entry_block_place_key UNIQUE (message_id, position)
);
-- What the agent context reads of another agent's response: its root text,
-- image and structured content blocks, by kind.
CREATE INDEX entry_block_entry_kind_idx ON threads.entry_block (entry_id, kind);
CREATE INDEX entry_block_parent_idx ON threads.entry_block (parent_id)
    WHERE parent_id IS NOT NULL;

-- ── annotations ─────────────────────────────────────────────────────────────
-- Threads' own blocks, after an entry of a message: not part of the library's
-- conversation, never sent to a runner. Their content is typed by their kind;
-- `position` orders the annotations after one entry.
CREATE TABLE threads.message_annotation (
    id             uuid    PRIMARY KEY,
    message_id     uuid    NOT NULL REFERENCES threads.message (id) ON DELETE CASCADE,
    after_entry_id uuid    NOT NULL REFERENCES threads.message_entry (id) ON DELETE CASCADE,
    position       integer NOT NULL CHECK (position >= 0),
    kind           text    NOT NULL CHECK (kind IN (
                       'ARTIFACT', 'THREAD_REFERENCE', 'JOB_REFERENCE', 'ERROR', 'WARNING')),
    content        jsonb   NOT NULL,
    CONSTRAINT message_annotation_place_key UNIQUE (after_entry_id, position)
);
CREATE INDEX message_annotation_message_idx ON threads.message_annotation (message_id);

-- ── identities ──────────────────────────────────────────────────────────────
-- Every id a message declares — its own (its turn's), its entries', its blocks'
-- (children included), its text blocks' citations', its annotations' — is used
-- once: the library refuses a conversation in which an id appears twice, so
-- Threads refuses (`KEY_REUSED`) any id another message already declared. The
-- key is the proof; a message's content and its identities are written and
-- removed together.
CREATE TABLE threads.message_identity (
    id         uuid PRIMARY KEY,
    message_id uuid NOT NULL REFERENCES threads.message (id) ON DELETE CASCADE
);
CREATE INDEX message_identity_message_idx ON threads.message_identity (message_id);
