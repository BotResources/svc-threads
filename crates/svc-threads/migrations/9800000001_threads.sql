-- Threads' own schema. The engine owns `service_engine` beside it (its outbox,
-- its accumulated-lane chunks and seals, its blob references); Threads never
-- writes it. This set is declared to the engine as the migration set of the
-- `threads` schema (see src/boot.rs), in band 9_800_000_001..=9_800_999_999.
CREATE SCHEMA threads;

-- Soft EDA throughout: each object's row is its current state and `version` the
-- number of its last change, from 1; every change is also a fact in
-- threads.fact, written in the same transaction, `seq` following `version`.
-- Statuses and kinds are stored as their published literal (contract-threads),
-- read back through a total mapping. User ids carry no foreign key to
-- threads.known_users: a user who leaves the roster keeps their row there, and
-- the references stay whole whatever the mirror does.

-- The local copy of Identity's roster, projected by the engine's directory
-- mirror from the `identity/users/…` keys of the PUBLISHED_LANGUAGE bucket. A
-- user who leaves the roster is kept, `in_roster = false`, with their last
-- name. `disabled_at` set means deactivated.
CREATE TABLE threads.known_users (
    user_id      uuid        PRIMARY KEY,
    display_name text        NOT NULL,
    disabled_at  timestamptz,
    in_roster    boolean     NOT NULL
);

-- A thread. `parent_thread_id` and `origin_block_id` are set together at the
-- creation of a child thread and never change; no foreign key — a child
-- survives its parent. `last_activity_at` is the date of the latest message, or
-- the creation date while there is none: the order of a member's list.
CREATE TABLE threads.thread (
    id               uuid        PRIMARY KEY,
    owner_id         uuid,
    title            text        CHECK (char_length(title) <= 200),
    parent_thread_id uuid,
    origin_block_id  uuid,
    status           text        NOT NULL
                                 CHECK (status IN ('OPEN', 'COMPLETED', 'FAILED', 'CANCELLED')),
    created_at       timestamptz NOT NULL,
    last_activity_at timestamptz NOT NULL,
    version          bigint      NOT NULL CHECK (version >= 1),
    CONSTRAINT thread_origin_together
        CHECK ((parent_thread_id IS NULL) = (origin_block_id IS NULL))
);

-- The threads a user owns: their deletion when the owner leaves the roster.
CREATE INDEX thread_owner_idx ON threads.thread (owner_id) WHERE owner_id IS NOT NULL;

-- The members of a thread: users only (a group is expanded by the commanding
-- service). The owner is one of them. Who added a member, and when, is in the
-- MembersAdded fact. Part of the thread's state, written by its store.
CREATE TABLE threads.thread_member (
    thread_id uuid NOT NULL REFERENCES threads.thread (id) ON DELETE CASCADE,
    user_id   uuid NOT NULL,
    PRIMARY KEY (thread_id, user_id)
);

-- The threads a user is a member of: their list, their visibility cohort.
CREATE INDEX thread_member_user_idx ON threads.thread_member (user_id);

-- The resources of a thread: one named list of ids per key, opaque to Threads.
-- An empty key disappears. Part of the thread's state; never published.
CREATE TABLE threads.thread_resource (
    thread_id uuid   NOT NULL REFERENCES threads.thread (id) ON DELETE CASCADE,
    key       text   NOT NULL CHECK (key <> ''),
    ids       text[] NOT NULL CHECK (cardinality(ids) > 0),
    PRIMARY KEY (thread_id, key)
);

-- A thread's title Job: at most one per thread, ever. `state` is RUNNING while
-- the Job is the thread's active title Job; any other state is final, and a
-- thread whose Job ended without a title stays untitled.
CREATE TABLE threads.title_job (
    thread_id  uuid        PRIMARY KEY REFERENCES threads.thread (id) ON DELETE CASCADE,
    job_id     uuid        NOT NULL UNIQUE,
    state      text        NOT NULL
                           CHECK (state IN ('RUNNING', 'DONE', 'FAILED', 'REJECTED', 'CANCELLED')),
    created_at timestamptz NOT NULL,
    ended_at   timestamptz,
    CONSTRAINT title_job_ended_when_final CHECK ((state = 'RUNNING') = (ended_at IS NULL))
);

-- A message. Exactly one author: a user (a member of the thread when the
-- message is stored), or an AI given by the storing service as an id and a name
-- Threads does not read. A user message is always stored complete. A deleted
-- message stays in the tree, without content, so that its forks remain
-- readable; only a user message is ever deleted. The impersonator of an author
-- is in the Created fact, never here.
CREATE TABLE threads.message (
    id                uuid        PRIMARY KEY,
    thread_id         uuid        NOT NULL REFERENCES threads.thread (id) ON DELETE CASCADE,
    parent_message_id uuid,
    author_user_id    uuid,
    author_ai_id      text,
    author_ai_name    text,
    job_id            uuid,
    status            text        NOT NULL
                                  CHECK (status IN ('STREAMING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    deleted           boolean     NOT NULL,
    created_at        timestamptz NOT NULL,
    version           bigint      NOT NULL CHECK (version >= 1),
    -- The target of the same-thread parent reference below.
    CONSTRAINT message_thread_id_key UNIQUE (thread_id, id),
    -- The parent is a message of the same thread.
    CONSTRAINT message_parent_fkey FOREIGN KEY (thread_id, parent_message_id)
        REFERENCES threads.message (thread_id, id),
    CONSTRAINT message_one_author CHECK (
        (author_user_id IS NOT NULL AND author_ai_id IS NULL AND author_ai_name IS NULL)
        OR (author_user_id IS NULL AND author_ai_id <> '' AND author_ai_name <> '')
    ),
    CONSTRAINT message_user_input_is_complete
        CHECK (author_user_id IS NULL OR status = 'COMPLETED'),
    CONSTRAINT message_ai_is_never_deleted CHECK (NOT deleted OR author_user_id IS NOT NULL)
);

-- A thread's messages in order: its pages, its active branch's tip.
CREATE INDEX message_thread_created_idx ON threads.message (thread_id, created_at, id);
-- A message's answers: its forks.
CREATE INDEX message_parent_idx ON threads.message (parent_message_id)
    WHERE parent_message_id IS NOT NULL;
-- A user's messages: the audience of a change of their name.
CREATE INDEX message_author_user_idx ON threads.message (author_user_id)
    WHERE author_user_id IS NOT NULL;
-- The messages still streaming: the stream window's sweep.
CREATE INDEX message_streaming_idx ON threads.message (created_at) WHERE status = 'STREAMING';

-- The items of an AI message, in the library's order: a STEP keeps what the
-- library gives for it (stop reason, optional usage, optional model — not read
-- by Threads), as the library's JSON; a TOOL_RESULTS item holds tool results.
-- An item's status follows its message's; a completed item never changes.
CREATE TABLE threads.message_item (
    message_id  uuid    NOT NULL REFERENCES threads.message (id) ON DELETE CASCADE,
    index       integer NOT NULL CHECK (index >= 0),
    kind        text    NOT NULL CHECK (kind IN ('STEP', 'TOOL_RESULTS')),
    status      text    NOT NULL
                        CHECK (status IN ('STREAMING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    stop_reason jsonb,
    usage       jsonb,
    model       text,
    PRIMARY KEY (message_id, index),
    CONSTRAINT message_item_step_fields CHECK (
        kind = 'STEP' OR (stop_reason IS NULL AND usage IS NULL AND model IS NULL)
    )
);

-- The blocks of a message: directly under a user message (`item_index` NULL),
-- or under an item of an AI message. A library block's content is the library's
-- JSON for it — strings, a thinking's signature and a redacted thinking among
-- them, are kept byte for byte (jsonb keeps no key order and no whitespace, and
-- neither is content); a Threads block's content is typed by its kind.
CREATE TABLE threads.message_block (
    id         uuid    PRIMARY KEY,
    message_id uuid    NOT NULL REFERENCES threads.message (id) ON DELETE CASCADE,
    item_index integer,
    index      integer NOT NULL CHECK (index >= 0),
    kind       text    NOT NULL CHECK (kind IN (
                   'TEXT', 'THINKING', 'REDACTED_THINKING', 'STRUCTURED', 'TOOL_CALL',
                   'TOOL_RESULT', 'ARTIFACT', 'THREAD_REFERENCE', 'JOB_REFERENCE', 'CITATION',
                   'ERROR', 'WARNING')),
    content    jsonb   NOT NULL,
    -- A block of an item belongs to an item of its message.
    CONSTRAINT message_block_item_fkey FOREIGN KEY (message_id, item_index)
        REFERENCES threads.message_item (message_id, index) ON DELETE CASCADE
);

-- One block per place: (message, index) for a user message's blocks, (message,
-- item, index) for an item's. Two partial indexes rather than NULLS NOT
-- DISTINCT, which PostgreSQL 14 does not have.
CREATE UNIQUE INDEX message_block_user_place_key ON threads.message_block (message_id, index)
    WHERE item_index IS NULL;
CREATE UNIQUE INDEX message_block_item_place_key
    ON threads.message_block (message_id, item_index, index)
    WHERE item_index IS NOT NULL;

-- An artifact: a file a runner produced for a thread, its bytes in Threads'
-- object storage behind the engine's blob reference (`blob_ref`, an
-- `service_engine.blob` row). PENDING at the upload request, ATTACHED once a
-- message of the same thread names it with its bytes present at the declared
-- digest. It lives as long as its message.
--
-- No cascade from the thread or the message, on purpose: the engine releases a
-- blob only when the aggregate holding its reference is deleted through the
-- pipeline (`cx.delete`), so a cascade would drop the row and leave its bytes in
-- storage for good. Deleting a thread or a message deletes its artifacts first,
-- through their own store; the restricting keys make a forgotten one fail loud.
CREATE TABLE threads.artifact (
    id         uuid        PRIMARY KEY,
    thread_id  uuid        NOT NULL REFERENCES threads.thread (id) ON DELETE RESTRICT,
    message_id uuid        REFERENCES threads.message (id) ON DELETE RESTRICT,
    name       text        NOT NULL CHECK (name <> ''),
    media_type text        NOT NULL CHECK (media_type <> ''),
    size       bigint      NOT NULL CHECK (size >= 0),
    sha256     text        NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    blob_ref   uuid        NOT NULL UNIQUE,
    status     text        NOT NULL CHECK (status IN ('PENDING', 'ATTACHED')),
    created_at timestamptz NOT NULL,
    version    bigint      NOT NULL CHECK (version >= 1),
    CONSTRAINT artifact_attached_to_a_message
        CHECK ((status = 'ATTACHED') = (message_id IS NOT NULL))
);

CREATE INDEX artifact_thread_idx ON threads.artifact (thread_id);
CREATE INDEX artifact_message_idx ON threads.artifact (message_id) WHERE message_id IS NOT NULL;

-- The watch over a STREAMING message's writer: how far its stream had come
-- (`seen_through`, the last contiguous sequence folded, NULL before the first
-- chunk) when the watch last looked, and since when that has not moved. The
-- watch fails the message once its writer pushed nothing for the idle window.
-- It goes with its message's stream: a closed message has none.
CREATE TABLE threads.stream_watch (
    message_id   uuid        PRIMARY KEY REFERENCES threads.message (id) ON DELETE CASCADE,
    seen_through bigint      CHECK (seen_through >= 0),
    since        timestamptz NOT NULL
);

-- A member's read mark: the reader's own state, with no fact and no thread
-- version. It goes with its member (removing a member removes their mark) and
-- names a message of the same thread.
CREATE TABLE threads.read_mark (
    thread_id            uuid NOT NULL,
    user_id              uuid NOT NULL,
    last_read_message_id uuid NOT NULL,
    PRIMARY KEY (thread_id, user_id),
    CONSTRAINT read_mark_member_fkey FOREIGN KEY (thread_id, user_id)
        REFERENCES threads.thread_member (thread_id, user_id) ON DELETE CASCADE,
    CONSTRAINT read_mark_message_fkey FOREIGN KEY (thread_id, last_read_message_id)
        REFERENCES threads.message (thread_id, id) ON DELETE CASCADE
);

-- The one fact table of the service: the facts of every object of Threads
-- (nouns thread, message, artifact). A fact never carries the content of a
-- message — only its kinds, positions, author and status — and never a name:
-- deleting a message leaves none of its content anywhere, and a change of name
-- shows everywhere. A fact survives its object (no foreign key): the Deleted
-- fact of a thread covers its members, resources, messages, items, blocks,
-- artifacts and read marks, and says why the thread was deleted.
--
-- `actor_id` / `actor_kind` name who sent the gesture (the commanding service's
-- account, a runner, Threads itself); `acting_user_id` the user whose gesture
-- led the commanding service to send it; `impersonator_id` the impersonator of
-- a message's author.
CREATE TABLE threads.fact (
    id              uuid        PRIMARY KEY,
    noun            text        NOT NULL CHECK (noun IN ('thread', 'message', 'artifact')),
    key             jsonb       NOT NULL,
    seq             bigint      NOT NULL CHECK (seq >= 1),
    version         integer     NOT NULL CHECK (version >= 1),
    event_type      text        NOT NULL CHECK (event_type <> ''),
    payload         jsonb       NOT NULL,
    actor_id        uuid        NOT NULL,
    actor_kind      text        NOT NULL CHECK (actor_kind IN ('human', 'service')),
    acting_user_id  uuid,
    impersonator_id uuid,
    correlation_id  uuid        NOT NULL,
    causation_id    uuid,
    occurred_at     timestamptz NOT NULL,
    -- Two writers of one object would hand the same seq: a lost update cannot
    -- hide. The index also serves an object's history.
    UNIQUE (noun, key, seq)
);

-- What an erase rewrites: the facts a person made, led or impersonated.
CREATE INDEX fact_actor_idx ON threads.fact (actor_id);
CREATE INDEX fact_acting_user_idx ON threads.fact (acting_user_id)
    WHERE acting_user_id IS NOT NULL;
CREATE INDEX fact_impersonator_idx ON threads.fact (impersonator_id)
    WHERE impersonator_id IS NOT NULL;
