-- Threads 0.12: a thread's messages form one list, in the order they were
-- stored (created_at, then id). A message names no other message.
--
-- A thread that held branches keeps its active branch: the chain of parents
-- that ends at its most recent message (created_at, then id). For each message
-- of that branch, it also keeps the latest answer to it (created_at, then id)
-- of each agent whose answer to that message is not on the branch. Every other
-- message goes, such as an earlier version left by a retry or a regeneration.
-- Its entries, blocks, annotations, identities, 0.4 items and blocks, stream
-- watch and the read marks set on it go by cascade. Its artifacts are removed
-- here, each ending its fact log on `Removed` (reason `PRUNED_AT_UPGRADE`,
-- actor Threads itself). The ids a removed message held stay reserved in its
-- thread (threads.erased_identity, as for an erased message). Chunks of a
-- removed `STREAMING` message still held by the engine's accumulator are
-- deleted. A thread without branches keeps every message. Kept messages keep
-- their ids and dates. Child threads started from a removed message stay. The
-- facts already recorded stay as recorded: they are append-only and hold no
-- content.
--
-- An agent is the library's agent id of a converted message, or the free-text
-- author id of a message Threads 0.4 stored and the upgrade has not converted
-- yet (the conversion runs at `serve`, after the migrations).
--
-- An artifact's bytes live in object storage, out of reach of SQL. The blob
-- references of the artifacts removed here are queued in
-- threads.released_blob. Threads drains that queue once it serves
-- (slices::artifact::release), purging the bytes through the engine's blob
-- reader.
--
-- All or nothing: the migration runs in one transaction under the migrator's
-- advisory lock, so pods that start together apply it once. A failure leaves
-- every message and the parent column as they were, and the next start applies
-- it again. `migrate` runs while no pod serves (the rollout is Recreate). The
-- explicit locks only keep the kept set and the deletions on one snapshot.

LOCK TABLE threads.thread, threads.message, threads.artifact IN EXCLUSIVE MODE;

-- No cycle: a parent exists before its child is stored (the same-thread
-- foreign key), and a message's parent never changes.
CREATE TEMPORARY TABLE kept_message ON COMMIT DROP AS
WITH RECURSIVE tip AS (
    SELECT DISTINCT ON (thread_id) id
    FROM threads.message
    ORDER BY thread_id, created_at DESC, id DESC
), branch AS (
    SELECT m.id, m.parent_message_id
    FROM threads.message m JOIN tip ON tip.id = m.id
    UNION ALL
    SELECT p.id, p.parent_message_id
    FROM threads.message p JOIN branch b ON p.id = b.parent_message_id
), answer AS (
    SELECT m.id, m.parent_message_id, m.created_at,
           COALESCE(m.author_agent_id::text, m.author_ai_id) AS agent
    FROM threads.message m
    WHERE m.parent_message_id IS NOT NULL
      AND (m.author_agent_id IS NOT NULL OR m.author_ai_id IS NOT NULL)
), latest_off_branch AS (
    SELECT DISTINCT ON (a.parent_message_id, a.agent) a.id
    FROM answer a
    JOIN branch b ON b.id = a.parent_message_id
    WHERE NOT EXISTS (
        SELECT 1 FROM answer o JOIN branch ob ON ob.id = o.id
        WHERE o.parent_message_id = a.parent_message_id AND o.agent = a.agent
    )
    ORDER BY a.parent_message_id, a.agent, a.created_at DESC, a.id DESC
)
SELECT id FROM branch
UNION
SELECT id FROM latest_off_branch;

ALTER TABLE kept_message ADD PRIMARY KEY (id);

CREATE TEMPORARY TABLE pruned_message ON COMMIT DROP AS
SELECT m.id, m.thread_id, m.status
FROM threads.message m
WHERE NOT EXISTS (SELECT 1 FROM kept_message k WHERE k.id = m.id);

ALTER TABLE pruned_message ADD PRIMARY KEY (id);

-- The ids of erased messages, their entries, blocks and annotations: ids only,
-- never content. An id is used once in its thread, for the life of the thread
-- (the rows go with it). Filled by `message.erase`, and here by the messages
-- the upgrade removes.
CREATE TABLE threads.erased_identity (
    id         uuid PRIMARY KEY,
    thread_id  uuid NOT NULL REFERENCES threads.thread (id) ON DELETE CASCADE,
    message_id uuid NOT NULL
);

CREATE INDEX erased_identity_thread_idx ON threads.erased_identity (thread_id);

-- A converted message's identities name its own id and every id of its
-- content; a message Threads 0.4 stored and the upgrade has not converted yet
-- holds its id and its 0.4 block ids.
INSERT INTO threads.erased_identity (id, thread_id, message_id)
SELECT i.id, p.thread_id, p.id
FROM pruned_message p JOIN threads.message_identity i ON i.message_id = p.id
UNION
SELECT p.id, p.thread_id, p.id FROM pruned_message p
UNION
SELECT b.id, p.thread_id, p.id
FROM pruned_message p JOIN threads.message_block b ON b.message_id = p.id
ON CONFLICT (id) DO NOTHING;

-- The blob references whose bytes Threads purges once it serves. A reference
-- is queued once: purging it twice is harmless, and a reference some artifact
-- still holds is never purged.
CREATE TABLE threads.released_blob (
    blob_ref    uuid        PRIMARY KEY,
    released_at timestamptz NOT NULL
);

CREATE TEMPORARY TABLE pruned_artifact ON COMMIT DROP AS
SELECT a.id, a.blob_ref, a.version
FROM threads.artifact a JOIN pruned_message p ON p.id = a.message_id;

INSERT INTO threads.released_blob (blob_ref, released_at)
SELECT blob_ref, now() FROM pruned_artifact;

-- A UUIDv7 for each fact this migration records (PostgreSQL 16 has no
-- generator of its own): the time in milliseconds over a random UUID, with
-- the version bits set to 7.
CREATE FUNCTION pg_temp.uuid_v7() RETURNS uuid LANGUAGE sql VOLATILE AS $$
    SELECT encode(
        set_bit(set_bit(
            overlay(uuid_send(gen_random_uuid())
                    PLACING substring(int8send(
                        (extract(epoch FROM clock_timestamp()) * 1000)::bigint) FROM 3)
                    FROM 1 FOR 6),
            52, 1), 53, 1),
        'hex')::uuid
$$;

-- Each removed artifact's `Removed` fact, numbered after its last fact, at the
-- version after its last change, by Threads itself, under one correlation id.
CREATE TEMPORARY TABLE upgrade_run ON COMMIT DROP AS
SELECT pg_temp.uuid_v7() AS correlation_id;

INSERT INTO threads.fact (id, noun, key, seq, version, event_type, payload, actor_id,
                          actor_kind, acting_user_id, impersonator_id, correlation_id,
                          causation_id, occurred_at)
SELECT pg_temp.uuid_v7(), 'artifact', to_jsonb(a.id::text),
       COALESCE((SELECT max(f.seq) FROM threads.fact f
                 WHERE f.noun = 'artifact' AND f.key = to_jsonb(a.id::text)), 0) + 1,
       (a.version + 1)::integer, 'Removed', '{"reason": "PRUNED_AT_UPGRADE"}'::jsonb,
       '01997000-7468-7265-6164-730000000001', 'service', NULL, NULL,
       (SELECT correlation_id FROM upgrade_run), NULL, now()
FROM pruned_artifact a;

-- Before their messages: artifact.message_id is ON DELETE RESTRICT.
DELETE FROM threads.artifact a USING pruned_artifact p WHERE a.id = p.id;

-- The chunks the engine's accumulator still holds for a removed message (only
-- a `STREAMING` one has any: a closed stream's chunks went with its seal),
-- under the stream's key, the message id as JSON. The same rows the engine's
-- seal deletes; no pod serves while `migrate` runs. The engine creates its
-- own schema: a database that only ever received these migrations (the one
-- the schema document is drawn from) has no such table and no chunk to drop.
DO $$
BEGIN
    IF to_regclass('service_engine.accumulator_chunk') IS NOT NULL THEN
        DELETE FROM service_engine.accumulator_chunk c
        USING pruned_message p
        WHERE c.accumulator IN ('turn_content', 'message_content')
          AND c.key = to_jsonb(p.id::text);
    END IF;
END
$$;

-- Before the removed messages: one may still be the parent of another.
ALTER TABLE threads.message DROP CONSTRAINT message_parent_fkey;

DELETE FROM threads.message m USING pruned_message p WHERE m.id = p.id;

DROP INDEX threads.message_parent_idx;

ALTER TABLE threads.message DROP COLUMN parent_message_id;
