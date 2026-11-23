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
-- here. A thread without branches keeps every message. Kept messages keep
-- their ids and dates. Child threads started from a removed message stay.
-- Facts are append-only and hold no content: they stay as recorded, and none is
-- added.
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

-- The blob references whose bytes Threads purges once it serves. A reference
-- is queued once: purging it twice is harmless, and a reference some artifact
-- still holds is never purged.
CREATE TABLE threads.released_blob (
    blob_ref    uuid        PRIMARY KEY,
    released_at timestamptz NOT NULL
);

INSERT INTO threads.released_blob (blob_ref, released_at)
SELECT a.blob_ref, now()
FROM threads.artifact a
WHERE a.message_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM kept_message k WHERE k.id = a.message_id);

-- Before their messages: artifact.message_id is ON DELETE RESTRICT.
DELETE FROM threads.artifact a
WHERE a.message_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM kept_message k WHERE k.id = a.message_id);

-- Before the removed messages: one may still be the parent of another.
ALTER TABLE threads.message DROP CONSTRAINT message_parent_fkey;

DELETE FROM threads.message m
WHERE NOT EXISTS (SELECT 1 FROM kept_message k WHERE k.id = m.id);

DROP INDEX threads.message_parent_idx;

ALTER TABLE threads.message DROP COLUMN parent_message_id;
