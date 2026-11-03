-- Threads 0.7: the child threads of a thread are listed, the oldest first
-- (creation, then id), by keyset pages. Only child threads carry a parent.
--
-- Not CONCURRENTLY: a migration runs in a transaction, and `migrate` runs while
-- no pod serves (the rollout is `Recreate`), so the write lock the build takes
-- blocks no one.
CREATE INDEX thread_child_idx ON threads.thread (parent_thread_id, created_at, id)
    WHERE parent_thread_id IS NOT NULL;
