-- Threads 0.4: a user message is edited in place by its author, and a thread
-- is resolved and reopened.
--
-- `edited_at` is absent until the author first edits the message, then the
-- date of its latest edit; the edit also raises `version`. Only a user message
-- is edited, and only while it is not deleted — an edit after which the
-- message is deleted keeps its date.
ALTER TABLE threads.message
    ADD COLUMN edited_at timestamptz,
    ADD CONSTRAINT message_only_a_user_message_is_edited
        CHECK (edited_at IS NULL OR author_user_id IS NOT NULL);

-- `resolved_by` and `resolved_at` are set together by a resolution and cleared
-- together by a reopening. A resolved thread is still `OPEN` — resolving is not
-- closing — and closing it keeps its resolution as it was, so a terminal
-- thread may hold one. `resolved_by` is a user id, with no foreign key to
-- threads.known_users, like every user id of the schema.
ALTER TABLE threads.thread
    ADD COLUMN resolved_by uuid,
    ADD COLUMN resolved_at timestamptz,
    ADD CONSTRAINT thread_resolution_together
        CHECK ((resolved_by IS NULL) = (resolved_at IS NULL));
