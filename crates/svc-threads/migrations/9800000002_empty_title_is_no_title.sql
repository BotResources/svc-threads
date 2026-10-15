-- Threads 0.2: a title holds 1 to 200 characters (`TITLE_EMPTY`). Threads 0.1
-- accepted an empty title; the load path validates the stored title against the
-- value object, so a row left at '' would be unreadable. An empty title was
-- never a title: such a thread becomes untitled. Data only, no schema change.
UPDATE threads.thread SET title = NULL WHERE title = '';
