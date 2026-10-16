-- Threads 0.3: a message's blocks are read by their message — a message's
-- content (`blocks_of`), its removal on deletion, the cascade from a deleted
-- message, and the tool call ids of the messages of a branch, which every
-- close of a stream calling tools reads under its thread's lock. The two
-- unique place indexes are partial (a user message's blocks, an item's), so
-- neither serves a read by message alone.
CREATE INDEX message_block_message_idx ON threads.message_block (message_id);
