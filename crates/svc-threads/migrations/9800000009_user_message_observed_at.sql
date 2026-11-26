-- Threads 0.13: a user message's turn carries, as the library's observed time
-- of the turn, the time Threads stored it: its created_at. A user message
-- stored before carries none; it takes its created_at here, written as
-- Threads writes it (UTC, to the microsecond, then `Z`), the value the
-- library's ObservedAt accepts. The conversion of a message stored by 0.4 or
-- earlier (at `serve`, after the migrations) leaves observed_at as it is. An
-- AI message keeps the time its writer gave its turn, or none. The facts
-- already recorded stay as recorded: they hold no time of the turn.
UPDATE threads.message
   SET observed_at = to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
 WHERE author_user_id IS NOT NULL
   AND observed_at IS NULL;
