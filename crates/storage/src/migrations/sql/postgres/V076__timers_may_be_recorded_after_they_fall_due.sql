-- V076: a timer may be recorded after it falls due.
--
-- A timer's fire_at is the engine's clock plus its duration, taken before the
-- transaction that records it; created_at is when the row is written. When
-- that transaction waits longer than the timer's duration (a 10 ms timer on a
-- busy engine), fire_at is already behind created_at, and the CHECK refused
-- the row, failing the whole workflow completion that scheduled it. A timer
-- that is due when it is recorded is not an error: the timer processor picks
-- up every pending timer with fire_at <= NOW(), so it fires at once.
ALTER TABLE timer_queue DROP CONSTRAINT IF EXISTS timer_queue_fire_at_future;
