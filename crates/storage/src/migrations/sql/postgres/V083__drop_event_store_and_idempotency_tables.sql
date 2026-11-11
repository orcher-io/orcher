-- V083: drop the event store and idempotency tables
--
-- V011 and V012 created a Postgres event store (events, their snapshots, and
-- snapshot bookkeeping) and V013 a table of idempotency records. The engine
-- built the event store at startup but never wrote to it: what it would have
-- held, a run's history, is the journal. Nothing read or wrote the
-- idempotency records. The code that went with both is gone; this drops what
-- their migrations made, with the functions only they, or nothing, called.
-- Their migrations stay as they were (applied migrations are checked by
-- checksum).
--
-- No CASCADE: if anything else has come to depend on these objects, this
-- fails rather than dropping it too.

-- snapshot_metadata references snapshots; snapshots' trigger goes with it,
-- and then the function only that trigger called.
DROP TABLE IF EXISTS snapshot_metadata;
DROP TABLE IF EXISTS snapshots;
DROP FUNCTION IF EXISTS update_snapshot_metadata();
DROP FUNCTION IF EXISTS cleanup_old_snapshots(text, integer);
DROP FUNCTION IF EXISTS cleanup_snapshots_older_than(interval);

DROP TABLE IF EXISTS event_snapshots;
DROP TABLE IF EXISTS events;

DROP TABLE IF EXISTS execution_idempotency;

-- Functions nothing calls any more.
DROP FUNCTION IF EXISTS can_parent_close(character varying, character varying);
DROP FUNCTION IF EXISTS cleanup_expired_actor_state();
DROP FUNCTION IF EXISTS get_workflow_execution_stats(text);
