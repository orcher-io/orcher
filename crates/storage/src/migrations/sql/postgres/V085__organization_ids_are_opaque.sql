-- V085: the engine's tables no longer reference the account tables
--
-- An organization id on a run, task, key or schedule is a tenancy key the
-- engine compares and never looks up. Yet every table that carries one had a
-- foreign key to `organizations`, and api_keys.user_id one to `users`: tables
-- the engine does not own and never writes, which belong to whatever manages
-- accounts (the cloud, or nothing at all on a self-hosted engine). So nothing
-- could be stored for an organization that table did not hold: not a tenant
-- named by an identity provider's token claim, not a shard database's rows
-- (each shard has its own, empty, organizations table).
--
-- Every key was ON DELETE CASCADE. Organizations and users are deleted softly
-- (their rows stay), so no cascade has fired; a deployment that hard-deletes
-- an organization now removes its engine data itself.
--
-- The keys are found by table and referenced table rather than by name, so a
-- database whose constraints were named otherwise is handled the same. Only
-- the engine's own tables are touched: the account tables keep the keys
-- between themselves. Dropping a key is a catalog change, but it locks the
-- table briefly; a lock not had within five seconds fails the migration,
-- which is retried at the next start, rather than stalling the engines still
-- serving.
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    fk record;
BEGIN
    FOR fk IN
        SELECT c.conrelid::regclass AS tbl, c.conname
        FROM pg_constraint c
        WHERE c.contype = 'f'
          AND c.confrelid IN (to_regclass('organizations'), to_regclass('users'))
          AND c.conrelid IN (
              to_regclass('workflow_executions'), to_regclass('workflow_execution_logs'),
              to_regclass('workflow_execution_metrics'), to_regclass('workflow_execution_journal'),
              to_regclass('child_workflow_executions'), to_regclass('task_executions'),
              to_regclass('execution_log'), to_regclass('outbox_queue'),
              to_regclass('timer_queue'), to_regclass('dead_letter_queue'),
              to_regclass('schedules'), to_regclass('schedule_executions'),
              to_regclass('schedule_instances'), to_regclass('actor_state'),
              to_regclass('batch_operations'), to_regclass('task_queue_releases'),
              to_regclass('api_keys')
          )
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk.tbl, fk.conname);
    END LOOP;
END
$$;
