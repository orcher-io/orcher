-- orcher: outside a transaction
--
-- V055: stamp tasks, and their dispatches, with their workflow's organization
--
-- A task row was written without an organization, and so was the outbox row
-- that dispatches it and the matching-partition row that delivers it. Every
-- claim that hands a task to a worker is now scoped by organization, so an
-- unstamped row would reach only a worker acting for none. New rows are
-- stamped as they are written, and the claims fall back to the workflow's
-- organization where the workflow is on the same database; this stamps what
-- was written before, so a row whose workflow lives elsewhere is covered too.
--
-- A row whose workflow belongs to no organization keeps none, and so still
-- goes only to a worker acting for none, which is the one that can complete
-- it.
--
-- Only live rows are stamped: tasks still scheduled or running, and dispatches
-- still pending or being processed. Finished ones are never claimed again, and
-- rewriting a table's history would cost time and I/O for nothing. The
-- matching partitions hold only tasks waiting for delivery, and are stamped in
-- full.
--
-- Each shard database is migrated on its own (the migrate Job runs every
-- database in ORCHER_SHARD_DATABASE_URLS), so a row is stamped from the
-- workflow on its own database. A matching-partition row lives on the
-- coordination database, and one whose workflow is on another shard database
-- finds no workflow here and stays unstamped: a worker acting for an
-- organization does not claim it, and it is delivered only to a worker acting
-- for none. Rows written from now on are stamped as they are written, so this
-- concerns only deliveries waiting while the upgrade runs.
--
-- Rollout: an engine refuses to start against a schema missing a migration it
-- was built with, so new pods wait until the migrate Job has finished this,
-- while the pods already running keep serving.
--
-- In batches of a thousand, walked by primary key and each committed on its
-- own, as in V050, so a large table is never held in one long transaction.
-- The directive on the first line has the runner execute this outside a
-- transaction, which a COMMIT inside a DO block needs. Re-runnable: a stamped
-- row no longer matches.
DO $$
DECLARE
    last_id uuid := '00000000-0000-0000-0000-000000000000';
    batch uuid[];
BEGIN
    LOOP
        SELECT array_agg(id ORDER BY id) INTO batch
        FROM (
            SELECT id FROM task_executions
            WHERE id > last_id AND organization_id IS NULL
              AND status IN ('scheduled', 'started')
            ORDER BY id
            LIMIT 1000
        ) next_batch;
        EXIT WHEN batch IS NULL;
        UPDATE task_executions t
        SET organization_id = w.organization_id
        FROM workflow_executions w
        WHERE t.id = ANY(batch)
          AND w.id = t.workflow_id
          AND w.organization_id IS NOT NULL;
        last_id := batch[array_length(batch, 1)];
        COMMIT;
    END LOOP;
END $$;

DO $$
DECLARE
    last_id uuid := '00000000-0000-0000-0000-000000000000';
    batch uuid[];
BEGIN
    LOOP
        SELECT array_agg(task_id ORDER BY task_id) INTO batch
        FROM (
            SELECT task_id FROM outbox_queue
            WHERE task_id > last_id AND organization_id IS NULL
              AND task_type = 'dispatch_task'
              AND status IN ('pending', 'processing')
            ORDER BY task_id
            LIMIT 1000
        ) next_batch;
        EXIT WHEN batch IS NULL;
        UPDATE outbox_queue o
        SET organization_id = w.organization_id
        FROM workflow_executions w
        WHERE o.task_id = ANY(batch)
          AND w.id = o.workflow_id
          AND w.organization_id IS NOT NULL;
        last_id := batch[array_length(batch, 1)];
        COMMIT;
    END LOOP;
END $$;

DO $$
DECLARE
    last_id uuid := '00000000-0000-0000-0000-000000000000';
    batch uuid[];
BEGIN
    LOOP
        SELECT array_agg(task_id ORDER BY task_id) INTO batch
        FROM (
            SELECT task_id FROM matching_partitions
            WHERE task_id > last_id AND organization_id IS NULL
            ORDER BY task_id
            LIMIT 1000
        ) next_batch;
        EXIT WHEN batch IS NULL;
        UPDATE matching_partitions m
        SET organization_id = w.organization_id
        FROM workflow_executions w
        WHERE m.task_id = ANY(batch)
          AND w.id = m.workflow_id
          AND w.organization_id IS NOT NULL;
        last_id := batch[array_length(batch, 1)];
        COMMIT;
    END LOOP;
END $$;
