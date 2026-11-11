-- V050: find a scheduled task that was never handed to the outbox
--
-- A task row and its outbox dispatch used to be written in two transactions.
-- An engine that stopped between them left a row that was scheduled and would
-- never run: nothing delivers a task but its outbox row, the ScheduleTask the
-- workflow re-emits on every replay found the row in flight and did nothing,
-- and the workflow waited on it for good. The two are now written together,
-- the row marked by last_dispatched_at, and a sweep dispatches whatever an
-- older engine left behind.
--
-- The index holds only first attempts still unmarked, which the sweep reads
-- in scheduled_at order between a lower and an upper bound.
--
-- CONCURRENTLY, as in V042 and V046 to V049, so building it on a large table
-- does not block the writes that go on meanwhile; the migration runner
-- executes this file statement by statement, outside a transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_exec_undispatched
    ON task_executions (scheduled_at)
    WHERE status = 'scheduled' AND attempt_number = 1 AND last_dispatched_at IS NULL;

-- Mark every first attempt already written that has no business in the sweep.
--
-- A first attempt from before this change was dispatched without being
-- marked, and its outbox row is the only record that it was. Data retention
-- deletes completed outbox rows, so a task no worker served within that
-- period would, once its row is gone, look exactly like one never dispatched,
-- and be dispatched again. Marked here with the time its outbox row was
-- written, it is out of the sweep for good, whatever retention later deletes.
-- (Rows past the retention window are also kept out by the sweep's own
-- bound; this covers the rows inside it.)
--
-- A first attempt still scheduled for a workflow that has finished will never
-- run and must never be dispatched. It is marked too, which takes it out of
-- the index — the mark means "no dispatch is to be made for this row", which
-- is as true of it as of a row that was dispatched.
--
-- What remains unmarked is a first attempt of a running workflow with no
-- outbox row: the rows this change exists to repair.
--
-- The key is the one the coordinator has given a first attempt's dispatch
-- since attempt numbers were added to it (`task_dispatch_key`).
--
-- In batches, each committed on its own — the runner executes this outside a
-- transaction — so a table with many such rows is never locked in one long
-- transaction. Re-runnable: a marked row no longer matches.
DO $$
DECLARE
    marked integer;
BEGIN
    LOOP
        UPDATE task_executions t
        SET last_dispatched_at = COALESCE(c.dispatched_at, NOW())
        FROM (
            SELECT t2.id, o.created_at AS dispatched_at
            FROM task_executions t2
            LEFT JOIN outbox_queue o
                   ON o.idempotency_key = t2.workflow_id::text || ':' || t2.execution_id::text
                                          || ':dispatch_task:' || t2.task_id || ':1'
            WHERE t2.status = 'scheduled'
              AND t2.attempt_number = 1
              AND t2.last_dispatched_at IS NULL
              AND (
                  o.task_id IS NOT NULL
                  OR EXISTS (
                      SELECT 1 FROM workflow_executions w
                      WHERE w.id = t2.workflow_id
                        AND w.status IN ('completed', 'failed', 'cancelled', 'terminated', 'timed_out')
                  )
              )
            LIMIT 1000
        ) c
        WHERE t.id = c.id
          AND t.last_dispatched_at IS NULL;
        GET DIAGNOSTICS marked = ROW_COUNT;
        COMMIT;
        EXIT WHEN marked = 0;
    END LOOP;
END $$;
