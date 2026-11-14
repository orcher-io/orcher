-- V088: where in its journal the activation that scheduled a task read to
--
-- scheduled_through_entry: the claimed_through_entry (V087) of the activation
-- whose ScheduleTask command wrote the task's first attempt. The activation
-- carried the run's journal up to that entry and no further, so whether it
-- had seen the run's cancellation request is whether that request's journal
-- entry is at or before it. A cancellation stops exactly the tasks scheduled
-- by activations that had not seen the request; the cleanup scheduled by one
-- that had runs.
--
-- Before, that was judged by time: a task first scheduled before the request
-- was interrupted work. An activation handed out before the request but
-- answered after it scheduled tasks stamped after the request, and they ran
-- as cleanup; and the two stamps could compare wrongly across clocks.
--
-- NULL: not known, as for every row before this column, for a retry (judged
-- by its first attempt) and for a task an activation with no recorded
-- position scheduled. Those are still judged by time.
ALTER TABLE task_executions
    ADD COLUMN IF NOT EXISTS scheduled_through_entry BIGINT;

-- The run's cancellation request entry, looked up wherever a task is asked
-- whether the cancellation stops it: at its start, its heartbeats, its retry
-- and the sweep. A run has at most one such entry, so the index is a row per
-- cancelled run, where the journal's own index would scan the run's entries.
--
-- CONCURRENTLY, as in V042, V046, V047 and V049, so building it on a large
-- journal does not block the writes that go on meanwhile; the migration runner
-- executes this file outside a transaction, statement by statement.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_journal_cancel_requested
    ON workflow_execution_journal (execution_id, event_id)
    WHERE event_type = 'WorkflowExecutionCancelRequested';
