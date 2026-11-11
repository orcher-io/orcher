-- V042: index the column every task completion looks up by
--
-- Completing, failing, cancelling or heartbeating a task finds its row by
-- task_token, and nothing indexed task_token, so each of those calls was a
-- sequential scan of task_executions. The cost grew with every task ever run —
-- and for as long as data retention was not running, that was every task ever
-- run, unpruned. It stayed invisible because the table was small; it is the
-- kind of slowdown that arrives gradually and then all at once.
--
-- Partial: only rows that have been handed to a worker carry a token, so rows
-- still waiting to be dispatched do not pay for an entry. CONCURRENTLY so that
-- building it on a large table does not block the writes it is meant to speed
-- up; the migration runner executes CONCURRENTLY statements outside a
-- transaction for exactly this.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_executions_task_token
    ON task_executions (task_token)
    WHERE task_token IS NOT NULL;
