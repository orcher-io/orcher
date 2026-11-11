-- V066: find a workflow id's latest run with one index probe
--
-- Every RPC that names a run by workflow id resolves it to the latest run in
-- its namespace (and organization, when one is in scope), and the result
-- long-poll repeats that every few seconds per waiter. On the workflow_id
-- index alone each lookup fetched every run of the id, then filtered by
-- namespace and sorted by submitted_at. This index answers it in order, so
-- LIMIT 1 stops at the first row.
--
-- Organization is not a key column: the lookup's organization is optional,
-- and a workflow id rarely repeats across organizations within a namespace,
-- so filtering it on the rows read in order costs nothing.
--
-- It leads with workflow_id, so it serves everything the single-column index
-- did; that one is dropped rather than maintained on every write beside it.
-- CONCURRENTLY, as in V063.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_executions_latest_run
    ON workflow_executions (workflow_id, namespace, submitted_at DESC);

DROP INDEX CONCURRENTLY IF EXISTS idx_workflow_exec_workflow_id;
