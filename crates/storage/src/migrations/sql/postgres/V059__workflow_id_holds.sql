-- V059: a workflow id names at most one open execution in a namespace
--
-- StartWorkflow accepted a second start of a workflow id whose execution was
-- still running, and recorded a second execution for it. Checking first and
-- inserting after cannot fix that on its own: two starts on two engines both
-- check, both find nothing, and both insert. The unique index below decides
-- it instead, inside the insert.
--
-- Not every execution with a workflow id holds it. A child carries the id
-- its parent correlates it by ("child_0"), which is unique per parent, not per
-- namespace: two parents each running a child_0 is correct. holds_workflow_id
-- marks the executions whose id is a namespace-wide name — a start's, and the
-- successors that carry it on (a retry, a continue-as-new, a reset) — and only
-- those are held to one open execution per (organization, namespace, id).
-- An unscoped deployment has no organization: COALESCE makes its NULLs one
-- scope rather than every row its own.
--
-- Open is what the engine treats as not yet over: anything not in a terminal
-- state, and a failure whose retry is still to come. A failure waiting for its
-- retry holds its id, so a new start cannot slip in before the retry and leave
-- two runs of one workflow.
--
-- The column defaults to FALSE, so a row written by an engine that predates it
-- — while a deploy rolls out — holds nothing and cannot be refused by it.
-- Existing open executions that hold their id are marked below. Where a
-- namespace already has more than one open execution of an id (the defect
-- this fixes), only the newest is marked: the others keep running, and are
-- simply not what a new start is checked against. Nothing is terminated.
--
-- Run outside a transaction, statement by statement: the column is added
-- without a rewrite, the backfill touches open executions only, and the index
-- is built CONCURRENTLY, so writes carry on throughout. A build that fails
-- leaves an invalid index, which the runner refuses to record, and dropping it
-- first makes a re-run rebuild it rather than skip it.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS holds_workflow_id BOOLEAN NOT NULL DEFAULT FALSE;

WITH open_top_level AS (
    SELECT e.id,
           row_number() OVER (
               PARTITION BY COALESCE(e.organization_id, '00000000-0000-0000-0000-000000000000'::uuid),
                            e.namespace, e.workflow_id
               ORDER BY e.holds_workflow_id DESC, e.submitted_at DESC, e.id DESC
           ) AS newest
    FROM workflow_executions e
    WHERE e.workflow_id IS NOT NULL
      AND (e.status NOT IN ('completed', 'failed', 'cancelled', 'terminated',
                            'timed_out', 'restarted_fresh', 'reset')
           OR (e.status = 'failed' AND e.retries_settled_at IS NULL
               AND e.retry_count < e.max_retries))
      -- Not a child: its parent's tracking row names its current run.
      AND NOT EXISTS (
          SELECT 1 FROM child_workflow_executions c
          WHERE c.child_execution_id = e.execution_id
      )
      -- A first run, or a successor of one with the same id. A child's chain
      -- is rooted in its parent's, whose id is not its own.
      AND (e.parent_execution_id IS NULL
           OR EXISTS (
               SELECT 1 FROM workflow_executions r
               WHERE r.execution_id = e.root_execution_id
                 AND r.workflow_id = e.workflow_id
                 AND r.namespace = e.namespace
           ))
)
UPDATE workflow_executions w
SET holds_workflow_id = TRUE
FROM open_top_level o
WHERE w.id = o.id
  AND o.newest = 1
  AND NOT w.holds_workflow_id;

DROP INDEX CONCURRENTLY IF EXISTS uq_workflow_executions_open_workflow_id;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_workflow_executions_open_workflow_id
    ON workflow_executions (
        (COALESCE(organization_id, '00000000-0000-0000-0000-000000000000'::uuid)),
        namespace,
        workflow_id
    )
    WHERE holds_workflow_id
      AND (status NOT IN ('completed', 'failed', 'cancelled', 'terminated',
                          'timed_out', 'restarted_fresh', 'reset')
           OR (status = 'failed' AND retries_settled_at IS NULL
               AND retry_count < max_retries));
