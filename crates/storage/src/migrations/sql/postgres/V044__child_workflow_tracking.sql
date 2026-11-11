-- V044: a child's tracking row names the run that is actually executing it
--
-- child_workflow_executions.child_execution_id was a random id minted beside
-- the real one: the execution itself was created with an id of its own, so the
-- two never matched and nothing could get from a finished run back to the row
-- that says who its parent is. The engine now writes the real id when it
-- starts a child, and moves it along to the successor when the child is
-- retried, continued as new, or reset. This brings the rows written before
-- that into line. The index the lookups need is built by V046, outside a
-- transaction.
--
-- A row is matched to the execution its parent started under the same
-- business id, then followed along the successor chain to the latest run.
-- A successor keeps the workflow_id and points back at its predecessor
-- through parent_execution_id; the predecessor is left 'restarted_fresh' or
-- 'reset'. The most recently created candidate is taken at each step, which
-- is the successor: it is only created once its predecessor has ended, after
-- anything that predecessor started itself.
--
-- `state` is deliberately left alone. Rows whose run has already finished are
-- delivered to their parent, once, by the engine's reconciliation sweep; doing
-- it here would mark them settled without the parent ever being told.
--
-- Rows with no matching execution (a start that was recorded but never
-- created) keep their id: replaying the parent's start creates the execution
-- under whatever id the row holds.

WITH RECURSIVE chain AS (
    SELECT c.id AS tracking_id,
           first_run.execution_id,
           first_run.workflow_id,
           first_run.status,
           0 AS hops
    FROM child_workflow_executions c
    CROSS JOIN LATERAL (
        SELECT e.execution_id, e.workflow_id, e.status
        FROM workflow_executions e
        WHERE e.parent_execution_id = c.parent_execution_id
          AND e.workflow_id = c.child_workflow_id
        ORDER BY e.created_at ASC
        LIMIT 1
    ) AS first_run
    UNION ALL
    SELECT chain.tracking_id,
           next_run.execution_id,
           next_run.workflow_id,
           next_run.status,
           chain.hops + 1
    FROM chain
    CROSS JOIN LATERAL (
        SELECT s.execution_id, s.workflow_id, s.status
        FROM workflow_executions s
        WHERE s.parent_execution_id = chain.execution_id
          AND s.workflow_id = chain.workflow_id
        ORDER BY s.created_at DESC
        LIMIT 1
    ) AS next_run
    -- The hop bound only guards against a malformed cycle; real chains are
    -- as long as a child's retries and continuations.
    WHERE chain.status IN ('restarted_fresh', 'reset')
      AND chain.hops < 1000
),
latest AS (
    SELECT DISTINCT ON (tracking_id) tracking_id, execution_id
    FROM chain
    ORDER BY tracking_id, hops DESC
)
UPDATE child_workflow_executions c
SET child_execution_id = latest.execution_id
FROM latest
WHERE c.id = latest.tracking_id
  AND c.child_execution_id IS DISTINCT FROM latest.execution_id;
