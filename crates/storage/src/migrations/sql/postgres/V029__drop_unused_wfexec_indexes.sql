-- V029: drop unused workflow_executions indexes (write-minimization, tier 1).
--
-- Every index on workflow_executions whose key/predicate references the columns a
-- state transition changes (status / current_step / completed_steps / updated_at)
-- is maintained on EVERY transition — a per-row write cost that batching cannot
-- amortize. Measured impact (bench-durability, realistic index load): the batched
-- write win drops from ~23x to ~2.5x purely from index maintenance. Removing
-- indexes the engine never reads is therefore a direct throughput lever, and it
-- helps single-node and non-clustered deployments too.
--
-- These four are safe first drops: on the dev DB (which has heavy engine traffic —
-- hundreds of thousands of scans on the hot indexes) each had ZERO scans, and each
-- is either superseded by a retained hot index or a duplicate:
--
--   idx_workflow_executions_polling  — (task_queue,namespace,created_at) WHERE
--       status='running'. Superseded by idx_workflow_executions_poll_available
--       (the poll index the engine actually uses) + idx_workflow_executions_claim.
--   idx_workflow_exec_long_running   — (started_at) WHERE status='running'. A
--       "running since" report pattern the engine never issues.
--   idx_workflow_exec_queue          — (namespace,status,priority,submitted_at)
--       WHERE status IN ('pending','queued'). Superseded by the claim / active
--       indexes the engine uses for queue scans.
--   idx_workflow_exec_pending_requeue — DUPLICATE of idx_workflow_executions_
--       pending_requeue (both partial on pending_requeue=true); the latter is kept
--       so the reclaimer path is unaffected.
--
-- The retained hot indexes (claim, claimed_by, stale_claims, status, failed,
-- execution_id, workflow_id, pkey, poll_available, …) cover every engine query.
-- Further candidates (by-user/org/name, GIN/JSONB) are deferred pending a review
-- of the cloud dashboard/API query layer — they may serve queries not exercisable
-- from the OSS engine. See orcher.io/workflow-executions-index-audit.md.

DROP INDEX IF EXISTS idx_workflow_executions_polling;
DROP INDEX IF EXISTS idx_workflow_exec_long_running;
DROP INDEX IF EXISTS idx_workflow_exec_queue;
DROP INDEX IF EXISTS idx_workflow_exec_pending_requeue;
