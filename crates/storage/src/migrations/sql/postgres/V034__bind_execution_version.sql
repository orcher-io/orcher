-- The code release a workflow execution is bound to.
--
-- Set from the version_id of the worker that first claims the execution, and
-- never changed afterwards: an execution replays its journal, so it must keep
-- running against the code it started on. Rebinding mid-flight is exactly the
-- divergence this exists to prevent.
--
-- NULL means unbound — every execution that predates this column, and every
-- execution claimed by a worker that declares no version. Unbound executions
-- behave as they always have and are claimable by any worker.
--
-- Opaque on purpose: a git sha, an image digest, a release tag, or later an
-- endpoint identifying a serverless deployment. The server compares it for
-- equality and never parses or orders it.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS bound_version_id VARCHAR(255) DEFAULT NULL;

-- Supports "which releases still have live executions", the question that
-- decides when an old release can be retired. Partial, because the column is
-- NULL for everything until workers start declaring a version.
CREATE INDEX IF NOT EXISTS idx_workflow_exec_bound_version
    ON workflow_executions (bound_version_id)
    WHERE bound_version_id IS NOT NULL;

COMMENT ON COLUMN workflow_executions.bound_version_id IS
    'Opaque code release this execution is bound to; set on first claim, never rebound. NULL = unbound.';
