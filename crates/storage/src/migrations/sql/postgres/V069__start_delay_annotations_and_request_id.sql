-- V069: a start's delay, its annotations and its request id
--
-- StartWorkflowRequest has always carried start_delay, annotations and
-- request_id, and every start path dropped all three.
--
-- annotations: the start's metadata, a JSON object of strings, stored as
-- given and reported by Describe. Not indexed: labels are what is searched.
-- NULL reads as none.
--
-- start_request_id: the request id of the start that made the run. One run in
-- an organization's namespace may carry a given request id, which the unique
-- index below decides, so a start repeated with the same id (a client
-- retrying a call whose answer it lost) is answered with the run the first
-- one made rather than refused or started twice. Only that run carries it —
-- its retries and successors do not — so a request id is remembered for as
-- long as that row is kept. Scoped as the workflow id hold (V059) is: an
-- unscoped deployment's runs, which have no organization, share one scope.
--
-- start_after / awaiting_start: a delayed start's run is written at once,
-- due at start_after (its submission plus the delay), and no claim hands it
-- out while awaiting_start is set. The engine's delayed-start release clears
-- awaiting_start once start_after has passed, and wakes the run's queue.
-- start_after is kept afterwards, as when the run's clocks started: a run's
-- run timeout and its execution deadline are measured from it, not from its
-- submission, so a delay is not spent from either. awaiting_start is a column
-- of its own, rather than a comparison of start_after with the time, so that
-- the claim indexes can leave a waiting run out: a predicate cannot name the
-- time, and a queue holding many delayed starts would otherwise be walked
-- past on every claim.
--
-- Run outside a transaction, statement by statement, as in V067: the columns
-- are added without a rewrite (a constant default is stored in the catalog),
-- and every index is built CONCURRENTLY so writes carry on throughout. Each
-- claim index is rebuilt beside the one it replaces and renamed into its
-- place, so a claim is never left without one; dropping a half-built one
-- first makes a re-run rebuild it. Until it is rebuilt, and on an engine from
-- before this, a claim reads idx_workflow_executions_poll_available instead,
-- whose predicate every claim implies.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS annotations JSONB;

ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS start_request_id TEXT;

ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS start_after TIMESTAMPTZ;

ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS awaiting_start BOOLEAN NOT NULL DEFAULT FALSE;

-- One run per start request id in an organization's namespace.
DROP INDEX CONCURRENTLY IF EXISTS uq_workflow_executions_start_request;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_workflow_executions_start_request
    ON workflow_executions (
        (COALESCE(organization_id, '00000000-0000-0000-0000-000000000000'::uuid)),
        namespace,
        start_request_id
    )
    WHERE start_request_id IS NOT NULL;

-- The delayed runs, by when they are due, for the release.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_executions_awaiting_start
    ON workflow_executions (start_after)
    WHERE awaiting_start;

-- The claim index (V003), without the runs waiting out a delay.
DROP INDEX CONCURRENTLY IF EXISTS idx_workflow_executions_claim_v069;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_executions_claim_v069
    ON workflow_executions (task_queue, namespace, priority DESC, submitted_at)
    WHERE status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
      AND claimed_at IS NULL
      AND cancelled_at IS NULL
      AND NOT awaiting_start;

DROP INDEX CONCURRENTLY IF EXISTS idx_workflow_executions_claim;

ALTER INDEX IF EXISTS idx_workflow_executions_claim_v069
    RENAME TO idx_workflow_executions_claim;

-- The claim index for polls made for an organization (V056), likewise.
DROP INDEX CONCURRENTLY IF EXISTS idx_workflow_executions_claim_by_organization_v069;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_executions_claim_by_organization_v069
    ON workflow_executions (task_queue, namespace, organization_id, priority DESC, submitted_at)
    WHERE status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
      AND claimed_at IS NULL
      AND cancelled_at IS NULL
      AND NOT awaiting_start;

DROP INDEX CONCURRENTLY IF EXISTS idx_workflow_executions_claim_by_organization;

ALTER INDEX IF EXISTS idx_workflow_executions_claim_by_organization_v069
    RENAME TO idx_workflow_executions_claim_by_organization;
