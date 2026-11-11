-- V056: an index for claiming an organization's workflows
--
-- A poll made for an organization claims only that organization's executions.
-- With the organization only a filter on idx_workflow_executions_claim, a poll
-- for an organization that holds a small share of a queue walked the other
-- organizations' entries to find its own: 38 ms a claim, five times a second
-- for every idle poll. With the organization in the key, the claim reads its
-- own entries only (0.6 to 0.9 ms). Polls that name no organization keep using
-- idx_workflow_executions_claim.
--
-- The same predicate as idx_workflow_executions_claim, so the claim's WHERE
-- implies it. CONCURRENTLY, as in V050, so building it does not block the
-- writes that go on meanwhile; the runner executes this outside a transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_executions_claim_by_organization
    ON workflow_executions (task_queue, namespace, organization_id, priority DESC, submitted_at)
    WHERE status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
      AND claimed_at IS NULL
      AND cancelled_at IS NULL;
