-- V057: find an organization's pending dispatches, and the unstamped ones
--
-- A poll made for an organization claims the pending dispatches stamped with
-- it, and the unstamped ones whose workflow belongs to it (V055). Without an
-- index for either, the claim walked every pending row in the outbox, and a
-- poll of an organization with no work at all, which is what an idle worker
-- sends five times a second, paid for the whole walk: 30 ms each against a
-- million-row outbox with 50,000 pending. With these it reads that
-- organization's pending rows and the unstamped ones only (0.1 ms idle, under
-- 1 ms with work). The unstamped index is all but empty once V055 has run.
--
-- CONCURRENTLY, as in V050, so building them does not block the writes that
-- go on meanwhile; the runner executes this outside a transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_pending_by_organization
    ON outbox_queue (organization_id, workflow_id, created_at)
    WHERE status = 'pending' AND organization_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_pending_unstamped
    ON outbox_queue (workflow_id, created_at)
    WHERE status = 'pending' AND organization_id IS NULL;
