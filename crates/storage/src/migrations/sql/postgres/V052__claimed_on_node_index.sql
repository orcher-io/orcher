-- V052: find the claims an engine handed out
--
-- Read when an engine's heartbeat goes stale, to release the claims it left
-- with an activation still out (V051). Partial over exactly those: a claim
-- whose activation has completed is a parked workflow, which the release
-- never touches and which may be most of an engine's claims. Indexing them
-- too had every survivor read them all again on every membership tick for as
-- long as the dead engine's claims could still be outstanding — measured at
-- 734ms a tick with 100,000 parked, against 7ms with this index. Ordered by
-- claim time within an engine, which the release bounds on both sides.
--
-- A delivered matching row stays claimed, so the matching index is by claim
-- time too: the release reads only the few claimed in an engine's last
-- seconds.
--
-- CONCURRENTLY, as in V042 and V046 to V050, so building it on a large table
-- does not block the writes that go on meanwhile; the migration runner
-- executes it outside a transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_executions_claimed_on_node
    ON workflow_executions (claimed_on_node, claimed_at)
    WHERE claimed_by IS NOT NULL
      AND claimed_on_node IS NOT NULL
      AND activation_seq > COALESCE(completed_activation, 0);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_matching_claimed_on_node
    ON matching_partitions (claimed_on_node, claimed_at)
    WHERE status = 'claimed' AND claimed_on_node IS NOT NULL;
