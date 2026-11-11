-- V068: find an organization's pending dispatches on one queue, oldest first
--
-- A poll claims the oldest pending dispatch of its own (namespace, task_queue).
-- It used to claim across every queue, one dispatch per workflow, and V057
-- indexed an organization's pending dispatches, and the unstamped ones, by
-- workflow for that. A poll made for an organization now reads its queue in
-- created_at order, its organization's stamped rows and the unstamped ones
-- apart: on V057's indexes that was every pending row of the organization,
-- or every unstamped one, read whole and sorted, and the planner chose the
-- queue's own index (V014) instead, every pending row of the queue across
-- organizations, which an idle organization sharing a queue name with a busy
-- one walked on each of its polls. This index answers both parts in order,
-- LIMIT stopping at the first row, and an idle organization's poll at none:
-- a NULL organization is a key like any other, so the unstamped part reads
-- the same index.
--
-- It replaces V057's two indexes, which served only the old claim.
-- CONCURRENTLY, as in V057.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_pending_dispatch_by_queue
    ON outbox_queue (organization_id, (payload->>'task_queue'), (payload->>'namespace'), created_at)
    WHERE status = 'pending' AND task_type = 'dispatch_task';

DROP INDEX CONCURRENTLY IF EXISTS idx_outbox_pending_by_organization;

DROP INDEX CONCURRENTLY IF EXISTS idx_outbox_pending_unstamped;
