-- Trim indexes from outbox_queue, the hottest write table in the engine.
--
-- Every task moves pending → processing → completed, and `status` is indexed by
-- several partial indexes. A status change therefore cannot be a HOT update, so
-- Postgres rewrites the tuple and maintains EVERY index on the table — 19 of
-- them. Measured over 120 workflows, the single-row completion UPDATE by
-- primary key averaged 0.897 ms, roughly ten times the mean of every other
-- outbox statement.
--
-- Three indexes go. Each is justified by what it can serve, not merely by a
-- local scan count, because a zero-scan index may simply mean the workload did
-- not exercise its path.

-- 1. The GIN index over the whole payload: 9.8 MB, and unusable by construction.
--    A default jsonb_ops GIN serves containment and existence operators (@>, ?,
--    <@). The codebase issues none of those against this column — the only
--    payload access is `payload->>'field'` text extraction, which this index
--    cannot accelerate. That is already served by the dedicated expression
--    index idx_outbox_queue_task_queue_match, which is heavily used.
--
--    It is also the most expensive index here to maintain: every non-HOT update
--    rewrites its entries even though `payload` itself never changes.
DROP INDEX IF EXISTS idx_outbox_payload_gin;

-- 2 & 3. Two exact duplicate pairs — same column, same partial predicate,
--    character for character. One of each is redundant, so dropping it removes
--    write cost without removing coverage. The survivor in both cases is the
--    one the planner is already choosing.
--
--    idx_outbox_lease_expired       ≡ idx_outbox_queue_lease_expiry
--    idx_outbox_pending_workflow    ≡ idx_outbox_queue_workflow_ordering
DROP INDEX IF EXISTS idx_outbox_lease_expired;
DROP INDEX IF EXISTS idx_outbox_pending_workflow;

-- Deliberately KEPT despite zero local scans, because their paths are simply not
-- exercised by a healthy single-tenant workload and dropping them on that
-- evidence would be a mistake:
--   idx_outbox_retry_ready            — retry sweeps (no failures in the run)
--   idx_outbox_failed                 — failure listing
--   idx_outbox_queue_organization_id  — multi-tenant filtering
--   idx_outbox_queue_lease_expiry     — lease reclaim (kept as the survivor)
