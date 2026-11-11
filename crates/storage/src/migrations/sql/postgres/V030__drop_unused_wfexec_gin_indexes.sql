-- V030: drop unused workflow_executions GIN indexes (write-minimization, tier 3).
--
-- GIN (JSONB) index maintenance is far heavier than btree, so these are the most
-- expensive indexes to keep on the write path. A query-layer review of the whole
-- OSS codebase found that FOUR of the six JSONB columns are never used in a
-- containment / filter query anywhere (0 `@>`, `?`, `->>` filter usages) — they
-- index the workflow's internal blobs and are only ever projected, never searched:
--
--   idx_workflow_exec_parameters_gin (120 kB) — workflow INPUT, set on start
--   idx_workflow_exec_result_gin      (64 kB) — workflow OUTPUT, set on completion
--   idx_workflow_exec_context_gin     (64 kB) — internal execution context
--   idx_workflow_exec_statistics_gin  (72 kB) — internal metrics
--
-- The two JSONB columns that ARE filtered — `labels` (5 `labels @> $n` query sites)
-- and `tags` (1, chronicle query builder) — keep their GIN indexes; they are the
-- user-facing tag/label filter dimensions.
--
-- Caveat: the cloud dashboard/API lives in a separate repo. If a
-- search-workflows-by-input/output/context feature exists there, these would need
-- re-adding — a one-line migration. It is an unusual product feature (searching by
-- raw JSONB containment on internal blobs) and has zero usage in OSS, so the
-- write-cost saving outweighs the small risk. See
-- orcher.io/workflow-executions-index-audit.md.

DROP INDEX IF EXISTS idx_workflow_exec_parameters_gin;
DROP INDEX IF EXISTS idx_workflow_exec_result_gin;
DROP INDEX IF EXISTS idx_workflow_exec_context_gin;
DROP INDEX IF EXISTS idx_workflow_exec_statistics_gin;
