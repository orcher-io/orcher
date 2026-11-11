-- V070: a run's step-id scheme and history lineage
--
-- step_id_scheme: how the run's step ids are derived, as
-- orcher.v1.StepIdScheme numbers it (1 the kernel's, 2-4 an SDK's legacy
-- scheme). Bound at the run's first activation by a worker that reports the
-- schemes it can replay, and never changed after: a run's history replays
-- only under the rules that wrote it. NULL is not bound yet. A run is handed
-- only to a worker that reports its scheme, refusing rather than falling
-- through, as release binding does (V034).
--
-- history_lineage: the line of decisions the run's steps belong to, bound
-- with the scheme. A reset copies it; a fresh history (a new start,
-- restart-fresh, a workflow retry) gets its own, which is its row id. Default
-- child workflow ids and the run's random seed derive from it, never from
-- the run id, so a reset run reproduces them. NULL reads as the row id.
--
-- activated_without_scheme: the run was activated by a worker that reported
-- no schemes, so its history may carry its SDK's legacy ids. Every run that
-- exists now predates binding and is marked; afterwards a claim by such a
-- worker marks the run it claims. When a worker that reports schemes first
-- activates a marked run, the run is bound to that worker's legacy scheme,
-- not the kernel's. Added with a default of true, which marks every existing
-- row without rewriting the table, then defaulted to false for new rows.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS step_id_scheme SMALLINT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS history_lineage TEXT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS activated_without_scheme BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE workflow_executions
    ALTER COLUMN activated_without_scheme SET DEFAULT FALSE;

COMMENT ON COLUMN workflow_executions.step_id_scheme IS
    'orcher.v1.StepIdScheme the run is bound to at its first activation; never rebound. NULL = unbound.';
COMMENT ON COLUMN workflow_executions.history_lineage IS
    'The run''s history lineage, bound with its scheme and copied by a reset. NULL reads as the row id.';
COMMENT ON COLUMN workflow_executions.activated_without_scheme IS
    'Activated by a worker that reported no step-id schemes, so bound to a legacy scheme when first bound.';
