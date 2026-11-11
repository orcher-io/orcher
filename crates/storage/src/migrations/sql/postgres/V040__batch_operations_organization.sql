-- V040: a batch operation belongs to an organisation
--
-- `batch_operations` had no owner at all. The processor selected the workflows
-- to act on by namespace *name* alone, and a namespace name is not unique
-- across the deployment — it is unique per organisation. So one caller posting
-- a cancel against namespace 'default' matched, and cancelled, every workflow
-- in every organisation that also had a namespace called 'default'. One request
-- to one endpoint, every tenant's running work terminated, and no record of who
-- had asked for it.
--
-- Nullable rather than NOT NULL, matching every other tenant-scoped table here:
-- a deployment running without tenancy leaves it NULL, and a NULL owner means
-- "unscoped", which is the correct behaviour when there is only one tenant.

ALTER TABLE batch_operations
    ADD COLUMN IF NOT EXISTS organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE;

-- Listing is always "this organisation's operations, newest first", so the
-- index carries the sort column. Without it the scoped list degrades to a scan
-- of every tenant's history to return one tenant's page.
CREATE INDEX IF NOT EXISTS idx_batch_ops_organization
    ON batch_operations (organization_id, created_at DESC);

-- Any operation still queued or running predates the column, so nothing records
-- whose it was, and the processor would execute it with no owner — which is the
-- unscoped behaviour this migration exists to remove. There is no safe way to
-- infer the owner after the fact, and guessing wrong means acting on the wrong
-- tenant's workflows, so these are stopped and must be re-submitted by a caller
-- whose organisation can be established.
UPDATE batch_operations
SET status = 'terminated',
    error = COALESCE(error || ' | ', '') ||
            'Terminated by migration V040: created before batch operations recorded an ' ||
            'owning organisation, so it cannot be executed within one. Re-submit it.',
    completed_at = NOW()
WHERE status IN ('pending', 'processing');

COMMENT ON COLUMN batch_operations.organization_id IS
    'Organisation the operation acts within. NULL means an unscoped deployment; '
    'the processor filters the workflows it matches by this value.';
