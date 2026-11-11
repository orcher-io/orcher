-- V041: a task queue's current release is per organisation
--
-- `task_queue_releases` was keyed `(namespace, task_queue)`. A namespace name
-- is unique within an organisation, not across the deployment, so two tenants
-- each running a namespace called 'production' shared one row: whoever set a
-- current release last set it for both. Pointing a queue at a release nobody is
-- running is how a deploy drains the previous one, so the failure is quiet and
-- total — the other tenant's new executions bind to a release that has no
-- workers, are never claimed, and simply sit there. Nothing errors, because
-- from the engine's side nothing is wrong.
--
-- The claim path reads this table on every poll, so the replacement has to stay
-- a single index hit. It does: the unique constraint below covers all three
-- equality columns the lookup uses.

ALTER TABLE task_queue_releases
    ADD COLUMN IF NOT EXISTS organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE;

-- The primary key has to go before the wider key can replace it: while
-- (namespace, task_queue) is still unique on its own, two organisations cannot
-- both hold a policy for the same queue name, which is the whole point.
--
-- A primary key cannot contain a nullable column, and this one must be
-- nullable: a deployment running without tenancy leaves it NULL. So the
-- replacement is a unique constraint with NULLS NOT DISTINCT, which treats two
-- NULL owners as the same owner and therefore still allows exactly one
-- unscoped row per queue. Same reasoning as V038 for schedule names.
ALTER TABLE task_queue_releases DROP CONSTRAINT IF EXISTS task_queue_releases_pkey;

-- Dropped first so a re-run after a partial failure is not blocked by its own
-- previous attempt.
ALTER TABLE task_queue_releases DROP CONSTRAINT IF EXISTS task_queue_releases_org_queue_key;

ALTER TABLE task_queue_releases
    ADD CONSTRAINT task_queue_releases_org_queue_key
    UNIQUE NULLS NOT DISTINCT (organization_id, namespace, task_queue);

COMMENT ON COLUMN task_queue_releases.organization_id IS
    'Organisation the policy belongs to. NULL means an unscoped deployment. '
    'Part of the uniqueness key: two organisations may each set a current '
    'release for a queue of the same name without overwriting each other.';
