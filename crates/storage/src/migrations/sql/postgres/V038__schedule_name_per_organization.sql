-- A schedule name belongs to an organisation, not to the deployment.
--
-- `UNIQUE(name)` made the name global, and the start path upserts on it with
-- ON CONFLICT (name) DO UPDATE. Cron schedule names are derived, not chosen —
-- "{workflow_type}:{cron}" — so two tenants scheduling the same workflow type
-- on the same cron collide by construction, and the second one silently
-- rewrites the first: cron, timezone, enabled, dates, max runs, and the task
-- template, which carries the namespace, task queue and input.
--
-- NULLS NOT DISTINCT keeps the old guarantee for a single-tenant deployment,
-- where every row has no organisation: without it, Postgres treats each NULL as
-- distinct and the name stops being unique there at all.

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_name_key;

-- Dropped first so a re-run after a partial failure is not blocked by its own
-- previous attempt.
ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_organization_name_key;

ALTER TABLE schedules
    ADD CONSTRAINT schedules_organization_name_key
    UNIQUE NULLS NOT DISTINCT (organization_id, name);
