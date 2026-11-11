-- Which code release should pick up NEW work on a task queue.
--
-- Binding (V034) decides where an execution that has already started must run.
-- It says nothing about where a brand-new execution goes: an unbound execution
-- is claimable by anyone, so while two releases are deployed side by side, new
-- work lands on whichever worker polls first. That is safe — nothing replays
-- against the wrong code — but it makes a rollout unsteerable, and it makes
-- retiring a release harder than it should be, because new work keeps binding
-- to the old release right up until its workers stop.
--
-- A current release makes that explicit: new executions on this queue bind to
-- this release. Set it as the last step of a deploy and the old release starts
-- draining monotonically.
--
-- NULL / no row means "no policy" — every release may take new work, which is
-- exactly today's behaviour and stays the default for anyone not using this.
CREATE TABLE IF NOT EXISTS task_queue_releases (
    namespace          VARCHAR(100) NOT NULL,
    task_queue         VARCHAR(255) NOT NULL,
    current_version_id VARCHAR(255),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, task_queue)
);

-- The claim path reads this per poll, keyed by the primary key, so the lookup
-- is an index hit on a table with one row per queue.
COMMENT ON TABLE task_queue_releases IS
    'Per-task-queue policy: which code release new workflow executions bind to. No row means any release may take new work.';
