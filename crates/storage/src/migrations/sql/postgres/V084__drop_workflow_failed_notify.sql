-- V084: drop the trigger that announced a failed workflow
--
-- V003 added a trigger on workflow_executions that, when a run became
-- failed, sent its error on the workflow_failed channel with pg_notify.
-- Nothing listens on that channel. And pg_notify refuses a payload over 8000
-- bytes, which a stack trace easily is, so the update that marked such a run
-- failed was itself refused and rolled back: the run stayed running, and its
-- activation was retried for good. Dropping the trigger lets every failure be
-- recorded, whatever the size of its error.
DROP TRIGGER IF EXISTS workflow_failed_notify ON workflow_executions;
DROP FUNCTION IF EXISTS notify_workflow_failed();
