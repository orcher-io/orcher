-- V032 — make get_next_event_id race-free under parallel appends.
--
-- The journal event_id was allocated as MAX(event_id)+1 with no locking, so two
-- transactions appending to the same workflow-execution journal at the same time
-- (e.g. an at-least-once task completion re-delivered during failover re-drive)
-- could compute the SAME next id and collide on the uq_journal_event_id unique
-- constraint, surfacing a hard "duplicate key" error to the client.
--
-- Serialize allocation per (workflow, execution) with a transaction-scoped
-- advisory lock: it is held until the caller commits, so the returned id is fully
-- consumed before the next allocation. Correct because a single execution's
-- journal events are logically ordered anyway. This fixes the race for EVERY
-- caller of get_next_event_id, not just the task-completion path.
CREATE OR REPLACE FUNCTION get_next_event_id(
    p_workflow_id VARCHAR(255),
    p_execution_id VARCHAR(255)
) RETURNS BIGINT AS $$
DECLARE
    v_max_event_id BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_workflow_id || ':' || p_execution_id, 0)
    );

    SELECT COALESCE(MAX(event_id), 0) INTO v_max_event_id
    FROM workflow_execution_journal
    WHERE workflow_id = p_workflow_id
      AND execution_id = p_execution_id;

    RETURN v_max_event_id + 1;
END;
$$ LANGUAGE plpgsql;
