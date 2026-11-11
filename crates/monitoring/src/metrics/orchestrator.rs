//! Orchestrator metrics (task submission, workflow coordination).

use metrics::{counter, gauge};

pub fn track_task_submitted(task_type: &str) {
    counter!(super::names::orchestrator::TASKS_SUBMITTED_TOTAL, "task_type" => task_type.to_string()).increment(1);
}

pub fn track_task_cancelled(task_type: &str, reason: &str) {
    counter!(super::names::orchestrator::TASKS_CANCELLED_TOTAL, "task_type" => task_type.to_string(), "reason" => reason.to_string()).increment(1);
}

pub fn track_workflow_paused(workflow_id: &str) {
    counter!(super::names::orchestrator::WORKFLOWS_PAUSED_TOTAL, "workflow_id" => workflow_id.to_string()).increment(1);
}

pub fn track_workflow_resumed(workflow_id: &str) {
    counter!(super::names::orchestrator::WORKFLOWS_RESUMED_TOTAL, "workflow_id" => workflow_id.to_string()).increment(1);
}

pub fn track_workflow_cancelled(workflow_id: &str, reason: &str) {
    counter!(super::names::orchestrator::WORKFLOWS_CANCELLED_TOTAL, "workflow_id" => workflow_id.to_string(), "reason" => reason.to_string()).increment(1);
}

pub fn update_active_workflows(count: usize) {
    gauge!(super::names::orchestrator::ACTIVE_WORKFLOWS).set(count as f64);
}

pub fn update_active_tasks(count: usize) {
    gauge!(super::names::orchestrator::ACTIVE_TASKS).set(count as f64);
}

/// Track a coordinator operation with mode ("local"/"distributed") and status.
pub fn track_coordinator_operation(operation: &str, mode: &str, status: &str) {
    counter!(super::names::orchestrator::COORDINATOR_OPERATIONS_TOTAL, "operation" => operation.to_string(), "mode" => mode.to_string(), "status" => status.to_string()).increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_task_submitted() {
        track_task_submitted("compute");
        track_task_submitted("io");
    }

    #[test]
    fn test_track_task_cancelled() {
        track_task_cancelled("compute", "user_requested");
        track_task_cancelled("io", "timeout");
    }

    #[test]
    fn test_track_workflow_operations() {
        track_workflow_paused("wf-123");
        track_workflow_resumed("wf-123");
        track_workflow_cancelled("wf-456", "step_failed");
    }

    #[test]
    fn test_update_active_counts() {
        update_active_workflows(42);
        update_active_tasks(128);
    }

    #[test]
    fn test_track_coordinator_operation() {
        track_coordinator_operation("submit_workflow", "local", "success");
        track_coordinator_operation("cancel_workflow", "distributed", "failed");
    }
}
