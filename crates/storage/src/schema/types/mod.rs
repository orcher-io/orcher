//! Storage entity definitions for all schema types.
#![allow(missing_docs)] // Field names are self-documenting; verbose per-field docs removed intentionally

pub mod actor;
pub mod child_workflow;
pub mod cluster;
pub mod coordination;
pub mod event;
pub mod execution;
pub mod journal;
pub mod namespace;
pub mod schedule;
pub mod worker;
pub mod workflow;

pub use actor::{ActorState, ActorStateFilter, ActorStateStats, ActorTypeStats};
pub use child_workflow::{
    ChildWorkflowExecution, ChildWorkflowExecutionBuilder, ChildWorkflowQueryFilters,
    ChildWorkflowState, OrphanPolicy,
};
pub use cluster::{ClusterMember, ClusterShard};
pub use coordination::{
    DeadLetterTask, OutboxTask, OutboxTaskType, QueueMetrics, TaskStatus, TimerTask, TimerTaskType,
};
pub use event::{EventFilter, EventMetadata, EventSnapshot, StoredEvent, StreamId};
pub use execution::{ExecutionTrigger, TaskExecution, WorkflowExecution};
pub use journal::{JournalEntry, JournalEntryBuilder, JournalQueryFilters, JournalStatistics};
pub use namespace::Namespace;
pub use schedule::{OverlapPolicy, Schedule, ScheduleStatus};
pub use worker::{Worker, WorkerStatus};
pub use workflow::Workflow;

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::{traits::Identifier, OrganizationId, UserId};

    // TODO: Update this test to match the new TaskExecution structure (workflow-scoped task execution)
    // The TaskExecution model no longer has a simple ::new() constructor
    // It now requires workflow_id, execution_id, task_queue, and other fields from V027 migration
    #[test]
    #[ignore]
    fn test_schema_types_integration() {
        // Test that all schema types work together with orcher-common
        let user_id = UserId::new_v4();
        let workflow_def = serde_json::json!({"steps": []});
        let org_id = OrganizationId::new_v4();
        let workflow = Workflow::new(org_id, "test".to_string(), workflow_def, user_id);

        // Test worker integration
        let org_id = OrganizationId::new_v4();
        let worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "test-worker".to_string(),
            "localhost".to_string(),
            8080,
        );

        // Test execution integration - DISABLED: TaskExecution no longer has ::new()
        // See TaskExecutionRepository.create_scheduled() for the correct way to create task executions
        let workflow_execution = WorkflowExecution::new(
            "test-workflow".to_string(),
            "1.0.0".to_string(),
            "default".to_string(),
            serde_json::json!({}),
        );

        assert!(workflow.id.is_valid());
        assert!(worker.id.is_valid());
        // assert!(task_execution.id.is_valid());
        assert!(workflow_execution.id.is_valid());
    }
}
