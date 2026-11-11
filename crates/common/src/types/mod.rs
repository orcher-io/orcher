//! Core type system: identifiers, task definitions, time utilities, and dispatch types.

pub mod dispatch;
pub mod identifiers;
pub mod organization;
pub mod task;
pub mod time;

// Re-export identifier types for convenience and backward compatibility
pub use identifiers::{
    ApiKeyId, ExecutionId, OrganizationId, OrganizationInvitationId, OrganizationMemberId,
    SessionId, TaskExecutionId, TaskId, UserId, WorkflowId,
};

// Re-export organization types for convenience
pub use organization::{
    InvitationStatus, MemberStatus, OrganizationPlan, OrganizationRole, OrganizationStatus,
};

// Re-export identifier traits
pub use identifiers::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};

// Re-export dispatch types for convenience
pub use dispatch::{
    // Routing types
    AffinityConfig,
    // Capacity types
    CapacityProvider,
    // Polling types
    ClaimInfo,
    EagerDispatchConfig,
    EagerDispatchStats,
    EagerDispatchable,
    // State types
    EventGateInfo,
    GlobalCapacityStats,
    HorizontalShardingConfig,
    JournalEntry,
    NotificationType,
    PollExecutionRequest,
    PollExecutionResponse,
    PollTaskRequest,
    PollTaskResponse,
    QueueCapacityStats,
    QueueNotification,
    QueueStats,
    RoutingConfig,
    SelectionReason,
    ShardRedirectInfo,
    ShardingConfig,
    SharedPollResponse,
    TaskInfo,
    TaskPermitStats,
    TimerInfo,
    WorkflowExecutionState,
    WorkflowStatus,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifier_types() {
        let task_id = TaskId::new_v4();
        let workflow_id = WorkflowId::new_v4();
        let execution_id = ExecutionId::new_v4();
        let user_id = UserId::new_v4();
        let session_id = SessionId::new_v4();
        let api_key_id = ApiKeyId::new_v4();

        // Verify they are valid identifiers
        assert!(!task_id.is_nil());
        assert!(!workflow_id.is_nil());
        assert!(!execution_id.is_nil());
        assert!(!user_id.is_nil());
        assert!(!session_id.is_nil());
        assert!(!api_key_id.is_nil());

        // Test type names
        assert_eq!(TaskId::type_name(), "TaskId");
        assert_eq!(WorkflowId::type_name(), "WorkflowId");
        assert_eq!(ExecutionId::type_name(), "ExecutionId");
        assert_eq!(UserId::type_name(), "UserId");

        // Test validation
        assert!(task_id.validate().is_ok());
        assert!(workflow_id.validate().is_ok());
    }
}
