//! Task Execution Lifecycle Integration Tests
//!
//! These tests verify the integration of task execution infrastructure components:
//! - TaskExecutionRepository lifecycle methods with database
//! - ExecutionLogRepository integration with task completion
//! - JournalRepository integration with task events
//! - Coordination between repositories for triple-write pattern
//!
//! ## Architecture Under Test
//!
//! ```text
//! Task Lifecycle:
//!     1. Create scheduled task_execution
//!     2. Mark as started (when service claims)
//!     3. Mark as completed/failed
//!     4. Write to execution_log (cache)
//!     5. Append to journal (source of truth)
//!
//! Triple-Write Pattern:
//!     task_executions → execution_log → workflow_execution_journal
//! ```
//!
//! ## Running Tests
//!
//! ```bash
//! # Setup PostgreSQL
//! createdb orcher_test
//!
//! # Run migrations
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo sqlx migrate run
//!
//! # Run all integration tests
//! cargo test --package orcher-core --test task_execution_lifecycle_integration -- --ignored
//!
//! # Run specific test
//! cargo test --package orcher-core --test task_execution_lifecycle_integration \
//!   test_task_execution_with_execution_log -- --ignored --nocapture
//! ```

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::{
    journal::JournalRepository, task_execution::TaskExecutionRepository,
};
use orcher_storage::repositories::{
    ExecutionLogEntry, ExecutionLogRepository, ExecutionLogStatus, ExecutionStepType,
    PostgresExecutionLogRepository,
};
use orcher_storage::schema::types::journal::JournalEntry;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

// ============================================================================
// Test Utilities
// ============================================================================

/// Test context with all required dependencies
struct TestContext {
    pool: PgPool,
    task_execution_repo: Arc<TaskExecutionRepository>,
    execution_log_repo: Arc<dyn ExecutionLogRepository>,
    journal_repo: Arc<JournalRepository>,
}

impl TestContext {
    /// Create a new test context with all dependencies initialized
    async fn new() -> Self {
        let database_url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });

        let pool = PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database. Set TEST_DATABASE_URL or ensure PostgreSQL is running.");

        // Initialize repositories
        let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
        let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));

        Self {
            pool,
            task_execution_repo,
            execution_log_repo: execution_log_repo as Arc<dyn ExecutionLogRepository>,
            journal_repo,
        }
    }

    /// Clean up test data for a workflow
    async fn cleanup(&self, workflow_id: WorkflowId) {
        let _ = sqlx::query("DELETE FROM task_executions WHERE workflow_id = $1")
            .bind(workflow_id.as_uuid())
            .execute(&self.pool)
            .await;

        let _ = sqlx::query("DELETE FROM execution_log WHERE workflow_id = $1")
            .bind(workflow_id.as_uuid())
            .execute(&self.pool)
            .await;

        let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id = $1")
            .bind(workflow_id.to_string().as_str())
            .execute(&self.pool)
            .await;
    }

    /// Close database connections
    async fn close(self) {
        self.pool.close().await;
    }
}

// ============================================================================
// Integration Tests: Task Execution + Execution Log
// ============================================================================

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn test_task_execution_with_execution_log() {
    let ctx = TestContext::new().await;
    let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
    let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

    // 1. Create scheduled task execution
    let task = ctx
        .task_execution_repo
        .create_scheduled(
            "integration-task-1".to_string(),
            "SendEmail".to_string(),
            workflow_id,
            execution_id,
            "email-queue".to_string(),
            "production".to_string(),
            1,
            Some(b"test input data".to_vec()),
            Some(serde_json::json!({"max_attempts": 3})),
            Some(300),
            Some(600),
            Some(30),
            Some(serde_json::json!({"x-request-id": "req-123"})),
            Some(serde_json::json!({"priority": "high"})),
        )
        .await
        .expect("Failed to create task execution");

    assert_eq!(task.status, "scheduled");
    assert_eq!(task.task_id, "integration-task-1");

    // 2. Mark task as started
    let started = ctx
        .task_execution_repo
        .mark_started(
            task.id,
            "email-service-01".to_string(),
            "task-token-abc123".to_string(),
        )
        .await
        .expect("Failed to mark task as started");

    assert_eq!(started.status, "started");
    assert!(started.started_at.is_some());

    // 3. Mark task as completed
    let result_data = serde_json::json!({"email_id": "msg-456", "status": "sent"});
    let completed = ctx
        .task_execution_repo
        .mark_completed(task.id, serde_json::to_vec(&result_data).unwrap())
        .await
        .expect("Failed to mark task as completed");

    assert_eq!(completed.status, "completed");
    assert!(completed.completed_at.is_some());

    // 4. Write to execution_log (cache for replay)
    let log_entry = ExecutionLogEntry {
        workflow_id: workflow_id.as_uuid(),
        step_name: "integration-task-1".to_string(),
        step_type: ExecutionStepType::Task,
        execution_attempt: 1,
        status: ExecutionLogStatus::Completed,
        input: Some(serde_json::json!("test input data")),
        result: Some(result_data),
        error_message: None,
        error_details: None,
        created_at: Timestamptz::now(),
        started_at: started.started_at,
        completed_at: completed.completed_at,
        duration_ms: completed
            .completed_at
            .and_then(|c| started.started_at.map(|s| (c - s).as_millis() as i64)),
        retry_count: 0,
        last_error: None,
    };

    ctx.execution_log_repo
        .store_entry(&log_entry)
        .await
        .expect("Failed to store execution log entry");

    // 5. Verify execution_log entry exists
    let retrieved_log = ctx
        .execution_log_repo
        .get_entry(workflow_id.as_uuid(), "integration-task-1")
        .await
        .expect("Failed to get execution log")
        .expect("Log entry not found");

    assert_eq!(retrieved_log.status, ExecutionLogStatus::Completed);
    assert_eq!(retrieved_log.step_name, "integration-task-1");

    // 6. Verify task_executions still has the record
    let task_check = ctx
        .task_execution_repo
        .find_by_id(task.id)
        .await
        .expect("Failed to find task")
        .expect("Task not found");

    assert_eq!(task_check.status, "completed");

    ctx.cleanup(workflow_id).await;
    ctx.close().await;
}

#[tokio::test]
#[ignore]
async fn test_task_failure_with_execution_log() {
    let ctx = TestContext::new().await;
    let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
    let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

    // 1. Create and start task
    let task = ctx
        .task_execution_repo
        .create_scheduled(
            "fail-task".to_string(),
            "ProcessPayment".to_string(),
            workflow_id,
            execution_id,
            "payment-queue".to_string(),
            "production".to_string(),
            1,
            Some(b"payment data".to_vec()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("Failed to create task");

    ctx.task_execution_repo
        .mark_started(
            task.id,
            "payment-service-01".to_string(),
            "token-def456".to_string(),
        )
        .await
        .expect("Failed to mark started");

    // 2. Mark task as failed
    let failed = ctx
        .task_execution_repo
        .mark_failed(
            task.id,
            "Payment gateway timeout".to_string(),
            Some("TimeoutError".to_string()),
            Some(serde_json::json!({"gateway": "stripe", "timeout_ms": 5000})),
        )
        .await
        .expect("Failed to mark task as failed");

    assert_eq!(failed.status, "failed");
    assert_eq!(
        failed.error_message,
        Some("Payment gateway timeout".to_string())
    );
    assert_eq!(failed.error_type, Some("TimeoutError".to_string()));

    // 3. Write failure to execution_log
    let log_entry = ExecutionLogEntry {
        workflow_id: workflow_id.as_uuid(),
        step_name: "fail-task".to_string(),
        step_type: ExecutionStepType::Task,
        execution_attempt: 1,
        status: ExecutionLogStatus::Failed,
        input: Some(serde_json::json!("payment data")),
        result: None,
        error_message: failed.error_message.clone(),
        error_details: failed.error_details.clone(),
        created_at: Timestamptz::now(),
        started_at: failed.started_at,
        completed_at: failed.completed_at,
        duration_ms: None,
        retry_count: 0,
        last_error: failed.error_message.clone(),
    };

    ctx.execution_log_repo
        .store_entry(&log_entry)
        .await
        .expect("Failed to store failed execution log");

    // 4. Verify both repositories have the failure
    let log_check = ctx
        .execution_log_repo
        .get_entry(workflow_id.as_uuid(), "fail-task")
        .await
        .expect("Failed to get log")
        .expect("Log not found");

    assert_eq!(log_check.status, ExecutionLogStatus::Failed);
    assert!(log_check.error_message.is_some());

    let task_check = ctx
        .task_execution_repo
        .find_by_id(task.id)
        .await
        .expect("Failed to find task")
        .expect("Task not found");

    assert_eq!(task_check.status, "failed");

    ctx.cleanup(workflow_id).await;
    ctx.close().await;
}

// ============================================================================
// Integration Tests: Triple-Write Pattern
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_triple_write_pattern() {
    let ctx = TestContext::new().await;
    let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
    let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

    // 1. Create and complete task execution
    let task = ctx
        .task_execution_repo
        .create_scheduled(
            "triple-write-task".to_string(),
            "TripleWriteTask".to_string(),
            workflow_id,
            execution_id,
            "default-queue".to_string(),
            "default".to_string(),
            1,
            Some(b"input data".to_vec()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("Failed to create task");

    let started = ctx
        .task_execution_repo
        .mark_started(task.id, "service-1".to_string(), "token-xyz".to_string())
        .await
        .expect("Failed to mark started");

    let result_bytes = b"result data".to_vec();
    let completed = ctx
        .task_execution_repo
        .mark_completed(task.id, result_bytes.clone())
        .await
        .expect("Failed to mark completed");

    // Write 1: task_executions (already done above)
    assert_eq!(completed.status, "completed");

    // Write 2: execution_log (cache for replay)
    let log_entry = ExecutionLogEntry {
        workflow_id: workflow_id.as_uuid(),
        step_name: "triple-write-task".to_string(),
        step_type: ExecutionStepType::Task,
        execution_attempt: 1,
        status: ExecutionLogStatus::Completed,
        input: Some(serde_json::json!("input data")),
        result: Some(serde_json::json!({"data": "result data"})),
        error_message: None,
        error_details: None,
        created_at: Timestamptz::now(),
        started_at: started.started_at,
        completed_at: completed.completed_at,
        duration_ms: Some(50),
        retry_count: 0,
        last_error: None,
    };

    ctx.execution_log_repo
        .store_entry(&log_entry)
        .await
        .expect("Failed to store execution log");

    // Write 3: workflow_execution_journal (source of truth)
    let journal_entry = JournalEntry::new(
        workflow_id.to_string(),
        execution_id.to_string(),
        1, // event_id
        "TaskCompleted".to_string(),
        serde_json::json!({
            "task_id": "triple-write-task",
            "task_type": "TripleWriteTask",
            "result": {"data": "result data"},
            "started_at": started.started_at,
            "completed_at": completed.completed_at,
        }),
    );

    let journal_result = ctx
        .journal_repo
        .append(journal_entry)
        .await
        .expect("Failed to append journal entry");

    assert!(journal_result.id > 0);

    // Verify all three writes succeeded

    // 1. task_executions
    let task_check = ctx
        .task_execution_repo
        .find_by_id(completed.id)
        .await
        .expect("Failed to find task")
        .expect("Task not found");
    assert_eq!(task_check.status, "completed");

    // 2. execution_log
    let log_check = ctx
        .execution_log_repo
        .get_entry(workflow_id.as_uuid(), "triple-write-task")
        .await
        .expect("Failed to get log")
        .expect("Log not found");
    assert_eq!(log_check.status, ExecutionLogStatus::Completed);

    // 3. workflow_execution_journal
    let journal_check = ctx
        .journal_repo
        .get_journal(&workflow_id.to_string(), &execution_id.to_string())
        .await
        .expect("Failed to get journal");
    assert!(!journal_check.is_empty());
    assert!(journal_check
        .iter()
        .any(|e| e.event_type == "TaskCompleted"));

    println!("✓ Triple-write pattern verified:");
    println!("  - task_executions: {}", task_check.status);
    println!("  - execution_log: {:?}", log_check.status);
    println!("  - journal entries: {}", journal_check.len());

    ctx.cleanup(workflow_id).await;
    ctx.close().await;
}

// ============================================================================
// Integration Tests: Retry Scenarios
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_task_retry_multiple_attempts() {
    let ctx = TestContext::new().await;
    let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
    let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

    // Attempt 1: Create, start, fail
    let task_attempt_1 = ctx
        .task_execution_repo
        .create_scheduled(
            "retry-task".to_string(),
            "RetryTask".to_string(),
            workflow_id,
            execution_id,
            "retry-queue".to_string(),
            "default".to_string(),
            1, // First attempt
            Some(b"input".to_vec()),
            Some(serde_json::json!({"max_attempts": 3})),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("Failed to create attempt 1");

    ctx.task_execution_repo
        .mark_started(
            task_attempt_1.id,
            "service-1".to_string(),
            "token-1".to_string(),
        )
        .await
        .expect("Failed to mark started");

    ctx.task_execution_repo
        .mark_failed(
            task_attempt_1.id,
            "Temporary error".to_string(),
            Some("RetryableError".to_string()),
            None,
        )
        .await
        .expect("Failed to mark failed");

    // Attempt 2: Retry
    let task_attempt_2 = ctx
        .task_execution_repo
        .create_scheduled(
            "retry-task".to_string(),
            "RetryTask".to_string(),
            workflow_id,
            execution_id,
            "retry-queue".to_string(),
            "default".to_string(),
            2, // Second attempt
            Some(b"input".to_vec()),
            Some(serde_json::json!({"max_attempts": 3})),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("Failed to create attempt 2");

    ctx.task_execution_repo
        .mark_started(
            task_attempt_2.id,
            "service-2".to_string(),
            "token-2".to_string(),
        )
        .await
        .expect("Failed to mark started");

    // Attempt 2 succeeds
    ctx.task_execution_repo
        .mark_completed(task_attempt_2.id, b"success".to_vec())
        .await
        .expect("Failed to mark completed");

    // Verify both attempts exist
    let all_tasks = ctx
        .task_execution_repo
        .find_by_workflow_id(workflow_id)
        .await
        .expect("Failed to find tasks");

    assert_eq!(all_tasks.len(), 2);
    assert!(all_tasks
        .iter()
        .any(|t| t.attempt_number == 1 && t.status == "failed"));
    assert!(all_tasks
        .iter()
        .any(|t| t.attempt_number == 2 && t.status == "completed"));

    println!("✓ Retry scenario verified:");
    println!("  - Attempt 1: failed");
    println!("  - Attempt 2: completed");

    ctx.cleanup(workflow_id).await;
    ctx.close().await;
}

// ============================================================================
// Integration Tests: Query and Analytics
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_query_tasks_by_status_across_workflows() {
    let ctx = TestContext::new().await;
    let workflow_id_1 = WorkflowId::from_uuid(Uuid::new_v4());
    let workflow_id_2 = WorkflowId::from_uuid(Uuid::new_v4());
    let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

    // Create tasks in different workflows and states
    let _task1 = ctx
        .task_execution_repo
        .create_scheduled(
            "task-1".to_string(),
            "Task1".to_string(),
            workflow_id_1,
            execution_id,
            "queue".to_string(),
            "default".to_string(),
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("Failed to create task1");

    let task2 = ctx
        .task_execution_repo
        .create_scheduled(
            "task-2".to_string(),
            "Task2".to_string(),
            workflow_id_2,
            execution_id,
            "queue".to_string(),
            "default".to_string(),
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("Failed to create task2");

    // Mark one as started
    ctx.task_execution_repo
        .mark_started(task2.id, "service-1".to_string(), "token-1".to_string())
        .await
        .expect("Failed to mark started");

    // Query scheduled tasks
    let scheduled = ctx
        .task_execution_repo
        .find_by_status("scheduled")
        .await
        .expect("Failed to find scheduled");

    assert!(!scheduled.is_empty());

    // Query started tasks
    let started = ctx
        .task_execution_repo
        .find_by_status("started")
        .await
        .expect("Failed to find started");

    assert!(started.iter().any(|t| t.id == task2.id));

    println!("✓ Query by status verified:");
    println!("  - Scheduled tasks: {}", scheduled.len());
    println!("  - Started tasks: {}", started.len());

    ctx.cleanup(workflow_id_1).await;
    ctx.cleanup(workflow_id_2).await;
    ctx.close().await;
}

#[tokio::test]
#[ignore]
async fn test_task_execution_with_heartbeat() {
    let ctx = TestContext::new().await;
    let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
    let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

    // Create and start task
    let task = ctx
        .task_execution_repo
        .create_scheduled(
            "long-running-task".to_string(),
            "LongTask".to_string(),
            workflow_id,
            execution_id,
            "long-queue".to_string(),
            "default".to_string(),
            1,
            None,
            None,
            None,
            None,
            Some(30), // heartbeat_timeout
            None,
            None,
        )
        .await
        .expect("Failed to create task");

    let started = ctx
        .task_execution_repo
        .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
        .await
        .expect("Failed to mark started");

    // Send heartbeat
    let heartbeat_details = b"progress: 50%".to_vec();
    let updated = ctx
        .task_execution_repo
        .update_heartbeat(started.id, Some(heartbeat_details.clone()))
        .await
        .expect("Failed to update heartbeat");

    assert!(updated.last_heartbeat_at.is_some());
    assert_eq!(updated.heartbeat_details, Some(heartbeat_details));
    assert_eq!(updated.status, "started");

    println!("✓ Heartbeat verified:");
    println!("  - Last heartbeat: {:?}", updated.last_heartbeat_at);
    println!("  - Status unchanged: {}", updated.status);

    ctx.cleanup(workflow_id).await;
    ctx.close().await;
}
