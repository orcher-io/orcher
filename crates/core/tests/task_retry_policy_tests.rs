//! # Task Retry Policy Integration Tests
//!
//! Comprehensive tests for retry policy evaluation, retry scheduling, and DLQ handling.
//!
//! ## Test Coverage
//!
//! - Retry policy evaluation (backoff calculation, attempt limits)
//! - Automatic rescheduling with backoff
//! - Dead letter queue (DLQ) handling
//! - Non-retryable error handling
//! - Timeout retry logic
//! - Retry statistics
//! - Integration with task execution repository

use jiff::SignedDuration;
use orcher_common::types::identifiers::{ExecutionId, TaskExecutionId, WorkflowId};
use orcher_common::types::time::Timestamptz;
use orcher_core::retries::retry_policy::{
    DlqReason, RetryDecision, RetryPolicy, RetryPolicyEvaluator,
};
use orcher_core::retries::retry_scheduler::{RetryOutcome, TaskRetryScheduler};
use orcher_storage::schema::types::execution::TaskExecution;
use serde_json::json;
use sqlx::PgPool;
use std::time::Duration;

// ================================================================================================
// TEST HELPERS
// ================================================================================================

async fn setup_test_db() -> PgPool {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/orcher_test".to_string());

    let pool = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test database. Set TEST_DATABASE_URL and ensure database is migrated.");

    // Clean up any existing test data
    sqlx::query("TRUNCATE TABLE task_executions CASCADE")
        .execute(&pool)
        .await
        .expect("Failed to clean task_executions");

    pool
}

fn create_test_task_execution(
    task_id: String,
    workflow_id: WorkflowId,
    execution_id: ExecutionId,
    attempt_number: i32,
    status: &str,
    retry_policy: Option<serde_json::Value>,
) -> TaskExecution {
    let now = Timestamptz::now();
    TaskExecution {
        id: TaskExecutionId::new_v4(),
        organization_id: None,
        task_id,
        task_type: "TestTask".to_string(),
        workflow_id,
        execution_id,
        task_queue: "test-queue".to_string(),
        namespace: "default".to_string(),
        service_identity: None,
        attempt_number,
        status: status.to_string(),
        scheduled_at: now,
        started_at: if status == "started" { Some(now) } else { None },
        completed_at: if status == "failed" || status == "completed" {
            Some(now)
        } else {
            None
        },
        task_token: None,
        schedule_to_start_timeout_secs: Some(30),
        start_to_close_timeout_secs: Some(60),
        heartbeat_timeout_secs: Some(10),
        input: Some(b"test input".to_vec()),
        result: None,
        error_message: if status == "failed" {
            Some("Test error".to_string())
        } else {
            None
        },
        error_type: if status == "failed" {
            Some("TransientError".to_string())
        } else {
            None
        },
        error_details: None,
        retry_policy,
        headers: json!({}),
        metadata: json!({}),
        last_heartbeat_at: None,
        heartbeat_details: None,
        created_at: now,
        updated_at: now,
    }
}

async fn insert_task_execution(pool: &PgPool, task: &TaskExecution) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO task_executions (
            id, task_id, task_type, workflow_id, execution_id,
            task_queue, namespace, service_identity, attempt_number, status,
            scheduled_at, started_at, completed_at, task_token,
            schedule_to_start_timeout_secs, start_to_close_timeout_secs,
            heartbeat_timeout_secs, input, result, error_message, error_type,
            error_details, retry_policy, headers, metadata,
            last_heartbeat_at, heartbeat_details, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
        "#,
    )
    .bind(task.id.as_uuid())
    .bind(&task.task_id)
    .bind(&task.task_type)
    .bind(task.workflow_id.as_uuid())
    .bind(task.execution_id.as_uuid())
    .bind(&task.task_queue)
    .bind(&task.namespace)
    .bind(&task.service_identity)
    .bind(task.attempt_number)
    .bind(&task.status)
    .bind(task.scheduled_at)
    .bind(task.started_at)
    .bind(task.completed_at)
    .bind(&task.task_token)
    .bind(task.schedule_to_start_timeout_secs)
    .bind(task.start_to_close_timeout_secs)
    .bind(task.heartbeat_timeout_secs)
    .bind(&task.input)
    .bind(&task.result)
    .bind(&task.error_message)
    .bind(&task.error_type)
    .bind(&task.error_details)
    .bind(&task.retry_policy)
    .bind(&task.headers)
    .bind(&task.metadata)
    .bind(task.last_heartbeat_at)
    .bind(&task.heartbeat_details)
    .bind(task.created_at)
    .bind(task.updated_at)
    .execute(pool)
    .await?;

    Ok(())
}

// ================================================================================================
// RETRY POLICY TESTS
// ================================================================================================

#[tokio::test]
async fn test_retry_policy_exponential_backoff() {
    let policy = RetryPolicy::exponential_backoff(5, 1.0);

    // Test backoff calculation
    assert_eq!(policy.calculate_backoff(1), Duration::from_secs(1));
    assert_eq!(policy.calculate_backoff(2), Duration::from_secs(2));
    assert_eq!(policy.calculate_backoff(3), Duration::from_secs(4));
    assert_eq!(policy.calculate_backoff(4), Duration::from_secs(8));
    assert_eq!(policy.calculate_backoff(5), Duration::from_secs(16));
}

#[tokio::test]
async fn test_retry_policy_max_interval_cap() {
    let policy = RetryPolicy::new(10, 1.0, 10.0, 2.0);

    // Should cap at max_interval_seconds (10)
    assert_eq!(policy.calculate_backoff(1), Duration::from_secs(1));
    assert_eq!(policy.calculate_backoff(2), Duration::from_secs(2));
    assert_eq!(policy.calculate_backoff(3), Duration::from_secs(4));
    assert_eq!(policy.calculate_backoff(4), Duration::from_secs(8));
    assert_eq!(policy.calculate_backoff(5), Duration::from_secs(10)); // Capped
    assert_eq!(policy.calculate_backoff(6), Duration::from_secs(10)); // Still capped
}

#[tokio::test]
async fn test_retry_policy_non_retryable_errors() {
    let policy = RetryPolicy::default()
        .with_non_retryable_error("ValidationError")
        .with_non_retryable_error("AuthenticationError");

    assert!(!policy.is_error_retryable(Some("ValidationError")));
    assert!(!policy.is_error_retryable(Some("AuthenticationError")));
    assert!(policy.is_error_retryable(Some("TransientError")));
    assert!(policy.is_error_retryable(Some("TimeoutError")));
    assert!(policy.is_error_retryable(None)); // Unknown errors are retryable
}

#[tokio::test]
async fn test_retry_policy_attempts_remaining() {
    let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);

    assert!(policy.has_attempts_remaining(1));
    assert!(policy.has_attempts_remaining(2));
    assert!(!policy.has_attempts_remaining(3));
    assert!(!policy.has_attempts_remaining(4));
}

#[tokio::test]
async fn test_retry_policy_unlimited_attempts() {
    let policy = RetryPolicy::new(0, 1.0, 60.0, 2.0); // 0 = unlimited

    assert!(policy.has_attempts_remaining(1));
    assert!(policy.has_attempts_remaining(100));
    assert!(policy.has_attempts_remaining(9999));
}

// ================================================================================================
// RETRY EVALUATOR TESTS
// ================================================================================================

#[tokio::test]
async fn test_evaluator_retry_decision() {
    let evaluator = RetryPolicyEvaluator::new();
    let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);
    let policy_json = serde_json::to_value(&policy).unwrap();

    // Attempt 1 should retry
    let decision = evaluator
        .evaluate_retry(1, Some(&policy_json), Some("TransientError"))
        .unwrap();

    match decision {
        RetryDecision::Retry {
            next_attempt,
            backoff_duration,
            ..
        } => {
            assert_eq!(next_attempt, 2);
            assert_eq!(backoff_duration, Duration::from_secs(2));
        }
        _ => panic!("Expected Retry decision"),
    }

    // Attempt 2 should retry
    let decision = evaluator
        .evaluate_retry(2, Some(&policy_json), Some("TransientError"))
        .unwrap();

    match decision {
        RetryDecision::Retry {
            next_attempt,
            backoff_duration,
            ..
        } => {
            assert_eq!(next_attempt, 3);
            assert_eq!(backoff_duration, Duration::from_secs(4));
        }
        _ => panic!("Expected Retry decision"),
    }

    // Attempt 3 should move to DLQ (max attempts reached)
    let decision = evaluator
        .evaluate_retry(3, Some(&policy_json), Some("TransientError"))
        .unwrap();

    match decision {
        RetryDecision::MoveToDlq { reason } => match reason {
            DlqReason::MaxAttemptsExceeded {
                max_attempts,
                current_attempt,
            } => {
                assert_eq!(max_attempts, 3);
                assert_eq!(current_attempt, 3);
            }
            _ => panic!("Expected MaxAttemptsExceeded"),
        },
        _ => panic!("Expected MoveToDlq decision"),
    }
}

#[tokio::test]
async fn test_evaluator_non_retryable_error() {
    let evaluator = RetryPolicyEvaluator::new();
    let policy = RetryPolicy::default()
        .with_non_retryable_error("ValidationError")
        .with_non_retryable_error("BadRequestError");
    let policy_json = serde_json::to_value(&policy).unwrap();

    let decision = evaluator
        .evaluate_retry(1, Some(&policy_json), Some("ValidationError"))
        .unwrap();

    match decision {
        RetryDecision::MoveToDlq { reason } => match reason {
            DlqReason::NonRetryableError { error_type } => {
                assert_eq!(error_type, "ValidationError");
            }
            _ => panic!("Expected NonRetryableError"),
        },
        _ => panic!("Expected MoveToDlq decision"),
    }
}

#[tokio::test]
async fn test_evaluator_default_policy_when_none_provided() {
    let evaluator = RetryPolicyEvaluator::new();

    // When no policy is provided, use default policy
    let decision = evaluator
        .evaluate_retry(1, None, Some("TransientError"))
        .unwrap();

    match decision {
        RetryDecision::Retry { next_attempt, .. } => {
            assert_eq!(next_attempt, 2);
        }
        _ => panic!("Expected Retry decision with default policy"),
    }
}

#[tokio::test]
async fn test_evaluator_invalid_policy() {
    let evaluator = RetryPolicyEvaluator::new();
    let invalid_json = json!({ "invalid": "policy" });

    let decision = evaluator
        .evaluate_retry(1, Some(&invalid_json), Some("TransientError"))
        .unwrap();

    // A malformed policy falls back to the default (retrying) policy rather than the DLQ.
    match decision {
        RetryDecision::Retry { next_attempt, .. } => assert_eq!(next_attempt, 2),
        _ => panic!("Expected Retry decision (invalid policy falls back to default)"),
    }
}

#[tokio::test]
async fn test_evaluator_timeout_retry() {
    let evaluator = RetryPolicyEvaluator::new();
    let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);
    let policy_json = serde_json::to_value(&policy).unwrap();

    let decision = evaluator
        .evaluate_timeout_retry(1, Some(&policy_json), "ScheduleToStartTimeout")
        .unwrap();

    match decision {
        RetryDecision::Retry {
            next_attempt,
            backoff_duration,
            ..
        } => {
            assert_eq!(next_attempt, 2);
            assert_eq!(backoff_duration, Duration::from_secs(2));
        }
        _ => panic!("Expected Retry decision for timeout"),
    }
}

// ================================================================================================
// RETRY SCHEDULER TESTS (DB Integration)
// ================================================================================================

#[tokio::test]
#[ignore = "Requires database"]
async fn test_scheduler_handle_failure_with_retry() {
    let pool = setup_test_db().await;
    let scheduler = TaskRetryScheduler::new(pool.clone());

    let workflow_id = WorkflowId::new_v4();
    let execution_id = ExecutionId::new_v4();

    // Create a failed task with retry policy
    let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);
    let policy_json = serde_json::to_value(&policy).unwrap();

    let mut failed_task = create_test_task_execution(
        "task-1".to_string(),
        workflow_id,
        execution_id,
        1,
        "failed",
        Some(policy_json),
    );
    failed_task.error_type = Some("TransientError".to_string());

    insert_task_execution(&pool, &failed_task)
        .await
        .expect("Failed to insert task execution");

    // Handle the failure
    let outcome = scheduler.handle_task_failure(&failed_task).await.unwrap();

    match outcome {
        RetryOutcome::Rescheduled {
            next_attempt,
            new_task_execution_id,
            ..
        } => {
            assert_eq!(next_attempt, 2);

            // Verify new task was created in database
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_executions WHERE id = $1 AND status = 'scheduled'",
            )
            .bind(new_task_execution_id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();

            assert_eq!(count, 1);
        }
        _ => panic!("Expected Rescheduled outcome"),
    }
}

#[tokio::test]
#[ignore = "Requires database"]
async fn test_scheduler_handle_failure_moves_to_dlq() {
    let pool = setup_test_db().await;
    let scheduler = TaskRetryScheduler::new(pool.clone());

    let workflow_id = WorkflowId::new_v4();
    let execution_id = ExecutionId::new_v4();

    // Create a failed task with non-retryable error
    let policy = RetryPolicy::default().with_non_retryable_error("ValidationError");
    let policy_json = serde_json::to_value(&policy).unwrap();

    let mut failed_task = create_test_task_execution(
        "task-1".to_string(),
        workflow_id,
        execution_id,
        1,
        "failed",
        Some(policy_json),
    );
    failed_task.error_type = Some("ValidationError".to_string());

    insert_task_execution(&pool, &failed_task)
        .await
        .expect("Failed to insert task execution");

    // Handle the failure
    let outcome = scheduler.handle_task_failure(&failed_task).await.unwrap();

    match outcome {
        RetryOutcome::MovedToDlq { reason } => match reason {
            DlqReason::NonRetryableError { error_type } => {
                assert_eq!(error_type, "ValidationError");
            }
            _ => panic!("Expected NonRetryableError"),
        },
        _ => panic!("Expected MovedToDlq outcome"),
    }

    // Verify metadata was updated with DLQ reason
    let metadata: serde_json::Value =
        sqlx::query_scalar("SELECT metadata FROM task_executions WHERE id = $1")
            .bind(failed_task.id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();

    assert!(metadata.get("dlq_reason").is_some());
    assert!(metadata.get("dlq_timestamp").is_some());
}

#[tokio::test]
#[ignore = "Requires database"]
async fn test_scheduler_handle_failure_max_attempts() {
    let pool = setup_test_db().await;
    let scheduler = TaskRetryScheduler::new(pool.clone());

    let workflow_id = WorkflowId::new_v4();
    let execution_id = ExecutionId::new_v4();

    // Create a failed task at max attempts
    let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);
    let policy_json = serde_json::to_value(&policy).unwrap();

    let failed_task = create_test_task_execution(
        "task-1".to_string(),
        workflow_id,
        execution_id,
        3, // At max attempts
        "failed",
        Some(policy_json),
    );

    insert_task_execution(&pool, &failed_task)
        .await
        .expect("Failed to insert task execution");

    // Handle the failure
    let outcome = scheduler.handle_task_failure(&failed_task).await.unwrap();

    match outcome {
        RetryOutcome::MovedToDlq { reason } => match reason {
            DlqReason::MaxAttemptsExceeded {
                max_attempts,
                current_attempt,
            } => {
                assert_eq!(max_attempts, 3);
                assert_eq!(current_attempt, 3);
            }
            _ => panic!("Expected MaxAttemptsExceeded"),
        },
        _ => panic!("Expected MovedToDlq outcome"),
    }
}

#[tokio::test]
#[ignore = "Requires database"]
async fn test_scheduler_handle_timeout() {
    let pool = setup_test_db().await;
    let scheduler = TaskRetryScheduler::new(pool.clone());

    let workflow_id = WorkflowId::new_v4();
    let execution_id = ExecutionId::new_v4();

    // Create a timed-out task
    let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);
    let policy_json = serde_json::to_value(&policy).unwrap();

    let timed_out_task = create_test_task_execution(
        "task-1".to_string(),
        workflow_id,
        execution_id,
        1,
        "timed_out",
        Some(policy_json),
    );

    insert_task_execution(&pool, &timed_out_task)
        .await
        .expect("Failed to insert task execution");

    // Handle the timeout
    let outcome = scheduler
        .handle_task_timeout(&timed_out_task, "StartToCloseTimeout")
        .await
        .unwrap();

    match outcome {
        RetryOutcome::Rescheduled {
            next_attempt,
            new_task_execution_id,
            ..
        } => {
            assert_eq!(next_attempt, 2);

            // Verify new task was created
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM task_executions WHERE id = $1")
                    .bind(new_task_execution_id.as_uuid())
                    .fetch_one(&pool)
                    .await
                    .unwrap();

            assert_eq!(count, 1);
        }
        _ => panic!("Expected Rescheduled outcome"),
    }
}

#[tokio::test]
#[ignore = "Requires database"]
async fn test_scheduler_retry_stats() {
    let pool = setup_test_db().await;
    let scheduler = TaskRetryScheduler::new(pool.clone());

    let workflow_id = WorkflowId::new_v4();
    let execution_id = ExecutionId::new_v4();

    // Create multiple task executions with different attempts
    let policy_json = serde_json::to_value(RetryPolicy::new(5, 1.0, 60.0, 2.0)).unwrap();

    // Task 1 - attempt 1 (completed)
    let task1 = create_test_task_execution(
        "task-1".to_string(),
        workflow_id,
        execution_id,
        1,
        "completed",
        Some(policy_json.clone()),
    );
    insert_task_execution(&pool, &task1)
        .await
        .expect("Failed to insert task execution");

    // Task 2 - attempt 1 (failed, retried)
    let task2_attempt1 = create_test_task_execution(
        "task-2".to_string(),
        workflow_id,
        execution_id,
        1,
        "failed",
        Some(policy_json.clone()),
    );
    insert_task_execution(&pool, &task2_attempt1)
        .await
        .expect("Failed to insert task execution");

    // Task 2 - attempt 2 (completed)
    let task2_attempt2 = create_test_task_execution(
        "task-2".to_string(),
        workflow_id,
        execution_id,
        2,
        "completed",
        Some(policy_json.clone()),
    );
    insert_task_execution(&pool, &task2_attempt2)
        .await
        .expect("Failed to insert task execution");

    // Task 3 - attempt 1, 2, 3 (all failed, moved to DLQ)
    for attempt in 1..=3 {
        let mut task3 = create_test_task_execution(
            "task-3".to_string(),
            workflow_id,
            execution_id,
            attempt,
            "failed",
            Some(policy_json.clone()),
        );
        if attempt == 3 {
            task3.metadata = json!({ "dlq_reason": "MaxAttemptsExceeded" });
        }
        insert_task_execution(&pool, &task3)
            .await
            .expect("Failed to insert task execution");
    }

    // Get retry stats
    let stats = scheduler
        .get_retry_stats(&workflow_id, &execution_id)
        .await
        .unwrap();

    assert_eq!(stats.total_tasks, 6); // 1 + 2 + 3 = 6 total attempts
    assert_eq!(stats.unique_tasks, 3); // 3 unique task IDs
    assert_eq!(stats.retried_tasks, 4); // 4 attempts with attempt_number > 1
    assert_eq!(stats.max_attempts, 3);
    assert_eq!(stats.dlq_tasks, 1); // Only task 3 attempt 3 has DLQ reason
}

#[tokio::test]
#[ignore = "Requires database"]
async fn test_scheduler_find_ready_retries() {
    let pool = setup_test_db().await;
    let scheduler = TaskRetryScheduler::new(pool.clone());

    let workflow_id = WorkflowId::new_v4();
    let execution_id = ExecutionId::new_v4();

    let policy_json = serde_json::to_value(RetryPolicy::new(3, 1.0, 60.0, 2.0)).unwrap();

    // Create a scheduled retry in the past (ready)
    let mut ready_task = create_test_task_execution(
        "task-1".to_string(),
        workflow_id,
        execution_id,
        2,
        "scheduled",
        Some(policy_json.clone()),
    );
    ready_task.scheduled_at = Timestamptz::now() - SignedDuration::from_secs(10);
    insert_task_execution(&pool, &ready_task)
        .await
        .expect("Failed to insert task execution");

    // Create a scheduled retry in the future (not ready)
    let mut future_task = create_test_task_execution(
        "task-2".to_string(),
        workflow_id,
        execution_id,
        2,
        "scheduled",
        Some(policy_json.clone()),
    );
    future_task.scheduled_at = Timestamptz::now() + SignedDuration::from_secs(60);
    insert_task_execution(&pool, &future_task)
        .await
        .expect("Failed to insert task execution");

    // Find ready retries
    let ready_retries = scheduler.find_ready_retries().await.unwrap();

    // Should only find the ready task
    assert_eq!(ready_retries.len(), 1);
    assert_eq!(ready_retries[0].task_id, "task-1");
    assert_eq!(ready_retries[0].attempt_number, 2);
}

#[tokio::test]
#[ignore = "Requires database"]
async fn test_scheduler_should_retry_check() {
    let pool = setup_test_db().await;
    let scheduler = TaskRetryScheduler::new(pool.clone());

    let workflow_id = WorkflowId::new_v4();
    let execution_id = ExecutionId::new_v4();

    // Task that should retry
    let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);
    let policy_json = serde_json::to_value(&policy).unwrap();

    let retryable_task = create_test_task_execution(
        "task-1".to_string(),
        workflow_id,
        execution_id,
        1,
        "failed",
        Some(policy_json.clone()),
    );

    assert!(scheduler.should_retry_task(&retryable_task));

    // Task at max attempts should not retry
    let max_attempts_task = create_test_task_execution(
        "task-2".to_string(),
        workflow_id,
        execution_id,
        3,
        "failed",
        Some(policy_json),
    );

    assert!(!scheduler.should_retry_task(&max_attempts_task));

    // Task with non-retryable error should not retry
    let policy_with_non_retryable =
        RetryPolicy::default().with_non_retryable_error("ValidationError");
    let policy_json = serde_json::to_value(&policy_with_non_retryable).unwrap();

    let mut non_retryable_task = create_test_task_execution(
        "task-3".to_string(),
        workflow_id,
        execution_id,
        1,
        "failed",
        Some(policy_json),
    );
    non_retryable_task.error_type = Some("ValidationError".to_string());

    assert!(!scheduler.should_retry_task(&non_retryable_task));
}

// ================================================================================================
// EDGE CASES
// ================================================================================================

#[tokio::test]
async fn test_zero_max_attempts_unlimited_retries() {
    let policy = RetryPolicy::new(0, 1.0, 60.0, 2.0); // 0 = unlimited

    assert!(policy.has_attempts_remaining(1));
    assert!(policy.has_attempts_remaining(100));
    assert!(policy.has_attempts_remaining(9999));
}

#[tokio::test]
async fn test_backoff_coefficient_one_constant_backoff() {
    let policy = RetryPolicy::new(5, 5.0, 100.0, 1.0);

    // With coefficient 1.0, backoff is constant
    assert_eq!(policy.calculate_backoff(1), Duration::from_secs(5));
    assert_eq!(policy.calculate_backoff(2), Duration::from_secs(5));
    assert_eq!(policy.calculate_backoff(3), Duration::from_secs(5));
    assert_eq!(policy.calculate_backoff(10), Duration::from_secs(5));
}

#[tokio::test]
async fn test_very_large_backoff_coefficient() {
    let policy = RetryPolicy::new(5, 1.0, 1000.0, 10.0);

    // Even with large coefficient, should cap at max_interval
    assert_eq!(policy.calculate_backoff(1), Duration::from_secs(1));
    assert_eq!(policy.calculate_backoff(2), Duration::from_secs(10));
    assert_eq!(policy.calculate_backoff(3), Duration::from_secs(100));
    assert_eq!(policy.calculate_backoff(4), Duration::from_secs(1000)); // Capped
}

#[tokio::test]
async fn test_retry_policy_serialization_roundtrip() {
    let policy = RetryPolicy::new(5, 2.0, 300.0, 1.5)
        .with_non_retryable_error("ValidationError")
        .with_non_retryable_error("AuthError");

    // Serialize
    let json = serde_json::to_string(&policy).unwrap();

    // Deserialize
    let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.max_attempts, 5);
    assert_eq!(deserialized.initial_interval_seconds, 2.0);
    assert_eq!(deserialized.max_interval_seconds, 300.0);
    assert_eq!(deserialized.backoff_coefficient, 1.5);
    assert_eq!(deserialized.non_retryable_error_types.len(), 2);
}
