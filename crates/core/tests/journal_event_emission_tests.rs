//! # Journal Event Emission Tests
//!
//! Integration tests for journal event emission
//!
//! These tests verify that TaskCompleted and TaskFailed events are properly
//! appended to the canonical journal when tasks complete or fail, enabling
//! complete event sourcing and workflow history reconstruction.
//!
//! ## Test Coverage
//!
//! 1. TaskCompleted event is appended to journal on task success
//! 2. TaskFailed event is appended to journal on task failure
//! 3. Journal contains correct event attributes
//! 4. Event IDs are sequential and monotonically increasing
//! 5. Multiple task events in same workflow
//! 6. Journal can reconstruct complete task lifecycle (Scheduled → Completed/Failed)
//!
//! ## Event Sourcing Verification
//!
//! The journal should contain a complete event history:
//! - WorkflowStarted → TaskScheduled → TaskCompleted → WorkflowCompleted
//! - WorkflowStarted → TaskScheduled → TaskFailed → WorkflowFailed
//!
//! This enables:
//! - Complete audit trail for compliance
//! - Workflow replay from journal alone
//! - Debugging with full execution history
//! - Durable execution event sourcing model

use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::schema::types::execution::WorkflowExecution;
use orcher_storage::schema::types::journal::JournalEntry;
use sqlx::PgPool;
use uuid::Uuid;

/// Create a test database pool
async fn create_test_pool() -> PgPool {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/orcher_test".to_string());

    PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test database")
}

/// Create a test workflow execution in the database
async fn create_test_workflow(
    pool: &PgPool,
    workflow_id: Uuid,
    execution_id: &str,
) -> WorkflowExecution {
    let repo = WorkflowExecutionRepository::new(pool.clone());

    let workflow = WorkflowExecution {
        bound_version_id: None,
        id: workflow_id.into(),
        execution_id: execution_id.to_string(),
        registry_id: None,
        workflow_name: "test-workflow".to_string(),
        workflow_type: "test-workflow".to_string(),
        workflow_version: "1.0.0".to_string(),
        namespace: "test-namespace".to_string(),
        status: "running".to_string(),
        parameters: serde_json::json!({}),
        result: None,
        error: None,
        error_details: None,
        context: serde_json::json!({}),
        environment: serde_json::json!({}),
        progress: None,
        current_step: None,
        statistics: None,
        total_steps: 0,
        completed_steps: 0,
        failed_steps: 0,
        retried_steps: 0,
        duration_ms: None,
        cpu_time_ms: None,
        memory_peak_bytes: None,
        average_step_duration_ms: None,
        idempotency_key: None,
        priority: 0,
        timeout_secs: None,
        execution_deadline: None,
        run_timeout_secs: None,
        task_timeout_secs: None,
        retry_count: 0,
        max_retries: 3,
        tags: serde_json::json!({}),
        labels: serde_json::json!({}),
        trigger_type: "manual".to_string(),
        triggered_by: None,
        parent_execution_id: None,
        root_execution_id: None,
        user_id: None,
        organization_id: None,
        submitted_at: Timestamptz::now(),
        started_at: Some(Timestamptz::now()),
        completed_at: None,
        cancelled_at: None,
        created_at: Timestamptz::now(),
        updated_at: Timestamptz::now(),
        claimed_by: None,
        claimed_at: None,
        claim_timeout_at: None,
        task_queue: "test-queue".to_string(),
        last_retry_at: None,
        next_retry_at: None,
        retries_settled_at: None,
        pending_requeue: false,
        pending_requeue_at: None,
        activation_seq: 0,
        workflow_id: None,
        shard_id: None,
        retry_policy: None,
        holds_workflow_id: false,
        annotations: serde_json::json!({}),
        start_request_id: None,
        start_after: None,
        awaiting_start: false,
        step_id_scheme: None,
        history_lineage: None,
        activated_without_scheme: false,
        step_results_journaled: true,
    };

    repo.create(&workflow)
        .await
        .expect("Failed to create test workflow")
}

/// Append a TaskScheduled event to set up test scenario
async fn append_task_scheduled_event(
    pool: &PgPool,
    workflow_id: &str,
    execution_id: &str,
    task_id: &str,
) -> i64 {
    let repo = JournalRepository::new(pool.clone());

    let attributes = serde_json::json!({
        "task_id": task_id,
        "task_type": "test-task-type",
        "input": {"test": "data"},
    });

    let entry = JournalEntry::new(
        workflow_id.to_string(),
        execution_id.to_string(),
        0,
        "TaskScheduled".to_string(),
        attributes,
    );

    repo.append_next(entry)
        .await
        .expect("Failed to append TaskScheduled event")
        .event_id
}

/// Simulate task completion by appending TaskCompleted event
async fn append_task_completed_event(
    pool: &PgPool,
    workflow_id: &str,
    execution_id: &str,
    task_id: &str,
) -> JournalEntry {
    let repo = JournalRepository::new(pool.clone());

    let attributes = serde_json::json!({
        "task_id": task_id,
        "result": b"task result bytes",
        "execution_attempt": 1,
        "duration_ms": 150,
        "completed_at": Timestamptz::now().to_rfc3339(),
    });

    let entry = JournalEntry::new(
        workflow_id.to_string(),
        execution_id.to_string(),
        0,
        "TaskCompleted".to_string(),
        attributes,
    );

    repo.append_next(entry)
        .await
        .expect("Failed to append TaskCompleted event")
}

/// Simulate task failure by appending TaskFailed event
async fn append_task_failed_event(
    pool: &PgPool,
    workflow_id: &str,
    execution_id: &str,
    task_id: &str,
) -> JournalEntry {
    let repo = JournalRepository::new(pool.clone());

    let attributes = serde_json::json!({
        "task_id": task_id,
        "error_message": "Task failed intentionally for test",
        "error_details": {
            "error_code": "TEST_ERROR",
            "stack_trace": "test stack trace"
        },
        "execution_attempt": 1,
        "duration_ms": 100,
        "failed_at": Timestamptz::now().to_rfc3339(),
    });

    let entry = JournalEntry::new(
        workflow_id.to_string(),
        execution_id.to_string(),
        0,
        "TaskFailed".to_string(),
        attributes,
    );

    repo.append_next(entry)
        .await
        .expect("Failed to append TaskFailed event")
}

#[tokio::test]
#[ignore] // Requires test database
async fn test_task_completed_event_appended_to_journal() {
    let pool = create_test_pool().await;
    let workflow_id = Uuid::new_v4();
    let workflow_id_str = workflow_id.to_string();
    let execution_id = "test-exec-1";

    // Create workflow
    create_test_workflow(&pool, workflow_id, execution_id).await;

    // Append TaskScheduled
    let scheduled_event_id =
        append_task_scheduled_event(&pool, &workflow_id_str, execution_id, "task-1").await;

    // Append TaskCompleted
    let completed_entry =
        append_task_completed_event(&pool, &workflow_id_str, execution_id, "task-1").await;

    // Verify TaskCompleted event
    assert_eq!(completed_entry.event_type, "TaskCompleted");
    assert_eq!(completed_entry.workflow_id, workflow_id_str);
    assert_eq!(completed_entry.execution_id, execution_id);
    assert!(completed_entry.event_id > scheduled_event_id);

    // Verify attributes
    assert_eq!(
        completed_entry.payload.get("task_id").unwrap().as_str(),
        Some("task-1")
    );
    assert!(completed_entry.payload.get("result").is_some());
    assert!(completed_entry.payload.get("execution_attempt").is_some());
    assert!(completed_entry.payload.get("duration_ms").is_some());
    assert!(completed_entry.payload.get("completed_at").is_some());

    println!(
        "✅ TaskCompleted event appended to journal with event_id {}",
        completed_entry.event_id
    );
}

#[tokio::test]
#[ignore] // Requires test database
async fn test_task_failed_event_appended_to_journal() {
    let pool = create_test_pool().await;
    let workflow_id = Uuid::new_v4();
    let workflow_id_str = workflow_id.to_string();
    let execution_id = "test-exec-2";

    // Create workflow
    create_test_workflow(&pool, workflow_id, execution_id).await;

    // Append TaskScheduled
    let scheduled_event_id =
        append_task_scheduled_event(&pool, &workflow_id_str, execution_id, "task-fail").await;

    // Append TaskFailed
    let failed_entry =
        append_task_failed_event(&pool, &workflow_id_str, execution_id, "task-fail").await;

    // Verify TaskFailed event
    assert_eq!(failed_entry.event_type, "TaskFailed");
    assert_eq!(failed_entry.workflow_id, workflow_id_str);
    assert_eq!(failed_entry.execution_id, execution_id);
    assert!(failed_entry.event_id > scheduled_event_id);

    // Verify attributes
    assert_eq!(
        failed_entry.payload.get("task_id").unwrap().as_str(),
        Some("task-fail")
    );
    assert!(failed_entry.payload.get("error_message").is_some());
    assert!(failed_entry.payload.get("error_details").is_some());
    assert!(failed_entry.payload.get("execution_attempt").is_some());
    assert!(failed_entry.payload.get("failed_at").is_some());

    println!(
        "✅ TaskFailed event appended to journal with event_id {}",
        failed_entry.event_id
    );
}

#[tokio::test]
#[ignore] // Requires test database
async fn test_journal_event_ids_are_sequential() {
    let pool = create_test_pool().await;
    let workflow_id = Uuid::new_v4();
    let workflow_id_str = workflow_id.to_string();
    let execution_id = "test-exec-3";

    // Create workflow
    create_test_workflow(&pool, workflow_id, execution_id).await;

    // Append multiple events
    let event1 = append_task_scheduled_event(&pool, &workflow_id_str, execution_id, "task-1").await;
    let event2 = append_task_completed_event(&pool, &workflow_id_str, execution_id, "task-1").await;
    let event3 = append_task_scheduled_event(&pool, &workflow_id_str, execution_id, "task-2").await;
    let event4 = append_task_failed_event(&pool, &workflow_id_str, execution_id, "task-2").await;

    // Verify sequential ordering
    assert!(event2.event_id > event1);
    assert!(event3 > event2.event_id);
    assert!(event4.event_id > event3);

    println!(
        "✅ Event IDs are sequential: {} → {} → {} → {}",
        event1, event2.event_id, event3, event4.event_id
    );
}

#[tokio::test]
#[ignore] // Requires test database
async fn test_journal_reconstruct_complete_task_lifecycle() {
    let pool = create_test_pool().await;
    let workflow_id = Uuid::new_v4();
    let workflow_id_str = workflow_id.to_string();
    let execution_id = "test-exec-4";
    let repo = JournalRepository::new(pool.clone());

    // Create workflow
    create_test_workflow(&pool, workflow_id, execution_id).await;

    // Task 1: Success path
    append_task_scheduled_event(&pool, &workflow_id_str, execution_id, "task-success").await;
    append_task_completed_event(&pool, &workflow_id_str, execution_id, "task-success").await;

    // Task 2: Failure path
    append_task_scheduled_event(&pool, &workflow_id_str, execution_id, "task-failure").await;
    append_task_failed_event(&pool, &workflow_id_str, execution_id, "task-failure").await;

    // Query complete journal
    let journal = repo
        .get_journal(&workflow_id_str, execution_id)
        .await
        .expect("Failed to get journal");

    // Verify we have 4 events
    assert_eq!(journal.len(), 4);

    // Verify event sequence
    assert_eq!(journal[0].event_type, "TaskScheduled");
    assert_eq!(journal[1].event_type, "TaskCompleted");
    assert_eq!(journal[2].event_type, "TaskScheduled");
    assert_eq!(journal[3].event_type, "TaskFailed");

    // Verify we can reconstruct task lifecycle
    let task_success_scheduled = &journal[0];
    let task_success_completed = &journal[1];
    assert_eq!(
        task_success_scheduled
            .payload
            .get("task_id")
            .unwrap()
            .as_str(),
        task_success_completed
            .payload
            .get("task_id")
            .unwrap()
            .as_str()
    );

    let task_failure_scheduled = &journal[2];
    let task_failure_failed = &journal[3];
    assert_eq!(
        task_failure_scheduled
            .payload
            .get("task_id")
            .unwrap()
            .as_str(),
        task_failure_failed.payload.get("task_id").unwrap().as_str()
    );

    println!("✅ Journal contains complete task lifecycle:");
    println!("  TaskScheduled(task-success) → TaskCompleted(task-success)");
    println!("  TaskScheduled(task-failure) → TaskFailed(task-failure)");
}

#[tokio::test]
#[ignore] // Requires test database
async fn test_multiple_tasks_same_workflow_journal() {
    let pool = create_test_pool().await;
    let workflow_id = Uuid::new_v4();
    let workflow_id_str = workflow_id.to_string();
    let execution_id = "test-exec-5";
    let repo = JournalRepository::new(pool.clone());

    // Create workflow
    create_test_workflow(&pool, workflow_id, execution_id).await;

    // Multiple tasks
    for i in 1..=5 {
        let task_id = format!("task-{}", i);
        append_task_scheduled_event(&pool, &workflow_id_str, execution_id, &task_id).await;

        if i % 2 == 0 {
            // Even tasks fail
            append_task_failed_event(&pool, &workflow_id_str, execution_id, &task_id).await;
        } else {
            // Odd tasks succeed
            append_task_completed_event(&pool, &workflow_id_str, execution_id, &task_id).await;
        }
    }

    // Query journal
    let journal = repo
        .get_journal(&workflow_id_str, execution_id)
        .await
        .expect("Failed to get journal");

    // Should have 10 events (5 scheduled + 5 completed/failed)
    assert_eq!(journal.len(), 10);

    // Count event types
    let scheduled_count = journal
        .iter()
        .filter(|e| e.event_type == "TaskScheduled")
        .count();
    let completed_count = journal
        .iter()
        .filter(|e| e.event_type == "TaskCompleted")
        .count();
    let failed_count = journal
        .iter()
        .filter(|e| e.event_type == "TaskFailed")
        .count();

    assert_eq!(scheduled_count, 5);
    assert_eq!(completed_count, 3); // Tasks 1, 3, 5
    assert_eq!(failed_count, 2); // Tasks 2, 4

    println!("✅ Journal contains events for all 5 tasks:");
    println!("  - {} TaskScheduled events", scheduled_count);
    println!("  - {} TaskCompleted events", completed_count);
    println!("  - {} TaskFailed events", failed_count);
}

#[tokio::test]
#[ignore] // Requires test database
async fn test_journal_event_attributes_completeness() {
    let pool = create_test_pool().await;
    let workflow_id = Uuid::new_v4();
    let workflow_id_str = workflow_id.to_string();
    let execution_id = "test-exec-6";
    let repo = JournalRepository::new(pool.clone());

    // Create workflow
    create_test_workflow(&pool, workflow_id, execution_id).await;

    // Append task events
    append_task_scheduled_event(&pool, &workflow_id_str, execution_id, "test-task").await;
    append_task_completed_event(&pool, &workflow_id_str, execution_id, "test-task").await;

    // Query journal
    let journal = repo
        .get_journal(&workflow_id_str, execution_id)
        .await
        .expect("Failed to get journal");

    let completed_event = &journal[1];

    // Verify all required attributes are present
    let attrs = &completed_event.payload;
    assert!(attrs.get("task_id").is_some(), "Missing task_id");
    assert!(attrs.get("result").is_some(), "Missing result");
    assert!(
        attrs.get("execution_attempt").is_some(),
        "Missing execution_attempt"
    );
    assert!(attrs.get("duration_ms").is_some(), "Missing duration_ms");
    assert!(attrs.get("completed_at").is_some(), "Missing completed_at");

    // Verify timestamp is valid RFC3339
    let completed_at = attrs.get("completed_at").unwrap().as_str().unwrap();
    Timestamptz::parse_rfc3339(completed_at).expect("Invalid timestamp format");

    println!("✅ TaskCompleted event has all required attributes:");
    println!("  - task_id: {:?}", attrs.get("task_id"));
    println!(
        "  - execution_attempt: {:?}",
        attrs.get("execution_attempt")
    );
    println!("  - duration_ms: {:?}", attrs.get("duration_ms"));
    println!("  - completed_at: {}", completed_at);
}

#[tokio::test]
#[ignore] // Requires test database
async fn test_journal_event_sourcing_pattern() {
    let pool = create_test_pool().await;
    let workflow_id = Uuid::new_v4();
    let workflow_id_str = workflow_id.to_string();
    let execution_id = "test-exec-7";
    let repo = JournalRepository::new(pool.clone());

    // Create workflow
    create_test_workflow(&pool, workflow_id, execution_id).await;

    // Simulate complete workflow execution with event sourcing
    // This uses Orcher's execution journal for durable state

    // Event 1: Task scheduled
    append_task_scheduled_event(&pool, &workflow_id_str, execution_id, "fetch-user").await;

    // Event 2: Task completed
    append_task_completed_event(&pool, &workflow_id_str, execution_id, "fetch-user").await;

    // Event 3: Another task scheduled
    append_task_scheduled_event(&pool, &workflow_id_str, execution_id, "send-email").await;

    // Event 4: Task completed
    append_task_completed_event(&pool, &workflow_id_str, execution_id, "send-email").await;

    // Query journal
    let journal = repo
        .get_journal(&workflow_id_str, execution_id)
        .await
        .expect("Failed to get journal");

    // Verify complete event sourcing trail exists
    assert_eq!(journal.len(), 4);

    // Verify we can replay workflow from journal alone
    let mut task_results = std::collections::HashMap::new();

    for entry in &journal {
        match entry.event_type.as_str() {
            "TaskScheduled" => {
                let task_id = entry.payload.get("task_id").unwrap().as_str().unwrap();
                println!("Replay: Task scheduled - {}", task_id);
            }
            "TaskCompleted" => {
                let task_id = entry.payload.get("task_id").unwrap().as_str().unwrap();
                let result = entry.payload.get("result").unwrap();
                task_results.insert(task_id.to_string(), result.clone());
                println!("Replay: Task completed - {}", task_id);
            }
            _ => {}
        }
    }

    // Verify we reconstructed task results from journal
    assert_eq!(task_results.len(), 2);
    assert!(task_results.contains_key("fetch-user"));
    assert!(task_results.contains_key("send-email"));

    println!("✅ Journal supports durable execution event sourcing:");
    println!("  - Complete event history captured");
    println!("  - Workflow can be replayed from journal");
    println!("  - Task results reconstructed: {:?}", task_results.keys());
}
