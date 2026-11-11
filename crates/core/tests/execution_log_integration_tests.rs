//! Execution Log Repository Integration Tests
//!
//! Tests focusing on ExecutionLogRepository core functionality:
//! 1. CRUD operations (store, retrieve, update, delete)
//! 2. Step result persistence for different step types
//! 3. Query operations (by workflow_id, step_type, status)
//! 4. Data integrity and idempotency
//!
//! **Scope:** Repository-level integration with PostgreSQL
//! **Note:** For storage-layer tests with complex scenarios, see execution_log_storage_tests.rs
//! **Note:** For full end-to-end tests with SDK flow, see durable_closure_e2e_tests.rs

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::{
    ExecutionLogEntry, ExecutionLogRepository, ExecutionLogStatus, ExecutionStepType,
    PostgresExecutionLogRepository,
};
use sqlx::PgPool;
use uuid::Uuid;

// ============================================================================
// Test Utilities
// ============================================================================

/// Setup test database connection
async fn setup_test_db() -> PgPool {
    // Get database URL from environment or use default
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/orcher_test".to_string());

    // Create connection pool
    let pool = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test database. Set TEST_DATABASE_URL env var.");

    // Note: Database must be pre-migrated before running tests
    // Run: cargo sqlx migrate run --source crates/storage/src/migrations/sql/postgres

    pool
}

/// Clean up test data
async fn cleanup_test_data(pool: &PgPool, workflow_id: &Uuid) {
    sqlx::query("DELETE FROM execution_log WHERE workflow_id = $1")
        .bind(workflow_id)
        .execute(pool)
        .await
        .ok();
}

// ============================================================================
// Integration Tests
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database setup
async fn test_closure_result_persistence() {
    let pool = setup_test_db().await;
    let repo = PostgresExecutionLogRepository::new(pool.clone());

    let workflow_id = Uuid::new_v4();

    // Create a closure execution entry
    let entry = ExecutionLogEntry {
        workflow_id,
        step_name: "fetch_user_1".to_string(),
        step_type: ExecutionStepType::Closure,
        execution_attempt: 1,
        status: ExecutionLogStatus::Completed,
        input: None,
        result: Some(serde_json::json!({
            "user_id": 123,
            "name": "Test User",
            "email": "test@example.com"
        })),
        error_message: None,
        error_details: None,
        created_at: Timestamptz::now(),
        started_at: Some(Timestamptz::now()),
        completed_at: Some(Timestamptz::now()),
        duration_ms: Some(150),
        retry_count: 0,
        last_error: None,
    };

    // Store the entry
    repo.store_entry(&entry)
        .await
        .expect("Failed to store entry");

    // Retrieve and verify
    let retrieved = repo
        .get_entry(workflow_id, "fetch_user_1")
        .await
        .expect("Failed to retrieve entry")
        .expect("Entry not found");

    assert_eq!(retrieved.step_name, "fetch_user_1");
    assert_eq!(retrieved.step_type, ExecutionStepType::Closure);
    assert_eq!(retrieved.status, ExecutionLogStatus::Completed);
    assert!(retrieved.result.is_some());
    assert_eq!(retrieved.result.unwrap()["user_id"], 123);

    // Cleanup
    cleanup_test_data(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires test database setup
async fn test_multiple_closures_for_workflow() {
    let pool = setup_test_db().await;
    let repo = PostgresExecutionLogRepository::new(pool.clone());

    let workflow_id = Uuid::new_v4();

    // Store multiple closure results
    let closures = vec![
        ("fetch_user_1", serde_json::json!({"user_id": 1})),
        ("query_orders_2", serde_json::json!({"order_count": 5})),
        ("api_call_3", serde_json::json!({"status": "ok"})),
    ];

    for (step_name, result) in closures {
        let entry = ExecutionLogEntry {
            workflow_id,
            step_name: step_name.to_string(),
            step_type: ExecutionStepType::Closure,
            execution_attempt: 1,
            status: ExecutionLogStatus::Completed,
            input: None,
            result: Some(result),
            error_message: None,
            error_details: None,
            created_at: Timestamptz::now(),
            started_at: Some(Timestamptz::now()),
            completed_at: Some(Timestamptz::now()),
            duration_ms: Some(100),
            retry_count: 0,
            last_error: None,
        };

        repo.store_entry(&entry).await.expect("Failed to store");
    }

    // Retrieve all entries for workflow
    let entries = repo
        .list_entries(workflow_id)
        .await
        .expect("Failed to list entries");

    assert_eq!(entries.len(), 3, "Expected 3 closure entries");

    for entry in entries {
        assert_eq!(entry.step_type, ExecutionStepType::Closure);
        assert_eq!(entry.status, ExecutionLogStatus::Completed);
        assert!(entry.result.is_some());
    }

    // Cleanup
    cleanup_test_data(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires test database setup
async fn test_failed_closure_persistence() {
    let pool = setup_test_db().await;
    let repo = PostgresExecutionLogRepository::new(pool.clone());

    let workflow_id = Uuid::new_v4();

    // Create a failed closure entry
    let entry = ExecutionLogEntry {
        workflow_id,
        step_name: "failing_api_call_1".to_string(),
        step_type: ExecutionStepType::Closure,
        execution_attempt: 1,
        status: ExecutionLogStatus::Failed,
        input: None,
        result: None,
        error_message: Some("Network timeout".to_string()),
        error_details: Some(serde_json::json!({
            "message": "Network timeout",
            "stack_trace": "at example.rs:123",
            "error_type": "NetworkError"
        })),
        created_at: Timestamptz::now(),
        started_at: Some(Timestamptz::now()),
        completed_at: Some(Timestamptz::now()),
        duration_ms: Some(5000),
        retry_count: 0,
        last_error: Some("Network timeout".to_string()),
    };

    repo.store_entry(&entry)
        .await
        .expect("Failed to store failed entry");

    // Retrieve and verify failure details
    let retrieved = repo
        .get_entry(workflow_id, "failing_api_call_1")
        .await
        .expect("Failed to retrieve")
        .expect("Entry not found");

    assert_eq!(retrieved.status, ExecutionLogStatus::Failed);
    assert!(retrieved.error_message.is_some());
    assert_eq!(retrieved.error_message.unwrap(), "Network timeout");
    assert!(retrieved.error_details.is_some());

    // Cleanup
    cleanup_test_data(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires test database setup
async fn test_different_step_types() {
    let pool = setup_test_db().await;
    let repo = PostgresExecutionLogRepository::new(pool.clone());

    let workflow_id = Uuid::new_v4();

    // Create entries with different step types
    let step_types = vec![
        ("closure_step", ExecutionStepType::Closure),
        ("task_step", ExecutionStepType::Task),
        ("side_effect_step", ExecutionStepType::SideEffect),
    ];

    for (step_name, step_type) in step_types {
        let entry = ExecutionLogEntry {
            workflow_id,
            step_name: step_name.to_string(),
            step_type,
            execution_attempt: 1,
            status: ExecutionLogStatus::Completed,
            input: None,
            result: Some(serde_json::json!({"type": format!("{:?}", step_type)})),
            error_message: None,
            error_details: None,
            created_at: Timestamptz::now(),
            started_at: Some(Timestamptz::now()),
            completed_at: Some(Timestamptz::now()),
            duration_ms: Some(100),
            retry_count: 0,
            last_error: None,
        };

        repo.store_entry(&entry).await.expect("Failed to store");
    }

    // Verify we can retrieve each type
    let closure_entry = repo
        .get_entry(workflow_id, "closure_step")
        .await
        .expect("Query failed")
        .expect("Closure not found");
    assert_eq!(closure_entry.step_type, ExecutionStepType::Closure);

    let task_entry = repo
        .get_entry(workflow_id, "task_step")
        .await
        .expect("Query failed")
        .expect("Task not found");
    assert_eq!(task_entry.step_type, ExecutionStepType::Task);

    // Query by step_type using raw SQL
    let closure_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_log WHERE workflow_id = $1 AND step_type = 'closure'",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("Failed to count closures");

    assert_eq!(closure_count, 1, "Expected 1 closure");

    // Cleanup
    cleanup_test_data(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires test database setup
async fn test_upsert_idempotency() {
    let pool = setup_test_db().await;
    let repo = PostgresExecutionLogRepository::new(pool.clone());

    let workflow_id = Uuid::new_v4();

    // Store initial entry
    let entry1 = ExecutionLogEntry {
        workflow_id,
        step_name: "idempotent_step".to_string(),
        step_type: ExecutionStepType::Closure,
        execution_attempt: 1,
        status: ExecutionLogStatus::Completed,
        input: None,
        result: Some(serde_json::json!({"attempt": 1})),
        error_message: None,
        error_details: None,
        created_at: Timestamptz::now(),
        started_at: Some(Timestamptz::now()),
        completed_at: Some(Timestamptz::now()),
        duration_ms: Some(100),
        retry_count: 0,
        last_error: None,
    };

    repo.store_entry(&entry1).await.expect("Failed first store");

    // Store again with updated result (should upsert)
    let entry2 = ExecutionLogEntry {
        result: Some(serde_json::json!({"attempt": 2})),
        ..entry1
    };

    repo.store_entry(&entry2)
        .await
        .expect("Failed second store (upsert)");

    // Verify only one entry exists with updated result
    let retrieved = repo
        .get_entry(workflow_id, "idempotent_step")
        .await
        .expect("Failed to retrieve")
        .expect("Entry not found");

    assert_eq!(retrieved.result.unwrap()["attempt"], 2);

    // Cleanup
    cleanup_test_data(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires test database setup
async fn test_large_result_storage() {
    let pool = setup_test_db().await;
    let repo = PostgresExecutionLogRepository::new(pool.clone());

    let workflow_id = Uuid::new_v4();

    // Create large result (simulating API response with many records)
    let large_result = serde_json::json!({
        "users": (0..100).map(|i| {
            serde_json::json!({
                "id": i,
                "name": format!("User {}", i),
                "email": format!("user{}@example.com", i),
                "metadata": {
                    "created_at": "2024-01-01",
                    "status": "active"
                }
            })
        }).collect::<Vec<_>>()
    });

    let entry = ExecutionLogEntry {
        workflow_id,
        step_name: "fetch_users".to_string(),
        step_type: ExecutionStepType::Closure,
        execution_attempt: 1,
        status: ExecutionLogStatus::Completed,
        input: None,
        result: Some(large_result),
        error_message: None,
        error_details: None,
        created_at: Timestamptz::now(),
        started_at: Some(Timestamptz::now()),
        completed_at: Some(Timestamptz::now()),
        duration_ms: Some(500),
        retry_count: 0,
        last_error: None,
    };

    // Store large result
    repo.store_entry(&entry)
        .await
        .expect("Failed to store large result");

    // Retrieve and verify
    let retrieved = repo
        .get_entry(workflow_id, "fetch_users")
        .await
        .expect("Failed to retrieve")
        .expect("Entry not found");

    assert!(retrieved.result.is_some());
    let result = retrieved.result.unwrap();
    assert!(result["users"].is_array());
    assert_eq!(result["users"].as_array().unwrap().len(), 100);

    // Cleanup
    cleanup_test_data(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires test database setup
async fn test_workflow_timeline_query() {
    let pool = setup_test_db().await;
    let repo = PostgresExecutionLogRepository::new(pool.clone());

    let workflow_id = Uuid::new_v4();
    let base_time = Timestamptz::now();

    // Create a workflow timeline with multiple steps
    let steps = [
        ("validate_input", 0, 100),
        ("fetch_user_1", 100, 150),
        ("check_permissions_2", 250, 50),
        ("api_call_3", 300, 200),
        ("save_result", 500, 80),
    ];

    for (i, (step_name, offset_ms, duration_ms)) in steps.iter().enumerate() {
        let created = base_time + SignedDuration::from_millis(*offset_ms);
        let completed = created + SignedDuration::from_millis(*duration_ms);

        let entry = ExecutionLogEntry {
            workflow_id,
            step_name: step_name.to_string(),
            step_type: if i % 2 == 0 {
                ExecutionStepType::Task
            } else {
                ExecutionStepType::Closure
            },
            execution_attempt: 1,
            status: ExecutionLogStatus::Completed,
            input: None,
            result: Some(serde_json::json!({"step": step_name})),
            error_message: None,
            error_details: None,
            created_at: created,
            started_at: Some(created),
            completed_at: Some(completed),
            duration_ms: Some(*duration_ms),
            retry_count: 0,
            last_error: None,
        };

        repo.store_entry(&entry).await.expect("Failed to store");
    }

    // Query complete timeline
    let entries = repo
        .list_entries(workflow_id)
        .await
        .expect("Failed to list");

    assert_eq!(entries.len(), 5, "Expected 5 steps in timeline");

    // Verify entries are returned (order may vary)
    let step_names: Vec<String> = entries.iter().map(|e| e.step_name.clone()).collect();
    assert!(step_names.contains(&"validate_input".to_string()));
    assert!(step_names.contains(&"fetch_user_1".to_string()));

    // Cleanup
    cleanup_test_data(&pool, &workflow_id).await;
}
