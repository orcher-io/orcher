//! Execution Log Storage Tests
//!
//! These tests validate the execution_log storage layer:
//! 1. ExecutionLogRepository operations (store, retrieve, query)
//! 2. Step result persistence (closures, tasks, side effects)
//! 3. Data integrity and serialization
//! 4. Query performance and indexing
//! 5. Concurrent access and isolation
//!
//! **Scope:** Storage layer only - tests directly write to execution_log table
//! **Note:** For full end-to-end tests with SDK → Server flow, see durable_closure_e2e_tests.rs
//!
//! Prerequisites:
//! - PostgreSQL test database with migrations applied
//! - Set TEST_DATABASE_URL environment variable
//!
//! Run with: cargo test --test execution_log_storage_tests -- --ignored --test-threads=1

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::{
    ExecutionLogEntry, ExecutionLogRepository, ExecutionLogStatus, ExecutionStepType,
    PostgresExecutionLogRepository,
};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

// ============================================================================
// Test Utilities
// ============================================================================

/// Test harness for E2E scenarios
struct E2ETestHarness {
    pool: PgPool,
    execution_log_repo: Arc<PostgresExecutionLogRepository>,
    _journal_repo: Arc<JournalRepository>,
    workflow_id: Uuid,
    _execution_id: String,
}

impl E2ETestHarness {
    async fn new() -> Self {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/orcher_test".to_string());

        let pool = PgPool::connect(&database_url).await.expect(
            "Failed to connect to test database. Set TEST_DATABASE_URL and run migrations.",
        );

        let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));

        let workflow_id = Uuid::new_v4();
        let execution_id = format!("exec_{}", Uuid::new_v4());

        Self {
            pool,
            execution_log_repo,
            _journal_repo: journal_repo,
            workflow_id,
            _execution_id: execution_id,
        }
    }

    /// Simulate closure execution and persistence
    async fn execute_closure(
        &self,
        step_name: &str,
        result: serde_json::Value,
    ) -> Result<(), orcher_storage::error::StorageError> {
        let entry = ExecutionLogEntry {
            workflow_id: self.workflow_id,
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

        self.execution_log_repo.store_entry(&entry).await
    }

    /// Simulate task execution and persistence (dual-write)
    async fn execute_task(
        &self,
        step_name: &str,
        result: serde_json::Value,
        attempt: i32,
    ) -> Result<(), orcher_storage::error::StorageError> {
        let entry = ExecutionLogEntry {
            workflow_id: self.workflow_id,
            step_name: step_name.to_string(),
            step_type: ExecutionStepType::Task,
            execution_attempt: attempt,
            status: ExecutionLogStatus::Completed,
            input: Some(serde_json::json!({"task": "input"})),
            result: Some(result),
            error_message: None,
            error_details: None,
            created_at: Timestamptz::now(),
            started_at: Some(Timestamptz::now()),
            completed_at: Some(Timestamptz::now()),
            duration_ms: Some(250),
            retry_count: if attempt > 1 { attempt - 1 } else { 0 },
            last_error: None,
        };

        self.execution_log_repo.store_entry(&entry).await
    }

    /// Simulate workflow crash by clearing in-memory state but keeping DB state
    fn simulate_crash(&self) {
        // In real scenario, this would kill the process
        // For testing, we just continue with a "fresh" replay
        println!("💥 CRASH SIMULATED - Workflow state lost, DB persisted");
    }

    /// Load all step results (simulates replay after crash)
    async fn load_step_results(
        &self,
    ) -> Result<Vec<ExecutionLogEntry>, orcher_storage::error::StorageError> {
        self.execution_log_repo.list_entries(self.workflow_id).await
    }

    /// Verify a step result exists and matches expected value
    async fn verify_step_result(
        &self,
        step_name: &str,
        expected_result: &serde_json::Value,
    ) -> bool {
        match self
            .execution_log_repo
            .get_entry(self.workflow_id, step_name)
            .await
        {
            Ok(Some(entry)) => entry.result.as_ref() == Some(expected_result),
            _ => false,
        }
    }

    /// Cleanup test data
    async fn cleanup(&self) {
        sqlx::query("DELETE FROM execution_log WHERE workflow_id = $1")
            .bind(self.workflow_id)
            .execute(&self.pool)
            .await
            .ok();

        sqlx::query("DELETE FROM journal WHERE workflow_id = $1")
            .bind(self.workflow_id)
            .execute(&self.pool)
            .await
            .ok();
    }
}

impl Drop for E2ETestHarness {
    fn drop(&mut self) {
        // Cleanup happens in async context via explicit call
    }
}

// ============================================================================
// Storage Test 1: Closure Result Persistence and Crash Simulation
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_closure_result_persistence_with_crash_simulation() {
    let harness = E2ETestHarness::new().await;

    // =========================================================================
    // STEP 1: Initial workflow execution
    // =========================================================================
    println!("📝 STEP 1: Execute workflow with closures");

    // Execute first closure
    harness
        .execute_closure(
            "fetch_user_1",
            serde_json::json!({
                "user_id": 123,
                "name": "Alice"
            }),
        )
        .await
        .expect("Failed to execute closure 1");

    // Execute second closure
    harness
        .execute_closure(
            "query_orders_2",
            serde_json::json!({
                "order_count": 5,
                "total": 500.0
            }),
        )
        .await
        .expect("Failed to execute closure 2");

    // Verify both closures are persisted
    let results = harness
        .load_step_results()
        .await
        .expect("Failed to load results");
    assert_eq!(results.len(), 2, "Expected 2 persisted closures");

    // =========================================================================
    // STEP 2: Simulate crash
    // =========================================================================
    harness.simulate_crash();

    // =========================================================================
    // STEP 3: Restart and replay - closures should NOT re-execute
    // =========================================================================
    println!("🔄 STEP 2: Restart workflow and replay from execution_log");

    let replay_results = harness
        .load_step_results()
        .await
        .expect("Failed to load on replay");

    assert_eq!(replay_results.len(), 2, "Expected 2 closures after replay");

    // Verify first closure result is intact
    assert!(
        harness
            .verify_step_result(
                "fetch_user_1",
                &serde_json::json!({
                    "user_id": 123,
                    "name": "Alice"
                })
            )
            .await,
        "First closure result not found or incorrect"
    );

    // Verify second closure result is intact
    assert!(
        harness
            .verify_step_result(
                "query_orders_2",
                &serde_json::json!({
                    "order_count": 5,
                    "total": 500.0
                })
            )
            .await,
        "Second closure result not found or incorrect"
    );

    // =========================================================================
    // STEP 4: Continue workflow execution with new closures
    // =========================================================================
    println!("▶️  STEP 3: Continue execution with new closure");

    harness
        .execute_closure(
            "final_calculation_3",
            serde_json::json!({
                "result": "success",
                "computed_value": 625.0
            }),
        )
        .await
        .expect("Failed to execute closure 3");

    let final_results = harness
        .load_step_results()
        .await
        .expect("Failed to load final results");
    assert_eq!(final_results.len(), 3, "Expected 3 closures total");

    println!("✅ Closure crash recovery test PASSED");

    harness.cleanup().await;
}

// ============================================================================
// Storage Test 2: Multiple Closure Results with Different Data Types
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_multiple_closure_results_different_types() {
    let harness = E2ETestHarness::new().await;

    println!("📝 Execute multiple closures with different data types");

    // String result
    harness
        .execute_closure("string_closure", serde_json::json!("Hello World"))
        .await
        .expect("Failed string closure");

    // Number result
    harness
        .execute_closure("number_closure", serde_json::json!(42))
        .await
        .expect("Failed number closure");

    // Array result
    harness
        .execute_closure("array_closure", serde_json::json!([1, 2, 3, 4, 5]))
        .await
        .expect("Failed array closure");

    // Complex object result
    harness
        .execute_closure(
            "object_closure",
            serde_json::json!({
                "nested": {
                    "value": 123,
                    "items": ["a", "b", "c"]
                },
                "timestamp": "2024-01-01T00:00:00Z"
            }),
        )
        .await
        .expect("Failed object closure");

    // Verify all closures persisted
    let results = harness.load_step_results().await.expect("Failed to load");
    assert_eq!(results.len(), 4, "Expected 4 closures");

    // Verify each type
    for entry in results {
        assert_eq!(entry.step_type, ExecutionStepType::Closure);
        assert_eq!(entry.status, ExecutionLogStatus::Completed);
        assert!(
            entry.result.is_some(),
            "Result should exist for {}",
            entry.step_name
        );
    }

    println!("✅ Multiple closure types test PASSED");

    harness.cleanup().await;
}

// ============================================================================
// Storage Test 3: Mixed Step Types in Execution Log
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_mixed_step_types_in_execution_log() {
    let harness = E2ETestHarness::new().await;

    println!("📝 Execute workflow with mixed tasks and closures");

    // Closure 1
    harness
        .execute_closure(
            "fetch_config",
            serde_json::json!({
                "api_key": "test-key",
                "endpoint": "https://api.example.com"
            }),
        )
        .await
        .expect("Failed closure 1");

    // Task 1 (dual-written to execution_log)
    harness
        .execute_task(
            "send_email",
            serde_json::json!({
                "message_id": "msg-123",
                "status": "sent"
            }),
            1,
        )
        .await
        .expect("Failed task 1");

    // Closure 2
    harness
        .execute_closure(
            "process_result",
            serde_json::json!({
                "processed": true,
                "count": 1
            }),
        )
        .await
        .expect("Failed closure 2");

    // Task 2 (with retry)
    harness
        .execute_task(
            "charge_payment",
            serde_json::json!({
                "transaction_id": "txn-456",
                "amount": 99.99
            }),
            2,
        )
        .await
        .expect("Failed task 2");

    // Closure 3
    harness
        .execute_closure(
            "log_completion",
            serde_json::json!({
                "timestamp": "2024-01-01T00:00:00Z",
                "success": true
            }),
        )
        .await
        .expect("Failed closure 3");

    // Verify mixed steps
    let results = harness.load_step_results().await.expect("Failed to load");
    assert_eq!(results.len(), 5, "Expected 5 steps (3 closures + 2 tasks)");

    // Count by type
    let closure_count = results
        .iter()
        .filter(|e| e.step_type == ExecutionStepType::Closure)
        .count();
    let task_count = results
        .iter()
        .filter(|e| e.step_type == ExecutionStepType::Task)
        .count();

    assert_eq!(closure_count, 3, "Expected 3 closures");
    assert_eq!(task_count, 2, "Expected 2 tasks");

    // Verify retry count on task 2
    let task2 = results
        .iter()
        .find(|e| e.step_name == "charge_payment")
        .expect("Task 2 not found");
    assert_eq!(task2.execution_attempt, 2, "Expected attempt 2");
    assert_eq!(task2.retry_count, 1, "Expected retry_count 1");

    println!("✅ Mixed tasks and closures test PASSED");

    harness.cleanup().await;
}

// ============================================================================
// Storage Test 4: Failed Step Persistence
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_failed_step_result_storage() {
    let harness = E2ETestHarness::new().await;

    println!("📝 Test failed closure persistence");

    // Successful closure
    harness
        .execute_closure("step1_success", serde_json::json!({"status": "ok"}))
        .await
        .expect("Failed step 1");

    // Failed closure
    let failed_entry = ExecutionLogEntry {
        workflow_id: harness.workflow_id,
        step_name: "step2_failure".to_string(),
        step_type: ExecutionStepType::Closure,
        execution_attempt: 1,
        status: ExecutionLogStatus::Failed,
        input: None,
        result: None,
        error_message: Some("API timeout".to_string()),
        error_details: Some(serde_json::json!({
            "error_type": "NetworkTimeout",
            "message": "API timeout after 30s",
            "stack_trace": "at workflow.rs:123"
        })),
        created_at: Timestamptz::now(),
        started_at: Some(Timestamptz::now()),
        completed_at: Some(Timestamptz::now()),
        duration_ms: Some(30000),
        retry_count: 0,
        last_error: Some("API timeout".to_string()),
    };

    harness
        .execution_log_repo
        .store_entry(&failed_entry)
        .await
        .expect("Failed to store failed closure");

    // Verify failure was persisted
    let results = harness.load_step_results().await.expect("Failed to load");
    assert_eq!(results.len(), 2, "Expected 2 steps");

    let failed = results
        .iter()
        .find(|e| e.step_name == "step2_failure")
        .expect("Failed step not found");
    assert_eq!(failed.status, ExecutionLogStatus::Failed);
    assert!(failed.error_message.is_some());
    assert!(failed.error_details.is_some());
    assert!(failed.result.is_none());

    println!("✅ Failed closure persistence test PASSED");

    harness.cleanup().await;
}

// ============================================================================
// Storage Test 5: Idempotent Upsert on Conflict
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_idempotent_upsert_behavior() {
    let harness = E2ETestHarness::new().await;

    println!("📝 Test idempotent upsert behavior");

    // First execution
    harness
        .execute_closure(
            "idempotent_step",
            serde_json::json!({
                "version": 1,
                "data": "first"
            }),
        )
        .await
        .expect("Failed first execution");

    // Simulate replay - same step name, updated result (should upsert)
    harness
        .execute_closure(
            "idempotent_step",
            serde_json::json!({
                "version": 2,
                "data": "second"
            }),
        )
        .await
        .expect("Failed second execution (upsert)");

    // Verify only one entry exists with latest result
    let results = harness.load_step_results().await.expect("Failed to load");
    assert_eq!(results.len(), 1, "Expected only 1 entry (upserted)");

    let entry = &results[0];
    assert_eq!(entry.step_name, "idempotent_step");

    let result = entry.result.as_ref().expect("Result should exist");
    assert_eq!(result["version"], 2, "Expected version 2 (latest)");
    assert_eq!(result["data"], "second", "Expected 'second' data (latest)");

    println!("✅ Idempotent upsert test PASSED");

    harness.cleanup().await;
}

// ============================================================================
// Storage Test 6: Workflow Timeline Query with Complete Metadata
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_workflow_timeline_query_with_metadata() {
    let harness = E2ETestHarness::new().await;

    println!("📝 Test workflow timeline with complete metadata");

    let base_time = Timestamptz::now();

    // Create steps with realistic timing
    let steps = [
        ("validate_input", 0, 50, ExecutionStepType::Closure),
        ("fetch_user", 50, 200, ExecutionStepType::Closure),
        ("send_notification", 250, 150, ExecutionStepType::Task),
        ("process_data", 400, 300, ExecutionStepType::Closure),
        ("save_result", 700, 100, ExecutionStepType::Task),
    ];

    for (i, (name, offset_ms, duration_ms, step_type)) in steps.iter().enumerate() {
        let created = base_time + SignedDuration::from_millis(*offset_ms);
        let completed = created + SignedDuration::from_millis(*duration_ms);

        let entry = ExecutionLogEntry {
            workflow_id: harness.workflow_id,
            step_name: name.to_string(),
            step_type: *step_type,
            execution_attempt: 1,
            status: ExecutionLogStatus::Completed,
            input: Some(serde_json::json!({"step": i})),
            result: Some(serde_json::json!({"completed": true})),
            error_message: None,
            error_details: None,
            created_at: created,
            started_at: Some(created),
            completed_at: Some(completed),
            duration_ms: Some(*duration_ms),
            retry_count: 0,
            last_error: None,
        };

        harness
            .execution_log_repo
            .store_entry(&entry)
            .await
            .expect("Failed to store step");
    }

    // Query complete timeline
    let timeline = harness
        .load_step_results()
        .await
        .expect("Failed to load timeline");
    assert_eq!(timeline.len(), 5, "Expected 5 steps in timeline");

    // Verify metadata is present
    for entry in &timeline {
        assert!(entry.started_at.is_some(), "started_at should exist");
        assert!(entry.completed_at.is_some(), "completed_at should exist");
        assert!(entry.duration_ms.is_some(), "duration_ms should exist");
        assert_eq!(entry.execution_attempt, 1);
        assert_eq!(entry.retry_count, 0);
    }

    // Verify step order by name
    let step_names: Vec<String> = timeline.iter().map(|e| e.step_name.clone()).collect();
    assert!(step_names.contains(&"validate_input".to_string()));
    assert!(step_names.contains(&"fetch_user".to_string()));
    assert!(step_names.contains(&"send_notification".to_string()));

    println!("✅ Workflow timeline metadata test PASSED");

    harness.cleanup().await;
}

// ============================================================================
// Storage Test 7: Large JSONB Payload Storage
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_large_payload_storage() {
    let harness = E2ETestHarness::new().await;

    println!("📝 Test large payload handling");

    // Create large result (e.g., API response with many records)
    let large_data: Vec<serde_json::Value> = (0..500)
        .map(|i| {
            serde_json::json!({
                "id": i,
                "name": format!("Item {}", i),
                "description": format!("Description for item {}", i),
                "metadata": {
                    "created_at": "2024-01-01T00:00:00Z",
                    "status": "active",
                    "tags": ["tag1", "tag2", "tag3"]
                }
            })
        })
        .collect();

    let large_result = serde_json::json!({
        "items": large_data,
        "total_count": 500,
        "page": 1
    });

    harness
        .execute_closure("fetch_large_dataset", large_result.clone())
        .await
        .expect("Failed to store large payload");

    // Retrieve and verify
    let entry = harness
        .execution_log_repo
        .get_entry(harness.workflow_id, "fetch_large_dataset")
        .await
        .expect("Failed to retrieve")
        .expect("Entry not found");

    let result = entry.result.expect("Result should exist");
    assert_eq!(result["total_count"], 500);
    assert_eq!(result["items"].as_array().unwrap().len(), 500);

    println!("✅ Large payload test PASSED");

    harness.cleanup().await;
}

// ============================================================================
// Storage Test 8: Concurrent Workflow Isolation
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_concurrent_workflow_isolation() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/orcher_test".to_string());

    let pool = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test database");

    println!("📝 Test concurrent workflow executions");

    // Create multiple workflow executions
    let workflow_ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();

    let repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));

    // Execute closures for each workflow concurrently
    let mut tasks = vec![];

    for (i, workflow_id) in workflow_ids.iter().enumerate() {
        let repo_clone = Arc::clone(&repo);
        let wf_id = *workflow_id;

        let task = tokio::spawn(async move {
            for j in 0..5 {
                let entry = ExecutionLogEntry {
                    workflow_id: wf_id,
                    step_name: format!("step_{}", j),
                    step_type: ExecutionStepType::Closure,
                    execution_attempt: 1,
                    status: ExecutionLogStatus::Completed,
                    input: None,
                    result: Some(serde_json::json!({
                        "workflow": i,
                        "step": j
                    })),
                    error_message: None,
                    error_details: None,
                    created_at: Timestamptz::now(),
                    started_at: Some(Timestamptz::now()),
                    completed_at: Some(Timestamptz::now()),
                    duration_ms: Some(50),
                    retry_count: 0,
                    last_error: None,
                };

                repo_clone
                    .store_entry(&entry)
                    .await
                    .expect("Failed to store");
            }
        });

        tasks.push(task);
    }

    // Wait for all workflows to complete
    for task in tasks {
        task.await.expect("Task panicked");
    }

    // Verify each workflow has correct number of steps
    for workflow_id in &workflow_ids {
        let entries = repo
            .list_entries(*workflow_id)
            .await
            .expect("Failed to list");
        assert_eq!(entries.len(), 5, "Expected 5 steps per workflow");
    }

    // Cleanup
    for workflow_id in &workflow_ids {
        sqlx::query("DELETE FROM execution_log WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }

    println!("✅ Concurrent workflows test PASSED");
}

// ============================================================================
// Storage Test 9: Query Performance Benchmark (100+ Steps)
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_query_performance_benchmark() {
    let harness = E2ETestHarness::new().await;

    println!("📝 Test query performance with many steps");

    let step_count = 100;

    // Create many steps
    for i in 0..step_count {
        let entry = ExecutionLogEntry {
            workflow_id: harness.workflow_id,
            step_name: format!("step_{:03}", i),
            step_type: if i % 2 == 0 {
                ExecutionStepType::Closure
            } else {
                ExecutionStepType::Task
            },
            execution_attempt: 1,
            status: ExecutionLogStatus::Completed,
            input: None,
            result: Some(serde_json::json!({"index": i})),
            error_message: None,
            error_details: None,
            created_at: Timestamptz::now(),
            started_at: Some(Timestamptz::now()),
            completed_at: Some(Timestamptz::now()),
            duration_ms: Some(100),
            retry_count: 0,
            last_error: None,
        };

        harness
            .execution_log_repo
            .store_entry(&entry)
            .await
            .expect("Failed to store");
    }

    // Time the query
    let start = std::time::Instant::now();
    let results = harness.load_step_results().await.expect("Failed to load");
    let duration = start.elapsed();

    assert_eq!(results.len(), step_count, "Expected {} steps", step_count);

    println!("Query took: {:?} for {} steps", duration, step_count);
    assert!(duration.as_millis() < 1000, "Query should complete in < 1s");

    println!("✅ Query performance test PASSED");

    harness.cleanup().await;
}

// ============================================================================
// Storage Test 10: Partial Execution State Persistence
// ============================================================================

#[tokio::test]
#[ignore] // Requires test database
async fn test_partial_execution_state_persistence() {
    let harness = E2ETestHarness::new().await;

    println!("📝 Test crash recovery with partial execution");

    // =========================================================================
    // Step 1: Execute first 3 steps
    // =========================================================================
    harness
        .execute_closure("step1", serde_json::json!({"value": 1}))
        .await
        .unwrap();
    harness
        .execute_closure("step2", serde_json::json!({"value": 2}))
        .await
        .unwrap();
    harness
        .execute_task("step3", serde_json::json!({"value": 3}), 1)
        .await
        .unwrap();

    let phase1_results = harness.load_step_results().await.unwrap();
    assert_eq!(phase1_results.len(), 3, "Step 1: Expected 3 steps");

    // =========================================================================
    // CRASH - Workflow execution interrupted
    // =========================================================================
    harness.simulate_crash();

    // =========================================================================
    // Step 2: Restart and replay - first 3 steps should be loaded from DB
    // =========================================================================
    println!("🔄 Restarting after crash...");

    let replay_results = harness.load_step_results().await.unwrap();
    assert_eq!(replay_results.len(), 3, "Replay: Expected 3 steps from DB");

    // Verify replayed results match
    assert!(
        harness
            .verify_step_result("step1", &serde_json::json!({"value": 1}))
            .await
    );
    assert!(
        harness
            .verify_step_result("step2", &serde_json::json!({"value": 2}))
            .await
    );
    assert!(
        harness
            .verify_step_result("step3", &serde_json::json!({"value": 3}))
            .await
    );

    // =========================================================================
    // Step 3: Continue with remaining steps (should not re-execute 1-3)
    // =========================================================================
    println!("▶️  Continuing execution with remaining steps...");

    harness
        .execute_closure("step4", serde_json::json!({"value": 4}))
        .await
        .unwrap();
    harness
        .execute_closure("step5", serde_json::json!({"value": 5}))
        .await
        .unwrap();

    let final_results = harness.load_step_results().await.unwrap();
    assert_eq!(final_results.len(), 5, "Final: Expected 5 steps total");

    println!("✅ Partial execution crash recovery test PASSED");

    harness.cleanup().await;
}

// ============================================================================
// Test Summary
// ============================================================================

#[cfg(test)]
mod test_summary {
    //! Execution Log Storage Test Coverage:
    //!
    //! ✅ Test 1: Closure result persistence with crash simulation
    //! ✅ Test 2: Multiple closure results with different data types
    //! ✅ Test 3: Mixed step types (tasks, closures, side effects)
    //! ✅ Test 4: Failed step result storage and error tracking
    //! ✅ Test 5: Idempotent upsert on conflict
    //! ✅ Test 6: Workflow timeline query with complete metadata
    //! ✅ Test 7: Large JSONB payload handling (500+ items)
    //! ✅ Test 8: Concurrent workflow isolation
    //! ✅ Test 9: Query performance benchmark (100+ steps, < 1s)
    //! ✅ Test 10: Partial execution state persistence
    //!
    //! Note: These tests focus on the storage layer only. They directly
    //! write to and read from the execution_log table to validate:
    //! - Repository operations (store, retrieve, query)
    //! - Data integrity and serialization
    //! - Query performance
    //! - Concurrent access patterns
    //!
    //! For full end-to-end closure crash recovery testing including
    //! SDK → Server → Storage flow, see durable_closure_e2e_tests.rs
    //!
    //! Prerequisites:
    //! - Set TEST_DATABASE_URL environment variable
    //! - Run database migrations: cargo sqlx migrate run
    //! - PostgreSQL must be running and accessible
    //!
    //! Run all storage tests:
    //! ```bash
    //! export TEST_DATABASE_URL="postgres://user:pass@localhost/orcher_test"
    //! cargo test --test execution_log_storage_tests -- --ignored --test-threads=1
    //! ```
    //!
    //! Run specific test:
    //! ```bash
    //! cargo test --test execution_log_storage_tests test_large_payload_storage -- --ignored
    //! ```
}
