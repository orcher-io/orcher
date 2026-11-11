//! Integration Tests for ORCHER Coordination System
//!
//! These tests verify the complete coordination system including:
//! - WorkflowCoordinator
//! - OrchestrationSystem
//! - Background processors
//! - Multi-coordinator coordination
//! - Crash recovery
//!
//! ## Running Tests
//!
//! ```bash
//! # Setup PostgreSQL
//! docker run -d --name orcher-test-db -p 5432:5432 \
//!   -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=orcher_test postgres:15
//!
//! # Run migrations
//! export DATABASE_URL="postgres://postgres:postgres@localhost:5432/orcher_test"
//! sqlx migrate run
//!
//! # Run tests
//! cargo test --package orcher-core coordination_integration -- --ignored --test-threads=1
//! ```

use jiff::SignedDuration;
use orcher_core::engine::{
    CoordinatorConfig, OrchestrationConfig, OrchestrationSystem, OutboxTaskType,
    WorkflowCoordinator,
};
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;

// ============================================================================
// Test Utilities
// ============================================================================

/// Get database connection pool for testing
async fn get_test_pool() -> Arc<PgPool> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/orcher_test".to_string());

    let pool = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?");

    Arc::new(pool)
}

/// Clean up test data
async fn cleanup_test_data(pool: &PgPool) {
    // Delete test workflows (cascade will clean up related tables)
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE workflow_name LIKE 'test_%'")
        .execute(pool)
        .await;

    let _ = sqlx::query("DELETE FROM outbox_queue WHERE workflow_id IN (SELECT id FROM workflow_executions WHERE workflow_name LIKE 'test_%')")
        .execute(pool)
        .await;

    let _ = sqlx::query("DELETE FROM timer_queue WHERE workflow_id IN (SELECT id FROM workflow_executions WHERE workflow_name LIKE 'test_%')")
        .execute(pool)
        .await;
}

// ============================================================================
// Coordinator Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_coordinator_workflow_lifecycle() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    // Initialize coordinator
    let coordinator = WorkflowCoordinator::new(pool.clone(), CoordinatorConfig::default())
        .await
        .expect("Failed to create coordinator");

    // Start workflow
    let workflow_id = coordinator
        .start_workflow(
            "test_lifecycle".to_string(),
            "test".to_string(),
            Some(json!({"test": "data"})),
        )
        .await
        .expect("Failed to start workflow");

    println!("✓ Started workflow: {}", workflow_id);

    // Get state (should be cached)
    let state = coordinator
        .get_state(workflow_id)
        .await
        .expect("Failed to get state");
    assert_eq!(state.workflow_id, workflow_id);
    assert_eq!(state.workflow_name, "test_lifecycle");
    println!("✓ Retrieved workflow state");

    // Complete a step
    coordinator
        .complete_step(
            workflow_id,
            "step1".to_string(),
            json!({"result": "success"}),
        )
        .await
        .expect("Failed to complete step");
    println!("✓ Completed step1");

    // Verify step completion
    let state = coordinator.get_state(workflow_id).await.unwrap();
    assert!(state.completed_steps.contains("step1"));

    // Complete workflow
    coordinator
        .complete_workflow(workflow_id, json!({"final": "result"}))
        .await
        .expect("Failed to complete workflow");
    println!("✓ Completed workflow");

    // Verify terminal state
    let state = coordinator.get_state(workflow_id).await.unwrap();
    assert!(state.is_terminal());

    // Check metrics
    let metrics = coordinator.get_metrics().await;
    assert!(metrics.workflows_started > 0);
    assert!(metrics.workflows_completed > 0);
    println!(
        "✓ Metrics: {} started, {} completed",
        metrics.workflows_started, metrics.workflows_completed
    );

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_coordinator_timer_scheduling() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    let coordinator = WorkflowCoordinator::new(pool.clone(), CoordinatorConfig::default())
        .await
        .expect("Failed to create coordinator");

    // Start workflow
    let workflow_id = coordinator
        .start_workflow("test_timers".to_string(), "test".to_string(), None)
        .await
        .expect("Failed to start workflow");

    println!("✓ Started workflow: {}", workflow_id);

    // Schedule a timer
    let timer_id = coordinator
        .schedule_timer(
            workflow_id,
            "test_timer".to_string(),
            SignedDuration::from_secs(5),
        )
        .await
        .expect("Failed to schedule timer");

    println!("✓ Scheduled timer: {}", timer_id);

    // Verify timer in state
    let state = coordinator.get_state(workflow_id).await.unwrap();
    assert_eq!(state.pending_timers.len(), 1);
    assert!(state.pending_timers.contains_key("test_timer"));

    // Handle timer fire
    coordinator
        .handle_timer_fire(workflow_id, "test_timer".to_string())
        .await
        .expect("Failed to handle timer fire");

    println!("✓ Handled timer fire");

    // Verify timer removed
    let state = coordinator.get_state(workflow_id).await.unwrap();
    assert_eq!(state.pending_timers.len(), 0);

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_coordinator_task_dispatch() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    let coordinator = WorkflowCoordinator::new(pool.clone(), CoordinatorConfig::default())
        .await
        .expect("Failed to create coordinator");

    let workflow_id = coordinator
        .start_workflow("test_tasks".to_string(), "test".to_string(), None)
        .await
        .expect("Failed to start workflow");

    // Dispatch a task
    let task_id = coordinator
        .dispatch_task(
            workflow_id,
            OutboxTaskType::DispatchTask,
            json!({"task": "process_payment", "amount": 99.99}),
        )
        .await
        .expect("Failed to dispatch task");

    println!("✓ Dispatched task: {}", task_id);

    // Verify task in outbox queue
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_queue WHERE workflow_id = $1 AND status = 'pending'",
    )
    .bind(workflow_id)
    .fetch_one(&*pool)
    .await
    .unwrap();

    assert_eq!(count, 1);
    println!("✓ Task found in outbox queue");

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_coordinator_failure_handling() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    let coordinator = WorkflowCoordinator::new(pool.clone(), CoordinatorConfig::default())
        .await
        .expect("Failed to create coordinator");

    let workflow_id = coordinator
        .start_workflow("test_failure".to_string(), "test".to_string(), None)
        .await
        .unwrap();

    // Complete a step
    coordinator
        .complete_step(workflow_id, "step1".to_string(), json!({}))
        .await
        .unwrap();

    // Fail the workflow
    coordinator
        .fail_workflow(workflow_id, "Test failure".to_string())
        .await
        .expect("Failed to fail workflow");

    println!("✓ Workflow failed successfully");

    // Verify failure state
    let state = coordinator.get_state(workflow_id).await.unwrap();
    assert!(state.is_terminal());
    assert!(state.last_error.is_some());
    assert_eq!(state.last_error.unwrap(), "Test failure");

    // Check metrics
    let metrics = coordinator.get_metrics().await;
    assert!(metrics.workflows_failed > 0);

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Multi-Coordinator Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_multi_coordinator_cache_invalidation() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    // Create two coordinators
    let mut config1 = CoordinatorConfig::default();
    config1.instance_id = "coordinator-1".to_string();
    config1.invalidation.coordinator_id = "coordinator-1".to_string();

    let mut config2 = CoordinatorConfig::default();
    config2.instance_id = "coordinator-2".to_string();
    config2.invalidation.coordinator_id = "coordinator-2".to_string();

    let coordinator1 = WorkflowCoordinator::new(pool.clone(), config1)
        .await
        .expect("Failed to create coordinator 1");

    let coordinator2 = WorkflowCoordinator::new(pool.clone(), config2)
        .await
        .expect("Failed to create coordinator 2");

    println!("✓ Created two coordinators");

    // Coordinator 1 starts a workflow
    let workflow_id = coordinator1
        .start_workflow("test_multi_coord".to_string(), "test".to_string(), None)
        .await
        .unwrap();

    println!("✓ Coordinator 1 started workflow: {}", workflow_id);

    // Coordinator 2 reads the state (cache miss, reconstructs)
    let state1 = coordinator2.get_state(workflow_id).await.unwrap();
    assert_eq!(state1.version, 1);
    println!(
        "✓ Coordinator 2 read initial state (version: {})",
        state1.version
    );

    // Coordinator 1 updates the workflow
    coordinator1
        .complete_step(workflow_id, "step1".to_string(), json!({}))
        .await
        .unwrap();

    println!("✓ Coordinator 1 completed step1");

    // Wait for cache invalidation to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Coordinator 2 reads again (should get updated version)
    let state2 = coordinator2.get_state(workflow_id).await.unwrap();
    assert!(state2.version > state1.version);
    assert!(state2.completed_steps.contains("step1"));
    println!(
        "✓ Coordinator 2 read updated state (version: {})",
        state2.version
    );

    cleanup_test_data(&pool).await;
}

// ============================================================================
// OrchestrationSystem Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_orchestration_system_initialization() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    let system = OrchestrationSystem::new(pool.clone(), OrchestrationConfig::default())
        .await
        .expect("Failed to create orchestration system");

    println!("✓ OrchestrationSystem initialized");

    // Check health
    let health = system.health().await;
    assert!(health.healthy);
    println!("✓ System healthy: {}", health.healthy);

    // Get summary
    let summary = system.summary().await;
    println!("✓ System summary:");
    println!("  Instance ID: {}", summary.instance_id);
    println!("  Healthy: {}", summary.healthy);

    // Use the coordinator
    let workflow_id = system
        .coordinator()
        .start_workflow("test_system".to_string(), "test".to_string(), None)
        .await
        .unwrap();

    println!("✓ Started workflow via system: {}", workflow_id);

    // Shutdown
    system.shutdown().await.expect("Failed to shutdown");
    println!("✓ System shutdown successfully");

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_orchestration_system_metrics() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    let system = OrchestrationSystem::new(pool.clone(), OrchestrationConfig::default())
        .await
        .unwrap();

    // Execute some workflows
    for i in 1..=5 {
        let workflow_id = system
            .coordinator()
            .start_workflow(format!("test_metrics_{}", i), "test".to_string(), None)
            .await
            .unwrap();

        system
            .coordinator()
            .complete_step(workflow_id, "step1".to_string(), json!({}))
            .await
            .unwrap();

        system
            .coordinator()
            .complete_workflow(workflow_id, json!({"result": i}))
            .await
            .unwrap();
    }

    println!("✓ Executed 5 workflows");

    // Check metrics
    let coord_metrics = system.coordinator_metrics().await;
    assert!(coord_metrics.workflows_started >= 5);
    assert!(coord_metrics.workflows_completed >= 5);
    assert!(coord_metrics.steps_completed >= 5);

    println!("✓ Coordinator metrics:");
    println!("  Started: {}", coord_metrics.workflows_started);
    println!("  Completed: {}", coord_metrics.workflows_completed);
    println!("  Steps: {}", coord_metrics.steps_completed);
    println!(
        "  Cache hit rate: {:.1}%",
        if coord_metrics.cache_hits + coord_metrics.cache_misses > 0 {
            100.0 * coord_metrics.cache_hits as f64
                / (coord_metrics.cache_hits + coord_metrics.cache_misses) as f64
        } else {
            0.0
        }
    );

    // Get summary
    let summary = system.summary().await;
    assert!(summary.workflows_started >= 5);
    assert!(summary.workflows_completed >= 5);
    println!("✓ System summary validated");

    system.shutdown().await.unwrap();
    cleanup_test_data(&pool).await;
}

// ============================================================================
// Crash Recovery Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_crash_recovery() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    // Step 1: Create workflows and "crash"
    let workflow_ids = {
        let coordinator = WorkflowCoordinator::new(pool.clone(), CoordinatorConfig::default())
            .await
            .unwrap();

        let mut ids = Vec::new();
        for i in 1..=3 {
            let id = coordinator
                .start_workflow(format!("test_recovery_{}", i), "test".to_string(), None)
                .await
                .unwrap();

            coordinator
                .complete_step(id, "step1".to_string(), json!({}))
                .await
                .unwrap();

            ids.push(id);
        }

        println!("✓ Started 3 workflows");
        ids
        // Coordinator drops here (simulating crash)
    };

    println!("✓ Simulated crash (coordinator dropped)");

    // Step 2: New coordinator recovers
    let mut recovery_config = CoordinatorConfig::default();
    recovery_config.enable_recovery = true;
    recovery_config.instance_id = "coordinator-recovery".to_string();

    let new_coordinator = WorkflowCoordinator::new(pool.clone(), recovery_config)
        .await
        .unwrap();

    println!("✓ New coordinator started with recovery");

    // Verify recovered workflows
    for workflow_id in &workflow_ids {
        let state = new_coordinator.get_state(*workflow_id).await.unwrap();
        assert!(state.completed_steps.contains("step1"));
        println!("✓ Recovered workflow {} with step1 completed", workflow_id);
    }

    // Check recovery metrics
    let metrics = new_coordinator.get_metrics().await;
    assert!(metrics.state_reconstructions >= 3);
    println!(
        "✓ Recovery metrics: {} reconstructions",
        metrics.state_reconstructions
    );

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Performance Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_performance_workflow_throughput() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    let coordinator = WorkflowCoordinator::new(pool.clone(), CoordinatorConfig::default())
        .await
        .unwrap();

    let start = std::time::Instant::now();
    let count = 100;

    // Create 100 workflows
    for i in 0..count {
        let _ = coordinator
            .start_workflow(format!("test_perf_{}", i), "test".to_string(), None)
            .await
            .unwrap();
    }

    let duration = start.elapsed();
    let throughput = count as f64 / duration.as_secs_f64();

    println!("✓ Created {} workflows in {:?}", count, duration);
    println!("✓ Throughput: {:.1} workflows/sec", throughput);

    assert!(
        throughput > 50.0,
        "Expected >50 workflows/sec, got {:.1}",
        throughput
    );

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_performance_cache_hit_rate() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    let coordinator = WorkflowCoordinator::new(pool.clone(), CoordinatorConfig::default())
        .await
        .unwrap();

    // Create a workflow
    let workflow_id = coordinator
        .start_workflow("test_cache_perf".to_string(), "test".to_string(), None)
        .await
        .unwrap();

    // Read it 100 times (should all be cache hits after first)
    for _ in 0..100 {
        let _ = coordinator.get_state(workflow_id).await.unwrap();
    }

    let stats = coordinator.get_cache_stats().await;
    let hit_rate = stats.hits as f64 / (stats.hits + stats.misses) as f64;

    println!(
        "✓ Cache stats: {} hits, {} misses",
        stats.hits, stats.misses
    );
    println!("✓ Hit rate: {:.1}%", hit_rate * 100.0);

    assert!(
        hit_rate > 0.95,
        "Expected >95% hit rate, got {:.1}%",
        hit_rate * 100.0
    );

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Integration Test Summary
// ============================================================================

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn test_complete_workflow_scenario() {
    let pool = get_test_pool().await;
    cleanup_test_data(&pool).await;

    println!("\n=== Complete Workflow Scenario ===\n");

    let system = OrchestrationSystem::new(pool.clone(), OrchestrationConfig::default())
        .await
        .unwrap();

    // Scenario: E-commerce order processing
    let workflow_id = system
        .coordinator()
        .start_workflow(
            "order_processing".to_string(),
            "production".to_string(),
            Some(json!({
                "order_id": "ORD-123",
                "amount": 99.99,
                "customer": "test@example.com"
            })),
        )
        .await
        .unwrap();

    println!("✓ Order workflow started: {}", workflow_id);

    // Step 1: Validate order
    system
        .coordinator()
        .complete_step(
            workflow_id,
            "validate_order".to_string(),
            json!({"valid": true}),
        )
        .await
        .unwrap();
    println!("✓ Order validated");

    // Step 2: Process payment (dispatch task)
    let task_id = system
        .coordinator()
        .dispatch_task(
            workflow_id,
            OutboxTaskType::DispatchTask,
            json!({"task": "charge_payment", "amount": 99.99}),
        )
        .await
        .unwrap();
    println!("✓ Payment task dispatched: {}", task_id);

    // Step 3: Reserve inventory
    system
        .coordinator()
        .complete_step(
            workflow_id,
            "reserve_inventory".to_string(),
            json!({"reserved": true}),
        )
        .await
        .unwrap();
    println!("✓ Inventory reserved");

    // Step 4: Schedule fulfillment timer
    let timer_id = system
        .coordinator()
        .schedule_timer(
            workflow_id,
            "fulfillment_delay".to_string(),
            SignedDuration::from_hours(2),
        )
        .await
        .unwrap();
    println!("✓ Fulfillment timer scheduled: {}", timer_id);

    // Step 5: Complete workflow
    system
        .coordinator()
        .complete_workflow(
            workflow_id,
            json!({"status": "completed", "tracking": "TRACK-123"}),
        )
        .await
        .unwrap();
    println!("✓ Order workflow completed");

    // Verify final state
    let state = system.coordinator().get_state(workflow_id).await.unwrap();
    assert!(state.is_terminal());
    assert_eq!(state.completed_steps.len(), 2);
    println!("✓ Final state verified");

    // Check system metrics
    let summary = system.summary().await;
    println!("\n=== System Summary ===");
    println!(
        "Workflows: {} started, {} completed",
        summary.workflows_started, summary.workflows_completed
    );
    println!("Tasks dispatched: {}", summary.tasks_dispatched);
    println!("Timers scheduled: {}", summary.timers_scheduled);
    println!("Cache hit rate: {:.1}%", summary.cache_hit_rate * 100.0);

    system.shutdown().await.unwrap();
    println!("\n✓ Complete scenario test passed");

    cleanup_test_data(&pool).await;
}
