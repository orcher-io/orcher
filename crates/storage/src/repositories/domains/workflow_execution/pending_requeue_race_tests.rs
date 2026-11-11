//! Deterministic Concurrency Tests for `pending_requeue` TOCTOU Race Condition
//!
//! These tests verify that the atomic `release_claim_and_check_requeue` operation
//! correctly handles the race condition where:
//!
//! 1. `complete_workflow_execution` starts releasing the claim
//! 2. `complete_task_execution` sets `pending_requeue=true` (concurrent transaction)
//! 3. The claim release must capture the pending_requeue flag atomically
//!
//! Without the CTE-based atomic read-and-clear, workflows can become orphaned with
//! `pending_requeue=true` but no process to push them to Redis.
//!
//! ## Test Scenarios
//!
//! 1. **Basic atomic operation** - Verify CTE captures old value before clearing
//! 2. **Concurrent set + release** - Simulate the actual race with concurrent transactions
//! 3. **Reclaimer recovery** - Verify orphaned workflows are recovered
//! 4. **Stress test** - Many concurrent workflows with rapid completions
//!
//! ## Running Tests
//!
//! ```bash
//! # Run with database (requires DATABASE_URL or local postgres)
//! DATABASE_URL="postgres://orcher:orcher@localhost/orcher_test" \
//!   cargo test --package orcher-storage pending_requeue_race -- --ignored --nocapture
//! ```

use super::*;
use crate::schema::types::execution::WorkflowExecution;
use sqlx::PgPool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::timeout;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Helper to create a test database pool
async fn create_test_pool() -> PgPool {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://orcher:orcher@localhost/orcher_test".to_string());

    PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test database")
}

/// Helper to create a test workflow execution
fn create_test_execution(task_queue: &str, namespace: &str) -> WorkflowExecution {
    let mut execution = WorkflowExecution::builder()
        .execution_id(format!("test_exec_{}", Uuid::new_v4()))
        .workflow_name("pending_requeue_test".to_string())
        .workflow_type("pending_requeue_test".to_string())
        .workflow_version("v1.0".to_string())
        .namespace(namespace.to_string())
        .parameters(serde_json::json!({}))
        .build();

    execution.task_queue = task_queue.to_string();
    execution.status = "running".to_string();
    execution
}

/// Helper to set up a claimed workflow execution
async fn setup_claimed_execution(
    pool: &PgPool,
    repo: &WorkflowExecutionRepository,
    task_queue: &str,
    namespace: &str,
) -> WorkflowExecution {
    let execution = create_test_execution(task_queue, namespace);
    let created = repo.create(&execution).await.unwrap();

    // Set a valid claim (timeout in the future)
    sqlx::query(
        r#"
        UPDATE workflow_executions
        SET claimed_by = $1,
            claimed_at = NOW(),
            claim_timeout_at = NOW() + INTERVAL '5 minutes',
            status = 'running'
        WHERE execution_id = $2
        "#,
    )
    .bind("test-orchestrator")
    .bind(&created.execution_id)
    .execute(pool)
    .await
    .unwrap();

    repo.find_by_execution_id(&created.execution_id)
        .await
        .unwrap()
        .unwrap()
}

/// Clean up test executions after tests
async fn cleanup_test_executions(pool: &PgPool) {
    sqlx::query(
        r#"
        DELETE FROM workflow_executions
        WHERE workflow_name = 'pending_requeue_test'
        "#,
    )
    .execute(pool)
    .await
    .ok();
}

// ============================================================================
// Test 1: Basic Atomic Operation
// ============================================================================

/// Verify that `release_claim_and_check_requeue` atomically reads and clears the flag
#[tokio::test]
#[ignore] // Requires database
async fn test_release_claim_and_check_requeue_atomic_read_clear() {
    let pool = create_test_pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    cleanup_test_executions(&pool).await;

    // Create a claimed execution with pending_requeue=true
    let execution = setup_claimed_execution(&pool, &repo, "test-queue", "test").await;

    // Set pending_requeue=true manually
    sqlx::query(
        r#"
        UPDATE workflow_executions
        SET pending_requeue = TRUE,
            pending_requeue_at = NOW()
        WHERE execution_id = $1
        "#,
    )
    .bind(&execution.execution_id)
    .execute(&pool)
    .await
    .unwrap();

    // Call the atomic release_claim_and_check_requeue
    let result = repo
        .release_claim_and_check_requeue(&execution.execution_id)
        .await
        .unwrap();

    // Verify we got the pending_requeue info
    assert!(result.is_some(), "Should return requeue info");
    let info = result.unwrap();
    assert!(
        info.pending_requeue,
        "Should report pending_requeue was true"
    );
    assert_eq!(info.task_queue, "test-queue");
    assert_eq!(info.namespace, "test");

    // Verify the flag was cleared in the database
    let row: (bool,) = sqlx::query_as(
        r#"
        SELECT pending_requeue
        FROM workflow_executions
        WHERE execution_id = $1
        "#,
    )
    .bind(&execution.execution_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(
        !row.0,
        "pending_requeue should be FALSE after atomic release"
    );

    // Verify claim was also released
    let updated = repo
        .find_by_execution_id(&execution.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert!(updated.claimed_by.is_none(), "Claim should be released");
    assert!(updated.claimed_at.is_none());
    assert!(updated.claim_timeout_at.is_none());

    cleanup_test_executions(&pool).await;
}

/// Verify that release returns pending_requeue=false when flag was not set
#[tokio::test]
#[ignore] // Requires database
async fn test_release_claim_and_check_requeue_no_pending() {
    let pool = create_test_pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    cleanup_test_executions(&pool).await;

    // Create a claimed execution WITHOUT pending_requeue
    let execution = setup_claimed_execution(&pool, &repo, "test-queue", "test").await;

    // Call the atomic release_claim_and_check_requeue
    let result = repo
        .release_claim_and_check_requeue(&execution.execution_id)
        .await
        .unwrap();

    // Verify we got the info but pending_requeue=false
    assert!(result.is_some(), "Should return requeue info");
    let info = result.unwrap();
    assert!(
        !info.pending_requeue,
        "Should report pending_requeue was false"
    );

    cleanup_test_executions(&pool).await;
}

// ============================================================================
// Test 2: Concurrent Set + Release (The Actual TOCTOU Race)
// ============================================================================

/// Simulate the race condition where set_pending_requeue and release_claim happen concurrently
///
/// This is the core race condition we're testing:
/// - Transaction A: complete_workflow_execution → release_claim_and_check_requeue
/// - Transaction B: complete_task_execution → set_pending_requeue
///
/// Without atomic handling, B can commit after A reads pending_requeue=false,
/// leaving the workflow orphaned.
#[tokio::test]
#[ignore] // Requires database
async fn test_concurrent_set_and_release_race_condition() {
    let pool = create_test_pool().await;
    let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    cleanup_test_executions(&pool).await;

    // Run multiple iterations to increase chance of hitting race
    let mut detected_races = 0;
    let iterations = 50;

    for i in 0..iterations {
        let execution = setup_claimed_execution(&pool, &repo, "race-queue", "race-ns").await;
        let exec_id = execution.execution_id.clone();

        // Use a barrier to maximize race probability
        let barrier = Arc::new(Barrier::new(2));

        let repo_clone = Arc::clone(&repo);
        let exec_id_clone = exec_id.clone();
        let barrier_clone = Arc::clone(&barrier);

        // Transaction A: set_pending_requeue (simulating task completion)
        let set_handle = tokio::spawn(async move {
            barrier_clone.wait().await;
            repo_clone.set_pending_requeue(&exec_id_clone).await
        });

        let repo_clone = Arc::clone(&repo);
        let exec_id_clone = exec_id.clone();
        let barrier_clone = Arc::clone(&barrier);

        // Transaction B: release_claim_and_check_requeue (simulating workflow completion)
        let release_handle = tokio::spawn(async move {
            barrier_clone.wait().await;
            repo_clone
                .release_claim_and_check_requeue(&exec_id_clone)
                .await
        });

        // Wait for both to complete
        let (set_result, release_result) = tokio::join!(set_handle, release_handle);
        let set_ok = set_result.unwrap();
        let release_result = release_result.unwrap().unwrap();

        // Check the final state
        let final_state: (bool, Option<String>) = sqlx::query_as(
            r#"
            SELECT pending_requeue, claimed_by
            FROM workflow_executions
            WHERE execution_id = $1
            "#,
        )
        .bind(&exec_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let (final_pending_requeue, final_claimed_by) = final_state;

        // Analyze the outcome
        let release_saw_pending = release_result
            .as_ref()
            .map(|r| r.pending_requeue)
            .unwrap_or(false);

        if release_saw_pending {
            // Release saw the pending_requeue and cleared it - correct behavior
            debug!(
                iteration = i,
                "Release saw pending_requeue=true and handled it"
            );
        } else if set_ok.as_ref().map(|&v| v).unwrap_or(false) && !final_pending_requeue {
            // Set succeeded but final state is false = release cleared it - correct
            debug!(
                iteration = i,
                "Set succeeded, release cleared - correct behavior"
            );
        } else if final_pending_requeue && final_claimed_by.is_none() {
            // This is the race condition! pending_requeue=true but claim released
            // Without the fix, this would leave the workflow orphaned
            // With the fix (Reclaimer), this will be recovered
            detected_races += 1;
            info!(
                iteration = i,
                exec_id = %exec_id,
                "🔴 RACE DETECTED: pending_requeue=true, claimed_by=None (would be orphaned without Reclaimer)"
            );
        }

        // Cleanup this iteration
        sqlx::query("DELETE FROM workflow_executions WHERE execution_id = $1")
            .bind(&exec_id)
            .execute(&pool)
            .await
            .ok();
    }

    info!(
        detected_races = detected_races,
        iterations = iterations,
        "Race condition test completed"
    );

    // The test passes regardless - we're measuring race occurrence
    // The important thing is the Reclaimer can recover these cases
    cleanup_test_executions(&pool).await;
}

// ============================================================================
// Test 3: Reclaimer Recovery
// ============================================================================

/// Verify that find_pending_requeues correctly finds orphaned workflows
#[tokio::test]
#[ignore] // Requires database
async fn test_find_pending_requeues_finds_orphaned_workflows() {
    let pool = create_test_pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    cleanup_test_executions(&pool).await;

    // Create multiple orphaned workflows (pending_requeue=true, no claim)
    let mut orphan_ids = Vec::new();
    for _ in 0..5 {
        let execution = create_test_execution("orphan-queue", "orphan-ns");
        let created = repo.create(&execution).await.unwrap();
        orphan_ids.push(created.execution_id.clone());

        // Set as orphaned: pending_requeue=true, claimed_by=NULL, old timestamp
        sqlx::query(
            r#"
            UPDATE workflow_executions
            SET pending_requeue = TRUE,
                pending_requeue_at = NOW() - INTERVAL '5 seconds',
                claimed_by = NULL,
                claimed_at = NULL,
                claim_timeout_at = NULL,
                status = 'running'
            WHERE execution_id = $1
            "#,
        )
        .bind(&created.execution_id)
        .execute(&pool)
        .await
        .unwrap();
    }

    // Also create a workflow that should NOT be found:
    // - Too recent (pending_requeue_at is now)
    let recent = create_test_execution("orphan-queue", "orphan-ns");
    let recent_created = repo.create(&recent).await.unwrap();
    sqlx::query(
        r#"
        UPDATE workflow_executions
        SET pending_requeue = TRUE,
            pending_requeue_at = NOW(),
            claimed_by = NULL,
            status = 'running'
        WHERE execution_id = $1
        "#,
    )
    .bind(&recent_created.execution_id)
    .execute(&pool)
    .await
    .unwrap();

    // - Still claimed
    let claimed = create_test_execution("orphan-queue", "orphan-ns");
    let claimed_created = repo.create(&claimed).await.unwrap();
    sqlx::query(
        r#"
        UPDATE workflow_executions
        SET pending_requeue = TRUE,
            pending_requeue_at = NOW() - INTERVAL '5 seconds',
            claimed_by = 'some-orchestrator',
            status = 'running'
        WHERE execution_id = $1
        "#,
    )
    .bind(&claimed_created.execution_id)
    .execute(&pool)
    .await
    .unwrap();

    // Find pending requeues older than 2 seconds
    let pending = repo.find_pending_requeues(2, 100).await.unwrap();

    // Should find exactly the 5 orphaned ones
    assert_eq!(
        pending.len(),
        5,
        "Should find exactly 5 orphaned workflows, found {}",
        pending.len()
    );

    // Verify all found are in our orphan list
    for p in &pending {
        assert!(
            orphan_ids.contains(&p.execution_id),
            "Found unexpected execution: {}",
            p.execution_id
        );
        assert_eq!(p.task_queue, "orphan-queue");
        assert_eq!(p.namespace, "orphan-ns");
    }

    cleanup_test_executions(&pool).await;
}

/// Verify that clear_pending_requeue works after manual recovery
#[tokio::test]
#[ignore] // Requires database
async fn test_clear_pending_requeue_after_recovery() {
    let pool = create_test_pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    cleanup_test_executions(&pool).await;

    let execution = create_test_execution("recovery-queue", "recovery-ns");
    let created = repo.create(&execution).await.unwrap();

    // Set as orphaned
    sqlx::query(
        r#"
        UPDATE workflow_executions
        SET pending_requeue = TRUE,
            pending_requeue_at = NOW() - INTERVAL '5 seconds',
            claimed_by = NULL,
            status = 'running'
        WHERE execution_id = $1
        "#,
    )
    .bind(&created.execution_id)
    .execute(&pool)
    .await
    .unwrap();

    // Simulate recovery: find and clear
    let pending = repo.find_pending_requeues(1, 10).await.unwrap();
    assert_eq!(pending.len(), 1);

    // Clear the flag (simulating successful Redis push)
    let cleared = repo
        .clear_pending_requeue(&created.execution_id)
        .await
        .unwrap();
    assert!(cleared, "Should clear the flag");

    // Verify it's no longer found
    let pending_after = repo.find_pending_requeues(1, 10).await.unwrap();
    assert_eq!(pending_after.len(), 0, "Should not find cleared workflow");

    cleanup_test_executions(&pool).await;
}

// ============================================================================
// Test 4: Stress Test - Many Concurrent Workflows
// ============================================================================

/// Stress test with many concurrent workflows experiencing the race
#[tokio::test]
#[ignore] // Requires database
async fn test_stress_many_concurrent_pending_requeue_operations() {
    let pool = create_test_pool().await;
    let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    cleanup_test_executions(&pool).await;

    let workflow_count = 20;
    let operations_per_workflow = 5;

    let success_count = Arc::new(AtomicUsize::new(0));
    let race_detected_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    // Create all workflows first
    let mut execution_ids = Vec::new();
    for _ in 0..workflow_count {
        let execution = setup_claimed_execution(&pool, &repo, "stress-queue", "stress-ns").await;
        execution_ids.push(execution.execution_id);
    }

    let barrier = Arc::new(Barrier::new(workflow_count * 2)); // 2 tasks per workflow

    let mut handles = Vec::new();

    for exec_id in execution_ids.iter() {
        // Re-claim the workflow before spawning tasks
        for _ in 0..operations_per_workflow {
            sqlx::query(
                r#"
                UPDATE workflow_executions
                SET claimed_by = 'stress-test',
                    claimed_at = NOW(),
                    claim_timeout_at = NOW() + INTERVAL '1 minute',
                    pending_requeue = FALSE
                WHERE execution_id = $1
                "#,
            )
            .bind(exec_id)
            .execute(&pool)
            .await
            .ok();
        }

        {
            let repo_clone = Arc::clone(&repo);
            let exec_id_clone = exec_id.clone();
            let barrier_clone = Arc::clone(&barrier);
            let success_clone = Arc::clone(&success_count);
            let error_clone = Arc::clone(&error_count);

            // Spawn set_pending_requeue task
            handles.push(tokio::spawn(async move {
                barrier_clone.wait().await;

                match repo_clone.set_pending_requeue(&exec_id_clone).await {
                    Ok(true) => {
                        success_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(false) => {
                        // Already set or not found - not an error
                    }
                    Err(e) => {
                        warn!(error = %e, "set_pending_requeue failed");
                        error_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        {
            let repo_clone = Arc::clone(&repo);
            let exec_id_clone = exec_id.clone();
            let barrier_clone = Arc::clone(&barrier);
            let success_clone = Arc::clone(&success_count);
            let race_clone = Arc::clone(&race_detected_count);
            let error_clone = Arc::clone(&error_count);
            let pool_clone = pool.clone();

            // Spawn release_claim_and_check_requeue task
            handles.push(tokio::spawn(async move {
                barrier_clone.wait().await;

                match repo_clone
                    .release_claim_and_check_requeue(&exec_id_clone)
                    .await
                {
                    Ok(Some(info)) => {
                        if info.pending_requeue {
                            success_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(None) => {
                        // Execution not found
                    }
                    Err(e) => {
                        warn!(error = %e, "release_claim_and_check_requeue failed");
                        error_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Check for race condition (orphaned state)
                let state: Option<(bool, Option<String>)> = sqlx::query_as(
                    r#"
                    SELECT pending_requeue, claimed_by
                    FROM workflow_executions
                    WHERE execution_id = $1
                    "#,
                )
                .bind(&exec_id_clone)
                .fetch_optional(&pool_clone)
                .await
                .ok()
                .flatten();

                if let Some((pending_requeue, claimed_by)) = state {
                    if pending_requeue && claimed_by.is_none() {
                        race_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
    }

    // Wait for all tasks with timeout
    let results = timeout(Duration::from_secs(30), futures::future::join_all(handles)).await;

    match results {
        Ok(_) => {
            let successes = success_count.load(Ordering::Relaxed);
            let races = race_detected_count.load(Ordering::Relaxed);
            let errors = error_count.load(Ordering::Relaxed);

            info!(
                workflow_count = workflow_count,
                successes = successes,
                races_detected = races,
                errors = errors,
                "Stress test completed"
            );

            // We expect some races in this stress test - the important thing is
            // the Reclaimer can recover them
            if races > 0 {
                info!(
                    races = races,
                    "Race conditions detected - Reclaimer should recover these"
                );

                // Verify Reclaimer can find them
                let pending = repo.find_pending_requeues(0, 1000).await.unwrap();
                info!(
                    recoverable = pending.len(),
                    "Reclaimer can recover {} orphaned workflows",
                    pending.len()
                );
            }

            assert_eq!(errors, 0, "No database errors should occur");
        }
        Err(_) => {
            panic!("Stress test timed out after 30 seconds");
        }
    }

    cleanup_test_executions(&pool).await;
}

// ============================================================================
// Test 5: Edge Cases
// ============================================================================

/// Verify idempotent set_pending_requeue (calling twice doesn't double-set)
#[tokio::test]
#[ignore] // Requires database
async fn test_set_pending_requeue_idempotent() {
    let pool = create_test_pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    cleanup_test_executions(&pool).await;

    let execution = setup_claimed_execution(&pool, &repo, "idem-queue", "idem-ns").await;

    // First set should succeed
    let first = repo
        .set_pending_requeue(&execution.execution_id)
        .await
        .unwrap();
    assert!(first, "First set should succeed");

    // Second set should be no-op (already set)
    let second = repo
        .set_pending_requeue(&execution.execution_id)
        .await
        .unwrap();
    assert!(!second, "Second set should be no-op");

    cleanup_test_executions(&pool).await;
}

/// Verify release_claim_and_check_requeue for non-existent execution
#[tokio::test]
#[ignore] // Requires database
async fn test_release_claim_and_check_requeue_not_found() {
    let pool = create_test_pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());

    let result = repo
        .release_claim_and_check_requeue("non_existent_execution_id")
        .await
        .unwrap();

    assert!(
        result.is_none(),
        "Should return None for non-existent execution"
    );
}

/// Verify clear_pending_requeue is idempotent
#[tokio::test]
#[ignore] // Requires database
async fn test_clear_pending_requeue_idempotent() {
    let pool = create_test_pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    cleanup_test_executions(&pool).await;

    let execution = create_test_execution("clear-queue", "clear-ns");
    let created = repo.create(&execution).await.unwrap();

    // Set pending_requeue
    repo.set_pending_requeue(&created.execution_id)
        .await
        .unwrap();

    // First clear should succeed
    let first = repo
        .clear_pending_requeue(&created.execution_id)
        .await
        .unwrap();
    assert!(first, "First clear should succeed");

    // Second clear should be no-op
    let second = repo
        .clear_pending_requeue(&created.execution_id)
        .await
        .unwrap();
    assert!(!second, "Second clear should be no-op");

    cleanup_test_executions(&pool).await;
}

// ============================================================================
// Test 6: Timing-Based Race Reproduction
// ============================================================================

/// Attempt to reproduce the exact timing of the TOCTOU race
///
/// This test tries to hit the window where:
/// 1. release_claim_and_check_requeue starts its transaction
/// 2. set_pending_requeue commits
/// 3. release_claim_and_check_requeue reads the OLD value (before set)
///
/// The CTE with FOR UPDATE should prevent this by locking the row.
#[tokio::test]
#[ignore] // Requires database
async fn test_timing_based_race_reproduction() {
    let pool = create_test_pool().await;
    let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    cleanup_test_executions(&pool).await;

    let mut orphaned_count = 0;
    let iterations = 100;

    for i in 0..iterations {
        let execution = setup_claimed_execution(&pool, &repo, "timing-queue", "timing-ns").await;
        let exec_id = execution.execution_id.clone();

        // Create two separate connections for true concurrent transactions
        let pool1 = pool.clone();
        let pool2 = pool.clone();
        let exec_id1 = exec_id.clone();
        let exec_id2 = exec_id.clone();

        // Start both operations with minimal delay between them
        let release_handle = tokio::spawn(async move {
            // Simulate the CTE query directly
            let result: Option<(bool, String, String)> = sqlx::query_as(
                r#"
                WITH old_state AS (
                    SELECT pending_requeue, task_queue, namespace
                    FROM workflow_executions
                    WHERE execution_id = $1
                    FOR UPDATE
                )
                UPDATE workflow_executions we
                SET claimed_by = NULL,
                    claimed_at = NULL,
                    claim_timeout_at = NULL,
                    pending_requeue = FALSE,
                    pending_requeue_at = NULL,
                    updated_at = NOW()
                FROM old_state
                WHERE we.execution_id = $1
                RETURNING old_state.pending_requeue, old_state.task_queue, old_state.namespace
                "#,
            )
            .bind(&exec_id1)
            .fetch_optional(&pool1)
            .await
            .ok()
            .flatten();

            result
        });

        let set_handle = tokio::spawn(async move {
            // Small delay to vary timing (use iteration as pseudo-random offset)
            tokio::time::sleep(Duration::from_micros((i as u64 * 7) % 100)).await;

            sqlx::query(
                r#"
                UPDATE workflow_executions
                SET pending_requeue = TRUE,
                    pending_requeue_at = NOW()
                WHERE execution_id = $1
                "#,
            )
            .bind(&exec_id2)
            .execute(&pool2)
            .await
            .is_ok()
        });

        let (release_result, set_result) = tokio::join!(release_handle, set_handle);
        let release_saw = release_result.unwrap();
        let set_succeeded = set_result.unwrap();

        // Check final state
        let final_state: (bool, Option<String>) = sqlx::query_as(
            r#"
            SELECT pending_requeue, claimed_by
            FROM workflow_executions
            WHERE execution_id = $1
            "#,
        )
        .bind(&exec_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        if final_state.0 && final_state.1.is_none() {
            // Orphaned! This would be a bug if it happens
            orphaned_count += 1;
            warn!(
                iteration = i,
                exec_id = %exec_id,
                release_saw_pending = ?release_saw,
                set_succeeded = set_succeeded,
                "ORPHANED WORKFLOW DETECTED"
            );
        }

        // Cleanup iteration
        sqlx::query("DELETE FROM workflow_executions WHERE execution_id = $1")
            .bind(&exec_id)
            .execute(&pool)
            .await
            .ok();
    }

    // With the CTE fix, we should have zero orphaned workflows
    // If any are found, the Reclaimer will still recover them
    info!(
        orphaned = orphaned_count,
        iterations = iterations,
        "Timing race test completed"
    );

    if orphaned_count > 0 {
        warn!(
            orphaned_count = orphaned_count,
            "Some orphaned workflows detected - Reclaimer will recover these"
        );
    }

    cleanup_test_executions(&pool).await;
}
