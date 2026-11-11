//! End-to-end integration tests for queue-based task dispatch
//!
//! These tests verify the complete flow:
//! Orchestrator → QueueDispatcher → Queue → QueueConsumer → TaskExecutor
//!
//! NOTE: These tests are temporarily ignored during the stream-based event migration.
//! Queue integration will be re-enabled after the migration is complete.

// Temporarily disable compilation of these tests until queue integration is re-enabled
#![cfg(any())]
#![allow(dead_code)]
#![allow(unused_imports)]

use orcher_common::types::time::Timestamptz;
use orcher_core::error::CoreResult;
use orcher_core::prelude::*;
use orcher_core::queue::dispatch::{QueueConsumer, TaskExecutionOutcome, TaskExecutor};
use orcher_core::queue::provider::QueueRoutingConfig;
use orcher_core::queue::providers::InMemoryTaskQueue;
use orcher_core::queue::{QueueDefinition, TaskQueue};
use orcher_core::runtime::{DispatchStrategy, Orchestrator};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// Mock task executor for testing
struct TestExecutor {
    executions: Arc<AtomicUsize>,
    executed_tasks: Arc<RwLock<Vec<TaskId>>>,
}

impl TestExecutor {
    fn new() -> Self {
        Self {
            executions: Arc::new(AtomicUsize::new(0)),
            executed_tasks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn execution_count(&self) -> usize {
        self.executions.load(Ordering::SeqCst)
    }

    async fn executed_task_ids(&self) -> Vec<TaskId> {
        self.executed_tasks.read().await.clone()
    }
}

#[async_trait::async_trait]
impl TaskExecutor for TestExecutor {
    async fn execute(&self, task: TaskSubmission) -> TaskExecutionOutcome {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.executed_tasks.write().await.push(task.task_id);
        TaskExecutionOutcome::Success
    }

    async fn has_capacity(&self) -> bool {
        true
    }
}

fn create_test_task(name: &str, priority: TaskPriority) -> TaskSubmission {
    TaskSubmission {
        task_id: TaskId::new_v4(),
        name: name.to_string(),
        task_type: "test".to_string(),
        payload: serde_json::json!({"data": name}),
        priority,
        resources: ResourceRequirements {
            min_cpu_cores: None,
            min_memory_mb: None,
            capabilities: vec![],
            tags: HashMap::new(),
        },
        timeout_seconds: Some(300),
        max_retries: 3,
        tags: HashMap::new(),
        submitted_at: Timestamptz::now(),
    }
}

fn create_gpu_task(name: &str) -> TaskSubmission {
    TaskSubmission {
        task_id: TaskId::new_v4(),
        name: name.to_string(),
        task_type: "gpu".to_string(),
        payload: serde_json::json!({"data": name}),
        priority: TaskPriority::Normal,
        resources: ResourceRequirements {
            min_cpu_cores: None,
            min_memory_mb: None,
            capabilities: vec![WorkerCapability {
                name: "gpu".to_string(),
                version: None,
            }],
            tags: HashMap::new(),
        },
        timeout_seconds: Some(300),
        max_retries: 3,
        tags: HashMap::new(),
        submitted_at: Timestamptz::now(),
    }
}

#[tokio::test]
#[ignore = "Queue integration temporarily disabled during stream-based event migration"]
async fn test_end_to_end_queue_dispatch() -> CoreResult<()> {
    // Create shared queue
    let queue: Arc<dyn TaskQueue<TaskSubmission>> = Arc::new(InMemoryTaskQueue::new());

    // Create orchestrator with queue-based dispatch
    let routing_config = QueueRoutingConfig {
        queues: HashMap::new(),
    };
    let orchestrator = Orchestrator::new_with_queue(queue.clone(), routing_config, None).await?;
    orchestrator.initialize().await?;

    // Create worker consumer
    let executor = Arc::new(TestExecutor::new());
    let capabilities = vec![]; // Generic worker
    let consumer = Arc::new(QueueConsumer::new(
        queue.clone(),
        capabilities,
        executor.clone(),
    ));

    // Submit a task
    let task = create_test_task("test-task", TaskPriority::Normal);
    let task_id = task.task_id;
    orchestrator.submit_task(task).await?;

    // Start consumer in background
    let consumer_clone = consumer.clone();
    let consumer_handle = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), consumer_clone.run())
            .await
            .ok();
    });

    // Wait for task to be processed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stop consumer
    consumer.stop();
    let _ = consumer_handle.await;

    // Verify task was executed
    assert_eq!(executor.execution_count(), 1);
    let executed = executor.executed_task_ids().await;
    assert_eq!(executed.len(), 1);
    assert_eq!(executed[0], task_id);

    // Verify consumer stats
    let stats = consumer.stats().await;
    assert_eq!(stats.total_consumed, 1);
    assert_eq!(stats.total_acked, 1);
    assert_eq!(stats.total_success, 1);

    Ok(())
}

#[tokio::test]
#[ignore = "Queue integration temporarily disabled during stream-based event migration"]
async fn test_multiple_workers_same_queue() -> CoreResult<()> {
    // Create shared queue
    let queue: Arc<dyn TaskQueue<TaskSubmission>> = Arc::new(InMemoryTaskQueue::new());

    // Create orchestrator
    let routing_config = QueueRoutingConfig {
        queues: HashMap::new(),
    };
    let orchestrator = Orchestrator::new_with_queue(queue.clone(), routing_config, None).await?;
    orchestrator.initialize().await?;

    // Create two workers
    let executor1 = Arc::new(TestExecutor::new());
    let executor2 = Arc::new(TestExecutor::new());

    let consumer1 = Arc::new(QueueConsumer::new(queue.clone(), vec![], executor1.clone()));
    let consumer2 = Arc::new(QueueConsumer::new(queue.clone(), vec![], executor2.clone()));

    // Submit multiple tasks
    let mut task_ids = Vec::new();
    for i in 0..5 {
        let task = create_test_task(&format!("task-{}", i), TaskPriority::Normal);
        task_ids.push(task.task_id);
        orchestrator.submit_task(task).await?;
    }

    // Start consumers
    let c1 = consumer1.clone();
    let c2 = consumer2.clone();

    let handle1 = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), c1.run())
            .await
            .ok();
    });

    let handle2 = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), c2.run())
            .await
            .ok();
    });

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Stop consumers
    consumer1.stop();
    consumer2.stop();
    let _ = tokio::join!(handle1, handle2);

    // Verify tasks were distributed across workers
    let total_executed = executor1.execution_count() + executor2.execution_count();
    assert_eq!(total_executed, 5);

    Ok(())
}

#[tokio::test]
#[ignore = "Queue integration temporarily disabled during stream-based event migration"]
async fn test_priority_based_consumption() -> CoreResult<()> {
    // Create shared queue
    let queue: Arc<dyn TaskQueue<TaskSubmission>> = Arc::new(InMemoryTaskQueue::new());

    // Create orchestrator
    let routing_config = QueueRoutingConfig {
        queues: HashMap::new(),
    };
    let orchestrator = Orchestrator::new_with_queue(queue.clone(), routing_config, None).await?;
    orchestrator.initialize().await?;

    // Submit tasks with different priorities
    let low_task = create_test_task("low", TaskPriority::Low);
    let high_task = create_test_task("high", TaskPriority::High);
    let critical_task = create_test_task("critical", TaskPriority::Critical);

    orchestrator.submit_task(low_task).await?;
    orchestrator.submit_task(high_task).await?;
    orchestrator.submit_task(critical_task).await?;

    // Create worker
    let executor = Arc::new(TestExecutor::new());
    let consumer = Arc::new(QueueConsumer::new(queue.clone(), vec![], executor.clone()));

    // Start consumer
    let c = consumer.clone();
    let handle = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), c.run())
            .await
            .ok();
    });

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Stop consumer
    consumer.stop();
    let _ = handle.await;

    // Verify all tasks were executed
    assert_eq!(executor.execution_count(), 3);

    Ok(())
}

#[tokio::test]
#[ignore = "Queue integration temporarily disabled during stream-based event migration"]
async fn test_capability_based_routing() -> CoreResult<()> {
    // Create shared queue
    let queue: Arc<dyn TaskQueue<TaskSubmission>> = Arc::new(InMemoryTaskQueue::new());

    // Create orchestrator with routing config
    let mut routing_config = QueueRoutingConfig {
        queues: HashMap::new(),
    };
    routing_config.queues.insert(
        "gpu-tasks".to_string(),
        QueueDefinition {
            pattern: "gpu.*".to_string(),
            config: orcher_core::queue::types::QueueConfig::default(),
        },
    );

    let orchestrator = Orchestrator::new_with_queue(queue.clone(), routing_config, None).await?;
    orchestrator.initialize().await?;

    // Submit GPU task
    let gpu_task = create_gpu_task("gpu-task");
    let gpu_task_id = gpu_task.task_id;
    orchestrator.submit_task(gpu_task).await?;

    // Create GPU worker
    let executor = Arc::new(TestExecutor::new());
    let gpu_capabilities = vec![WorkerCapability {
        name: "gpu".to_string(),
        version: None,
    }];
    let consumer = Arc::new(QueueConsumer::new(
        queue.clone(),
        gpu_capabilities,
        executor.clone(),
    ));

    // Verify worker subscribed to GPU queues
    let subscriptions = consumer.subscribed_queues();
    assert!(subscriptions
        .iter()
        .any(|q| q.contains("gpu.normal") || q.contains("gpu")));

    // Start consumer
    let c = consumer.clone();
    let handle = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), c.run())
            .await
            .ok();
    });

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stop consumer
    consumer.stop();
    let _ = handle.await;

    // Verify GPU task was executed
    let executed = executor.executed_task_ids().await;
    assert!(executed.contains(&gpu_task_id));

    Ok(())
}

#[tokio::test]
#[ignore = "Queue integration temporarily disabled during stream-based event migration"]
async fn test_queue_based_priority_handling() -> CoreResult<()> {
    // Create queue with small max size
    let queue: Arc<dyn TaskQueue<TaskSubmission>> = Arc::new(InMemoryTaskQueue::new());

    // Create a queue with size limit
    queue
        .create_queue(
            "orcher:queue:default.normal",
            orcher_core::queue::types::QueueConfig {
                max_size: 2,
                ..Default::default()
            },
        )
        .await?;

    // Create orchestrator
    let routing_config = QueueRoutingConfig {
        queues: HashMap::new(),
    };
    let orchestrator = Orchestrator::new_with_queue(queue.clone(), routing_config, None).await?;
    orchestrator.initialize().await?;

    // Submit tasks up to limit
    let task1 = create_test_task("task1", TaskPriority::Normal);
    let task2 = create_test_task("task2", TaskPriority::Normal);

    assert!(orchestrator.submit_task(task1).await.is_ok());
    assert!(orchestrator.submit_task(task2).await.is_ok());

    // Third task should fail (queue full)
    let task3 = create_test_task("task3", TaskPriority::Normal);
    let result = orchestrator.submit_task(task3).await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
#[ignore = "Queue integration temporarily disabled during stream-based event migration"]
async fn test_hybrid_dispatch_strategy() -> CoreResult<()> {
    // This test verifies the hybrid dispatch logic
    // Critical tasks should bypass queue, normal tasks should use queue

    let queue: Arc<dyn TaskQueue<TaskSubmission>> = Arc::new(InMemoryTaskQueue::new());
    let routing_config = QueueRoutingConfig {
        queues: HashMap::new(),
    };

    let mut orchestrator =
        Orchestrator::new_with_queue(queue.clone(), routing_config, None).await?;
    orchestrator.initialize().await?;

    // Switch to hybrid mode
    orchestrator
        .set_dispatch_strategy(DispatchStrategy::Hybrid)
        .await?;

    assert_eq!(orchestrator.dispatch_strategy(), DispatchStrategy::Hybrid);

    // Submit tasks with different priorities
    let normal_task = create_test_task("normal", TaskPriority::Normal);
    let critical_task = create_test_task("critical", TaskPriority::Critical);

    // Both should succeed
    assert!(orchestrator.submit_task(normal_task).await.is_ok());
    assert!(orchestrator.submit_task(critical_task).await.is_ok());

    Ok(())
}
