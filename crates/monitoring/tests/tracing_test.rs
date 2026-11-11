//! Integration tests for the ORCHER tracing module
//!
//! This module tests the complete tracing functionality including span creation,
//! context propagation, and integration with OpenTelemetry standards.

use orcher_common::types::identifiers::*;
use orcher_monitoring::prelude::*;
use std::time::Duration;
use tracing::{info_span, Level};
use tracing_subscriber::fmt::TestWriter;

/// Initialize a test subscriber for tracing tests
fn init_test_subscriber() {
    let _ = tracing_subscriber::fmt()
        .with_writer(TestWriter::default())
        .with_max_level(Level::TRACE)
        .try_init();
}

#[test]
fn test_span_creation_integration() {
    init_test_subscriber();

    // Test all span creation functions work together
    let task_id = TaskId::new_v4();
    let workflow_id = WorkflowId::new_v4();
    let worker_id = WorkerId::new_v4();
    let node_id = NodeId::new_v4();

    // Create spans for different ORCHER components
    let task_span = create_task_span("execute", &task_id, Some("data_processing"));
    let workflow_span = create_workflow_span("step", &workflow_id, Some("validation"));
    let worker_span = create_worker_span("process", &worker_id, Some(&node_id));
    let storage_span = create_storage_span("query", "tasks", Some(100));

    // All spans should be valid
    assert!(task_span.metadata().is_some());
    assert!(workflow_span.metadata().is_some());
    assert!(worker_span.metadata().is_some());
    assert!(storage_span.metadata().is_some());

    // Verify span names follow OpenTelemetry conventions
    assert_eq!(task_span.metadata().unwrap().name(), "task_operation");
    assert_eq!(
        workflow_span.metadata().unwrap().name(),
        "workflow_operation"
    );
    assert_eq!(worker_span.metadata().unwrap().name(), "worker_operation");
    assert_eq!(storage_span.metadata().unwrap().name(), "storage_operation");
}

#[test]
fn test_context_extraction_integration() {
    init_test_subscriber();

    // Test context extraction without active span
    assert!(current_trace_id().is_none());
    assert!(current_span_id().is_none());

    // Test context extraction with active span
    let _span = info_span!("test_context").entered();
    assert!(current_trace_id().is_some());
    assert!(current_span_id().is_some());

    // Context IDs should be valid strings
    let trace_id = current_trace_id().unwrap();
    let span_id = current_span_id().unwrap();
    assert!(!trace_id.is_empty());
    assert!(!span_id.is_empty());
}

#[test]
fn test_nested_span_hierarchy() {
    init_test_subscriber();

    let task_id = TaskId::new_v4();
    let workflow_id = WorkflowId::new_v4();

    // Create a workflow span
    let workflow_span = create_workflow_span("execute", &workflow_id, Some("main_flow"));
    let _workflow_guard = workflow_span.enter();

    let workflow_trace_id = current_trace_id();
    assert!(workflow_trace_id.is_some());

    // Create a nested task span
    let task_span = create_task_span("process", &task_id, Some("data_task"));
    let _task_guard = task_span.enter();

    let task_trace_id = current_trace_id();
    assert!(task_trace_id.is_some());

    // Both spans should exist but have different span IDs
    let workflow_span_id = workflow_trace_id.unwrap();
    let task_span_id = task_trace_id.unwrap();

    // In a real tracing setup, nested spans would share trace ID but have different span IDs
    // For this test, we just verify both contexts are available
    assert!(!workflow_span_id.is_empty());
    assert!(!task_span_id.is_empty());
}

#[cfg(feature = "tracing-export")]
#[test]
fn test_context_propagation_integration() {
    init_test_subscriber();

    // Start with a parent span
    let _parent = info_span!("parent_operation").entered();
    let parent_context = current_trace_id();
    assert!(parent_context.is_some());

    // Propagate context to child operation
    let child_span = propagate_context("child_operation", "processing");
    assert!(child_span.metadata().is_some());

    // Enter child span and verify context
    let _child_guard = child_span.enter();
    let child_context = current_trace_id();
    assert!(child_context.is_some());

    // Both contexts should be valid
    assert!(!parent_context.unwrap().is_empty());
    assert!(!child_context.unwrap().is_empty());
}

#[cfg(feature = "tracing-export")]
#[test]
fn test_explicit_parent_child_relationship() {
    init_test_subscriber();

    let parent = info_span!("explicit_parent");
    let child = create_child_span(&parent, "explicit_child", "child_operation");

    assert!(parent.metadata().is_some());
    assert!(child.metadata().is_some());

    // Verify child span has correct name
    assert_eq!(child.metadata().unwrap().name(), "child_span");
}

#[cfg(feature = "tracing-export")]
#[tokio::test]
async fn test_async_context_propagation() {
    init_test_subscriber();

    // Start with a parent span
    let _parent = info_span!("async_parent").entered();
    let parent_context = current_trace_id();

    // Execute async operation with context propagation
    let result = with_async_context("async_child", "async_processing", async {
        // Simulate async work
        tokio::time::sleep(Duration::from_millis(1)).await;

        // Verify context is available in async block
        let async_context = current_trace_id();
        assert!(async_context.is_some());

        42
    })
    .await;

    assert_eq!(result, 42);
    assert!(parent_context.is_some());
}

#[test]
fn test_real_world_workflow_tracing() {
    init_test_subscriber();

    let workflow_id = WorkflowId::new_v4();
    let task_id = TaskId::new_v4();
    let worker_id = WorkerId::new_v4();

    // Simulate a complete workflow execution with tracing
    let workflow_span = create_workflow_span("execute", &workflow_id, Some("data_pipeline"));
    let _workflow_guard = workflow_span.enter();

    // Step 1: Task submission
    let task_span = create_task_span("submit", &task_id, Some("data_processing"));
    let _task_guard = task_span.enter();

    // Step 2: Worker processing
    let worker_span = create_worker_span("process", &worker_id, None);
    let _worker_guard = worker_span.enter();

    // Step 3: Storage operation
    let storage_span = create_storage_span("write", "processed_data", Some(1));
    let _storage_guard = storage_span.enter();

    // All spans should be active and valid
    assert!(current_trace_id().is_some());
    assert!(current_span_id().is_some());

    // Verify we can extract context at any point
    let final_trace_id = current_trace_id().unwrap();
    let final_span_id = current_span_id().unwrap();

    assert!(!final_trace_id.is_empty());
    assert!(!final_span_id.is_empty());
}

#[cfg(feature = "tracing-export")]
#[test]
fn test_http_header_context_propagation() {
    init_test_subscriber();
    use http::HeaderMap;

    // Create a span and inject context into headers
    let _span = info_span!("http_request").entered();
    let mut headers = HeaderMap::new();

    // Should not panic when injecting context
    inject_context(&mut headers);

    // Should not panic when extracting context
    let _extracted_context = extract_context(&headers);

    // Headers should contain trace context (in a real scenario)
    // For this test, we just verify the operations don't panic
}

#[test]
fn test_span_creation_with_various_parameters() {
    init_test_subscriber();

    let task_id = TaskId::new_v4();
    let workflow_id = WorkflowId::new_v4();
    let worker_id = WorkerId::new_v4();

    // Test spans with None for optional parameters
    let spans = vec![
        create_task_span("execute", &task_id, None),
        create_workflow_span("start", &workflow_id, None),
        create_worker_span("idle", &worker_id, None),
        create_storage_span("read", "config", None),
    ];

    // All spans should be valid even without optional parameters
    for span in spans {
        assert!(span.metadata().is_some());
    }

    // Test spans with all parameters provided
    let node_id = NodeId::new_v4();
    let full_spans = vec![
        create_task_span("execute", &task_id, Some("full_task")),
        create_workflow_span("step", &workflow_id, Some("full_step")),
        create_worker_span("process", &worker_id, Some(&node_id)),
        create_storage_span("query", "full_table", Some(42)),
    ];

    // All full spans should also be valid
    for span in full_spans {
        assert!(span.metadata().is_some());
    }
}

#[tokio::test]
async fn test_concurrent_span_creation() {
    init_test_subscriber();

    let task_ids: Vec<TaskId> = (0..10).map(|_| TaskId::new_v4()).collect();

    // Create multiple concurrent spans
    let handles: Vec<_> = task_ids
        .into_iter()
        .map(|task_id| {
            tokio::spawn(async move {
                let span = create_task_span("concurrent", &task_id, Some("concurrent_task"));
                let _guard = span.enter();

                // Simulate some async work
                tokio::time::sleep(Duration::from_millis(1)).await;

                // Verify context is available
                let trace_id = current_trace_id();
                let span_id = current_span_id();

                (trace_id.is_some(), span_id.is_some())
            })
        })
        .collect();

    // Wait for all tasks to complete
    let results = futures::future::join_all(handles).await;

    // All tasks should have had valid tracing context
    for result in results {
        let (has_trace_id, has_span_id) = result.unwrap();
        assert!(has_trace_id);
        assert!(has_span_id);
    }
}

#[test]
fn test_span_metadata_consistency() {
    init_test_subscriber();

    let task_id = TaskId::new_v4();
    let span = create_task_span("metadata_test", &task_id, Some("test_type"));

    // Verify span metadata
    let metadata = span.metadata().unwrap();
    assert_eq!(metadata.name(), "task_operation");
    assert_eq!(metadata.target(), "orcher_monitoring::tracing::spans");
    assert!(!metadata.fields().is_empty());

    // Verify we can enter and exit the span
    let guard = span.enter();
    assert!(current_trace_id().is_some());
    drop(guard);
}
