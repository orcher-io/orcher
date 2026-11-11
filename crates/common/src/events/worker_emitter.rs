//! Shared trait for emitting worker task events.

use super::WorkerTaskEvent;
use crate::types::identifiers::{TaskId, WorkerId};
use crate::OrcherResult;
use async_trait::async_trait;

#[async_trait]
pub trait WorkerEventEmitter: Send + Sync {
    async fn emit(&self, event: WorkerTaskEvent) -> OrcherResult<()>;

    async fn emit_task_started(&self, task_id: TaskId, worker_id: WorkerId) -> OrcherResult<()> {
        let event = WorkerTaskEvent::execution_started(task_id, worker_id);
        self.emit(event).await
    }

    async fn emit_task_completed(
        &self,
        task_id: TaskId,
        worker_id: WorkerId,
        output: serde_json::Value,
        duration_ms: u64,
    ) -> OrcherResult<()> {
        let event = WorkerTaskEvent::execution_completed(task_id, worker_id, output, duration_ms);
        self.emit(event).await
    }

    async fn emit_task_failed(
        &self,
        task_id: TaskId,
        worker_id: WorkerId,
        error: String,
        duration_ms: u64,
    ) -> OrcherResult<()> {
        let event = WorkerTaskEvent::execution_failed(task_id, worker_id, error, duration_ms);
        self.emit(event).await
    }

    async fn emit_task_timed_out(
        &self,
        task_id: TaskId,
        worker_id: WorkerId,
        timeout_seconds: u64,
    ) -> OrcherResult<()> {
        let event = WorkerTaskEvent::execution_timed_out(task_id, worker_id, timeout_seconds);
        self.emit(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::identifiers::Identifier;
    use std::sync::{Arc, Mutex};

    /// Mock emitter for testing
    struct MockEmitter {
        events: Arc<Mutex<Vec<WorkerTaskEvent>>>,
    }

    impl MockEmitter {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_events(&self) -> Vec<WorkerTaskEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WorkerEventEmitter for MockEmitter {
        async fn emit(&self, event: WorkerTaskEvent) -> OrcherResult<()> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_emit_task_started() {
        let emitter = MockEmitter::new();
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();

        emitter.emit_task_started(task_id, worker_id).await.unwrap();

        let events = emitter.get_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].task_id, task_id);
        assert_eq!(events[0].worker_id, worker_id);
    }

    #[tokio::test]
    async fn test_emit_task_completed() {
        let emitter = MockEmitter::new();
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let output = serde_json::json!({"status": "success"});
        let duration_ms = 1000;

        emitter
            .emit_task_completed(task_id, worker_id, output.clone(), duration_ms)
            .await
            .unwrap();

        let events = emitter.get_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].task_id, task_id);
    }

    #[tokio::test]
    async fn test_emit_task_failed() {
        let emitter = MockEmitter::new();
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let error = "Test error".to_string();
        let duration_ms = 500;

        emitter
            .emit_task_failed(task_id, worker_id, error.clone(), duration_ms)
            .await
            .unwrap();

        let events = emitter.get_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].task_id, task_id);
    }

    #[tokio::test]
    async fn test_emit_task_timed_out() {
        let emitter = MockEmitter::new();
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let timeout_seconds = 300;

        emitter
            .emit_task_timed_out(task_id, worker_id, timeout_seconds)
            .await
            .unwrap();

        let events = emitter.get_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].task_id, task_id);
    }

    #[tokio::test]
    async fn test_multiple_events() {
        let emitter = MockEmitter::new();
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();

        emitter.emit_task_started(task_id, worker_id).await.unwrap();
        emitter
            .emit_task_completed(task_id, worker_id, serde_json::json!({}), 100)
            .await
            .unwrap();

        let events = emitter.get_events();
        assert_eq!(events.len(), 2);
    }
}
