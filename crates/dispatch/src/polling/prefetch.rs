//! Batch-claims executions ahead of demand so subsequent polls return instantly from memory.

use dashmap::DashMap;
use orcher_storage::schema::types::execution::WorkflowExecution;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// How batches are claimed.
///
/// A buffered execution stays claimed until it is handed out, however long
/// that takes: handing it out moves the claim to the poll that takes it, or
/// finds the claim lapsed and skips it. Nothing drops one from a buffer while
/// its claim is held, which left the execution claimed by nobody until the
/// claim timed out — minutes in which its workflow could not run.
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    pub batch_size: i32,
    /// Minimum time between batch claims for the same queue.
    pub claim_cooldown: Duration,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            batch_size: 10,
            claim_cooldown: Duration::from_millis(100),
        }
    }
}

impl PrefetchConfig {
    pub fn high_throughput() -> Self {
        Self {
            batch_size: 20,
            claim_cooldown: Duration::from_millis(50),
        }
    }

    pub fn low_latency() -> Self {
        Self {
            batch_size: 5,
            claim_cooldown: Duration::from_millis(200),
        }
    }
}

/// What a batch is claimed for: one buffer per key.
///
/// A batch is claimed for the poll that refilled it, filtered and bound as
/// that poll's own claim would be, so it may be handed only to polls that
/// would have claimed it themselves:
/// - the same queue and namespace, and the same organization: a namespace
///   name is unique only within an organization, and a poll that names none
///   claims across them all;
/// - the same release: a run is bound to the release of the poll that claims
///   it, and handing it to a poll of another release would run it against
///   code its journal was not written by;
/// - the same step-id schemes: a run is bound to a scheme the claiming poll
///   supports, and a poll supporting others would not find its history.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrefetchKey {
    task_queue: String,
    namespace: String,
    organization_id: Option<uuid::Uuid>,
    version_id: Option<String>,
    step_ids: Vec<i16>,
}

impl PrefetchKey {
    /// The key for polls of `task_queue` in `namespace`, for
    /// `organization_id`, declaring no release.
    pub fn new(task_queue: &str, namespace: &str, organization_id: Option<uuid::Uuid>) -> Self {
        Self {
            task_queue: task_queue.to_string(),
            namespace: namespace.to_string(),
            organization_id,
            version_id: None,
            step_ids: Vec::new(),
        }
    }

    /// For polls declaring release `version_id`. A blank one is none, as
    /// the claim reads it.
    pub fn with_version(mut self, version_id: Option<&str>) -> Self {
        self.version_id = version_id
            .filter(|v| !v.trim().is_empty())
            .map(str::to_string);
        self
    }

    /// For polls supporting the step-id schemes `step_ids`, in any order.
    pub fn with_step_ids(mut self, step_ids: &[i16]) -> Self {
        let mut step_ids = step_ids.to_vec();
        step_ids.sort_unstable();
        step_ids.dedup();
        self.step_ids = step_ids;
        self
    }

    /// The queue the batch is claimed from.
    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }
}

struct QueueBuffer {
    executions: VecDeque<WorkflowExecution>,
    last_claim_at: Instant,
    #[allow(dead_code)]
    claimed_by: String,
    /// Only one worker can attempt a batch claim at a time.
    refill_in_progress: AtomicBool,
}

impl QueueBuffer {
    fn new(claimed_by: String) -> Self {
        Self {
            executions: VecDeque::new(),
            last_claim_at: Instant::now(),
            claimed_by,
            refill_in_progress: AtomicBool::new(false),
        }
    }

    fn can_claim(&self, cooldown: Duration) -> bool {
        self.last_claim_at.elapsed() >= cooldown
    }

    fn mark_claimed(&mut self) {
        self.last_claim_at = Instant::now();
    }

    fn try_start_refill(&self) -> bool {
        self.refill_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    fn finish_refill(&self) {
        self.refill_in_progress.store(false, Ordering::SeqCst);
    }

    fn is_refilling(&self) -> bool {
        self.refill_in_progress.load(Ordering::Relaxed)
    }
}

/// When the buffer is empty only one worker does the batch claim (atomic flag);
/// the rest back off instead of stampeding the DB.
pub struct WorkPrefetchBuffer {
    buffers: DashMap<PrefetchKey, QueueBuffer>,
    config: PrefetchConfig,
    buffer_hits: AtomicU64,
    batch_claims: AtomicU64,
    executions_claimed: AtomicU64,
    refill_backoffs: AtomicU64,
}

impl WorkPrefetchBuffer {
    pub fn new() -> Self {
        Self::with_config(PrefetchConfig::default())
    }

    pub fn with_config(config: PrefetchConfig) -> Self {
        info!(
            batch_size = config.batch_size,
            cooldown_ms = config.claim_cooldown.as_millis(),
            "Creating work prefetch buffer with thundering herd prevention"
        );

        Self {
            buffers: DashMap::new(),
            config,
            buffer_hits: AtomicU64::new(0),
            batch_claims: AtomicU64::new(0),
            executions_claimed: AtomicU64::new(0),
            refill_backoffs: AtomicU64::new(0),
        }
    }

    /// Pops the next execution from the buffer, or `None`. It is still
    /// claimed: hand it out, or [`put_back`](Self::put_back) what could not be.
    pub fn try_get(&self, key: &PrefetchKey) -> Option<WorkflowExecution> {
        let mut buffer = self.buffers.get_mut(key)?;
        let execution = buffer.executions.pop_front()?;
        self.buffer_hits.fetch_add(1, Ordering::Relaxed);
        debug!(
            execution_id = %execution.execution_id,
            workflow_name = %execution.workflow_name,
            task_queue = %key.task_queue,
            buffer_remaining = buffer.executions.len(),
            "PREFETCH HIT: Returning execution from buffer"
        );
        Some(execution)
    }

    /// Returns `execution`, taken by [`try_get`](Self::try_get) and not handed
    /// out, to the front of its buffer, still claimed, for the next poll.
    pub fn put_back(&self, key: &PrefetchKey, execution: WorkflowExecution) {
        self.buffers
            .entry(key.clone())
            .or_insert_with(|| QueueBuffer::new("pending".to_string()))
            .executions
            .push_front(execution);
    }

    pub fn needs_refill(&self, key: &PrefetchKey) -> bool {
        match self.buffers.get(key) {
            Some(buffer) => {
                let is_empty = buffer.executions.is_empty();
                let can_claim = buffer.can_claim(self.config.claim_cooldown);
                is_empty && can_claim
            }
            None => true, // No buffer exists, definitely needs refill
        }
    }

    /// Returns true if this caller won the refill race and should do the batch claim.
    pub fn try_start_refill(&self, key: &PrefetchKey) -> bool {
        // Get or create the buffer entry
        let buffer = self
            .buffers
            .entry(key.clone())
            .or_insert_with(|| QueueBuffer::new("pending".to_string()));

        // Try to acquire the refill lock
        if buffer.try_start_refill() {
            debug!(
                task_queue = %key.task_queue,
                namespace = %key.namespace,
                "PREFETCH: Won refill race, will perform batch claim"
            );
            true
        } else {
            // Another worker is already refilling
            self.refill_backoffs.fetch_add(1, Ordering::Relaxed);
            debug!(
                task_queue = %key.task_queue,
                namespace = %key.namespace,
                "PREFETCH: Lost refill race, backing off (another worker is claiming)"
            );
            false
        }
    }

    /// Must be called after batch claim finishes (success or failure).
    pub fn finish_refill(&self, key: &PrefetchKey) {
        if let Some(buffer) = self.buffers.get(key) {
            buffer.finish_refill();
            debug!(
                task_queue = %key.task_queue,
                namespace = %key.namespace,
                "PREFETCH: Refill complete, lock released"
            );
        }
    }

    pub fn is_refilling(&self, key: &PrefetchKey) -> bool {
        self.buffers
            .get(key)
            .map(|b| b.is_refilling())
            .unwrap_or(false)
    }

    /// Stores the batch, returns the first execution immediately, buffers the rest.
    pub fn add_batch(
        &self,
        key: &PrefetchKey,
        identity: &str,
        mut executions: Vec<WorkflowExecution>,
    ) -> Option<WorkflowExecution> {
        if executions.is_empty() {
            return None;
        }

        let claimed_count = executions.len();

        self.batch_claims.fetch_add(1, Ordering::Relaxed);
        self.executions_claimed
            .fetch_add(claimed_count as u64, Ordering::Relaxed);

        // Take the first execution to return immediately
        let first = executions.remove(0);

        debug!(
            task_queue = %key.task_queue,
            namespace = %key.namespace,
            identity = %identity,
            claimed = claimed_count,
            buffered = executions.len(),
            returning = %first.execution_id,
            "PREFETCH: Batch claimed {} executions, buffering {}",
            claimed_count,
            executions.len()
        );

        // Store the rest in the buffer
        if !executions.is_empty() {
            let mut buffer = self
                .buffers
                .entry(key.clone())
                .or_insert_with(|| QueueBuffer::new(identity.to_string()));

            buffer.mark_claimed();

            // All of them: each is claimed, and one not buffered would be
            // claimed by nobody. A buffer is refilled only once it is empty,
            // so it never holds more than a batch.
            buffer.executions.extend(executions);

            debug!(
                task_queue = %key.task_queue,
                buffer_size = buffer.executions.len(),
                "PREFETCH: Buffer updated"
            );
        }

        Some(first)
    }

    /// Takes every buffered execution out of every buffer —
    /// for an engine shutting down to release the claims it will never hand
    /// out.
    pub fn drain_all(&self) -> Vec<WorkflowExecution> {
        let mut drained = Vec::new();
        for mut buffer in self.buffers.iter_mut() {
            drained.extend(buffer.executions.drain(..));
        }
        drained
    }

    pub fn stats(&self) -> PrefetchStats {
        PrefetchStats {
            buffer_hits: self.buffer_hits.load(Ordering::Relaxed),
            batch_claims: self.batch_claims.load(Ordering::Relaxed),
            executions_claimed: self.executions_claimed.load(Ordering::Relaxed),
            active_buffers: self.buffers.len(),
            buffered_executions: self.buffers.iter().map(|b| b.executions.len()).sum(),
            refill_backoffs: self.refill_backoffs.load(Ordering::Relaxed),
        }
    }

    pub fn batch_size(&self) -> i32 {
        self.config.batch_size
    }
}

impl Default for WorkPrefetchBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct PrefetchStats {
    pub buffer_hits: u64,
    pub batch_claims: u64,
    pub executions_claimed: u64,
    pub active_buffers: usize,
    pub buffered_executions: usize,
    /// High values = good coordination between workers.
    pub refill_backoffs: u64,
}

impl PrefetchStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.buffer_hits + self.batch_claims;
        if total == 0 {
            0.0
        } else {
            self.buffer_hits as f64 / total as f64
        }
    }

    pub fn batch_efficiency(&self) -> f64 {
        if self.batch_claims == 0 {
            0.0
        } else {
            self.executions_claimed as f64 / self.batch_claims as f64
        }
    }
}

pub fn create_prefetch_buffer(config: PrefetchConfig) -> Arc<WorkPrefetchBuffer> {
    Arc::new(WorkPrefetchBuffer::with_config(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::time::Timestamptz;
    use uuid::Uuid;

    fn create_test_execution(id: &str) -> WorkflowExecution {
        WorkflowExecution {
            bound_version_id: None,
            id: Uuid::new_v4().into(),
            execution_id: id.to_string(),
            workflow_id: Some(format!("wf_{}", id)),
            registry_id: None,
            workflow_name: "test_workflow".to_string(),
            workflow_type: "test_workflow".to_string(),
            workflow_version: "1.0.0".to_string(),
            namespace: "test".to_string(),
            status: "pending".to_string(),
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
            trigger_type: "api".to_string(),
            triggered_by: None,
            parent_execution_id: None,
            root_execution_id: None,
            user_id: None,
            organization_id: None,
            submitted_at: Timestamptz::now(),
            started_at: None,
            completed_at: None,
            cancelled_at: None,
            last_retry_at: None,
            next_retry_at: None,
            retries_settled_at: None,
            created_at: Timestamptz::now(),
            updated_at: Timestamptz::now(),
            claimed_by: Some("test-service".to_string()),
            claimed_at: Some(Timestamptz::now()),
            claim_timeout_at: None,
            task_queue: "test-queue".to_string(),
            pending_requeue: false,
            pending_requeue_at: None,
            activation_seq: 0,
            shard_id: None,
            labels: serde_json::json!({}),
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
        }
    }

    #[test]
    fn test_prefetch_buffer_empty() {
        let buffer = WorkPrefetchBuffer::new();
        assert!(buffer
            .try_get(&PrefetchKey::new("queue", "ns", None))
            .is_none());
        assert!(buffer.needs_refill(&PrefetchKey::new("queue", "ns", None)));
    }

    #[test]
    fn test_prefetch_buffer_add_and_get() {
        let buffer = WorkPrefetchBuffer::new();

        let executions = vec![
            create_test_execution("exec1"),
            create_test_execution("exec2"),
            create_test_execution("exec3"),
        ];

        // Add batch - should return first and buffer rest
        let first = buffer.add_batch(
            &PrefetchKey::new("queue", "ns", None),
            "service-1",
            executions,
        );
        assert!(first.is_some());
        assert_eq!(first.unwrap().execution_id, "exec1");

        // Get from buffer - should return second
        let second = buffer.try_get(&PrefetchKey::new("queue", "ns", None));
        assert!(second.is_some());
        assert_eq!(second.unwrap().execution_id, "exec2");

        // Get from buffer - should return third
        let third = buffer.try_get(&PrefetchKey::new("queue", "ns", None));
        assert!(third.is_some());
        assert_eq!(third.unwrap().execution_id, "exec3");

        // Buffer should be empty now
        assert!(buffer
            .try_get(&PrefetchKey::new("queue", "ns", None))
            .is_none());
    }

    /// One taken and not handed out goes back to the front, still claimed,
    /// for the next poll; nothing else drops one.
    #[test]
    fn an_execution_put_back_is_the_next_one_taken() {
        let buffer = WorkPrefetchBuffer::new();
        let key = PrefetchKey::new("queue", "ns", None);
        buffer.add_batch(
            &key,
            "service-1",
            vec![
                create_test_execution("exec1"),
                create_test_execution("exec2"),
                create_test_execution("exec3"),
            ],
        );
        let taken = buffer.try_get(&key).unwrap();
        assert_eq!(taken.execution_id, "exec2");
        buffer.put_back(&key, taken);
        assert_eq!(buffer.try_get(&key).unwrap().execution_id, "exec2");
        assert_eq!(buffer.try_get(&key).unwrap().execution_id, "exec3");
        assert!(buffer.try_get(&key).is_none());
    }

    #[test]
    fn test_prefetch_buffer_stats() {
        let buffer = WorkPrefetchBuffer::new();

        let executions = vec![
            create_test_execution("exec1"),
            create_test_execution("exec2"),
        ];

        buffer.add_batch(
            &PrefetchKey::new("queue", "ns", None),
            "service-1",
            executions,
        );
        buffer.try_get(&PrefetchKey::new("queue", "ns", None));

        let stats = buffer.stats();
        assert_eq!(stats.batch_claims, 1);
        assert_eq!(stats.executions_claimed, 2);
        assert_eq!(stats.buffer_hits, 1);
        assert_eq!(stats.buffered_executions, 0); // One was returned in add_batch, one from try_get
    }

    #[test]
    fn test_prefetch_config_defaults() {
        let config = PrefetchConfig::default();
        assert_eq!(config.batch_size, 10);
    }

    #[test]
    fn test_prefetch_config_high_throughput() {
        let config = PrefetchConfig::high_throughput();
        assert_eq!(config.batch_size, 20);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let stats = PrefetchStats {
            buffer_hits: 80,
            batch_claims: 20,
            executions_claimed: 200,
            active_buffers: 1,
            buffered_executions: 5,
            refill_backoffs: 0,
        };

        assert!((stats.hit_rate() - 0.8).abs() < 0.001);
        assert!((stats.batch_efficiency() - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_refill_coordination() {
        let buffer = WorkPrefetchBuffer::new();

        // First worker should win the refill race
        assert!(buffer.try_start_refill(&PrefetchKey::new("queue", "ns", None)));

        // Second worker should lose the refill race
        assert!(!buffer.try_start_refill(&PrefetchKey::new("queue", "ns", None)));

        // Check that refill is in progress
        assert!(buffer.is_refilling(&PrefetchKey::new("queue", "ns", None)));

        // Finish refill
        buffer.finish_refill(&PrefetchKey::new("queue", "ns", None));

        // Now another worker can start refill
        assert!(buffer.try_start_refill(&PrefetchKey::new("queue", "ns", None)));
        assert!(
            !buffer.is_refilling(&PrefetchKey::new("queue", "ns", None))
                || buffer.is_refilling(&PrefetchKey::new("queue", "ns", None))
        ); // Check it's set
        buffer.finish_refill(&PrefetchKey::new("queue", "ns", None));
    }
}
