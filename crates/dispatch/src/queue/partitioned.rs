//! Partitioned task queue with work stealing.

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct PartitionConfig {
    /// Should be a power of 2 for efficient hashing.
    pub num_partitions: usize,

    pub max_tasks_per_partition: usize,

    pub work_stealing_enabled: bool,

    pub max_steal_batch: usize,

    /// Partitions with fewer tasks than this will try to steal.
    pub steal_threshold: usize,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            num_partitions: 16,
            max_tasks_per_partition: 10000,
            work_stealing_enabled: true,
            max_steal_batch: 10,
            steal_threshold: 5,
        }
    }
}

impl PartitionConfig {
    pub fn high_concurrency() -> Self {
        Self {
            num_partitions: 64,
            max_tasks_per_partition: 5000,
            work_stealing_enabled: true,
            max_steal_batch: 20,
            steal_threshold: 10,
        }
    }

    pub fn low_concurrency() -> Self {
        Self {
            num_partitions: 4,
            max_tasks_per_partition: 20000,
            work_stealing_enabled: true,
            max_steal_batch: 5,
            steal_threshold: 2,
        }
    }

    pub fn no_stealing() -> Self {
        Self {
            work_stealing_enabled: false,
            ..Default::default()
        }
    }
}

pub trait Partitionable {
    fn partition_key(&self) -> &str;

    /// If set, overrides hash-based partitioning with a specific partition index.
    fn affinity_hint(&self) -> Option<usize> {
        None
    }
}

pub struct TaskQueuePartition<T> {
    tasks: Mutex<VecDeque<T>>,
    index: usize,
    max_capacity: usize,
    size: AtomicUsize,
    enqueues: AtomicU64,
    dequeues: AtomicU64,
    stolen_from: AtomicU64,
    stolen_to: AtomicU64,
}

impl<T> TaskQueuePartition<T> {
    fn new(index: usize, max_capacity: usize) -> Self {
        Self {
            tasks: Mutex::new(VecDeque::with_capacity(max_capacity.min(1000))),
            index,
            max_capacity,
            size: AtomicUsize::new(0),
            enqueues: AtomicU64::new(0),
            dequeues: AtomicU64::new(0),
            stolen_from: AtomicU64::new(0),
            stolen_to: AtomicU64::new(0),
        }
    }

    /// Returns `Err(task)` if at capacity.
    fn enqueue(&self, task: T) -> Result<(), T> {
        let mut queue = self.tasks.lock().unwrap();
        if queue.len() >= self.max_capacity {
            return Err(task);
        }
        queue.push_back(task);
        self.size.fetch_add(1, Ordering::Relaxed);
        self.enqueues.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn dequeue(&self) -> Option<T> {
        let mut queue = self.tasks.lock().unwrap();
        let task = queue.pop_front();
        if task.is_some() {
            self.size.fetch_sub(1, Ordering::Relaxed);
            self.dequeues.fetch_add(1, Ordering::Relaxed);
        }
        task
    }

    /// Steals up to half the queue, capped at `max_count`. Takes from the back.
    fn steal(&self, max_count: usize) -> Vec<T> {
        let mut queue = self.tasks.lock().unwrap();
        let steal_count = (queue.len() / 2).min(max_count);

        if steal_count == 0 {
            return Vec::new();
        }

        let mut stolen = Vec::with_capacity(steal_count);
        for _ in 0..steal_count {
            // Steal from the back (opposite end from dequeue)
            if let Some(task) = queue.pop_back() {
                stolen.push(task);
            } else {
                break;
            }
        }

        let stolen_count = stolen.len();
        self.size.fetch_sub(stolen_count, Ordering::Relaxed);
        self.stolen_from
            .fetch_add(stolen_count as u64, Ordering::Relaxed);

        stolen
    }

    fn receive_stolen(&self, tasks: Vec<T>) {
        let count = tasks.len();
        let mut queue = self.tasks.lock().unwrap();
        for task in tasks {
            queue.push_front(task); // Add to front for LIFO locality
        }
        self.size.fetch_add(count, Ordering::Relaxed);
        self.stolen_to.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn stats(&self) -> PartitionStats {
        PartitionStats {
            partition_index: self.index,
            current_size: self.size.load(Ordering::Relaxed),
            total_enqueues: self.enqueues.load(Ordering::Relaxed),
            total_dequeues: self.dequeues.load(Ordering::Relaxed),
            stolen_from: self.stolen_from.load(Ordering::Relaxed),
            stolen_to: self.stolen_to.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub partition_index: usize,
    pub current_size: usize,
    pub total_enqueues: u64,
    pub total_dequeues: u64,
    pub stolen_from: u64,
    pub stolen_to: u64,
}

#[derive(Debug, Clone)]
pub struct QueueStats {
    pub total_tasks: usize,
    pub partitions: Vec<PartitionStats>,
    pub total_enqueues: u64,
    pub total_dequeues: u64,
    pub total_steals: u64,
    pub num_partitions: usize,
    pub avg_tasks_per_partition: f64,
    /// Stddev of per-partition task counts - higher means more imbalanced.
    pub load_imbalance: f64,
}

pub struct PartitionedTaskQueue<T> {
    partitions: Vec<TaskQueuePartition<T>>,
    config: PartitionConfig,
    total_tasks: AtomicUsize,
    steal_index: AtomicUsize,
}

impl<T> PartitionedTaskQueue<T> {
    pub fn new(config: PartitionConfig) -> Arc<Self> {
        let partitions = (0..config.num_partitions)
            .map(|i| TaskQueuePartition::new(i, config.max_tasks_per_partition))
            .collect();

        Arc::new(Self {
            partitions,
            config,
            total_tasks: AtomicUsize::new(0),
            steal_index: AtomicUsize::new(0),
        })
    }

    pub fn with_defaults() -> Arc<Self> {
        Self::new(PartitionConfig::default())
    }

    pub fn enqueue(&self, task: T) -> Result<(), T>
    where
        T: Partitionable,
    {
        // Check for affinity hint first
        let partition_idx = if let Some(hint) = task.affinity_hint() {
            hint % self.partitions.len()
        } else {
            self.partition_for_key(task.partition_key())
        };

        self.enqueue_to_partition(partition_idx, task)
    }

    pub fn enqueue_to_partition(&self, partition_idx: usize, task: T) -> Result<(), T> {
        let idx = partition_idx % self.partitions.len();
        match self.partitions[idx].enqueue(task) {
            Ok(()) => {
                self.total_tasks.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(task) => Err(task),
        }
    }

    /// Uses `key` for partition selection instead of the task's own partition key.
    pub fn enqueue_with_key(&self, key: &str, task: T) -> Result<(), T> {
        let partition_idx = self.partition_for_key(key);
        self.enqueue_to_partition(partition_idx, task)
    }

    /// Falls back to work stealing if the partition is empty and stealing is enabled.
    pub fn dequeue_from_partition(&self, partition_idx: usize) -> Option<T> {
        let idx = partition_idx % self.partitions.len();
        let task = self.partitions[idx].dequeue();

        if task.is_some() {
            self.total_tasks.fetch_sub(1, Ordering::Relaxed);
            return task;
        }

        // Try work stealing if enabled and local partition is empty
        if self.config.work_stealing_enabled {
            self.try_steal_for(idx)
        } else {
            None
        }
    }

    pub fn dequeue_any(&self) -> Option<T> {
        // Try each partition in order
        for i in 0..self.partitions.len() {
            if let Some(task) = self.partitions[i].dequeue() {
                self.total_tasks.fetch_sub(1, Ordering::Relaxed);
                return Some(task);
            }
        }

        None
    }

    fn try_steal_for(&self, hungry_partition: usize) -> Option<T> {
        let num_partitions = self.partitions.len();

        // Start from a semi-random position to distribute stealing
        let start = self.steal_index.fetch_add(1, Ordering::Relaxed) % num_partitions;

        // Check all other partitions (num_partitions - 1 others)
        for offset in 0..num_partitions {
            let victim_idx = (start + offset) % num_partitions;

            // Don't steal from self
            if victim_idx == hungry_partition {
                continue;
            }

            // Check if victim has enough to steal
            let victim = &self.partitions[victim_idx];
            if victim.len() > self.config.steal_threshold {
                let stolen = victim.steal(self.config.max_steal_batch);
                if !stolen.is_empty() {
                    // Take one task for immediate return
                    let mut stolen = stolen;
                    let task = stolen.pop();

                    // Give rest to hungry partition
                    if !stolen.is_empty() {
                        self.partitions[hungry_partition].receive_stolen(stolen);
                    }

                    if let Some(t) = task {
                        self.total_tasks.fetch_sub(1, Ordering::Relaxed);
                        return Some(t);
                    }
                }
            }
        }

        None
    }

    fn partition_for_key(&self, key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.partitions.len()
    }

    pub fn len(&self) -> usize {
        self.total_tasks.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    pub fn partition_stats(&self, partition_idx: usize) -> Option<PartitionStats> {
        self.partitions.get(partition_idx).map(|p| p.stats())
    }

    pub fn stats(&self) -> QueueStats {
        let partition_stats: Vec<PartitionStats> =
            self.partitions.iter().map(|p| p.stats()).collect();

        let total_tasks: usize = partition_stats.iter().map(|s| s.current_size).sum();
        let total_enqueues: u64 = partition_stats.iter().map(|s| s.total_enqueues).sum();
        let total_dequeues: u64 = partition_stats.iter().map(|s| s.total_dequeues).sum();
        let total_steals: u64 = partition_stats.iter().map(|s| s.stolen_from).sum();

        let num_partitions = partition_stats.len();
        let avg_tasks = if num_partitions > 0 {
            total_tasks as f64 / num_partitions as f64
        } else {
            0.0
        };

        // Calculate standard deviation for load imbalance
        let variance: f64 = if num_partitions > 0 {
            partition_stats
                .iter()
                .map(|s| {
                    let diff = s.current_size as f64 - avg_tasks;
                    diff * diff
                })
                .sum::<f64>()
                / num_partitions as f64
        } else {
            0.0
        };
        let load_imbalance = variance.sqrt();

        QueueStats {
            total_tasks,
            partitions: partition_stats,
            total_enqueues,
            total_dequeues,
            total_steals,
            num_partitions,
            avg_tasks_per_partition: avg_tasks,
            load_imbalance,
        }
    }

    pub fn config(&self) -> &PartitionConfig {
        &self.config
    }

    pub fn clear(&self) {
        for partition in &self.partitions {
            let mut queue = partition.tasks.lock().unwrap();
            let count = queue.len();
            queue.clear();
            partition.size.store(0, Ordering::Relaxed);
            self.total_tasks.fetch_sub(count, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test task implementation
    #[derive(Debug, Clone, PartialEq)]
    struct TestTask {
        id: String,
        workflow_id: String,
        affinity: Option<usize>,
    }

    impl TestTask {
        fn new(id: &str, workflow_id: &str) -> Self {
            Self {
                id: id.to_string(),
                workflow_id: workflow_id.to_string(),
                affinity: None,
            }
        }

        fn with_affinity(mut self, partition: usize) -> Self {
            self.affinity = Some(partition);
            self
        }
    }

    impl Partitionable for TestTask {
        fn partition_key(&self) -> &str {
            &self.workflow_id
        }

        fn affinity_hint(&self) -> Option<usize> {
            self.affinity
        }
    }

    #[test]
    fn test_partition_config_defaults() {
        let config = PartitionConfig::default();
        assert_eq!(config.num_partitions, 16);
        assert!(config.work_stealing_enabled);
    }

    #[test]
    fn test_partition_config_presets() {
        let high = PartitionConfig::high_concurrency();
        assert_eq!(high.num_partitions, 64);

        let low = PartitionConfig::low_concurrency();
        assert_eq!(low.num_partitions, 4);

        let no_steal = PartitionConfig::no_stealing();
        assert!(!no_steal.work_stealing_enabled);
    }

    #[test]
    fn test_queue_creation() {
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::with_defaults();
        assert_eq!(queue.num_partitions(), 16);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_enqueue_dequeue() {
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::with_defaults();
        let task = TestTask::new("task-1", "workflow-1");

        queue.enqueue(task.clone()).unwrap();
        assert_eq!(queue.len(), 1);

        let dequeued = queue.dequeue_any();
        assert!(dequeued.is_some());
        assert_eq!(dequeued.unwrap().id, "task-1");
        assert!(queue.is_empty());
    }

    #[test]
    fn test_enqueue_to_partition() {
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::with_defaults();

        // Enqueue to specific partition
        let task = TestTask::new("task-1", "wf-1");
        queue.enqueue_to_partition(5, task).unwrap();

        // Dequeue from same partition
        let dequeued = queue.dequeue_from_partition(5);
        assert!(dequeued.is_some());
        assert_eq!(dequeued.unwrap().id, "task-1");
    }

    #[test]
    fn test_affinity_routing() {
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::with_defaults();

        // Task with affinity to partition 3
        let task = TestTask::new("task-1", "wf-1").with_affinity(3);
        queue.enqueue(task).unwrap();

        // Should be in partition 3
        let stats = queue.partition_stats(3).unwrap();
        assert_eq!(stats.current_size, 1);
    }

    #[test]
    fn test_work_stealing() {
        let config = PartitionConfig {
            num_partitions: 4,
            work_stealing_enabled: true,
            steal_threshold: 1, // Lower threshold for test
            max_steal_batch: 5,
            ..Default::default()
        };
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::new(config);

        // Load up partition 0 with many tasks (more than steal_threshold)
        for i in 0..20 {
            let task = TestTask::new(&format!("task-{}", i), "wf-1");
            queue.enqueue_to_partition(0, task).unwrap();
        }

        // Verify partition 0 has tasks
        let stats_before = queue.partition_stats(0).unwrap();
        assert!(
            stats_before.current_size > 1,
            "Partition 0 should have tasks"
        );

        // Partition 1 is empty - should steal from partition 0
        let dequeued = queue.dequeue_from_partition(1);
        assert!(
            dequeued.is_some(),
            "Should have stolen a task from partition 0"
        );

        // Check that stealing happened
        let stats = queue.stats();
        assert!(
            stats.total_steals > 0,
            "Should have recorded steal operations"
        );
    }

    #[test]
    fn test_work_stealing_disabled() {
        let config = PartitionConfig::no_stealing();
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::new(config);

        // Load up partition 0
        let task = TestTask::new("task-1", "wf-1");
        queue.enqueue_to_partition(0, task).unwrap();

        // Partition 1 is empty - should NOT steal
        let dequeued = queue.dequeue_from_partition(1);
        assert!(dequeued.is_none());

        // Task should still be in partition 0
        let dequeued = queue.dequeue_from_partition(0);
        assert!(dequeued.is_some());
    }

    #[test]
    fn test_multiple_tasks_same_partition() {
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::with_defaults();

        // Enqueue multiple tasks with same key (same partition)
        for i in 0..5 {
            let task = TestTask::new(&format!("task-{}", i), "same-workflow");
            queue.enqueue(task).unwrap();
        }

        assert_eq!(queue.len(), 5);

        // All should be in same partition
        let partition_idx = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            "same-workflow".hash(&mut hasher);
            (hasher.finish() as usize) % queue.num_partitions()
        };

        let stats = queue.partition_stats(partition_idx).unwrap();
        assert_eq!(stats.current_size, 5);
    }

    #[test]
    fn test_dequeue_any() {
        let config = PartitionConfig {
            num_partitions: 4,
            work_stealing_enabled: false,
            ..Default::default()
        };
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::new(config);

        // Enqueue to different partitions
        queue
            .enqueue_to_partition(0, TestTask::new("task-0", "wf"))
            .unwrap();
        queue
            .enqueue_to_partition(2, TestTask::new("task-2", "wf"))
            .unwrap();

        // dequeue_any should get both
        assert!(queue.dequeue_any().is_some());
        assert!(queue.dequeue_any().is_some());
        assert!(queue.dequeue_any().is_none());
    }

    #[test]
    fn test_partition_stats() {
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::with_defaults();

        let task = TestTask::new("task-1", "wf-1");
        queue.enqueue_to_partition(0, task).unwrap();

        let stats = queue.partition_stats(0).unwrap();
        assert_eq!(stats.current_size, 1);
        assert_eq!(stats.total_enqueues, 1);
        assert_eq!(stats.partition_index, 0);
    }

    #[test]
    fn test_aggregate_stats() {
        let config = PartitionConfig {
            num_partitions: 4,
            ..Default::default()
        };
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::new(config);

        // Enqueue tasks to different partitions
        queue
            .enqueue_to_partition(0, TestTask::new("t0", "wf"))
            .unwrap();
        queue
            .enqueue_to_partition(0, TestTask::new("t1", "wf"))
            .unwrap();
        queue
            .enqueue_to_partition(1, TestTask::new("t2", "wf"))
            .unwrap();

        let stats = queue.stats();
        assert_eq!(stats.total_tasks, 3);
        assert_eq!(stats.num_partitions, 4);
        assert_eq!(stats.total_enqueues, 3);
    }

    #[test]
    fn test_load_imbalance_metric() {
        let config = PartitionConfig {
            num_partitions: 4,
            ..Default::default()
        };
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::new(config);

        // All tasks in one partition - high imbalance
        for i in 0..100 {
            queue
                .enqueue_to_partition(0, TestTask::new(&format!("t{}", i), "wf"))
                .unwrap();
        }

        let stats = queue.stats();
        assert!(stats.load_imbalance > 0.0);
    }

    #[test]
    fn test_clear() {
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::with_defaults();

        for i in 0..10 {
            let task = TestTask::new(&format!("task-{}", i), &format!("wf-{}", i));
            queue.enqueue(task).unwrap();
        }

        assert_eq!(queue.len(), 10);

        queue.clear();
        assert!(queue.is_empty());
    }

    #[test]
    fn test_enqueue_with_key() {
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::with_defaults();

        let task = TestTask::new("task-1", "ignored-key");
        queue.enqueue_with_key("custom-key", task).unwrap();

        // Should use custom key for partitioning
        let partition_idx = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            "custom-key".hash(&mut hasher);
            (hasher.finish() as usize) % queue.num_partitions()
        };

        let stats = queue.partition_stats(partition_idx).unwrap();
        assert_eq!(stats.current_size, 1);
    }

    #[test]
    fn test_capacity_limit() {
        let config = PartitionConfig {
            num_partitions: 4,
            max_tasks_per_partition: 5,
            ..Default::default()
        };
        let queue: Arc<PartitionedTaskQueue<TestTask>> = PartitionedTaskQueue::new(config);

        // Fill up partition 0
        for i in 0..5 {
            let task = TestTask::new(&format!("task-{}", i), "wf");
            assert!(queue.enqueue_to_partition(0, task).is_ok());
        }

        // 6th should fail
        let overflow = TestTask::new("overflow", "wf");
        assert!(queue.enqueue_to_partition(0, overflow).is_err());
    }
}
