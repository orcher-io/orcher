//! Timer acceleration for testing and development.

use super::{TimerEntry, TimerId, TimerWheel, TimerWheelConfig};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Configuration for the timer accelerator
#[derive(Debug, Clone)]
pub struct AcceleratorConfig {
    /// Timer wheel configuration
    pub wheel_config: TimerWheelConfig,

    /// Horizon: timers within this duration are loaded into wheel
    pub horizon: Duration,

    /// How often to check database for new timers
    pub db_poll_interval: Duration,

    /// Maximum timers to load from database per poll
    pub max_load_batch: usize,

    /// Whether to preload timers on startup
    pub preload_on_start: bool,
}

impl Default for AcceleratorConfig {
    fn default() -> Self {
        Self {
            wheel_config: TimerWheelConfig::default(),
            horizon: Duration::from_secs(60), // 1 minute lookahead
            db_poll_interval: Duration::from_secs(10),
            max_load_batch: 1000,
            preload_on_start: true,
        }
    }
}

impl AcceleratorConfig {
    /// Create config optimized for low latency
    pub fn low_latency() -> Self {
        Self {
            wheel_config: TimerWheelConfig::millisecond(),
            horizon: Duration::from_secs(30),
            db_poll_interval: Duration::from_secs(5),
            max_load_batch: 500,
            preload_on_start: true,
        }
    }

    /// Create config optimized for high volume
    pub fn high_volume() -> Self {
        Self {
            wheel_config: TimerWheelConfig::second(),
            horizon: Duration::from_secs(120),
            db_poll_interval: Duration::from_secs(15),
            max_load_batch: 5000,
            preload_on_start: true,
        }
    }
}

/// Timer accelerator that combines in-memory wheel with database persistence
pub struct TimerAccelerator {
    /// In-memory timer wheel for fast expiration
    wheel: RwLock<TimerWheel>,

    /// Configuration
    config: AcceleratorConfig,

    /// IDs of timers currently in the wheel (for dedup)
    loaded_timer_ids: RwLock<HashSet<String>>,

    /// Mapping from external timer ID to internal wheel timer ID
    external_to_internal: RwLock<HashMap<String, TimerId>>,

    /// Metrics
    metrics: AcceleratorMetrics,

    /// Last database poll time
    last_db_poll: RwLock<Instant>,
}

impl TimerAccelerator {
    /// Create a new timer accelerator
    pub fn new(config: AcceleratorConfig) -> Self {
        Self {
            wheel: RwLock::new(TimerWheel::new(config.wheel_config.clone())),
            config,
            loaded_timer_ids: RwLock::new(HashSet::new()),
            external_to_internal: RwLock::new(HashMap::new()),
            metrics: AcceleratorMetrics::default(),
            last_db_poll: RwLock::new(Instant::now()),
        }
    }

    /// Add a timer to the accelerator
    ///
    /// If the timer is within the horizon, it's added to the wheel.
    /// Otherwise, it should be persisted to the database.
    pub fn add_timer(
        &self,
        external_id: impl Into<String>,
        workflow_id: impl Into<String>,
        timer_name: impl Into<String>,
        fire_in: Duration,
        payload: serde_json::Value,
    ) -> AddTimerResult {
        let external_id = external_id.into();
        let workflow_id = workflow_id.into();
        let timer_name = timer_name.into();

        // Check if within horizon
        if fire_in <= self.config.horizon {
            // Add to in-memory wheel
            let entry = TimerEntry::new(workflow_id, timer_name, payload);

            let mut wheel = self.wheel.write().unwrap_or_else(|e| e.into_inner());
            let internal_id = wheel.add_timer(fire_in, entry);

            // Track mapping
            self.loaded_timer_ids
                .write()
                .unwrap()
                .insert(external_id.clone());
            self.external_to_internal
                .write()
                .unwrap()
                .insert(external_id.clone(), internal_id);

            self.metrics
                .timers_added_to_wheel
                .fetch_add(1, Ordering::Relaxed);

            AddTimerResult::AddedToWheel {
                external_id,
                internal_id,
            }
        } else {
            // Timer is beyond horizon, should be persisted to database
            self.metrics
                .timers_beyond_horizon
                .fetch_add(1, Ordering::Relaxed);
            AddTimerResult::BeyondHorizon {
                external_id,
                fire_in,
            }
        }
    }

    /// Mark a timer as cancelled (removes from tracking)
    ///
    /// Note: This only removes from the accelerator's tracking.
    /// The timer may still fire from the wheel, but it will be ignored
    /// if not in the loaded_timer_ids set.
    pub fn cancel_timer(&self, external_id: &str) -> bool {
        let removed = self
            .loaded_timer_ids
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(external_id);
        if removed {
            self.external_to_internal
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(external_id);
            self.metrics
                .timers_cancelled
                .fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    /// Check if a timer is loaded in the wheel
    pub fn is_loaded(&self, external_id: &str) -> bool {
        self.loaded_timer_ids
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(external_id)
    }

    /// Advance the wheel and get expired timers
    pub fn tick(&self) -> Vec<TimerEntry> {
        let mut wheel = self.wheel.write().unwrap_or_else(|e| e.into_inner());
        let expired = wheel.tick();

        // Update tracking for expired timers
        if !expired.is_empty() {
            let mut loaded = self
                .loaded_timer_ids
                .write()
                .unwrap_or_else(|e| e.into_inner());
            let mut mapping = self
                .external_to_internal
                .write()
                .unwrap_or_else(|e| e.into_inner());

            for entry in &expired {
                // Remove from tracking (using workflow_id:timer_name as key)
                let key = format!("{}:{}", entry.workflow_id, entry.timer_name);
                loaded.remove(&key);
                mapping.remove(&key);
            }

            self.metrics
                .timers_fired
                .fetch_add(expired.len() as u64, Ordering::Relaxed);
        }

        expired
    }

    /// Advance by a specific duration and get all expired timers
    pub fn advance(&self, duration: Duration) -> Vec<TimerEntry> {
        let mut wheel = self.wheel.write().unwrap_or_else(|e| e.into_inner());
        let expired = wheel.advance(duration);

        // Update metrics
        if !expired.is_empty() {
            self.metrics
                .timers_fired
                .fetch_add(expired.len() as u64, Ordering::Relaxed);
        }

        expired
    }

    /// Load timers from an external source into the wheel
    ///
    /// This should be called with timers that are within the horizon
    /// and need to be tracked in the wheel.
    pub fn load_timers(&self, timers: Vec<ExternalTimer>) -> LoadResult {
        let mut loaded = 0;
        let mut skipped = 0;
        let mut failed = 0;

        let now = Instant::now();

        for timer in timers {
            // Skip if already loaded
            if self
                .loaded_timer_ids
                .read()
                .unwrap()
                .contains(&timer.external_id)
            {
                skipped += 1;
                continue;
            }

            // Calculate fire_in from fire_at
            let fire_in = if timer.fire_at > now {
                timer.fire_at.duration_since(now)
            } else {
                Duration::ZERO // Already expired
            };

            // Only load if within horizon
            if fire_in > self.config.horizon {
                skipped += 1;
                continue;
            }

            // Add to wheel
            let result = self.add_timer(
                timer.external_id.clone(),
                timer.workflow_id,
                timer.timer_name,
                fire_in,
                timer.payload,
            );

            match result {
                AddTimerResult::AddedToWheel { .. } => loaded += 1,
                AddTimerResult::BeyondHorizon { .. } => skipped += 1,
                AddTimerResult::Failed { .. } => failed += 1,
            }
        }

        self.metrics
            .timers_loaded
            .fetch_add(loaded, Ordering::Relaxed);

        *self.last_db_poll.write().unwrap_or_else(|e| e.into_inner()) = Instant::now();

        LoadResult {
            loaded,
            skipped,
            failed,
        }
    }

    /// Check if it's time to poll the database for new timers
    pub fn should_poll_db(&self) -> bool {
        self.last_db_poll
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .elapsed()
            >= self.config.db_poll_interval
    }

    /// Get the number of timers currently in the wheel
    pub fn wheel_size(&self) -> usize {
        self.wheel
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .pending_count()
    }

    /// Get the time until the next timer fires
    pub fn time_until_next(&self) -> Option<Duration> {
        self.wheel
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .time_until_next()
    }

    /// Get current metrics
    pub fn metrics(&self) -> AcceleratorStats {
        AcceleratorStats {
            timers_in_wheel: self.wheel_size(),
            timers_added_to_wheel: self.metrics.timers_added_to_wheel.load(Ordering::Relaxed),
            timers_beyond_horizon: self.metrics.timers_beyond_horizon.load(Ordering::Relaxed),
            timers_fired: self.metrics.timers_fired.load(Ordering::Relaxed),
            timers_cancelled: self.metrics.timers_cancelled.load(Ordering::Relaxed),
            timers_loaded: self.metrics.timers_loaded.load(Ordering::Relaxed),
            wheel_add_failures: self.metrics.wheel_add_failures.load(Ordering::Relaxed),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &AcceleratorConfig {
        &self.config
    }
}

impl Default for TimerAccelerator {
    fn default() -> Self {
        Self::new(AcceleratorConfig::default())
    }
}

/// Result of adding a timer
#[derive(Debug)]
pub enum AddTimerResult {
    /// Timer was added to the in-memory wheel
    AddedToWheel {
        /// External timer ID
        external_id: String,
        /// Internal wheel timer ID
        internal_id: TimerId,
    },

    /// Timer is beyond the horizon and should be persisted
    BeyondHorizon {
        /// External timer ID
        external_id: String,
        /// Duration until fire
        fire_in: Duration,
    },

    /// Failed to add timer
    Failed {
        /// External timer ID
        external_id: String,
        /// Error message
        error: String,
    },
}

impl AddTimerResult {
    /// Check if timer was added to wheel
    pub fn is_in_wheel(&self) -> bool {
        matches!(self, AddTimerResult::AddedToWheel { .. })
    }

    /// Check if timer needs database persistence
    pub fn needs_persistence(&self) -> bool {
        matches!(self, AddTimerResult::BeyondHorizon { .. })
    }
}

/// External timer to be loaded into the wheel
#[derive(Debug, Clone)]
pub struct ExternalTimer {
    /// External timer ID (e.g., database ID)
    pub external_id: String,

    /// Workflow ID
    pub workflow_id: String,

    /// Timer name
    pub timer_name: String,

    /// When the timer should fire
    pub fire_at: Instant,

    /// Payload data
    pub payload: serde_json::Value,
}

/// Result of loading timers
#[derive(Debug, Clone)]
pub struct LoadResult {
    /// Number of timers loaded
    pub loaded: u64,

    /// Number of timers skipped (already loaded or beyond horizon)
    pub skipped: u64,

    /// Number of timers that failed to load
    pub failed: u64,
}

impl LoadResult {
    /// Get total timers processed
    pub fn total(&self) -> u64 {
        self.loaded + self.skipped + self.failed
    }
}

/// Metrics for the accelerator
#[derive(Debug, Default)]
struct AcceleratorMetrics {
    /// Timers added to wheel
    timers_added_to_wheel: AtomicU64,

    /// Timers beyond horizon (need DB)
    timers_beyond_horizon: AtomicU64,

    /// Timers fired
    timers_fired: AtomicU64,

    /// Timers cancelled
    timers_cancelled: AtomicU64,

    /// Timers loaded from external source
    timers_loaded: AtomicU64,

    /// Wheel add failures
    wheel_add_failures: AtomicU64,
}

/// Statistics snapshot
#[derive(Debug, Clone)]
pub struct AcceleratorStats {
    /// Current timers in wheel
    pub timers_in_wheel: usize,

    /// Total timers added to wheel
    pub timers_added_to_wheel: u64,

    /// Total timers beyond horizon
    pub timers_beyond_horizon: u64,

    /// Total timers fired
    pub timers_fired: u64,

    /// Total timers cancelled
    pub timers_cancelled: u64,

    /// Total timers loaded from external source
    pub timers_loaded: u64,

    /// Total wheel add failures
    pub wheel_add_failures: u64,
}

impl AcceleratorStats {
    /// Calculate wheel hit rate (timers served from wheel vs total)
    pub fn wheel_hit_rate(&self) -> f64 {
        let total = self.timers_added_to_wheel + self.timers_beyond_horizon;
        if total > 0 {
            self.timers_added_to_wheel as f64 / total as f64
        } else {
            1.0
        }
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accelerator_config_defaults() {
        let config = AcceleratorConfig::default();
        assert_eq!(config.horizon, Duration::from_secs(60));
        assert_eq!(config.db_poll_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_accelerator_config_presets() {
        let low_latency = AcceleratorConfig::low_latency();
        assert_eq!(low_latency.horizon, Duration::from_secs(30));

        let high_volume = AcceleratorConfig::high_volume();
        assert_eq!(high_volume.horizon, Duration::from_secs(120));
    }

    #[test]
    fn test_accelerator_creation() {
        let accelerator = TimerAccelerator::new(AcceleratorConfig::default());
        assert_eq!(accelerator.wheel_size(), 0);
    }

    #[test]
    fn test_add_timer_within_horizon() {
        let accelerator = TimerAccelerator::new(AcceleratorConfig::default());

        let result = accelerator.add_timer(
            "timer-1",
            "workflow-1",
            "timeout",
            Duration::from_secs(30), // Within 60s horizon
            serde_json::json!({"key": "value"}),
        );

        assert!(result.is_in_wheel());
        assert!(!result.needs_persistence());
        assert_eq!(accelerator.wheel_size(), 1);
        assert!(accelerator.is_loaded("timer-1"));
    }

    #[test]
    fn test_add_timer_beyond_horizon() {
        let accelerator = TimerAccelerator::new(AcceleratorConfig::default());

        let result = accelerator.add_timer(
            "timer-1",
            "workflow-1",
            "timeout",
            Duration::from_secs(120), // Beyond 60s horizon
            serde_json::json!({}),
        );

        assert!(!result.is_in_wheel());
        assert!(result.needs_persistence());
        assert_eq!(accelerator.wheel_size(), 0);
    }

    #[test]
    fn test_cancel_timer() {
        let accelerator = TimerAccelerator::new(AcceleratorConfig::default());

        accelerator.add_timer(
            "timer-1",
            "workflow-1",
            "timeout",
            Duration::from_secs(30),
            serde_json::json!({}),
        );

        assert!(accelerator.is_loaded("timer-1"));
        assert!(accelerator.cancel_timer("timer-1"));
        assert!(!accelerator.is_loaded("timer-1"));
    }

    #[test]
    fn test_cancel_nonexistent_timer() {
        let accelerator = TimerAccelerator::new(AcceleratorConfig::default());
        assert!(!accelerator.cancel_timer("nonexistent"));
    }

    #[test]
    fn test_tick_expires_timers() {
        let mut config = AcceleratorConfig::default();
        config.wheel_config.tick_duration = Duration::from_millis(1);
        let accelerator = TimerAccelerator::new(config);

        // Add timer that fires immediately (0 duration)
        accelerator.add_timer(
            "timer-1",
            "workflow-1",
            "timeout",
            Duration::ZERO,
            serde_json::json!({}),
        );

        // First tick should expire it
        let expired = accelerator.tick();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].workflow_id, "workflow-1");
    }

    #[test]
    fn test_load_timers() {
        let accelerator = TimerAccelerator::new(AcceleratorConfig::default());

        let timers = vec![
            ExternalTimer {
                external_id: "ext-1".to_string(),
                workflow_id: "wf-1".to_string(),
                timer_name: "timer1".to_string(),
                fire_at: Instant::now() + Duration::from_secs(30),
                payload: serde_json::json!({}),
            },
            ExternalTimer {
                external_id: "ext-2".to_string(),
                workflow_id: "wf-2".to_string(),
                timer_name: "timer2".to_string(),
                fire_at: Instant::now() + Duration::from_secs(45),
                payload: serde_json::json!({}),
            },
        ];

        let result = accelerator.load_timers(timers);
        assert_eq!(result.loaded, 2);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.failed, 0);
        assert_eq!(accelerator.wheel_size(), 2);
    }

    #[test]
    fn test_load_timers_skips_duplicates() {
        let accelerator = TimerAccelerator::new(AcceleratorConfig::default());

        let timer = ExternalTimer {
            external_id: "ext-1".to_string(),
            workflow_id: "wf-1".to_string(),
            timer_name: "timer1".to_string(),
            fire_at: Instant::now() + Duration::from_secs(30),
            payload: serde_json::json!({}),
        };

        // Load once
        accelerator.load_timers(vec![timer.clone()]);

        // Load again - should skip
        let result = accelerator.load_timers(vec![timer]);
        assert_eq!(result.loaded, 0);
        assert_eq!(result.skipped, 1);
    }

    #[test]
    fn test_should_poll_db() {
        let config = AcceleratorConfig {
            db_poll_interval: Duration::from_millis(50),
            ..Default::default()
        };
        let accelerator = TimerAccelerator::new(config);

        // Initially should not poll (just created)
        assert!(!accelerator.should_poll_db());

        // After waiting past interval
        std::thread::sleep(Duration::from_millis(60));
        assert!(accelerator.should_poll_db());
    }

    #[test]
    fn test_metrics() {
        let accelerator = TimerAccelerator::new(AcceleratorConfig::default());

        // Add some timers
        accelerator.add_timer(
            "t1",
            "wf1",
            "timer",
            Duration::from_secs(30),
            serde_json::json!({}),
        );
        accelerator.add_timer(
            "t2",
            "wf2",
            "timer",
            Duration::from_secs(120),
            serde_json::json!({}),
        );

        let stats = accelerator.metrics();
        assert_eq!(stats.timers_added_to_wheel, 1);
        assert_eq!(stats.timers_beyond_horizon, 1);
        assert!((stats.wheel_hit_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_load_result_total() {
        let result = LoadResult {
            loaded: 5,
            skipped: 3,
            failed: 2,
        };
        assert_eq!(result.total(), 10);
    }
}
