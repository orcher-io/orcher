//! Hierarchical timing wheel for efficient timer management.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Unique identifier for a timer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerId(u64);

impl TimerId {
    /// Create a new timer ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the underlying ID value
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TimerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "timer-{}", self.0)
    }
}

/// Timer entry containing the callback data
#[derive(Debug, Clone)]
pub struct TimerEntry {
    /// Timer ID
    pub id: TimerId,

    /// Workflow ID this timer belongs to
    pub workflow_id: String,

    /// Timer name/identifier within the workflow
    pub timer_name: String,

    /// Timer payload data
    pub payload: serde_json::Value,

    /// When the timer was created
    pub created_at: Instant,

    /// When the timer should fire (absolute)
    pub fire_at: Instant,

    /// Number of ticks remaining (for internal use)
    remaining_ticks: u64,
}

impl TimerEntry {
    /// Create a new timer entry
    pub fn new(
        workflow_id: impl Into<String>,
        timer_name: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: TimerId(0), // Will be set when added to wheel
            workflow_id: workflow_id.into(),
            timer_name: timer_name.into(),
            payload,
            created_at: Instant::now(),
            fire_at: Instant::now(),
            remaining_ticks: 0,
        }
    }

    /// Get the timer ID
    pub fn id(&self) -> TimerId {
        self.id
    }

    /// Check if the timer has expired
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.fire_at
    }

    /// Get time until fire
    pub fn time_until_fire(&self) -> Duration {
        self.fire_at.saturating_duration_since(Instant::now())
    }
}

/// Configuration for the timer wheel
#[derive(Debug, Clone)]
pub struct TimerWheelConfig {
    /// Number of slots in the wheel (should be power of 2)
    pub slots: usize,

    /// Duration per tick
    pub tick_duration: Duration,

    /// Number of wheel levels (for hierarchical wheel)
    pub levels: usize,

    /// Maximum timer duration supported
    pub max_duration: Duration,
}

impl Default for TimerWheelConfig {
    fn default() -> Self {
        Self {
            slots: 256,                                      // 256 slots per level
            tick_duration: Duration::from_millis(10),        // 10ms per tick
            levels: 4, // 4 levels: ~10ms, ~2.5s, ~10min, ~44hr
            max_duration: Duration::from_secs(24 * 60 * 60), // 24 hours max
        }
    }
}

impl TimerWheelConfig {
    /// Create config for millisecond-precision timers
    pub fn millisecond() -> Self {
        Self {
            slots: 256,
            tick_duration: Duration::from_millis(1),
            levels: 4,
            max_duration: Duration::from_secs(60 * 60), // 1 hour
        }
    }

    /// Create config for second-precision timers
    pub fn second() -> Self {
        Self {
            slots: 64,
            tick_duration: Duration::from_secs(1),
            levels: 3,
            max_duration: Duration::from_secs(24 * 60 * 60), // 24 hours
        }
    }

    /// Calculate the duration covered by one full wheel rotation at a given level
    #[allow(dead_code)]
    fn level_duration(&self, level: usize) -> Duration {
        let ticks_per_rotation = self.slots.pow(level as u32 + 1);
        self.tick_duration * ticks_per_rotation as u32
    }
}

/// A single level in the hierarchical timer wheel
#[derive(Debug)]
struct WheelLevel {
    /// Slots containing timer entries
    slots: Vec<VecDeque<TimerEntry>>,

    /// Current position in the wheel
    current_slot: usize,

    /// Number of complete rotations
    rotations: u64,
}

impl WheelLevel {
    fn new(slots: usize) -> Self {
        Self {
            slots: (0..slots).map(|_| VecDeque::new()).collect(),
            current_slot: 0,
            rotations: 0,
        }
    }

    /// Add a timer to a specific slot
    fn add_to_slot(&mut self, slot: usize, entry: TimerEntry) {
        self.slots[slot].push_back(entry);
    }

    /// Take all timers from the current slot
    fn take_current(&mut self) -> VecDeque<TimerEntry> {
        std::mem::take(&mut self.slots[self.current_slot])
    }

    /// Advance to the next slot, returning true if we completed a rotation
    fn advance(&mut self) -> bool {
        self.current_slot = (self.current_slot + 1) % self.slots.len();
        if self.current_slot == 0 {
            self.rotations += 1;
            true
        } else {
            false
        }
    }

    /// Get the current slot index
    fn current(&self) -> usize {
        self.current_slot
    }

    /// Get total timers in this level
    fn timer_count(&self) -> usize {
        self.slots.iter().map(|s| s.len()).sum()
    }
}

/// Metrics for the timer wheel
#[derive(Debug, Default)]
pub struct TimerWheelMetrics {
    /// Total timers added
    pub timers_added: AtomicU64,

    /// Total timers fired
    pub timers_fired: AtomicU64,

    /// Total timers cancelled
    pub timers_cancelled: AtomicU64,

    /// Total ticks processed
    pub ticks_processed: AtomicU64,

    /// Total cascades (timer moved to lower level)
    pub cascades: AtomicU64,
}

impl TimerWheelMetrics {
    /// Record a timer addition
    pub fn record_add(&self) {
        self.timers_added.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timer firing
    pub fn record_fire(&self) {
        self.timers_fired.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timer cancellation
    pub fn record_cancel(&self) {
        self.timers_cancelled.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a tick
    pub fn record_tick(&self) {
        self.ticks_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cascade
    pub fn record_cascade(&self) {
        self.cascades.fetch_add(1, Ordering::Relaxed);
    }
}

/// Statistics snapshot of the timer wheel
#[derive(Debug, Clone)]
pub struct TimerWheelStats {
    /// Total pending timers
    pub pending_timers: usize,

    /// Timers per level
    pub timers_per_level: Vec<usize>,

    /// Total ticks processed
    pub ticks_processed: u64,

    /// Total timers fired
    pub timers_fired: u64,

    /// Total cascades
    pub cascades: u64,
}

/// Hierarchical timer wheel for O(1) timer operations
pub struct TimerWheel {
    /// Configuration
    config: TimerWheelConfig,

    /// Wheel levels (level 0 is finest granularity)
    levels: Vec<WheelLevel>,

    /// Next timer ID
    next_id: AtomicU64,

    /// Start time of the wheel
    _start_time: Instant,

    /// Current tick count
    current_tick: u64,

    /// Metrics
    metrics: TimerWheelMetrics,
}

impl TimerWheel {
    /// Create a new timer wheel with the given configuration
    pub fn new(config: TimerWheelConfig) -> Self {
        let levels = (0..config.levels)
            .map(|_| WheelLevel::new(config.slots))
            .collect();

        Self {
            config,
            levels,
            next_id: AtomicU64::new(1),
            _start_time: Instant::now(),
            current_tick: 0,
            metrics: TimerWheelMetrics::default(),
        }
    }

    /// Create a timer wheel with default configuration
    pub fn with_defaults() -> Self {
        Self::new(TimerWheelConfig::default())
    }

    /// Add a timer that fires after the specified duration
    pub fn add_timer(&mut self, duration: Duration, mut entry: TimerEntry) -> TimerId {
        let id = TimerId(self.next_id.fetch_add(1, Ordering::Relaxed));
        entry.id = id;
        entry.fire_at = Instant::now() + duration;

        // Calculate which slot and level to insert into
        let ticks = self.duration_to_ticks(duration);
        entry.remaining_ticks = ticks;

        self.insert_timer(entry, ticks);
        self.metrics.record_add();

        id
    }

    /// Add a timer that fires at a specific instant
    pub fn add_timer_at(&mut self, fire_at: Instant, mut entry: TimerEntry) -> TimerId {
        let id = TimerId(self.next_id.fetch_add(1, Ordering::Relaxed));
        entry.id = id;
        entry.fire_at = fire_at;

        let duration = fire_at.saturating_duration_since(Instant::now());
        let ticks = self.duration_to_ticks(duration);
        entry.remaining_ticks = ticks;

        self.insert_timer(entry, ticks);
        self.metrics.record_add();

        id
    }

    /// Advance the wheel by one tick, returning expired timers
    pub fn tick(&mut self) -> Vec<TimerEntry> {
        self.current_tick += 1;
        self.metrics.record_tick();

        let mut expired = Vec::new();

        // Process level 0 (finest granularity)
        let mut current_timers = self.levels[0].take_current();

        for timer in current_timers.drain(..) {
            if timer.remaining_ticks <= 1 {
                // Timer has expired
                expired.push(timer);
                self.metrics.record_fire();
            } else {
                // Timer still has time remaining (shouldn't happen at level 0)
                // Re-insert with updated remaining ticks
                let mut timer = timer;
                timer.remaining_ticks -= 1;
                self.insert_timer(timer, 1);
            }
        }

        // Advance level 0
        let mut cascade = self.levels[0].advance();

        // Cascade through higher levels
        for level in 1..self.levels.len() {
            if !cascade {
                break;
            }

            // Take timers from current slot of this level
            let higher_timers = self.levels[level].take_current();

            // Move them down to level 0
            for mut timer in higher_timers {
                self.metrics.record_cascade();

                // Recalculate remaining ticks based on current time
                let remaining = timer.fire_at.saturating_duration_since(Instant::now());
                timer.remaining_ticks = self.duration_to_ticks(remaining);

                if timer.remaining_ticks == 0 {
                    expired.push(timer);
                    self.metrics.record_fire();
                } else {
                    self.insert_timer_at_level(timer, 0);
                }
            }

            cascade = self.levels[level].advance();
        }

        expired
    }

    /// Advance the wheel by the specified duration, returning all expired timers
    pub fn advance(&mut self, duration: Duration) -> Vec<TimerEntry> {
        let ticks = self.duration_to_ticks(duration);
        let mut all_expired = Vec::new();

        for _ in 0..ticks {
            let expired = self.tick();
            all_expired.extend(expired);
        }

        all_expired
    }

    /// Get the current tick count
    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    /// Get the number of pending timers
    pub fn pending_count(&self) -> usize {
        self.levels.iter().map(|l| l.timer_count()).sum()
    }

    /// Get statistics about the timer wheel
    pub fn stats(&self) -> TimerWheelStats {
        TimerWheelStats {
            pending_timers: self.pending_count(),
            timers_per_level: self.levels.iter().map(|l| l.timer_count()).collect(),
            ticks_processed: self.metrics.ticks_processed.load(Ordering::Relaxed),
            timers_fired: self.metrics.timers_fired.load(Ordering::Relaxed),
            cascades: self.metrics.cascades.load(Ordering::Relaxed),
        }
    }

    /// Get the metrics
    pub fn metrics(&self) -> &TimerWheelMetrics {
        &self.metrics
    }

    /// Get the configuration
    pub fn config(&self) -> &TimerWheelConfig {
        &self.config
    }

    /// Calculate time until the next timer fires (or None if no timers)
    pub fn time_until_next(&self) -> Option<Duration> {
        // Check each level starting from 0
        for (level_idx, level) in self.levels.iter().enumerate() {
            // Check slots starting from current
            for offset in 0..level.slots.len() {
                let slot_idx = (level.current() + offset) % level.slots.len();
                if !level.slots[slot_idx].is_empty() {
                    // Found a timer
                    let ticks = offset as u64 * self.config.slots.pow(level_idx as u32) as u64;
                    return Some(self.config.tick_duration * ticks as u32);
                }
            }
        }
        None
    }

    // ========== Internal Methods ==========

    /// Convert duration to ticks
    fn duration_to_ticks(&self, duration: Duration) -> u64 {
        let nanos = duration.as_nanos() as u64;
        let tick_nanos = self.config.tick_duration.as_nanos() as u64;
        nanos.div_ceil(tick_nanos) // Round up
    }

    /// Insert a timer into the appropriate level
    fn insert_timer(&mut self, entry: TimerEntry, ticks: u64) {
        if ticks == 0 {
            // Insert into slot 0 of level 0 (will fire on next tick)
            self.levels[0].add_to_slot(0, entry);
            return;
        }

        // Find the appropriate level
        for level in 0..self.levels.len() {
            let level_ticks = self.config.slots.pow(level as u32 + 1) as u64;

            if ticks < level_ticks {
                // This level can handle the timer
                let slot_size = self.config.slots.pow(level as u32) as u64;
                let slot_offset = (ticks / slot_size) as usize;
                let slot = (self.levels[level].current() + slot_offset) % self.config.slots;

                self.levels[level].add_to_slot(slot, entry);
                return;
            }
        }

        // Timer is beyond our max duration, put in last slot of highest level
        let last_level = self.levels.len() - 1;
        let last_slot =
            (self.levels[last_level].current() + self.config.slots - 1) % self.config.slots;
        self.levels[last_level].add_to_slot(last_slot, entry);
    }

    /// Insert a timer at a specific level
    fn insert_timer_at_level(&mut self, entry: TimerEntry, level: usize) {
        let ticks = entry.remaining_ticks;

        if level >= self.levels.len() {
            // Shouldn't happen, but handle gracefully
            let last_level = self.levels.len() - 1;
            self.levels[last_level].add_to_slot(0, entry);
            return;
        }

        let slot_size = self.config.slots.pow(level as u32) as u64;
        let slot_offset = (ticks / slot_size.max(1)) as usize;
        let slot = (self.levels[level].current() + slot_offset) % self.config.slots;

        self.levels[level].add_to_slot(slot, entry);
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer_id() {
        let id = TimerId::new(42);
        assert_eq!(id.value(), 42);
        assert_eq!(id.to_string(), "timer-42");
    }

    #[test]
    fn test_timer_entry_creation() {
        let entry = TimerEntry::new("wf-123", "timer-1", serde_json::json!({"key": "value"}));
        assert_eq!(entry.workflow_id, "wf-123");
        assert_eq!(entry.timer_name, "timer-1");
    }

    #[test]
    fn test_timer_wheel_creation() {
        let wheel = TimerWheel::with_defaults();
        assert_eq!(wheel.pending_count(), 0);
        assert_eq!(wheel.current_tick(), 0);
    }

    #[test]
    fn test_add_timer() {
        let mut wheel = TimerWheel::new(TimerWheelConfig {
            slots: 8,
            tick_duration: Duration::from_millis(10),
            levels: 2,
            max_duration: Duration::from_secs(60),
        });

        let entry = TimerEntry::new("wf-1", "t-1", serde_json::json!({}));
        let id = wheel.add_timer(Duration::from_millis(50), entry);

        assert_eq!(id.value(), 1);
        assert_eq!(wheel.pending_count(), 1);
    }

    #[test]
    fn test_timer_fires_after_ticks() {
        let mut wheel = TimerWheel::new(TimerWheelConfig {
            slots: 8,
            tick_duration: Duration::from_millis(10),
            levels: 2,
            max_duration: Duration::from_secs(60),
        });

        let entry = TimerEntry::new("wf-1", "t-1", serde_json::json!({}));
        wheel.add_timer(Duration::from_millis(30), entry);

        assert_eq!(wheel.pending_count(), 1);

        // Advance enough ticks to fire the timer (30ms = 3 ticks, plus some buffer)
        let mut total_expired = 0;
        for _ in 0..10 {
            let expired = wheel.tick();
            total_expired += expired.len();
        }

        // Timer should have fired
        assert!(
            total_expired >= 1,
            "Expected at least 1 timer to fire, got {}",
            total_expired
        );
        assert_eq!(wheel.pending_count(), 0, "Expected 0 pending timers");
    }

    #[test]
    fn test_multiple_timers() {
        let mut wheel = TimerWheel::new(TimerWheelConfig {
            slots: 16,
            tick_duration: Duration::from_millis(10),
            levels: 2,
            max_duration: Duration::from_secs(60),
        });

        // Add 5 timers at different intervals
        for i in 0..5 {
            let entry = TimerEntry::new(
                format!("wf-{}", i),
                format!("t-{}", i),
                serde_json::json!({}),
            );
            wheel.add_timer(Duration::from_millis((i + 1) as u64 * 20), entry);
        }

        assert_eq!(wheel.pending_count(), 5);

        // Advance and collect all expired
        let expired = wheel.advance(Duration::from_millis(200));
        assert_eq!(expired.len(), 5);
        assert_eq!(wheel.pending_count(), 0);
    }

    #[test]
    fn test_stats() {
        let mut wheel = TimerWheel::new(TimerWheelConfig {
            slots: 8,
            tick_duration: Duration::from_millis(10),
            levels: 3,
            max_duration: Duration::from_secs(60),
        });

        let entry = TimerEntry::new("wf-1", "t-1", serde_json::json!({}));
        wheel.add_timer(Duration::from_millis(50), entry);

        let stats = wheel.stats();
        assert_eq!(stats.pending_timers, 1);
        assert_eq!(stats.timers_per_level.len(), 3);
    }

    #[test]
    fn test_config_presets() {
        let ms_config = TimerWheelConfig::millisecond();
        assert_eq!(ms_config.tick_duration, Duration::from_millis(1));

        let sec_config = TimerWheelConfig::second();
        assert_eq!(sec_config.tick_duration, Duration::from_secs(1));
    }

    #[test]
    fn test_time_until_next() {
        let mut wheel = TimerWheel::new(TimerWheelConfig {
            slots: 8,
            tick_duration: Duration::from_millis(100),
            levels: 2,
            max_duration: Duration::from_secs(60),
        });

        // Empty wheel
        assert!(wheel.time_until_next().is_none());

        // Add a timer
        let entry = TimerEntry::new("wf-1", "t-1", serde_json::json!({}));
        wheel.add_timer(Duration::from_millis(500), entry);

        // Should have a time until next
        let time = wheel.time_until_next();
        assert!(time.is_some());
    }

    #[test]
    fn test_metrics() {
        let mut wheel = TimerWheel::new(TimerWheelConfig {
            slots: 8,
            tick_duration: Duration::from_millis(10),
            levels: 2,
            max_duration: Duration::from_secs(60),
        });

        let entry = TimerEntry::new("wf-1", "t-1", serde_json::json!({}));
        wheel.add_timer(Duration::from_millis(20), entry);

        assert_eq!(wheel.metrics().timers_added.load(Ordering::Relaxed), 1);

        // Advance until timer fires
        wheel.advance(Duration::from_millis(50));

        assert!(wheel.metrics().timers_fired.load(Ordering::Relaxed) >= 1);
        assert!(wheel.metrics().ticks_processed.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn test_long_duration_timer() {
        let mut wheel = TimerWheel::new(TimerWheelConfig {
            slots: 8,
            tick_duration: Duration::from_millis(100),
            levels: 3,
            max_duration: Duration::from_secs(60),
        });

        // Add a timer that should go to a higher level
        let entry = TimerEntry::new("wf-1", "t-1", serde_json::json!({}));
        wheel.add_timer(Duration::from_secs(5), entry);

        assert_eq!(wheel.pending_count(), 1);

        // Timer should be in a higher level
        let stats = wheel.stats();
        assert!(stats.timers_per_level.iter().any(|&c| c > 0));
    }
}
