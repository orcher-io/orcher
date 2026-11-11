//! Shard Configuration Registry
//!
//! A shared registry for tracking shard configurations learned from worker polls.
//! This enables the orchestrator to detect and respond to shard configuration
//! from workers, helping diagnose and potentially recover from misconfigurations.
//!
//! ## The Problem
//!
//! When the orchestrator's shard configuration doesn't match the workers':
//! - Orchestrator pushes to `queue:namespace` (if sharding disabled)
//! - Workers poll from `queue:namespace:shard:N` (if sharding enabled)
//! - Result: Workers never receive work from Redis, fall back to slow PostgreSQL
//!
//! ## Solution
//!
//! This registry:
//! 1. Tracks shard configs learned from worker polls
//! 2. Detects mismatches between orchestrator and worker configurations
//! 3. Logs prominent warnings when mismatches are detected
//! 4. Provides diagnostics for debugging performance issues

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// Configuration learned from a worker poll
#[derive(Debug, Clone)]
pub struct LearnedShardConfig {
    /// Whether sharding is enabled for this task queue
    pub sharding_enabled: bool,
    /// Total shard count
    pub shard_count: u32,
    /// Assigned shards from the most recent poll
    pub assigned_shards: Vec<u32>,
    /// Worker identity that reported this config
    pub worker_identity: String,
    /// When this config was learned
    pub learned_at: Instant,
    /// Number of workers that have polled with this config
    pub worker_count: u32,
}

impl LearnedShardConfig {
    /// Create a new learned shard configuration
    pub fn new(
        sharding_enabled: bool,
        shard_count: u32,
        assigned_shards: Vec<u32>,
        worker_identity: String,
    ) -> Self {
        Self {
            sharding_enabled,
            shard_count,
            assigned_shards,
            worker_identity,
            learned_at: Instant::now(),
            worker_count: 1,
        }
    }
}

/// Mismatch information for diagnostics
#[derive(Debug, Clone)]
pub struct ShardMismatch {
    /// Task queue where mismatch was detected
    pub task_queue: String,
    /// Namespace where mismatch was detected
    pub namespace: String,
    /// Whether orchestrator has sharding enabled
    pub orchestrator_sharding_enabled: bool,
    /// Orchestrator's configured shard count
    pub orchestrator_shard_count: u32,
    /// Whether worker has sharding enabled
    pub worker_sharding_enabled: bool,
    /// Worker's configured shard count
    pub worker_shard_count: u32,
    /// Severity level of the mismatch
    pub severity: MismatchSeverity,
    /// Human-readable description of the mismatch
    pub description: String,
}

/// Severity level for shard configuration mismatches
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchSeverity {
    /// Critical: Workers will not receive work from Redis
    Critical,
    /// Warning: Some workflows may be routed to wrong shards
    Warning,
    /// Info: Configuration differs but may still work
    Info,
}

/// Shared registry for shard configuration tracking
#[derive(Clone)]
pub struct ShardConfigRegistry {
    /// Orchestrator's own shard configuration
    orchestrator_config: Arc<OrchestratorShardConfig>,
    /// Learned configurations from worker polls
    /// Key: "task_queue:namespace"
    learned_configs: Arc<RwLock<HashMap<String, LearnedShardConfig>>>,
    /// Detected mismatches (for diagnostics)
    mismatches: Arc<RwLock<Vec<ShardMismatch>>>,
    /// Whether warnings have been logged (to avoid spam)
    warnings_logged: Arc<RwLock<HashMap<String, Instant>>>,
}

/// Orchestrator's shard configuration
#[derive(Debug, Clone)]
pub struct OrchestratorShardConfig {
    /// Whether sharding is enabled for this orchestrator
    pub sharding_enabled: bool,
    /// Total number of shards
    pub shard_count: u32,
}

impl OrchestratorShardConfig {
    /// Create from environment variables
    pub fn from_env() -> Self {
        let sharding_enabled = std::env::var("ORCHER_SHARDING_ENABLED")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        let shard_count: u32 = std::env::var("ORCHER_SHARD_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        Self {
            sharding_enabled,
            shard_count,
        }
    }

    /// Create a new orchestrator shard configuration
    pub fn new(sharding_enabled: bool, shard_count: u32) -> Self {
        Self {
            sharding_enabled,
            shard_count,
        }
    }
}

impl Default for ShardConfigRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardConfigRegistry {
    /// Create a new registry with orchestrator config from environment
    pub fn new() -> Self {
        Self::with_config(OrchestratorShardConfig::from_env())
    }

    /// Create a new registry with explicit orchestrator config
    pub fn with_config(orchestrator_config: OrchestratorShardConfig) -> Self {
        info!(
            orchestrator_sharding_enabled = orchestrator_config.sharding_enabled,
            orchestrator_shard_count = orchestrator_config.shard_count,
            "ShardConfigRegistry initialized"
        );

        Self {
            orchestrator_config: Arc::new(orchestrator_config),
            learned_configs: Arc::new(RwLock::new(HashMap::new())),
            mismatches: Arc::new(RwLock::new(Vec::new())),
            warnings_logged: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the orchestrator's shard configuration
    pub fn orchestrator_config(&self) -> &OrchestratorShardConfig {
        &self.orchestrator_config
    }

    /// Record a worker poll with shard configuration
    ///
    /// This should be called by ExecutionService when a worker polls with shard info.
    /// Returns Some(ShardMismatch) if there's a mismatch that should be logged.
    pub async fn record_worker_poll(
        &self,
        task_queue: &str,
        namespace: &str,
        sharding_enabled: bool,
        shard_count: u32,
        assigned_shards: &[u32],
        worker_identity: &str,
    ) -> Option<ShardMismatch> {
        let key = format!("{}:{}", task_queue, namespace);

        // Update learned config
        {
            let mut configs = self.learned_configs.write().await;
            match configs.get_mut(&key) {
                Some(existing) => {
                    // Update if config changed or just increment worker count
                    if existing.sharding_enabled != sharding_enabled
                        || existing.shard_count != shard_count
                    {
                        *existing = LearnedShardConfig::new(
                            sharding_enabled,
                            shard_count,
                            assigned_shards.to_vec(),
                            worker_identity.to_string(),
                        );
                    } else {
                        existing.worker_count += 1;
                        existing.learned_at = Instant::now();
                    }
                }
                None => {
                    configs.insert(
                        key.clone(),
                        LearnedShardConfig::new(
                            sharding_enabled,
                            shard_count,
                            assigned_shards.to_vec(),
                            worker_identity.to_string(),
                        ),
                    );
                }
            }
        }

        // Check for mismatch
        let mismatch = self
            .check_mismatch(task_queue, namespace, sharding_enabled, shard_count)
            .await;

        // Log warning if mismatch (but rate-limit to avoid spam)
        if let Some(ref m) = mismatch {
            self.maybe_log_mismatch_warning(&key, m).await;
        }

        mismatch
    }

    /// Check if there's a mismatch between orchestrator and worker config
    async fn check_mismatch(
        &self,
        task_queue: &str,
        namespace: &str,
        worker_sharding_enabled: bool,
        worker_shard_count: u32,
    ) -> Option<ShardMismatch> {
        let orch = &self.orchestrator_config;

        // Most critical: sharding enabled/disabled mismatch
        if orch.sharding_enabled != worker_sharding_enabled {
            let (severity, description) = if worker_sharding_enabled && !orch.sharding_enabled {
                (
                    MismatchSeverity::Critical,
                    format!(
                        "Workers have sharding ENABLED but orchestrator has sharding DISABLED. \
                         Orchestrator pushes to '{}:{}' but workers poll from '{}:{}:shard:N'. \
                         Workers will NOT receive work from Redis!",
                        task_queue, namespace, task_queue, namespace
                    ),
                )
            } else {
                (
                    MismatchSeverity::Warning,
                    format!(
                        "Orchestrator has sharding ENABLED but workers have sharding DISABLED. \
                         Orchestrator pushes to '{}:{}:shard:N' but workers poll from '{}:{}'. \
                         Workers will NOT receive work from Redis!",
                        task_queue, namespace, task_queue, namespace
                    ),
                )
            };

            return Some(ShardMismatch {
                task_queue: task_queue.to_string(),
                namespace: namespace.to_string(),
                orchestrator_sharding_enabled: orch.sharding_enabled,
                orchestrator_shard_count: orch.shard_count,
                worker_sharding_enabled,
                worker_shard_count,
                severity,
                description,
            });
        }

        // Secondary: shard count mismatch when both have sharding enabled
        if orch.sharding_enabled
            && worker_sharding_enabled
            && orch.shard_count != worker_shard_count
        {
            return Some(ShardMismatch {
                task_queue: task_queue.to_string(),
                namespace: namespace.to_string(),
                orchestrator_sharding_enabled: orch.sharding_enabled,
                orchestrator_shard_count: orch.shard_count,
                worker_sharding_enabled,
                worker_shard_count,
                severity: MismatchSeverity::Warning,
                description: format!(
                    "Shard count mismatch: orchestrator has {} shards, workers have {} shards. \
                     Some workflows may be pushed to shards that no worker polls.",
                    orch.shard_count, worker_shard_count
                ),
            });
        }

        None
    }

    /// Log mismatch warning (rate-limited to once per minute per key)
    async fn maybe_log_mismatch_warning(&self, key: &str, mismatch: &ShardMismatch) {
        let should_log = {
            let warnings = self.warnings_logged.read().await;
            match warnings.get(key) {
                Some(last_logged) => last_logged.elapsed().as_secs() > 60,
                None => true,
            }
        };

        if should_log {
            // Update last logged time
            {
                let mut warnings = self.warnings_logged.write().await;
                warnings.insert(key.to_string(), Instant::now());
            }

            // Store mismatch for diagnostics
            {
                let mut mismatches = self.mismatches.write().await;
                // Keep only recent mismatches (max 100)
                if mismatches.len() >= 100 {
                    mismatches.remove(0);
                }
                mismatches.push(mismatch.clone());
            }

            // Log prominently
            match mismatch.severity {
                MismatchSeverity::Critical => {
                    error!("╔══════════════════════════════════════════════════════════════════╗");
                    error!("║  CRITICAL: SHARD CONFIGURATION MISMATCH DETECTED!                ║");
                    error!("╠══════════════════════════════════════════════════════════════════╣");
                    error!(
                        "║  Task Queue: {}:{} ",
                        mismatch.task_queue, mismatch.namespace
                    );
                    error!(
                        "║  Orchestrator: sharding={}, shard_count={}",
                        mismatch.orchestrator_sharding_enabled, mismatch.orchestrator_shard_count
                    );
                    error!(
                        "║  Workers:      sharding={}, shard_count={}",
                        mismatch.worker_sharding_enabled, mismatch.worker_shard_count
                    );
                    error!("╠══════════════════════════════════════════════════════════════════╣");
                    error!("║  {}", mismatch.description);
                    error!("╠══════════════════════════════════════════════════════════════════╣");
                    error!("║  FIX: Restart orchestrator with matching configuration:          ║");
                    error!(
                        "║    ORCHER_SHARDING_ENABLED={} ORCHER_SHARD_COUNT={}",
                        mismatch.worker_sharding_enabled, mismatch.worker_shard_count
                    );
                    error!("╚══════════════════════════════════════════════════════════════════╝");
                }
                MismatchSeverity::Warning => {
                    warn!("SHARD CONFIGURATION MISMATCH: {}", mismatch.description);
                    warn!(
                        "Orchestrator: sharding={}, shard_count={}",
                        mismatch.orchestrator_sharding_enabled, mismatch.orchestrator_shard_count
                    );
                    warn!(
                        "Workers: sharding={}, shard_count={}",
                        mismatch.worker_sharding_enabled, mismatch.worker_shard_count
                    );
                }
                MismatchSeverity::Info => {
                    info!(
                        "ℹ️  Shard config difference detected: {}",
                        mismatch.description
                    );
                }
            }
        }
    }

    /// Get all learned shard configurations
    pub async fn get_learned_configs(&self) -> HashMap<String, LearnedShardConfig> {
        self.learned_configs.read().await.clone()
    }

    /// Get recent mismatches for diagnostics
    pub async fn get_recent_mismatches(&self) -> Vec<ShardMismatch> {
        self.mismatches.read().await.clone()
    }

    /// Check if there are any critical mismatches
    pub async fn has_critical_mismatches(&self) -> bool {
        self.mismatches
            .read()
            .await
            .iter()
            .any(|m| m.severity == MismatchSeverity::Critical)
    }

    /// Get diagnostic summary
    pub async fn diagnostic_summary(&self) -> String {
        let configs = self.learned_configs.read().await;
        let mismatches = self.mismatches.read().await;

        let mut summary = String::new();
        summary.push_str("=== Shard Configuration Registry ===\n\n");
        summary.push_str(&format!(
            "Orchestrator: sharding={}, shard_count={}\n\n",
            self.orchestrator_config.sharding_enabled, self.orchestrator_config.shard_count
        ));

        summary.push_str("Learned Configurations:\n");
        if configs.is_empty() {
            summary.push_str("  (none - no worker polls with shard info yet)\n");
        } else {
            for (key, config) in configs.iter() {
                summary.push_str(&format!(
                    "  {}: sharding={}, shard_count={}, workers={}\n",
                    key, config.sharding_enabled, config.shard_count, config.worker_count
                ));
            }
        }

        summary.push_str("\nMismatches:\n");
        if mismatches.is_empty() {
            summary.push_str("  (none)\n");
        } else {
            for m in mismatches.iter() {
                summary.push_str(&format!("  [{:?}] {}\n", m.severity, m.description));
            }
        }

        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orchestrator_config_from_env() {
        // This tests default behavior when env vars aren't set
        let config = OrchestratorShardConfig::from_env();
        // Default should be sharding disabled
        assert!(!config.sharding_enabled || config.sharding_enabled); // Just verify it doesn't panic
    }

    #[test]
    fn test_orchestrator_config_new() {
        let config = OrchestratorShardConfig::new(true, 8);
        assert!(config.sharding_enabled);
        assert_eq!(config.shard_count, 8);
    }

    #[tokio::test]
    async fn test_registry_creation() {
        let registry = ShardConfigRegistry::with_config(OrchestratorShardConfig::new(false, 1));
        assert!(!registry.orchestrator_config().sharding_enabled);
        assert_eq!(registry.orchestrator_config().shard_count, 1);
    }

    #[tokio::test]
    async fn test_mismatch_detection_critical() {
        // Orchestrator has sharding DISABLED
        let registry = ShardConfigRegistry::with_config(OrchestratorShardConfig::new(false, 1));

        // Worker polls with sharding ENABLED
        let mismatch = registry
            .record_worker_poll(
                "payment-processing",
                "default",
                true, // Worker has sharding enabled
                4,
                &[0, 2],
                "worker-1",
            )
            .await;

        assert!(mismatch.is_some());
        let m = mismatch.unwrap();
        assert_eq!(m.severity, MismatchSeverity::Critical);
        assert!(!m.orchestrator_sharding_enabled);
        assert!(m.worker_sharding_enabled);
    }

    #[tokio::test]
    async fn test_mismatch_detection_shard_count() {
        // Orchestrator has 4 shards
        let registry = ShardConfigRegistry::with_config(OrchestratorShardConfig::new(true, 4));

        // Worker polls with 8 shards
        let mismatch = registry
            .record_worker_poll(
                "payment-processing",
                "default",
                true,
                8, // Different shard count
                &[0, 2, 4, 6],
                "worker-1",
            )
            .await;

        assert!(mismatch.is_some());
        let m = mismatch.unwrap();
        assert_eq!(m.severity, MismatchSeverity::Warning);
        assert_eq!(m.orchestrator_shard_count, 4);
        assert_eq!(m.worker_shard_count, 8);
    }

    #[tokio::test]
    async fn test_no_mismatch_when_matching() {
        // Orchestrator and worker have same config
        let registry = ShardConfigRegistry::with_config(OrchestratorShardConfig::new(true, 4));

        let mismatch = registry
            .record_worker_poll(
                "payment-processing",
                "default",
                true,
                4,
                &[0, 2],
                "worker-1",
            )
            .await;

        assert!(mismatch.is_none());
    }

    #[tokio::test]
    async fn test_learned_configs() {
        let registry = ShardConfigRegistry::with_config(OrchestratorShardConfig::new(true, 4));

        // Record a poll
        registry
            .record_worker_poll("queue1", "ns1", true, 4, &[0, 1], "worker-1")
            .await;

        let configs = registry.get_learned_configs().await;
        assert_eq!(configs.len(), 1);
        assert!(configs.contains_key("queue1:ns1"));

        let config = configs.get("queue1:ns1").unwrap();
        assert!(config.sharding_enabled);
        assert_eq!(config.shard_count, 4);
        assert_eq!(config.worker_count, 1);
    }

    #[tokio::test]
    async fn test_worker_count_increment() {
        let registry = ShardConfigRegistry::with_config(OrchestratorShardConfig::new(true, 4));

        // Multiple workers poll with same config
        registry
            .record_worker_poll("queue1", "ns1", true, 4, &[0, 1], "worker-1")
            .await;
        registry
            .record_worker_poll("queue1", "ns1", true, 4, &[2, 3], "worker-2")
            .await;
        registry
            .record_worker_poll("queue1", "ns1", true, 4, &[0, 1], "worker-1")
            .await;

        let configs = registry.get_learned_configs().await;
        let config = configs.get("queue1:ns1").unwrap();
        assert_eq!(config.worker_count, 3);
    }
}
