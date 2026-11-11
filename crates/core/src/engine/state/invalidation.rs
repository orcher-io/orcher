//! Cache invalidation on state changes.

use crate::engine::state::WorkflowStateCache;
use orcher_common::error::{OrcherError, OrcherResult};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgListener, PgPool};
use std::sync::Arc;

use orcher_common::types::time::Timestamptz;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ============================================================================
// Invalidation Message Types
// ============================================================================

/// Type of invalidation message
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationType {
    /// Invalidate a single workflow
    Single,
    /// Invalidate all workflows in a namespace
    Namespace,
    /// Invalidate all workflows (full cache clear)
    All,
}

/// Cache invalidation message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationMessage {
    /// Type of invalidation
    pub invalidation_type: InvalidationType,

    /// Workflow ID (for Single invalidation)
    pub workflow_id: Option<Uuid>,

    /// Namespace (for Namespace invalidation)
    pub namespace: Option<String>,

    /// Coordinator ID that sent the invalidation
    pub coordinator_id: String,

    /// Timestamp when invalidation was sent
    pub timestamp: Timestamptz,
}

impl InvalidationMessage {
    /// Create a single workflow invalidation message
    pub fn single(workflow_id: Uuid, coordinator_id: String) -> Self {
        Self {
            invalidation_type: InvalidationType::Single,
            workflow_id: Some(workflow_id),
            namespace: None,
            coordinator_id,
            timestamp: Timestamptz::now(),
        }
    }

    /// Create a namespace invalidation message
    pub fn namespace(namespace: String, coordinator_id: String) -> Self {
        Self {
            invalidation_type: InvalidationType::Namespace,
            workflow_id: None,
            namespace: Some(namespace),
            coordinator_id,
            timestamp: Timestamptz::now(),
        }
    }

    /// Create a full cache clear message
    pub fn all(coordinator_id: String) -> Self {
        Self {
            invalidation_type: InvalidationType::All,
            workflow_id: None,
            namespace: None,
            coordinator_id,
            timestamp: Timestamptz::now(),
        }
    }

    /// Serialize to JSON string
    pub fn to_json(&self) -> OrcherResult<String> {
        serde_json::to_string(self).map_err(|e| OrcherError::Internal {
            message: format!("Failed to serialize invalidation message: {}", e),
            source: None,
        })
    }

    /// Deserialize from JSON string
    pub fn from_json(json: &str) -> OrcherResult<Self> {
        serde_json::from_str(json).map_err(|e| OrcherError::Internal {
            message: format!("Failed to deserialize invalidation message: {}", e),
            source: None,
        })
    }
}

// ============================================================================
// Cache Invalidator Configuration
// ============================================================================

/// Configuration for cache invalidation
#[derive(Debug, Clone)]
pub struct InvalidatorConfig {
    /// PostgreSQL notification channel name
    pub channel_name: String,

    /// This coordinator's unique ID
    pub coordinator_id: String,

    /// Buffer size for invalidation message queue
    pub message_buffer_size: usize,

    /// Enable metrics tracking
    pub enable_metrics: bool,
}

impl Default for InvalidatorConfig {
    fn default() -> Self {
        Self {
            channel_name: "orcher_cache_invalidation".to_string(),
            coordinator_id: format!("coordinator-{}", Uuid::new_v4()),
            message_buffer_size: 1000,
            enable_metrics: true,
        }
    }
}

// ============================================================================
// Invalidation Metrics
// ============================================================================

/// Metrics for cache invalidation
#[derive(Debug, Clone, Default)]
pub struct InvalidationMetrics {
    /// Total invalidations sent
    pub sent: u64,

    /// Total invalidations received
    pub received: u64,

    /// Total invalidations processed
    pub processed: u64,

    /// Total invalidation errors
    pub errors: u64,

    /// Messages ignored (from self)
    pub ignored_self: u64,
}

// ============================================================================
// Cache Invalidator
// ============================================================================

/// Cache invalidator for cross-coordinator coordination.
///
/// Invalidations ride a single Postgres `LISTEN`/`NOTIFY` channel on `pool`. With
/// partitioned persistence (multiple shard databases), `pool` is deliberately the
/// **coordination database** (the primary / shard-pool 0) — which every node in the
/// cluster shares (it also holds the cluster/routing tables and the distributed
/// lock). Because all nodes both publish and listen on that one shared channel, an
/// invalidation reaches every node regardless of which shard database the workflow's
/// state actually lives in. This is the "coordination-DB broadcast" model: one
/// channel, correct across shards, at the cost of one small `NOTIFY` write on the
/// coordination DB per invalidation (workflows are also claim-serialized to one node
/// and reconstruct on claim, so cross-node staleness is already bounded). Publishing
/// on each workflow's shard pool with a listener per pool would remove that single
/// write but buys no correctness here, so it is intentionally not done.
pub struct CacheInvalidator {
    pool: Arc<PgPool>,
    config: InvalidatorConfig,
    cache: Arc<WorkflowStateCache>,
    metrics: Arc<tokio::sync::RwLock<InvalidationMetrics>>,
}

impl CacheInvalidator {
    /// Create a new cache invalidator
    pub fn new(
        pool: Arc<PgPool>,
        config: InvalidatorConfig,
        cache: Arc<WorkflowStateCache>,
    ) -> Self {
        Self {
            pool,
            config,
            cache,
            metrics: Arc::new(tokio::sync::RwLock::new(InvalidationMetrics::default())),
        }
    }

    /// Invalidate a single workflow across all coordinators
    pub async fn invalidate_workflow(&self, workflow_id: Uuid) -> OrcherResult<()> {
        let message = InvalidationMessage::single(workflow_id, self.config.coordinator_id.clone());
        self.broadcast(message).await
    }

    /// Invalidate all workflows in a namespace
    pub async fn invalidate_namespace(&self, namespace: String) -> OrcherResult<()> {
        let message = InvalidationMessage::namespace(namespace, self.config.coordinator_id.clone());
        self.broadcast(message).await
    }

    /// Invalidate all cached workflows
    pub async fn invalidate_all(&self) -> OrcherResult<()> {
        let message = InvalidationMessage::all(self.config.coordinator_id.clone());
        self.broadcast(message).await
    }

    /// Broadcast an invalidation message
    async fn broadcast(&self, message: InvalidationMessage) -> OrcherResult<()> {
        let payload = message.to_json()?;

        debug!(
            invalidation_type = ?message.invalidation_type,
            workflow_id = ?message.workflow_id,
            namespace = ?message.namespace,
            "Broadcasting invalidation"
        );

        // Send notification via PostgreSQL
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&self.config.channel_name)
            .bind(&payload)
            .execute(&*self.pool)
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to send invalidation notification: {}", e),
                source: None,
            })?;

        if self.config.enable_metrics {
            self.metrics.write().await.sent += 1;
        }

        Ok(())
    }

    /// Start listening for invalidation messages
    ///
    /// This spawns a background task that listens for invalidation messages
    /// and processes them by invalidating the local cache.
    pub async fn start_listener(self: Arc<Self>) -> OrcherResult<()> {
        info!(
            channel = %self.config.channel_name,
            coordinator_id = %self.config.coordinator_id,
            "Starting cache invalidation listener"
        );

        // Create PostgreSQL listener
        let mut listener =
            PgListener::connect_with(&self.pool)
                .await
                .map_err(|e| OrcherError::Internal {
                    message: format!("Failed to create PostgreSQL listener: {}", e),
                    source: None,
                })?;

        // Subscribe to the invalidation channel
        listener
            .listen(&self.config.channel_name)
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to listen on channel: {}", e),
                source: None,
            })?;

        info!("Cache invalidation listener started");

        // Spawn background task to process notifications
        let invalidator = self.clone();
        tokio::spawn(async move {
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        if let Err(e) = invalidator
                            .process_notification(notification.payload())
                            .await
                        {
                            error!(
                                error = %e,
                                "Failed to process invalidation notification"
                            );

                            if invalidator.config.enable_metrics {
                                invalidator.metrics.write().await.errors += 1;
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            error = %e,
                            "Error receiving notification, reconnecting..."
                        );

                        // The listener will automatically try to reconnect
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Process a received invalidation notification
    async fn process_notification(&self, payload: &str) -> OrcherResult<()> {
        // Parse the message
        let message = InvalidationMessage::from_json(payload)?;

        if self.config.enable_metrics {
            self.metrics.write().await.received += 1;
        }

        // Ignore messages from ourselves
        if message.coordinator_id == self.config.coordinator_id {
            debug!("Ignoring invalidation from self");
            if self.config.enable_metrics {
                self.metrics.write().await.ignored_self += 1;
            }
            return Ok(());
        }

        debug!(
            invalidation_type = ?message.invalidation_type,
            workflow_id = ?message.workflow_id,
            namespace = ?message.namespace,
            coordinator_id = %message.coordinator_id,
            "Processing invalidation"
        );

        // Process based on invalidation type
        match message.invalidation_type {
            InvalidationType::Single => {
                if let Some(workflow_id) = message.workflow_id {
                    self.cache.invalidate(&workflow_id).await?;
                    debug!(workflow_id = %workflow_id, "Invalidated single workflow");
                } else {
                    warn!("Single invalidation message missing workflow_id");
                }
            }
            InvalidationType::Namespace => {
                if let Some(_namespace) = message.namespace {
                    // Note: Current cache doesn't support namespace-level invalidation
                    // For now, we'd need to track namespace per entry or clear all
                    warn!("Namespace invalidation not yet supported, clearing all");
                    self.cache.clear().await?;
                } else {
                    warn!("Namespace invalidation message missing namespace");
                }
            }
            InvalidationType::All => {
                self.cache.clear().await?;
                debug!("Cleared entire cache");
            }
        }

        if self.config.enable_metrics {
            self.metrics.write().await.processed += 1;
        }

        Ok(())
    }

    /// Get invalidation metrics
    pub async fn get_metrics(&self) -> InvalidationMetrics {
        self.metrics.read().await.clone()
    }

    /// Reset metrics
    pub async fn reset_metrics(&self) {
        *self.metrics.write().await = InvalidationMetrics::default();
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalidation_message_single() {
        let workflow_id = Uuid::new_v4();
        let message = InvalidationMessage::single(workflow_id, "coord-1".to_string());

        assert_eq!(message.invalidation_type, InvalidationType::Single);
        assert_eq!(message.workflow_id, Some(workflow_id));
        assert_eq!(message.namespace, None);
        assert_eq!(message.coordinator_id, "coord-1");
    }

    #[test]
    fn test_invalidation_message_namespace() {
        let message =
            InvalidationMessage::namespace("production".to_string(), "coord-1".to_string());

        assert_eq!(message.invalidation_type, InvalidationType::Namespace);
        assert_eq!(message.workflow_id, None);
        assert_eq!(message.namespace, Some("production".to_string()));
        assert_eq!(message.coordinator_id, "coord-1");
    }

    #[test]
    fn test_invalidation_message_all() {
        let message = InvalidationMessage::all("coord-1".to_string());

        assert_eq!(message.invalidation_type, InvalidationType::All);
        assert_eq!(message.workflow_id, None);
        assert_eq!(message.namespace, None);
        assert_eq!(message.coordinator_id, "coord-1");
    }

    #[test]
    fn test_message_serialization() {
        let workflow_id = Uuid::new_v4();
        let message = InvalidationMessage::single(workflow_id, "coord-1".to_string());

        // Serialize
        let json = message.to_json().unwrap();
        assert!(json.contains("coord-1"));

        // Deserialize
        let deserialized = InvalidationMessage::from_json(&json).unwrap();
        assert_eq!(deserialized.invalidation_type, message.invalidation_type);
        assert_eq!(deserialized.workflow_id, message.workflow_id);
        assert_eq!(deserialized.coordinator_id, message.coordinator_id);
    }

    #[test]
    fn test_config_default() {
        let config = InvalidatorConfig::default();

        assert_eq!(config.channel_name, "orcher_cache_invalidation");
        assert!(config.coordinator_id.starts_with("coordinator-"));
        assert_eq!(config.message_buffer_size, 1000);
        assert!(config.enable_metrics);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_invalidator_broadcast() {
        // Test requires real PostgreSQL connection
        // Implementation would test actual broadcast
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_invalidator_listener() {
        // Test requires real PostgreSQL connection
        // Implementation would test listener receiving messages
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_cross_coordinator_invalidation() {
        // Test requires real PostgreSQL connection
        // Implementation would test two coordinators invalidating each other's caches
    }
}
