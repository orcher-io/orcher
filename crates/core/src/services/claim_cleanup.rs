//! Stale workflow claim cleanup service.

use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info};

/// Configuration for claim cleanup service
#[derive(Debug, Clone)]
pub struct ClaimCleanupConfig {
    /// Interval between cleanup runs (in seconds)
    pub cleanup_interval_secs: u64,

    /// Whether cleanup is enabled
    pub enabled: bool,
}

impl Default for ClaimCleanupConfig {
    fn default() -> Self {
        Self {
            cleanup_interval_secs: 30,
            enabled: true,
        }
    }
}

/// Background service for cleaning up stale claims
///
/// Periodically scans for workflow executions with expired claims
/// and releases them so other services can pick up the work.
#[derive(Debug)]
pub struct ClaimCleanupService {
    /// Repository for workflow executions
    repository: Arc<WorkflowExecutionRepository>,

    /// Service configuration
    config: ClaimCleanupConfig,

    /// Shutdown signal receiver
    shutdown_rx: watch::Receiver<bool>,

    /// Shutdown signal sender (kept for cloning)
    shutdown_tx: watch::Sender<bool>,
}

impl ClaimCleanupService {
    /// Create a new claim cleanup service
    ///
    /// # Arguments
    ///
    /// * `repository` - Workflow execution repository
    /// * `config` - Service configuration
    pub fn new(repository: Arc<WorkflowExecutionRepository>, config: ClaimCleanupConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Self {
            repository,
            config,
            shutdown_rx,
            shutdown_tx,
        }
    }

    /// Create with default configuration
    pub fn with_defaults(repository: Arc<WorkflowExecutionRepository>) -> Self {
        Self::new(repository, ClaimCleanupConfig::default())
    }

    /// Create with custom cleanup interval
    pub fn with_interval(
        repository: Arc<WorkflowExecutionRepository>,
        cleanup_interval_secs: u64,
    ) -> Self {
        Self::new(
            repository,
            ClaimCleanupConfig {
                cleanup_interval_secs,
                enabled: true,
            },
        )
    }

    /// Get a shutdown signal sender
    ///
    /// Use this to signal the background task to stop
    pub fn shutdown_handle(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    /// Start the background cleanup task
    ///
    /// Returns a shutdown sender that can be used to stop the task.
    ///
    /// # Returns
    ///
    /// Shutdown signal sender
    pub fn start(self) -> watch::Sender<bool> {
        if !self.config.enabled {
            info!("Claim cleanup service is disabled");
            return self.shutdown_tx;
        }

        info!(
            cleanup_interval_secs = self.config.cleanup_interval_secs,
            "Starting claim cleanup service"
        );

        let shutdown_tx = self.shutdown_tx.clone();

        // Spawn background task
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                error!(error = %e, "Claim cleanup service failed");
            }
        });

        shutdown_tx
    }

    /// Run the cleanup loop
    ///
    /// This is the main background task that runs until shutdown.
    async fn run(mut self) -> OrcherResult<()> {
        let mut interval =
            tokio::time::interval(Duration::from_secs(self.config.cleanup_interval_secs));

        // Skip first tick (happens immediately)
        interval.tick().await;

        let mut iteration = 0u64;

        loop {
            tokio::select! {
                // Shutdown signal received
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        info!(
                            iterations = iteration,
                            "Claim cleanup service shutting down"
                        );
                        break;
                    }
                }

                // Periodic cleanup tick
                _ = interval.tick() => {
                    iteration += 1;

                    debug!(
                        iteration = iteration,
                        "Running claim cleanup"
                    );

                    match self.cleanup_stale_claims().await {
                        Ok(count) => {
                            if count > 0 {
                                info!(
                                    iteration = iteration,
                                    stale_claims_released = count,
                                    "Released stale claims"
                                );
                            } else {
                                debug!(
                                    iteration = iteration,
                                    "No stale claims found"
                                );
                            }
                        }
                        Err(e) => {
                            error!(
                                iteration = iteration,
                                error = %e,
                                "Failed to release stale claims"
                            );
                        }
                    }
                }
            }
        }

        info!("Claim cleanup service stopped");
        Ok(())
    }

    /// Perform stale claim cleanup
    ///
    /// Calls repository method to release stale claims and returns count.
    async fn cleanup_stale_claims(&self) -> OrcherResult<u64> {
        self.repository.release_stale_claims().await
    }
}

// ================================================================================================
// BUILDER PATTERN
// ================================================================================================

/// Builder for ClaimCleanupService
pub struct ClaimCleanupServiceBuilder {
    repository: Option<Arc<WorkflowExecutionRepository>>,
    cleanup_interval_secs: u64,
    enabled: bool,
}

impl ClaimCleanupServiceBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            repository: None,
            cleanup_interval_secs: 30,
            enabled: true,
        }
    }

    /// Set the workflow execution repository
    pub fn repository(mut self, repository: Arc<WorkflowExecutionRepository>) -> Self {
        self.repository = Some(repository);
        self
    }

    /// Set the cleanup interval
    pub fn interval_secs(mut self, secs: u64) -> Self {
        self.cleanup_interval_secs = secs;
        self
    }

    /// Enable or disable the cleanup service
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Build the service
    pub fn build(self) -> Result<ClaimCleanupService, &'static str> {
        let repository = self.repository.ok_or("repository is required")?;

        let config = ClaimCleanupConfig {
            cleanup_interval_secs: self.cleanup_interval_secs,
            enabled: self.enabled,
        };

        Ok(ClaimCleanupService::new(repository, config))
    }
}

impl Default for ClaimCleanupServiceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = ClaimCleanupConfig::default();
        assert_eq!(config.cleanup_interval_secs, 30);
        assert!(config.enabled);
    }

    #[test]
    fn test_builder() {
        let builder = ClaimCleanupServiceBuilder::new()
            .interval_secs(60)
            .enabled(true);

        assert_eq!(builder.cleanup_interval_secs, 60);
        assert!(builder.enabled);
    }

    #[test]
    fn test_builder_requires_repository() {
        let builder = ClaimCleanupServiceBuilder::new();
        let result = builder.build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "repository is required");
    }
}
