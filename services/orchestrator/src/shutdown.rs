//! Graceful shutdown coordination

use tokio::sync::broadcast;
use tracing::info;

/// Coordinator for graceful shutdown across all service components
pub struct ShutdownCoordinator {
    tx: broadcast::Sender<()>,
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(16);
        Self { tx }
    }

    /// Subscribe to shutdown notifications
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.tx.subscribe()
    }

    /// Trigger graceful shutdown for all subscribers
    pub fn trigger(&self) {
        info!("Triggering graceful shutdown");
        let _ = self.tx.send(());
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Wait for shutdown signal (SIGTERM, SIGINT, or Ctrl+C)
pub async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = sigint.recv() => info!("Received SIGINT"),
        }
    }

    #[cfg(not(unix))]
    {
        use tokio::signal;

        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Received Ctrl+C");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_coordinator_creation() {
        let coordinator = ShutdownCoordinator::new();
        let _rx = coordinator.subscribe();
        // Should not panic
    }

    #[tokio::test]
    async fn test_shutdown_trigger() {
        let coordinator = ShutdownCoordinator::new();
        let mut rx = coordinator.subscribe();

        coordinator.trigger();

        // Should receive shutdown signal
        let result = rx.recv().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let coordinator = ShutdownCoordinator::new();
        let mut rx1 = coordinator.subscribe();
        let mut rx2 = coordinator.subscribe();

        coordinator.trigger();

        // Both subscribers should receive the signal
        assert!(rx1.recv().await.is_ok());
        assert!(rx2.recv().await.is_ok());
    }

    #[test]
    fn test_default_impl() {
        let coordinator = ShutdownCoordinator::default();
        let _rx = coordinator.subscribe();
        // Should not panic
    }
}
