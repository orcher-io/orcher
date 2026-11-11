//! Engine performance metrics collection.

use crate::engine::system::{OrchestrationSystem, ProcessorState};
use std::fmt::Write;

// ============================================================================
// Prometheus Exporter
// ============================================================================

/// Prometheus metrics exporter for ORCHER coordination system
pub struct PrometheusExporter {
    /// Namespace prefix for metrics (default: "orcher")
    namespace: String,
}

impl PrometheusExporter {
    /// Create a new Prometheus exporter with default namespace
    pub fn new() -> Self {
        Self {
            namespace: "orcher".to_string(),
        }
    }

    /// Create a new Prometheus exporter with custom namespace
    pub fn with_namespace(namespace: String) -> Self {
        Self { namespace }
    }

    /// Export all metrics in Prometheus text format
    pub async fn export_metrics(&self, system: &OrchestrationSystem) -> String {
        let mut output = String::new();

        // Get metrics from system
        let coord_metrics = system.coordinator_metrics().await;
        let outbox_metrics = system.outbox_metrics().await;
        let timer_metrics = system.timer_metrics().await;
        let health = system.health().await;
        let cache_stats = system.coordinator().get_cache_stats().await;

        // Workflow metrics
        self.write_counter(
            &mut output,
            "workflows_started_total",
            "Total number of workflows started",
            coord_metrics.workflows_started,
        );

        self.write_counter(
            &mut output,
            "workflows_completed_total",
            "Total number of workflows completed",
            coord_metrics.workflows_completed,
        );

        self.write_counter(
            &mut output,
            "workflows_failed_total",
            "Total number of workflows failed",
            coord_metrics.workflows_failed,
        );

        self.write_counter(
            &mut output,
            "steps_completed_total",
            "Total number of workflow steps completed",
            coord_metrics.steps_completed,
        );

        // Cache metrics
        self.write_counter(
            &mut output,
            "cache_hits_total",
            "Total number of cache hits",
            coord_metrics.cache_hits,
        );

        self.write_counter(
            &mut output,
            "cache_misses_total",
            "Total number of cache misses",
            coord_metrics.cache_misses,
        );

        let cache_hit_rate = if coord_metrics.cache_hits + coord_metrics.cache_misses > 0 {
            coord_metrics.cache_hits as f64
                / (coord_metrics.cache_hits + coord_metrics.cache_misses) as f64
        } else {
            0.0
        };

        self.write_gauge(
            &mut output,
            "cache_hit_rate",
            "Cache hit rate (0.0 to 1.0)",
            cache_hit_rate,
        );

        self.write_gauge(
            &mut output,
            "cache_entries",
            "Current number of entries in cache",
            cache_stats.entries as f64,
        );

        self.write_gauge(
            &mut output,
            "cache_memory_bytes",
            "Current cache memory usage in bytes",
            cache_stats.memory_bytes as f64,
        );

        self.write_gauge(
            &mut output,
            "cache_memory_max_bytes",
            "Maximum cache memory limit in bytes",
            cache_stats.max_memory_bytes as f64,
        );

        // State management metrics
        self.write_counter(
            &mut output,
            "state_reconstructions_total",
            "Total number of workflow state reconstructions",
            coord_metrics.state_reconstructions,
        );

        self.write_counter(
            &mut output,
            "lock_acquisitions_total",
            "Total number of distributed lock acquisitions",
            coord_metrics.lock_acquisitions,
        );

        self.write_counter(
            &mut output,
            "invalidations_sent_total",
            "Total number of cache invalidation messages sent",
            coord_metrics.invalidations_sent,
        );

        // Task & timer metrics
        self.write_counter(
            &mut output,
            "tasks_dispatched_total",
            "Total number of tasks dispatched via outbox",
            coord_metrics.tasks_dispatched,
        );

        self.write_counter(
            &mut output,
            "timers_scheduled_total",
            "Total number of timers scheduled",
            coord_metrics.timers_scheduled,
        );

        self.write_counter(
            &mut output,
            "outbox_processed_total",
            "Total number of outbox tasks processed successfully",
            outbox_metrics.tasks_succeeded,
        );

        self.write_counter(
            &mut output,
            "outbox_failed_total",
            "Total number of outbox tasks failed",
            outbox_metrics.tasks_failed,
        );

        self.write_counter(
            &mut output,
            "outbox_dlq_total",
            "Total number of tasks moved to dead letter queue",
            outbox_metrics.tasks_dlq,
        );

        self.write_counter(
            &mut output,
            "timers_fired_total",
            "Total number of timers fired",
            timer_metrics.timers_fired,
        );

        self.write_counter(
            &mut output,
            "timers_failed_total",
            "Total number of timers failed",
            timer_metrics.timers_failed,
        );

        // Health metrics
        self.write_gauge(
            &mut output,
            "system_healthy",
            "System health status (1 = healthy, 0 = unhealthy)",
            if health.healthy { 1.0 } else { 0.0 },
        );

        self.write_gauge(
            &mut output,
            "coordinator_healthy",
            "Coordinator health status (1 = healthy, 0 = unhealthy)",
            if health.coordinator_healthy { 1.0 } else { 0.0 },
        );

        self.write_gauge(
            &mut output,
            "outbox_processor_state",
            "Outbox processor state (0 = stopped, 1 = starting, 2 = running, 3 = shutting down, 4 = failed)",
            self.processor_state_value(health.outbox_state),
        );

        self.write_gauge(
            &mut output,
            "timer_processor_state",
            "Timer processor state (0 = stopped, 1 = starting, 2 = running, 3 = shutting down, 4 = failed)",
            self.processor_state_value(health.timer_state),
        );

        output
    }

    /// Export metrics with labels
    pub async fn export_metrics_with_labels(
        &self,
        system: &OrchestrationSystem,
        labels: &[(&str, &str)],
    ) -> String {
        let mut output = String::new();

        // Get metrics
        let coord_metrics = system.coordinator_metrics().await;
        let _outbox_metrics = system.outbox_metrics().await;
        let _timer_metrics = system.timer_metrics().await;
        let _health = system.health().await;

        let label_str = self.format_labels(labels);

        // Workflow metrics with labels
        self.write_counter_with_labels(
            &mut output,
            "workflows_started_total",
            "Total number of workflows started",
            coord_metrics.workflows_started,
            &label_str,
        );

        self.write_counter_with_labels(
            &mut output,
            "workflows_completed_total",
            "Total number of workflows completed",
            coord_metrics.workflows_completed,
            &label_str,
        );

        self.write_counter_with_labels(
            &mut output,
            "workflows_failed_total",
            "Total number of workflows failed",
            coord_metrics.workflows_failed,
            &label_str,
        );

        // Add more metrics with labels as needed...

        output
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    fn write_counter(&self, output: &mut String, name: &str, help: &str, value: u64) {
        let _ = writeln!(output, "# HELP {}_{} {}", self.namespace, name, help);
        let _ = writeln!(output, "# TYPE {}_{} counter", self.namespace, name);
        let _ = writeln!(output, "{}_{} {}", self.namespace, name, value);
        let _ = writeln!(output);
    }

    fn write_gauge(&self, output: &mut String, name: &str, help: &str, value: f64) {
        let _ = writeln!(output, "# HELP {}_{} {}", self.namespace, name, help);
        let _ = writeln!(output, "# TYPE {}_{} gauge", self.namespace, name);
        let _ = writeln!(output, "{}_{} {}", self.namespace, name, value);
        let _ = writeln!(output);
    }

    fn write_counter_with_labels(
        &self,
        output: &mut String,
        name: &str,
        help: &str,
        value: u64,
        labels: &str,
    ) {
        let _ = writeln!(output, "# HELP {}_{} {}", self.namespace, name, help);
        let _ = writeln!(output, "# TYPE {}_{} counter", self.namespace, name);
        let _ = writeln!(output, "{}_{}{} {}", self.namespace, name, labels, value);
        let _ = writeln!(output);
    }

    fn format_labels(&self, labels: &[(&str, &str)]) -> String {
        if labels.is_empty() {
            return String::new();
        }

        let label_pairs: Vec<String> = labels
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, v))
            .collect();

        format!("{{{}}}", label_pairs.join(","))
    }

    fn processor_state_value(&self, state: ProcessorState) -> f64 {
        match state {
            ProcessorState::Stopped => 0.0,
            ProcessorState::Starting => 1.0,
            ProcessorState::Running => 2.0,
            ProcessorState::ShuttingDown => 3.0,
            ProcessorState::Failed => 4.0,
        }
    }
}

impl Default for PrometheusExporter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// HTTP Handler Helpers
// ============================================================================

/// Helper function to create metrics endpoint response
pub async fn metrics_endpoint_handler(system: &OrchestrationSystem) -> String {
    let exporter = PrometheusExporter::new();
    exporter.export_metrics(system).await
}

/// Helper function to create metrics endpoint response with labels
pub async fn metrics_endpoint_handler_with_labels(
    system: &OrchestrationSystem,
    instance_id: &str,
    environment: &str,
) -> String {
    let exporter = PrometheusExporter::new();
    let labels = vec![("instance", instance_id), ("environment", environment)];
    exporter.export_metrics_with_labels(system, &labels).await
}

// ============================================================================
// Grafana Dashboard Helper
// ============================================================================

/// Generate a basic Grafana dashboard JSON for ORCHER metrics
pub fn generate_grafana_dashboard() -> String {
    r#"{
  "dashboard": {
    "title": "ORCHER Workflow Orchestration",
    "panels": [
      {
        "title": "Workflows",
        "targets": [
          {
            "expr": "rate(orcher_workflows_started_total[5m])",
            "legendFormat": "Started"
          },
          {
            "expr": "rate(orcher_workflows_completed_total[5m])",
            "legendFormat": "Completed"
          },
          {
            "expr": "rate(orcher_workflows_failed_total[5m])",
            "legendFormat": "Failed"
          }
        ]
      },
      {
        "title": "Cache Performance",
        "targets": [
          {
            "expr": "orcher_cache_hit_rate",
            "legendFormat": "Hit Rate"
          }
        ]
      },
      {
        "title": "System Health",
        "targets": [
          {
            "expr": "orcher_system_healthy",
            "legendFormat": "System"
          },
          {
            "expr": "orcher_coordinator_healthy",
            "legendFormat": "Coordinator"
          }
        ]
      }
    ]
  }
}"#
    .to_string()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exporter_creation() {
        let exporter = PrometheusExporter::new();
        assert_eq!(exporter.namespace, "orcher");

        let custom = PrometheusExporter::with_namespace("custom".to_string());
        assert_eq!(custom.namespace, "custom");
    }

    #[test]
    fn test_format_labels() {
        let exporter = PrometheusExporter::new();

        let labels = vec![("instance", "coord-1"), ("env", "prod")];
        let result = exporter.format_labels(&labels);
        assert_eq!(result, r#"{instance="coord-1",env="prod"}"#);

        let empty: Vec<(&str, &str)> = vec![];
        let result = exporter.format_labels(&empty);
        assert_eq!(result, "");
    }

    #[test]
    fn test_processor_state_value() {
        let exporter = PrometheusExporter::new();

        assert_eq!(exporter.processor_state_value(ProcessorState::Stopped), 0.0);
        assert_eq!(
            exporter.processor_state_value(ProcessorState::Starting),
            1.0
        );
        assert_eq!(exporter.processor_state_value(ProcessorState::Running), 2.0);
        assert_eq!(
            exporter.processor_state_value(ProcessorState::ShuttingDown),
            3.0
        );
        assert_eq!(exporter.processor_state_value(ProcessorState::Failed), 4.0);
    }

    #[test]
    fn test_write_counter() {
        let exporter = PrometheusExporter::new();
        let mut output = String::new();

        exporter.write_counter(&mut output, "test_total", "Test counter", 42);

        assert!(output.contains("# HELP orcher_test_total Test counter"));
        assert!(output.contains("# TYPE orcher_test_total counter"));
        assert!(output.contains("orcher_test_total 42"));
    }

    #[test]
    fn test_write_gauge() {
        let exporter = PrometheusExporter::new();
        let mut output = String::new();

        exporter.write_gauge(&mut output, "test_gauge", "Test gauge", 3.14);

        assert!(output.contains("# HELP orcher_test_gauge Test gauge"));
        assert!(output.contains("# TYPE orcher_test_gauge gauge"));
        assert!(output.contains("orcher_test_gauge 3.14"));
    }
}
