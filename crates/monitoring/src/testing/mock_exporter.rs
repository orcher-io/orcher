//! Mock exporter for testing metric collection
//!
//! This module provides a mock metrics exporter that captures emitted metrics
//! for testing purposes without requiring external dependencies.

use crate::error::{MonitoringError, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Mock metrics exporter for capturing metrics in tests
#[derive(Debug, Clone)]
pub struct MockExporter {
    inner: Arc<RwLock<MockExporterInner>>,
}

#[derive(Debug, Default)]
struct MockExporterInner {
    counters: HashMap<String, u64>,
    gauges: HashMap<String, f64>,
    histograms: HashMap<String, Vec<f64>>,
    export_count: usize,
    last_export_error: Option<String>,
}

impl MockExporter {
    /// Create a new mock exporter
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MockExporterInner::default())),
        }
    }

    /// Record a counter value
    pub fn record_counter(&self, name: impl Into<String>, value: u64) {
        let mut inner = self.inner.write();
        *inner.counters.entry(name.into()).or_insert(0) += value;
    }

    /// Record a gauge value
    pub fn record_gauge(&self, name: impl Into<String>, value: f64) {
        let mut inner = self.inner.write();
        inner.gauges.insert(name.into(), value);
    }

    /// Record a histogram value
    pub fn record_histogram(&self, name: impl Into<String>, value: f64) {
        let mut inner = self.inner.write();
        inner.histograms.entry(name.into()).or_default().push(value);
    }

    /// Get the current value of a counter
    pub fn get_counter(&self, name: &str) -> u64 {
        let inner = self.inner.read();
        inner.counters.get(name).copied().unwrap_or(0)
    }

    /// Get the current value of a gauge
    pub fn get_gauge(&self, name: &str) -> Option<f64> {
        let inner = self.inner.read();
        inner.gauges.get(name).copied()
    }

    /// Get all recorded histogram values
    pub fn get_histogram_values(&self, name: &str) -> Vec<f64> {
        let inner = self.inner.read();
        inner.histograms.get(name).cloned().unwrap_or_default()
    }

    /// Get the number of times export was called
    pub fn export_count(&self) -> usize {
        let inner = self.inner.read();
        inner.export_count
    }

    /// Simulate an export operation
    pub fn export(&self) -> Result<()> {
        let mut inner = self.inner.write();
        inner.export_count += 1;

        if let Some(error) = &inner.last_export_error {
            return Err(MonitoringError::ExporterError {
                exporter: "mock".to_string(),
                message: error.clone(),
            });
        }

        Ok(())
    }

    /// Set an error to be returned on the next export
    pub fn set_export_error(&self, error: impl Into<String>) {
        let mut inner = self.inner.write();
        inner.last_export_error = Some(error.into());
    }

    /// Clear the export error
    pub fn clear_export_error(&self) {
        let mut inner = self.inner.write();
        inner.last_export_error = None;
    }

    /// Clear all recorded metrics
    pub fn clear_metrics(&self) {
        let mut inner = self.inner.write();
        inner.counters.clear();
        inner.gauges.clear();
        inner.histograms.clear();
    }

    /// Get all counter names
    pub fn counter_names(&self) -> Vec<String> {
        let inner = self.inner.read();
        inner.counters.keys().cloned().collect()
    }

    /// Get all gauge names
    pub fn gauge_names(&self) -> Vec<String> {
        let inner = self.inner.read();
        inner.gauges.keys().cloned().collect()
    }

    /// Get all histogram names
    pub fn histogram_names(&self) -> Vec<String> {
        let inner = self.inner.read();
        inner.histograms.keys().cloned().collect()
    }
}

impl Default for MockExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_exporter_counters() {
        let exporter = MockExporter::new();

        exporter.record_counter("test_counter", 5);
        assert_eq!(exporter.get_counter("test_counter"), 5);

        exporter.record_counter("test_counter", 3);
        assert_eq!(exporter.get_counter("test_counter"), 8);
    }

    #[test]
    fn test_mock_exporter_gauges() {
        let exporter = MockExporter::new();

        exporter.record_gauge("test_gauge", 42.0);
        assert_eq!(exporter.get_gauge("test_gauge"), Some(42.0));

        exporter.record_gauge("test_gauge", 100.0);
        assert_eq!(exporter.get_gauge("test_gauge"), Some(100.0));
    }

    #[test]
    fn test_mock_exporter_histograms() {
        let exporter = MockExporter::new();

        exporter.record_histogram("test_histogram", 1.5);
        exporter.record_histogram("test_histogram", 2.5);
        exporter.record_histogram("test_histogram", 3.5);

        let values = exporter.get_histogram_values("test_histogram");
        assert_eq!(values.len(), 3);
        assert_eq!(values, vec![1.5, 2.5, 3.5]);
    }

    #[test]
    fn test_export_success() {
        let exporter = MockExporter::new();

        assert_eq!(exporter.export_count(), 0);
        assert!(exporter.export().is_ok());
        assert_eq!(exporter.export_count(), 1);
        assert!(exporter.export().is_ok());
        assert_eq!(exporter.export_count(), 2);
    }

    #[test]
    fn test_export_error() {
        let exporter = MockExporter::new();

        exporter.set_export_error("test error");
        assert!(exporter.export().is_err());

        exporter.clear_export_error();
        assert!(exporter.export().is_ok());
    }

    #[test]
    fn test_clear_metrics() {
        let exporter = MockExporter::new();

        exporter.record_counter("counter", 10);
        exporter.record_gauge("gauge", 20.0);
        exporter.record_histogram("histogram", 30.0);

        assert_eq!(exporter.get_counter("counter"), 10);
        assert_eq!(exporter.get_gauge("gauge"), Some(20.0));

        exporter.clear_metrics();

        assert_eq!(exporter.get_counter("counter"), 0);
        assert_eq!(exporter.get_gauge("gauge"), None);
        assert!(exporter.get_histogram_values("histogram").is_empty());
    }

    #[test]
    fn test_metric_names() {
        let exporter = MockExporter::new();

        exporter.record_counter("counter1", 1);
        exporter.record_counter("counter2", 2);
        exporter.record_gauge("gauge1", 1.0);
        exporter.record_histogram("hist1", 1.0);

        let counters = exporter.counter_names();
        assert_eq!(counters.len(), 2);
        assert!(counters.contains(&"counter1".to_string()));
        assert!(counters.contains(&"counter2".to_string()));

        let gauges = exporter.gauge_names();
        assert_eq!(gauges.len(), 1);
        assert!(gauges.contains(&"gauge1".to_string()));

        let histograms = exporter.histogram_names();
        assert_eq!(histograms.len(), 1);
        assert!(histograms.contains(&"hist1".to_string()));
    }
}
