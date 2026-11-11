//! Manages multiple exporters simultaneously.

use crate::error::MonitoringError;
use crate::exporters::{Exporter, ExporterResult};
use std::collections::HashMap;

pub struct ExporterGroup {
    exporters: HashMap<String, Box<dyn Exporter>>,
}

impl Default for ExporterGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl ExporterGroup {
    pub fn new() -> Self {
        Self {
            exporters: HashMap::new(),
        }
    }

    pub fn add_exporter<E>(&mut self, name: &str, exporter: E) -> ExporterResult<()>
    where
        E: Exporter + 'static,
    {
        if self.exporters.contains_key(name) {
            return Err(MonitoringError::exporter_error(
                "multi",
                format!("Exporter '{}' already exists in group", name),
            ));
        }

        self.exporters.insert(name.to_string(), Box::new(exporter));
        Ok(())
    }

    /// Stops the exporter if running before removing it.
    pub fn remove_exporter(&mut self, name: &str) -> ExporterResult<()> {
        if let Some(mut exporter) = self.exporters.remove(name) {
            if exporter.is_running() {
                exporter.stop()?;
            }
            Ok(())
        } else {
            Err(MonitoringError::exporter_error(
                "multi",
                format!("Exporter '{}' not found in group", name),
            ))
        }
    }

    pub fn exporter_names(&self) -> Vec<String> {
        self.exporters.keys().cloned().collect()
    }

    pub fn has_exporter(&self, name: &str) -> bool {
        self.exporters.contains_key(name)
    }

    pub fn len(&self) -> usize {
        self.exporters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.exporters.is_empty()
    }
}

impl Exporter for ExporterGroup {
    fn start(&mut self) -> ExporterResult<()> {
        let mut errors = Vec::new();

        for (name, exporter) in &mut self.exporters {
            if let Err(e) = exporter.start() {
                errors.push(format!("{}: {}", name, e));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(MonitoringError::exporter_error(
                "multi",
                format!("Failed to start exporters: {}", errors.join(", ")),
            ))
        }
    }

    fn stop(&mut self) -> ExporterResult<()> {
        let mut errors = Vec::new();

        for (name, exporter) in &mut self.exporters {
            if let Err(e) = exporter.stop() {
                errors.push(format!("{}: {}", name, e));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(MonitoringError::exporter_error(
                "multi",
                format!("Failed to stop exporters: {}", errors.join(", ")),
            ))
        }
    }

    fn is_running(&self) -> bool {
        self.exporters.values().any(|e| e.is_running())
    }

    fn health_status(&self) -> ExporterResult<String> {
        let mut statuses = Vec::new();

        for (name, exporter) in &self.exporters {
            match exporter.health_status() {
                Ok(status) => statuses.push(format!("{}: {}", name, status)),
                Err(e) => statuses.push(format!("{}: ERROR - {}", name, e)),
            }
        }

        if statuses.is_empty() {
            Ok("Multi-exporter: empty group".to_string())
        } else {
            Ok(format!("Multi-exporter: [{}]", statuses.join("; ")))
        }
    }
}

/// Builder for configuring an [`ExporterGroup`].
pub struct MultiExporter {
    group: ExporterGroup,
}

impl MultiExporter {
    pub fn new() -> Self {
        Self {
            group: ExporterGroup::new(),
        }
    }

    pub fn with_exporter<E>(mut self, name: &str, exporter: E) -> ExporterResult<Self>
    where
        E: Exporter + 'static,
    {
        self.group.add_exporter(name, exporter)?;
        Ok(self)
    }

    pub fn build(self) -> ExporterGroup {
        self.group
    }
}

impl Default for MultiExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::jaeger::JaegerExporter;
    use crate::exporters::prometheus::PrometheusExporter;

    #[test]
    fn test_exporter_group_creation() {
        let group = ExporterGroup::new();
        assert!(group.is_empty());
        assert_eq!(group.len(), 0);
    }

    #[test]
    fn test_add_remove_exporters() {
        let mut group = ExporterGroup::new();

        let prometheus = PrometheusExporter::builder().build().unwrap();
        assert!(group.add_exporter("prometheus", prometheus).is_ok());

        let jaeger = JaegerExporter::builder().build().unwrap();
        assert!(group.add_exporter("jaeger", jaeger).is_ok());

        assert_eq!(group.len(), 2);
        assert!(group.has_exporter("prometheus"));
        assert!(group.has_exporter("jaeger"));

        // Test duplicate name
        let duplicate_prometheus = PrometheusExporter::builder().build().unwrap();
        assert!(group
            .add_exporter("prometheus", duplicate_prometheus)
            .is_err());

        // Test removal
        assert!(group.remove_exporter("prometheus").is_ok());
        assert_eq!(group.len(), 1);
        assert!(!group.has_exporter("prometheus"));

        // Test removing non-existent
        assert!(group.remove_exporter("non-existent").is_err());
    }

    #[test]
    fn test_exporter_group_lifecycle() {
        let mut group = ExporterGroup::new();

        let prometheus = PrometheusExporter::builder()
            .with_test_mode(true)
            .build()
            .unwrap();
        group.add_exporter("prometheus", prometheus).unwrap();

        let jaeger = JaegerExporter::builder().build().unwrap();
        group.add_exporter("jaeger", jaeger).unwrap();

        // Initially not running
        assert!(!group.is_running());

        // Start all
        assert!(group.start().is_ok());
        assert!(group.is_running());

        // Stop all
        assert!(group.stop().is_ok());
        assert!(!group.is_running());
    }

    #[test]
    fn test_multi_exporter_builder() {
        let prometheus = PrometheusExporter::builder()
            .with_test_mode(true)
            .build()
            .unwrap();
        let jaeger = JaegerExporter::builder().build().unwrap();

        let multi = MultiExporter::new()
            .with_exporter("prometheus", prometheus)
            .unwrap()
            .with_exporter("jaeger", jaeger)
            .unwrap()
            .build();

        assert_eq!(multi.len(), 2);
        assert!(multi.has_exporter("prometheus"));
        assert!(multi.has_exporter("jaeger"));
    }

    #[test]
    fn test_exporter_names() {
        let mut group = ExporterGroup::new();

        let prometheus = PrometheusExporter::builder()
            .with_test_mode(true)
            .build()
            .unwrap();
        group.add_exporter("prometheus", prometheus).unwrap();

        let jaeger = JaegerExporter::builder().build().unwrap();
        group.add_exporter("jaeger", jaeger).unwrap();

        let names = group.exporter_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"prometheus".to_string()));
        assert!(names.contains(&"jaeger".to_string()));
    }

    #[test]
    fn test_health_status() {
        let mut group = ExporterGroup::new();

        let prometheus = PrometheusExporter::builder()
            .with_test_mode(true)
            .build()
            .unwrap();
        group.add_exporter("prometheus", prometheus).unwrap();

        let status = group.health_status().unwrap();
        assert!(status.contains("prometheus"));
        assert!(status.contains("Multi-exporter"));
    }
}
