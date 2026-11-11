//! Routing key generation for message queues.
//!
//! Keys follow `{prefix}.{type}.{capability}.{priority}` structure.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for routing key generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Separator between key components (default: `"."`).
    pub separator: String,

    /// Include capability in routing key (default: true).
    pub capability_routing: bool,

    /// Include priority in routing key (default: true).
    pub priority_routing: bool,

    pub custom_patterns: HashMap<String, String>,

    /// Default capability when none specified (default: "default").
    pub default_capability: String,

    /// Default priority when none specified (default: "normal").
    pub default_priority: String,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            separator: ".".to_string(),
            capability_routing: true,
            priority_routing: true,
            custom_patterns: HashMap::new(),
            default_capability: "default".to_string(),
            default_priority: "normal".to_string(),
        }
    }
}

impl RoutingConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_separator(mut self, separator: impl Into<String>) -> Self {
        self.separator = separator.into();
        self
    }

    pub fn with_capability_routing(mut self, enabled: bool) -> Self {
        self.capability_routing = enabled;
        self
    }

    pub fn with_priority_routing(mut self, enabled: bool) -> Self {
        self.priority_routing = enabled;
        self
    }

    pub fn with_default_capability(mut self, capability: impl Into<String>) -> Self {
        self.default_capability = capability.into();
        self
    }

    pub fn with_default_priority(mut self, priority: impl Into<String>) -> Self {
        self.default_priority = priority.into();
        self
    }

    pub fn with_custom_pattern(
        mut self,
        key: impl Into<String>,
        pattern: impl Into<String>,
    ) -> Self {
        self.custom_patterns.insert(key.into(), pattern.into());
        self
    }
}

/// Generates routing keys for queue messages.
pub struct RoutingKeyGenerator {
    config: RoutingConfig,
}

impl RoutingKeyGenerator {
    pub fn new(config: RoutingConfig) -> Self {
        Self { config }
    }

    /// Join components into a routing key using the configured separator.
    pub fn generate(&self, components: &[&str]) -> String {
        components.join(&self.config.separator)
    }

    /// Generate `events.task.{event_type}.{capability}.{priority}` routing key.
    pub fn generate_for_task_event(
        &self,
        event_type: &str,
        capability: Option<&str>,
        priority: &str,
    ) -> String {
        let mut parts = vec!["events", "task", event_type];

        if self.config.capability_routing {
            parts.push(capability.unwrap_or(&self.config.default_capability));
        }

        if self.config.priority_routing {
            parts.push(priority);
        }

        self.generate(&parts)
    }

    /// Generate `tasks.{capability}.{priority}` routing key.
    pub fn generate_for_task_dispatch(&self, capability: Option<&str>, priority: &str) -> String {
        let mut parts = vec!["tasks"];

        if self.config.capability_routing {
            parts.push(capability.unwrap_or(&self.config.default_capability));
        }

        if self.config.priority_routing {
            parts.push(priority);
        }

        self.generate(&parts)
    }

    /// Generate `events.worker.{event_type}.{worker_id}` routing key.
    pub fn generate_for_worker_event(&self, event_type: &str, worker_id: Option<&str>) -> String {
        let mut parts = vec!["events", "worker", event_type];

        if let Some(id) = worker_id {
            parts.push(id);
        }

        self.generate(&parts)
    }

    pub fn has_custom_pattern(&self, key: &str) -> bool {
        self.config.custom_patterns.contains_key(key)
    }

    pub fn get_custom_pattern(&self, key: &str) -> Option<&String> {
        self.config.custom_patterns.get(key)
    }

    pub fn config(&self) -> &RoutingConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RoutingConfig::default();
        assert_eq!(config.separator, ".");
        assert!(config.capability_routing);
        assert!(config.priority_routing);
        assert_eq!(config.default_capability, "default");
        assert_eq!(config.default_priority, "normal");
    }

    #[test]
    fn test_config_builder() {
        let config = RoutingConfig::default()
            .with_separator(":")
            .with_capability_routing(false)
            .with_default_capability("custom");

        assert_eq!(config.separator, ":");
        assert!(!config.capability_routing);
        assert_eq!(config.default_capability, "custom");
    }

    #[test]
    fn test_generate_basic() {
        let generator = RoutingKeyGenerator::new(RoutingConfig::default());
        let key = generator.generate(&["a", "b", "c"]);
        assert_eq!(key, "a.b.c");
    }

    #[test]
    fn test_generate_task_event_with_capability() {
        let generator = RoutingKeyGenerator::new(RoutingConfig::default());
        let key = generator.generate_for_task_event("completed", Some("gpu"), "high");
        assert_eq!(key, "events.task.completed.gpu.high");
    }

    #[test]
    fn test_generate_task_event_without_capability() {
        let generator = RoutingKeyGenerator::new(RoutingConfig::default());
        let key = generator.generate_for_task_event("started", None, "normal");
        assert_eq!(key, "events.task.started.default.normal");
    }

    #[test]
    fn test_generate_task_event_no_capability_routing() {
        let config = RoutingConfig::default().with_capability_routing(false);
        let generator = RoutingKeyGenerator::new(config);
        let key = generator.generate_for_task_event("failed", Some("gpu"), "low");
        assert_eq!(key, "events.task.failed.low");
    }

    #[test]
    fn test_generate_task_event_no_priority_routing() {
        let config = RoutingConfig::default().with_priority_routing(false);
        let generator = RoutingKeyGenerator::new(config);
        let key = generator.generate_for_task_event("timeout", Some("python"), "high");
        assert_eq!(key, "events.task.timeout.python");
    }

    #[test]
    fn test_generate_task_dispatch() {
        let generator = RoutingKeyGenerator::new(RoutingConfig::default());
        let key = generator.generate_for_task_dispatch(Some("python"), "high");
        assert_eq!(key, "tasks.python.high");
    }

    #[test]
    fn test_generate_task_dispatch_default() {
        let generator = RoutingKeyGenerator::new(RoutingConfig::default());
        let key = generator.generate_for_task_dispatch(None, "normal");
        assert_eq!(key, "tasks.default.normal");
    }

    #[test]
    fn test_generate_worker_event() {
        let generator = RoutingKeyGenerator::new(RoutingConfig::default());
        let key = generator.generate_for_worker_event("registered", Some("worker-1"));
        assert_eq!(key, "events.worker.registered.worker-1");
    }

    #[test]
    fn test_generate_worker_event_no_id() {
        let generator = RoutingKeyGenerator::new(RoutingConfig::default());
        let key = generator.generate_for_worker_event("heartbeat", None);
        assert_eq!(key, "events.worker.heartbeat");
    }

    #[test]
    fn test_custom_separator() {
        let config = RoutingConfig::default().with_separator(":");
        let generator = RoutingKeyGenerator::new(config);
        let key = generator.generate(&["a", "b", "c"]);
        assert_eq!(key, "a:b:c");
    }

    #[test]
    fn test_custom_pattern() {
        let config =
            RoutingConfig::default().with_custom_pattern("special", "custom.route.pattern");
        let generator = RoutingKeyGenerator::new(config);

        assert!(generator.has_custom_pattern("special"));
        assert_eq!(
            generator.get_custom_pattern("special"),
            Some(&"custom.route.pattern".to_string())
        );
    }

    #[test]
    fn test_all_event_types() {
        let generator = RoutingKeyGenerator::new(RoutingConfig::default());

        let events = vec![
            ("started", "events.task.started.default.normal"),
            ("completed", "events.task.completed.default.normal"),
            ("failed", "events.task.failed.default.normal"),
            ("timeout", "events.task.timeout.default.normal"),
        ];

        for (event_type, expected) in events {
            let key = generator.generate_for_task_event(event_type, None, "normal");
            assert_eq!(key, expected);
        }
    }
}
