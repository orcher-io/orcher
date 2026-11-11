//! Worker storage types.

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_common::{
    types::identifiers::{OrganizationId, WorkerId},
    validation::{Validatable, ValidationResult},
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worker {
    pub id: WorkerId,
    pub organization_id: OrganizationId,
    /// Human-readable unique string, separate from UUID.
    pub worker_id: String,
    pub name: String,
    /// e.g., "standard", "gpu", "memory-intensive".
    pub worker_type: String,
    pub status: WorkerStatus,
    pub capabilities: Vec<String>,
    pub max_concurrent_tasks: i32,
    pub current_task_count: i32,
    pub cpu_cores: Option<f64>,
    pub memory_mb: Option<i32>,
    pub disk_space_mb: Option<i32>,
    pub metadata: serde_json::Value,
    pub tags: Vec<String>,
    pub host_address: Option<String>,
    pub port: Option<i32>,
    pub last_heartbeat: Option<Timestamptz>,
    /// Must be one of: healthy, degraded, unhealthy, unknown.
    pub health_status: String,
    pub health_details: serde_json::Value,
    pub created_at: Timestamptz,
    pub updated_at: Timestamptz,
    pub registered_at: Option<Timestamptz>,
}

/// Not the same as `WorkerState` in the worker crate (runtime state).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "postgres",
    sqlx(type_name = "text", rename_all = "lowercase")
)]
pub enum WorkerStatus {
    Idle,
    Busy,
    /// Finishing current tasks, not accepting new ones.
    Draining,
    Offline,
    Error,
}

impl Default for WorkerStatus {
    fn default() -> Self {
        Self::Idle
    }
}

impl std::fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Idle => "idle",
            Self::Busy => "busy",
            Self::Draining => "draining",
            Self::Offline => "offline",
            Self::Error => "error",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for WorkerStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "idle" => Ok(Self::Idle),
            "busy" => Ok(Self::Busy),
            "draining" => Ok(Self::Draining),
            "offline" => Ok(Self::Offline),
            "error" => Ok(Self::Error),
            _ => Err(format!("Invalid worker status: {}", s)),
        }
    }
}

impl Worker {
    pub fn new(
        organization_id: OrganizationId,
        worker_id: String,
        name: String,
        host_address: String,
        port: i32,
    ) -> Self {
        let now = Timestamptz::now();
        Self {
            id: WorkerId::new_v4(),
            organization_id,
            worker_id,
            name,
            worker_type: "standard".to_string(),
            status: WorkerStatus::Idle,
            capabilities: Vec::new(),
            max_concurrent_tasks: 10,
            current_task_count: 0,
            cpu_cores: None,
            memory_mb: None,
            disk_space_mb: None,
            metadata: serde_json::json!({}),
            tags: Vec::new(),
            host_address: Some(host_address),
            port: Some(port),
            last_heartbeat: None,
            health_status: "unknown".to_string(),
            health_details: serde_json::json!({}),
            created_at: now,
            updated_at: now,
            registered_at: Some(now),
        }
    }

    pub fn can_accept_tasks(&self) -> bool {
        matches!(self.status, WorkerStatus::Idle | WorkerStatus::Busy)
            && self.current_task_count < self.max_concurrent_tasks
    }

    /// Returns 0.0 to 1.0.
    pub fn utilization(&self) -> f64 {
        if self.max_concurrent_tasks == 0 {
            0.0
        } else {
            self.current_task_count as f64 / self.max_concurrent_tasks as f64
        }
    }

    pub fn is_healthy(&self, timeout_seconds: i64) -> bool {
        if let Some(last_heartbeat) = self.last_heartbeat {
            let now = Timestamptz::now();
            let threshold = now - SignedDuration::from_secs(timeout_seconds);
            last_heartbeat > threshold
        } else {
            false
        }
    }

    pub fn with_worker_type(mut self, worker_type: String) -> Self {
        self.worker_type = worker_type;
        self
    }

    pub fn with_capabilities(mut self, capabilities: Vec<String>) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn with_resources(
        mut self,
        cpu_cores: Option<f64>,
        memory_mb: Option<i32>,
        disk_space_mb: Option<i32>,
    ) -> Self {
        self.cpu_cores = cpu_cores;
        self.memory_mb = memory_mb;
        self.disk_space_mb = disk_space_mb;
        self
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_max_concurrent_tasks(mut self, max: i32) -> Self {
        self.max_concurrent_tasks = max.max(1);
        self
    }
}

impl Validatable for Worker {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::new();

        // Validate worker_id
        if self.worker_id.trim().is_empty() {
            result.add_error("worker_id", "Worker ID cannot be empty");
        }
        if self.worker_id.len() > 255 {
            result.add_error("worker_id", "Worker ID cannot exceed 255 characters");
        }

        // Validate name
        if self.name.trim().is_empty() {
            result.add_error("name", "Worker name cannot be empty");
        }
        if self.name.len() > 255 {
            result.add_error("name", "Worker name cannot exceed 255 characters");
        }

        // Validate worker_type
        if self.worker_type.len() > 100 {
            result.add_error("worker_type", "Worker type cannot exceed 100 characters");
        }

        // Validate max_concurrent_tasks
        if self.max_concurrent_tasks <= 0 || self.max_concurrent_tasks > 1000 {
            result.add_error(
                "max_concurrent_tasks",
                "Max concurrent tasks must be between 1 and 1000",
            );
        }

        // Validate current_task_count
        if self.current_task_count < 0 {
            result.add_error(
                "current_task_count",
                "Current task count cannot be negative",
            );
        }
        if self.current_task_count > self.max_concurrent_tasks {
            result.add_error(
                "current_task_count",
                "Current task count cannot exceed max concurrent tasks",
            );
        }

        // Validate health_status
        let valid_health = ["healthy", "degraded", "unhealthy", "unknown"];
        if !valid_health.contains(&self.health_status.as_str()) {
            result.add_error(
                "health_status",
                format!("Health status must be one of: {}", valid_health.join(", ")),
            );
        }

        // Validate resource limits if present
        if let Some(cpu) = self.cpu_cores {
            if cpu <= 0.0 || cpu > 1024.0 {
                result.add_error("cpu_cores", "CPU cores must be between 0 and 1024");
            }
        }

        if let Some(mem) = self.memory_mb {
            if mem <= 0 {
                result.add_error("memory_mb", "Memory must be positive");
            }
        }

        if let Some(disk) = self.disk_space_mb {
            if disk <= 0 {
                result.add_error("disk_space_mb", "Disk space must be positive");
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::traits::Identifier;

    #[test]
    fn test_worker_creation() {
        let org_id = OrganizationId::new_v4();
        let worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        );

        assert!(worker.id.is_valid());
        assert_eq!(worker.worker_id, "worker-001");
        assert_eq!(worker.name, "Test Worker");
        assert_eq!(worker.status, WorkerStatus::Idle);
        assert_eq!(worker.max_concurrent_tasks, 10);
        assert_eq!(worker.current_task_count, 0);
        assert_eq!(worker.worker_type, "standard");
    }

    #[test]
    fn test_worker_status_string_conversion() {
        // Test Display
        assert_eq!(WorkerStatus::Idle.to_string(), "idle");
        assert_eq!(WorkerStatus::Busy.to_string(), "busy");
        assert_eq!(WorkerStatus::Draining.to_string(), "draining");
        assert_eq!(WorkerStatus::Offline.to_string(), "offline");
        assert_eq!(WorkerStatus::Error.to_string(), "error");

        // Test FromStr
        assert_eq!("idle".parse::<WorkerStatus>().unwrap(), WorkerStatus::Idle);
        assert_eq!("busy".parse::<WorkerStatus>().unwrap(), WorkerStatus::Busy);
        assert_eq!(
            "draining".parse::<WorkerStatus>().unwrap(),
            WorkerStatus::Draining
        );
        assert_eq!(
            "offline".parse::<WorkerStatus>().unwrap(),
            WorkerStatus::Offline
        );
        assert_eq!(
            "error".parse::<WorkerStatus>().unwrap(),
            WorkerStatus::Error
        );

        // Test case insensitivity
        assert_eq!("IDLE".parse::<WorkerStatus>().unwrap(), WorkerStatus::Idle);
        assert_eq!("Busy".parse::<WorkerStatus>().unwrap(), WorkerStatus::Busy);

        // Test invalid
        assert!("invalid".parse::<WorkerStatus>().is_err());
        assert!("starting".parse::<WorkerStatus>().is_err());
        assert!("active".parse::<WorkerStatus>().is_err());
    }

    #[test]
    fn test_all_status_values_are_valid() {
        let statuses = vec![
            WorkerStatus::Idle,
            WorkerStatus::Busy,
            WorkerStatus::Draining,
            WorkerStatus::Offline,
            WorkerStatus::Error,
        ];

        for status in statuses {
            let s = status.to_string();
            let parsed: WorkerStatus = s.parse().unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_database_constraint_values() {
        // These are the exact values from the CHECK constraint
        let db_values = vec!["idle", "busy", "draining", "offline", "error"];

        for val in db_values {
            let status: WorkerStatus = val.parse().unwrap();
            assert_eq!(status.to_string(), val);
        }
    }

    #[test]
    fn test_worker_can_accept_tasks() {
        let org_id = OrganizationId::new_v4();
        let mut worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        );

        // Idle with capacity
        assert!(worker.can_accept_tasks());

        // Busy with capacity
        worker.status = WorkerStatus::Busy;
        worker.current_task_count = 5;
        assert!(worker.can_accept_tasks());

        // At capacity
        worker.current_task_count = 10;
        assert!(!worker.can_accept_tasks());

        // Offline
        worker.status = WorkerStatus::Offline;
        worker.current_task_count = 0;
        assert!(!worker.can_accept_tasks());

        // Draining
        worker.status = WorkerStatus::Draining;
        assert!(!worker.can_accept_tasks());

        // Error
        worker.status = WorkerStatus::Error;
        assert!(!worker.can_accept_tasks());
    }

    #[test]
    fn test_worker_utilization() {
        let org_id = OrganizationId::new_v4();
        let mut worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        );

        assert_eq!(worker.utilization(), 0.0);

        worker.current_task_count = 5;
        assert_eq!(worker.utilization(), 0.5);

        worker.current_task_count = 10;
        assert_eq!(worker.utilization(), 1.0);

        // Test zero max
        worker.max_concurrent_tasks = 0;
        assert_eq!(worker.utilization(), 0.0);
    }

    #[test]
    fn test_worker_health_check() {
        let org_id = OrganizationId::new_v4();
        let mut worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        );

        // No heartbeat
        assert!(!worker.is_healthy(60));

        // Recent heartbeat
        worker.last_heartbeat = Some(Timestamptz::now());
        assert!(worker.is_healthy(60));

        // Old heartbeat
        worker.last_heartbeat = Some(Timestamptz::now() - SignedDuration::from_secs(120));
        assert!(!worker.is_healthy(60));
    }

    #[test]
    fn test_worker_validation() {
        let org_id = OrganizationId::new_v4();
        let worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        );

        let result = worker.validate();
        assert!(result.is_valid());
    }

    #[test]
    fn test_worker_validation_empty_worker_id() {
        let org_id = OrganizationId::new_v4();
        let mut worker = Worker::new(
            org_id,
            "".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        );
        worker.worker_id = "".to_string();

        let result = worker.validate();
        assert!(!result.is_valid());
        assert!(!result.errors_for_field("worker_id").is_empty());
    }

    #[test]
    fn test_worker_validation_empty_name() {
        let org_id = OrganizationId::new_v4();
        let mut worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "".to_string(),
            "localhost".to_string(),
            8080,
        );
        worker.name = "".to_string();

        let result = worker.validate();
        assert!(!result.is_valid());
        assert!(!result.errors_for_field("name").is_empty());
    }

    #[test]
    fn test_worker_validation_invalid_health_status() {
        let org_id = OrganizationId::new_v4();
        let mut worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        );
        worker.health_status = "invalid".to_string();

        let result = worker.validate();
        assert!(!result.is_valid());
        assert!(!result.errors_for_field("health_status").is_empty());
    }

    #[test]
    fn test_worker_serialization() {
        let org_id = OrganizationId::new_v4();
        let worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        );

        let json = serde_json::to_string(&worker).unwrap();
        let deserialized: Worker = serde_json::from_str(&json).unwrap();

        assert_eq!(worker.id, deserialized.id);
        assert_eq!(worker.worker_id, deserialized.worker_id);
        assert_eq!(worker.name, deserialized.name);
        assert_eq!(worker.status, deserialized.status);
    }

    #[test]
    fn test_worker_builder_methods() {
        let org_id = OrganizationId::new_v4();
        let worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        )
        .with_worker_type("gpu".to_string())
        .with_capabilities(vec!["cuda".to_string(), "tensorflow".to_string()])
        .with_resources(Some(8.0), Some(16384), Some(102400))
        .with_tags(vec!["production".to_string(), "gpu".to_string()])
        .with_metadata(serde_json::json!({"region": "us-west"}));

        assert_eq!(worker.worker_type, "gpu");
        assert_eq!(worker.capabilities.len(), 2);
        assert_eq!(worker.cpu_cores, Some(8.0));
        assert_eq!(worker.memory_mb, Some(16384));
        assert_eq!(worker.tags.len(), 2);
    }

    #[test]
    fn test_worker_status_default() {
        assert_eq!(WorkerStatus::default(), WorkerStatus::Idle);
    }
}
