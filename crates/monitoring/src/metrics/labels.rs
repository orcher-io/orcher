//! Standard label key definitions for consistent metric labeling.

pub mod tasks {
    pub const TASK_ID: &str = "task_id";
    pub const TASK_TYPE: &str = "task_type";
    pub const TASK_STATUS: &str = "task_status";
    pub const TASK_PRIORITY: &str = "task_priority";
}

pub mod workflows {
    pub const WORKFLOW_ID: &str = "workflow_id";
    pub const WORKFLOW_NAME: &str = "workflow_name";
    pub const WORKFLOW_STEP: &str = "workflow_step";
}

pub mod workers {
    pub const WORKER_ID: &str = "worker_id";
    /// e.g., "cpu", "gpu", "memory_intensive"
    pub const WORKER_TYPE: &str = "worker_type";
}

pub mod operations {
    pub const OPERATION: &str = "operation";
    /// e.g., "success", "failure", "timeout"
    pub const RESULT: &str = "result";
    pub const ERROR_TYPE: &str = "error_type";
}

pub mod security {
    pub const USER_ID: &str = "user_id";
    pub const TENANT_ID: &str = "tenant_id";
}

pub mod scheduler {
    pub const SCHEDULER_ID: &str = "scheduler_id";
    /// e.g., "cron", "interval", "manual"
    pub const TRIGGER_TYPE: &str = "trigger_type";
}

pub mod storage {
    /// e.g., "postgres", "redis", "filesystem"
    pub const BACKEND_TYPE: &str = "backend_type";
    /// e.g., "read", "write", "delete"
    pub const OPERATION_TYPE: &str = "operation_type";
}

pub mod system {
    pub const CPU_CORE: &str = "cpu_core";
    pub const MEMORY_TYPE: &str = "memory_type";
    pub const DISK_DEVICE: &str = "disk_device";
    pub const NETWORK_INTERFACE: &str = "network_interface";
    /// "rx" or "tx"
    pub const NETWORK_DIRECTION: &str = "network_direction";
}

pub mod monitoring {
    pub const COMPONENT_NAME: &str = "component_name";
    pub const EXPORTER_TYPE: &str = "exporter_type";
    pub const HEALTH_CHECK_NAME: &str = "health_check_name";
    pub const CIRCUIT_BREAKER_NAME: &str = "circuit_breaker_name";
    pub const ALERT_SEVERITY: &str = "alert_severity";
    pub const CONFIG_SECTION: &str = "config_section";
}
