//! Schedule Models
//!
//! Request and response types for schedule management REST API endpoints.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Request to create a new schedule
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateScheduleRequest {
    /// Unique schedule name
    pub name: String,

    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Cron expression (e.g. "0 0 * * *" for daily at midnight)
    pub cron_expression: String,

    /// IANA timezone (e.g. "UTC", "America/New_York")
    #[serde(default = "default_timezone")]
    pub timezone: String,

    /// Workflow configuration to execute on each trigger
    pub task_template: TaskTemplateRequest,

    /// Maximum concurrent instances (default: 1)
    #[serde(default = "default_max_instances")]
    pub max_instances: i32,

    /// Overlap policy when previous run is still active
    #[serde(default)]
    pub overlap_policy: OverlapPolicyDto,

    /// Optional timeout per execution in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,

    /// Optional start date (schedule inactive before this)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<Timestamptz>,

    /// Optional end date (schedule completes after this)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<Timestamptz>,

    /// Optional maximum number of runs (unlimited if not set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_runs: Option<i64>,

    /// Optional tags for filtering
    #[serde(default)]
    pub tags: Vec<String>,

    /// Optional metadata
    #[serde(default = "default_metadata")]
    pub metadata: JsonValue,
}

/// Workflow configuration within a schedule
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskTemplateRequest {
    /// Workflow type to execute
    pub workflow_type: String,

    /// Task queue (default: "default")
    #[serde(default = "default_task_queue")]
    pub task_queue: String,

    /// Namespace (default: "default")
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Input payload for the workflow
    #[serde(default)]
    pub input: JsonValue,
}

/// Request to update an existing schedule
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateScheduleRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron_expression: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_template: Option<TaskTemplateRequest>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_instances: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub overlap_policy: Option<OverlapPolicyDto>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<Timestamptz>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<Timestamptz>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_runs: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
}

/// Schedule response (returned from GET/POST/PUT)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleResponse {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub cron_expression: String,
    pub timezone: String,
    pub enabled: bool,
    pub task_template: JsonValue,
    pub max_instances: i32,
    pub overlap_policy: OverlapPolicyDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<Timestamptz>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<Timestamptz>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_runs: Option<i64>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<Timestamptz>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<Timestamptz>,
    pub run_count: i64,
    pub success_count: i64,
    pub failure_count: i64,
    pub tags: Vec<String>,
    pub metadata: JsonValue,
    pub created_at: Timestamptz,
    pub updated_at: Timestamptz,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
}

/// Paginated list of schedules
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSchedulesResponse {
    pub schedules: Vec<ScheduleResponse>,
    pub total: u64,
    pub count: u32,
    pub offset: u32,
}

/// Overlap policy DTO matching the storage enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OverlapPolicyDto {
    #[default]
    Skip,
    Queue,
    Replace,
    Parallel,
}

/// Simple message response for pause/resume/delete
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleActionResponse {
    pub id: String,
    pub status: String,
    pub message: String,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_max_instances() -> i32 {
    1
}

fn default_task_queue() -> String {
    "default".to_string()
}

fn default_namespace() -> String {
    "default".to_string()
}

fn default_metadata() -> JsonValue {
    serde_json::json!({})
}
