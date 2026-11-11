//! Schedule execution service.
//!
//! When a cron schedule fires, this service starts the corresponding workflow
//! execution using the `task_template` stored in the schedule record.

use crate::engine::dispatch::PollingManager;
use crate::workflows::execution::{RunStart, RunTimeouts, WorkflowExecutionService};
use orcher_common::OrcherResult;
use orcher_storage::schema::types::Schedule;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Service responsible for executing scheduled workflow starts.
///
/// When the `ScheduleProcessor` fires a due schedule, this service:
/// 1. Extracts workflow configuration from the schedule's `task_template`
/// 2. Starts a new workflow execution via `WorkflowExecutionService`
/// 3. Notifies pollers via `PollingManager` so a worker picks it up
pub struct ScheduleExecutionService {
    workflow_execution_service: Arc<WorkflowExecutionService>,
    polling_manager: Arc<PollingManager>,
}

impl ScheduleExecutionService {
    /// Create a new schedule execution service.
    pub fn new(
        workflow_execution_service: Arc<WorkflowExecutionService>,
        polling_manager: Arc<PollingManager>,
    ) -> Self {
        Self {
            workflow_execution_service,
            polling_manager,
        }
    }

    /// Execute a schedule by starting the configured workflow.
    /// Returns the workflow execution ID for linking back to schedule_executions.
    pub async fn execute_schedule(&self, schedule: Schedule) -> OrcherResult<uuid::Uuid> {
        debug!(
            schedule_id = %schedule.id,
            schedule_name = %schedule.name,
            run_count = schedule.run_count,
            "Executing scheduled workflow"
        );

        // The run the schedule starts, as its start stored it.
        let run = run_start_of_template(&schedule.task_template, scheduled_workflow_id(&schedule));

        if run.workflow_name.is_empty() {
            error!(
                schedule_id = %schedule.id,
                schedule_name = %schedule.name,
                "Schedule task_template missing workflow_type"
            );
            return Err(orcher_common::error::OrcherError::Configuration {
                key: "task_template.workflow_type".to_string(),
                message: format!(
                    "Schedule '{}' has no workflow_type in task_template",
                    schedule.name
                ),
            });
        }
        let workflow_type = run.workflow_name.clone();
        let task_queue = run.task_queue.clone();
        let namespace = run.namespace.clone();

        // Start workflow execution
        let execution = self
            .workflow_execution_service
            .start_workflow(
                run,
                None, // No parent execution
                // Inherited from the schedule. A scheduled run belongs to
                // whoever owns the schedule; there is no request to read it
                // from, which is exactly why this is an argument rather than
                // something ambient.
                schedule.organization_id,
            )
            .await?;

        // Notify pollers so a worker picks up the new workflow
        self.polling_manager
            .notify_work_available(&task_queue, &namespace)
            .await;

        let execution_id_str = execution.execution_id;
        let execution_id = uuid::Uuid::parse_str(&execution_id_str).map_err(|e| {
            orcher_common::error::OrcherError::Internal {
                message: format!("Invalid execution_id UUID '{}': {}", execution_id_str, e),
                source: None,
            }
        })?;

        info!(
            schedule_id = %schedule.id,
            schedule_name = %schedule.name,
            execution_id = %execution_id,
            workflow_type = %workflow_type,
            "Scheduled workflow started successfully"
        );

        Ok(execution_id)
    }
}

/// The workflow id of the run a schedule starts at its fire time: the
/// schedule's id and the fire time it was due at. A run is then addressable by
/// id like any other, and one fire time names one run, so a schedule fired
/// twice for the same time collides on the id rather than starting twice.
fn scheduled_workflow_id(schedule: &Schedule) -> String {
    let fire_time = schedule
        .next_run_at
        .unwrap_or_else(orcher_common::types::time::Timestamptz::now);
    format!("{}@{}", schedule.id, fire_time.to_rfc3339())
}

/// The `task_template` a start on a cron schedule stores: the run each
/// firing starts, read back by [`run_start_of_template`]. The two are the
/// only writer and reader of the format, and between them carry every
/// setting of the run a start asked for but its workflow id, which each
/// firing makes its own. A schedule used to keep only the retry policy, so
/// a scheduled run had none of its start's labels or time limits.
pub fn schedule_template(run: &RunStart) -> serde_json::Value {
    let RunStart {
        workflow_name,
        // A scheduled run's version is the one every start writes.
        workflow_version: _,
        namespace,
        task_queue,
        input,
        // Each firing names its own run.
        workflow_id: _,
        labels,
        max_retries,
        retry_policy,
        timeouts,
        annotations,
        // A start on a schedule is refused a delay (`read_start_request`):
        // each firing is the time its run starts.
        start_delay: _,
        // The schedule is upserted under its name, which is what makes a
        // repeated cron start idempotent; each firing's run is the engine's,
        // not a request's.
        request_id: _,
    } = run;
    let RunTimeouts {
        execution_secs,
        run_secs,
        task_secs,
    } = timeouts;
    let mut template = serde_json::json!({
        "workflow_type": workflow_name,
        "task_queue": task_queue,
        "namespace": namespace,
        "input": input,
    });
    // Each optional setting only when set, so a schedule written before it
    // existed reads the same as one that did not ask for it.
    let mut set = |key: &str, value: Option<serde_json::Value>| {
        if let Some(value) = value {
            template[key] = value;
        }
    };
    set("max_retries", max_retries.map(Into::into));
    set("retry_policy", retry_policy.clone());
    set("labels", labels.clone());
    set("annotations", annotations.clone());
    set("execution_timeout_secs", execution_secs.map(Into::into));
    set("run_timeout_secs", run_secs.map(Into::into));
    set("task_timeout_secs", task_secs.map(Into::into));
    template
}

/// The run a schedule's `task_template` starts, under `workflow_id`; see
/// [`schedule_template`].
fn run_start_of_template(template: &serde_json::Value, workflow_id: String) -> RunStart {
    let text = |key: &str, default: &str| {
        template
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or(default)
            .to_string()
    };
    let int = |key: &str| {
        template
            .get(key)
            .and_then(|v| v.as_i64())
            .and_then(|n| i32::try_from(n).ok())
    };
    let value = |key: &str| template.get(key).filter(|v| !v.is_null()).cloned();
    RunStart {
        workflow_name: text("workflow_type", ""),
        workflow_version: "1.0".to_string(),
        namespace: text("namespace", "default"),
        task_queue: text("task_queue", "default"),
        input: value("input").unwrap_or(serde_json::Value::Null),
        workflow_id: Some(workflow_id),
        labels: value("labels"),
        max_retries: int("max_retries"),
        retry_policy: value("retry_policy"),
        timeouts: RunTimeouts {
            execution_secs: int("execution_timeout_secs"),
            run_secs: int("run_timeout_secs"),
            task_secs: int("task_timeout_secs"),
        },
        annotations: value("annotations"),
        start_delay: None,
        request_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scheduled_run_is_named_by_its_schedule_and_fire_time() {
        let mut schedule = Schedule::new(
            "nightly:0 3 * * *".to_string(),
            "0 3 * * *".to_string(),
            "UTC".to_string(),
            serde_json::json!({}),
        );
        schedule.next_run_at =
            orcher_common::types::time::Timestamptz::parse_rfc3339("2026-10-14T03:00:00Z").ok();
        assert_eq!(
            scheduled_workflow_id(&schedule),
            format!("{}@2026-10-14T03:00:00Z", schedule.id)
        );
    }

    /// Every setting a cron start stores is the one its runs start with.
    #[test]
    fn a_scheduled_run_starts_with_every_setting_its_start_stored() {
        let run = RunStart {
            workflow_name: "nightly".to_string(),
            workflow_version: "1.0".to_string(),
            namespace: "billing".to_string(),
            task_queue: "reports".to_string(),
            input: serde_json::json!({"day": 1}),
            workflow_id: Some("fired".to_string()),
            labels: Some(serde_json::json!({"team": "finance"})),
            max_retries: Some(2),
            retry_policy: Some(serde_json::json!({"backoff_multiplier": 2.0})),
            timeouts: RunTimeouts {
                execution_secs: Some(600),
                run_secs: Some(60),
                task_secs: Some(10),
            },
            annotations: Some(serde_json::json!({"owner": "finance-ops"})),
            start_delay: None,
            request_id: None,
        };
        assert_eq!(
            run_start_of_template(&schedule_template(&run), "fired".to_string()),
            run
        );
    }

    /// A schedule stored before it kept labels and time limits starts its
    /// runs with none.
    #[test]
    fn an_older_template_starts_runs_without_limits() {
        let run = run_start_of_template(
            &serde_json::json!({"workflow_type": "nightly", "task_queue": "q", "namespace": "n"}),
            "fired".to_string(),
        );
        assert_eq!(run.timeouts, RunTimeouts::default());
        assert_eq!(
            (run.labels, run.annotations, run.max_retries),
            (None, None, None)
        );
    }

    #[test]
    fn test_schedule_execution_service_is_send_sync() {
        fn _is_send<T: Send>() {}
        fn _is_sync<T: Sync>() {}
        _is_send::<super::ScheduleExecutionService>();
        _is_sync::<super::ScheduleExecutionService>();
    }
}
