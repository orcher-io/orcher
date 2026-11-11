//! Schedule Service Layer
//!
//! Business logic for schedule CRUD, pause/resume, and listing.
//! Talks directly to the ScheduleRepository (same pattern as WorkflowService).

use crate::api::rest::v1::models::schedules::{
    CreateScheduleRequest, ListSchedulesResponse, OverlapPolicyDto, ScheduleActionResponse,
    ScheduleResponse, UpdateScheduleRequest,
};
use crate::error::{Result, ServerError};
use orcher_common::types::identifiers::ScheduleId;
use orcher_storage::repositories::base::CrudOperations;
use orcher_storage::repositories::domains::schedule::ScheduleRepository;
use orcher_storage::schema::types::{OverlapPolicy, Schedule, ScheduleStatus};
use orcher_storage::traits::domain::ScheduleRepositoryTrait;
use std::sync::Arc;
use tracing::{debug, info};

pub struct ScheduleService {
    schedule_repository: Arc<ScheduleRepository>,
}

impl ScheduleService {
    pub fn new(schedule_repository: Arc<ScheduleRepository>) -> Self {
        Self {
            schedule_repository,
        }
    }

    pub async fn create_schedule(&self, req: CreateScheduleRequest) -> Result<ScheduleResponse> {
        // Validate
        if req.name.is_empty() {
            return Err(ServerError::validation("Schedule name cannot be empty"));
        }
        if req.cron_expression.is_empty() {
            return Err(ServerError::validation("Cron expression cannot be empty"));
        }
        if req.task_template.workflow_type.is_empty() {
            return Err(ServerError::validation(
                "task_template.workflow_type cannot be empty",
            ));
        }

        let task_template = serde_json::json!({
            "workflow_type": req.task_template.workflow_type,
            "task_queue": req.task_template.task_queue,
            "namespace": req.task_template.namespace,
            "input": req.task_template.input,
        });

        let mut schedule =
            Schedule::new(req.name, req.cron_expression, req.timezone, task_template);
        // Stamp the owner, or the row belongs to nobody and every scoped read
        // of it — including the listing this service already filters — hides a
        // schedule its own creator just made.
        schedule.organization_id = orcher_core::grpc::tenant::current_organization();
        schedule.description = req.description;
        schedule.max_instances = req.max_instances;
        schedule.overlap_policy = overlap_from_dto(req.overlap_policy);
        schedule.timeout_seconds = req.timeout_seconds;
        schedule.start_date = req.start_date;
        schedule.end_date = req.end_date;
        schedule.max_runs = req.max_runs;
        schedule.tags = req.tags;
        schedule.metadata = req.metadata;

        // Validate the cron and compute the first run. Reject an unparsable
        // expression (400) rather than silently creating a schedule that never
        // fires; a valid cron with no upcoming run is allowed.
        match orcher_core::engine::cron::next_fire_time(
            &schedule.cron_expression,
            &schedule.timezone,
            orcher_common::types::time::Timestamptz::now(),
        ) {
            Ok(next) => schedule.next_run_at = next,
            Err(e) => {
                return Err(ServerError::validation(format!(
                    "Invalid cron expression '{}': {}",
                    schedule.cron_expression, e
                )));
            }
        }

        let created = self
            .schedule_repository
            .create(schedule)
            .await
            .map_err(|e| ServerError::internal(format!("Failed to create schedule: {e}")))?;

        info!(schedule_id = %created.id, name = %created.name, "Schedule created");
        Ok(schedule_to_response(created))
    }

    /// Fetch a schedule the caller is entitled to act on, or report it missing.
    ///
    /// Every by-id entry point goes through here. `list_schedules` was filtered
    /// and `create_schedule` stamps the owner, but each of get, update, delete,
    /// pause and resume took an id and acted on whatever row it named — so a
    /// schedule id was sufficient authority over another organisation's
    /// schedule, up to and including rewriting its `task_template`, which
    /// decides what workflow runs and with what input.
    ///
    /// A row that is not the caller's reads as absent rather than forbidden.
    /// The two answers have to be indistinguishable, or the error itself
    /// confirms the id exists and turns a guess into an enumeration.
    async fn owned_schedule(&self, id: &str) -> Result<Schedule> {
        let schedule_id = parse_schedule_id(id)?;
        self.schedule_repository
            .find_by_id(schedule_id)
            .await
            .map_err(|e| ServerError::internal(format!("Failed to fetch schedule: {e}")))?
            .filter(|schedule| {
                orcher_core::grpc::tenant::may_act_on(
                    schedule.organization_id,
                    orcher_core::grpc::tenant::current_organization(),
                )
            })
            .ok_or_else(|| ServerError::not_found(format!("Schedule not found: {id}")))
    }

    pub async fn get_schedule(&self, id: &str) -> Result<ScheduleResponse> {
        Ok(schedule_to_response(self.owned_schedule(id).await?))
    }

    pub async fn update_schedule(
        &self,
        id: &str,
        req: UpdateScheduleRequest,
    ) -> Result<ScheduleResponse> {
        let mut schedule = self.owned_schedule(id).await?;

        // Apply partial updates
        if let Some(desc) = req.description {
            schedule.description = Some(desc);
        }
        if let Some(cron) = req.cron_expression {
            if cron.is_empty() {
                return Err(ServerError::validation("Cron expression cannot be empty"));
            }
            schedule.cron_expression = cron;
        }
        if let Some(tz) = req.timezone {
            schedule.timezone = tz;
        }
        if let Some(enabled) = req.enabled {
            schedule.enabled = enabled;
        }
        if let Some(tmpl) = req.task_template {
            schedule.task_template = serde_json::json!({
                "workflow_type": tmpl.workflow_type,
                "task_queue": tmpl.task_queue,
                "namespace": tmpl.namespace,
                "input": tmpl.input,
            });
        }
        if let Some(max) = req.max_instances {
            schedule.max_instances = max;
        }
        if let Some(policy) = req.overlap_policy {
            schedule.overlap_policy = overlap_from_dto(policy);
        }
        if let Some(timeout) = req.timeout_seconds {
            schedule.timeout_seconds = Some(timeout);
        }
        if let Some(start) = req.start_date {
            schedule.start_date = Some(start);
        }
        if let Some(end) = req.end_date {
            schedule.end_date = Some(end);
        }
        if let Some(max_runs) = req.max_runs {
            schedule.max_runs = Some(max_runs);
        }
        if let Some(tags) = req.tags {
            schedule.tags = tags;
        }
        if let Some(metadata) = req.metadata {
            schedule.metadata = metadata;
        }

        // Recompute next_run_at if cron or timezone changed
        if let Some(next) = compute_next_run(&schedule.cron_expression, &schedule.timezone) {
            schedule.next_run_at = Some(next);
        }

        let updated = self
            .schedule_repository
            .update(schedule)
            .await
            .map_err(|e| ServerError::internal(format!("Failed to update schedule: {e}")))?;

        info!(schedule_id = %updated.id, "Schedule updated");
        Ok(schedule_to_response(updated))
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<ScheduleActionResponse> {
        let schedule_id = self.owned_schedule(id).await?.id;
        let deleted = self
            .schedule_repository
            .delete(schedule_id)
            .await
            .map_err(|e| ServerError::internal(format!("Failed to delete schedule: {e}")))?;

        if !deleted {
            return Err(ServerError::not_found(format!("Schedule not found: {id}")));
        }

        info!(schedule_id = %id, "Schedule deleted");
        Ok(ScheduleActionResponse {
            id: id.to_string(),
            status: "deleted".to_string(),
            message: "Schedule deleted successfully".to_string(),
        })
    }

    pub async fn list_schedules(
        &self,
        status: Option<String>,
        name: Option<String>,
        limit: u32,
        offset: u32,
    ) -> Result<ListSchedulesResponse> {
        debug!(status = ?status, name = ?name, limit, offset, "Listing schedules");

        // Use appropriate repository query based on filters
        let all_schedules = if let Some(ref name_pattern) = name {
            self.schedule_repository
                .find_by_name_pattern(name_pattern)
                .await
                .map_err(|e| ServerError::internal(format!("Failed to list schedules: {e}")))?
        } else if let Some(ref status_str) = status {
            let sched_status = parse_schedule_status(status_str)?;
            self.schedule_repository
                .find_by_status(sched_status)
                .await
                .map_err(|e| ServerError::internal(format!("Failed to list schedules: {e}")))?
        } else {
            self.schedule_repository
                .find_all_schedules()
                .await
                .map_err(|e| ServerError::internal(format!("Failed to list schedules: {e}")))?
        };

        // Only this caller's schedules.
        //
        // The repository has no organisation-filtered query and the service
        // pages in memory, so the scoping happens here — before the count, so
        // the total describes what the caller can actually see rather than the
        // whole deployment. None means nobody claimed an organisation, which is
        // a single-tenant deployment and stays unscoped.
        let organization_id = orcher_core::grpc::tenant::current_organization();
        let all_schedules: Vec<_> = all_schedules
            .into_iter()
            .filter(|schedule| {
                orcher_core::grpc::tenant::may_act_on(schedule.organization_id, organization_id)
            })
            .collect();

        let total = all_schedules.len() as u64;
        let page: Vec<ScheduleResponse> = all_schedules
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .map(schedule_to_response)
            .collect();
        let count = page.len() as u32;

        Ok(ListSchedulesResponse {
            schedules: page,
            total,
            count,
            offset,
        })
    }

    pub async fn pause_schedule(&self, id: &str) -> Result<ScheduleActionResponse> {
        let schedule_id = self.owned_schedule(id).await?.id;

        self.schedule_repository
            .update_status(schedule_id, ScheduleStatus::Paused)
            .await
            .map_err(|e| ServerError::internal(format!("Failed to pause schedule: {e}")))?;

        info!(schedule_id = %id, "Schedule paused");
        Ok(ScheduleActionResponse {
            id: id.to_string(),
            status: "paused".to_string(),
            message: "Schedule paused successfully".to_string(),
        })
    }

    pub async fn resume_schedule(&self, id: &str) -> Result<ScheduleActionResponse> {
        let schedule_id = self.owned_schedule(id).await?.id;

        self.schedule_repository
            .update_status(schedule_id, ScheduleStatus::Active)
            .await
            .map_err(|e| ServerError::internal(format!("Failed to resume schedule: {e}")))?;

        // Re-enable the schedule as well
        self.schedule_repository
            .enable_schedule(schedule_id)
            .await
            .map_err(|e| ServerError::internal(format!("Failed to enable schedule: {e}")))?;

        info!(schedule_id = %id, "Schedule resumed");
        Ok(ScheduleActionResponse {
            id: id.to_string(),
            status: "active".to_string(),
            message: "Schedule resumed successfully".to_string(),
        })
    }
}

// --- Conversion helpers ---

fn schedule_to_response(s: Schedule) -> ScheduleResponse {
    ScheduleResponse {
        id: s.id.to_string(),
        name: s.name,
        description: s.description,
        cron_expression: s.cron_expression,
        timezone: s.timezone,
        enabled: s.enabled,
        task_template: s.task_template,
        max_instances: s.max_instances,
        overlap_policy: overlap_to_dto(s.overlap_policy),
        timeout_seconds: s.timeout_seconds,
        start_date: s.start_date,
        end_date: s.end_date,
        max_runs: s.max_runs,
        status: format!("{:?}", s.status).to_lowercase(),
        next_run_at: s.next_run_at,
        last_run_at: s.last_run_at,
        run_count: s.run_count,
        success_count: s.success_count,
        failure_count: s.failure_count,
        tags: s.tags,
        metadata: s.metadata,
        created_at: s.created_at,
        updated_at: s.updated_at,
        created_by: s.created_by,
    }
}

fn overlap_from_dto(dto: OverlapPolicyDto) -> OverlapPolicy {
    match dto {
        OverlapPolicyDto::Skip => OverlapPolicy::Skip,
        OverlapPolicyDto::Queue => OverlapPolicy::Queue,
        OverlapPolicyDto::Replace => OverlapPolicy::Replace,
        OverlapPolicyDto::Parallel => OverlapPolicy::Parallel,
    }
}

fn overlap_to_dto(policy: OverlapPolicy) -> OverlapPolicyDto {
    match policy {
        OverlapPolicy::Skip => OverlapPolicyDto::Skip,
        OverlapPolicy::Queue => OverlapPolicyDto::Queue,
        OverlapPolicy::Replace => OverlapPolicyDto::Replace,
        OverlapPolicy::Parallel => OverlapPolicyDto::Parallel,
    }
}

fn parse_schedule_id(id: &str) -> Result<ScheduleId> {
    let raw = id.strip_prefix("sch_").unwrap_or(id);
    let uuid = uuid::Uuid::parse_str(raw)
        .map_err(|_| ServerError::validation(format!("Invalid schedule ID: {id}")))?;
    Ok(ScheduleId::from(uuid))
}

fn parse_schedule_status(status: &str) -> Result<ScheduleStatus> {
    match status.to_lowercase().as_str() {
        "active" => Ok(ScheduleStatus::Active),
        "paused" => Ok(ScheduleStatus::Paused),
        "disabled" => Ok(ScheduleStatus::Disabled),
        "completed" => Ok(ScheduleStatus::Completed),
        "failed" => Ok(ScheduleStatus::Failed),
        "processing" => Ok(ScheduleStatus::Processing),
        _ => Err(ServerError::validation(format!(
            "Invalid schedule status: {status}. Valid values: active, paused, disabled, completed, failed, processing"
        ))),
    }
}

/// Compute the next run time from a cron expression and timezone.
/// Returns None if parsing fails (caller should handle gracefully).
fn compute_next_run(
    cron_expr: &str,
    timezone: &str,
) -> Option<orcher_common::types::time::Timestamptz> {
    let now = orcher_common::types::time::Timestamptz::now();
    orcher_core::engine::cron::next_fire_time(cron_expr, timezone, now)
        .ok()
        .flatten()
}

// The ownership predicate this module used to define itself now lives in
// `orcher_core::grpc::tenant::may_act_on`, which was byte-identical to it. Two
// copies of one rule is a rule that will eventually be changed in one place,
// and the tests that covered this copy are in `tenant.rs` beside the survivor.
