//! # Schedule Domain Repository
//!
//! Schedule persistence implementation using PostgreSQL backend.
//! Handles CRUD operations, schedule claiming for distributed processing,
//! and schedule-specific queries.

use async_trait::async_trait;
use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_common::{types::identifiers::ScheduleId, OrcherResult};
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::{
    repositories::base::CrudOperations,
    schema::types::{Schedule, ScheduleStatus},
    traits::{core::Repository, domain::ScheduleRepositoryTrait},
};

/// All columns for SELECT queries on the schedules table.
const SCHEDULE_COLUMNS: &str = r#"
    id, organization_id, name, description, cron_expression, timezone, enabled,
    task_template, max_instances, overlap_policy, timeout_seconds, retry_policy,
    start_date, end_date, max_runs, status, next_run_at, last_run_at,
    run_count, success_count, failure_count, tags, metadata, created_at, updated_at, created_by
"#;

/// PostgreSQL implementation of ScheduleRepository
#[derive(Debug, Clone)]
pub struct ScheduleRepository {
    pool: PgPool,
}

impl ScheduleRepository {
    /// Create a new ScheduleRepository instance without caching
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Generate cache key for a schedule ID
    #[allow(dead_code)]
    fn cache_key(id: &ScheduleId) -> String {
        format!("schedule:{}", id)
    }

    // ========================================================================
    // Schedule claiming (FOR UPDATE SKIP LOCKED)
    // ========================================================================

    /// Claim due schedules for processing using FOR UPDATE SKIP LOCKED.
    ///
    /// Atomically transitions active, enabled schedules whose `next_run_at` has
    /// passed to `processing` status. Multiple orchestrator instances can safely
    /// call this concurrently — each will claim a disjoint set of schedules.
    pub async fn claim_ready_schedules(&self, batch_size: usize) -> OrcherResult<Vec<Schedule>> {
        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            r#"
            UPDATE schedules
            SET status = 'processing', updated_at = NOW()
            WHERE id IN (
                SELECT id FROM schedules
                WHERE enabled = true
                  AND status = 'active'
                  AND next_run_at IS NOT NULL
                  AND next_run_at <= NOW()
                ORDER BY next_run_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING {SCHEDULE_COLUMNS}
            "#
        ))
        .bind(batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to claim ready schedules: {}",
                e
            ))
        })?;

        if !schedules.is_empty() {
            debug!(count = schedules.len(), "Claimed schedules for processing");
        }

        Ok(schedules)
    }

    /// Get the earliest next_run_at across all active schedules.
    ///
    /// Used by the processor to determine how long to sleep before the next check.
    pub async fn get_next_fire_time(&self) -> OrcherResult<Option<Timestamptz>> {
        let next: Option<Timestamptz> = sqlx::query_scalar(
            r#"
            SELECT MIN(next_run_at)
            FROM schedules
            WHERE enabled = true
              AND status = 'active'
              AND next_run_at IS NOT NULL
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to get next fire time: {}",
                e
            ))
        })?;

        Ok(next)
    }

    /// Reset schedules stuck in `processing` state for longer than the threshold.
    ///
    /// This handles the case where a processor crashes after claiming a schedule
    /// but before completing it. Resets them to `active` for re-claiming.
    pub async fn reset_stuck_schedules(
        &self,
        stuck_threshold: SignedDuration,
    ) -> OrcherResult<i64> {
        let stuck_time = Timestamptz::now() - stuck_threshold;

        let result = sqlx::query(
            r#"
            UPDATE schedules
            SET status = 'active', updated_at = NOW()
            WHERE status = 'processing'
              AND updated_at < $1
            "#,
        )
        .bind(stuck_time)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to reset stuck schedules: {}",
                e
            ))
        })?;

        let count = result.rows_affected() as i64;
        if count > 0 {
            info!(count, "Reset stuck schedules back to active");
        }

        Ok(count)
    }

    /// Upsert a schedule by name — used for SDK-driven registration.
    ///
    /// If a schedule with the given name already exists, updates the cron expression,
    /// timezone, overlap policy, and other configuration. Preserves run statistics.
    pub async fn upsert_by_name(&self, entity: Schedule) -> OrcherResult<Schedule> {
        debug!("Upserting schedule by name: {}", entity.name);

        let schedule = sqlx::query_as::<_, Schedule>(&format!(
            r#"
            INSERT INTO schedules (
                id, organization_id, name, description, cron_expression, timezone, enabled,
                task_template, max_instances, overlap_policy, timeout_seconds, retry_policy,
                start_date, end_date, max_runs, status, next_run_at, last_run_at,
                run_count, success_count, failure_count, tags, metadata, created_at, updated_at, created_by
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
            -- Keyed by owner and name together: the name is unique within an
            -- organisation, not across the deployment, so this updates the
            -- caller's schedule rather than whichever tenant got there first.
            ON CONFLICT (organization_id, name) DO UPDATE SET
                cron_expression = EXCLUDED.cron_expression,
                timezone = EXCLUDED.timezone,
                enabled = EXCLUDED.enabled,
                task_template = EXCLUDED.task_template,
                max_instances = EXCLUDED.max_instances,
                overlap_policy = EXCLUDED.overlap_policy,
                timeout_seconds = EXCLUDED.timeout_seconds,
                retry_policy = EXCLUDED.retry_policy,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                max_runs = EXCLUDED.max_runs,
                description = EXCLUDED.description,
                tags = EXCLUDED.tags,
                metadata = EXCLUDED.metadata,
                updated_at = NOW()
            RETURNING {SCHEDULE_COLUMNS}
            "#
        ))
        .bind(entity.id.as_uuid())
        .bind(entity.organization_id)
        .bind(&entity.name)
        .bind(&entity.description)
        .bind(&entity.cron_expression)
        .bind(&entity.timezone)
        .bind(entity.enabled)
        .bind(&entity.task_template)
        .bind(entity.max_instances)
        .bind(entity.overlap_policy)
        .bind(entity.timeout_seconds)
        .bind(&entity.retry_policy)
        .bind(entity.start_date)
        .bind(entity.end_date)
        .bind(entity.max_runs)
        .bind(entity.status)
        .bind(entity.next_run_at)
        .bind(entity.last_run_at)
        .bind(entity.run_count)
        .bind(entity.success_count)
        .bind(entity.failure_count)
        .bind(&entity.tags)
        .bind(&entity.metadata)
        .bind(entity.created_at)
        .bind(entity.updated_at)
        .bind(&entity.created_by)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to upsert schedule: {}",
                e
            ))
        })?;

        debug!("Schedule upserted successfully: name={}", schedule.name);
        Ok(schedule)
    }

    /// Mark a schedule as fired: update last_run_at, next_run_at, increment run_count,
    /// and reset status back to active in a single atomic operation.
    pub async fn mark_fired(
        &self,
        schedule_id: ScheduleId,
        next_run_at: Option<Timestamptz>,
    ) -> OrcherResult<()> {
        let now = Timestamptz::now();

        let result = sqlx::query(
            r#"
            UPDATE schedules
            SET status = 'active',
                last_run_at = $1,
                next_run_at = $2,
                run_count = run_count + 1,
                updated_at = $1
            WHERE id = $3
            "#,
        )
        .bind(now)
        .bind(next_run_at)
        .bind(schedule_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to mark schedule as fired: {}",
                e
            ))
        })?;

        if result.rows_affected() == 0 {
            warn!(schedule_id = %schedule_id, "Schedule not found when marking as fired");
        }

        Ok(())
    }

    /// Count active instances for a schedule (for overlap policy checks).
    pub async fn count_active_instances(&self, schedule_id: ScheduleId) -> OrcherResult<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM schedule_instances WHERE schedule_id = $1 AND status = 'running'",
        )
        .bind(schedule_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to count active instances: {}",
                e
            ))
        })?;

        Ok(count)
    }

    /// Insert a schedule execution record.
    pub async fn insert_execution(
        &self,
        schedule_id: ScheduleId,
        execution_number: i64,
        scheduled_at: Timestamptz,
        workflow_execution_id: Option<uuid::Uuid>,
    ) -> OrcherResult<uuid::Uuid> {
        let id = uuid::Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO schedule_executions (id, schedule_id, task_id, execution_number, scheduled_at, status, trigger_type)
            VALUES ($1, $2, $3, $4, $5, 'running', 'cron')
            "#,
        )
        .bind(id)
        .bind(schedule_id.as_uuid())
        .bind(workflow_execution_id)
        .bind(execution_number)
        .bind(scheduled_at)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to insert schedule execution: {}",
                e
            ))
        })?;

        Ok(id)
    }
}

#[async_trait]
impl CrudOperations<Schedule, ScheduleId> for ScheduleRepository {
    async fn create(&self, entity: Schedule) -> OrcherResult<Schedule> {
        debug!("Creating schedule: id={}, name={}", entity.id, entity.name);

        let result = sqlx::query(
            r#"
            INSERT INTO schedules (id, organization_id, name, description, cron_expression, timezone, enabled,
                task_template, max_instances, overlap_policy, timeout_seconds, retry_policy,
                start_date, end_date, max_runs, status, next_run_at, last_run_at,
                run_count, success_count, failure_count, tags, metadata, created_at, updated_at, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
            "#,
        )
        .bind(entity.id.as_uuid())
        .bind(entity.organization_id)
        .bind(&entity.name)
        .bind(&entity.description)
        .bind(&entity.cron_expression)
        .bind(&entity.timezone)
        .bind(entity.enabled)
        .bind(&entity.task_template)
        .bind(entity.max_instances)
        .bind(entity.overlap_policy)
        .bind(entity.timeout_seconds)
        .bind(&entity.retry_policy)
        .bind(entity.start_date)
        .bind(entity.end_date)
        .bind(entity.max_runs)
        .bind(entity.status)
        .bind(entity.next_run_at)
        .bind(entity.last_run_at)
        .bind(entity.run_count)
        .bind(entity.success_count)
        .bind(entity.failure_count)
        .bind(&entity.tags)
        .bind(&entity.metadata)
        .bind(entity.created_at)
        .bind(entity.updated_at)
        .bind(&entity.created_by)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to create schedule: {}", e))
        })?;

        if result.rows_affected() > 0 {
            debug!("Schedule created successfully: id={}", entity.id);
            Ok(entity)
        } else {
            Err(orcher_common::error::OrcherError::internal(
                "Failed to create schedule: no rows affected".to_string(),
            ))
        }
    }

    async fn find_by_id(&self, id: ScheduleId) -> OrcherResult<Option<Schedule>> {
        debug!("Finding schedule by id: {}", id);

        let schedule = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE id = $1"
        ))
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find schedule by id: {}",
                e
            ))
        })?;

        Ok(schedule)
    }

    async fn update(&self, entity: Schedule) -> OrcherResult<Schedule> {
        debug!("Updating schedule: id={}, name={}", entity.id, entity.name);

        let result = sqlx::query(
            r#"
            UPDATE schedules
            SET name = $2, description = $3, cron_expression = $4, timezone = $5, enabled = $6,
                task_template = $7, max_instances = $8, overlap_policy = $9, timeout_seconds = $10,
                retry_policy = $11, start_date = $12, end_date = $13, max_runs = $14, status = $15,
                next_run_at = $16, last_run_at = $17, run_count = $18, success_count = $19,
                failure_count = $20, tags = $21, metadata = $22, updated_at = $23, created_by = $24
            WHERE id = $1
            "#,
        )
        .bind(entity.id.as_uuid())
        .bind(&entity.name)
        .bind(&entity.description)
        .bind(&entity.cron_expression)
        .bind(&entity.timezone)
        .bind(entity.enabled)
        .bind(&entity.task_template)
        .bind(entity.max_instances)
        .bind(entity.overlap_policy)
        .bind(entity.timeout_seconds)
        .bind(&entity.retry_policy)
        .bind(entity.start_date)
        .bind(entity.end_date)
        .bind(entity.max_runs)
        .bind(entity.status)
        .bind(entity.next_run_at)
        .bind(entity.last_run_at)
        .bind(entity.run_count)
        .bind(entity.success_count)
        .bind(entity.failure_count)
        .bind(&entity.tags)
        .bind(&entity.metadata)
        .bind(Timestamptz::now())
        .bind(&entity.created_by)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to update schedule: {}", e))
        })?;

        if result.rows_affected() > 0 {
            debug!("Schedule updated successfully: id={}", entity.id);
            Ok(entity)
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                entity.id.to_string(),
            ))
        }
    }

    async fn delete(&self, id: ScheduleId) -> OrcherResult<bool> {
        debug!("Deleting schedule: id={}", id);

        let result = sqlx::query("DELETE FROM schedules WHERE id = $1")
            .bind(id.as_uuid())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to delete schedule: {}",
                    e
                ))
            })?;

        Ok(result.rows_affected() > 0)
    }

    async fn exists(&self, id: ScheduleId) -> OrcherResult<bool> {
        debug!("Checking if schedule exists: id={}", id);

        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schedules WHERE id = $1)")
                .bind(id.as_uuid())
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to check schedule existence: {}",
                        e
                    ))
                })?;

        Ok(exists)
    }
}

#[async_trait]
impl Repository<Schedule> for ScheduleRepository {
    type Id = ScheduleId;

    async fn create_validated(&self, entity: Schedule) -> OrcherResult<Schedule> {
        CrudOperations::create(self, entity).await
    }

    async fn find_by_id(&self, id: Self::Id) -> OrcherResult<Option<Schedule>> {
        CrudOperations::find_by_id(self, id).await
    }

    async fn update_validated(&self, entity: Schedule) -> OrcherResult<Schedule> {
        CrudOperations::update(self, entity).await
    }

    async fn delete(&self, id: Self::Id) -> OrcherResult<bool> {
        CrudOperations::delete(self, id).await
    }
}

#[async_trait]
impl ScheduleRepositoryTrait for ScheduleRepository {
    async fn find_all_schedules(&self) -> OrcherResult<Vec<Schedule>> {
        debug!("Finding all schedules");

        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules ORDER BY created_at DESC"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find all schedules: {}",
                e
            ))
        })?;

        debug!("Found {} schedules", schedules.len());
        Ok(schedules)
    }

    async fn find_by_status(&self, status: ScheduleStatus) -> OrcherResult<Vec<Schedule>> {
        debug!("Finding schedules by status: {:?}", status);

        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE status = $1"
        ))
        .bind(status)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find schedules by status: {}",
                e
            ))
        })?;

        debug!(
            "Found {} schedules with status {:?}",
            schedules.len(),
            status
        );
        Ok(schedules)
    }

    async fn find_ready_to_run(&self) -> OrcherResult<Vec<Schedule>> {
        debug!("Finding schedules ready to run");

        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            r#"
            SELECT {SCHEDULE_COLUMNS} FROM schedules
            WHERE enabled = true
              AND status = 'active'
              AND next_run_at IS NOT NULL
              AND next_run_at <= $1
            ORDER BY next_run_at ASC
            "#
        ))
        .bind(Timestamptz::now())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find ready schedules: {}",
                e
            ))
        })?;

        debug!("Found {} schedules ready to run", schedules.len());
        Ok(schedules)
    }

    async fn find_by_name_pattern(&self, pattern: &str) -> OrcherResult<Vec<Schedule>> {
        debug!("Finding schedules by name pattern: {}", pattern);

        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE name ILIKE $1"
        ))
        .bind(format!("%{}%", pattern))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find schedules by name pattern: {}",
                e
            ))
        })?;

        Ok(schedules)
    }

    async fn update_status(
        &self,
        schedule_id: ScheduleId,
        status: ScheduleStatus,
    ) -> OrcherResult<()> {
        debug!("Updating schedule {} status to {:?}", schedule_id, status);

        let result = sqlx::query("UPDATE schedules SET status = $1, updated_at = $2 WHERE id = $3")
            .bind(status)
            .bind(Timestamptz::now())
            .bind(schedule_id.as_uuid())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to update schedule status: {}",
                    e
                ))
            })?;

        if result.rows_affected() > 0 {
            debug!("Schedule status updated successfully: id={}", schedule_id);
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                schedule_id.to_string(),
            ))
        }
    }

    async fn update_next_run(
        &self,
        schedule_id: ScheduleId,
        next_run_at: Timestamptz,
    ) -> OrcherResult<()> {
        debug!(
            "Updating schedule {} next_run_at to {}",
            schedule_id, next_run_at
        );

        let result =
            sqlx::query("UPDATE schedules SET next_run_at = $1, updated_at = $2 WHERE id = $3")
                .bind(next_run_at)
                .bind(Timestamptz::now())
                .bind(schedule_id.as_uuid())
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to update schedule next_run_at: {}",
                        e
                    ))
                })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                schedule_id.to_string(),
            ))
        }
    }

    async fn update_last_run(
        &self,
        schedule_id: ScheduleId,
        last_run_at: Timestamptz,
    ) -> OrcherResult<()> {
        debug!(
            "Updating schedule {} last_run_at to {}",
            schedule_id, last_run_at
        );

        let result =
            sqlx::query("UPDATE schedules SET last_run_at = $1, updated_at = $2 WHERE id = $3")
                .bind(last_run_at)
                .bind(Timestamptz::now())
                .bind(schedule_id.as_uuid())
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to update schedule last_run_at: {}",
                        e
                    ))
                })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                schedule_id.to_string(),
            ))
        }
    }

    async fn increment_run_count(&self, schedule_id: ScheduleId) -> OrcherResult<()> {
        debug!("Incrementing run count for schedule: {}", schedule_id);

        let result = sqlx::query(
            "UPDATE schedules SET run_count = run_count + 1, updated_at = $1 WHERE id = $2",
        )
        .bind(Timestamptz::now())
        .bind(schedule_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to increment run count: {}",
                e
            ))
        })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                schedule_id.to_string(),
            ))
        }
    }

    async fn increment_success_count(&self, schedule_id: ScheduleId) -> OrcherResult<()> {
        debug!("Incrementing success count for schedule: {}", schedule_id);

        let result = sqlx::query(
            "UPDATE schedules SET success_count = success_count + 1, updated_at = $1 WHERE id = $2",
        )
        .bind(Timestamptz::now())
        .bind(schedule_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to increment success count: {}",
                e
            ))
        })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                schedule_id.to_string(),
            ))
        }
    }

    async fn increment_failure_count(&self, schedule_id: ScheduleId) -> OrcherResult<()> {
        debug!("Incrementing failure count for schedule: {}", schedule_id);

        let result = sqlx::query(
            "UPDATE schedules SET failure_count = failure_count + 1, updated_at = $1 WHERE id = $2",
        )
        .bind(Timestamptz::now())
        .bind(schedule_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to increment failure count: {}",
                e
            ))
        })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                schedule_id.to_string(),
            ))
        }
    }

    async fn enable_schedule(&self, schedule_id: ScheduleId) -> OrcherResult<()> {
        debug!("Enabling schedule: {}", schedule_id);

        let result =
            sqlx::query("UPDATE schedules SET enabled = true, updated_at = $1 WHERE id = $2")
                .bind(Timestamptz::now())
                .bind(schedule_id.as_uuid())
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to enable schedule: {}",
                        e
                    ))
                })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                schedule_id.to_string(),
            ))
        }
    }

    async fn disable_schedule(&self, schedule_id: ScheduleId) -> OrcherResult<()> {
        debug!("Disabling schedule: {}", schedule_id);

        let result =
            sqlx::query("UPDATE schedules SET enabled = false, updated_at = $1 WHERE id = $2")
                .bind(Timestamptz::now())
                .bind(schedule_id.as_uuid())
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to disable schedule: {}",
                        e
                    ))
                })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                schedule_id.to_string(),
            ))
        }
    }

    async fn find_by_tag(&self, tag: &str) -> OrcherResult<Vec<Schedule>> {
        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE $1 = ANY(tags)"
        ))
        .bind(tag)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find schedules by tag: {}",
                e
            ))
        })?;

        Ok(schedules)
    }

    async fn find_by_cron_expression(&self, cron_expression: &str) -> OrcherResult<Vec<Schedule>> {
        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE cron_expression = $1"
        ))
        .bind(cron_expression)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find schedules by cron expression: {}",
                e
            ))
        })?;

        Ok(schedules)
    }

    async fn find_by_creator(&self, created_by: &str) -> OrcherResult<Vec<Schedule>> {
        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE created_by = $1"
        ))
        .bind(created_by)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find schedules by creator: {}",
                e
            ))
        })?;

        Ok(schedules)
    }

    async fn find_completed_schedules(&self) -> OrcherResult<Vec<Schedule>> {
        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE status = 'completed'"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find completed schedules: {}",
                e
            ))
        })?;

        Ok(schedules)
    }

    async fn find_schedules_to_complete(&self) -> OrcherResult<Vec<Schedule>> {
        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            r#"
            SELECT {SCHEDULE_COLUMNS} FROM schedules
            WHERE status = 'active'
              AND (
                (max_runs IS NOT NULL AND run_count >= max_runs)
                OR (end_date IS NOT NULL AND end_date <= NOW())
              )
            "#
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find schedules to complete: {}",
                e
            ))
        })?;

        Ok(schedules)
    }

    async fn count_by_status(&self, status: ScheduleStatus) -> OrcherResult<u64> {
        debug!("Counting schedules by status: {:?}", status);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM schedules WHERE status = $1")
            .bind(status)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to count schedules: {}",
                    e
                ))
            })?;

        Ok(count as u64)
    }

    async fn find_by_timezone(&self, timezone: &str) -> OrcherResult<Vec<Schedule>> {
        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE timezone = $1"
        ))
        .bind(timezone)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find schedules by timezone: {}",
                e
            ))
        })?;

        Ok(schedules)
    }

    async fn update_statistics(
        &self,
        schedule_id: ScheduleId,
        run_count: i64,
        success_count: i64,
        failure_count: i64,
    ) -> OrcherResult<()> {
        debug!("Updating statistics for schedule: {}", schedule_id);

        let result = sqlx::query(
            r#"
            UPDATE schedules
            SET run_count = $1, success_count = $2, failure_count = $3, updated_at = $4
            WHERE id = $5
            "#,
        )
        .bind(run_count)
        .bind(success_count)
        .bind(failure_count)
        .bind(Timestamptz::now())
        .bind(schedule_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update schedule statistics: {}",
                e
            ))
        })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Schedule",
                schedule_id.to_string(),
            ))
        }
    }

    async fn find_active_schedules(&self) -> OrcherResult<Vec<Schedule>> {
        debug!("Finding active schedules");

        let schedules = sqlx::query_as::<_, Schedule>(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE enabled = true AND status = 'active'"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find active schedules: {}",
                e
            ))
        })?;

        debug!("Found {} active schedules", schedules.len());
        Ok(schedules)
    }
}

#[cfg(test)]
mod tenancy_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use uuid::Uuid;

    async fn repo() -> ScheduleRepository {
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("a database to test against");
        ScheduleRepository::new(pool)
    }

    /// Schedules carry a foreign key to `organizations`, so a tenant in a test
    /// has to be a real row rather than a loose uuid.
    async fn an_organisation(repo: &ScheduleRepository) -> Uuid {
        let id = Uuid::new_v4();
        let slug = format!("tenancy-{}", &id.to_string()[..8]);
        // An organisation needs an owner, and an owner is a user.
        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, 'x')")
            .bind(owner)
            .bind(format!("{slug}@example.test"))
            .execute(repo.pool())
            .await
            .expect("create a user");
        sqlx::query(
            "INSERT INTO organizations (id, name, display_name, slug, owner_id) \
             VALUES ($1, $2, $2, $3, $4)",
        )
        .bind(id)
        .bind(&slug)
        .bind(&slug)
        .bind(owner)
        .execute(repo.pool())
        .await
        .expect("create an organisation");
        id
    }

    fn a_schedule(name: &str, owner: Uuid, cron: &str) -> Schedule {
        let mut schedule = Schedule::new(
            name.to_string(),
            cron.to_string(),
            "UTC".to_string(),
            serde_json::json!({ "workflow_type": "reconcile", "input": owner.to_string() }),
        );
        schedule.organization_id = Some(owner);
        schedule
    }

    /// The overwrite this closes.
    ///
    /// Cron schedule names are derived — "{workflow_type}:{cron}" — so two
    /// tenants scheduling the same workflow collide by construction. With the
    /// name unique across the deployment and the upsert keyed on it alone, the
    /// second tenant rewrote the first one's schedule: cron, timezone, dates,
    /// and the task template carrying its namespace, queue and input.
    #[tokio::test]
    #[ignore]
    async fn two_organisations_can_hold_the_same_schedule_name() {
        let repo = repo().await;
        let name = format!("reconcile:{}", Uuid::new_v4());
        let mine = an_organisation(&repo).await;
        let theirs = an_organisation(&repo).await;

        let ours = repo
            .upsert_by_name(a_schedule(&name, mine, "0 * * * *"))
            .await
            .expect("my schedule");
        let other = repo
            .upsert_by_name(a_schedule(&name, theirs, "*/5 * * * *"))
            .await
            .expect("their schedule");

        assert_ne!(ours.id, other.id, "each organisation gets its own row");

        let reread = CrudOperations::find_by_id(&repo, ours.id)
            .await
            .expect("read back")
            .expect("my schedule still exists");
        assert_eq!(
            reread.cron_expression, "0 * * * *",
            "my schedule was not rewritten by the other organisation"
        );
        assert_eq!(
            reread.organization_id,
            Some(mine),
            "and still belongs to me"
        );
    }

    /// The same owner upserting the same name still updates in place, which is
    /// what the start path relies on for a schedule it has already created.
    #[tokio::test]
    #[ignore]
    async fn one_organisation_upserting_twice_updates_in_place() {
        let repo = repo().await;
        let name = format!("reconcile:{}", Uuid::new_v4());
        let mine = an_organisation(&repo).await;

        let first = repo
            .upsert_by_name(a_schedule(&name, mine, "0 * * * *"))
            .await
            .expect("first");
        let second = repo
            .upsert_by_name(a_schedule(&name, mine, "*/5 * * * *"))
            .await
            .expect("second");

        assert_eq!(first.id, second.id, "the same row");
        assert_eq!(second.cron_expression, "*/5 * * * *", "updated in place");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_repository_creation() {
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<ScheduleRepository>();
    }

    #[test]
    fn test_schedule_repository_debug() {
        fn _assert_debug<T: std::fmt::Debug>() {}
        _assert_debug::<ScheduleRepository>();
    }

    #[test]
    fn test_schedule_repository_trait_impl() {
        fn _assert_crud<T: CrudOperations<Schedule, ScheduleId>>() {}
        _assert_crud::<ScheduleRepository>();
    }

    #[test]
    fn test_schedule_repository_repository_trait() {
        fn _assert_repository<T: Repository<Schedule>>() {}
        _assert_repository::<ScheduleRepository>();
    }
}
