//! Persistent storage for projection state and metadata.

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;

use serde_json::Value as JsonValue;
use sqlx::{PgPool, Row as _};

use super::traits::{ProjectionMetadata, ProjectionStatus};

#[async_trait]
pub trait ProjectionStateStore: Send + Sync {
    async fn save_metadata(&self, metadata: &ProjectionMetadata) -> super::Result<()>;

    async fn load_metadata(
        &self,
        projection_name: &str,
    ) -> super::Result<Option<ProjectionMetadata>>;

    async fn delete_metadata(&self, projection_name: &str) -> super::Result<()>;

    /// Saves state and updates metadata position atomically.
    async fn save_state(
        &self,
        projection_name: &str,
        state: &JsonValue,
        position: u64,
    ) -> super::Result<()>;

    async fn load_state(&self, projection_name: &str) -> super::Result<Option<JsonValue>>;
    async fn delete_state(&self, projection_name: &str) -> super::Result<()>;

    async fn list_projections(&self) -> super::Result<Vec<ProjectionMetadata>> {
        Ok(Vec::new())
    }

    async fn exists(&self, projection_name: &str) -> super::Result<bool> {
        Ok(self.load_metadata(projection_name).await?.is_some())
    }
}

/// PostgreSQL-backed projection state store.
#[derive(Clone)]
pub struct PostgresProjectionStateStore {
    pool: PgPool,
}

impl PostgresProjectionStateStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates the `projection_metadata` and `projection_state` tables if they don't exist.
    pub async fn create_tables(&self) -> super::Result<()> {
        // Create metadata table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS projection_metadata (
                name TEXT PRIMARY KEY,
                last_processed_position BIGINT NOT NULL DEFAULT 0,
                last_update TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                status TEXT NOT NULL DEFAULT 'Building',
                error_count BIGINT NOT NULL DEFAULT 0,
                version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                category TEXT
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        // Create state table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS projection_state (
                projection_name TEXT PRIMARY KEY REFERENCES projection_metadata(name) ON DELETE CASCADE,
                state JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        // Create indexes
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_projection_metadata_status
            ON projection_metadata(status)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_projection_metadata_category
            ON projection_metadata(category)
            WHERE category IS NOT NULL
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_projection_state_updated_at
            ON projection_state(updated_at DESC)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn status_to_string(status: ProjectionStatus) -> &'static str {
        match status {
            ProjectionStatus::Building => "Building",
            ProjectionStatus::Running => "Running",
            ProjectionStatus::Paused => "Paused",
            ProjectionStatus::Failed => "Failed",
            ProjectionStatus::Stopped => "Stopped",
        }
    }

    fn string_to_status(s: &str) -> super::Result<ProjectionStatus> {
        match s {
            "Building" => Ok(ProjectionStatus::Building),
            "Running" => Ok(ProjectionStatus::Running),
            "Paused" => Ok(ProjectionStatus::Paused),
            "Failed" => Ok(ProjectionStatus::Failed),
            "Stopped" => Ok(ProjectionStatus::Stopped),
            _ => Err(super::ProjectionError::StateError(format!(
                "Unknown projection status: {}",
                s
            ))),
        }
    }
}

#[async_trait]
impl ProjectionStateStore for PostgresProjectionStateStore {
    async fn save_metadata(&self, metadata: &ProjectionMetadata) -> super::Result<()> {
        let status = Self::status_to_string(metadata.status);

        sqlx::query(
            r#"
            INSERT INTO projection_metadata (
                name, last_processed_position, last_update, status,
                error_count, version, created_at, category
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (name)
            DO UPDATE SET
                last_processed_position = EXCLUDED.last_processed_position,
                last_update = EXCLUDED.last_update,
                status = EXCLUDED.status,
                error_count = EXCLUDED.error_count,
                version = EXCLUDED.version,
                category = EXCLUDED.category
            "#,
        )
        .bind(&metadata.name)
        .bind(metadata.last_processed_position as i64)
        .bind(metadata.last_update)
        .bind(status)
        .bind(metadata.error_count as i64)
        .bind(metadata.version as i32)
        .bind(metadata.created_at)
        .bind(&metadata.category)
        .execute(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        Ok(())
    }

    async fn load_metadata(
        &self,
        projection_name: &str,
    ) -> super::Result<Option<ProjectionMetadata>> {
        let row = sqlx::query(
            r#"
            SELECT name, last_processed_position, last_update, status,
                   error_count, version, created_at, category
            FROM projection_metadata
            WHERE name = $1
            "#,
        )
        .bind(projection_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        match row {
            Some(row) => {
                let name: String = row
                    .try_get("name")
                    .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
                let last_processed_position: i64 = row
                    .try_get("last_processed_position")
                    .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
                let last_update: Timestamptz = row
                    .try_get("last_update")
                    .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
                let status_str: String = row
                    .try_get("status")
                    .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
                let error_count: i64 = row
                    .try_get("error_count")
                    .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
                let version: i32 = row
                    .try_get("version")
                    .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
                let created_at: Timestamptz = row
                    .try_get("created_at")
                    .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
                let category: Option<String> = row
                    .try_get("category")
                    .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

                let status = Self::string_to_status(&status_str)?;

                Ok(Some(ProjectionMetadata {
                    name,
                    last_processed_position: last_processed_position as u64,
                    last_update,
                    status,
                    error_count: error_count as u64,
                    version: version as u32,
                    created_at,
                    category,
                }))
            }
            None => Ok(None),
        }
    }

    async fn delete_metadata(&self, projection_name: &str) -> super::Result<()> {
        sqlx::query(
            r#"
            DELETE FROM projection_metadata
            WHERE name = $1
            "#,
        )
        .bind(projection_name)
        .execute(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        Ok(())
    }

    async fn save_state(
        &self,
        projection_name: &str,
        state: &JsonValue,
        position: u64,
    ) -> super::Result<()> {
        // Use a transaction to update both state and metadata atomically
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        // Update or insert state
        sqlx::query(
            r#"
            INSERT INTO projection_state (projection_name, state, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (projection_name)
            DO UPDATE SET
                state = EXCLUDED.state,
                updated_at = NOW()
            "#,
        )
        .bind(projection_name)
        .bind(state)
        .execute(&mut *tx)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        // Update metadata position
        sqlx::query(
            r#"
            UPDATE projection_metadata
            SET last_processed_position = $1,
                last_update = NOW()
            WHERE name = $2
            "#,
        )
        .bind(position as i64)
        .bind(projection_name)
        .execute(&mut *tx)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        Ok(())
    }

    async fn load_state(&self, projection_name: &str) -> super::Result<Option<JsonValue>> {
        let row = sqlx::query(
            r#"
            SELECT state
            FROM projection_state
            WHERE projection_name = $1
            "#,
        )
        .bind(projection_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        match row {
            Some(row) => {
                let state: JsonValue = row
                    .try_get("state")
                    .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    async fn delete_state(&self, projection_name: &str) -> super::Result<()> {
        sqlx::query(
            r#"
            DELETE FROM projection_state
            WHERE projection_name = $1
            "#,
        )
        .bind(projection_name)
        .execute(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        Ok(())
    }

    async fn list_projections(&self) -> super::Result<Vec<ProjectionMetadata>> {
        let rows = sqlx::query(
            r#"
            SELECT name, last_processed_position, last_update, status,
                   error_count, version, created_at, category
            FROM projection_metadata
            ORDER BY name
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        let mut projections = Vec::new();
        for row in rows {
            let name: String = row
                .try_get("name")
                .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
            let last_processed_position: i64 = row
                .try_get("last_processed_position")
                .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
            let last_update: Timestamptz = row
                .try_get("last_update")
                .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
            let status_str: String = row
                .try_get("status")
                .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
            let error_count: i64 = row
                .try_get("error_count")
                .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
            let version: i32 = row
                .try_get("version")
                .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
            let created_at: Timestamptz = row
                .try_get("created_at")
                .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;
            let category: Option<String> = row
                .try_get("category")
                .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

            let status = Self::string_to_status(&status_str)?;

            projections.push(ProjectionMetadata {
                name,
                last_processed_position: last_processed_position as u64,
                last_update,
                status,
                error_count: error_count as u64,
                version: version as u32,
                created_at,
                category,
            });
        }

        Ok(projections)
    }

    async fn exists(&self, projection_name: &str) -> super::Result<bool> {
        let row = sqlx::query(
            r#"
            SELECT EXISTS(SELECT 1 FROM projection_metadata WHERE name = $1) as exists
            "#,
        )
        .bind(projection_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        let exists: bool = row
            .try_get("exists")
            .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        Ok(exists)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_conversion() {
        let status = ProjectionStatus::Running;
        let str_status = PostgresProjectionStateStore::status_to_string(status);
        assert_eq!(str_status, "Running");

        let converted = PostgresProjectionStateStore::string_to_status(str_status).unwrap();
        assert_eq!(converted, status);
    }

    #[test]
    fn test_all_status_conversions() {
        let statuses = vec![
            ProjectionStatus::Building,
            ProjectionStatus::Running,
            ProjectionStatus::Paused,
            ProjectionStatus::Failed,
            ProjectionStatus::Stopped,
        ];

        for status in statuses {
            let str_status = PostgresProjectionStateStore::status_to_string(status);
            let converted = PostgresProjectionStateStore::string_to_status(str_status).unwrap();
            assert_eq!(converted, status);
        }
    }

    #[test]
    fn test_unknown_status() {
        let result = PostgresProjectionStateStore::string_to_status("Unknown");
        assert!(result.is_err());
    }
}
