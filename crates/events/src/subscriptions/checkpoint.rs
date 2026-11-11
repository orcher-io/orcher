//! Persistent checkpoint storage so subscriptions can resume after restarts.

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row as _};

use crate::replay::StreamPosition;

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn save_checkpoint(
        &self,
        subscription_id: &str,
        position: StreamPosition,
    ) -> super::Result<()>;

    /// Returns `None` if no checkpoint exists yet.
    async fn load_checkpoint(&self, subscription_id: &str)
        -> super::Result<Option<StreamPosition>>;

    async fn delete_checkpoint(&self, subscription_id: &str) -> super::Result<()>;

    async fn list_checkpoints(&self) -> super::Result<Vec<CheckpointInfo>> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    pub subscription_id: String,
    pub position: StreamPosition,
    pub updated_at: Timestamptz,
}

/// PostgreSQL-backed checkpoint store.
#[derive(Clone)]
pub struct PostgresCheckpointStore {
    pool: PgPool,
}

impl PostgresCheckpointStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates the `subscription_checkpoints` table if it doesn't exist.
    pub async fn create_table(&self) -> super::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS subscription_checkpoints (
                subscription_id TEXT PRIMARY KEY,
                position BIGINT NOT NULL,
                position_type TEXT NOT NULL DEFAULT 'position',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;

        // Create index on updated_at for efficient querying
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_checkpoints_updated_at
            ON subscription_checkpoints(updated_at DESC)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;

        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl CheckpointStore for PostgresCheckpointStore {
    async fn save_checkpoint(
        &self,
        subscription_id: &str,
        position: StreamPosition,
    ) -> super::Result<()> {
        let (position_value, position_type) = match position {
            StreamPosition::Start => (0i64, "start"),
            StreamPosition::End => (-1i64, "end"),
            StreamPosition::Position(pos) => (pos as i64, "position"),
        };

        sqlx::query(
            r#"
            INSERT INTO subscription_checkpoints (subscription_id, position, position_type, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (subscription_id)
            DO UPDATE SET
                position = EXCLUDED.position,
                position_type = EXCLUDED.position_type,
                updated_at = NOW()
            "#,
        )
        .bind(subscription_id)
        .bind(position_value)
        .bind(position_type)
        .execute(&self.pool)
        .await
        .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;

        Ok(())
    }

    async fn load_checkpoint(
        &self,
        subscription_id: &str,
    ) -> super::Result<Option<StreamPosition>> {
        let row = sqlx::query(
            r#"
            SELECT position, position_type
            FROM subscription_checkpoints
            WHERE subscription_id = $1
            "#,
        )
        .bind(subscription_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;

        match row {
            Some(row) => {
                let position_value: i64 = row
                    .try_get("position")
                    .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;
                let position_type: String = row
                    .try_get("position_type")
                    .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;

                let position = match position_type.as_str() {
                    "start" => StreamPosition::Start,
                    "end" => StreamPosition::End,
                    "position" => StreamPosition::Position(position_value as u64),
                    _ => {
                        return Err(super::SubscriptionError::CheckpointError(format!(
                            "Unknown position type: {}",
                            position_type
                        )))
                    }
                };

                Ok(Some(position))
            }
            None => Ok(None),
        }
    }

    async fn delete_checkpoint(&self, subscription_id: &str) -> super::Result<()> {
        sqlx::query(
            r#"
            DELETE FROM subscription_checkpoints
            WHERE subscription_id = $1
            "#,
        )
        .bind(subscription_id)
        .execute(&self.pool)
        .await
        .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;

        Ok(())
    }

    async fn list_checkpoints(&self) -> super::Result<Vec<CheckpointInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT subscription_id, position, position_type, updated_at
            FROM subscription_checkpoints
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;

        let mut checkpoints = Vec::new();
        for row in rows {
            let subscription_id: String = row
                .try_get("subscription_id")
                .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;
            let position_value: i64 = row
                .try_get("position")
                .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;
            let position_type: String = row
                .try_get("position_type")
                .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;
            let updated_at: Timestamptz = row
                .try_get("updated_at")
                .map_err(|e| super::SubscriptionError::CheckpointError(e.to_string()))?;

            let position = match position_type.as_str() {
                "start" => StreamPosition::Start,
                "end" => StreamPosition::End,
                "position" => StreamPosition::Position(position_value as u64),
                _ => continue, // Skip unknown position types
            };

            checkpoints.push(CheckpointInfo {
                subscription_id,
                position,
                updated_at,
            });
        }

        Ok(checkpoints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_info_creation() {
        let info = CheckpointInfo {
            subscription_id: "test-sub".to_string(),
            position: StreamPosition::Position(100),
            updated_at: Timestamptz::now(),
        };

        assert_eq!(info.subscription_id, "test-sub");
        assert_eq!(info.position, StreamPosition::Position(100));
    }

    #[test]
    fn test_position_serialization() {
        let start = StreamPosition::Start;
        let end = StreamPosition::End;
        let pos = StreamPosition::Position(42);

        // Test that positions can be compared
        assert_ne!(start, end);
        assert_ne!(start, pos);
        assert_eq!(pos, StreamPosition::Position(42));
    }
}
