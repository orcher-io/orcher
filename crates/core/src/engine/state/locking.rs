//! Workflow-level state locks.

use orcher_common::error::{OrcherError, OrcherResult};
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use uuid::Uuid;

// ============================================================================
// Lock Configuration
// ============================================================================

/// Configuration for distributed locking
#[derive(Debug, Clone)]
pub struct LockConfig {
    /// Timeout for lock acquisition in seconds
    pub acquire_timeout_secs: i64,

    /// Enable lock metrics
    pub enable_metrics: bool,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            acquire_timeout_secs: 10,
            enable_metrics: true,
        }
    }
}

// ============================================================================
// Workflow State Lock
// ============================================================================

/// Distributed lock for workflow state using PostgreSQL advisory locks.
///
/// Advisory locks taken with `pg_advisory_lock` belong to the database session
/// that took them. This used to take the lock through the pool and release it
/// through the pool, which usually meant a different session: the unlock was a
/// no-op, the lock stayed held on the first connection for its whole life, and
/// every later attempt for that workflow failed unless it happened to land on
/// that same connection. A workflow that scheduled two timers failed on the
/// second. The guard now keeps the connection that took the lock and releases
/// on it.
pub struct WorkflowStateLock {
    pool: Arc<PgPool>,
    config: LockConfig,
}

/// How often `acquire` retries while the lock is held elsewhere.
const ACQUIRE_RETRY: Duration = Duration::from_millis(25);

impl WorkflowStateLock {
    /// Create a new workflow state lock
    pub fn new(pool: Arc<PgPool>, config: LockConfig) -> Self {
        Self { pool, config }
    }

    /// Create with default configuration
    pub fn with_defaults(pool: Arc<PgPool>) -> Self {
        Self::new(pool, LockConfig::default())
    }

    /// Acquire a lock for a workflow, waiting up to the configured timeout.
    ///
    /// Release it with [`LockGuard::release`]. Dropping the guard also
    /// releases it, but in the background, so an immediate next acquisition
    /// may still find it held.
    pub async fn acquire(&self, workflow_id: Uuid) -> OrcherResult<LockGuard> {
        let deadline =
            Instant::now() + Duration::from_secs(self.config.acquire_timeout_secs.max(0) as u64);
        loop {
            if let Some(guard) = self.try_acquire(workflow_id).await? {
                return Ok(guard);
            }
            if Instant::now() >= deadline {
                warn!(
                    workflow_id = %workflow_id,
                    timeout_secs = self.config.acquire_timeout_secs,
                    "Failed to acquire lock (already held)"
                );
                return Err(OrcherError::Internal {
                    message: format!(
                        "Failed to acquire lock for workflow {} (already held)",
                        workflow_id
                    ),
                    source: None,
                });
            }
            tokio::time::sleep(ACQUIRE_RETRY).await;
        }
    }

    /// Try to acquire a lock (non-blocking)
    ///
    /// Returns None if the lock is already held by another session.
    pub async fn try_acquire(&self, workflow_id: Uuid) -> OrcherResult<Option<LockGuard>> {
        let lock_id = Self::workflow_id_to_lock_id(&workflow_id);

        debug!(
            workflow_id = %workflow_id,
            lock_id = lock_id,
            "Trying to acquire workflow lock"
        );

        // The lock belongs to this connection's session, so the guard keeps it.
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to get a connection for the lock: {}", e),
                source: None,
            })?;

        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to try lock: {}", e),
                source: None,
            })?;

        if acquired {
            debug!(
                workflow_id = %workflow_id,
                lock_id = lock_id,
                "Lock acquired"
            );
            Ok(Some(LockGuard {
                workflow_id,
                lock_id,
                conn: Some(conn),
            }))
        } else {
            debug!(
                workflow_id = %workflow_id,
                lock_id = lock_id,
                "Lock not available"
            );
            Ok(None)
        }
    }

    /// Convert workflow UUID to a PostgreSQL advisory lock ID
    ///
    /// PostgreSQL advisory locks use bigint (i64), so we need to convert
    /// the UUID to a stable i64. We use the first 8 bytes of the UUID.
    pub(crate) fn workflow_id_to_lock_id(workflow_id: &Uuid) -> i64 {
        let bytes = workflow_id.as_bytes();
        // Take first 8 bytes and convert to i64
        i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ])
    }
}

// ============================================================================
// Lock Guard
// ============================================================================

/// Holds a workflow lock together with the database session that owns it.
pub struct LockGuard {
    workflow_id: Uuid,
    lock_id: i64,
    conn: Option<PoolConnection<Postgres>>,
}

impl LockGuard {
    /// Get the workflow ID this lock is for
    pub fn workflow_id(&self) -> Uuid {
        self.workflow_id
    }

    /// Release the lock now, on the session that took it.
    pub async fn release(mut self) {
        if let Some(conn) = self.conn.take() {
            unlock(conn, self.lock_id, self.workflow_id).await;
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // Released explicitly already, or the guard is being dropped on an
        // early return: release in the background on the owning session.
        if let Some(conn) = self.conn.take() {
            let (lock_id, workflow_id) = (self.lock_id, self.workflow_id);
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(unlock(conn, lock_id, workflow_id));
                }
                // No runtime to release on: close the session instead of
                // returning it to the pool, which ends the lock with it.
                Err(_) => drop(conn.detach()),
            }
        }
    }
}

/// Unlock on the owning session. If that fails, close the session rather than
/// hand a connection that still holds the lock back to the pool.
async fn unlock(mut conn: PoolConnection<Postgres>, lock_id: i64, workflow_id: Uuid) {
    match sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(lock_id)
        .fetch_one(&mut *conn)
        .await
    {
        Ok(true) => {
            debug!(workflow_id = %workflow_id, lock_id = lock_id, "Lock released");
        }
        Ok(false) => {
            warn!(
                workflow_id = %workflow_id,
                lock_id = lock_id,
                "Lock was not held by its own session; closing the session"
            );
            let _ = conn.close().await;
        }
        Err(e) => {
            warn!(
                workflow_id = %workflow_id,
                lock_id = lock_id,
                error = %e,
                "Failed to release lock; closing the session so the lock ends with it"
            );
            let _ = conn.close().await;
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_id_to_lock_id_stability() {
        let workflow_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        // Should always produce the same lock ID for the same UUID
        let lock_id1 = WorkflowStateLock::workflow_id_to_lock_id(&workflow_id);
        let lock_id2 = WorkflowStateLock::workflow_id_to_lock_id(&workflow_id);

        assert_eq!(lock_id1, lock_id2);
    }

    #[test]
    fn test_workflow_id_to_lock_id_uniqueness() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let lock_id1 = WorkflowStateLock::workflow_id_to_lock_id(&id1);
        let lock_id2 = WorkflowStateLock::workflow_id_to_lock_id(&id2);

        // Different UUIDs should (very likely) produce different lock IDs
        // Note: There's a tiny chance of collision since we're hashing 128 bits to 64 bits
        assert_ne!(lock_id1, lock_id2);
    }

    async fn test_pool() -> Arc<PgPool> {
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        Arc::new(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(8)
                .connect(&url)
                .await
                .expect("connect to the test database"),
        )
    }

    fn quick(pool: Arc<PgPool>) -> WorkflowStateLock {
        WorkflowStateLock::new(
            pool,
            LockConfig {
                acquire_timeout_secs: 1,
                enable_metrics: false,
            },
        )
    }

    /// The defect: released through the pool, the lock stayed held on the
    /// session that took it, and the next acquisition failed.
    #[tokio::test]
    #[ignore] // Requires database
    async fn a_released_lock_can_be_taken_again_many_times() {
        let locks = quick(test_pool().await);
        let id = Uuid::new_v4();
        for _ in 0..20 {
            let guard = locks.acquire(id).await.expect("acquire");
            guard.release().await;
        }
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn a_held_lock_excludes_others_until_released() {
        let locks = quick(test_pool().await);
        let id = Uuid::new_v4();

        let guard = locks.acquire(id).await.expect("acquire");
        assert!(locks.try_acquire(id).await.expect("try").is_none());

        guard.release().await;
        let again = locks.try_acquire(id).await.expect("try");
        assert!(again.is_some());
        again.unwrap().release().await;
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn acquire_waits_for_a_brief_holder() {
        let locks = Arc::new(quick(test_pool().await));
        let id = Uuid::new_v4();

        let guard = locks.acquire(id).await.expect("acquire");
        let waiter = {
            let locks = locks.clone();
            tokio::spawn(async move { locks.acquire(id).await.map(|g| g.workflow_id()) })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        guard.release().await;

        assert_eq!(waiter.await.unwrap().expect("waiter acquires"), id);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn acquire_gives_up_after_its_timeout() {
        let locks = quick(test_pool().await);
        let id = Uuid::new_v4();

        let guard = locks.acquire(id).await.expect("acquire");
        let started = Instant::now();
        assert!(locks.acquire(id).await.is_err());
        assert!(started.elapsed() >= Duration::from_millis(900));
        guard.release().await;
    }
}
