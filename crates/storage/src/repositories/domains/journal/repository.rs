//! # Journal Entry Repository
//!
//! Repository for workflow execution journal entries (event-sourced history).
//! Provides CRUD operations for journal entries with support for:
//! - Appending new journal entries (append-only)
//! - Retrieving journal entries by execution
//! - Querying journal entries with filters
//! - Getting journal statistics
//! - Event ID sequencing
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_storage::repositories::domains::journal::JournalRepository;
//! use orcher_storage::schema::types::journal::JournalEntry;
//!
//! let repo = JournalRepository::new(pool);
//!
//! // Append journal entry
//! let entry = JournalEntry::new(
//!     "wf-123".to_string(),
//!     "exec-456".to_string(),
//!     1,
//!     "WorkflowExecutionStarted".to_string(),
//!     serde_json::json!({"input": "data"}),
//! );
//! // The event id is allocated in the insert's transaction.
//! let created = repo.append_next(entry).await?;
//!
//! // Get full journal for execution
//! let journal = repo.get_journal("wf-123", "exec-456").await?;
//! ```

use orcher_common::types::time::Timestamptz;
use orcher_common::{error::OrcherError, OrcherResult};
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

use crate::schema::types::journal::{JournalEntry, JournalQueryFilters, JournalStatistics};
use crate::sharding::pool_routing::ShardPoolResolver;
use crate::stale_plan::retry_on_stale_plan;

/// Database row representation for journal entries
#[derive(Debug, Clone, sqlx::FromRow)]
struct JournalEntryRow {
    id: i64,
    organization_id: Option<uuid::Uuid>,
    workflow_id: String,
    execution_id: String,
    event_id: i64,
    event_type: String,
    timestamp: Timestamptz,
    payload: serde_json::Value,
    version: i32,
    task_id: i64,
    created_at: Timestamptz,
    // The shard this row belongs to
    shard_id: Option<i32>,
}

impl From<JournalEntryRow> for JournalEntry {
    fn from(row: JournalEntryRow) -> Self {
        Self {
            id: row.id,
            organization_id: row.organization_id,
            workflow_id: row.workflow_id,
            execution_id: row.execution_id,
            event_id: row.event_id,
            event_type: row.event_type,
            timestamp: row.timestamp,
            payload: row.payload,
            version: row.version,
            task_id: row.task_id,
            created_at: row.created_at,
            shard_id: row.shard_id,
        }
    }
}

/// Repository for journal entry persistence operations
pub struct JournalRepository {
    pool: PgPool,
    /// Optional shard-pool routing for partitioned persistence. A workflow's journal
    /// lives on the same shard as its `workflow_executions` row (routing key =
    /// `workflow_executions.id`, which is the `workflow_id` string on journal rows).
    /// Unset ⇒ the single `pool` is used, byte-identical to before.
    shard_resolver: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn ShardPoolResolver>>>,
}

impl JournalRepository {
    /// Create a new journal repository
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            shard_resolver: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Inject the shard-pool resolver (partitioned persistence). Idempotent; a second
    /// call is ignored. Wired once at startup after the router is built.
    pub fn set_shard_resolver(&self, resolver: std::sync::Arc<dyn ShardPoolResolver>) {
        let _ = self.shard_resolver.set(resolver);
    }

    /// The pool owning `workflow_id`'s shard. `workflow_id` is the routing UUID as a
    /// string; if a resolver is set and the id parses, route to its shard, else use the
    /// single pool (unpartitioned, or a non-UUID id that can't be routed).
    fn pool_for(&self, workflow_id: &str) -> &PgPool {
        match (
            self.shard_resolver.get(),
            uuid::Uuid::parse_str(workflow_id),
        ) {
            (Some(resolver), Ok(id)) => resolver.pool_for_workflow(None, id),
            _ => &self.pool,
        }
    }

    /// Every pool, for a read not keyed by a workflow: all shard pools when a
    /// resolver is set, else the single pool.
    fn scan_pools(&self) -> Vec<&PgPool> {
        match self.shard_resolver.get() {
            Some(resolver) => resolver.all_pools(),
            None => vec![&self.pool],
        }
    }

    /// Append a new journal entry (append-only operation)
    ///
    /// This is the primary write operation. Journal entries are immutable
    /// once created. The event_id must be unique within the execution.
    ///
    /// # Arguments
    ///
    /// * `entry` - The journal entry to append
    ///
    /// # Returns
    ///
    /// The created journal entry with database-generated ID
    pub async fn append(&self, entry: JournalEntry) -> OrcherResult<JournalEntry> {
        debug!(
            workflow_id = %entry.workflow_id,
            execution_id = %entry.execution_id,
            event_id = entry.event_id,
            event_type = %entry.event_type,
            "Appending journal entry"
        );

        let mut conn = self.acquire_for(&entry.workflow_id).await?;
        let row = retry_on_stale_plan!("journal append", conn => {
            Self::insert_on(&mut conn, &entry).await
        })
        .map_err(|e| Self::append_failed(&entry, e))?;

        info!(
            workflow_id = %entry.workflow_id,
            execution_id = %entry.execution_id,
            event_id = entry.event_id,
            event_type = %entry.event_type,
            "Journal entry appended successfully"
        );

        Ok(row)
    }

    /// Append an entry at the next event id for its execution, allocating the
    /// id and inserting the row in one transaction.
    ///
    /// `entry.event_id` is ignored and replaced. This is the way to append one
    /// entry. Allocating an id and appending it as two statements — the
    /// repository used to offer exactly that, and every caller used it — is a
    /// race: the per-execution advisory lock the SQL `get_next_event_id` takes is a
    /// transaction lock — released when its own statement commits, before the
    /// insert that uses the id. Two appends to one execution at the same moment
    /// (two sibling children finishing, two parallel tasks completing) both
    /// read the same maximum, both insert the same id, and the loser fails on
    /// `uq_journal_event_id`. Callers treated that failure as best-effort and
    /// dropped the event, so a parent could wait forever on a child whose
    /// completion was never recorded.
    ///
    /// Here the lock is taken inside the transaction that inserts, so it is
    /// held until the row is committed and appends to one execution serialize.
    pub async fn append_next(&self, entry: JournalEntry) -> OrcherResult<JournalEntry> {
        let mut conn = self.acquire_for(&entry.workflow_id).await?;
        retry_on_stale_plan!("journal append", conn => {
            async {
                let mut tx = sqlx::Connection::begin(&mut *conn).await?;
                let row = Self::insert_next_on(&mut tx, &entry).await?;
                tx.commit().await?;
                Ok::<_, sqlx::Error>(row)
            }
            .await
        })
        .map_err(|e| Self::append_failed(&entry, e))
    }

    /// [`append_next`](Self::append_next), unless the execution already holds
    /// an entry of one of `event_types` whose payload `key` equals `value`.
    ///
    /// Returns `None` when it does. The check and the insert run under the same
    /// per-execution lock, so two deliveries of one event racing each other
    /// record it once — for events that can arrive more than once, such as a
    /// child's outcome reported again after a failed attempt to report it.
    pub async fn append_next_once(
        &self,
        entry: JournalEntry,
        event_types: &[&str],
        key: &str,
        value: &str,
    ) -> OrcherResult<Option<JournalEntry>> {
        let mut conn = self.acquire_for(&entry.workflow_id).await?;
        retry_on_stale_plan!("journal append", conn => {
            async {
                let mut tx = sqlx::Connection::begin(&mut *conn).await?;
                let row = Self::insert_next_once_on(&mut tx, &entry, event_types, key, value).await?;
                tx.commit().await?;
                Ok::<_, sqlx::Error>(row)
            }
            .await
        })
        .map_err(|e| Self::append_failed(&entry, e))
    }

    /// [`append_next_once`](Self::append_next_once) inside a caller's
    /// transaction on the workflow's pool, so the entry commits — or does
    /// not — with the caller's other writes. The per-execution lock is held
    /// until that transaction ends.
    pub async fn append_next_once_on(
        conn: &mut sqlx::PgConnection,
        entry: JournalEntry,
        event_types: &[&str],
        key: &str,
        value: &str,
    ) -> OrcherResult<Option<JournalEntry>> {
        Self::insert_next_once_on(conn, &entry, event_types, key, value)
            .await
            .map_err(|e| Self::append_failed(&entry, e))
    }

    /// The check and insert of [`append_next_once`](Self::append_next_once),
    /// on `conn`.
    async fn insert_next_once_on(
        conn: &mut sqlx::PgConnection,
        entry: &JournalEntry,
        event_types: &[&str],
        key: &str,
        value: &str,
    ) -> Result<Option<JournalEntry>, sqlx::Error> {
        // get_next_event_id takes the lock, so take it first explicitly: the
        // existence check has to be inside it to mean anything.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2, 0))")
            .bind(&entry.workflow_id)
            .bind(&entry.execution_id)
            .execute(&mut *conn)
            .await?;

        // The entry types and the key are written into the statement rather
        // than bound, so in every plan, cached ones included, it reads as
        // `event_type IN ('StepCompleted') AND payload->>'step_name' = $3`
        // and a partial index on that expression answers it from one entry;
        // bound, the check read every entry of the execution. Types and keys
        // are names the engine chooses, never input; anything else is refused.
        let name =
            |n: &str| !n.is_empty() && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
        if event_types.is_empty() || !event_types.iter().all(|t| name(t)) || !name(key) {
            return Err(sqlx::Error::Protocol(format!(
                "not journal entry types and a payload key: {event_types:?}, {key:?}"
            )));
        }
        let types = event_types
            .iter()
            .map(|t| format!("'{t}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let already: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(\
                SELECT 1 FROM workflow_execution_journal \
                WHERE workflow_id = $1 AND execution_id = $2 \
                  AND event_type IN ({types}) \
                  AND payload->>'{key}' = $3)"
        ))
        .bind(&entry.workflow_id)
        .bind(&entry.execution_id)
        .bind(value)
        .fetch_one(&mut *conn)
        .await?;

        if already {
            Ok(None)
        } else {
            Self::insert_next_on(conn, entry).await.map(Some)
        }
    }

    /// A connection from the pool owning `workflow_id`'s shard.
    async fn acquire_for(
        &self,
        workflow_id: &str,
    ) -> OrcherResult<sqlx::pool::PoolConnection<sqlx::Postgres>> {
        self.pool_for(workflow_id).acquire().await.map_err(|e| {
            OrcherError::internal(format!("Failed to acquire a journal connection: {}", e))
        })
    }

    fn append_failed(entry: &JournalEntry, e: sqlx::Error) -> OrcherError {
        warn!(
            error = %e,
            workflow_id = %entry.workflow_id,
            execution_id = %entry.execution_id,
            event_id = entry.event_id,
            "Failed to append journal entry"
        );
        OrcherError::internal(format!("Failed to append journal entry: {}", e))
    }

    /// [`append_next`](Self::append_next) inside a caller's transaction on
    /// the workflow's pool, so the entry commits — or does not — with the
    /// caller's other writes. The per-execution lock is held until that
    /// transaction ends.
    pub async fn append_next_on(
        conn: &mut sqlx::PgConnection,
        entry: JournalEntry,
    ) -> OrcherResult<JournalEntry> {
        Self::insert_next_on(conn, &entry)
            .await
            .map_err(|e| Self::append_failed(&entry, e))
    }

    /// Allocate the next event id for `entry`'s execution and insert it
    /// there, on `conn`. The lock `get_next_event_id` takes is held until the
    /// transaction `conn` is in ends.
    async fn insert_next_on(
        conn: &mut sqlx::PgConnection,
        entry: &JournalEntry,
    ) -> Result<JournalEntry, sqlx::Error> {
        let event_id = sqlx::query_scalar::<_, i64>("SELECT get_next_event_id($1, $2)")
            .bind(&entry.workflow_id)
            .bind(&entry.execution_id)
            .fetch_one(&mut *conn)
            .await?;
        Self::insert_on(
            conn,
            &JournalEntry {
                event_id,
                ..entry.clone()
            },
        )
        .await
    }

    /// Insert `entry` as it is, on `conn`.
    async fn insert_on(
        conn: &mut sqlx::PgConnection,
        entry: &JournalEntry,
    ) -> Result<JournalEntry, sqlx::Error> {
        let row = sqlx::query_as::<_, JournalEntryRow>(concat!(
            r#"
            INSERT INTO workflow_execution_journal (
                organization_id, workflow_id, execution_id, event_id, event_type,
                timestamp, payload, version, task_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING "#,
            crate::repositories::columns::workflow_execution_journal!(),
            r#"
            "#
        ))
        .bind(entry.organization_id)
        .bind(&entry.workflow_id)
        .bind(&entry.execution_id)
        .bind(entry.event_id)
        .bind(&entry.event_type)
        .bind(entry.timestamp)
        .bind(&entry.payload)
        .bind(entry.version)
        .bind(entry.task_id)
        .fetch_one(&mut *conn)
        .await?;
        Ok(row.into())
    }

    /// Append multiple journal entries in a single batch operation
    ///
    /// This is an optimized write operation for inserting multiple journal
    /// entries at once. Uses PostgreSQL's UNNEST for efficient bulk inserts.
    ///
    /// # Arguments
    ///
    /// * `entries` - The journal entries to append
    ///
    /// # Returns
    ///
    /// The number of entries successfully inserted
    ///
    /// # Performance
    ///
    /// - 5-10x faster than individual inserts for batches of 10+ entries
    /// - Uses single round-trip to database
    /// - Maintains atomicity (all-or-nothing)
    pub async fn append_batch(&self, entries: &[JournalEntry]) -> OrcherResult<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        debug!(
            count = entries.len(),
            first_workflow_id = %entries[0].workflow_id,
            first_execution_id = %entries[0].execution_id,
            "Appending batch of journal entries"
        );

        // A batch may span workflows and therefore shards. Group entry indices by their
        // target pool (all entries of a workflow share one pool) and run one UNNEST
        // INSERT per distinct pool so every entry lands on its workflow's shard. With no
        // resolver this collapses to a single group on the one pool (unchanged).
        let mut by_pool: std::collections::HashMap<usize, Vec<usize>> =
            std::collections::HashMap::new();
        for (i, e) in entries.iter().enumerate() {
            let pool = self.pool_for(&e.workflow_id);
            by_pool
                .entry(pool as *const PgPool as usize)
                .or_default()
                .push(i);
        }

        let mut inserted = 0usize;
        for idxs in by_pool.values() {
            let organization_ids: Vec<Option<uuid::Uuid>> =
                idxs.iter().map(|&i| entries[i].organization_id).collect();
            let workflow_ids: Vec<&str> = idxs
                .iter()
                .map(|&i| entries[i].workflow_id.as_str())
                .collect();
            let execution_ids: Vec<&str> = idxs
                .iter()
                .map(|&i| entries[i].execution_id.as_str())
                .collect();
            let event_ids: Vec<i64> = idxs.iter().map(|&i| entries[i].event_id).collect();
            let event_types: Vec<&str> = idxs
                .iter()
                .map(|&i| entries[i].event_type.as_str())
                .collect();
            let timestamps: Vec<Timestamptz> = idxs.iter().map(|&i| entries[i].timestamp).collect();
            let payload: Vec<&serde_json::Value> =
                idxs.iter().map(|&i| &entries[i].payload).collect();
            let versions: Vec<i32> = idxs.iter().map(|&i| entries[i].version).collect();
            let task_ids: Vec<i64> = idxs.iter().map(|&i| entries[i].task_id).collect();

            // Every entry in this group shares a pool; resolve from the first.
            let pool = self.pool_for(&entries[idxs[0]].workflow_id);
            let result = sqlx::query(
                r#"
            INSERT INTO workflow_execution_journal (
                organization_id, workflow_id, execution_id, event_id, event_type,
                timestamp, payload, version, task_id
            )
            SELECT * FROM UNNEST(
                $1::uuid[], $2::text[], $3::text[], $4::bigint[], $5::text[],
                $6::timestamptz[], $7::jsonb[], $8::int[], $9::bigint[]
            )
            "#,
            )
            .bind(&organization_ids)
            .bind(&workflow_ids)
            .bind(&execution_ids)
            .bind(&event_ids)
            .bind(&event_types)
            .bind(&timestamps)
            .bind(&payload)
            .bind(&versions)
            .bind(&task_ids)
            .execute(pool)
            .await
            .map_err(|e| {
                warn!(
                    error = %e,
                    count = idxs.len(),
                    "Failed to append journal entries batch"
                );
                OrcherError::internal(format!("Failed to append journal entries batch: {}", e))
            })?;
            inserted += result.rows_affected() as usize;
        }

        info!(
            count = inserted,
            requested = entries.len(),
            "Journal entries batch appended successfully"
        );

        Ok(inserted)
    }

    /// Get the complete journal for a workflow execution
    ///
    /// Returns all journal entries ordered by event_id ascending.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    pub async fn get_journal(
        &self,
        workflow_id: &str,
        execution_id: &str,
    ) -> OrcherResult<Vec<JournalEntry>> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Retrieving complete journal"
        );

        let mut conn = self.acquire_for(workflow_id).await?;
        let rows = retry_on_stale_plan!("journal read", conn => {
            sqlx::query_as::<_, JournalEntryRow>(concat!(
                r#"
                SELECT "#,
                crate::repositories::columns::workflow_execution_journal!(),
                r#" FROM workflow_execution_journal
                WHERE workflow_id = $1 AND execution_id = $2
                ORDER BY event_id ASC
                "#
            ))
            .bind(workflow_id)
            .bind(execution_id)
            .fetch_all(&mut *conn)
            .await
        })
        .map_err(|e| {
            warn!(error = %e, "Failed to retrieve journal");
            OrcherError::internal(format!("Failed to retrieve journal: {}", e))
        })?;

        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            entry_count = rows.len(),
            "Journal retrieved successfully"
        );

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Get a journal entry by event ID
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `event_id` - The event identifier
    pub async fn get_entry(
        &self,
        workflow_id: &str,
        execution_id: &str,
        event_id: i64,
    ) -> OrcherResult<Option<JournalEntry>> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            event_id = event_id,
            "Retrieving journal entry"
        );

        let mut conn = self.acquire_for(workflow_id).await?;
        let row = retry_on_stale_plan!("journal read", conn => {
            sqlx::query_as::<_, JournalEntryRow>(concat!(
                r#"
                SELECT "#,
                crate::repositories::columns::workflow_execution_journal!(),
                r#" FROM workflow_execution_journal
                WHERE workflow_id = $1 AND execution_id = $2 AND event_id = $3
                "#
            ))
            .bind(workflow_id)
            .bind(execution_id)
            .bind(event_id)
            .fetch_optional(&mut *conn)
            .await
        })
        .map_err(|e| {
            warn!(error = %e, "Failed to retrieve journal entry");
            OrcherError::internal(format!("Failed to retrieve journal entry: {}", e))
        })?;

        Ok(row.map(Into::into))
    }

    /// Get a range of journal entries
    ///
    /// Returns entries with event_ids between min_event_id and max_event_id (inclusive).
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `min_event_id` - Minimum event ID (inclusive)
    /// * `max_event_id` - Maximum event ID (inclusive)
    pub async fn get_range(
        &self,
        workflow_id: &str,
        execution_id: &str,
        min_event_id: i64,
        max_event_id: i64,
    ) -> OrcherResult<Vec<JournalEntry>> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            min_event_id = min_event_id,
            max_event_id = max_event_id,
            "Retrieving journal range"
        );

        let mut conn = self.acquire_for(workflow_id).await?;
        let rows = retry_on_stale_plan!("journal read", conn => {
            sqlx::query_as::<_, JournalEntryRow>(concat!(
                r#"
                SELECT "#,
                crate::repositories::columns::workflow_execution_journal!(),
                r#" FROM workflow_execution_journal
                WHERE workflow_id = $1 AND execution_id = $2
                  AND event_id >= $3 AND event_id <= $4
                ORDER BY event_id ASC
                "#
            ))
            .bind(workflow_id)
            .bind(execution_id)
            .bind(min_event_id)
            .bind(max_event_id)
            .fetch_all(&mut *conn)
            .await
        })
        .map_err(|e| {
            warn!(error = %e, "Failed to retrieve journal range");
            OrcherError::internal(format!("Failed to retrieve journal range: {}", e))
        })?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Query journal entries with filters
    ///
    /// Provides flexible querying with various filter options.
    ///
    /// # Arguments
    ///
    /// * `filters` - Query filters to apply
    pub async fn query(&self, filters: JournalQueryFilters) -> OrcherResult<Vec<JournalEntry>> {
        let mut query = String::from(concat!(
            "SELECT ",
            crate::repositories::columns::workflow_execution_journal!(),
            " FROM workflow_execution_journal WHERE 1=1"
        ));
        let mut bind_index = 1;

        // Build dynamic query based on filters
        if filters.workflow_id.is_some() {
            query.push_str(&format!(" AND workflow_id = ${}", bind_index));
            bind_index += 1;
        }

        if filters.execution_id.is_some() {
            query.push_str(&format!(" AND execution_id = ${}", bind_index));
            bind_index += 1;
        }

        if filters.event_type.is_some() {
            query.push_str(&format!(" AND event_type = ${}", bind_index));
            bind_index += 1;
        }

        if filters.min_event_id.is_some() {
            query.push_str(&format!(" AND event_id >= ${}", bind_index));
            bind_index += 1;
        }

        if filters.max_event_id.is_some() {
            query.push_str(&format!(" AND event_id <= ${}", bind_index));
            bind_index += 1;
        }

        if filters.min_timestamp.is_some() {
            query.push_str(&format!(" AND timestamp >= ${}", bind_index));
            bind_index += 1;
        }

        if filters.max_timestamp.is_some() {
            query.push_str(&format!(" AND timestamp <= ${}", bind_index));
            bind_index += 1;
        }

        if filters.task_id.is_some() {
            query.push_str(&format!(" AND task_id = ${}", bind_index));
            bind_index += 1;
        }

        // Add ordering
        if filters.ascending {
            query.push_str(" ORDER BY event_id ASC");
        } else {
            query.push_str(" ORDER BY event_id DESC");
        }

        // Add pagination
        if let Some(_limit) = filters.limit {
            query.push_str(&format!(" LIMIT ${}", bind_index));
            bind_index += 1;
        }

        if let Some(_offset) = filters.offset {
            query.push_str(&format!(" OFFSET ${}", bind_index));
        }

        // A workflow's journal lives on its workflow's pool. Without a
        // workflow to route by, every pool is read, each for the first
        // offset + limit entries in order, and the page is cut once.
        let pools = match filters.workflow_id {
            Some(ref workflow_id) => vec![self.pool_for(workflow_id)],
            None => self.scan_pools(),
        };
        let merged = pools.len() > 1;
        let (limit, offset) = if merged {
            (
                filters.limit.map(|l| l + filters.offset.unwrap_or(0)),
                filters.offset.map(|_| 0),
            )
        } else {
            (filters.limit, filters.offset)
        };

        let mut rows: Vec<JournalEntryRow> = Vec::new();
        for pool in pools {
            let mut sql_query = sqlx::query_as::<_, JournalEntryRow>(&query);

            if let Some(ref wf_id) = filters.workflow_id {
                sql_query = sql_query.bind(wf_id);
            }
            if let Some(ref exec_id) = filters.execution_id {
                sql_query = sql_query.bind(exec_id);
            }
            if let Some(ref evt_type) = filters.event_type {
                sql_query = sql_query.bind(evt_type);
            }
            if let Some(min_evt) = filters.min_event_id {
                sql_query = sql_query.bind(min_evt);
            }
            if let Some(max_evt) = filters.max_event_id {
                sql_query = sql_query.bind(max_evt);
            }
            if let Some(min_ts) = filters.min_timestamp {
                sql_query = sql_query.bind(min_ts);
            }
            if let Some(max_ts) = filters.max_timestamp {
                sql_query = sql_query.bind(max_ts);
            }
            if let Some(tid) = filters.task_id {
                sql_query = sql_query.bind(tid);
            }
            if let Some(lim) = limit {
                sql_query = sql_query.bind(lim);
            }
            if let Some(off) = offset {
                sql_query = sql_query.bind(off);
            }

            rows.extend(sql_query.fetch_all(pool).await.map_err(|e| {
                warn!(error = %e, "Failed to query journal entries");
                OrcherError::internal(format!("Failed to query journal entries: {}", e))
            })?);
        }
        if merged {
            if filters.ascending {
                rows.sort_by_key(|r| r.event_id);
            } else {
                rows.sort_by_key(|r| std::cmp::Reverse(r.event_id));
            }
            let skip = filters.offset.unwrap_or(0).max(0) as usize;
            let take = filters.limit.map_or(usize::MAX, |l| l.max(0) as usize);
            rows = rows.into_iter().skip(skip).take(take).collect();
        }

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Get the maximum event ID for a workflow execution
    ///
    /// Returns None if no journal entries exist.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    pub async fn get_max_event_id(
        &self,
        workflow_id: &str,
        execution_id: &str,
    ) -> OrcherResult<Option<i64>> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Getting max event ID"
        );

        let result = sqlx::query(
            r#"
            SELECT MAX(event_id) as max_id
            FROM workflow_execution_journal
            WHERE workflow_id = $1 AND execution_id = $2
            "#,
        )
        .bind(workflow_id)
        .bind(execution_id)
        .fetch_one(self.pool_for(workflow_id))
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get max event ID");
            OrcherError::internal(format!("Failed to get max event ID: {}", e))
        })?;

        let max_id: Option<i64> = result
            .try_get("max_id")
            .map_err(|e| OrcherError::internal(format!("Failed to parse max event ID: {}", e)))?;

        Ok(max_id)
    }

    /// Get statistics about a workflow execution's journal
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    pub async fn get_statistics(
        &self,
        workflow_id: &str,
        execution_id: &str,
    ) -> OrcherResult<JournalStatistics> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Getting journal statistics"
        );

        // Get basic stats
        let result = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_entries,
                COALESCE(MAX(event_id), 0) as max_event_id,
                MIN(timestamp) as earliest_timestamp,
                MAX(timestamp) as latest_timestamp
            FROM workflow_execution_journal
            WHERE workflow_id = $1 AND execution_id = $2
            "#,
        )
        .bind(workflow_id)
        .bind(execution_id)
        .fetch_one(self.pool_for(workflow_id))
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get journal statistics");
            OrcherError::internal(format!("Failed to get journal statistics: {}", e))
        })?;

        let total_entries: i64 = result
            .try_get("total_entries")
            .map_err(|e| OrcherError::internal(format!("Failed to parse total_entries: {}", e)))?;
        let max_event_id: i64 = result
            .try_get("max_event_id")
            .map_err(|e| OrcherError::internal(format!("Failed to parse max_event_id: {}", e)))?;
        let earliest_timestamp: Option<Timestamptz> = result.try_get("earliest_timestamp").ok();
        let latest_timestamp: Option<Timestamptz> = result.try_get("latest_timestamp").ok();

        // Get event type counts
        let type_counts = sqlx::query(
            r#"
            SELECT event_type, COUNT(*) as count
            FROM workflow_execution_journal
            WHERE workflow_id = $1 AND execution_id = $2
            GROUP BY event_type
            "#,
        )
        .bind(workflow_id)
        .bind(execution_id)
        .fetch_all(self.pool_for(workflow_id))
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get event type counts");
            OrcherError::internal(format!("Failed to get event type counts: {}", e))
        })?;

        let mut type_counts_map = serde_json::Map::new();
        for row in type_counts {
            let event_type: String = row
                .try_get("event_type")
                .map_err(|e| OrcherError::internal(format!("Failed to parse event_type: {}", e)))?;
            let count: i64 = row
                .try_get("count")
                .map_err(|e| OrcherError::internal(format!("Failed to parse count: {}", e)))?;
            type_counts_map.insert(event_type, serde_json::json!(count));
        }

        // Count specific event categories
        let child_workflow_count = sqlx::query(
            r#"
            SELECT COUNT(*) as count
            FROM workflow_execution_journal
            WHERE workflow_id = $1 AND execution_id = $2
              AND event_type LIKE 'ChildWorkflowExecution%'
            "#,
        )
        .bind(workflow_id)
        .bind(execution_id)
        .fetch_one(self.pool_for(workflow_id))
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to count child workflow events: {}", e))
        })?
        .try_get("count")
        .map_err(|e| {
            OrcherError::internal(format!("Failed to parse child workflow count: {}", e))
        })?;

        let task_event_count = sqlx::query(
            r#"
            SELECT COUNT(*) as count
            FROM workflow_execution_journal
            WHERE workflow_id = $1 AND execution_id = $2
              AND (event_type LIKE '%Task%' AND event_type NOT LIKE 'ChildWorkflowExecution%')
            "#,
        )
        .bind(workflow_id)
        .bind(execution_id)
        .fetch_one(self.pool_for(workflow_id))
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to count task events: {}", e)))?
        .try_get("count")
        .map_err(|e| OrcherError::internal(format!("Failed to parse task event count: {}", e)))?;

        let timer_event_count = sqlx::query(
            r#"
            SELECT COUNT(*) as count
            FROM workflow_execution_journal
            WHERE workflow_id = $1 AND execution_id = $2
              AND event_type LIKE '%Timer%'
            "#,
        )
        .bind(workflow_id)
        .bind(execution_id)
        .fetch_one(self.pool_for(workflow_id))
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to count timer events: {}", e)))?
        .try_get("count")
        .map_err(|e| OrcherError::internal(format!("Failed to parse timer event count: {}", e)))?;

        Ok(JournalStatistics {
            total_entries,
            max_event_id,
            earliest_timestamp,
            latest_timestamp,
            event_type_counts: serde_json::Value::Object(type_counts_map),
            child_workflow_event_count: child_workflow_count,
            task_event_count,
            timer_event_count,
        })
    }

    /// Get journal entries after a specific event ID (for incremental loading)
    ///
    /// This method is optimized for incremental cache updates. It returns only
    /// entries with event_id > after_event_id, which is useful when the cache
    /// already has entries up to a certain point.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `after_event_id` - Only return entries with event_id > this value
    ///
    /// # Returns
    ///
    /// Journal entries after the specified event ID, ordered by event_id ASC
    ///
    /// # Performance
    ///
    /// - Uses index on (workflow_id, execution_id, event_id)
    /// - Returns only new entries, reducing data transfer
    /// - Enables efficient cache incremental updates
    pub async fn get_entries_after(
        &self,
        workflow_id: &str,
        execution_id: &str,
        after_event_id: i64,
    ) -> OrcherResult<Vec<JournalEntry>> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            after_event_id = after_event_id,
            "Retrieving journal entries incrementally"
        );

        let mut conn = self.acquire_for(workflow_id).await?;
        let rows = retry_on_stale_plan!("journal read", conn => {
            sqlx::query_as::<_, JournalEntryRow>(concat!(
                r#"
                SELECT "#,
                crate::repositories::columns::workflow_execution_journal!(),
                r#" FROM workflow_execution_journal
                WHERE workflow_id = $1 AND execution_id = $2
                  AND event_id > $3
                ORDER BY event_id ASC
                "#
            ))
            .bind(workflow_id)
            .bind(execution_id)
            .bind(after_event_id)
            .fetch_all(&mut *conn)
            .await
        })
        .map_err(|e| {
            warn!(error = %e, "Failed to retrieve incremental journal entries");
            OrcherError::internal(format!(
                "Failed to retrieve incremental journal entries: {}",
                e
            ))
        })?;

        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            after_event_id = after_event_id,
            entry_count = rows.len(),
            "Incremental journal entries retrieved"
        );

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Check if a journal exists for a workflow execution
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    pub async fn exists(&self, workflow_id: &str, execution_id: &str) -> OrcherResult<bool> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Checking if journal exists"
        );

        let result = sqlx::query(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM workflow_execution_journal
                WHERE workflow_id = $1 AND execution_id = $2
            ) as exists
            "#,
        )
        .bind(workflow_id)
        .bind(execution_id)
        .fetch_one(self.pool_for(workflow_id))
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to check journal existence");
            OrcherError::internal(format!("Failed to check journal existence: {}", e))
        })?;

        let exists: bool = result
            .try_get("exists")
            .map_err(|e| OrcherError::internal(format!("Failed to parse exists: {}", e)))?;

        Ok(exists)
    }

    /// Delete all journal entries for a workflow execution
    ///
    /// ⚠️ WARNING: This deletes immutable journal entries. Use with extreme caution.
    /// Typically only used for testing or data cleanup.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    ///
    /// # Returns
    ///
    /// The number of entries deleted
    pub async fn delete_journal(&self, workflow_id: &str, execution_id: &str) -> OrcherResult<u64> {
        warn!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Deleting journal (DESTRUCTIVE OPERATION)"
        );

        let result = sqlx::query(
            r#"
            DELETE FROM workflow_execution_journal
            WHERE workflow_id = $1 AND execution_id = $2
            "#,
        )
        .bind(workflow_id)
        .bind(execution_id)
        .execute(self.pool_for(workflow_id))
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to delete journal");
            OrcherError::internal(format!("Failed to delete journal: {}", e))
        })?;

        let deleted = result.rows_affected();

        warn!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            deleted_count = deleted,
            "Journal deleted"
        );

        Ok(deleted)
    }

    /// Copy journal entries up to (and including) a target event_id into a new execution.
    ///
    /// Used by workflow reset: the new execution gets the same journal history
    /// up to the reset point, so replay reconstructs the state correctly.
    /// Events after `target_event_id` are not copied.
    ///
    /// Returns the number of entries copied.
    pub async fn copy_journal_up_to(
        &self,
        source_workflow_id: &str,
        source_execution_id: &str,
        target_workflow_id: &str,
        target_execution_id: &str,
        target_event_id: i64,
    ) -> OrcherResult<u64> {
        info!(
            source_execution = %source_execution_id,
            target_execution = %target_execution_id,
            up_to_event = target_event_id,
            "Copying journal entries for workflow reset"
        );

        let source = self.pool_for(source_workflow_id);
        let target = self.pool_for(target_workflow_id);
        if !std::ptr::eq(source, target) {
            // Different shard pools: one statement cannot read one database
            // and write another. Read the entries and append them to the
            // target's pool, keeping their event ids. It ran the copy on the
            // default pool, which with shards read nothing and wrote there.
            let entries: Vec<JournalEntry> = sqlx::query_as::<_, JournalEntryRow>(concat!(
                "SELECT ",
                crate::repositories::columns::workflow_execution_journal!(),
                " FROM workflow_execution_journal \
                 WHERE workflow_id = $1 AND execution_id = $2 AND event_id <= $3 \
                 ORDER BY event_id ASC"
            ))
            .bind(source_workflow_id)
            .bind(source_execution_id)
            .bind(target_event_id)
            .fetch_all(source)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to read journal for reset: {}", e)))?
            .into_iter()
            .map(|row| {
                let mut entry: JournalEntry = row.into();
                entry.workflow_id = target_workflow_id.to_string();
                entry.execution_id = target_execution_id.to_string();
                entry
            })
            .collect();
            let copied = self.append_batch(&entries).await? as u64;
            info!(
                source_execution = %source_execution_id,
                target_execution = %target_execution_id,
                copied_count = copied,
                "Journal entries copied for reset across shard pools"
            );
            return Ok(copied);
        }

        let result = sqlx::query(
            r#"
            INSERT INTO workflow_execution_journal (
                organization_id, workflow_id, execution_id, event_id, event_type,
                timestamp, payload, version, task_id, shard_id
            )
            SELECT
                organization_id, $3, $4, event_id, event_type,
                timestamp, payload, version, task_id, shard_id
            FROM workflow_execution_journal
            WHERE workflow_id = $1 AND execution_id = $2 AND event_id <= $5
            ORDER BY event_id ASC
            "#,
        )
        .bind(source_workflow_id)
        .bind(source_execution_id)
        .bind(target_workflow_id)
        .bind(target_execution_id)
        .bind(target_event_id)
        .execute(target)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to copy journal entries for reset");
            OrcherError::internal(format!("Failed to copy journal entries: {}", e))
        })?;

        let copied = result.rows_affected();

        info!(
            source_execution = %source_execution_id,
            target_execution = %target_execution_id,
            copied_count = copied,
            "Journal entries copied for reset"
        );

        Ok(copied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a PostgreSQL database with the V024 migration applied.
    // Run with: cargo test --package orcher-storage --lib repositories::domains::journal

    async fn test_pool() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        PgPool::connect(&url)
            .await
            .expect("TEST_DATABASE_URL must point at a migrated database")
    }

    fn entry(
        workflow_id: &str,
        execution_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> JournalEntry {
        JournalEntry::new(
            workflow_id.to_string(),
            execution_id.to_string(),
            0,
            event_type.to_string(),
            payload,
        )
    }

    async fn event_ids(pool: &PgPool, workflow_id: &str, execution_id: &str) -> Vec<i64> {
        sqlx::query_scalar(
            "SELECT event_id FROM workflow_execution_journal \
             WHERE workflow_id = $1 AND execution_id = $2 ORDER BY event_id",
        )
        .bind(workflow_id)
        .bind(execution_id)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    async fn delete_journal(pool: &PgPool, workflow_id: &str) {
        sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(pool)
            .await
            .unwrap();
    }

    /// Two sibling children finishing, or parallel tasks completing, append to
    /// one execution at the same moment. Allocating the id and inserting in
    /// separate statements let them take the same id; the loser failed on the
    /// unique constraint and its event was dropped. Every append must land,
    /// each with its own id, and the ids must have no gaps.
    #[tokio::test]
    #[ignore]
    async fn concurrent_appends_to_one_execution_all_land() {
        const APPENDS: i64 = 32;
        let pool = test_pool().await;
        let repo = std::sync::Arc::new(JournalRepository::new(pool.clone()));
        let workflow_id = uuid::Uuid::new_v4().to_string();
        let execution_id = uuid::Uuid::new_v4().to_string();

        let appends = (0..APPENDS).map(|i| {
            let repo = repo.clone();
            let entry = entry(
                &workflow_id,
                &execution_id,
                "TaskCompleted",
                serde_json::json!({ "task_id": i }),
            );
            tokio::spawn(async move { repo.append_next(entry).await })
        });
        for result in futures::future::join_all(appends).await {
            result
                .unwrap()
                .expect("an append to a busy execution failed");
        }

        assert_eq!(
            event_ids(&pool, &workflow_id, &execution_id).await,
            (1..=APPENDS).collect::<Vec<_>>()
        );
        delete_journal(&pool, &workflow_id).await;
    }

    /// A child's outcome can be reported more than once — a retried report
    /// after a failure that happened after the append committed, or two
    /// deliveries racing. The parent must see it once; a different child's
    /// outcome must still be recorded.
    #[tokio::test]
    #[ignore]
    async fn a_repeated_child_outcome_is_recorded_once() {
        let pool = test_pool().await;
        let repo = std::sync::Arc::new(JournalRepository::new(pool.clone()));
        let workflow_id = uuid::Uuid::new_v4().to_string();
        let execution_id = uuid::Uuid::new_v4().to_string();
        const OUTCOMES: [&str; 2] = [
            "ChildWorkflowExecutionCompleted",
            "ChildWorkflowExecutionFailed",
        ];

        let reports = (0..16).map(|_| {
            let repo = repo.clone();
            let entry = entry(
                &workflow_id,
                &execution_id,
                "ChildWorkflowExecutionCompleted",
                serde_json::json!({ "child_execution_id": "child-a" }),
            );
            tokio::spawn(async move {
                repo.append_next_once(entry, &OUTCOMES, "child_execution_id", "child-a")
                    .await
            })
        });
        let mut recorded = 0;
        for result in futures::future::join_all(reports).await {
            if result
                .unwrap()
                .expect("reporting an outcome failed")
                .is_some()
            {
                recorded += 1;
            }
        }
        assert_eq!(recorded, 1, "one report should have recorded the outcome");

        // A failure reported for the same child is the same outcome slot.
        let again = repo
            .append_next_once(
                entry(
                    &workflow_id,
                    &execution_id,
                    "ChildWorkflowExecutionFailed",
                    serde_json::json!({ "child_execution_id": "child-a" }),
                ),
                &OUTCOMES,
                "child_execution_id",
                "child-a",
            )
            .await
            .unwrap();
        assert!(again.is_none());

        let other = repo
            .append_next_once(
                entry(
                    &workflow_id,
                    &execution_id,
                    "ChildWorkflowExecutionCompleted",
                    serde_json::json!({ "child_execution_id": "child-b" }),
                ),
                &OUTCOMES,
                "child_execution_id",
                "child-b",
            )
            .await
            .unwrap();
        assert!(other.is_some(), "another child's outcome was refused");

        assert_eq!(
            event_ids(&pool, &workflow_id, &execution_id).await,
            vec![1, 2]
        );
        delete_journal(&pool, &workflow_id).await;
    }

    #[test]
    fn test_repository_creation() {
        // This is a compile-time test to ensure the repository can be constructed
        fn _assert_repository_constructible() {
            let pool =
                PgPool::connect_lazy("postgresql://localhost/test").expect("Failed to create pool");
            let _repo = JournalRepository::new(pool);
        }
    }
}
