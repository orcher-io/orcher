//! Configuration management for orchestrator service

use anyhow::{Context, Result};
use orcher_common::types::dispatch::{AffinityConfig, RoutingConfig};
use orcher_core::grpc::auth_layer::AuthMode;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    pub server: ServerConfig,
    pub orchestrator: OrchestratorCoreConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
    pub health: HealthConfig,
    // Tuning sections: every field has a default, so a file that leaves the
    // whole section out means "the defaults", the same as an empty one.
    // Without this a file had to name each section merely to say nothing
    // about it, and the shipped config.toml failed to parse the first time
    // it was read.
    #[serde(default)]
    pub polling: PollingConfig,
    #[serde(default)]
    pub timeout_watcher: TimeoutWatcherConfig,
    #[serde(default)]
    pub workflow_retry: WorkflowRetryConfig,
    #[serde(default)]
    pub child_reconciler: ChildReconcilerConfig,
    #[serde(default)]
    pub worker_liveness: WorkerLivenessConfig,
    #[serde(default)]
    pub background_jobs: BackgroundJobsConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub cluster: ClusterSettings,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub ui: UiConfig,
}

/// The dashboard: static files the engine serves on its HTTP port, after the
/// REST API, health and metrics. `ORCHER_UI_ENABLED` and `ORCHER_UI_DIR`
/// override the file; `--no-ui` turns it off.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiConfig {
    /// Default true. With no dashboard in `dir`, the engine serves the API
    /// alone and says so once at start-up.
    #[serde(default = "default_ui_enabled")]
    pub enabled: bool,
    /// Default `/usr/share/orcher/ui`, where the engine image installs it.
    #[serde(default = "default_ui_dir")]
    pub dir: String,
}

fn default_ui_enabled() -> bool {
    true
}

fn default_ui_dir() -> String {
    "/usr/share/orcher/ui".to_string()
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            enabled: default_ui_enabled(),
            dir: default_ui_dir(),
        }
    }
}

/// How large a gRPC message and a single payload may be.
///
/// `grpc_max_message_bytes` bounds every message the engine sends or accepts;
/// `ORCHER_GRPC_MAX_MESSAGE_BYTES` overrides it. `payload_max_bytes` bounds one
/// payload — a workflow's input or result, a task's input or result, an
/// event's payload — where it enters the engine, and must be smaller, since an
/// activation carries every payload its workflow has received;
/// `ORCHER_PAYLOAD_MAX_BYTES` overrides it. A payload over the limit is refused
/// with an error that names it, or fails the task or workflow that produced it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    /// Default 32 MiB.
    #[serde(default = "default_grpc_max_message_bytes")]
    pub grpc_max_message_bytes: usize,
    /// Default 8 MiB.
    #[serde(default = "default_payload_max_bytes")]
    pub payload_max_bytes: usize,
}

fn default_grpc_max_message_bytes() -> usize {
    orcher_core::grpc::payload_limits::DEFAULT_MAX_MESSAGE_BYTES
}

fn default_payload_max_bytes() -> usize {
    orcher_core::grpc::payload_limits::DEFAULT_PAYLOAD_MAX_BYTES
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            grpc_max_message_bytes: default_grpc_max_message_bytes(),
            payload_max_bytes: default_payload_max_bytes(),
        }
    }
}

impl LimitsConfig {
    /// The limits as the engine's services take them.
    pub fn payload_limits(&self) -> orcher_core::grpc::payload_limits::PayloadLimits {
        orcher_core::grpc::payload_limits::PayloadLimits {
            max_message_bytes: self.grpc_max_message_bytes,
            payload_max_bytes: self.payload_max_bytes,
        }
    }
}

/// Clustering settings read from the config file. Clustering itself is
/// switched on by `ORCHER_CLUSTER_ENABLED`; this holds what a file can say
/// about it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterSettings {
    /// The `host:port` other nodes reach this one at, for relaying a request to
    /// the node that owns its workflow. `ORCHER_ADVERTISE_ADDR` overrides it.
    /// Unset, the gRPC bind address is used if it names a concrete host; a
    /// wildcard bind address (`0.0.0.0`) cannot be dialed by a peer, and
    /// forwarding is then off on this node.
    #[serde(default)]
    pub advertise_address: Option<String>,
}

/// Security configuration for the orchestrator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Authentication mode: "none", "api-key", "mtls", "api-key-and-mtls"
    #[serde(default)]
    pub auth_mode: AuthMode,

    /// Whether the deployment serves one tenant (`"single"`) or many
    /// (`"multi"`): what decides whether a credential that belongs to no
    /// organisation may act. `"multi"` needs a credential to identify every
    /// caller, so it requires `auth_mode` `"api-key"` or `"api-key-and-mtls"`.
    ///
    /// Unset, the deployment serves one tenant; it refuses to start then if
    /// an active API key is bound to an organization, or if OIDC tokens name
    /// one, since those say it serves several.
    #[serde(default)]
    pub tenancy: Option<orcher_core::auth::Tenancy>,

    /// The OpenID Connect provider whose tokens identify callers: required by
    /// `auth_mode = "oidc"`, and accepted beside API keys when set with
    /// `auth_mode = "api-key"` (a token goes to the provider's keys, an API key
    /// to the api_keys table).
    #[serde(default)]
    pub oidc: Option<orcher_core::auth::OidcConfig>,

    /// TTL for cached API key validations (seconds)
    #[serde(default = "default_api_key_cache_ttl_secs")]
    pub api_key_cache_ttl_secs: u64,

    /// Maximum number of cached API key entries
    #[serde(default = "default_api_key_cache_max_entries")]
    pub api_key_cache_max_entries: u64,

    /// TLS configuration (required when auth_mode includes "mtls")
    pub tls: Option<TlsServerConfig>,
}

/// TLS configuration for the gRPC server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsServerConfig {
    /// Path to server certificate PEM file
    pub cert_path: String,

    /// Path to server private key PEM file
    pub key_path: String,

    /// Path to CA certificate PEM file (for client certificate verification)
    pub ca_cert_path: Option<String>,

    /// Require client certificates (mTLS)
    #[serde(default)]
    pub require_client_cert: bool,
}

fn default_api_key_cache_ttl_secs() -> u64 {
    300 // 5 minutes
}

fn default_api_key_cache_max_entries() -> u64 {
    10_000
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            auth_mode: AuthMode::None,
            tenancy: None,
            oidc: None,
            api_key_cache_ttl_secs: default_api_key_cache_ttl_secs(),
            api_key_cache_max_entries: default_api_key_cache_max_entries(),
            tls: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub grpc_addr: String,
    pub http_addr: String,
    pub max_concurrent_streams: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorCoreConfig {
    pub max_workers: usize,
    pub max_concurrent_tasks: usize,
    pub heartbeat_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub database_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    pub readiness_enabled: bool,
    pub liveness_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollingConfig {
    /// Claim timeout in seconds (how long a Service can hold a claim)
    /// If a Service crashes while processing, the claim will be reclaimed
    /// after this timeout.
    #[serde(default = "default_claim_timeout_secs")]
    pub claim_timeout_secs: u64,

    /// Claim query timeout in milliseconds (how long to wait for the claim query to execute)
    /// This is NOT the claim lease duration - it's how long we wait for PostgreSQL to respond.
    /// Increased from 200ms to handle PostgreSQL latency under load.
    /// Default: 2000 (2 seconds)
    #[serde(default = "default_claim_query_timeout_ms")]
    pub claim_query_timeout_ms: u64,

    /// Poll timeout in seconds (how long to wait for work)
    /// This is the long-poll timeout. Services will wait up to this duration
    /// for work to become available before returning empty.
    #[serde(default = "default_poll_timeout_secs")]
    pub poll_timeout_secs: u64,

    /// Reclaim interval in seconds (how often to reclaim expired claims)
    /// The reclaimer runs in the background to find executions with expired
    /// claims and make them available for other Services.
    #[serde(default = "default_reclaim_interval_secs")]
    pub reclaim_interval_secs: u64,

    /// Enable notification-based wake-ups for efficient polling
    /// When enabled, Services are notified immediately when work becomes
    /// available, reducing latency from poll_timeout_secs to <100ms.
    #[serde(default = "default_enable_notifications")]
    pub enable_notifications: bool,

    /// Enable sync-match dispatch: work is handed straight to a worker already
    /// waiting on its queue, in-process, instead of waiting for its next poll.
    /// Durability is unchanged: the work is an outbox row committed with the
    /// workflow state first. Needs nothing beyond PostgreSQL.
    #[serde(default = "default_use_sync_match")]
    pub use_sync_match: bool,

    /// Maximum waiters per queue (sync-match)
    /// Limits memory usage by capping the number of workers that can
    /// register as waiting for work on a single queue.
    /// Recommended: 10,000 (~2MB per queue at ~200 bytes per waiter)
    /// Default: 10,000
    #[serde(default = "default_max_waiters_per_queue")]
    pub max_waiters_per_queue: usize,

    /// How long a run released to resume is held for the worker process that
    /// had its last activation, in milliseconds. That process keeps the run's
    /// journal, and an activation going back to it carries only the entries
    /// it does not hold. 0 holds nothing. Default: 100, or
    /// `ORCHER_JOURNAL_HOLDER_HOLD_MS`.
    #[serde(default = "default_journal_holder_hold_ms")]
    pub journal_holder_hold_ms: u64,

    /// Enable worker affinity for cache locality (sync-match)
    /// Routes consecutive tasks from the same workflow to the same worker.
    /// Default: true
    #[serde(default = "default_affinity_enabled")]
    pub affinity_enabled: bool,

    /// Default affinity lease duration in seconds (sync-match)
    /// How long an affinity mapping remains valid after the last task.
    /// Default: 30 seconds
    #[serde(default = "default_affinity_lease_secs")]
    pub affinity_lease_secs: u64,

    /// Maximum affinity lease duration in seconds (sync-match)
    /// Prevents permanent affinity which could cause load imbalance.
    /// Default: 300 seconds (5 minutes)
    #[serde(default = "default_affinity_max_lease_secs")]
    pub affinity_max_lease_secs: u64,

    /// Cleanup interval for expired waiters and leases in seconds (sync-match)
    /// How often the background cleanup task runs.
    /// Default: 30 seconds
    #[serde(default = "default_routing_cleanup_interval_secs")]
    pub routing_cleanup_interval_secs: u64,

    /// Batch claim size for workflow execution polling
    /// When enabled (> 1), claims multiple workflow executions in a single
    /// database query, dramatically reducing contention when multiple workers
    /// are polling for work.
    ///
    /// Benefits:
    /// - Reduces database round-trips: 1 query claims N workflows vs N queries
    /// - Reduces lock contention: fewer concurrent SELECT FOR UPDATE operations
    /// - Better scaling: 4-8x improvement in multi-worker scenarios
    ///
    /// Recommended values:
    /// - 1: Disabled (legacy single-claim behavior)
    /// - 5-10: Good for moderate load
    /// - 10-20: Good for high load with many workers
    ///
    /// Default: 10
    #[serde(default = "default_batch_claim_size")]
    pub batch_claim_size: i32,
}

fn default_claim_timeout_secs() -> u64 {
    // Use shorter timeout for development/debug builds to speed up failure recovery
    // In production (release builds), use longer timeout to avoid false reclaims
    if cfg!(debug_assertions) {
        60 // 1 minute for dev/test - faster failure recovery
    } else {
        300 // 5 minutes for production - more tolerance for slow operations
    }
}

fn default_claim_query_timeout_ms() -> u64 {
    // Query timeout for PostgreSQL claim query execution
    // Increased from 200ms to handle PostgreSQL latency under load
    2000 // 2 seconds
}

fn default_poll_timeout_secs() -> u64 {
    60 // 1 minute
}

fn default_reclaim_interval_secs() -> u64 {
    // Use more aggressive reclaim interval for development/debug builds
    // to speed up recovery from stuck claims
    if cfg!(debug_assertions) {
        5 // 5 seconds for dev/test - faster recovery
    } else {
        10 // 10 seconds for production
    }
}

fn default_enable_notifications() -> bool {
    true
}

fn default_use_sync_match() -> bool {
    // Check environment variable for sync-match opt-in
    std::env::var("ORCHER_USE_SYNC_MATCH")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false)
}

fn default_max_waiters_per_queue() -> usize {
    10_000
}

fn default_journal_holder_hold_ms() -> u64 {
    std::env::var("ORCHER_JOURNAL_HOLDER_HOLD_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(
            orcher_storage::repositories::domains::workflow_execution::DEFAULT_JOURNAL_HOLD
                .as_millis() as u64,
        )
}

fn default_affinity_enabled() -> bool {
    true
}

fn default_affinity_lease_secs() -> u64 {
    30
}

fn default_affinity_max_lease_secs() -> u64 {
    300 // 5 minutes
}

fn default_routing_cleanup_interval_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutWatcherConfig {
    /// Enable the timeout watcher background service
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// How often to check for timeouts (in seconds)
    #[serde(default = "default_timeout_poll_interval_secs")]
    pub poll_interval_secs: u64,

    /// Maximum number of tasks to process per poll
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Enable schedule-to-start timeout detection
    #[serde(default = "default_enabled")]
    pub enable_schedule_to_start: bool,

    /// Enable start-to-close timeout detection
    #[serde(default = "default_enabled")]
    pub enable_start_to_close: bool,

    /// Enable heartbeat timeout detection
    #[serde(default = "default_enabled")]
    pub enable_heartbeat: bool,

    /// Enable workflow execution timeout detection (total duration vs `timeout_secs`)
    #[serde(default = "default_enabled")]
    pub enable_workflow_timeout: bool,

    /// Grace period before marking timeout (seconds)
    #[serde(default = "default_grace_period_secs")]
    pub grace_period_secs: i64,

    /// The heartbeat timeout, in seconds, an attempt runs under when its task
    /// sets none and the worker taking it heartbeats every task on its own
    /// (it says so on its poll). Such an attempt is timed out — and retried
    /// under its retry policy — once this long passes without a heartbeat, so
    /// a task whose worker died or whose poll response was lost is recovered
    /// even with no timeouts configured. Zero gives no default. A worker that
    /// does not heartbeat on its own is never given one. Overridden by
    /// `ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS`.
    #[serde(default = "default_heartbeat_timeout_secs")]
    pub default_heartbeat_timeout_secs: u64,
}

fn default_enabled() -> bool {
    true
}

fn default_timeout_poll_interval_secs() -> u64 {
    30
}

fn default_batch_size() -> usize {
    100
}

fn default_grace_period_secs() -> i64 {
    5
}

fn default_heartbeat_timeout_secs() -> u64 {
    orcher_core::grpc::services::execution::DEFAULT_HEARTBEAT_TIMEOUT_SECS as u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackgroundJobsConfig {
    /// Enable metrics aggregation job
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,

    /// Interval between metrics aggregation runs (in seconds)
    #[serde(default = "default_metrics_interval_secs")]
    pub metrics_interval_secs: u64,

    /// Enable data retention job
    #[serde(default = "default_retention_enabled")]
    pub retention_enabled: bool,

    /// Interval between retention runs (in seconds)
    #[serde(default = "default_retention_interval_secs")]
    pub retention_interval_secs: u64,

    /// Retention periods for different data types
    #[serde(default)]
    pub retention_days: RetentionDaysConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionDaysConfig {
    /// How many days to keep completed workflow executions
    #[serde(default = "default_workflow_executions_days")]
    pub workflow_executions: i32,

    /// How many days to keep execution logs
    #[serde(default = "default_execution_logs_days")]
    pub execution_logs: i32,

    /// How many days to keep journal entries
    #[serde(default = "default_journal_entries_days")]
    pub journal_entries: i32,

    /// How many days to keep completed task executions
    #[serde(default = "default_task_executions_days")]
    pub task_executions: i32,

    /// How many days to keep completed outbox queue tasks
    #[serde(default = "default_outbox_queue_days")]
    pub outbox_queue: i32,
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_interval_secs() -> u64 {
    3600 // 1 hour
}

fn default_retention_enabled() -> bool {
    true
}

fn default_retention_interval_secs() -> u64 {
    86400 // 1 day
}

fn default_workflow_executions_days() -> i32 {
    90 // 3 months
}

fn default_execution_logs_days() -> i32 {
    30 // 1 month
}

fn default_journal_entries_days() -> i32 {
    90 // 3 months
}

fn default_task_executions_days() -> i32 {
    90 // 3 months
}

fn default_outbox_queue_days() -> i32 {
    7 // 1 week
}

impl Default for RetentionDaysConfig {
    fn default() -> Self {
        Self {
            workflow_executions: default_workflow_executions_days(),
            execution_logs: default_execution_logs_days(),
            journal_entries: default_journal_entries_days(),
            task_executions: default_task_executions_days(),
            outbox_queue: default_outbox_queue_days(),
        }
    }
}

impl Default for BackgroundJobsConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: default_metrics_enabled(),
            metrics_interval_secs: default_metrics_interval_secs(),
            retention_enabled: default_retention_enabled(),
            retention_interval_secs: default_retention_interval_secs(),
            retention_days: RetentionDaysConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRetryConfig {
    /// Enable automatic workflow retry scheduling
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// How often to poll for failed workflows (in seconds)
    #[serde(default = "default_workflow_retry_poll_interval_secs")]
    pub poll_interval_secs: u64,

    /// Maximum number of workflows to process per poll
    #[serde(default = "default_workflow_retry_batch_size")]
    pub batch_size: usize,

    /// Also act on every workflow failure as it is written, via PostgreSQL
    /// LISTEN/NOTIFY. A scheduled retry wakes the scheduler whether or not
    /// this is set, so a retry starts when its backoff ends; the poll is the
    /// backstop for a missed notification.
    #[serde(default = "default_realtime_notifications")]
    pub enable_realtime_notifications: bool,

    /// Default maximum retry attempts for workflows
    #[serde(default = "default_workflow_max_attempts")]
    pub default_max_attempts: u32,

    /// Default initial backoff duration in seconds
    #[serde(default = "default_workflow_initial_backoff_secs")]
    pub default_initial_backoff_secs: u64,

    /// Default backoff multiplier for exponential backoff
    #[serde(default = "default_workflow_backoff_multiplier")]
    pub default_backoff_multiplier: f64,

    /// Default maximum backoff duration in seconds
    #[serde(default = "default_workflow_max_backoff_secs")]
    pub default_max_backoff_secs: u64,
}

fn default_workflow_retry_poll_interval_secs() -> u64 {
    30
}

fn default_workflow_retry_batch_size() -> usize {
    50
}

fn default_realtime_notifications() -> bool {
    false // Disabled by default for backward compatibility
}

fn default_workflow_max_attempts() -> u32 {
    3
}

fn default_workflow_initial_backoff_secs() -> u64 {
    1
}

fn default_workflow_backoff_multiplier() -> f64 {
    2.0
}

fn default_workflow_max_backoff_secs() -> u64 {
    300 // 5 minutes
}

/// The sweep that reports every child's ending to its parent and applies
/// parent close policies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildReconcilerConfig {
    /// Run the sweep. Off, only children that complete or fail through a
    /// worker reach their parent, and only if that delivery succeeds.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Seconds between passes: the longest a parent waits to hear of a child
    /// that was cancelled, timed out or terminated.
    #[serde(default = "default_child_reconciler_poll_interval_secs")]
    pub poll_interval_secs: u64,

    /// Open children examined per pass; each pass continues where the last
    /// one stopped.
    #[serde(default = "default_child_reconciler_batch_size")]
    pub batch_size: i64,
}

fn default_child_reconciler_poll_interval_secs() -> u64 {
    5
}

fn default_child_reconciler_batch_size() -> i64 {
    500
}

/// Releasing what a worker process held once it stops polling.
///
/// Every engine records, each `refresh_interval_secs`, the worker processes it
/// has seen polling (by the `worker_instance_id` their polls carry). A process
/// nobody has seen for `missed_refreshes` of those intervals is taken for
/// dead: the activations it was handed and never answered are offered to
/// other workers at once, instead of after the claim timeout, and the task
/// attempts it was running are timed out and retried once their own
/// heartbeats have stopped too. A process that was only slow has its late
/// completion acknowledged without being applied, and the next activation
/// replays the same work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLivenessConfig {
    /// Release a lapsed process's claims. Off, they wait for the claim
    /// timeout and the heartbeat timeouts, as they did before. An engine with
    /// this off still records the processes it sees, so that engines with it
    /// on do not take the workers polling it for dead. Every engine of a
    /// deployment should use the same interval and count.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Seconds between the records each engine makes of the processes it
    /// sees, and between its sweeps for lapsed ones.
    #[serde(default = "default_worker_liveness_refresh_interval_secs")]
    pub refresh_interval_secs: u64,

    /// How many refresh intervals a process may go unseen before what it
    /// holds is released: 3 × 5s = 15s by default. At least 2, so that one
    /// late record is never taken for a death.
    #[serde(default = "default_worker_liveness_missed_refreshes")]
    pub missed_refreshes: u32,
}

fn default_worker_liveness_refresh_interval_secs() -> u64 {
    orcher_core::engine::dispatch::lapsed_worker_claims::DEFAULT_REFRESH.as_secs()
}

fn default_worker_liveness_missed_refreshes() -> u32 {
    orcher_core::engine::dispatch::lapsed_worker_claims::DEFAULT_MISSED_REFRESHES
}

impl Default for WorkerLivenessConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            refresh_interval_secs: default_worker_liveness_refresh_interval_secs(),
            missed_refreshes: default_worker_liveness_missed_refreshes(),
        }
    }
}

impl WorkerLivenessConfig {
    /// How long a process goes unseen before what it holds is released.
    pub fn lapse_secs(&self) -> u64 {
        self.refresh_interval_secs * u64::from(self.missed_refreshes)
    }
}

impl Default for ChildReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            poll_interval_secs: default_child_reconciler_poll_interval_secs(),
            batch_size: default_child_reconciler_batch_size(),
        }
    }
}

impl Default for WorkflowRetryConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            poll_interval_secs: default_workflow_retry_poll_interval_secs(),
            batch_size: default_workflow_retry_batch_size(),
            enable_realtime_notifications: default_realtime_notifications(),
            default_max_attempts: default_workflow_max_attempts(),
            default_initial_backoff_secs: default_workflow_initial_backoff_secs(),
            default_backoff_multiplier: default_workflow_backoff_multiplier(),
            default_max_backoff_secs: default_workflow_max_backoff_secs(),
        }
    }
}

impl Default for TimeoutWatcherConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            poll_interval_secs: default_timeout_poll_interval_secs(),
            batch_size: default_batch_size(),
            enable_schedule_to_start: default_enabled(),
            enable_start_to_close: default_enabled(),
            enable_heartbeat: default_enabled(),
            enable_workflow_timeout: default_enabled(),
            grace_period_secs: default_grace_period_secs(),
            default_heartbeat_timeout_secs: default_heartbeat_timeout_secs(),
        }
    }
}

/// Default batch claim size for reduced database contention
fn default_batch_claim_size() -> i32 {
    10
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            claim_timeout_secs: default_claim_timeout_secs(),
            claim_query_timeout_ms: default_claim_query_timeout_ms(),
            poll_timeout_secs: default_poll_timeout_secs(),
            reclaim_interval_secs: default_reclaim_interval_secs(),
            enable_notifications: default_enable_notifications(),
            use_sync_match: default_use_sync_match(),
            max_waiters_per_queue: default_max_waiters_per_queue(),
            journal_holder_hold_ms: default_journal_holder_hold_ms(),
            affinity_enabled: default_affinity_enabled(),
            affinity_lease_secs: default_affinity_lease_secs(),
            affinity_max_lease_secs: default_affinity_max_lease_secs(),
            routing_cleanup_interval_secs: default_routing_cleanup_interval_secs(),
            batch_claim_size: default_batch_claim_size(),
        }
    }
}

impl PollingConfig {
    /// Convert to RoutingConfig for sync-match dispatch
    ///
    /// Creates a RoutingConfig from the polling configuration values.
    /// This is used when initializing PollingManager with sync-match support.
    pub fn to_routing_config(&self) -> RoutingConfig {
        RoutingConfig {
            enabled: self.use_sync_match,
            claim_timeout_secs: self.claim_timeout_secs as i64,
            max_waiters_per_queue: self.max_waiters_per_queue,
            affinity: AffinityConfig {
                enabled: self.affinity_enabled,
                default_lease_secs: self.affinity_lease_secs as i64,
                max_lease_secs: self.affinity_max_lease_secs as i64,
            },
            cleanup_interval_secs: self.routing_cleanup_interval_secs,
            sharding: Default::default(),
        }
    }

    /// Check if sync-match dispatch is enabled
    #[allow(dead_code)]
    pub fn is_sync_match_enabled(&self) -> bool {
        self.use_sync_match
    }
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                // This machine only: a deployment that serves others says so,
                // and authenticates them (see `exposes_unauthenticated_api`).
                grpc_addr: "127.0.0.1:50051".to_string(),
                http_addr: "127.0.0.1:8080".to_string(),
                max_concurrent_streams: 1000,
            },
            orchestrator: OrchestratorCoreConfig {
                max_workers: 100,
                max_concurrent_tasks: 1000,
                heartbeat_timeout_secs: 90,
            },
            storage: StorageConfig {
                database_url: "postgresql://localhost/orcher".to_string(),
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "json".to_string(),
            },
            health: HealthConfig {
                readiness_enabled: true,
                liveness_enabled: true,
            },
            polling: PollingConfig::default(),
            timeout_watcher: TimeoutWatcherConfig::default(),
            workflow_retry: WorkflowRetryConfig::default(),
            child_reconciler: ChildReconcilerConfig::default(),
            worker_liveness: WorkerLivenessConfig::default(),
            background_jobs: BackgroundJobsConfig::default(),
            security: SecurityConfig::default(),
            cluster: ClusterSettings::default(),
            limits: LimitsConfig::default(),
            ui: UiConfig::default(),
        }
    }
}

impl OrchestratorConfig {
    /// Create a configuration optimized for development and testing
    ///
    /// This configuration uses shorter timeouts and more aggressive reclaim
    /// intervals to speed up failure recovery during development.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = OrchestratorConfig::for_development();
    /// // claim_timeout_secs: 60 (vs 300 in production)
    /// // reclaim_interval_secs: 5 (vs 10 in production)
    /// ```
    #[allow(dead_code)]
    pub fn for_development() -> Self {
        let mut config = Self::default();
        config.polling.claim_timeout_secs = 60; // 1 minute
        config.polling.reclaim_interval_secs = 5; // 5 seconds - aggressive reclaim
        config.polling.poll_timeout_secs = 30; // 30 seconds
        config
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents =
            std::fs::read_to_string(path.as_ref()).context("Failed to read config file")?;
        let config: Self = toml::from_str(&contents).context("Failed to parse config file")?;
        Ok(config)
    }

    /// Whether the API would serve callers it does not authenticate on an
    /// address other machines can reach: authentication off, and the gRPC
    /// address, or the HTTP address the REST API is served on, not this
    /// machine's loopback. The engine refuses to start so unless told to
    /// (`--insecure`), because the API can start, cancel and read every
    /// workflow.
    ///
    /// Inside a container the engine listens on every interface and the port
    /// is published, or not, outside it, where the engine cannot see; a
    /// container run without authentication therefore has to say it means to.
    pub fn exposes_unauthenticated_api(&self) -> bool {
        self.security.auth_mode == AuthMode::None
            && !(is_loopback(&self.server.grpc_addr) && is_loopback(&self.server.http_addr))
    }

    /// Whether the engine serves the REST API on its HTTP address. It does
    /// whenever it can identify a REST caller as it would a gRPC one: with no
    /// authentication, an API key or a token. A client certificate is checked
    /// only on gRPC connections, so a deployment that requires one serves no
    /// REST API rather than one that asks for less.
    pub fn serves_rest_api(&self) -> bool {
        matches!(
            self.security.auth_mode,
            AuthMode::None | AuthMode::ApiKey | AuthMode::Oidc
        )
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.storage.database_url.is_empty(),
            "database_url cannot be empty"
        );
        anyhow::ensure!(
            !self.server.grpc_addr.is_empty(),
            "grpc_addr cannot be empty"
        );
        anyhow::ensure!(
            !self.server.http_addr.is_empty(),
            "http_addr cannot be empty"
        );

        // Validate polling configuration
        anyhow::ensure!(
            self.polling.claim_timeout_secs > 0,
            "claim_timeout_secs must be greater than 0"
        );
        anyhow::ensure!(
            self.polling.poll_timeout_secs > 0,
            "poll_timeout_secs must be greater than 0"
        );
        anyhow::ensure!(
            self.polling.reclaim_interval_secs > 0,
            "reclaim_interval_secs must be greater than 0"
        );
        anyhow::ensure!(
            self.polling.claim_timeout_secs > self.polling.reclaim_interval_secs,
            "claim_timeout_secs ({}) must be greater than reclaim_interval_secs ({})",
            self.polling.claim_timeout_secs,
            self.polling.reclaim_interval_secs
        );

        if self.worker_liveness.enabled {
            anyhow::ensure!(
                self.worker_liveness.refresh_interval_secs > 0,
                "worker_liveness.refresh_interval_secs must be greater than 0"
            );
            anyhow::ensure!(
                self.worker_liveness.missed_refreshes >= 2,
                "worker_liveness.missed_refreshes must be at least 2"
            );
            anyhow::ensure!(
                self.worker_liveness.lapse_secs() < self.polling.claim_timeout_secs,
                "worker_liveness: a process unseen for {}s ({} × {}s) must lapse before its \
                 claims time out (claim_timeout_secs {})",
                self.worker_liveness.lapse_secs(),
                self.worker_liveness.missed_refreshes,
                self.worker_liveness.refresh_interval_secs,
                self.polling.claim_timeout_secs
            );
        }

        // Validate timeout watcher configuration
        if self.timeout_watcher.enabled {
            anyhow::ensure!(
                self.timeout_watcher.poll_interval_secs > 0,
                "timeout_watcher.poll_interval_secs must be greater than 0"
            );
            anyhow::ensure!(
                self.timeout_watcher.batch_size > 0,
                "timeout_watcher.batch_size must be greater than 0"
            );
            anyhow::ensure!(
                self.timeout_watcher.grace_period_secs >= 0,
                "timeout_watcher.grace_period_secs must be non-negative"
            );
        }

        // Validate workflow retry configuration
        if self.workflow_retry.enabled {
            anyhow::ensure!(
                self.workflow_retry.poll_interval_secs > 0,
                "workflow_retry.poll_interval_secs must be greater than 0"
            );
            anyhow::ensure!(
                self.workflow_retry.batch_size > 0,
                "workflow_retry.batch_size must be greater than 0"
            );
            anyhow::ensure!(
                self.workflow_retry.default_max_attempts > 0,
                "workflow_retry.default_max_attempts must be greater than 0"
            );
            anyhow::ensure!(
                self.workflow_retry.default_backoff_multiplier >= 1.0,
                "workflow_retry.default_backoff_multiplier must be >= 1.0"
            );
            anyhow::ensure!(
                self.workflow_retry.default_max_backoff_secs
                    >= self.workflow_retry.default_initial_backoff_secs,
                "workflow_retry.default_max_backoff_secs must be >= default_initial_backoff_secs"
            );
        }

        if self.child_reconciler.enabled {
            anyhow::ensure!(
                self.child_reconciler.poll_interval_secs > 0,
                "child_reconciler.poll_interval_secs must be greater than 0"
            );
            anyhow::ensure!(
                self.child_reconciler.batch_size > 0,
                "child_reconciler.batch_size must be greater than 0"
            );
        }

        // Validate background jobs configuration
        if self.background_jobs.metrics_enabled {
            anyhow::ensure!(
                self.background_jobs.metrics_interval_secs > 0,
                "background_jobs.metrics_interval_secs must be greater than 0"
            );
        }

        if self.background_jobs.retention_enabled {
            anyhow::ensure!(
                self.background_jobs.retention_interval_secs > 0,
                "background_jobs.retention_interval_secs must be greater than 0"
            );
            anyhow::ensure!(
                self.background_jobs.retention_days.workflow_executions > 0,
                "background_jobs.retention_days.workflow_executions must be greater than 0"
            );
            anyhow::ensure!(
                self.background_jobs.retention_days.execution_logs > 0,
                "background_jobs.retention_days.execution_logs must be greater than 0"
            );
            anyhow::ensure!(
                self.background_jobs.retention_days.journal_entries > 0,
                "background_jobs.retention_days.journal_entries must be greater than 0"
            );
            anyhow::ensure!(
                self.background_jobs.retention_days.task_executions > 0,
                "background_jobs.retention_days.task_executions must be greater than 0"
            );
            anyhow::ensure!(
                self.background_jobs.retention_days.outbox_queue > 0,
                "background_jobs.retention_days.outbox_queue must be greater than 0"
            );
        }

        // Validate sync-match configuration
        if self.polling.use_sync_match {
            anyhow::ensure!(
                self.polling.max_waiters_per_queue > 0,
                "max_waiters_per_queue must be greater than 0"
            );
            anyhow::ensure!(
                self.polling.affinity_lease_secs > 0,
                "affinity_lease_secs must be greater than 0"
            );
            anyhow::ensure!(
                self.polling.affinity_max_lease_secs >= self.polling.affinity_lease_secs,
                "affinity_max_lease_secs must be >= affinity_lease_secs"
            );
            anyhow::ensure!(
                self.polling.routing_cleanup_interval_secs > 0,
                "routing_cleanup_interval_secs must be greater than 0"
            );
        }

        // A payload must fit in a message with room for the activation around
        // it, and a message must at least hold what every SDK sends by default.
        anyhow::ensure!(
            self.limits.grpc_max_message_bytes
                >= orcher_core::grpc::payload_limits::LEGACY_RECEIVE_LIMIT_BYTES,
            "limits.grpc_max_message_bytes ({}) must be at least 4 MiB (4194304), the gRPC default",
            self.limits.grpc_max_message_bytes
        );
        anyhow::ensure!(
            self.limits.payload_max_bytes > 0
                && self.limits.payload_max_bytes < self.limits.grpc_max_message_bytes,
            "limits.payload_max_bytes ({}) must be greater than 0 and less than \
             limits.grpc_max_message_bytes ({})",
            self.limits.payload_max_bytes,
            self.limits.grpc_max_message_bytes
        );

        // Validate security configuration
        anyhow::ensure!(
            self.security.tenancy != Some(orcher_core::auth::Tenancy::Multi)
                || matches!(
                    self.security.auth_mode,
                    AuthMode::ApiKey | AuthMode::ApiKeyAndMtls | AuthMode::Oidc
                ),
            "security.tenancy 'multi' needs every caller identified by a credential that \
             names its organization: set security.auth_mode to 'api-key', \
             'api-key-and-mtls' or 'oidc'"
        );
        anyhow::ensure!(
            self.security.auth_mode != AuthMode::Oidc || self.security.oidc.is_some(),
            "security.auth_mode 'oidc' needs the provider: set [security.oidc] issuer and audience"
        );
        if let Some(oidc) = &self.security.oidc {
            anyhow::ensure!(
                oidc.issuer.starts_with("https://") || oidc.issuer.starts_with("http://"),
                "security.oidc.issuer must be the provider's issuer URL"
            );
            anyhow::ensure!(
                !oidc.audience.is_empty(),
                "security.oidc.audience must name this engine at the provider"
            );
        }
        if matches!(
            self.security.auth_mode,
            AuthMode::Mtls | AuthMode::ApiKeyAndMtls
        ) {
            anyhow::ensure!(
                self.security.tls.is_some(),
                "TLS configuration is required when auth_mode is 'mtls' or 'api-key-and-mtls'"
            );
        }

        if let Some(ref tls) = self.security.tls {
            anyhow::ensure!(!tls.cert_path.is_empty(), "tls.cert_path cannot be empty");
            anyhow::ensure!(!tls.key_path.is_empty(), "tls.key_path cannot be empty");
            if tls.require_client_cert {
                anyhow::ensure!(
                    tls.ca_cert_path.is_some(),
                    "tls.ca_cert_path is required when require_client_cert is true"
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = OrchestratorConfig::default();
        assert_eq!(config.server.grpc_addr, "127.0.0.1:50051");
        assert_eq!(config.server.http_addr, "127.0.0.1:8080");
        assert_eq!(config.orchestrator.max_workers, 100);
    }

    #[test]
    fn a_payload_limit_must_fit_inside_the_message_limit() {
        let mut config = OrchestratorConfig::default();
        assert_eq!(config.limits.grpc_max_message_bytes, 32 * 1024 * 1024);
        assert_eq!(config.limits.payload_max_bytes, 8 * 1024 * 1024);
        assert!(config.validate().is_ok());

        config.limits.payload_max_bytes = config.limits.grpc_max_message_bytes;
        assert!(config.validate().is_err());

        config.limits = LimitsConfig::default();
        config.limits.grpc_max_message_bytes = 1024 * 1024;
        config.limits.payload_max_bytes = 512 * 1024;
        assert!(config.validate().is_err(), "below the gRPC default");
    }

    #[test]
    fn the_limits_are_read_from_a_config_file() {
        let config: OrchestratorConfig = toml::from_str(
            r#"
            [server]
            grpc_addr = "0.0.0.0:50051"
            http_addr = "0.0.0.0:8001"
            max_concurrent_streams = 1000
            [orchestrator]
            max_workers = 1
            max_concurrent_tasks = 1
            heartbeat_timeout_secs = 90
            [storage]
            database_url = "postgresql://localhost/orcher"
            [logging]
            level = "info"
            format = "json"
            [health]
            readiness_enabled = true
            liveness_enabled = true
            [limits]
            grpc_max_message_bytes = 67108864
            payload_max_bytes = 16777216
            "#,
        )
        .expect("parse");
        assert_eq!(config.limits.grpc_max_message_bytes, 64 * 1024 * 1024);
        assert_eq!(config.limits.payload_max_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn test_validate_success() {
        let config = OrchestratorConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_database_url() {
        let mut config = OrchestratorConfig::default();
        config.storage.database_url = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_workflow_retry_config_defaults() {
        let config = WorkflowRetryConfig::default();
        assert!(config.enabled);
        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.default_max_attempts, 3);
        assert_eq!(config.default_initial_backoff_secs, 1);
        assert_eq!(config.default_backoff_multiplier, 2.0);
        assert_eq!(config.default_max_backoff_secs, 300);
    }

    #[test]
    fn test_validate_workflow_retry_config() {
        let config = OrchestratorConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_workflow_retry_zero_poll_interval() {
        let mut config = OrchestratorConfig::default();
        config.workflow_retry.poll_interval_secs = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("workflow_retry.poll_interval_secs must be greater than 0"));
    }

    #[test]
    fn test_validate_workflow_retry_zero_batch_size() {
        let mut config = OrchestratorConfig::default();
        config.workflow_retry.batch_size = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("workflow_retry.batch_size must be greater than 0"));
    }

    #[test]
    fn test_validate_workflow_retry_zero_max_attempts() {
        let mut config = OrchestratorConfig::default();
        config.workflow_retry.default_max_attempts = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("workflow_retry.default_max_attempts must be greater than 0"));
    }

    #[test]
    fn test_validate_workflow_retry_invalid_backoff_multiplier() {
        let mut config = OrchestratorConfig::default();
        config.workflow_retry.default_backoff_multiplier = 0.5;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("workflow_retry.default_backoff_multiplier must be >= 1.0"));
    }

    #[test]
    fn test_validate_workflow_retry_max_backoff_less_than_initial() {
        let mut config = OrchestratorConfig::default();
        config.workflow_retry.default_initial_backoff_secs = 100;
        config.workflow_retry.default_max_backoff_secs = 50;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "workflow_retry.default_max_backoff_secs must be >= default_initial_backoff_secs"
        ));
    }

    #[test]
    fn test_polling_config_defaults() {
        let config = PollingConfig::default();
        // claim_timeout_secs is environment-aware: 60s for debug, 300s for release
        let expected_claim_timeout = if cfg!(debug_assertions) { 60 } else { 300 };
        assert_eq!(config.claim_timeout_secs, expected_claim_timeout);
        assert_eq!(config.poll_timeout_secs, 60);
        // reclaim_interval_secs is environment-aware: 5s for debug, 10s for release
        let expected_reclaim_interval = if cfg!(debug_assertions) { 5 } else { 10 };
        assert_eq!(config.reclaim_interval_secs, expected_reclaim_interval);
        assert!(config.enable_notifications);
    }

    #[test]
    fn test_for_development_config() {
        let config = OrchestratorConfig::for_development();
        assert_eq!(config.polling.claim_timeout_secs, 60);
        assert_eq!(config.polling.reclaim_interval_secs, 5); // Aggressive reclaim for dev
        assert_eq!(config.polling.poll_timeout_secs, 30);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_polling_config() {
        let config = OrchestratorConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_claim_timeout_too_short() {
        let mut config = OrchestratorConfig::default();
        config.polling.claim_timeout_secs = 5;
        config.polling.reclaim_interval_secs = 10;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_zero_claim_timeout() {
        let mut config = OrchestratorConfig::default();
        config.polling.claim_timeout_secs = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("claim_timeout_secs must be greater than 0"));
    }

    #[test]
    fn test_timeout_watcher_config_defaults() {
        let config = TimeoutWatcherConfig::default();
        assert!(config.enabled);
        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.batch_size, 100);
        assert!(config.enable_schedule_to_start);
        assert!(config.enable_start_to_close);
        assert!(config.enable_heartbeat);
        assert!(config.enable_workflow_timeout);
        assert_eq!(config.grace_period_secs, 5);
        assert_eq!(config.default_heartbeat_timeout_secs, 60);
    }

    #[test]
    fn test_validate_timeout_watcher_config() {
        let config = OrchestratorConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_timeout_watcher_zero_poll_interval() {
        let mut config = OrchestratorConfig::default();
        config.timeout_watcher.poll_interval_secs = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("timeout_watcher.poll_interval_secs must be greater than 0"));
    }

    #[test]
    fn test_validate_timeout_watcher_zero_batch_size() {
        let mut config = OrchestratorConfig::default();
        config.timeout_watcher.batch_size = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("timeout_watcher.batch_size must be greater than 0"));
    }

    // ========================================================================
    // Sync-Match Configuration Tests
    // ========================================================================

    #[test]
    fn test_sync_match_config_defaults() {
        let config = PollingConfig::default();
        // Sync-match defaults (disabled by default unless env var set)
        assert!(!config.use_sync_match || std::env::var("ORCHER_USE_SYNC_MATCH").is_ok());
        assert_eq!(config.max_waiters_per_queue, 10_000);
        assert!(config.affinity_enabled);
        assert_eq!(config.affinity_lease_secs, 30);
        assert_eq!(config.affinity_max_lease_secs, 300);
        assert_eq!(config.routing_cleanup_interval_secs, 30);
    }

    #[test]
    fn test_sync_match_to_routing_config() {
        let polling = PollingConfig {
            use_sync_match: true,
            max_waiters_per_queue: 5000,
            affinity_enabled: true,
            affinity_lease_secs: 60,
            affinity_max_lease_secs: 600,
            routing_cleanup_interval_secs: 45,
            ..Default::default()
        };

        let routing = polling.to_routing_config();

        assert!(routing.enabled);
        assert_eq!(routing.max_waiters_per_queue, 5000);
        assert!(routing.affinity.enabled);
        assert_eq!(routing.affinity.default_lease_secs, 60);
        assert_eq!(routing.affinity.max_lease_secs, 600);
        assert_eq!(routing.cleanup_interval_secs, 45);
    }

    /// Sync-match hands work over in-process, with the outbox in PostgreSQL
    /// as its durability; it needs no Redis, and enabling it must not ask
    /// for any.
    #[test]
    fn sync_match_needs_no_redis() {
        let mut config = OrchestratorConfig::default();
        config.polling.use_sync_match = true;
        assert!(config.validate().is_ok(), "{:?}", config.validate());
    }

    #[test]
    fn test_validate_sync_match_max_waiters() {
        let mut config = OrchestratorConfig::default();
        config.polling.use_sync_match = true;
        config.polling.max_waiters_per_queue = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("max_waiters_per_queue must be greater than 0"));
    }

    #[test]
    fn test_validate_sync_match_affinity_lease() {
        let mut config = OrchestratorConfig::default();
        config.polling.use_sync_match = true;
        config.polling.affinity_lease_secs = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("affinity_lease_secs must be greater than 0"));
    }

    #[test]
    fn test_validate_sync_match_max_lease_less_than_default() {
        let mut config = OrchestratorConfig::default();
        config.polling.use_sync_match = true;
        config.polling.affinity_lease_secs = 100;
        config.polling.affinity_max_lease_secs = 50;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("affinity_max_lease_secs must be >= affinity_lease_secs"));
    }

    #[test]
    fn test_validate_sync_match_valid_config() {
        let mut config = OrchestratorConfig::default();
        config.polling.use_sync_match = true;

        let result = config.validate();
        assert!(result.is_ok());
    }
}

/// Whether `addr` (`host:port`) is this machine's loopback, so that only
/// processes on it can connect.
fn is_loopback(addr: &str) -> bool {
    if let Ok(socket) = addr.parse::<std::net::SocketAddr>() {
        return socket.ip().is_loopback();
    }
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    host.eq_ignore_ascii_case("localhost")
}

#[cfg(test)]
mod section_defaults {
    use super::OrchestratorConfig;

    /// The file shipped in infra/k8s/deploy/configmap.yaml names only the
    /// sections an operator has an opinion about. It must parse.
    #[test]
    fn a_file_without_the_tuning_sections_parses() {
        let toml = r#"
            [server]
            grpc_addr = "0.0.0.0:50051"
            http_addr = "0.0.0.0:8080"
            max_concurrent_streams = 1000

            [orchestrator]
            max_workers = 100
            max_concurrent_tasks = 1000
            heartbeat_timeout_secs = 90

            [storage]
            database_url = "postgresql://example"

            [logging]
            level = "info"
            format = "json"

            [health]
            readiness_enabled = true
            liveness_enabled = true
        "#;
        let config: OrchestratorConfig = toml::from_str(toml).expect("shipped shape parses");
        assert_eq!(
            config.polling.claim_timeout_secs,
            super::PollingConfig::default().claim_timeout_secs
        );
        assert_eq!(config.security.auth_mode, super::AuthMode::None);
        // Files written before tenancy was declared leave it to be inferred.
        assert_eq!(config.security.tenancy, None);
        // Files written before the child reconciler existed get it, on.
        assert!(config.child_reconciler.enabled);
        assert_eq!(config.child_reconciler.poll_interval_secs, 5);
        assert_eq!(config.child_reconciler.batch_size, 500);
        // And the release of a dead worker process's claims, on.
        assert!(config.worker_liveness.enabled);
        assert_eq!(config.worker_liveness.lapse_secs(), 15);
    }

    #[test]
    fn a_worker_must_lapse_after_two_refreshes_and_before_its_claims_time_out() {
        let mut config = OrchestratorConfig::default();
        assert!(config.validate().is_ok());
        config.worker_liveness.missed_refreshes = 1;
        assert!(config.validate().is_err());
        config.worker_liveness.missed_refreshes = 3;
        config.worker_liveness.refresh_interval_secs = 0;
        assert!(config.validate().is_err());
        config.worker_liveness.refresh_interval_secs = config.polling.claim_timeout_secs;
        assert!(config.validate().is_err());
        // Off, it is not held to any of it.
        config.worker_liveness.enabled = false;
        assert!(config.validate().is_ok());
    }

    /// Many tenants are told apart by their credentials, so `multi` needs
    /// every caller to present one that names its organisation.
    #[test]
    fn many_tenants_need_api_keys() {
        use orcher_core::auth::Tenancy;
        let mut config = OrchestratorConfig::default();
        config.security.tenancy = Some(Tenancy::Multi);
        for mode in [super::AuthMode::None, super::AuthMode::Mtls] {
            config.security.auth_mode = mode;
            let error = config.validate().unwrap_err().to_string();
            assert!(error.contains("security.tenancy 'multi'"), "{error}");
        }
        config.security.auth_mode = super::AuthMode::ApiKey;
        assert!(config.validate().is_ok());

        config.security.tenancy = Some(Tenancy::Single);
        config.security.auth_mode = super::AuthMode::None;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn tenancy_is_read_from_the_security_section() {
        let security: super::SecurityConfig =
            toml::from_str("auth_mode = \"api-key\"\ntenancy = \"multi\"").unwrap();
        assert_eq!(security.tenancy, Some(orcher_core::auth::Tenancy::Multi));
        assert!(toml::from_str::<super::SecurityConfig>("tenancy = \"several\"").is_err());
    }

    #[test]
    fn an_api_without_authentication_is_exposed_unless_it_listens_on_loopback() {
        let mut config = OrchestratorConfig::default();
        for addr in ["127.0.0.1:50051", "[::1]:50051", "localhost:50051"] {
            config.server.grpc_addr = addr.into();
            assert!(!config.exposes_unauthenticated_api(), "{addr}");
        }
        for addr in [
            "0.0.0.0:50051",
            "[::]:50051",
            "10.0.0.5:50051",
            "orcher:50051",
        ] {
            config.server.grpc_addr = addr.into();
            assert!(config.exposes_unauthenticated_api(), "{addr}");
        }
        // The REST API is the same API, on the HTTP address.
        config.server.grpc_addr = "127.0.0.1:50051".into();
        config.server.http_addr = "0.0.0.0:8080".into();
        assert!(config.exposes_unauthenticated_api());
        // Authenticated callers may come from anywhere.
        config.server.grpc_addr = "0.0.0.0:50051".into();
        for mode in [
            super::AuthMode::ApiKey,
            super::AuthMode::Mtls,
            super::AuthMode::ApiKeyAndMtls,
        ] {
            config.security.auth_mode = mode;
            assert!(!config.exposes_unauthenticated_api());
        }
    }

    #[test]
    fn the_engine_listens_on_this_machine_unless_told_otherwise() {
        assert!(!OrchestratorConfig::default().exposes_unauthenticated_api());
    }

    #[test]
    fn oidc_needs_its_provider_and_identifies_tenants() {
        use orcher_core::auth::{OidcConfig, Tenancy};
        let provider = OidcConfig {
            issuer: "https://idp.example.com/realms/acme".into(),
            audience: "orcher".into(),
            organization_claim: Some("org_id".into()),
            scopes_claim: None,
            leeway_secs: 60,
        };
        let mut config = OrchestratorConfig::default();
        config.security.auth_mode = super::AuthMode::Oidc;
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("[security.oidc]"), "{error}");

        config.security.oidc = Some(provider.clone());
        config.security.tenancy = Some(Tenancy::Multi);
        assert!(config.validate().is_ok());
        // An authenticated API, wherever it listens.
        config.server.grpc_addr = "0.0.0.0:50051".into();
        assert!(!config.exposes_unauthenticated_api());

        config.security.oidc = Some(OidcConfig {
            issuer: "idp.example.com".into(),
            ..provider
        });
        assert!(config.validate().is_err());
    }

    #[test]
    fn the_oidc_provider_is_read_from_the_security_section() {
        let security: super::SecurityConfig = toml::from_str(
            "auth_mode = \"oidc\"\n[oidc]\nissuer = \"https://idp.example.com/realms/acme\"\naudience = \"orcher\"\norganization_claim = \"org_id\"",
        )
        .unwrap();
        assert_eq!(security.auth_mode, super::AuthMode::Oidc);
        let oidc = security.oidc.unwrap();
        assert_eq!(oidc.audience, "orcher");
        assert_eq!(oidc.organization_claim.as_deref(), Some("org_id"));
        assert_eq!(oidc.leeway_secs, 60);
    }
}
