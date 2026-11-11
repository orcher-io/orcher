//! ORCHER Orchestrator Service
//!
//! Central coordination service that manages workflow execution,
//! task scheduling, and worker coordination using the orcher-core library.

use anyhow::Result;
use clap::Parser;
use orcher_monitoring::logging::initialize_logging;
use std::path::PathBuf;
use tracing::{debug, info, warn};

mod admin;
mod config;
mod health;
mod server;
mod shutdown;
mod ui;

/// ORCHER Orchestrator Service
#[derive(Parser, Debug)]
#[command(name = "orcher-orchestrator")]
#[command(about = "ORCHER central orchestration service", long_about = None)]
#[command(version)]
struct Cli {
    /// Configuration file path
    #[arg(short, long, value_name = "FILE", env = "ORCHER_ORCHESTRATOR_CONFIG")]
    config: Option<PathBuf>,

    /// gRPC server bind address
    #[arg(long, env = "GRPC_BIND_ADDR")]
    grpc_addr: Option<String>,

    /// HTTP server bind address (for health/metrics)
    #[arg(long, env = "HTTP_BIND_ADDR")]
    http_addr: Option<String>,

    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Workflow coordination mode (local or distributed)
    #[arg(long, env = "COORDINATION_MODE")]
    coordination_mode: Option<String>,

    /// Log level
    #[arg(long, default_value = "info", env = "LOG_LEVEL")]
    log_level: String,

    /// Serve the gRPC API without authentication on an address other machines
    /// can reach. Refused without this, because anyone who can connect can
    /// start, cancel and read every workflow. For a container whose port is
    /// published only to the host's loopback, or a private network; otherwise
    /// set `security.auth_mode`.
    #[arg(long, env = "ORCHER_INSECURE")]
    insecure: bool,

    /// Do not serve the dashboard on the HTTP port (same as `ui.enabled =
    /// false`): the REST API, health and metrics only.
    #[arg(long)]
    no_ui: bool,

    /// Log format (compact, json, or pretty)
    #[arg(long, default_value = "compact", env = "LOG_FORMAT")]
    log_format: String,

    /// Serve the engine when omitted.
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Manage the engine's API keys in its database: issue a deployment's
    /// first key, or recover one that has lost them all.
    #[command(subcommand)]
    ApiKey(admin::ApiKeyCommand),
}

/// A worker that has not heartbeated for this long is reported degraded: it
/// is late, not gone. The SDKs heartbeat every 10 s.
const WORKER_DEGRADED_AFTER_SECS: i64 = 60;

/// And this long without one is reported offline. Matches what the console
/// tells the reader about staleness.
const WORKER_OFFLINE_AFTER_SECS: i64 = 300;

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file from workspace root (best-effort, don't fail if missing)
    let mut env_loaded = false;
    for env_path in &[".env", "../.env", "../../.env"] {
        if std::path::Path::new(env_path).exists() {
            if let Err(_e) = dotenvy::from_path(env_path) {
                // Continue trying other paths
            } else {
                env_loaded = true;
                break;
            }
        }
    }

    let mut cli = Cli::parse();

    // An administrative command says what it did itself; the log carries only
    // what went wrong.
    if let Some(Command::ApiKey(command)) = cli.command.take() {
        initialize_logging("orchestrator", "warn", &cli.log_format)?;
        let config = load_configuration(&cli)?;
        return admin::run(&config, command).await;
    }

    // Initialize logging using centralized orcher-monitoring
    initialize_logging("orchestrator", &cli.log_level, &cli.log_format)?;

    if !env_loaded {
        debug!("No .env file found (checked .env, ../.env, ../../.env)");
    }

    if std::env::var("DATABASE_URL").is_err() {
        warn!("DATABASE_URL is not set — database connection will fail");
    }

    info!(
        service = "orchestrator",
        version = env!("CARGO_PKG_VERSION"),
        "Starting ORCHER Orchestrator Service"
    );

    // Load configuration
    let config = load_configuration(&cli)?;
    refuse_unauthenticated_exposure(&config, cli.insecure)?;
    info!(
        grpc_addr = %config.server.grpc_addr,
        http_addr = %config.server.http_addr,
        "Configuration loaded successfully"
    );

    // Run the service
    run_service(config).await
}

/// The heartbeat timeout an attempt of a worker that heartbeats on its own runs
/// under when its task sets none: `ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS` if
/// set, `timeout_watcher.default_heartbeat_timeout_secs` otherwise. Zero, or a
/// timeout watcher that is off, gives none: nothing would enforce it.
fn default_heartbeat_timeout(config: &config::OrchestratorConfig) -> Option<std::time::Duration> {
    let secs = std::env::var("ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(config.timeout_watcher.default_heartbeat_timeout_secs);
    let enforced = config.timeout_watcher.enabled && config.timeout_watcher.enable_heartbeat;
    (enforced && secs > 0).then(|| std::time::Duration::from_secs(secs))
}

/// Refuse to serve the API without authentication on an address other
/// machines can reach, unless told to (`--insecure`).
fn refuse_unauthenticated_exposure(
    config: &config::OrchestratorConfig,
    insecure: bool,
) -> Result<()> {
    if config.exposes_unauthenticated_api() {
        anyhow::ensure!(
            insecure,
            "The API would listen on {} (gRPC) and {} (HTTP) without authentication, open to \
             anyone who can reach those addresses. Set security.auth_mode (for example \
             \"api-key\"), listen on 127.0.0.1, or pass --insecure (ORCHER_INSECURE=true) to run \
             it open on purpose: in a container published only to the host's loopback, or on a \
             private network.",
            config.server.grpc_addr,
            config.server.http_addr
        );
        warn!(
            grpc_addr = %config.server.grpc_addr,
            http_addr = %config.server.http_addr,
            "The API is serving without authentication on an address other machines can reach \
             (--insecure). Anyone who can connect can start, cancel and read every workflow."
        );
    }

    Ok(())
}

fn load_configuration(cli: &Cli) -> Result<config::OrchestratorConfig> {
    let mut config = if let Some(config_path) = &cli.config {
        config::OrchestratorConfig::load_from_file(config_path)?
    } else {
        config::OrchestratorConfig::default()
    };

    // Override with environment variables (highest priority after CLI)
    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        info!(
            source = "DATABASE_URL env var",
            "Using database URL from environment"
        );
        config.storage.database_url = database_url;
    }

    // The timeout watcher's pace, for an engine that must time a silent task
    // out within seconds — the contract suite's, say — without a config file.
    let env_number = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
    };
    if let Some(secs) = env_number("ORCHER_TIMEOUT_WATCHER_POLL_INTERVAL_SECS").filter(|s| *s > 0) {
        config.timeout_watcher.poll_interval_secs = secs as u64;
    }
    if let Some(secs) = env_number("ORCHER_TIMEOUT_WATCHER_GRACE_SECS").filter(|s| *s >= 0) {
        config.timeout_watcher.grace_period_secs = secs;
    }
    // The workflow retry scheduler's backstop poll. A scheduled retry wakes
    // the scheduler itself; the poll catches what that announcement missed.
    if let Some(secs) = env_number("ORCHER_WORKFLOW_RETRY_POLL_INTERVAL_SECS").filter(|s| *s > 0) {
        config.workflow_retry.poll_interval_secs = secs as u64;
    }
    // How soon a worker process that stops polling has its claims released.
    if let Ok(on) = std::env::var("ORCHER_WORKER_LIVENESS_ENABLED") {
        config.worker_liveness.enabled = !matches!(
            on.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off"
        );
    }
    if let Some(secs) = env_number("ORCHER_WORKER_LIVENESS_REFRESH_SECS").filter(|s| *s > 0) {
        config.worker_liveness.refresh_interval_secs = secs as u64;
    }
    if let Some(missed) = env_number("ORCHER_WORKER_LIVENESS_MISSED_REFRESHES").filter(|n| *n > 0) {
        config.worker_liveness.missed_refreshes = missed as u32;
    }

    // How large a message and a payload may be.
    if let Some(bytes) = env_number("ORCHER_GRPC_MAX_MESSAGE_BYTES").filter(|n| *n > 0) {
        config.limits.grpc_max_message_bytes = bytes as usize;
    }
    if let Some(bytes) = env_number("ORCHER_PAYLOAD_MAX_BYTES").filter(|n| *n > 0) {
        config.limits.payload_max_bytes = bytes as usize;
    }

    // The dashboard
    if let Ok(on) = std::env::var("ORCHER_UI_ENABLED") {
        config.ui.enabled = !matches!(
            on.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off"
        );
    }
    if let Ok(dir) = std::env::var("ORCHER_UI_DIR") {
        if !dir.trim().is_empty() {
            config.ui.dir = dir.trim().to_string();
        }
    }

    // Override with CLI arguments (highest priority)
    if cli.no_ui {
        config.ui.enabled = false;
    }
    if let Some(grpc_addr) = &cli.grpc_addr {
        config.server.grpc_addr = grpc_addr.clone();
    }
    if let Some(http_addr) = &cli.http_addr {
        config.server.http_addr = http_addr.clone();
    }
    if let Some(database_url) = &cli.database_url {
        config.storage.database_url = database_url.clone();
    }

    // Override coordination mode if provided
    if let Some(coordination_mode) = &cli.coordination_mode {
        // This would update the workflow coordination config
        // For now, just log it
        info!(coordination_mode = %coordination_mode, "Workflow coordination mode override");
    }

    // Settings that used to choose a Redis dispatch path. Task delivery is
    // PostgreSQL-only, so nothing reads them; say so rather than ignore them.
    for retired in ["REDIS_URL", "ORCHER_USE_STREAMS"] {
        if std::env::var_os(retired).is_some() {
            warn!(
                setting = retired,
                "This setting has no effect: the orchestrator uses only PostgreSQL"
            );
        }
    }

    // Validate configuration
    config.validate()?;

    Ok(config)
}

async fn run_service(config: config::OrchestratorConfig) -> Result<()> {
    use orcher_core::actor::{ActorRouter, ActorStateStorage, ServiceRegistry};
    use orcher_core::engine::dispatch::PollingManager;
    use orcher_core::engine::queues::OutboxQueue;
    use orcher_core::engine::state::high_perf_cache::{HighPerfCache, HighPerfCacheConfig};
    use orcher_core::grpc::services::execution::ExecutionService;
    use orcher_core::grpc::services::query::QueryService;
    use orcher_core::grpc::services::workflow::WorkflowService;
    use orcher_core::grpc::services::ActorServiceImpl;
    use orcher_core::grpc::services::WorkerServiceImpl;
    use orcher_core::projections::WorkerStateProjection;
    use orcher_core::workflows::{
        ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
    };
    use orcher_events::prelude::{EventBus, EventBusConfig};
    use orcher_storage::repositories::domains::child_workflow::ChildWorkflowRepository;
    use orcher_storage::repositories::domains::coordination::CoordinationRepository;
    use orcher_storage::repositories::domains::coordination::OutboxRepository;
    use orcher_storage::repositories::domains::journal::JournalRepository;
    use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
    use orcher_storage::repositories::domains::worker::WorkerRepository;
    use orcher_storage::repositories::domains::workflow_execution::{
        WorkflowExecutionLogsRepository, WorkflowExecutionRepository,
    };
    use orcher_storage::repositories::PostgresExecutionLogRepository;
    use std::sync::Arc;
    use std::time::Duration;

    // Setup shutdown coordinator
    let shutdown = Arc::new(shutdown::ShutdownCoordinator::new());

    // Initialize PostgreSQL connection pool
    // Redacted: this line used to log the connection string verbatim,
    // password included, on every start.
    info!(
        database_url = %orcher_storage::setup::redact_connection_url(&config.storage.database_url),
        "Connecting to PostgreSQL"
    );
    // Pool size is configurable so multiple orchestrator instances can share one
    // PostgreSQL (each instance holds `max_connections`; N instances must fit under
    // the server's `max_connections`, default 100). Defaults preserve prior behavior
    // (80/20). For higher concurrency, enable PgBouncer (see docker/docker-compose.yml).
    let pg_max_connections = std::env::var("ORCHER_PG_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(80);
    let pg_min_connections = std::env::var("ORCHER_PG_MIN_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(20);
    //
    // Statement cache capacity optimization:
    // Each connection caches prepared statements to avoid repeated query planning.
    // Default is 100, but we increase to 256 for high-throughput claim queries.
    // This reduces query planning overhead from ~20ms to <1ms for repeated queries.
    let connect_options = config
        .storage
        .database_url
        .parse::<sqlx::postgres::PgConnectOptions>()
        .map_err(|e| anyhow::anyhow!("Failed to parse database URL: {}", e))?
        .statement_cache_capacity(256); // Increase from default 100 for high-throughput

    // A connection opened before this process saw a statement refused as
    // prepared against an older schema is closed rather than handed out:
    // see orcher_storage::stale_plan.
    let pool =
        orcher_storage::stale_plan::recycle_stale_connections(sqlx::postgres::PgPoolOptions::new())
            .max_connections(pg_max_connections)
            .min_connections(pg_min_connections)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Duration::from_secs(600))
            .connect_with(connect_options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;
    info!(
        max_connections = pool.size(),
        "PostgreSQL connection pool created"
    );

    // Verify database connection
    sqlx::query("SELECT 1")
        .fetch_one(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("Database health check failed: {}", e))?;
    info!("Database connection verified");

    // Refuse to run against a schema this build does not match. Migrations are
    // applied by `orcher-migrate` (or the k8s migration Job), never by this
    // process — and until now nothing checked they had been. A stale schema
    // produced runtime failures that look exactly like product defects.
    orcher_storage::setup::ensure_schema_current(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    // ── Partitioned persistence: additional shard databases (opt-in) ──────────
    // ORCHER_SHARD_DATABASE_URLS (comma-separated) lists extra Postgres instances
    // holding shard ranges beyond the primary. Absent ⇒ single-pool, unchanged.
    // The primary is shard-pool 0 (also the coordination DB); each listed URL
    // becomes an additional pool. A MultiPoolRouter (built below) splits the shard
    // space into contiguous ranges across [primary, shard_1, …] so aggregate write
    // capacity grows with the database count. URLs are never logged (credentials).
    let shard_database_urls: Vec<String> = std::env::var("ORCHER_SHARD_DATABASE_URLS")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|u| u.trim().to_string())
                .filter(|u| !u.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let mut shard_pools: Vec<Arc<sqlx::PgPool>> = Vec::with_capacity(shard_database_urls.len());
    for url in &shard_database_urls {
        let opts = url
            .parse::<sqlx::postgres::PgConnectOptions>()
            .map_err(|e| anyhow::anyhow!("Failed to parse shard database URL: {}", e))?
            .statement_cache_capacity(256);
        let shard_pool = orcher_storage::stale_plan::recycle_stale_connections(
            sqlx::postgres::PgPoolOptions::new(),
        )
        .max_connections(pg_max_connections)
        .min_connections(pg_min_connections)
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(600))
        .connect_with(opts)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to shard PostgreSQL: {}", e))?;
        sqlx::query("SELECT 1")
            .fetch_one(&shard_pool)
            .await
            .map_err(|e| anyhow::anyhow!("Shard database health check failed: {}", e))?;
        shard_pools.push(Arc::new(shard_pool));
    }
    if !shard_pools.is_empty() {
        info!(
            shard_db_count = shard_pools.len(),
            "partitioned persistence: shard databases connected"
        );
        // Each shard database holds workflows of its own, so each is held to
        // the same schema check as the primary, named by position in the
        // list rather than by URL.
        let labels: Vec<String> = (1..=shard_pools.len())
            .map(|i| format!("shard database {i}"))
            .collect();
        let shards: Vec<(&str, &sqlx::PgPool)> = labels
            .iter()
            .map(String::as_str)
            .zip(shard_pools.iter().map(|p| &**p))
            .collect();
        orcher_storage::setup::ensure_schemas_current(&shards)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
    }

    // ── Cluster membership + shard-ownership fencing (opt-in) ─────────────────
    // With ORCHER_CLUSTER_ENABLED=true this node registers itself in
    // cluster_members and heartbeats on a timer, refreshing its view of the live
    // set, which feeds the contiguous shard-range ownership every node computes,
    // and maintains the fencing generations of the shards it holds. The ShardOwnership handle is injected
    // into the coordinator below so per-shard writes are guarded. With the flag off
    // (default), nothing here runs and single-node behavior is byte-identical.
    let mut shard_ownership: Option<Arc<orcher_cluster::ShardOwnership>> = None;
    // Kept to hand it the shard router once that exists, so it publishes each
    // acquired generation where the shard's rows are written.
    let mut cluster_membership: Option<
        Arc<orcher_storage::repositories::domains::cluster::ClusterMembershipRepository>,
    > = None;
    // Restricts claim queries to this node's shards. Set only when clustering is
    // on; `None` leaves every claim unfiltered, which is the single-node default.
    let mut claim_scope: Option<Arc<dyn orcher_storage::sharding::ClaimScope>> = None;
    // Who this engine is in the cluster, how stale a member has to be to be
    // out of it, and the heartbeat interval. Set only when clustering is on.
    let mut cluster_node: Option<(String, i64, i64)> = None;
    // Releases the claims of engines that died, from the membership loop.
    // Filled in once the polling manager it offers the work to exists; until
    // then the loop has nothing to run.
    let dead_node_claims: Arc<
        std::sync::OnceLock<Arc<orcher_core::engine::dispatch::DeadNodeClaims>>,
    > = Arc::new(std::sync::OnceLock::new());
    // Stops the membership loop once the servers have drained; see the loop.
    let mut membership_stop: Option<(
        tokio::sync::watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    )> = None;
    // Relays a workflow completion to the node owning the workflow. Set only
    // when clustering and forwarding are both on.
    let mut owner_forwarder: Option<Arc<orcher_core::grpc::forward::OwnerForwarder>> = None;
    if std::env::var("ORCHER_CLUSTER_ENABLED")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
    {
        use orcher_cluster::{ClusterStore, DbCluster, NodeId, ShardOwnership};
        use orcher_storage::repositories::domains::cluster::ClusterMembershipRepository;

        // Stable identity (defaults to the gRPC bind address).
        let node_id =
            std::env::var("ORCHER_NODE_ID").unwrap_or_else(|_| config.server.grpc_addr.clone());
        // The address peers dial to relay a request to this node. Without one,
        // the bind address is registered as before — peers recognise it as not
        // dialable and never relay here.
        let advertised = advertise_address(
            std::env::var("ORCHER_ADVERTISE_ADDR").ok(),
            config.cluster.advertise_address.as_deref(),
            &config.server.grpc_addr,
        );
        let address = advertised
            .clone()
            .unwrap_or_else(|| config.server.grpc_addr.clone());
        // Relaying to the owner, and the range-strict shard acquisition that
        // depends on it, are on by default with clustering. Off, ownership is
        // first-touch and every request is handled where it lands, as before.
        let forwarding_requested = std::env::var("ORCHER_CLUSTER_FORWARDING")
            .map(|v| !matches!(v.to_ascii_lowercase().as_str(), "0" | "false" | "off"))
            .unwrap_or(true);
        let forwarding = forwarding_requested && advertised.is_some();
        if forwarding_requested && advertised.is_none() {
            warn!(
                grpc_addr = %config.server.grpc_addr,
                "cluster: owner forwarding is OFF on this node: it has no address peers can \
                 dial. Set ORCHER_ADVERTISE_ADDR (e.g. the pod IP and gRPC port) to turn it on"
            );
        }
        let heartbeat_secs: u64 = std::env::var("ORCHER_CLUSTER_HEARTBEAT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        // A node is live if seen within ~3 heartbeat intervals (tolerates a missed tick).
        let stale_after_secs = (heartbeat_secs * 3) as i64;

        let membership = Arc::new(ClusterMembershipRepository::new(pool.clone()));
        cluster_membership = Some(membership.clone());
        cluster_node = Some((node_id.clone(), stale_after_secs, heartbeat_secs as i64));
        let members = membership.clone();
        let store: Arc<dyn ClusterStore> = membership;
        // Align ownership to the shard databases, so a node's range stays inside
        // one of them rather than straddling two. The primary is pool 0, so the
        // count is the extra databases plus one.
        let cluster = Arc::new(
            DbCluster::with_defaults(
                NodeId::new(node_id.clone()),
                store.clone(),
                stale_after_secs,
            )
            .with_pools(shard_pools.len() + 1),
        );
        // The relay to workflow owners. Built before ownership, which must
        // decide range-strictness by the same view of which peers are reachable.
        let forwarder = if forwarding {
            use orcher_core::grpc::forward::{ForwardConfig, OwnerForwarder};
            let mut forward_config = ForwardConfig {
                // A relayed completion is as large as the worker's.
                max_message_bytes: config.limits.grpc_max_message_bytes,
                ..ForwardConfig::default()
            };
            if let Some(ms) = std::env::var("ORCHER_FORWARD_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
            {
                forward_config.default_timeout = Duration::from_millis(ms);
            }
            // Dial peers the way this node serves: TLS if it serves TLS, and
            // then only trusting the configured CA. Without one there is
            // nothing to verify a peer against, so relaying stays off here
            // and ownership stays first-touch, rather than every relay
            // failing one request at a time.
            let peer_tls = match &config.security.tls {
                None => Ok(None),
                Some(tls) => server::build_peer_tls_config(tls).await.map(Some),
            };
            match peer_tls {
                Ok(tls) => {
                    forward_config.tls = tls;
                    info!(
                        advertise_address = %address,
                        timeout_ms = forward_config.default_timeout.as_millis() as u64,
                        tls = forward_config.tls.is_some(),
                        "cluster: workflow completions are relayed to the workflow's owner"
                    );
                    Some(OwnerForwarder::new(cluster.clone(), forward_config))
                }
                Err(e) => {
                    tracing::error!(
                        error = %format!("{e:#}"),
                        "cluster: owner forwarding is OFF on this node: peers cannot be dialed \
                         over TLS. Set security.tls.ca_cert_path to the CA that signs the \
                         engines' certificates to turn it on"
                    );
                    None
                }
            }
        } else {
            None
        };
        let mut ownership = ShardOwnership::new(
            NodeId::new(node_id.clone()),
            store.clone(),
            cluster.clone(), // DbCluster is the ShardMap
            stale_after_secs,
        );
        if let Some(forwarder) = &forwarder {
            ownership = ownership.with_range_strict(forwarder.peers());
        }
        let ownership = Arc::new(ownership);

        // Register before serving so we're in the live set from the start.
        match cluster.register(&address).await {
            Ok(()) => info!(
                node_id = %node_id,
                address = %address,
                heartbeat_secs,
                forwarding,
                "cluster membership + fencing enabled"
            ),
            Err(e) => warn!(error = %e, "cluster: initial member registration failed (continuing)"),
        }

        // The loop outlives the shutdown signal: it stops only once the gRPC
        // server has drained (`membership_stop`, sent at the end of shutdown).
        // A node that stopped heartbeating as soon as it began draining would
        // be declared dead by the others while still answering the requests
        // in flight, and have the activations it was handing out released
        // under it. From the signal on it no longer releases anyone else's.
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let mut draining_rx = shutdown.subscribe();
        let hb = cluster.clone();
        let own = ownership.clone();
        let addr = address.clone();
        let dead_claims = dead_node_claims.clone();
        let sweeping = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let this_node = node_id.clone();
        // A sweep that has not finished well within a heartbeat interval is
        // abandoned, so a slow database cannot pile sweeps up behind it.
        let sweep_bound = Duration::from_millis((heartbeat_secs * 1000 / 2).max(500));
        let membership_loop = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(heartbeat_secs));
            let mut draining = false;
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        if draining {
                            // Draining: only the heartbeat's timestamp, which
                            // keeps the others from treating this node as dead
                            // and releasing what it is still answering. Not
                            // register(), which would mark it active again, and
                            // no shard maintenance, which would renew shards it
                            // has handed over.
                            if let Err(e) = hb.heartbeat().await {
                                warn!(error = %e, "cluster: heartbeat failed");
                            }
                            if let Err(e) = hb.refresh().await {
                                warn!(error = %e, "cluster: membership refresh failed");
                            }
                            continue;
                        }
                        // register() is an idempotent upsert — doubles as heartbeat and
                        // re-creates our row if a reaper removed it.
                        let registered = hb.register(&addr).await;
                        if let Err(e) = &registered {
                            warn!(error = %e, "cluster: heartbeat failed");
                        }
                        let refreshed = hb.refresh().await;
                        if let Err(e) = &refreshed {
                            warn!(error = %e, "cluster: membership refresh failed");
                        }
                        // Renew held shards / release the ones we no longer own.
                        if let Err(e) = own.maintain().await {
                            warn!(error = %e, "cluster: shard maintenance failed");
                        }
                        // Release what members that stopped heartbeating left
                        // claimed — only on a tick whose own heartbeat and
                        // refresh went through, and that shows this node live.
                        // A failed refresh leaves the previous snapshot in
                        // place, so without that a node cut off from the
                        // database would go on judging the others from what
                        // it saw before it was.
                        //
                        // In a task of its own, and never two at once: the
                        // heartbeat must not wait for a sweep, or a slow one
                        // would make this node look dead to the others.
                        use orcher_cluster::Membership;
                        if registered.is_ok()
                            && refreshed.is_ok()
                            && hb.live_nodes().contains(hb.this_node())
                        {
                            if let Some(release) = dead_claims.get() {
                                if !sweeping.swap(true, std::sync::atomic::Ordering::AcqRel) {
                                    let release = release.clone();
                                    // Clears the flag however the sweep ends —
                                    // a panic included — or no sweep would
                                    // ever run again.
                                    let running = SweepRunning(sweeping.clone());
                                    tokio::spawn(async move {
                                        let _running = running;
                                        // Cut short, what was released is
                                        // still released; the executions are
                                        // found by the pollers' scan instead
                                        // of by continuation.
                                        match tokio::time::timeout(sweep_bound, release.sweep())
                                            .await
                                        {
                                            Ok(Ok(_)) => {}
                                            Ok(Err(e)) => warn!(
                                                error = %e,
                                                "cluster: releasing a dead member's claims failed"
                                            ),
                                            Err(_) => warn!(
                                                bound_ms = sweep_bound.as_millis() as u64,
                                                "cluster: releasing a dead member's claims took \
                                                 too long; the next tick tries again"
                                            ),
                                        }
                                    });
                                }
                            }
                        }
                    }
                    _ = draining_rx.recv(), if !draining => {
                        // Hand the shards over now rather than once this node
                        // has gone stale: marked draining it is out of every
                        // node's live set, its own included, so the others
                        // take its shards and it stops acquiring them; then
                        // it lets go of the ones it holds.
                        draining = true;
                        if let Err(e) = members.set_member_status(&this_node, "draining").await {
                            warn!(error = %e, "cluster: could not mark this node as draining");
                        }
                        if let Err(e) = hb.refresh().await {
                            warn!(error = %e, "cluster: membership refresh failed");
                        }
                        if let Err(e) = own.release_all().await {
                            warn!(error = %e, "cluster: could not release this node's shards");
                        }
                        info!("cluster: draining; shards released, heartbeating until the servers have stopped");
                    }
                    _ = stop_rx.changed() => {
                        info!("cluster: membership loop shutting down");
                        break;
                    }
                }
            }
        });
        membership_stop = Some((stop_tx, membership_loop));

        // Same cluster view that drives fencing, so a node claims from exactly the
        // shards it acquires rather than two derivations that can disagree.
        claim_scope = Some(Arc::new(orcher_cluster::ClusterClaimScope::new(
            cluster.clone(),
        )));

        owner_forwarder =
            forwarder.map(|forwarder| Arc::new(forwarder.with_ownership(ownership.clone())));
        shard_ownership = Some(ownership);
    }

    // Initialize execution repositories for Service polling
    let workflow_execution_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    workflow_execution_repo.set_activation_in_flight_bound(activation_in_flight_bound(
        config.polling.claim_timeout_secs,
    ));
    // A run released to resume is held a moment for the worker process that
    // keeps its journal; the polling manager orders its queue the same way.
    let journal_holder_hold =
        std::time::Duration::from_millis(config.polling.journal_holder_hold_ms);
    workflow_execution_repo.set_journal_holder_hold(journal_holder_hold);
    // Every claim names the engine that made it, so that if this engine dies
    // the others can release what it left claimed.
    if let Some((node_id, _, _)) = &cluster_node {
        workflow_execution_repo.set_claim_node(node_id);
    }
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let child_workflow_repo = Arc::new(ChildWorkflowRepository::new(pool.clone()));
    let outbox_repo = Arc::new(OutboxRepository::new(Arc::new(pool.clone())));
    let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
    let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
    let coordination_repo = Arc::new(CoordinationRepository::new(Arc::new(pool.clone())));
    // Restrict the claim queries to this node's shards. Off unless clustering is
    // on; the repositories then claim exactly as they did before.
    if let Some(scope) = &claim_scope {
        workflow_execution_repo.set_claim_scope(scope.clone());
        outbox_repo.set_claim_scope(scope.clone());
        info!("shard-filtered claims enabled: this node claims only rows on the shards it owns");
    }
    let workflow_logs_repo = Arc::new(WorkflowExecutionLogsRepository::new(pool.clone()));
    // Route best-effort workflow-execution logs through a process-wide async writer
    // that coalesces them into multi-row inserts, keeping ~7 per-workflow log INSERTTs
    // off the synchronous transition path. Opt out with ORCHER_ASYNC_LOGS=0 to fall
    // back to synchronous logging.
    let async_logs_enabled = std::env::var("ORCHER_ASYNC_LOGS")
        .map(|v| !matches!(v.as_str(), "0" | "false" | "off"))
        .unwrap_or(true);
    if async_logs_enabled {
        orcher_storage::repositories::domains::workflow_execution::init_async_log_writer(
            pool.clone(),
            orcher_storage::repositories::domains::workflow_execution::LogWriterConfig::default(),
        );
    }
    info!(
        component = "repositories",
        "Execution repositories initialized for Service polling (including ExecutionLogRepository, TaskExecutionRepository, and CoordinationRepository)"
    );

    // Initialize event bus
    let event_bus_config = EventBusConfig::default();
    let event_bus = EventBus::new(event_bus_config.clone());
    info!(
        config = ?event_bus_config,
        "Event bus initialized"
    );

    // Initialize projections
    #[allow(unused_variables)]
    let worker_projection = Arc::new(tokio::sync::RwLock::new(WorkerStateProjection::new()));
    info!("Projections initialized");

    // Initialize background jobs system from configuration
    use orcher_core::jobs::{start_background_jobs, BackgroundJobsConfig, RetentionDays};

    let job_config = BackgroundJobsConfig {
        metrics_enabled: config.background_jobs.metrics_enabled,
        metrics_interval_secs: config.background_jobs.metrics_interval_secs,
        retention_enabled: config.background_jobs.retention_enabled,
        retention_interval_secs: config.background_jobs.retention_interval_secs,
        retention_days: RetentionDays {
            workflow_executions: config.background_jobs.retention_days.workflow_executions,
            execution_logs: config.background_jobs.retention_days.execution_logs,
            journal_entries: config.background_jobs.retention_days.journal_entries,
            task_executions: config.background_jobs.retention_days.task_executions,
            outbox_queue: config.background_jobs.retention_days.outbox_queue,
        },
    };

    // Create watch channel for background jobs shutdown coordination
    let (jobs_shutdown_tx, jobs_shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn task to bridge broadcast shutdown to watch channel
    let mut shutdown_listener = shutdown.subscribe();
    tokio::spawn(async move {
        let _ = shutdown_listener.recv().await;
        let _ = jobs_shutdown_tx.send(true);
    });

    let job_handles =
        start_background_jobs(Arc::new(pool.clone()), job_config.clone(), jobs_shutdown_rx).await;

    info!(
        jobs_count = job_handles.len(),
        metrics_enabled = job_config.metrics_enabled,
        metrics_interval_secs = job_config.metrics_interval_secs,
        retention_enabled = job_config.retention_enabled,
        retention_interval_secs = job_config.retention_interval_secs,
        "Background jobs system started"
    );

    // Initialize WorkerRegistry for tracking connected SDK workers
    let worker_registry = Arc::new(orcher_core::worker::WorkerRegistry::new());
    info!("WorkerRegistry initialized for worker registration tracking");

    // Build orchestrator using config-driven builder pattern
    info!("Building orchestrator with config-driven coordination");

    // Configure workflow settings using WorkflowConfig

    // Build orchestrator with automatic coordinator creation

    // =========================================================================
    // Task delivery is PostgreSQL-only. A Redis Streams path used to sit here,
    // writing every dispatch with XADD before handing off. Nothing ever read it
    // back — the recovery consumer was never implemented — so it bought no
    // durability, and it could not have: a Redis write cannot commit atomically
    // with the Postgres transaction that produced the task, so the outbox has to
    // stay the source of truth regardless.

    // Initialize HighPerfCache for sync-match (shared across components)
    let state_cache = Arc::new(HighPerfCache::new(HighPerfCacheConfig::default()));
    info!(
        component = "state_cache",
        max_workflow_states = state_cache.config().max_workflow_states,
        workflow_ttl_secs = state_cache.config().workflow_state_ttl_secs,
        "HighPerfCache initialized for workflow state caching"
    );

    // Initialize PollingManager for Service execution polling
    // Sync-match when enabled, PostgreSQL polling otherwise.
    //
    // Prefetch buffer optimization: Enabled when batch_claim_size > 1
    let prefetch_enabled = config.polling.batch_claim_size > 1;
    let prefetch_config = if prefetch_enabled {
        Some(orcher_dispatch::polling::prefetch::PrefetchConfig {
            batch_size: config.polling.batch_claim_size,
            claim_cooldown: std::time::Duration::from_millis(100),
        })
    } else {
        None
    };

    let polling_manager = if config.polling.use_sync_match {
        {
            // Sync-match: hand work straight to a waiting worker, in-process.
            let routing_config = config.polling.to_routing_config();
            info!("╔══════════════════════════════════════════════════════════════════╗");
            info!("║                      SYNC-MATCH ENABLED                          ║");
            info!("╠══════════════════════════════════════════════════════════════════╣");
            info!("║  Mode: Direct dispatch to waiting workers                        ║");
            info!("║  Durability: outbox row, committed with the workflow state       ║");
            info!("║  Expected latency: P99 < 1ms for sync-matched tasks              ║");
            info!(
                "║  Max waiters/queue: {:>6}                                        ║",
                routing_config.max_waiters_per_queue
            );
            info!(
                "║  Affinity enabled: {:>5}                                          ║",
                routing_config.affinity.enabled
            );
            info!(
                "║  Affinity lease: {:>3}s (max {:>3}s)                                ║",
                routing_config.affinity.default_lease_secs, routing_config.affinity.max_lease_secs
            );
            if prefetch_enabled {
                info!(
                    "║  Prefetch batch size: {:>3}                                        ║",
                    config.polling.batch_claim_size
                );
            }
            info!("╚══════════════════════════════════════════════════════════════════╝");

            let manager = PollingManager::with_sync_match(
                workflow_execution_repo.clone(),
                journal_repo.clone(),
                state_cache.clone(),
                routing_config,
            )
            .with_task_repo(task_execution_repo.clone())
            .with_journal_holder_hold(journal_holder_hold);

            // Enable prefetch if configured
            if let Some(prefetch_cfg) = prefetch_config.clone() {
                Arc::new(manager.with_prefetch(prefetch_cfg))
            } else {
                Arc::new(manager)
            }
        }
    } else {
        info!(
            component = "polling",
            mode = "postgresql",
            prefetch_enabled = prefetch_enabled,
            batch_size = config.polling.batch_claim_size,
            "Initializing PollingManager for Service execution polling"
        );
        let manager = PollingManager::new(
            workflow_execution_repo.clone(),
            journal_repo.clone(),
            config.polling.claim_timeout_secs as i64,
        )
        .with_task_repo(task_execution_repo.clone())
        .with_journal_holder_hold(journal_holder_hold);

        // Enable prefetch if configured
        if let Some(prefetch_cfg) = prefetch_config {
            Arc::new(manager.with_prefetch(prefetch_cfg))
        } else {
            Arc::new(manager)
        }
    };

    // Start sync-match cleanup task for expired waiters and affinity leases
    let _sync_match_cleanup_handle: Option<tokio::task::JoinHandle<()>> =
        if config.polling.use_sync_match && polling_manager.is_sync_match_enabled() {
            let pm = polling_manager.clone();
            let interval_secs = config.polling.routing_cleanup_interval_secs;
            let mut shutdown_rx = shutdown.subscribe();
            let handle = tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                interval.tick().await; // skip immediate tick
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if let Some(registry) = pm.waiters_registry() {
                                let expired = registry.cleanup_expired();
                                if expired > 0 {
                                    tracing::debug!(
                                        expired_count = expired,
                                        "Sync-match cleanup: removed expired waiters"
                                    );
                                }
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            tracing::info!("Sync-match cleanup task shutting down");
                            break;
                        }
                    }
                }
            });
            info!(
                cleanup_interval_secs = interval_secs,
                "Sync-match cleanup task started for expired waiters and affinity leases"
            );
            Some(handle)
        } else {
            None
        };

    // Start reclaimer background task if notifications enabled
    let _reclaimer_handle = if config.polling.enable_notifications {
        info!(
            reclaim_interval_secs = config.polling.reclaim_interval_secs,
            "Starting reclaimer background task"
        );
        let handle = polling_manager.start().await;
        info!("Reclaimer background task started successfully");
        Some(handle)
    } else {
        warn!(
            "Notifications disabled - reclaimer not started (polling-only mode). \
             This may result in higher latency and unclaimed work accumulation."
        );
        None
    };

    info!(
        claim_timeout_secs = config.polling.claim_timeout_secs,
        poll_timeout_secs = config.polling.poll_timeout_secs,
        reclaim_interval_secs = config.polling.reclaim_interval_secs,
        notifications_enabled = config.polling.enable_notifications,
        "PollingManager initialized successfully"
    );

    // Initialize TimeoutWatcher for automatic timeout detection and retry scheduling
    use orcher_core::retries::timeout_watcher::{TimeoutWatcher, TimeoutWatcherConfig};

    let timeout_watcher_handle = if config.timeout_watcher.enabled {
        info!(
            component = "timeout_watcher",
            poll_interval_secs = config.timeout_watcher.poll_interval_secs,
            batch_size = config.timeout_watcher.batch_size,
            "Initializing TimeoutWatcher for task timeout detection"
        );

        let watcher_config = TimeoutWatcherConfig {
            poll_interval_secs: config.timeout_watcher.poll_interval_secs,
            batch_size: config.timeout_watcher.batch_size,
            enable_schedule_to_start: config.timeout_watcher.enable_schedule_to_start,
            enable_start_to_close: config.timeout_watcher.enable_start_to_close,
            enable_heartbeat: config.timeout_watcher.enable_heartbeat,
            enable_workflow_timeout: config.timeout_watcher.enable_workflow_timeout,
            grace_period_secs: config.timeout_watcher.grace_period_secs,
        };

        let timeout_watcher = TimeoutWatcher::new(pool.clone(), watcher_config);
        timeout_watcher.set_activation_in_flight_bound(activation_in_flight_bound(
            config.polling.claim_timeout_secs,
        ));
        let handle = timeout_watcher.start();

        info!(
            poll_interval_secs = config.timeout_watcher.poll_interval_secs,
            schedule_to_start = config.timeout_watcher.enable_schedule_to_start,
            start_to_close = config.timeout_watcher.enable_start_to_close,
            heartbeat = config.timeout_watcher.enable_heartbeat,
            "TimeoutWatcher started successfully"
        );

        Some((timeout_watcher, handle))
    } else {
        warn!(
            "TimeoutWatcher disabled - tasks will not be automatically detected for timeouts. \
             This is not recommended for production deployments."
        );
        None
    };

    // Initialize WorkflowRetryScheduler for automatic workflow retry handling
    use orcher_core::workflows::{WorkflowRetryScheduler, WorkflowRetrySchedulerConfig};

    let workflow_retry_scheduler_handle = if config.workflow_retry.enabled {
        info!(
            component = "workflow_retry_scheduler",
            poll_interval_secs = config.workflow_retry.poll_interval_secs,
            batch_size = config.workflow_retry.batch_size,
            "Initializing WorkflowRetryScheduler for automatic workflow retry"
        );

        let scheduler_config = WorkflowRetrySchedulerConfig {
            poll_interval_secs: config.workflow_retry.poll_interval_secs,
            batch_size: config.workflow_retry.batch_size,
            enable_metrics: true,
            enable_realtime_notifications: config.workflow_retry.enable_realtime_notifications,
            ..WorkflowRetrySchedulerConfig::default()
        };

        // Create retry scheduler with database pool for real-time notifications.
        // Reuse the resolver-wired `workflow_execution_repo` (it receives
        // `set_shard_resolver` below), so the scheduler's eligibility scan and retry
        // updates route across shards instead of hitting the primary only.
        let workflow_retry_scheduler = Arc::new(
            WorkflowRetryScheduler::new_with_pool(
                workflow_execution_repo.clone(),
                scheduler_config,
                Arc::new(pool.clone()),
            )
            // A retried child's successor stays its parent's child.
            .with_child_repository(child_workflow_repo.clone()),
        );

        let handle = workflow_retry_scheduler.clone().start();

        info!(
            poll_interval_secs = config.workflow_retry.poll_interval_secs,
            realtime_notifications = config.workflow_retry.enable_realtime_notifications,
            max_attempts = config.workflow_retry.default_max_attempts,
            initial_backoff = config.workflow_retry.default_initial_backoff_secs,
            backoff_multiplier = config.workflow_retry.default_backoff_multiplier,
            max_backoff = config.workflow_retry.default_max_backoff_secs,
            "WorkflowRetryScheduler started successfully"
        );

        Some((workflow_retry_scheduler, handle))
    } else {
        warn!(
            "WorkflowRetryScheduler disabled - failed workflows will NOT automatically retry. \
             This is not recommended for production deployments."
        );
        None
    };

    // Initialize ExecutionService dependencies
    info!(
        component = "execution_service",
        "Creating ExecutionService for Service polling API"
    );

    // Create JournalService for journal management
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    info!("JournalService initialized");

    // Create OutboxQueue for atomic task dispatch
    let outbox_queue = Arc::new(OutboxQueue::new(outbox_repo.clone()));
    info!("OutboxQueue initialized");

    // Create OrchestrationSystem (wraps WorkflowCoordinator + processors)
    use orcher_core::engine::OutboxProcessorConfig;
    use orcher_core::engine::TimerProcessorConfig;
    use orcher_core::engine::{OrchestrationConfig, OrchestrationSystem};

    // Dev-friendly configuration: shorter stuck thresholds for faster recovery
    // Production should use longer thresholds (300s) to avoid unnecessary reclaims
    // Matching layer (opt-in). When on, the outbox processors also claim `dispatch_task`
    // rows and transfer them into their (namespace, task_queue) matching partition (the
    // poller then claims from the partition). Must agree with the store injection below.
    // On by default. `ORCHER_MATCHING_ENABLED=false` opts out.
    //
    // Without it, a worker polling one task queue claims `dispatch_task` rows for
    // *every* queue, filters them in Rust, and releases each non-match with its own
    // UPDATE — because `task_queue` and `namespace` live in the payload JSON rather
    // than in columns the claim can filter on. Any queue whose worker is down or
    // scaled to zero therefore leaves rows that every other poller repeatedly claims
    // and releases, forever: there is no backoff, and `attempt_count` is decremented
    // back each time so nothing ages out. Measured with 40 such rows on an otherwise
    // idle system: 3581 release UPDATEEs in 25 seconds, against 0 with matching on.
    //
    // The matching layer transfers each `dispatch_task` into its
    // (namespace, task_queue) partition and claims from there — filtered in SQL, so
    // there is nothing to release. Throughput is unchanged (6 benchmark runs per
    // mode: 8.98 vs 8.99 wf/s), and the store is Postgres-backed, so this adds no
    // external dependency.
    let matching_opt_out = std::env::var("ORCHER_MATCHING_ENABLED")
        .map(|v| v.eq_ignore_ascii_case("false") || v == "0")
        .unwrap_or(false);

    // Partitioned persistence REQUIRES the matching layer: with extra shard databases a
    // task's `dispatch_task` outbox row is co-located on its shard, and only the matching
    // layer's per-shard transfer + central (namespace, task_queue) partition delivers it
    // to workers. Without it, the primary-only TaskPollingManager never sees shard-resident
    // tasks and they are silently stranded — so opting out is refused there rather than
    // honoured into a broken configuration.
    let matching_enabled = !matching_opt_out || !shard_pools.is_empty();
    if matching_opt_out && matching_enabled {
        warn!(
            shard_databases = shard_pools.len(),
            "ORCHER_MATCHING_ENABLED=false ignored: partitioned persistence requires the \
             matching layer, and disabling it would strand every shard-resident task"
        );
    } else if matching_opt_out {
        warn!(
            "matching layer disabled: workers will claim dispatch_task rows for other \
             queues and release them individually; a queue with no worker will spin"
        );
    }

    let outbox_processor_config = OutboxProcessorConfig {
        batch_size: 100,
        polling_interval: std::time::Duration::from_millis(50), // Low-latency dispatch (wake signal handles immediate dispatch)
        cleanup_interval: std::time::Duration::from_secs(10),   // More frequent cleanup
        stuck_threshold_secs: 30, // 30 seconds instead of 300 (5 min) for faster recovery
        max_concurrent_workflows: 1000,
        partition_id: None,  // No partitioning by default
        total_partitions: 1, // Single partition
        claim_dispatch_task: matching_enabled,
    };

    let timer_processor_config = TimerProcessorConfig {
        batch_size: 100,
        max_sleep_duration: std::time::Duration::from_secs(30),
        min_sleep_duration: std::time::Duration::from_millis(10),
        stuck_threshold_secs: 30, // 30 seconds instead of 300 for faster recovery
        cleanup_interval: std::time::Duration::from_secs(10),
    };

    let orchestration_config = OrchestrationConfig {
        enable_processors: false, // We manually start processors with custom executors below
        shutdown_timeout_secs: 30,
        outbox_processor: outbox_processor_config,
        timer_processor: timer_processor_config,
        ..Default::default()
    };

    let orchestration_system = Arc::new(
        OrchestrationSystem::new_partitioned(
            Arc::new(pool.clone()),
            shard_pools.clone(),
            orchestration_config,
        )
        .await
        .expect("Failed to initialize OrchestrationSystem"),
    );
    info!("OrchestrationSystem initialized (processors will be started with custom executors)");

    // Get WorkflowCoordinator Arc from OrchestrationSystem
    let workflow_coordinator = orchestration_system.coordinator_arc();
    info!("WorkflowCoordinator Arc retrieved from OrchestrationSystem");

    // Configure shard routing on the coordinator's write path. With extra shard
    // databases (ORCHER_SHARD_DATABASE_URLS) this is a MultiPoolRouter splitting the
    // shard space across [primary, shard_1, …] — the switch that turns on
    // partitioned persistence. Otherwise, when clustering is on, a SinglePoolRouter
    // exercises the routing path as a no-op. Neither ⇒ writes use the primary pool
    // directly (unchanged).
    // Resolver retained for components built/started earlier than this block (the
    // timeout watcher and retry schedulers). They hold an `Arc<OnceLock>`, so setting it
    // here still reaches their already-spawned loops. `None` ⇒ unpartitioned.
    let shard_resolver: Option<
        std::sync::Arc<dyn orcher_storage::sharding::pool_routing::ShardPoolResolver>,
    >;
    if !shard_pools.is_empty() {
        let mut pools: Vec<sqlx::PgPool> = Vec::with_capacity(shard_pools.len() + 1);
        pools.push(pool.clone());
        for sp in &shard_pools {
            pools.push((**sp).clone());
        }
        // One router instance drives both the coordinator's write path and the
        // repositories' shard-pool routing, so a workflow's ledger/journal/outbox and
        // its coordinator state co-locate on the same shard's database.
        // Namespace-aware placement: OSS ships a single shared default pool-set over
        // all pools (routing is namespace-agnostic, identical to a contiguous-range
        // router). A Cloud policy can later pin a namespace to a dedicated pool-set via
        // `with_override`. One instance drives both the coordinator's `ShardRouter` and
        // the repositories' `ShardPoolResolver`, so a workflow's state co-locates.
        let router = std::sync::Arc::new(orcher_cluster::NamespacePlacement::new(pools));
        workflow_coordinator.set_shard_router(router.clone());
        workflow_execution_repo.set_shard_resolver(router.clone());
        // Co-locate the rest of a workflow's state (its task rows and journal) on the
        // same shard so completion — which reads the task summary + journal and updates
        // task/journal/workflow rows — routes to the pool that holds them.
        task_execution_repo.set_shard_resolver(router.clone());
        journal_repo.set_shard_resolver(router.clone());
        // Timeout watcher (constructed + started earlier): route its sweeps + per-row
        // handling to the shards, so a hung/timed-out task on a shard is reaped and
        // rescheduled on its own pool rather than stranding.
        if let Some((ref timeout_watcher, _)) = timeout_watcher_handle {
            timeout_watcher.set_shard_resolver(router.clone());
        }
        info!(
            shard_db_count = shard_pools.len() + 1,
            "partitioned persistence enabled: shard writes routed across databases"
        );
        shard_resolver = Some(
            router as std::sync::Arc<dyn orcher_storage::sharding::pool_routing::ShardPoolResolver>,
        );
    } else {
        // Single pool. The router is a no-op for *pool selection* — every shard
        // resolves to the one pool — but the repositories still need it, because
        // they ask it which shard a row belongs to and persist that as `shard_id`.
        //
        // A row's shard is a property of the workflow, not of how many databases
        // exist: ownership partitions work across *nodes*, which is orthogonal to
        // pool count. Gating this on multi-DB left `shard_id` NULL on every row in
        // single-pool deployments, which is every deployment we actually run.
        let router = std::sync::Arc::new(orcher_cluster::SinglePoolRouter::new(pool.clone()));
        // Ungated for the same reason as the repositories below: the coordinator
        // writes `dispatch_task` outbox rows and derives their `shard_id` from this
        // router. Gating it on clustering left exactly those rows NULL while every
        // other row on the same workflow carried a shard.
        workflow_coordinator.set_shard_router(router.clone());
        workflow_execution_repo.set_shard_resolver(router.clone());
        task_execution_repo.set_shard_resolver(router.clone());
        journal_repo.set_shard_resolver(router.clone());
        if let Some((ref timeout_watcher, _)) = timeout_watcher_handle {
            timeout_watcher.set_shard_resolver(router.clone());
        }
        shard_resolver = Some(
            router as std::sync::Arc<dyn orcher_storage::sharding::pool_routing::ShardPoolResolver>,
        );
    }

    // Enable shard-ownership fencing on the coordinator's write path when clustering
    // is on (the ShardOwnership built above). Off → writes are unguarded.
    // Before any write path can acquire a shard: ownership is attached below.
    if let (Some(membership), Some(resolver)) = (&cluster_membership, &shard_resolver) {
        membership.set_data_pools(resolver.clone());
    }
    if let Some(ownership) = &shard_ownership {
        workflow_coordinator.set_shard_ownership(ownership.clone());
        // Also fence the repository's status writes. The coordinator's guard only
        // covers timer scheduling; workflow completion goes through the
        // repository, which is where a superseded owner could otherwise overwrite
        // state a live node now owns.
        workflow_execution_repo.set_write_fence(ownership.clone());
        info!("shard-ownership fencing enabled on the coordinator and workflow status writes");
    }

    // Create WorkflowExecutionService for workflow lifecycle management
    let workflow_execution_service = Arc::new(
        WorkflowExecutionService::new(
            workflow_execution_repo.clone(),
            journal_service.clone(),
            outbox_queue.clone(),
            event_bus.clone(),
            pool.clone(),
        )
        .with_polling_manager(polling_manager.clone()),
    );
    workflow_execution_service.set_queued_task_cancellation(
        orcher_core::retries::QueuedTaskCancellation::new(
            task_execution_repo.clone(),
            execution_log_repo.clone(),
        ),
    );
    info!("WorkflowExecutionService initialized with polling manager integration");

    // Create ChildWorkflowManager for child workflow orchestration
    let child_workflow_manager = Arc::new(ChildWorkflowManager::new(
        child_workflow_repo.clone(),
        journal_service.clone(),
        workflow_execution_service.clone(),
    ));
    info!("ChildWorkflowManager initialized");

    // Create TaskPollingManager BEFORE CommandProcessor so we can wire them together
    // This is critical for the TypeScript SDK task dispatch fix - when SCHEDULE_TASK
    // commands are processed, the CommandProcessor needs to notify TaskPollingManager
    // so task pollers are immediately woken up instead of waiting for poll timeout.
    use orcher_core::engine::dispatch::TaskPollingManager;
    let task_polling_manager = Arc::new(TaskPollingManager::new(Arc::clone(
        workflow_coordinator.outbox(),
    )));
    info!(
        service = "grpc",
        component = "TaskPollingManager",
        "TaskPollingManager created for notification-based task polling"
    );

    // Create CommandProcessor with polling manager, task polling manager, and coordinator integration
    let command_processor = Arc::new(
        CommandProcessor::new(
            journal_service.clone(),
            child_workflow_manager.clone(),
            event_bus.clone(),
            execution_log_repo.clone(),
            task_execution_repo.clone(),
            workflow_execution_repo.clone(),
            Arc::new(pool.clone()),
        )
        .with_polling_manager(polling_manager.clone())
        .with_payload_limits(config.limits.payload_limits())
        // TODO: Add with_task_polling_manager support to CommandProcessor
        // .with_task_polling_manager(task_polling_manager.clone())
        .with_coordinator(workflow_coordinator.clone()),
    );
    info!(
        "CommandProcessor initialized with polling manager, task polling manager, coordinator, ExecutionLogRepository, and TaskExecutionRepository"
    );

    // Wire up TaskPollingManager to WorkflowCoordinator for notification-based dispatch
    workflow_coordinator
        .set_task_polling_manager(task_polling_manager.clone())
        .await;
    info!(
        service = "grpc",
        component = "WorkflowCoordinator",
        "TaskPollingManager wired to WorkflowCoordinator - notifications enabled for task dispatch"
    );

    // Start the task-retry dispatcher. When a task fails and is scheduled for retry, the
    // retry scheduler INSERTTs a new task_executions row (status=scheduled, attempt>1,
    // scheduled_at=now+backoff); this loop is the sole path that delivers that row to a
    // worker once its backoff has elapsed, reusing the coordinator's normal dispatch path.
    {
        use orcher_core::retries::{TaskRetryDispatcher, TaskRetryScheduler};
        let retry_scheduler = TaskRetryScheduler::new(pool.clone());
        // Route the dispatcher's ready-retry scan across shards so a backoff-elapsed
        // retry on any shard is picked up and delivered.
        if let Some(resolver) = &shard_resolver {
            retry_scheduler.set_shard_resolver(resolver.clone());
        }
        // The sweep for first attempts never dispatched looks back only as far
        // as completed outbox rows are kept; see `undispatched_repair_window`.
        workflow_coordinator
            .set_outbox_retention_days(config.background_jobs.retention_days.outbox_queue);
        let dispatcher = TaskRetryDispatcher::new(retry_scheduler, workflow_coordinator.clone());
        let _task_retry_dispatcher_handle = dispatcher.start();
        info!(
            component = "task_retry_dispatcher",
            "TaskRetryDispatcher started - scheduled task retries are delivered once their backoff elapses"
        );
    }

    // Create NamespaceRepository (shared between WorkflowService and NamespaceServiceImpl)
    let namespace_repo = Arc::new(
        orcher_storage::repositories::domains::namespace::NamespaceRepository::new(pool.clone()),
    );

    // Create WorkflowService first so we can share its completion notifier
    let workflow_service = WorkflowService::new(
        workflow_execution_service.clone(),
        workflow_execution_repo.clone(),
    )
    .with_event_support(journal_repo.clone(), polling_manager.clone())
    .with_namespace_repository(namespace_repo.clone())
    .with_child_repository(child_workflow_repo.clone())
    // A start with a cron schedule stores the schedule here; the schedule
    // processor fires it.
    .with_schedule_queue(Arc::new(orcher_core::engine::queues::ScheduleQueue::new(
        Arc::new(
            orcher_storage::repositories::domains::schedule::ScheduleRepository::new(pool.clone()),
        ),
    )))
    .with_payload_limits(config.limits.payload_limits());

    // Create ExecutionService for gRPC server with both polling managers
    let execution_service = ExecutionService::new(
        command_processor,
        polling_manager.clone(),
        execution_log_repo.clone(),
        journal_repo.clone(),
        workflow_execution_repo.clone(),
        task_execution_repo.clone(),
        pool.clone(),
    )
    .with_coordinator(workflow_coordinator.clone())
    .with_task_polling_manager(task_polling_manager.clone())
    .with_workflow_completion_notifier(workflow_service.completion_notifier())
    .with_pending_queries(workflow_service.pending_queries())
    .with_pending_updates(workflow_service.pending_updates())
    .with_namespace_repository(namespace_repo.clone())
    // A completion carrying an activation token is always held to it. This
    // decides only what happens to one carrying none — an SDK that predates
    // the token, or a failure report, which SDKs send with a token of their
    // own making — and it stays accepted until they all send the real one.
    .with_activation_token_required(
        std::env::var("ORCHER_REQUIRE_ACTIVATION_TOKEN")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "on"))
            .unwrap_or(false),
    )
    .with_default_heartbeat_timeout(default_heartbeat_timeout(&config))
    .with_payload_limits(config.limits.payload_limits());
    let execution_service = match owner_forwarder {
        Some(forwarder) => execution_service.with_owner_forwarder(forwarder),
        None => execution_service,
    };
    // A worker's polls reach any replica, so each one listens for the workers
    // shut down through the others.
    let _worker_shutdown_listener = execution_service.start_worker_shutdown_listener();

    // The child reconciliation sweep: delivers every child outcome the
    // completion paths did not — cancellations, timeouts, terminations, and
    // deliveries that failed part-way — and applies parent close policies. It
    // shares the completion path's delivery component, so both write the
    // same thing and either may repeat the other's work.
    let child_reconciler_handle = if config.child_reconciler.enabled {
        use orcher_core::workflows::{ChildReconciler, ChildReconcilerConfig};
        let reconciler = Arc::new(
            ChildReconciler::new(
                execution_service.child_outcome_delivery(),
                workflow_execution_repo.clone(),
                ChildReconcilerConfig {
                    poll_interval_secs: config.child_reconciler.poll_interval_secs,
                    batch_size: config.child_reconciler.batch_size,
                },
            )
            .with_cancellation(workflow_execution_service.clone()),
        );
        let handle = reconciler.clone().start();
        info!(
            component = "child_reconciler",
            poll_interval_secs = config.child_reconciler.poll_interval_secs,
            batch_size = config.child_reconciler.batch_size,
            "Child reconciler started"
        );
        Some((reconciler, handle))
    } else {
        warn!(
            "Child reconciler disabled - a child that is cancelled, times out or is \
             terminated is never reported to its parent, and parent close policies \
             are not applied."
        );
        None
    };

    // Return activation permits whose completion never came back here.
    //
    // A permit is returned by its activation's completion, but with more than
    // one replica that completion can reach another replica. Anything held
    // past the claim timeout — after which the activation is treated as lost
    // and handed out again anyway — is returned, so a worker's capacity on
    // this replica cannot drain away one cross-replica completion at a time.
    {
        let capacity = execution_service.capacity_registry();
        let ttl = std::time::Duration::from_secs(config.polling.claim_timeout_secs);
        let every =
            std::time::Duration::from_secs((config.polling.claim_timeout_secs / 4).clamp(5, 30));
        let mut stop = shutdown.subscribe();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let returned = capacity.release_expired(ttl);
                        if returned > 0 {
                            info!(returned, "Returned activation permits held past the claim timeout");
                        }
                    }
                    _ = stop.recv() => break,
                }
            }
        });
    }

    // Release what a worker process held once nobody sees it polling. Its
    // unanswered activations are handed out again within seconds, and its
    // task attempts timed out once their heartbeats stop too, rather than
    // both waiting out their timeouts. Every replica records the processes it
    // sees — one that does not release included, or the others would take
    // the workers polling it for dead — and the ones that release sweep; see
    // `orcher_core::engine::dispatch::lapsed_worker_claims`.
    {
        use orcher_core::engine::dispatch::{LapsedWorkerClaims, LivenessSettings};
        let settings = LivenessSettings {
            refresh: Duration::from_secs(config.worker_liveness.refresh_interval_secs.max(1)),
            missed_refreshes: config.worker_liveness.missed_refreshes,
            claim_timeout_secs: config.polling.claim_timeout_secs as i64,
        };
        let mut release = LapsedWorkerClaims::new(
            execution_service.worker_liveness(),
            orcher_storage::repositories::domains::worker::WorkerInstanceRepository::new(
                pool.clone(),
            ),
            workflow_execution_repo.clone(),
            polling_manager.clone(),
            settings,
        )
        .releasing(config.worker_liveness.enabled);
        if let Some((ref timeout_watcher, _)) = timeout_watcher_handle {
            release = release.with_tasks(timeout_watcher.clone());
        }
        let mut stop = shutdown.subscribe();
        tokio::spawn(Arc::new(release).run(async move {
            let _ = stop.recv().await;
        }));
        if config.worker_liveness.enabled {
            info!(
                refresh_interval_secs = settings.refresh.as_secs(),
                lapse_secs = settings.lapse_secs(),
                "A worker process's claims are released once it stops polling"
            );
        } else {
            warn!(
                "worker_liveness disabled - a worker process that dies holding an activation \
                 leaves its workflow claimed until the claim timeout"
            );
        }
    }

    // Hand out each delayed start's run once it is due. Every engine runs
    // this, each for its own shards; see
    // `orcher_core::engine::dispatch::delayed_starts`.
    {
        use orcher_core::engine::dispatch::DelayedStarts;
        let delayed_starts = DelayedStarts::new(
            workflow_execution_repo.clone(),
            polling_manager.clone(),
            workflow_execution_service.delayed_start_wake(),
        );
        let mut stop = shutdown.subscribe();
        tokio::spawn(Arc::new(delayed_starts).run(async move {
            let _ = stop.recv().await;
        }));
        info!("Delayed starts are handed out as they come due");
    }

    // Route the failure-driven retry (handle_task_failure → schedule_retry) to the
    // workflow's shard so a rescheduled attempt co-locates rather than stranding.
    if let Some(resolver) = &shard_resolver {
        execution_service.set_shard_resolver(resolver.clone());
    }

    info!(
        service = "grpc",
        component = "WorkflowService",
        endpoints = "StartWorkflow, GetWorkflowStatus, CancelWorkflow, etc.",
        "WorkflowService created successfully for client workflow API"
    );

    // Create QueryService for querying workflow executions
    let query_service = QueryService::new(
        workflow_execution_repo.clone(),
        journal_repo.clone(),
        workflow_logs_repo.clone(),
        task_execution_repo.clone(),
        coordination_repo.clone(),
    );

    info!(
        service = "grpc",
        component = "QueryService",
        endpoints = "ListWorkflows, SearchWorkflows, GetExecutionJournal, etc.",
        "QueryService created successfully for workflow query API"
    );

    // Initialize TaskDispatchService for outbox task execution
    use orcher_core::services::TaskDispatchService;

    let task_dispatch_service = if config.polling.use_sync_match {
        {
            // Sync-match: direct handoff to a waiting worker.
            let _routing_config = config.polling.to_routing_config();
            info!("╔══════════════════════════════════════════════════════════════════╗");
            info!("║                TASK DISPATCH WITH SYNC-MATCH                     ║");
            info!("╠══════════════════════════════════════════════════════════════════╣");
            info!("║  Mode: Direct dispatch to waiting workers                        ║");
            info!("║  Durability: outbox row, committed with the workflow state       ║");
            info!("║  Expected: 80%+ sync-match rate, P99 < 1ms dispatch latency      ║");
            info!("╚══════════════════════════════════════════════════════════════════╝");

            Arc::new(
                TaskDispatchService::with_sync_match(polling_manager.clone(), state_cache.clone())
                    .with_repositories(workflow_execution_repo.clone(), journal_repo.clone()),
            )
        }
    } else {
        // NOTE: Sharding configuration is read from environment variables:
        //   - ORCHER_SHARDING_ENABLED (default: false)
        //   - ORCHER_SHARD_COUNT (default: 1)
        // These MUST match the worker configuration or performance will degrade!

        // Log sharding configuration prominently at startup
        let sharding_enabled = std::env::var("ORCHER_SHARDING_ENABLED")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);
        let shard_count: u32 = std::env::var("ORCHER_SHARD_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        info!("╔══════════════════════════════════════════════════════════════════╗");
        info!("║                   SHARDING CONFIGURATION                         ║");
        info!("╠══════════════════════════════════════════════════════════════════╣");
        if sharding_enabled {
            info!("║  SHARDING: ENABLED                                              ║");
            info!(
                "║  Shard Count: {:>4}                                              ║",
                shard_count
            );
            info!("║  Queue Pattern: task_queue:namespace:shard:N                    ║");
            info!("╠══════════════════════════════════════════════════════════════════╣");
            info!("║  Workers MUST set:                                               ║");
            info!("║    ORCHER_SHARDING_ENABLED=true                                  ║");
            info!(
                "║    ORCHER_SHARD_COUNT={}                                          ║",
                shard_count
            );
            info!("║    ORCHER_ASSIGNED_SHARDS=<comma-separated shard IDs>            ║");
        } else {
            warn!("║  SHARDING: DISABLED                                             ║");
            warn!("║  Queue Pattern: task_queue:namespace                             ║");
            warn!("╠══════════════════════════════════════════════════════════════════╣");
            warn!("║  If workers have ORCHER_SHARDING_ENABLED=true, they will NOT     ║");
            warn!("║                                                                   ║");
            warn!("║  To enable sharding, restart orchestrator with:                  ║");
            warn!("║    ORCHER_SHARDING_ENABLED=true ORCHER_SHARD_COUNT=N             ║");
        }
        info!("╚══════════════════════════════════════════════════════════════════╝");

        Arc::new(TaskDispatchService::new(polling_manager.clone()))
    };

    info!(
        sync_match_enabled = config.polling.use_sync_match,
        "TaskDispatchService initialized for task dispatch"
    );

    // Matching layer (opt-in via ORCHER_MATCHING_ENABLED). Delivery is separated from
    // partitioned persistence: the dispatch service (run by the per-shard outbox
    // processors) transfers a DispatchTask into its (namespace, task_queue) matching
    // partition, and the worker poll claims from that partition — so a worker reaches a
    // shard-resident task without scanning every shard. The store is on the coordination
    // database. Absent ⇒ the legacy primary-outbox path, unchanged.
    let mut pg_matching_store: Option<
        std::sync::Arc<orcher_storage::repositories::domains::matching::PgMatchingStore>,
    > = None;
    if matching_enabled {
        let mut store =
            orcher_storage::repositories::domains::matching::PgMatchingStore::new(pool.clone());
        // Each claim names this engine, for the release of a dead one's claims.
        if let Some((node_id, _, _)) = &cluster_node {
            store = store.with_node(node_id.clone());
        }
        let store = std::sync::Arc::new(store);
        pg_matching_store = Some(store.clone());
        let matching_store: std::sync::Arc<
            dyn orcher_storage::repositories::domains::matching::MatchingStore,
        > = store;
        task_dispatch_service.set_matching_store(matching_store.clone());
        task_polling_manager.set_matching_store(matching_store.clone());
        // Its reclaimer returns the rows whose lease ran out before their task
        // was taken.
        polling_manager.set_matching_store(matching_store);
        info!(
            "matching layer enabled: DispatchTask delivery via (namespace, task_queue) partitions"
        );
    }

    // A member is treated as dead one heartbeat interval after it drops out
    // of the live set — about four missed heartbeats. By then a worker that
    // got an activation from it just before it died has almost always
    // completed it through another engine, and is not made to repeat it; the
    // wait costs seconds where the claim timeout costs minutes.
    if let (Some((node_id, stale_after_secs, heartbeat_secs)), Some(membership)) =
        (&cluster_node, &cluster_membership)
    {
        let grace_secs = *heartbeat_secs;
        let mut release = orcher_core::engine::dispatch::DeadNodeClaims::new(
            node_id.clone(),
            membership.clone(),
            workflow_execution_repo.clone(),
            polling_manager.clone(),
            *stale_after_secs,
            grace_secs,
            config.polling.claim_timeout_secs as i64,
        );
        if let Some(store) = &pg_matching_store {
            release = release.with_tasks(
                store.clone(),
                task_execution_repo.clone(),
                task_polling_manager.clone(),
            );
        }
        let _ = dead_node_claims.set(Arc::new(release));
        info!(
            stale_after_secs,
            grace_secs, "cluster: a dead member's outstanding claims are released"
        );
    }

    // Wire sync-match fast path into WorkflowExecutionService.
    // This allows start_workflow to dispatch directly to waiting workers
    // instead of waiting for the outbox processor poll cycle.
    workflow_execution_service.set_task_dispatch(task_dispatch_service.clone());
    workflow_coordinator.set_task_dispatch_service(task_dispatch_service.clone());
    // Let the dispatch service wake the activity task pollers directly (their waiters
    // live in TaskPollingManager, not polling_manager) — otherwise a dispatched task is
    // only picked up on the poller's ~200ms retry.
    task_dispatch_service.set_task_polling_manager(task_polling_manager.clone());
    info!("Sync-match fast path wired to WorkflowExecutionService and WorkflowCoordinator");

    // Initialize TimerExecutionService for timer task execution
    use orcher_core::services::TimerExecutionService;
    let timer_execution_service = Arc::new(TimerExecutionService::new(
        workflow_execution_repo.clone(),
        journal_service.clone(),
        polling_manager.clone(),
    ));
    info!("TimerExecutionService initialized for timer execution");

    // Initialize ScheduleExecutionService for cron-based workflow scheduling
    use orcher_core::services::ScheduleExecutionService;
    let schedule_execution_service = Arc::new(ScheduleExecutionService::new(
        workflow_execution_service.clone(),
        polling_manager.clone(),
    ));
    info!("ScheduleExecutionService initialized for cron schedule execution");

    // Start OutboxProcessor with TaskDispatchService executor
    // Using the new method that properly tracks handles for health checks
    info!("Starting OutboxProcessor with TaskDispatchService");
    {
        let task_dispatch = task_dispatch_service.clone();
        let outbox_executor = move |task: orcher_core::engine::queues::OutboxTask| {
            let service = task_dispatch.clone();
            async move { service.execute_task(task).await }
        };

        orchestration_system
            .start_outbox_processor_with_executor(outbox_executor)
            .await
            .expect("Failed to start OutboxProcessor");

        info!("OutboxProcessor started successfully with TaskDispatchService (handle tracked)");
    }

    // Start TimerProcessor with TimerExecutionService executor
    // Using the new method that properly tracks handles for health checks
    info!("Starting TimerProcessor with TimerExecutionService");
    {
        let timer_execution = timer_execution_service.clone();
        let timer_executor = move |task: orcher_core::engine::queues::TimerTask| {
            let service = timer_execution.clone();
            async move { service.execute_task(task).await }
        };

        orchestration_system
            .start_timer_processor_with_executor(timer_executor)
            .await
            .expect("Failed to start TimerProcessor");

        info!("TimerProcessor started successfully with TimerExecutionService (handle tracked)");
    }

    // Start ScheduleProcessor with ScheduleExecutionService executor
    info!("Starting ScheduleProcessor with ScheduleExecutionService");
    {
        let schedule_execution = schedule_execution_service.clone();
        let schedule_executor = move |schedule: orcher_storage::schema::types::Schedule| -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = orcher_common::OrcherResult<orcher_common::uuid::Uuid>,
                    > + Send,
            >,
        > {
            let service = schedule_execution.clone();
            Box::pin(async move { service.execute_schedule(schedule).await })
        };

        orchestration_system
            .start_schedule_processor_with_executor(schedule_executor)
            .await
            .expect("Failed to start ScheduleProcessor");

        info!(
            "ScheduleProcessor started successfully with ScheduleExecutionService (handle tracked)"
        );
    }

    // Start BatchProcessor for bulk workflow operations
    info!("Starting BatchProcessor");
    {
        let batch_processor = std::sync::Arc::new(
            orcher_core::engine::processors::BatchProcessor::new(
                pool.clone(),
                workflow_execution_repo.clone(),
                workflow_execution_service.clone(),
                orcher_core::engine::processors::BatchProcessorConfig::default(),
            )
            // A send_event operation delivers to each run as SendEvent does.
            .with_event_delivery(
                orcher_core::workflows::event_delivery::EventDelivery::new(
                    journal_repo.clone(),
                    workflow_execution_repo.clone(),
                )
                .with_wake(polling_manager.clone()),
            ),
        );
        let bp = batch_processor.clone();
        tokio::spawn(async move {
            if let Err(e) = bp.start().await {
                tracing::error!("BatchProcessor failed: {}", e);
            }
        });
        info!("BatchProcessor started successfully");
    }

    // Initialize ActorService for actor operations
    info!(
        service = "grpc",
        component = "ActorService",
        "Initializing ActorService for stateful actor operations"
    );

    // Initialize actor state storage
    let actor_storage = Arc::new(ActorStateStorage::new(pool.clone()));
    info!("ActorStateStorage initialized");

    // Initialize actor service registry
    let actor_registry = Arc::new(ServiceRegistry::new());
    info!("Actor ServiceRegistry initialized");

    // Initialize actor router (fair readers–writer scheduling per actor key:
    // exclusive ops serialize, shared ops run concurrently between barriers)
    let actor_router = Arc::new(ActorRouter::new());
    info!("ActorRouter initialized with fair readers-writer per-key scheduling");

    // Create ActorServiceImpl (clone the registry Arc so the stale-service
    // reaper below shares the same registration state)
    let actor_service = ActorServiceImpl::new(actor_router, actor_storage, actor_registry.clone());

    info!(
        service = "grpc",
        component = "ActorService",
        endpoints =
            "InvokeOperation, ExecuteOperation, GetState, SetState, RegisterHandlers, Heartbeat",
        "ActorService created successfully for actor operations"
    );

    // Create WorkerServiceImpl for worker registration with DB persistence
    let worker_repo = WorkerRepository::new(pool.clone());
    let worker_service =
        WorkerServiceImpl::new(worker_registry.clone()).with_worker_repository(worker_repo.clone());

    info!(
        service = "grpc",
        component = "WorkerService",
        endpoints = "RegisterWorker, WorkerHeartbeat, DeregisterWorker",
        "WorkerService created successfully for worker registration"
    );

    // Create NamespaceServiceImpl for namespace CRUD operations (reuses shared namespace_repo)
    let namespace_service =
        orcher_core::grpc::services::namespace::NamespaceServiceImpl::new(namespace_repo.clone());

    info!(
        service = "grpc",
        component = "NamespaceService",
        endpoints = "CreateNamespace, GetNamespace, ListNamespaces, UpdateNamespace, DeprecateNamespace, DeleteNamespace",
        "NamespaceService created successfully"
    );

    // Spawn background task to clean up stale workers every 60 seconds
    {
        let registry = worker_registry.clone();
        // The in-memory cleanup below drops a silent worker from the registry
        // but leaves its row untouched, so the workers list kept showing dead
        // pods as idle and healthy. Age the rows out on the same tick.
        let worker_rows = worker_repo.clone();
        let mut cleanup_shutdown = shutdown.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await; // skip immediate tick
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let removed = registry.cleanup_stale_workers().await;
                        if removed > 0 {
                            info!(removed = removed, "Stale worker cleanup completed");
                        }
                        match worker_rows
                            .age_out_silent_workers(
                                WORKER_DEGRADED_AFTER_SECS,
                                WORKER_OFFLINE_AFTER_SECS,
                            )
                            .await
                        {
                            Ok((degraded, offline)) if degraded + offline > 0 => info!(
                                degraded = degraded,
                                offline = offline,
                                "Aged out workers that stopped heartbeating"
                            ),
                            Ok(_) => {}
                            Err(e) => warn!(error = %e, "Failed to age out silent workers"),
                        }
                    }
                    _ = cleanup_shutdown.recv() => {
                        info!("Worker cleanup task shutting down");
                        break;
                    }
                }
            }
        });
        info!("Worker stale-cleanup background task started (60s interval)");
    }

    // Spawn background task to reap stale actor service registrations every
    // 60 seconds. Dead workers' handler registrations otherwise linger
    // forever: a stale exclusive declaration can outvote the live fleet when
    // resolving an operation's mode, and InvokeOperation's fast-fail cannot
    // tell that an actor's only workers have died. The registry reaps a
    // service after 2× its heartbeat timeout (60s by default) of silence.
    {
        let registry = actor_registry.clone();
        let mut cleanup_shutdown = shutdown.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await; // skip immediate tick
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let removed = registry.cleanup_stale_services().await;
                        if removed > 0 {
                            info!(removed = removed, "Stale actor service cleanup completed");
                        }
                    }
                    _ = cleanup_shutdown.recv() => {
                        info!("Actor service cleanup task shutting down");
                        break;
                    }
                }
            }
        });
        info!("Actor service stale-cleanup background task started (60s interval)");
    }

    // Initialize result consumer if queue is enabled
    // Install the global Prometheus metrics recorder so all counter!/gauge!/histogram!
    // calls are captured in a real prometheus::Registry (not a no-op sink).
    let prometheus_registry =
        orcher_monitoring::exporters::prometheus::install_prometheus_recorder();
    info!(
        component = "metrics",
        format = "prometheus",
        "Prometheus metrics recorder installed"
    );

    // Create health manager with OrchestrationSystem integration and Prometheus registry
    let health = Arc::new(
        health::HealthManager::with_orchestration_system(orchestration_system.clone())
            .with_prometheus_registry(prometheus_registry)
            // Every database holding executions, so in-flight workflows are
            // counted where they live rather than tracked in memory.
            .with_metric_pools(
                std::iter::once(pool.clone())
                    .chain(shard_pools.iter().map(|shard| (**shard).clone()))
                    .collect(),
            ),
    );
    info!("Health manager initialized with OrchestrationSystem integration");

    // Initialize authentication layer
    let (auth_layer, authenticator) = {
        use orcher_core::auth::{
            ApiKeyAuthenticator, AuthCacheConfig, Authenticator, CachedAuthenticator, KeyOrToken,
            NoAuthenticator, OidcAuthenticator,
        };
        use orcher_core::grpc::auth_layer::{AuthLayer, AuthMode};
        use std::sync::Arc;

        let auth_mode = config.security.auth_mode.clone();
        let tenancy = config.security.tenancy;
        info!(auth_mode = ?auth_mode, tenancy = ?tenancy, "Configuring gRPC authentication");

        let cache = AuthCacheConfig {
            ttl: Duration::from_secs(config.security.api_key_cache_ttl_secs),
            max_entries: config.security.api_key_cache_max_entries,
        };
        let tokens: Option<Arc<dyn Authenticator>> = config.security.oidc.clone().map(|oidc| {
            info!(issuer = %oidc.issuer, audience = %oidc.audience, "Accepting OIDC tokens");
            Arc::new(OidcAuthenticator::new(oidc)) as Arc<dyn Authenticator>
        });
        // Unset, a deployment serves one tenant. Refused when its own data
        // says it serves several: then every caller without an organization
        // would see every tenant's runs.
        if tenancy.is_none() {
            refuse_undeclared_tenants(&pool, &config.security).await?;
            info!("security.tenancy is not set: serving a single tenant");
        }

        let authenticator: Arc<dyn Authenticator> = match &auth_mode {
            AuthMode::Oidc => Arc::new(CachedAuthenticator::with_config(
                tokens.clone().expect("validated: oidc mode has a provider"),
                cache.clone(),
            )),
            AuthMode::ApiKey | AuthMode::ApiKeyAndMtls => {
                // Keys live in the `api_keys` table of the primary database.
                warn_of_unscoped_keys(&pool).await;
                let keys = ApiKeyAuthenticator::new(pool.clone());
                let keys: Arc<dyn Authenticator> = Arc::new(keys);
                let accepted: Arc<dyn Authenticator> = match tokens.clone() {
                    Some(tokens) => Arc::new(KeyOrToken::new(keys, tokens)),
                    None => keys,
                };
                let cached = Arc::new(CachedAuthenticator::with_config(accepted, cache.clone()));
                // A key revoked anywhere is forgotten here at once, rather
                // than accepted until its cache entry expires.
                tokio::spawn(orcher_core::auth::keys::follow_revocations(
                    pool.clone(),
                    cached.clone(),
                ));
                cached
            }
            // No credential is asked for: the layer never consults it.
            AuthMode::None | AuthMode::Mtls => Arc::new(NoAuthenticator),
        };
        (
            AuthLayer::new(authenticator.clone(), auth_mode).with_tenancy(tenancy),
            authenticator,
        )
    };

    // The REST API, served on the HTTP address. Its handlers call the gRPC
    // services over an in-process connection, which the gRPC server serves
    // behind the same authentication as its socket, so a REST caller is
    // granted exactly what the same credential is granted over gRPC.
    let (loopback, rest_api) = if config.serves_rest_api() {
        let (dialer, incoming) = orcher_core::grpc::loopback::loopback();
        let orchestrator = orcher_server::config::OrchestratorConfig {
            endpoint: "in-process".to_string(),
            ..orcher_server::config::OrchestratorConfig::development()
        };
        let mut state = orcher_server::state::AppState::with_orchestrator_channel(
            pool.clone(),
            orchestrator,
            dialer.channel(),
        );
        state.set_authenticator(authenticator);
        state.set_tenancy(config.security.tenancy);
        let rest_api = orcher_server::server::Server::new(
            orcher_server::server::ServerConfig {
                host: String::new(),
                port: 0,
                ..Default::default()
            },
            state,
        )
        .router();
        (Some(incoming), Some(rest_api))
    } else {
        info!(
            auth_mode = ?config.security.auth_mode,
            "The REST API is not served: this deployment requires a client certificate, which \
             only gRPC connections present"
        );
        drop(authenticator);
        (None, None)
    };

    // Start gRPC server with all services
    let grpc_manager = server::GrpcServerManager::new(
        config.server.clone(),
        config.limits.grpc_max_message_bytes,
        auth_layer,
        config.security.tls.clone(),
        execution_service,
        workflow_service,
        query_service,
        actor_service,
        worker_service,
        namespace_service,
    );
    let grpc_manager = match loopback {
        Some(incoming) => grpc_manager.with_loopback(incoming),
        None => grpc_manager,
    };
    let mut grpc_handle = grpc_manager.start(shutdown.subscribe()).await?;
    info!(
        protocol = "grpc",
        address = %config.server.grpc_addr,
        services = "ExecutionService, WorkflowService, QueryService, ActorService, WorkerService, NamespaceService",
        max_concurrent_streams = config.server.max_concurrent_streams,
        "gRPC server started"
    );

    // Start HTTP server with health endpoints
    let serves_rest_api = rest_api.is_some();
    let http_manager = server::HttpServerManager::new(config.server.clone(), (*health).clone());
    let http_manager = match rest_api {
        Some(rest_api) => http_manager.with_rest_api(rest_api),
        None => http_manager,
    };
    // The dashboard calls the REST API, so it is served only beside it
    let dashboard = match ui::Dashboard::from_config(&config.ui) {
        ui::Dashboard::Serve(dir) if serves_rest_api => Some(dir),
        ui::Dashboard::Serve(_) => {
            info!("The dashboard is not served: it needs the REST API, which this deployment does not serve");
            None
        }
        ui::Dashboard::Off => {
            info!("The dashboard is off (ui.enabled = false)");
            None
        }
        ui::Dashboard::Missing(dir) => {
            info!(
                dir = %dir.display(),
                "No dashboard to serve (no index.html in ui.dir): serving the API only"
            );
            None
        }
    };
    let serves_dashboard = dashboard.is_some();
    let http_manager = match dashboard {
        Some(dir) => http_manager.with_dashboard(dir),
        None => http_manager,
    };
    let mut http_handle = http_manager.start(shutdown.subscribe()).await?;
    info!(
        protocol = "http",
        address = %config.server.http_addr,
        endpoints = match (serves_rest_api, serves_dashboard) {
            (true, true) => "health,metrics,api/v1,dashboard",
            (true, false) => "health,metrics,api/v1",
            _ => "health,metrics",
        },
        "HTTP server started"
    );

    // Log OrchestrationSystem summary
    let system_summary = orchestration_system.summary().await;
    info!(
        component = "orchestration_system",
        summary = %system_summary,
        "OrchestrationSystem operational"
    );

    // Mark service as ready
    health.set_ready(true).await;
    // Check OrchestrationSystem health
    let system_health = orchestration_system.health().await;
    info!(
        status = "ready",
        grpc_addr = %config.server.grpc_addr,
        http_addr = %config.server.http_addr,
        orchestration_healthy = system_health.healthy,
        orchestration_operational = system_health.is_operational(),
        "Orchestrator service initialized successfully - all components ready"
    );

    // Spawn signal handler
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        shutdown::wait_for_signal().await;
        shutdown_clone.trigger();
    });

    // Wait for shutdown signal
    let mut shutdown_rx = shutdown.subscribe();
    // Or for either server to stop on its own. An engine whose gRPC server has
    // died still runs its background work and still reports ready, but no
    // worker can reach it; exiting with an error lets the supervisor restart it.
    let server_failure = tokio::select! {
        _ = shutdown_rx.recv() => None,
        exit = &mut grpc_handle => Some(server_exit_error("gRPC", exit)),
        exit = &mut http_handle => Some(server_exit_error("HTTP", exit)),
    };
    if let Some(ref error) = server_failure {
        tracing::error!(error = %format!("{error:#}"), "A server stopped unexpectedly; shutting down");
        shutdown.trigger();
    }

    info!(
        signal = "shutdown",
        "Shutdown signal received, stopping services"
    );

    // Mark service as not ready
    health.set_ready(false).await;
    info!(status = "draining", "Service marked as not ready");

    // Stop background jobs gracefully
    info!(jobs_count = job_handles.len(), "Stopping background jobs");
    for handle in job_handles {
        if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(10), handle).await {
            warn!("Background job did not complete within timeout: {}", e);
        }
    }
    info!("Background jobs stopped");

    // Stop TimeoutWatcher gracefully
    if let Some((timeout_watcher, watcher_handle)) = timeout_watcher_handle {
        info!("Stopping TimeoutWatcher...");
        timeout_watcher.stop().await;

        match tokio::time::timeout(std::time::Duration::from_secs(10), watcher_handle).await {
            Ok(Ok(_)) => info!("TimeoutWatcher stopped successfully"),
            Ok(Err(e)) => warn!("TimeoutWatcher stopped with error: {}", e),
            Err(_) => warn!("TimeoutWatcher did not stop within timeout, continuing shutdown"),
        }
    }

    // Stop WorkflowRetryScheduler gracefully
    if let Some((workflow_retry_scheduler, scheduler_handle)) = workflow_retry_scheduler_handle {
        info!("Stopping WorkflowRetryScheduler...");
        workflow_retry_scheduler.stop().await;

        match tokio::time::timeout(std::time::Duration::from_secs(10), scheduler_handle).await {
            Ok(Ok(_)) => info!("WorkflowRetryScheduler stopped successfully"),
            Ok(Err(e)) => warn!("WorkflowRetryScheduler stopped with error: {}", e),
            Err(_) => {
                warn!("WorkflowRetryScheduler did not stop within timeout, continuing shutdown")
            }
        }
    }

    // Stop the child reconciler after the current pass
    if let Some((child_reconciler, reconciler_handle)) = child_reconciler_handle {
        info!("Stopping child reconciler...");
        child_reconciler.stop();
        match tokio::time::timeout(std::time::Duration::from_secs(10), reconciler_handle).await {
            Ok(Ok(_)) => info!("Child reconciler stopped successfully"),
            Ok(Err(e)) => warn!("Child reconciler stopped with error: {}", e),
            Err(_) => warn!("Child reconciler did not stop within timeout, continuing shutdown"),
        }
    }

    // Shutdown OrchestrationSystem gracefully
    info!("Shutting down OrchestrationSystem");
    if let Ok(system) = Arc::try_unwrap(orchestration_system) {
        if let Err(e) = system.shutdown().await {
            warn!("Error during OrchestrationSystem shutdown: {}", e);
        } else {
            info!("OrchestrationSystem shut down successfully");
        }
    } else {
        info!("OrchestrationSystem has multiple references, skipping graceful shutdown");
    }

    // Wait for servers to stop. A handle that already resolved above has been
    // consumed and must not be polled again.
    for handle in [grpc_handle, http_handle] {
        if !handle.is_finished() || server_failure.is_none() {
            let _ = handle.await;
        }
    }

    // Nothing can be handed out any more: give back what the prefetch buffer
    // still holds, then stop heartbeating and say this node has left, so the
    // others take over its shards now rather than once it has gone stale.
    let released = polling_manager.release_prefetched().await;
    if released > 0 {
        info!(released, "Released the prefetched claims no poll will take");
    }
    if let Some((stop, membership_loop)) = membership_stop {
        // Stopped before the row is marked, so no last heartbeat marks it
        // active again.
        let _ = stop.send(true);
        let _ = membership_loop.await;
        if let (Some(membership), Some((node_id, _, _))) = (&cluster_membership, &cluster_node) {
            if let Err(e) = membership.set_member_status(node_id, "offline").await {
                warn!(error = %e, "cluster: could not mark this node as left");
            }
        }
    }

    info!(
        status = "shutdown",
        duration = "graceful",
        "Orchestrator service shut down successfully"
    );
    match server_failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The address this node advertises to its peers, if it has one they can dial:
/// `ORCHER_ADVERTISE_ADDR`, else the config file's `cluster.advertise_address`,
/// else the gRPC bind address when that names a concrete host. A wildcard bind
/// address (`0.0.0.0:50051`) is where this node listens, not where a peer can
/// reach it, so it yields `None`.
fn advertise_address(
    from_env: Option<String>,
    configured: Option<&str>,
    grpc_addr: &str,
) -> Option<String> {
    let explicit = from_env
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| {
            configured
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        });
    use orcher_cluster::address::{is_loopback, normalize};
    match explicit {
        // Explicit, it is taken as meant — loopback included, which is what
        // several engines on one machine need.
        Some(address) => {
            let normalized = normalize(&address);
            if normalized.is_none() {
                warn!(
                    address = %address,
                    "cluster: the configured advertise address cannot be dialed by a peer; ignoring it"
                );
            }
            normalized
        }
        // The bind address only when it names a host peers elsewhere can
        // reach: never the wildcard, and never loopback, which on another
        // machine means that machine.
        None => normalize(grpc_addr).filter(|a| !is_loopback(a)),
    }
}

/// Why a server task ended before the engine asked it to.
fn server_exit_error(
    server: &str,
    exit: Result<Result<()>, tokio::task::JoinError>,
) -> anyhow::Error {
    match exit {
        Ok(Ok(())) => anyhow::anyhow!("{server} server stopped without a shutdown signal"),
        Ok(Err(error)) => error.context(format!("{server} server stopped")),
        Err(join) => anyhow::anyhow!("{server} server task panicked: {join}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pool that is never connected: the OIDC branch of the guard decides
    /// before it reads anything.
    fn unconnected_pool() -> sqlx::PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://nobody@127.0.0.1:1/none")
            .expect("a lazy pool")
    }

    fn oidc(organization_claim: Option<&str>) -> orcher_core::auth::OidcConfig {
        serde_json::from_value(serde_json::json!({
            "issuer": "https://idp.example.com",
            "audience": "orcher",
            "organization_claim": organization_claim,
        }))
        .expect("an OIDC configuration")
    }

    #[tokio::test]
    async fn tokens_that_name_organizations_need_a_declared_tenancy() {
        let mut security = config::OrchestratorConfig::default().security;
        security.auth_mode = orcher_core::grpc::auth_layer::AuthMode::Oidc;
        security.oidc = Some(oidc(Some("org_id")));
        let refused = refuse_undeclared_tenants(&unconnected_pool(), &security)
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.contains("security.tenancy"), "{refused}");

        // Tokens that name no organization: a single tenant, and no key is
        // read in OIDC mode.
        security.oidc = Some(oidc(None));
        refuse_undeclared_tenants(&unconnected_pool(), &security)
            .await
            .expect("single-tenant tokens start");
    }

    #[test]
    fn test_cli_parsing() {
        let cli = Cli::parse_from(&["orchestrator", "--log-level", "debug"]);
        assert_eq!(cli.log_level, "debug");
        assert_eq!(cli.log_format, "compact");
    }

    #[test]
    fn test_default_config() {
        let config = config::OrchestratorConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn the_environment_wins_over_the_file_and_the_bind_address() {
        assert_eq!(
            advertise_address(
                Some("10.0.0.7:50051".into()),
                Some("10.0.0.8:50051"),
                "10.0.0.9:50051"
            )
            .as_deref(),
            Some("10.0.0.7:50051")
        );
        assert_eq!(
            advertise_address(None, Some("10.0.0.8:50051"), "0.0.0.0:50051").as_deref(),
            Some("10.0.0.8:50051")
        );
    }

    #[test]
    fn a_concrete_bind_address_is_advertised_and_a_wildcard_or_loopback_is_not() {
        assert_eq!(
            advertise_address(None, None, "10.0.0.9:50051").as_deref(),
            Some("10.0.0.9:50051")
        );
        assert_eq!(advertise_address(None, None, "0.0.0.0:50051"), None);
        assert_eq!(
            advertise_address(Some("  ".into()), None, "[::]:50051"),
            None
        );
        assert_eq!(
            advertise_address(None, None, "127.0.0.1:50151"),
            None,
            "a loopback bind address means this machine only"
        );
        assert_eq!(advertise_address(None, None, "[::1]:50051"), None);
    }

    #[test]
    fn an_explicit_loopback_address_is_advertised() {
        // Several engines on one machine, as the multi-node harness runs them.
        assert_eq!(
            advertise_address(Some("127.0.0.1:50151".into()), None, "0.0.0.0:50151").as_deref(),
            Some("127.0.0.1:50151")
        );
    }

    #[test]
    fn an_unbracketed_ipv6_pod_address_is_advertised_bracketed() {
        assert_eq!(
            advertise_address(Some("fd00:10:244::5:50051".into()), None, "[::]:50051").as_deref(),
            Some("[fd00:10:244::5]:50051")
        );
    }

    #[test]
    fn an_undialable_advertise_address_is_not_used() {
        assert_eq!(
            advertise_address(Some("0.0.0.0:50051".into()), None, "10.0.0.9:50051"),
            None,
            "an explicit but useless setting is a mistake, not a cue to guess"
        );
    }
}

/// How long after its claim an activation counts as in flight: a wake-up that
/// arrives sooner is left for that activation's completion to take, and one
/// that arrives later releases the claim.
///
/// `ORCHER_ACTIVATION_IN_FLIGHT_SECS` if set; otherwise half the workflow claim
/// timeout, which follows whatever that is configured to. The bound is a
/// trade-off both ways. Too long, and a workflow completed by an engine from
/// before activation numbering, or by a worker that died mid-activation, holds
/// every wake-up for that long. Too short, and an activation that really runs
/// longer than the bound can be superseded by a wake-up: the workflow is handed
/// to a second worker, and the first one's completion is then acknowledged
/// without being applied — the second replays and does the same work. Half the
/// claim timeout keeps the first well inside the time a lost activation is
/// already allowed, and leaves the second to activations that are close to
/// being taken back by the reclaimer anyway.
fn activation_in_flight_bound(claim_timeout_secs: u64) -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("ORCHER_ACTIVATION_IN_FLIGHT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|secs| *secs > 0)
            .unwrap_or((claim_timeout_secs / 2).max(1)),
    )
}

/// Marks a dead-member sweep as running for as long as it lives; see the
/// membership loop.
struct SweepRunning(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Drop for SweepRunning {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Warn when a key that belongs to no organization, and may not name one, can
/// be used beside keys that do belong to one.
///
/// A request with such a key acts for no organization, and a caller acting for
/// none is served every organization's work: its polls claim any execution and
/// any task, as a single-tenant deployment needs. Where organizations exist,
/// that key reaches all of them. It is left working, since refusing it would
/// break single-tenant setups that later add an organization, but said once
/// at startup so it is not a surprise. A failed check is only logged.
/// Refuses to serve, with tenancy unset, a deployment whose own data says it
/// has tenants: an active API key bound to an organization, or an identity
/// provider whose tokens name one. Unset tenancy is single-tenant, under which
/// a caller without an organization sees every organization's runs.
///
/// Only the engine's own `api_keys` is read; which organizations exist is not
/// the engine's to know.
async fn refuse_undeclared_tenants(
    pool: &sqlx::PgPool,
    security: &config::SecurityConfig,
) -> Result<()> {
    let tokens_name_tenants = matches!(
        security.auth_mode,
        orcher_core::grpc::auth_layer::AuthMode::Oidc
            | orcher_core::grpc::auth_layer::AuthMode::ApiKey
            | orcher_core::grpc::auth_layer::AuthMode::ApiKeyAndMtls
    ) && security
        .oidc
        .as_ref()
        .is_some_and(|oidc| oidc.organization_claim.is_some());
    anyhow::ensure!(
        !tokens_name_tenants,
        "security.oidc.organization_claim names each caller's organization, so this \
         deployment serves several tenants, but security.tenancy is not set. Set \
         security.tenancy = \"multi\"."
    );

    if !matches!(
        security.auth_mode,
        orcher_core::grpc::auth_layer::AuthMode::ApiKey
            | orcher_core::grpc::auth_layer::AuthMode::ApiKeyAndMtls
    ) {
        return Ok(());
    }
    let bound: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM api_keys \
         WHERE organization_id IS NOT NULL AND is_active AND revoked_at IS NULL \
           AND (expires_at IS NULL OR expires_at > NOW())",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to read the API keys: {e}"))?;
    anyhow::ensure!(
        bound == 0,
        "{bound} active API key(s) are bound to an organization, so this deployment serves \
         several tenants, but security.tenancy is not set. Set security.tenancy = \"multi\" \
         (or \"single\", if every caller without an organization should see every \
         organization's runs)."
    );
    Ok(())
}

async fn warn_of_unscoped_keys(pool: &sqlx::PgPool) {
    let counted: Result<(i64, i64), sqlx::Error> = sqlx::query_as(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE organization_id IS NULL AND NOT can_delegate_organization),
            COUNT(*) FILTER (WHERE organization_id IS NOT NULL)
        FROM api_keys
        WHERE is_active AND (expires_at IS NULL OR expires_at > NOW())
        "#,
    )
    .fetch_one(pool)
    .await;
    match counted {
        Ok((unscoped, scoped)) if unscoped > 0 && scoped > 0 => warn!(
            unscoped_keys = unscoped,
            organization_keys = scoped,
            "Some API keys belong to no organization and may not name one, beside keys that \
             do belong to one. A worker using such a key acts for no organization and is \
             handed every organization's executions and tasks. Give each key an \
             organization unless that is intended."
        ),
        Ok(_) => {}
        Err(e) => debug!(error = %e, "Could not check for API keys without an organization"),
    }
}
