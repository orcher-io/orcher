//! Health check endpoints for Kubernetes probes and monitoring

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use orcher_core::engine::OrchestrationSystem;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Manages health check state for the orchestrator service
#[derive(Clone)]
pub struct HealthManager {
    state: Arc<RwLock<HealthState>>,
    orchestration_system: Option<Arc<OrchestrationSystem>>,
    prometheus_registry: Option<prometheus::Registry>,
    /// Every database that holds workflow executions — the main pool and any
    /// shard pools — so in-flight workflows can be counted where they live.
    metric_pools: Vec<sqlx::PgPool>,
}

struct HealthState {
    ready: bool,
    live: bool,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    ready: bool,
    live: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    orchestration: Option<OrchestrationHealthStatus>,
}

#[derive(Serialize)]
struct OrchestrationHealthStatus {
    healthy: bool,
    operational: bool,
    coordinator_healthy: bool,
    outbox_state: String,
    timer_state: String,
}

impl HealthManager {
    /// Create a new health manager
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(HealthState {
                ready: false,
                live: true,
            })),
            orchestration_system: None,
            prometheus_registry: None,
            metric_pools: Vec::new(),
        }
    }

    /// Create a new health manager with OrchestrationSystem integration
    pub fn with_orchestration_system(orchestration_system: Arc<OrchestrationSystem>) -> Self {
        Self {
            state: Arc::new(RwLock::new(HealthState {
                ready: false,
                live: true,
            })),
            orchestration_system: Some(orchestration_system),
            prometheus_registry: None,
            metric_pools: Vec::new(),
        }
    }

    /// Set the Prometheus registry for rendering crate-level metrics
    pub fn with_prometheus_registry(mut self, registry: prometheus::Registry) -> Self {
        self.prometheus_registry = Some(registry);
        self
    }

    /// Count in-flight workflows from these databases when metrics are scraped.
    pub fn with_metric_pools(mut self, pools: Vec<sqlx::PgPool>) -> Self {
        self.metric_pools = pools;
        self
    }

    /// Set the readiness state
    pub async fn set_ready(&self, ready: bool) {
        self.state.write().await.ready = ready;
    }

    /// Set the liveness state
    #[allow(dead_code)]
    pub async fn set_live(&self, live: bool) {
        self.state.write().await.live = live;
    }

    /// Create the health check router
    pub fn router(self) -> Router {
        Router::new()
            .route("/health/ready", get(readiness_handler))
            .route("/health/live", get(liveness_handler))
            .route("/health", get(health_handler))
            .route("/metrics/orchestration", get(orchestration_summary_handler))
            .route("/metrics", get(prometheus_metrics_handler))
            .with_state(self)
    }
}

impl Default for HealthManager {
    fn default() -> Self {
        Self::new()
    }
}

async fn readiness_handler(State(health): State<HealthManager>) -> impl IntoResponse {
    let state = health.state.read().await;
    if state.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn liveness_handler(State(health): State<HealthManager>) -> impl IntoResponse {
    let state = health.state.read().await;
    if state.live {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn health_handler(State(health): State<HealthManager>) -> impl IntoResponse {
    let state = health.state.read().await;

    // Get OrchestrationSystem health if available
    let orchestration_health = if let Some(ref system) = health.orchestration_system {
        let system_health = system.health().await;
        Some(OrchestrationHealthStatus {
            healthy: system_health.healthy,
            operational: system_health.is_operational(),
            coordinator_healthy: system_health.coordinator_healthy,
            outbox_state: format!("{:?}", system_health.outbox_state),
            timer_state: format!("{:?}", system_health.timer_state),
        })
    } else {
        None
    };

    let overall_healthy =
        state.ready && state.live && orchestration_health.as_ref().is_none_or(|h| h.healthy);

    let response = HealthResponse {
        status: if overall_healthy {
            "healthy"
        } else {
            "unhealthy"
        },
        ready: state.ready,
        live: state.live,
        orchestration: orchestration_health,
    };
    Json(response)
}

async fn orchestration_summary_handler(State(health): State<HealthManager>) -> impl IntoResponse {
    if let Some(ref system) = health.orchestration_system {
        let summary = system.summary().await;
        (StatusCode::OK, format!("{}", summary))
    } else {
        (
            StatusCode::NOT_FOUND,
            "OrchestrationSystem not available".to_string(),
        )
    }
}

/// Prometheus metrics endpoint handler
/// Returns metrics in Prometheus text exposition format
async fn prometheus_metrics_handler(State(health): State<HealthManager>) -> impl IntoResponse {
    use std::fmt::Write;

    let mut output = String::new();

    // Add orchestration system metrics if available
    if let Some(ref system) = health.orchestration_system {
        let system_health = system.health().await;

        // Coordinator health
        writeln!(
            output,
            "# HELP orcher_coordinator_healthy Whether the coordinator is healthy (1=healthy, 0=unhealthy)"
        )
        .unwrap();
        writeln!(output, "# TYPE orcher_coordinator_healthy gauge").unwrap();
        writeln!(
            output,
            "orcher_coordinator_healthy {}",
            if system_health.coordinator_healthy {
                1
            } else {
                0
            }
        )
        .unwrap();

        // Overall system health
        writeln!(
            output,
            "# HELP orcher_system_healthy Whether the orchestration system is healthy"
        )
        .unwrap();
        writeln!(output, "# TYPE orcher_system_healthy gauge").unwrap();
        writeln!(
            output,
            "orcher_system_healthy {}",
            if system_health.healthy { 1 } else { 0 }
        )
        .unwrap();

        // System operational status
        writeln!(
            output,
            "# HELP orcher_system_operational Whether the orchestration system is operational"
        )
        .unwrap();
        writeln!(output, "# TYPE orcher_system_operational gauge").unwrap();
        writeln!(
            output,
            "orcher_system_operational {}",
            if system_health.is_operational() { 1 } else { 0 }
        )
        .unwrap();

        // Processor states (0=Stopped, 1=Starting, 2=Running, 3=ShuttingDown)
        writeln!(
            output,
            "# HELP orcher_outbox_processor_state State of the outbox processor"
        )
        .unwrap();
        writeln!(output, "# TYPE orcher_outbox_processor_state gauge").unwrap();
        writeln!(
            output,
            "orcher_outbox_processor_state {}",
            processor_state_value(&system_health.outbox_state)
        )
        .unwrap();

        writeln!(
            output,
            "# HELP orcher_timer_processor_state State of the timer processor"
        )
        .unwrap();
        writeln!(output, "# TYPE orcher_timer_processor_state gauge").unwrap();
        writeln!(
            output,
            "orcher_timer_processor_state {}",
            processor_state_value(&system_health.timer_state)
        )
        .unwrap();
    }

    // Add basic service info
    writeln!(
        output,
        "# HELP orcher_info Orcher orchestrator service info"
    )
    .unwrap();
    writeln!(output, "# TYPE orcher_info gauge").unwrap();
    writeln!(
        output,
        "orcher_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    )
    .unwrap();

    // Readiness and liveness
    let state = health.state.read().await;
    writeln!(output, "# HELP orcher_ready Whether the service is ready").unwrap();
    writeln!(output, "# TYPE orcher_ready gauge").unwrap();
    writeln!(output, "orcher_ready {}", if state.ready { 1 } else { 0 }).unwrap();

    writeln!(output, "# HELP orcher_live Whether the service is live").unwrap();
    writeln!(output, "# TYPE orcher_live gauge").unwrap();
    writeln!(output, "orcher_live {}", if state.live { 1 } else { 0 }).unwrap();

    write_workflows_executing(&mut output, &health.metric_pools).await;
    write_oldest_ready(&mut output, &health.metric_pools).await;

    // Append all crate-level metrics from the Prometheus registry
    if let Some(ref registry) = health.prometheus_registry {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = registry.gather();
        if let Ok(crate_metrics) = encoder.encode_to_string(&metric_families) {
            writeln!(output).unwrap();
            output.push_str(&crate_metrics);
        }
    }

    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
}

/// Convert processor state to numeric value for Prometheus
fn processor_state_value(state: &orcher_core::engine::ProcessorState) -> i32 {
    use orcher_core::engine::ProcessorState;
    match state {
        ProcessorState::Stopped => 0,
        ProcessorState::Starting => 1,
        ProcessorState::Running => 2,
        ProcessorState::ShuttingDown => 3,
        ProcessorState::Failed => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_manager_creation() {
        let manager = HealthManager::new();
        let state = manager.state.read().await;
        assert!(!state.ready);
        assert!(state.live);
    }

    #[tokio::test]
    async fn test_set_ready() {
        let manager = HealthManager::new();
        manager.set_ready(true).await;
        let state = manager.state.read().await;
        assert!(state.ready);
    }

    #[tokio::test]
    async fn test_set_live() {
        let manager = HealthManager::new();
        manager.set_live(false).await;
        let state = manager.state.read().await;
        assert!(!state.live);
    }

    #[tokio::test]
    async fn test_readiness_endpoint_not_ready() {
        let manager = HealthManager::new();
        let app = manager.router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_readiness_endpoint_ready() {
        let manager = HealthManager::new();
        manager.set_ready(true).await;
        let app = manager.router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_liveness_endpoint() {
        let manager = HealthManager::new();
        let app = manager.router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_json_endpoint() {
        let manager = HealthManager::new();
        manager.set_ready(true).await;
        let app = manager.router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_default_impl() {
        let _manager = HealthManager::default();
    }
}

/// Statuses an execution is in while it is still in flight. The same set as the
/// `idx_workflow_exec_active` partial index, which this count reads.
const ACTIVE_STATUSES: &str = "'pending','queued','running','retrying','waiting'";

/// How long one database may take to answer before the count is given up for
/// this scrape. A metrics endpoint that hangs is worse than one that says it
/// could not count.
const COUNT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Write `orcher_workflows_executing`, counted from the database.
///
/// This used to be an up/down gauge in process memory. It read zero after every
/// restart while workflows were still running, only one of the ways an
/// execution is created incremented it while every completion decremented it,
/// and the two sides used different labels — which panicked the completion RPC
/// wherever the Prometheus recorder was installed. The number lives in the
/// database, so it is read from there.
///
/// Every replica reports the same absolute count; aggregate with `max`, not
/// `sum`. `orcher_workflows_executing_count_ok` says whether this scrape's count
/// succeeded, so a failing count is visible rather than merely absent.
async fn write_workflows_executing(output: &mut String, pools: &[sqlx::PgPool]) {
    use std::collections::BTreeMap;
    use std::fmt::Write;

    if pools.is_empty() {
        return;
    }

    let sql = format!(
        "SELECT namespace, COALESCE(NULLIF(workflow_type, ''), 'unknown'), count(*) \
         FROM workflow_executions \
         WHERE status IN ({ACTIVE_STATUSES}) AND cancelled_at IS NULL \
         GROUP BY 1, 2"
    );

    let mut totals: BTreeMap<(String, String), i64> = BTreeMap::new();
    let mut ok = true;
    for pool in pools {
        let query = sqlx::query_as::<_, (String, String, i64)>(&sql).fetch_all(pool);
        match tokio::time::timeout(COUNT_TIMEOUT, query).await {
            Ok(Ok(rows)) => {
                for (namespace, workflow_type, n) in rows {
                    *totals.entry((namespace, workflow_type)).or_default() += n;
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "Could not count in-flight workflows for metrics");
                ok = false;
                break;
            }
            Err(_) => {
                tracing::warn!("Counting in-flight workflows for metrics timed out");
                ok = false;
                break;
            }
        }
    }

    writeln!(
        output,
        "# HELP orcher_workflows_executing_count_ok Whether in-flight workflows could be counted on this scrape"
    )
    .unwrap();
    writeln!(output, "# TYPE orcher_workflows_executing_count_ok gauge").unwrap();
    writeln!(
        output,
        "orcher_workflows_executing_count_ok {}",
        u8::from(ok)
    )
    .unwrap();

    if !ok {
        return;
    }

    writeln!(
        output,
        "# HELP orcher_workflows_executing Workflow executions in flight, counted from the database"
    )
    .unwrap();
    writeln!(output, "# TYPE orcher_workflows_executing gauge").unwrap();
    for ((namespace, workflow_type), n) in totals {
        writeln!(
            output,
            "orcher_workflows_executing{{namespace=\"{}\",workflow_type=\"{}\"}} {}",
            escape_label(&namespace),
            escape_label(&workflow_type),
            n
        )
        .unwrap();
    }
}

/// Write how long the oldest *ready* workflow and task have been waiting to be
/// picked up, per namespace, counted from the database.
///
/// This is the stall signal. "Work in flight and nothing completing" is not:
/// a workflow waiting hours on a timer or an external event is in flight and
/// healthy, and at low volume nothing completing for an hour is normal, so an
/// alert built on that fires on healthy systems and gets ignored. Ready work
/// that nobody picks up is never healthy, whatever the cause — no worker on the
/// queue, a worker with no capacity left, a namespace or release it cannot see.
///
/// A timing histogram cannot stand in for this either: it only records when
/// work *starts*, so work that never starts leaves it empty and an alert on it
/// stays silent in exactly the case it exists for.
///
/// Parked workflows are `waiting`, not ready, and are excluded; a retry is ready
/// only once its time has come. Series are written only for namespaces that have
/// ready work, so absence means nothing is waiting.
async fn write_oldest_ready(output: &mut String, pools: &[sqlx::PgPool]) {
    use std::collections::BTreeMap;
    use std::fmt::Write;

    if pools.is_empty() {
        return;
    }

    // Ready = unclaimed, not cancelled, and runnable now. `updated_at` is when
    // the row last changed state, which for an unclaimed row is when it became
    // ready; `submitted_at` would make a long-lived workflow look stalled every
    // time it was requeued.
    const WORKFLOWS: &str = "SELECT namespace, \
            EXTRACT(EPOCH FROM (NOW() - min(updated_at)))::float8 \
         FROM workflow_executions \
         WHERE claimed_at IS NULL AND cancelled_at IS NULL \
           AND (status IN ('pending', 'queued', 'running') \
                OR (status = 'retrying' AND (next_retry_at IS NULL OR next_retry_at <= NOW()))) \
         GROUP BY namespace";
    const TASKS: &str = "SELECT namespace, \
            EXTRACT(EPOCH FROM (NOW() - min(scheduled_at)))::float8 \
         FROM task_executions \
         WHERE status = 'scheduled' AND scheduled_at <= NOW() \
         GROUP BY namespace";

    for (metric, help, sql) in [
        (
            "orcher_workflows_oldest_ready_seconds",
            "Age of the oldest workflow execution ready to run and not picked up",
            WORKFLOWS,
        ),
        (
            "orcher_tasks_oldest_ready_seconds",
            "Age of the oldest task ready to run and not picked up",
            TASKS,
        ),
    ] {
        let mut oldest: BTreeMap<String, f64> = BTreeMap::new();
        let mut ok = true;
        for pool in pools {
            let query = sqlx::query_as::<_, (String, f64)>(sql).fetch_all(pool);
            match tokio::time::timeout(COUNT_TIMEOUT, query).await {
                Ok(Ok(rows)) => {
                    for (namespace, age) in rows {
                        let entry = oldest.entry(namespace).or_insert(age);
                        *entry = entry.max(age);
                    }
                }
                _ => {
                    tracing::warn!(metric, "Could not read ready-work age for metrics");
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            // Say nothing rather than something false. The executing count's
            // `_count_ok` gauge reports the database as unreachable.
            continue;
        }
        writeln!(output, "# HELP {metric} {help}").unwrap();
        writeln!(output, "# TYPE {metric} gauge").unwrap();
        for (namespace, age) in oldest {
            writeln!(
                output,
                "{metric}{{namespace=\"{}\"}} {:.3}",
                escape_label(&namespace),
                age.max(0.0)
            )
            .unwrap();
        }
    }
}

/// Escape a label value for the Prometheus text format. Namespace and workflow
/// type names come from callers, and an unescaped quote or newline in one would
/// make the whole scrape unparsable.
fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod executing_tests {
    use super::escape_label;

    #[test]
    fn label_values_are_escaped_for_the_text_format() {
        assert_eq!(escape_label(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escape_label("a\\b"), "a\\\\b");
        assert_eq!(escape_label("a\nb"), "a\\nb");
        assert_eq!(escape_label("plain"), "plain");
    }
}
