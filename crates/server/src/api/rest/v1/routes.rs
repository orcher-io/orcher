//! REST API v1 Routes
//!
//! This module defines all route configurations for the v1 REST API.
//! Routes map HTTP methods and paths to their corresponding handlers.

use crate::api::rest::v1::handlers;
use crate::middleware::auth;
use crate::state::AppState;
use axum::{
    middleware,
    routing::{delete, get, post, put},
    Router,
};

/// Configure all v1 API routes
///
/// This function sets up all routes for the v1 REST API, including:
/// - Task management routes
/// - Workflow management routes
/// - Schedule management routes
/// - User management routes
/// - Authentication routes
/// - System information routes
/// - Health check routes
/// - Metrics routes
/// - File management routes
/// - Administrative routes
///
/// # Example Route Structure
///
/// ```text
/// /api/v1/tasks          POST   - Create task
/// /api/v1/tasks/:id      GET    - Get task status
/// /api/v1/tasks/:id      DELETE - Cancel task
/// /api/v1/tasks          GET    - List tasks
///
/// /api/v1/workflows      POST   - Create workflow
/// /api/v1/workflows/:id  GET    - Get workflow
/// /api/v1/workflows/:id  PUT    - Update workflow
/// /api/v1/workflows/:id  DELETE - Delete workflow
/// /api/v1/workflows      GET    - List workflows
///
/// /api/v1/health/live    GET    - Liveness probe
/// /api/v1/health/ready   GET    - Readiness probe
/// ```
///
/// # Returns
///
/// Returns a configured router ready to handle requests.
pub fn configure_routes() -> Router<AppState> {
    // Public routes (no authentication required)
    let public_routes = Router::new()
        // Health check routes (for k8s/monitoring)
        .route("/health/live", get(handlers::health::liveness))
        .route("/health/ready", get(handlers::health::readiness))
        // Version information routes
        .merge(version_routes())
        // Add system routes (public)
        .merge(system_routes());

    // Protected core routes (authentication required)
    // Account routes (auth, users, orgs, API keys, audit) are added by
    // the cloud gateway via Server::merge_routes() from orcher-account.
    let protected_routes = Router::new()
        .merge(registry_routes())
        .merge(workflow_routes())
        .merge(schedule_routes())
        .merge(namespace_routes())
        .merge(worker_routes())
        .merge(release_routes())
        .merge(batch_operation_routes())
        .merge(api_key_routes())
        .layer(middleware::from_fn(auth::require_authentication));

    // Combine public and protected routes
    Router::new().merge(public_routes).merge(protected_routes)
}

/// Version information routes
pub fn version_routes() -> Router<AppState> {
    Router::new()
        .route("/version", get(handlers::version::get_version_info))
        .route(
            "/version/:version",
            get(handlers::version::get_specific_version_info),
        )
        .route(
            "/version/migration/:from/:to",
            get(handlers::version::get_migration_guide),
        )
        .route(
            "/version/compatibility/:client/:server",
            get(handlers::version::check_compatibility),
        )
}

/// Workflow execution routes
pub fn workflow_routes() -> Router<AppState> {
    Router::new()
        // Workflow execution
        .route(
            "/workflows/execute",
            post(handlers::workflows::execute_workflow),
        )
        // Execution status and results
        .route(
            "/executions/:id/status",
            get(handlers::workflows::get_execution_status),
        )
        .route(
            "/executions/:id/result",
            get(handlers::workflows::get_execution_result),
        )
        // Execution control
        .route(
            "/executions/:id/cancel",
            post(handlers::workflows::cancel_execution),
        )
        // Execution listing, logs, and journal
        .route("/executions", get(handlers::workflows::list_executions))
        .route(
            "/executions/:id/logs",
            get(handlers::workflows::get_execution_logs),
        )
        // Execution journal (event-sourced history)
        .route(
            "/executions/:id/journal",
            get(handlers::workflows::get_execution_journal),
        )
        // Real-time log streaming via Server-Sent Events
        .route(
            "/executions/:id/logs/stream",
            get(handlers::workflows::stream_execution_logs),
        )
}

/// Schedule management routes
pub fn schedule_routes() -> Router<AppState> {
    Router::new()
        .route("/schedules", post(handlers::schedules::create_schedule))
        .route("/schedules/:id", get(handlers::schedules::get_schedule))
        .route("/schedules/:id", put(handlers::schedules::update_schedule))
        .route(
            "/schedules/:id",
            delete(handlers::schedules::delete_schedule),
        )
        .route("/schedules", get(handlers::schedules::list_schedules))
        .route(
            "/schedules/:id/pause",
            post(handlers::schedules::pause_schedule),
        )
        .route(
            "/schedules/:id/resume",
            post(handlers::schedules::resume_schedule),
        )
        .route(
            "/schedules/:id/executions",
            get(handlers::schedules::list_schedule_executions),
        )
}

/// System information routes
pub fn system_routes() -> Router<AppState> {
    Router::new()
        .route("/system/info", get(handlers::system::get_system_info))
        .route("/system/status", get(handlers::system::get_system_status))
        .route("/system/version", get(handlers::system::get_version))
        .route("/system/config", get(handlers::system::get_configuration))
}

/// Health check routes
pub fn health_routes() -> Router<AppState> {
    Router::new()
        .route("/health/live", get(handlers::health::liveness))
        .route("/health/ready", get(handlers::health::readiness))
}

/// Registry management routes (protected - require authentication)
pub fn registry_routes() -> Router<AppState> {
    Router::new()
        // Workflow registration and management
        .route(
            "/registry/workflows",
            post(handlers::registry::register_workflow),
        )
        .route(
            "/registry/workflows/wasm",
            post(handlers::registry::upload_wasm),
        )
        .route(
            "/registry/workflows",
            get(handlers::registry::list_workflows),
        )
        .route(
            "/registry/workflows/:id",
            get(handlers::registry::get_workflow),
        )
        .route(
            "/registry/workflows/:id",
            put(handlers::registry::update_workflow),
        )
        .route(
            "/registry/workflows/:id",
            delete(handlers::registry::delete_workflow),
        )
        // Deployment operations
        .route(
            "/registry/workflows/:id/deploy",
            post(handlers::registry::deploy_workflow),
        )
        // Health check for registry
        .route("/registry/health", get(handlers::registry::health_check))
}

/// Worker management routes
pub fn worker_routes() -> Router<AppState> {
    Router::new().route("/workers", get(handlers::workers::list_workers))
}

/// Release management routes.
///
/// Operator surface for workflow versioning: steer where new work goes, see
/// what is still draining, and unstick an execution whose release is gone.
pub fn release_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/task-queues/:task_queue/current-release",
            get(handlers::releases::get_current_release),
        )
        .route(
            "/task-queues/:task_queue/current-release",
            put(handlers::releases::set_current_release),
        )
        .route(
            "/task-queues/:task_queue/releases",
            get(handlers::releases::list_waiting_releases),
        )
        .route(
            "/executions/:execution_id/rebind",
            post(handlers::releases::rebind_execution),
        )
}

/// Batch operation management routes
pub fn batch_operation_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/batch-operations",
            post(handlers::batch_operations::create_batch_operation),
        )
        .route(
            "/batch-operations",
            get(handlers::batch_operations::list_batch_operations),
        )
        .route(
            "/batch-operations/:id",
            get(handlers::batch_operations::get_batch_operation),
        )
        .route(
            "/batch-operations/:id/terminate",
            post(handlers::batch_operations::terminate_batch_operation),
        )
}

/// Namespace management routes
pub fn namespace_routes() -> Router<AppState> {
    Router::new()
        .route("/namespaces", get(handlers::namespaces::list_namespaces))
        .route("/namespaces", post(handlers::namespaces::create_namespace))
        .route(
            "/namespaces/:name",
            get(handlers::namespaces::get_namespace),
        )
        .route(
            "/namespaces/:name",
            put(handlers::namespaces::update_namespace),
        )
        .route(
            "/namespaces/:name",
            delete(handlers::namespaces::delete_namespace),
        )
        .route(
            "/namespaces/:name/deprecate",
            post(handlers::namespaces::deprecate_namespace),
        )
}

/// The engine's own API keys
pub fn api_key_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api-keys",
            get(handlers::api_keys::list_api_keys).post(handlers::api_keys::create_api_key),
        )
        .route(
            "/api-keys/:id",
            get(handlers::api_keys::get_api_key).delete(handlers::api_keys::revoke_api_key),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_configure_routes() {
        let router = configure_routes();
        // Router is created successfully
        let _ = router;
    }

    #[test]
    fn test_workflow_routes() {
        let router = workflow_routes();
        let _ = router;
    }

    #[test]
    fn test_health_routes() {
        let router = health_routes();
        let _ = router;
    }
}
