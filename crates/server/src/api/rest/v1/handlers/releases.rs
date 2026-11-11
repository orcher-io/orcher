//! Release Handlers
//!
//! Operator endpoints for workflow versioning. A worker declares the code
//! release it runs; an execution binds to the release that first claimed it and
//! never moves. These endpoints cover the three questions that leaves open:
//!
//! * **where does new work go** while two releases are deployed side by side
//! * **what is still running** on a release I want to retire
//! * **how do I unstick** an execution bound to a release nobody runs any more
//!
//! All three are operator actions rather than anything a workflow author calls,
//! which is why they live in the REST surface and not in the SDKs.

use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::info;

/// Which queue an operation applies to. The namespace defaults, since most
/// deployments only ever use one.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueQuery {
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

fn default_namespace() -> String {
    "default".to_string()
}

/// The release new executions on a queue bind to.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentReleaseResponse {
    pub task_queue: String,
    pub namespace: String,
    /// `null` means no policy: any release may take new work.
    pub current_version_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCurrentReleaseRequest {
    /// `null` or blank clears the policy and reopens the queue to any release.
    pub version_id: Option<String>,
}

/// A release with executions still waiting for it.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WaitingRelease {
    pub version_id: String,
    pub waiting: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainageResponse {
    pub task_queue: String,
    pub namespace: String,
    /// Empty means nothing is bound and waiting — every release on this queue
    /// has drained and old workers can be retired.
    pub releases: Vec<WaitingRelease>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RebindRequest {
    /// The release to move this execution onto. `null` unbinds it, so any
    /// release may pick it up.
    pub version_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebindResponse {
    pub execution_id: String,
    pub version_id: Option<String>,
    pub rebound: bool,
}

/// `GET /task-queues/:task_queue/current-release`
pub async fn get_current_release(
    State(state): State<AppState>,
    Path(task_queue): Path<String>,
    Query(query): Query<QueueQuery>,
) -> Result<Json<CurrentReleaseResponse>> {
    let current = state
        .execution_repository()
        .current_release(
            &task_queue,
            &query.namespace,
            orcher_core::grpc::tenant::current_organization(),
        )
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(CurrentReleaseResponse {
        task_queue,
        namespace: query.namespace,
        current_version_id: current,
    }))
}

/// `PUT /task-queues/:task_queue/current-release`
///
/// Point new executions at a release. Run this as the last step of a deploy:
/// from then on the outgoing release drains monotonically instead of taking new
/// work until its workers stop.
///
/// Executions already bound are untouched — this decides where new work goes,
/// never where running work continues.
pub async fn set_current_release(
    State(state): State<AppState>,
    Path(task_queue): Path<String>,
    Query(query): Query<QueueQuery>,
    Json(body): Json<SetCurrentReleaseRequest>,
) -> Result<Json<CurrentReleaseResponse>> {
    state
        .execution_repository()
        // Scoped to the caller. A namespace name is unique per organisation,
        // not per deployment, so an unscoped policy let one tenant's deploy
        // point another tenant's queue at a release that has no workers —
        // their new executions then bind to it and are never claimed.
        .set_current_release(
            &task_queue,
            &query.namespace,
            body.version_id.as_deref(),
            orcher_core::grpc::tenant::current_organization(),
        )
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    // Read it back rather than echoing the request: blank normalises to "no
    // policy", so what was stored is not always what was sent.
    let current = state
        .execution_repository()
        .current_release(
            &task_queue,
            &query.namespace,
            orcher_core::grpc::tenant::current_organization(),
        )
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    info!(
        task_queue = %task_queue,
        namespace = %query.namespace,
        version_id = ?current,
        "Current release set for task queue"
    );

    Ok(Json(CurrentReleaseResponse {
        task_queue,
        namespace: query.namespace,
        current_version_id: current,
    }))
}

/// `GET /task-queues/:task_queue/releases`
///
/// Which releases still have executions waiting, and how many. This is the
/// question to answer before retiring a release: an empty list means everything
/// bound to it has finished.
pub async fn list_waiting_releases(
    State(state): State<AppState>,
    Path(task_queue): Path<String>,
    Query(query): Query<QueueQuery>,
) -> Result<Json<DrainageResponse>> {
    let rows = state
        .execution_repository()
        .releases_with_waiting_work(
            &task_queue,
            &query.namespace,
            orcher_core::grpc::tenant::current_organization(),
        )
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(DrainageResponse {
        task_queue,
        namespace: query.namespace,
        releases: rows
            .into_iter()
            .map(|(version_id, waiting)| WaitingRelease {
                version_id,
                waiting,
            })
            .collect(),
    }))
}

/// `POST /executions/:execution_id/rebind`
///
/// Move a stalled execution onto a release that is actually running, or unbind
/// it so any release may take it.
///
/// This is the escape hatch for the one failure mode routing introduces: an
/// execution bound to a retired release simply waits. Starting a worker on that
/// release is the first answer; this is for when that build is gone.
///
/// Deliberately a manual action. Replay against different code is exactly what
/// binding prevents, and whether the new release is compatible with the journal
/// already written is something only the operator can judge.
pub async fn rebind_execution(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
    Json(body): Json<RebindRequest>,
) -> Result<Json<RebindResponse>> {
    let rebound = state
        .execution_repository()
        // Rebinding another organisation's execution moves running work onto
        // code the operator asking for it cannot see — or onto a release that
        // does not exist, which stalls it for good.
        .rebind_execution(
            &execution_id,
            body.version_id.as_deref(),
            orcher_core::grpc::tenant::current_organization(),
        )
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    if !rebound {
        return Err(ServerError::NotFound(format!(
            "No running execution '{execution_id}' to rebind (it may have already finished)"
        )));
    }

    info!(
        execution_id = %execution_id,
        version_id = ?body.version_id,
        "Execution rebound to a different release"
    );

    Ok(Json(RebindResponse {
        execution_id,
        version_id: body.version_id,
        rebound,
    }))
}
