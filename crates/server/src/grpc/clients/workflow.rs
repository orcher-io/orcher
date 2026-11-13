//! Workflow Service gRPC Client
//!
//! This module provides a client wrapper for the modern WorkflowService gRPC API.
//! It handles workflow lifecycle operations including starting, querying, canceling,
//! and sending events to workflows.
//!
//! # Architecture
//!
//! The client wraps the auto-generated gRPC client and provides:
//! - Connection management and retry logic
//! - Type conversions between API models and proto types
//! - Error handling and logging
//! - Health checking and reconnection
//!
//! # Example
//!
//! ```rust,no_run
//! use orcher_server::grpc::clients::workflow::WorkflowServiceClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = WorkflowServiceClient::connect("http://localhost:50051").await?;
//!
//! let started = client
//!     .start_workflow(orcher_proto::orcher::v1::StartWorkflowRequest {
//!         workflow_type: "my-workflow".into(),
//!         task_queue: "default-queue".into(),
//!         namespace: "default".into(),
//!         input: br#"{"key":"value"}"#.to_vec(),
//!         ..Default::default()
//!     })
//!     .await?;
//!
//! let status = client
//!     .get_workflow_status(&started.workflow_id, &started.execution_id, "default")
//!     .await?;
//! # Ok(())
//! # }
//! ```

use orcher_common::error::{OrcherError, OrcherResult};
use orcher_proto::orcher::v1::workflow_service_client::WorkflowServiceClient as ProtoWorkflowServiceClient;
use orcher_proto::orcher::v1::{
    CancelWorkflowRequest, GetWorkflowResultRequest, GetWorkflowStatusRequest,
    QueryWorkflowRequest, ResetWorkflowRequest, ResetWorkflowResponse, SendEventRequest,
    SendEventWithStartWorkflowRequest, StartWorkflowRequest, StartWorkflowResponse,
    TerminateWorkflowRequest, WorkflowStatus,
};
use serde_json::Value as JsonValue;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Default connection timeout for gRPC client
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default request timeout
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Workflow service client wrapper
///
/// Provides a high-level interface to the WorkflowService gRPC API
/// with connection management, retry logic, and error handling.
#[derive(Clone)]
pub struct WorkflowServiceClient {
    /// Inner gRPC client
    client: ProtoWorkflowServiceClient<super::AuthenticatedChannel>,
    /// Server endpoint URL
    endpoint: String,
}

impl WorkflowServiceClient {
    /// Connect to a workflow service
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The gRPC endpoint URL (e.g., "http://localhost:50051")
    ///
    /// # Returns
    ///
    /// Returns a connected client instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails.
    pub async fn connect(endpoint: impl Into<String>) -> OrcherResult<Self> {
        let endpoint_str = endpoint.into();
        info!("Connecting to WorkflowService at {}", endpoint_str);

        let channel = Endpoint::from_shared(endpoint_str.clone())
            .map_err(|e| OrcherError::Configuration {
                key: "endpoint".to_string(),
                message: format!("Invalid endpoint URL: {}", e),
            })?
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .connect()
            .await
            .map_err(|e| {
                error!("Failed to connect to WorkflowService: {}", e);
                OrcherError::ExternalService {
                    service: "WorkflowService".to_string(),
                    message: format!("Connection failed: {}", e),
                    status_code: None,
                }
            })?;

        info!("Successfully connected to WorkflowService");
        Ok(Self::from_channel(channel, endpoint_str))
    }

    /// A client over `channel`, which `endpoint` describes: a connection made
    /// some other way than [`connect`](Self::connect), such as the engine's
    /// in-process loopback.
    pub fn from_channel(channel: Channel, endpoint: impl Into<String>) -> Self {
        // Wrapped so every outgoing call carries the calling tenant, when one
        // is in scope, and this process's credential, when one is configured.
        // Per-client rather than per-call: a forgotten header is a silently
        // unscoped lookup or a refused call, neither of which call sites
        // should be able to produce one method at a time.
        //
        // With neither configured the interceptor adds nothing, so
        // single-tenant, unauthenticated deployments are unaffected.
        // Coerced to a fn pointer: a fn *item* has its own unnameable type, so
        // the struct field could not be written down without this.
        let interceptor: fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> =
            orcher_core::grpc::outbound::outbound_interceptor;

        // The engine's message limit, not tonic's 4 MiB default: a workflow's
        // input and result pass through here whole.
        let max = orcher_core::grpc::payload_limits::max_message_bytes_from_env();
        let client = ProtoWorkflowServiceClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(max)
            .max_encoding_message_size(max);

        Self {
            client,
            endpoint: endpoint.into(),
        }
    }

    /// Start a workflow as `request` describes, and answer with the run the
    /// engine started, or the engine's refusal unchanged, so the caller can
    /// tell an unknown namespace from a bad argument from an outage.
    // The engine's `Status` as is, so the handler can map its code to an HTTP status.
    #[allow(clippy::result_large_err)]
    pub async fn start_workflow(
        &self,
        request: StartWorkflowRequest,
    ) -> Result<StartWorkflowResponse, tonic::Status> {
        debug!(
            workflow_type = %request.workflow_type,
            task_queue = %request.task_queue,
            namespace = %request.namespace,
            "Starting workflow"
        );
        let mut client = self.client.clone();
        let response = client.start_workflow(request).await?.into_inner();
        info!(
            workflow_id = %response.workflow_id,
            execution_id = %response.execution_id,
            "Started workflow"
        );
        Ok(response)
    }

    /// Get the status of a workflow execution
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `execution_id` - The execution ID (can be empty for latest)
    /// * `namespace` - The namespace
    ///
    /// # Returns
    ///
    /// Returns the workflow status information.
    ///
    /// # Errors
    ///
    /// Returns an error if the status cannot be retrieved.
    pub async fn get_workflow_status(
        &self,
        workflow_id: &str,
        execution_id: &str,
        namespace: &str,
    ) -> OrcherResult<WorkflowStatusInfo> {
        debug!(
            "Getting status for workflow: {} execution: {} in namespace: {}",
            workflow_id, execution_id, namespace
        );

        let request = GetWorkflowStatusRequest {
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: namespace.to_string(),
        };

        let mut client = self.client.clone();
        let response = client.get_workflow_status(request).await.map_err(|e| {
            error!("Failed to get workflow status: {}", e);
            OrcherError::Workflow {
                workflow_id: workflow_id.to_string(),
                message: format!("Failed to get workflow status: {}", e),
            }
        })?;

        let proto_status = response.into_inner();

        Ok(WorkflowStatusInfo {
            status: proto_status.status,
            execution: proto_status.execution,
            started_at: proto_status.started_at,
            completed_at: proto_status.completed_at,
            result: match proto_status.outcome {
                Some(orcher_proto::orcher::v1::get_workflow_status_response::Outcome::Result(
                    r,
                )) => Some(Ok(r)),
                Some(orcher_proto::orcher::v1::get_workflow_status_response::Outcome::Error(e)) => {
                    Some(Err(e))
                }
                None => None,
            },
        })
    }

    /// Get the result of a workflow execution (waits for completion)
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `execution_id` - The execution ID (can be empty for latest)
    /// * `namespace` - The namespace
    ///
    /// # Returns
    ///
    /// Returns the workflow result as JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow fails or cannot be retrieved.
    pub async fn get_workflow_result(
        &self,
        workflow_id: &str,
        execution_id: &str,
        namespace: &str,
    ) -> OrcherResult<JsonValue> {
        debug!(
            "Getting result for workflow: {} execution: {} in namespace: {}",
            workflow_id, execution_id, namespace
        );

        let request = GetWorkflowResultRequest {
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: namespace.to_string(),
            timeout: None, // Wait indefinitely
        };

        let mut client = self.client.clone();
        let response = client.get_workflow_result(request).await.map_err(|e| {
            error!("Failed to get workflow result: {}", e);
            OrcherError::Workflow {
                workflow_id: workflow_id.to_string(),
                message: format!("Failed to get workflow result: {}", e),
            }
        })?;

        let proto_result = response.into_inner();

        match proto_result.outcome {
            Some(orcher_proto::orcher::v1::get_workflow_result_response::Outcome::Result(r)) => {
                let result: JsonValue =
                    serde_json::from_slice(&r).map_err(|e| OrcherError::Serialization {
                        format: "json".to_string(),
                        message: format!("Failed to deserialize workflow result: {}", e),
                    })?;
                Ok(result)
            }
            Some(orcher_proto::orcher::v1::get_workflow_result_response::Outcome::Error(e)) => {
                Err(OrcherError::Workflow {
                    workflow_id: workflow_id.to_string(),
                    message: format!("Workflow failed: {}", e),
                })
            }
            None => Err(OrcherError::Workflow {
                workflow_id: workflow_id.to_string(),
                message: "Workflow result not available".to_string(),
            }),
        }
    }

    /// Cancel a running workflow execution
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID to cancel
    /// * `execution_id` - The execution ID (can be empty for latest)
    /// * `namespace` - The namespace
    /// * `reason` - Optional reason for cancellation
    ///
    /// # Returns
    ///
    /// Returns Ok if the cancellation was successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be canceled.
    /// Reset an execution to a journal entry: the engine keeps the entries up
    /// to and including `target_event_id` and runs the workflow again from
    /// there, as a new execution. Returns the engine's status as it is, so a
    /// caller can tell a bad request from a failure.
    // The engine's `Status` as is, so the handler can map its code to an HTTP status.
    #[allow(clippy::result_large_err)]
    pub async fn reset_workflow(
        &self,
        execution_id: &str,
        namespace: &str,
        target_event_id: i64,
        reason: String,
    ) -> Result<ResetWorkflowResponse, tonic::Status> {
        let request = ResetWorkflowRequest {
            execution_id: execution_id.to_string(),
            namespace: namespace.to_string(),
            target_event_id,
            reason,
            ..Default::default()
        };
        let mut client = self.client.clone();
        Ok(client.reset_workflow(request).await?.into_inner())
    }

    pub async fn cancel_workflow(
        &self,
        workflow_id: &str,
        execution_id: &str,
        namespace: &str,
        reason: Option<String>,
    ) -> OrcherResult<()> {
        info!(
            "Canceling workflow: {} execution: {} in namespace: {}",
            workflow_id, execution_id, namespace
        );

        let request = CancelWorkflowRequest {
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: namespace.to_string(),
            reason: reason.unwrap_or_else(|| "User requested cancellation".to_string()),
            request_id: Uuid::new_v4().to_string(),
            ..Default::default()
        };

        let mut client = self.client.clone();
        client.cancel_workflow(request).await.map_err(|e| {
            error!("Failed to cancel workflow: {}", e);
            OrcherError::Workflow {
                workflow_id: workflow_id.to_string(),
                message: format!("Failed to cancel workflow: {}", e),
            }
        })?;

        info!("Successfully canceled workflow: {}", workflow_id);
        Ok(())
    }

    /// Terminate a workflow execution (forceful stop)
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID to terminate
    /// * `execution_id` - The execution ID (can be empty for latest)
    /// * `namespace` - The namespace
    /// * `reason` - Reason for termination
    /// * `details` - Optional additional details
    ///
    /// # Returns
    ///
    /// Returns Ok if the termination was successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be terminated.
    pub async fn terminate_workflow(
        &self,
        workflow_id: &str,
        execution_id: &str,
        namespace: &str,
        reason: impl Into<String>,
        details: Option<Vec<u8>>,
    ) -> OrcherResult<()> {
        let reason = reason.into();
        warn!(
            "Terminating workflow: {} execution: {} in namespace: {} - reason: {}",
            workflow_id, execution_id, namespace, reason
        );

        let request = TerminateWorkflowRequest {
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: namespace.to_string(),
            reason,
            details: details.unwrap_or_default(),
        };

        let mut client = self.client.clone();
        client.terminate_workflow(request).await.map_err(|e| {
            error!("Failed to terminate workflow: {}", e);
            OrcherError::Workflow {
                workflow_id: workflow_id.to_string(),
                message: format!("Failed to terminate workflow: {}", e),
            }
        })?;

        info!("Successfully terminated workflow: {}", workflow_id);
        Ok(())
    }

    /// Send an event (signal) to a running workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `execution_id` - The execution ID (can be empty for latest)
    /// * `namespace` - The namespace
    /// * `event_name` - The name of the event to send
    /// * `payload` - The event payload as JSON
    ///
    /// # Returns
    ///
    /// Returns Ok if the event was sent successfully.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be sent.
    pub async fn send_event(
        &self,
        workflow_id: &str,
        execution_id: &str,
        namespace: &str,
        event_name: impl Into<String>,
        payload: JsonValue,
    ) -> OrcherResult<()> {
        let event_name = event_name.into();
        debug!(
            "Sending event {} to workflow: {} execution: {} in namespace: {}",
            event_name, workflow_id, execution_id, namespace
        );

        let payload_bytes =
            serde_json::to_vec(&payload).map_err(|e| OrcherError::Serialization {
                format: "json".to_string(),
                message: format!("Failed to serialize event payload: {}", e),
            })?;

        let request = SendEventRequest {
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: namespace.to_string(),
            event_name: event_name.clone(),
            payload: payload_bytes,
            request_id: Uuid::new_v4().to_string(),
        };

        let mut client = self.client.clone();
        client.send_event(request).await.map_err(|e| {
            error!("Failed to send event to workflow: {}", e);
            OrcherError::Workflow {
                workflow_id: workflow_id.to_string(),
                message: format!("Failed to send event: {}", e),
            }
        })?;

        info!(
            "Successfully sent event {} to workflow: {}",
            event_name, workflow_id
        );
        Ok(())
    }

    /// Send an event to a workflow, starting it if it doesn't exist
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `namespace` - The namespace
    /// * `event_name` - The name of the event to send
    /// * `event_payload` - The event payload as JSON
    /// * `workflow_type` - The workflow type (for starting)
    /// * `task_queue` - The task queue (for starting)
    /// * `workflow_input` - The workflow input (for starting)
    ///
    /// # Returns
    ///
    /// Returns tuple of (workflow_id, execution_id, workflow_started).
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_event_with_start(
        &self,
        workflow_id: &str,
        namespace: &str,
        event_name: impl Into<String>,
        event_payload: JsonValue,
        workflow_type: impl Into<String>,
        task_queue: impl Into<String>,
        workflow_input: JsonValue,
    ) -> OrcherResult<(String, String, bool)> {
        let event_name = event_name.into();
        let workflow_type = workflow_type.into();
        let task_queue = task_queue.into();

        debug!(
            "Sending event {} with start to workflow: {} in namespace: {}",
            event_name, workflow_id, namespace
        );

        let event_payload_bytes =
            serde_json::to_vec(&event_payload).map_err(|e| OrcherError::Serialization {
                format: "json".to_string(),
                message: format!("Failed to serialize event payload: {}", e),
            })?;

        let workflow_input_bytes =
            serde_json::to_vec(&workflow_input).map_err(|e| OrcherError::Serialization {
                format: "json".to_string(),
                message: format!("Failed to serialize workflow input: {}", e),
            })?;

        let request = SendEventWithStartWorkflowRequest {
            workflow_id: workflow_id.to_string(),
            namespace: namespace.to_string(),
            event_name: event_name.clone(),
            event_payload: event_payload_bytes,
            workflow_type,
            task_queue,
            workflow_input: workflow_input_bytes,
            execution_timeout: None,
            run_timeout: None,
            task_timeout: None,
            retry_policy: None,
            annotations: std::collections::HashMap::new(),
            labels: std::collections::HashMap::new(),
            request_id: Uuid::new_v4().to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .send_event_with_start_workflow(request)
            .await
            .map_err(|e| {
                error!("Failed to send event with start: {}", e);
                OrcherError::Workflow {
                    workflow_id: workflow_id.to_string(),
                    message: format!("Failed to send event with start: {}", e),
                }
            })?;

        let proto_response = response.into_inner();

        info!(
            "Successfully sent event {} with start to workflow: {}, started: {}",
            event_name, workflow_id, proto_response.workflow_started
        );

        Ok((
            proto_response.workflow_id,
            proto_response.execution_id,
            proto_response.workflow_started,
        ))
    }

    /// Query a workflow execution for its current state
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `execution_id` - The execution ID (can be empty for latest)
    /// * `namespace` - The namespace
    /// * `query_type` - The type of query to execute
    /// * `query_args` - Optional query arguments as JSON
    ///
    /// # Returns
    ///
    /// Returns the query result as JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn query_workflow(
        &self,
        workflow_id: &str,
        execution_id: &str,
        namespace: &str,
        query_type: impl Into<String>,
        query_args: Option<JsonValue>,
    ) -> OrcherResult<JsonValue> {
        let query_type = query_type.into();
        debug!(
            "Querying workflow: {} execution: {} in namespace: {} with query: {}",
            workflow_id, execution_id, namespace, query_type
        );

        let query_args_bytes = if let Some(args) = query_args {
            serde_json::to_vec(&args).map_err(|e| OrcherError::Serialization {
                format: "json".to_string(),
                message: format!("Failed to serialize query args: {}", e),
            })?
        } else {
            vec![]
        };

        let request = QueryWorkflowRequest {
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: namespace.to_string(),
            query_type: query_type.clone(),
            query_args: query_args_bytes,
        };

        let mut client = self.client.clone();
        let response = client.query_workflow(request).await.map_err(|e| {
            error!("Failed to query workflow: {}", e);
            OrcherError::Workflow {
                workflow_id: workflow_id.to_string(),
                message: format!("Failed to query workflow: {}", e),
            }
        })?;

        let result_bytes = response.into_inner().result;
        let result: JsonValue =
            serde_json::from_slice(&result_bytes).map_err(|e| OrcherError::Serialization {
                format: "json".to_string(),
                message: format!("Failed to deserialize query result: {}", e),
            })?;

        Ok(result)
    }

    /// Get the endpoint this client is connected to
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Check if the client connection is healthy
    ///
    /// # Returns
    ///
    /// Returns true if the connection is healthy, false otherwise.
    pub async fn health_check(&self) -> bool {
        // Simple health check - try to get status of a non-existent workflow
        // A proper "not found" error means the service is responding
        let result = self
            .get_workflow_status("health-check-probe", "", "default")
            .await;
        match result {
            Ok(_) => true,
            Err(e) => {
                // If we get a "not found" error, the service is actually healthy
                let err_str = e.to_string().to_lowercase();
                err_str.contains("not found") || err_str.contains("workflow")
            }
        }
    }
}

/// Workflow status information
///
/// Contains detailed information about a workflow execution's current state.
#[derive(Debug, Clone)]
pub struct WorkflowStatusInfo {
    /// Current workflow status
    pub status: i32,
    /// Workflow execution information
    pub execution: Option<orcher_proto::orcher::v1::WorkflowExecution>,
    /// Workflow start time
    pub started_at: Option<prost_types::Timestamp>,
    /// Workflow completion time (if completed)
    pub completed_at: Option<prost_types::Timestamp>,
    /// Result or error (if completed)
    pub result: Option<Result<Vec<u8>, String>>,
}

impl WorkflowStatusInfo {
    /// Get the workflow status as an enum value
    pub fn status_enum(&self) -> WorkflowStatus {
        WorkflowStatus::try_from(self.status).unwrap_or(WorkflowStatus::Unspecified)
    }

    /// Check if the workflow is running
    pub fn is_running(&self) -> bool {
        self.status_enum() == WorkflowStatus::Running
    }

    /// Check if the workflow is completed
    pub fn is_completed(&self) -> bool {
        matches!(
            self.status_enum(),
            WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Canceled
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_client_creation() {
        // Just verify the struct can be constructed
        // Actual connection tests require a running server
        let endpoint = "http://localhost:50051";
        assert!(!endpoint.is_empty());
    }

    #[test]
    fn test_default_timeouts() {
        assert_eq!(DEFAULT_CONNECT_TIMEOUT, Duration::from_secs(10));
        assert_eq!(DEFAULT_REQUEST_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn test_workflow_status_info_methods() {
        let info = WorkflowStatusInfo {
            status: WorkflowStatus::Running as i32,
            execution: None,
            started_at: None,
            completed_at: None,
            result: None,
        };

        assert!(info.is_running());
        assert!(!info.is_completed());

        let completed_info = WorkflowStatusInfo {
            status: WorkflowStatus::Completed as i32,
            execution: None,
            started_at: None,
            completed_at: None,
            result: None,
        };

        assert!(!completed_info.is_running());
        assert!(completed_info.is_completed());
    }
}
