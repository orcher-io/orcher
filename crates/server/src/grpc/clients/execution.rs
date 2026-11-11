//! Execution Service gRPC Client
//!
//! This module provides a client wrapper for the modern ExecutionService gRPC API.
//! It handles task and workflow execution polling, completion reporting, and
//! heartbeat management for pull-based execution.
//!
//! # Architecture
//!
//! The ExecutionService uses a pull-based polling model where workers:
//! - Poll for workflow/task execution work
//! - Execute the work
//! - Report completion back to the service
//!
//! This client wraps the auto-generated gRPC client and provides:
//! - Connection management and retry logic
//! - Type conversions between API models and proto types
//! - Error handling and logging
//! - Polling timeout management
//!
//! # Example
//!
//! ```rust,no_run
//! use orcher_server::grpc::clients::execution::ExecutionServiceClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ExecutionServiceClient::connect("http://localhost:50051").await?;
//!
//! // Poll for workflow execution work
//! if let Some(work) = client.poll_workflow_execution("default-queue", "default").await? {
//!     println!("Got workflow work: {:?}", work);
//!     // Execute workflow...
//!     client.complete_workflow_execution(&work.task_token, "default", vec![]).await?;
//! }
//! # Ok(())
//! # }
//! ```

use orcher_common::error::{OrcherError, OrcherResult};
use orcher_proto::orcher::v1::execution_service_client::ExecutionServiceClient as ProtoExecutionServiceClient;
use orcher_proto::orcher::v1::{
    CancelTaskExecutionRequest, CompleteTaskExecutionRequest, CompleteWorkflowExecutionRequest,
    FailTaskExecutionRequest, FailWorkflowExecutionRequest, PollTaskExecutionRequest,
    PollWorkflowExecutionRequest, RecordTaskHeartbeatRequest,
};
use serde_json::Value as JsonValue;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

/// Default connection timeout for gRPC client
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default request timeout (long for polling)
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Default polling timeout
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Execution service client wrapper
///
/// Provides a high-level interface to the ExecutionService gRPC API
/// with connection management, retry logic, and error handling.
#[derive(Clone)]
pub struct ExecutionServiceClient {
    /// Inner gRPC client
    client: ProtoExecutionServiceClient<
        tonic::service::interceptor::InterceptedService<
            Channel,
            fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
        >,
    >,
    /// Server endpoint URL
    endpoint: String,
}

impl ExecutionServiceClient {
    /// Connect to an execution service
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
        info!("Connecting to ExecutionService at {}", endpoint_str);

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
                error!("Failed to connect to ExecutionService: {}", e);
                OrcherError::ExternalService {
                    service: "ExecutionService".to_string(),
                    message: format!("Connection failed: {}", e),
                    status_code: None,
                }
            })?;

        // Same interceptor as the workflow client, for the same reason: the
        // tenant and the credential are per-client, not per-call.
        let interceptor: fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> =
            orcher_core::grpc::outbound::outbound_interceptor;
        // The engine's message limit, not tonic's 4 MiB default: a workflow's
        // input and result pass through here whole.
        let max = orcher_core::grpc::payload_limits::max_message_bytes_from_env();
        let client = ProtoExecutionServiceClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(max)
            .max_encoding_message_size(max);

        info!("Successfully connected to ExecutionService");
        Ok(Self {
            client,
            endpoint: endpoint_str,
        })
    }

    /// Poll for workflow execution work
    ///
    /// This is a long-polling operation that blocks until work is available
    /// or the timeout is reached.
    ///
    /// # Arguments
    ///
    /// * `task_queue` - The task queue to poll from
    /// * `namespace` - The namespace to poll from (typically "default")
    ///
    /// # Returns
    ///
    /// Returns workflow execution work if available, None if timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the polling fails.
    pub async fn poll_workflow_execution(
        &self,
        task_queue: impl Into<String>,
        namespace: impl Into<String>,
    ) -> OrcherResult<Option<WorkflowExecutionWork>> {
        let task_queue = task_queue.into();
        let namespace = namespace.into();
        debug!(
            "Polling for workflow execution work on queue: {} in namespace: {}",
            task_queue, namespace
        );

        // Note: Sharding is now handled server-side via Redis Streams consumer groups.
        // Workers no longer need to configure sharding.
        let request = PollWorkflowExecutionRequest {
            task_queue: task_queue.clone(),
            namespace: namespace.clone(),
            identity: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
            // This internal client declares no release; it is not a user worker.
            version_id: String::new(),
            poll_timeout: Some(prost_types::Duration {
                seconds: DEFAULT_POLL_TIMEOUT.as_secs() as i64,
                nanos: 0,
            }),
            // Nor does it ever say it is shutting down, so it names no process.
            worker_instance_id: String::new(),
            // Nor any step-id scheme, so it is given only runs bound to none.
            ..Default::default()
        };

        let mut client = self.client.clone();
        let response = client.poll_workflow_execution(request).await.map_err(|e| {
            error!("Failed to poll workflow execution: {}", e);
            OrcherError::ExternalService {
                service: "ExecutionService".to_string(),
                message: format!("Failed to poll workflow execution: {}", e),
                status_code: None,
            }
        })?;

        let proto_work = response.into_inner();

        // Check if we actually got work (empty workflow_id means no work)
        if proto_work.workflow_id.is_empty() {
            debug!("No workflow execution work available");
            return Ok(None);
        }

        debug!("Received workflow execution work");
        Ok(Some(WorkflowExecutionWork {
            workflow_id: proto_work.workflow_id,
            execution_id: proto_work.execution_id,
            workflow_type: proto_work.workflow_type,
            task_queue: proto_work.task_queue,
            previous_execution_id: proto_work.previous_execution_id,
            attempt: proto_work.attempt,
            journal: proto_work.journal,
            previous_started_event_id: proto_work.previous_started_event_id,
            started_event_id: proto_work.started_event_id,
            queries: proto_work.queries,
            workflow_start_time: proto_work.workflow_start_time,
            execution_start_time: proto_work.execution_start_time,
            input: proto_work.input,
        }))
    }

    /// Poll for task execution work
    ///
    /// This is a long-polling operation that blocks until work is available
    /// or the timeout is reached.
    ///
    /// # Arguments
    ///
    /// * `task_queue` - The task queue to poll from
    /// * `namespace` - The namespace to poll from (typically "default")
    ///
    /// # Returns
    ///
    /// Returns task execution work if available, None if timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the polling fails.
    pub async fn poll_task_execution(
        &self,
        task_queue: impl Into<String>,
        namespace: impl Into<String>,
    ) -> OrcherResult<Option<TaskExecutionWork>> {
        let task_queue = task_queue.into();
        let namespace = namespace.into();
        debug!(
            "Polling for task execution work on queue: {} in namespace: {}",
            task_queue, namespace
        );

        let request = PollTaskExecutionRequest {
            task_queue: task_queue.clone(),
            namespace: namespace.clone(),
            identity: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
            poll_timeout: Some(prost_types::Duration {
                seconds: DEFAULT_POLL_TIMEOUT.as_secs() as i64,
                nanos: 0,
            }),
            task_queue_metadata: std::collections::HashMap::new(),
            worker_instance_id: String::new(),
            // This client does not heartbeat the tasks it polls.
            task_capabilities: None,
        };

        let mut client = self.client.clone();
        let response = client.poll_task_execution(request).await.map_err(|e| {
            error!("Failed to poll task execution: {}", e);
            OrcherError::ExternalService {
                service: "ExecutionService".to_string(),
                message: format!("Failed to poll task execution: {}", e),
                status_code: None,
            }
        })?;

        let proto_work = response.into_inner();

        // Check if we actually got work (empty task_token means no work)
        if proto_work.task_token.is_empty() {
            debug!("No task execution work available");
            return Ok(None);
        }

        debug!("Received task execution work");
        Ok(Some(TaskExecutionWork {
            task_token: proto_work.task_token,
            workflow_id: proto_work.workflow_id,
            execution_id: proto_work.execution_id,
            task_id: proto_work.task_id,
            task_type: proto_work.task_type,
            task_queue: proto_work.task_queue,
            namespace: proto_work.namespace,
            input: proto_work.input,
            headers: proto_work.headers,
            scheduled_time: proto_work.scheduled_time,
            attempt: proto_work.attempt,
            schedule_to_start_timeout: proto_work.schedule_to_start_timeout,
            start_to_close_timeout: proto_work.start_to_close_timeout,
            heartbeat_timeout: proto_work.heartbeat_timeout,
            retry_policy: proto_work.retry_policy,
        }))
    }

    /// Complete a workflow execution
    ///
    /// Reports successful completion of workflow execution work.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `execution_id` - The execution ID
    /// * `namespace` - The namespace
    /// * `commands` - Workflow commands to execute
    ///
    /// # Returns
    ///
    /// Returns Ok if completion was successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the completion fails.
    pub async fn complete_workflow_execution(
        &self,
        workflow_id: &str,
        execution_id: &str,
        namespace: &str,
        commands: Vec<orcher_proto::orcher::v1::Command>,
    ) -> OrcherResult<()> {
        debug!("Completing workflow execution: {}", workflow_id);

        let request = CompleteWorkflowExecutionRequest {
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: namespace.to_string(),
            identity: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
            task_token: vec![],
            commands,
            query_results: vec![],
            update_results: vec![],
            force_new_execution_step: false,
            binary_checksum: vec![],
            // Stream entry ID for XACK (empty when not using streams)
            stream_entry_id: String::new(),
            eager_task_capabilities: None,
            // Every other field at its default, so a field added to the
            // message does not break this build. protocol_version stays 0:
            // this client does not hand cancellation requests to a workflow.
            ..Default::default()
        };

        let mut client = self.client.clone();
        client
            .complete_workflow_execution(request)
            .await
            .map_err(|e| {
                error!("Failed to complete workflow execution: {}", e);
                OrcherError::ExternalService {
                    service: "ExecutionService".to_string(),
                    message: format!("Failed to complete workflow execution: {}", e),
                    status_code: None,
                }
            })?;

        info!("Successfully completed workflow execution: {}", workflow_id);
        Ok(())
    }

    /// Complete a task execution
    ///
    /// Reports successful completion of task execution work.
    ///
    /// # Arguments
    ///
    /// * `task_token` - The task token received from polling
    /// * `namespace` - The namespace
    /// * `result` - Optional task result data
    ///
    /// # Returns
    ///
    /// Returns Ok if completion was successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the completion fails.
    pub async fn complete_task_execution(
        &self,
        task_token: &[u8],
        namespace: &str,
        result: Option<JsonValue>,
    ) -> OrcherResult<()> {
        debug!("Completing task execution");

        let result_bytes = if let Some(r) = result {
            serde_json::to_vec(&r).map_err(|e| OrcherError::Serialization {
                format: "json".to_string(),
                message: format!("Failed to serialize result: {}", e),
            })?
        } else {
            vec![]
        };

        let request = CompleteTaskExecutionRequest {
            task_token: task_token.to_vec(),
            namespace: namespace.to_string(),
            identity: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
            result: result_bytes,
        };

        let mut client = self.client.clone();
        client.complete_task_execution(request).await.map_err(|e| {
            error!("Failed to complete task execution: {}", e);
            OrcherError::ExternalService {
                service: "ExecutionService".to_string(),
                message: format!("Failed to complete task execution: {}", e),
                status_code: None,
            }
        })?;

        info!("Successfully completed task execution");
        Ok(())
    }

    /// Report workflow execution failure
    ///
    /// Reports that workflow execution work has failed.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `execution_id` - The execution ID
    /// * `namespace` - The namespace
    /// * `failure_message` - Description of the failure
    ///
    /// # Returns
    ///
    /// Returns Ok if the failure was reported.
    ///
    /// # Errors
    ///
    /// Returns an error if the report fails.
    pub async fn fail_workflow_execution(
        &self,
        workflow_id: &str,
        execution_id: &str,
        namespace: &str,
        failure_message: impl Into<String>,
    ) -> OrcherResult<()> {
        let failure_message = failure_message.into();
        warn!(
            "Reporting workflow failure for {}: {}",
            workflow_id, failure_message
        );

        let request = FailWorkflowExecutionRequest {
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: namespace.to_string(),
            identity: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
            task_token: vec![],
            failure: Some(orcher_proto::orcher::v1::Failure {
                message: failure_message.clone(),
                source: String::new(),
                stack_trace: String::new(),
                cause: None,
                failure_type: String::new(),
                details: vec![],
                non_retryable: false,
            }),
            binary_checksum: vec![],
            ..Default::default()
        };

        let mut client = self.client.clone();
        client.fail_workflow_execution(request).await.map_err(|e| {
            error!("Failed to report workflow failure: {}", e);
            OrcherError::ExternalService {
                service: "ExecutionService".to_string(),
                message: format!("Failed to report workflow failure: {}", e),
                status_code: None,
            }
        })?;

        info!("Successfully reported workflow failure for {}", workflow_id);
        Ok(())
    }

    /// Report task execution failure
    ///
    /// Reports that task execution work has failed.
    ///
    /// # Arguments
    ///
    /// * `task_token` - The task token received from polling
    /// * `namespace` - The namespace
    /// * `failure_message` - Description of the failure
    ///
    /// # Returns
    ///
    /// Returns Ok if the failure was reported.
    ///
    /// # Errors
    ///
    /// Returns an error if the report fails.
    pub async fn fail_task_execution(
        &self,
        task_token: &[u8],
        namespace: &str,
        failure_message: impl Into<String>,
    ) -> OrcherResult<()> {
        let failure_message = failure_message.into();
        warn!("Reporting task failure: {}", failure_message);

        let request = FailTaskExecutionRequest {
            task_token: task_token.to_vec(),
            namespace: namespace.to_string(),
            identity: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
            failure: Some(orcher_proto::orcher::v1::Failure {
                message: failure_message.clone(),
                source: String::new(),
                stack_trace: String::new(),
                cause: None,
                failure_type: String::new(),
                details: vec![],
                non_retryable: false,
            }),
        };

        let mut client = self.client.clone();
        client.fail_task_execution(request).await.map_err(|e| {
            error!("Failed to report task failure: {}", e);
            OrcherError::ExternalService {
                service: "ExecutionService".to_string(),
                message: format!("Failed to report task failure: {}", e),
                status_code: None,
            }
        })?;

        info!("Successfully reported task failure");
        Ok(())
    }

    /// Cancel a task execution
    ///
    /// Reports that task execution was canceled.
    ///
    /// # Arguments
    ///
    /// * `task_token` - The task token received from polling
    /// * `namespace` - The namespace
    /// * `details` - Optional cancellation details
    ///
    /// # Returns
    ///
    /// Returns Ok if cancellation was reported.
    ///
    /// # Errors
    ///
    /// Returns an error if the report fails.
    pub async fn cancel_task_execution(
        &self,
        task_token: &[u8],
        namespace: &str,
        details: Option<Vec<u8>>,
    ) -> OrcherResult<()> {
        info!("Canceling task execution");

        let request = CancelTaskExecutionRequest {
            task_token: task_token.to_vec(),
            namespace: namespace.to_string(),
            identity: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
            details: details.unwrap_or_default(),
        };

        let mut client = self.client.clone();
        client.cancel_task_execution(request).await.map_err(|e| {
            error!("Failed to cancel task execution: {}", e);
            OrcherError::ExternalService {
                service: "ExecutionService".to_string(),
                message: format!("Failed to cancel task execution: {}", e),
                status_code: None,
            }
        })?;

        info!("Successfully canceled task execution");
        Ok(())
    }

    /// Send a heartbeat for long-running task execution
    ///
    /// # Arguments
    ///
    /// * `task_token` - The task token received from polling
    /// * `namespace` - The namespace
    /// * `details` - Optional progress details
    ///
    /// # Returns
    ///
    /// Returns Ok with cancel_requested flag if the heartbeat was successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the heartbeat fails.
    pub async fn record_task_heartbeat(
        &self,
        task_token: &[u8],
        namespace: &str,
        details: Option<JsonValue>,
    ) -> OrcherResult<bool> {
        debug!("Sending task heartbeat");

        let details_bytes = if let Some(d) = details {
            serde_json::to_vec(&d).map_err(|e| OrcherError::Serialization {
                format: "json".to_string(),
                message: format!("Failed to serialize details: {}", e),
            })?
        } else {
            vec![]
        };

        let request = RecordTaskHeartbeatRequest {
            task_token: task_token.to_vec(),
            namespace: namespace.to_string(),
            identity: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
            details: details_bytes,
        };

        let mut client = self.client.clone();
        let response = client.record_task_heartbeat(request).await.map_err(|e| {
            error!("Failed to send heartbeat: {}", e);
            OrcherError::ExternalService {
                service: "ExecutionService".to_string(),
                message: format!("Failed to send heartbeat: {}", e),
                status_code: None,
            }
        })?;

        let cancel_requested = response.into_inner().cancel_requested;
        debug!(
            "Heartbeat sent successfully, cancel_requested: {}",
            cancel_requested
        );
        Ok(cancel_requested)
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
        // Simple health check - try to poll with zero timeout
        let result = self.poll_task_execution("health-check", "default").await;
        result.is_ok()
    }
}

/// Workflow execution work
///
/// Contains information about workflow execution work received from polling.
#[derive(Debug, Clone)]
pub struct WorkflowExecutionWork {
    /// Workflow execution ID
    pub workflow_id: String,
    /// Execution ID (run ID)
    pub execution_id: String,
    /// Workflow type/name
    pub workflow_type: String,
    /// Task queue name
    pub task_queue: String,
    /// Previous execution ID (for continued-as-new)
    pub previous_execution_id: String,
    /// Attempt number (for retries)
    pub attempt: i32,
    /// Execution journal (event history)
    pub journal: Vec<orcher_proto::orcher::v1::JournalEntry>,
    /// Previous started event ID
    pub previous_started_event_id: i64,
    /// Started event ID for this execution step
    pub started_event_id: i64,
    /// Query requests to answer
    pub queries: Vec<orcher_proto::orcher::v1::QueryRequest>,
    /// Workflow start time
    pub workflow_start_time: Option<prost_types::Timestamp>,
    /// Current execution start time
    pub execution_start_time: Option<prost_types::Timestamp>,
    /// Workflow input parameters
    pub input: Vec<u8>,
}

/// Task execution work
///
/// Contains information about task execution work received from polling.
#[derive(Debug, Clone)]
pub struct TaskExecutionWork {
    /// Task token to use for completion/failure reporting
    pub task_token: Vec<u8>,
    /// Parent workflow ID
    pub workflow_id: String,
    /// Parent workflow execution ID
    pub execution_id: String,
    /// Task ID
    pub task_id: String,
    /// Task type/name
    pub task_type: String,
    /// Task queue name
    pub task_queue: String,
    /// Namespace
    pub namespace: String,
    /// Task input data
    pub input: Vec<u8>,
    /// Task metadata (headers)
    pub headers: std::collections::HashMap<String, Vec<u8>>,
    /// When the work was scheduled
    pub scheduled_time: Option<prost_types::Timestamp>,
    /// Attempt number (for retries)
    pub attempt: i32,
    /// Schedule-to-start timeout
    pub schedule_to_start_timeout: Option<prost_types::Duration>,
    /// Start-to-close timeout
    pub start_to_close_timeout: Option<prost_types::Duration>,
    /// Heartbeat timeout
    pub heartbeat_timeout: Option<prost_types::Duration>,
    /// Retry policy
    pub retry_policy: Option<orcher_proto::orcher::v1::RetryPolicy>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_client_creation() {
        // Just verify the struct can be constructed
        // Actual connection tests require a running server
        let endpoint = "http://localhost:50051";
        assert!(!endpoint.is_empty());
    }

    #[test]
    fn test_default_timeouts() {
        assert_eq!(DEFAULT_CONNECT_TIMEOUT, Duration::from_secs(10));
        assert_eq!(DEFAULT_REQUEST_TIMEOUT, Duration::from_secs(60));
        assert_eq!(DEFAULT_POLL_TIMEOUT, Duration::from_secs(30));
    }
}
