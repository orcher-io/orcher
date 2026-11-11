//! Service-to-service authentication types.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Identity and capabilities of an authenticated worker service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceIdentity {
    /// Unique service identifier (`svc_<uuid>`).
    pub service_id: String,
    /// Human-readable service name.
    pub service_name: String,
    /// Namespace the service operates in.
    pub namespace: String,
    /// Task queues the service may poll; empty means all queues.
    pub allowed_task_queues: Vec<String>,
    /// Operations the service is permitted to perform.
    pub permissions: ServicePermissions,
    /// Arbitrary service metadata.
    pub metadata: HashMap<String, String>,
    /// When the service registered.
    pub registered_at: Timestamptz,
    /// When the service was last seen (heartbeat), if ever.
    pub last_seen_at: Option<Timestamptz>,
    /// Current lifecycle status.
    pub status: ServiceStatus,
    /// The organisation the credential belongs to, if it belongs to one.
    ///
    /// Set by a validator that resolves the key to a tenant. When present it
    /// is the authority on which organisation a request acts for: the auth
    /// layer stamps it onto the request and refuses a caller that claims a
    /// different one.
    #[serde(default)]
    pub organization_id: Option<Uuid>,
}

impl ServiceIdentity {
    /// Create a new active service identity with a generated `service_id`.
    pub fn new(
        service_name: String,
        namespace: String,
        allowed_task_queues: Vec<String>,
        permissions: ServicePermissions,
    ) -> Self {
        Self {
            service_id: format!("svc_{}", Uuid::new_v4()),
            service_name,
            namespace,
            allowed_task_queues,
            permissions,
            metadata: HashMap::new(),
            registered_at: Timestamptz::now(),
            last_seen_at: None,
            status: ServiceStatus::Active,
            organization_id: None,
        }
    }

    /// Whether the service may poll the given task queue (empty allowlist = all).
    pub fn can_access_task_queue(&self, task_queue: &str) -> bool {
        self.allowed_task_queues.is_empty()
            || self.allowed_task_queues.contains(&task_queue.to_string())
    }
}

/// The set of operations a service is allowed to perform.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServicePermissions {
    /// May poll for workflow steps to execute.
    pub can_poll_workflow_steps: bool,
    /// May poll for activity/tasks to execute.
    pub can_poll_activity_tasks: bool,
    /// May start new workflows.
    pub can_start_workflows: bool,
    /// May send events/signals to running workflows.
    pub can_send_events_to_workflows: bool,
    /// May query workflows.
    pub can_query_workflows: bool,
    /// May cancel workflows.
    pub can_cancel_workflows: bool,
    /// May act for whichever organisation the request names.
    ///
    /// This is the power a trusted edge needs: the gateway authenticates the
    /// user, checks their membership, and forwards the organisation it
    /// resolved. Nothing else should hold it, because a credential with it can
    /// act as every tenant on the deployment.
    ///
    /// It used to be implicit — any API key stored without an organisation
    /// could claim any organisation by sending the header — so a single key
    /// inserted without one became a key to every tenant, and nothing about
    /// the row said so. Being a named capability, it is now granted on purpose
    /// and visible where it is granted.
    ///
    /// `serde(default)` so permissions persisted before the field existed read
    /// back as `false`, which is the only safe reading.
    #[serde(default)]
    pub can_act_for_any_organization: bool,
}

impl ServicePermissions {
    /// Full permissions (a trusted client that can drive workflows).
    pub fn default() -> Self {
        Self {
            can_poll_workflow_steps: true,
            can_poll_activity_tasks: true,
            can_start_workflows: true,
            can_send_events_to_workflows: true,
            can_query_workflows: true,
            can_cancel_workflows: true,
            // Deliberately absent from "full" permissions: being able to drive
            // workflows is not the same as being able to act as any tenant.
            can_act_for_any_organization: false,
        }
    }

    /// Worker permissions: may poll/execute work but not control workflows.
    pub fn worker() -> Self {
        Self {
            can_poll_workflow_steps: true,
            can_poll_activity_tasks: true,
            can_start_workflows: false,
            can_send_events_to_workflows: false,
            can_query_workflows: false,
            can_cancel_workflows: false,
            can_act_for_any_organization: false,
        }
    }
}

/// Lifecycle status of a registered service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceStatus {
    /// Accepting and executing work.
    Active,
    /// Finishing in-flight work; not accepting new work.
    Draining,
    /// Temporarily not accepting work.
    Suspended,
    /// Permanently stopped.
    Terminated,
}

/// An operation a service may attempt, checked against its [`ServicePermissions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServiceOperation {
    /// Poll for workflow steps.
    PollWorkflowSteps,
    /// Poll for activity tasks.
    PollActivityTasks,
    /// Start a workflow.
    StartWorkflow,
    /// Send an event/signal to a workflow.
    SendEventToWorkflow,
    /// Query a workflow.
    QueryWorkflow,
    /// Cancel a workflow.
    CancelWorkflow,
    /// Report a completed workflow step.
    CompleteWorkflowStep,
    /// Report a completed activity task.
    CompleteActivityTask,
    /// Report a failed workflow step.
    FailWorkflowStep,
    /// Report a failed activity task.
    FailActivityTask,
    /// Apply a workflow update.
    UpdateWorkflow,
}

impl ServiceOperation {
    /// Whether the given permissions authorize this operation.
    pub fn check_permission(&self, permissions: &ServicePermissions) -> bool {
        match self {
            Self::PollWorkflowSteps => permissions.can_poll_workflow_steps,
            Self::PollActivityTasks => permissions.can_poll_activity_tasks,
            Self::StartWorkflow => permissions.can_start_workflows,
            Self::SendEventToWorkflow => permissions.can_send_events_to_workflows,
            Self::QueryWorkflow => permissions.can_query_workflows,
            Self::CancelWorkflow => permissions.can_cancel_workflows,
            Self::CompleteWorkflowStep => permissions.can_poll_workflow_steps,
            Self::CompleteActivityTask => permissions.can_poll_activity_tasks,
            Self::FailWorkflowStep => permissions.can_poll_workflow_steps,
            Self::FailActivityTask => permissions.can_poll_activity_tasks,
            Self::UpdateWorkflow => permissions.can_query_workflows,
        }
    }

    /// Stable string form used in logs and the audit trail.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PollWorkflowSteps => "poll_workflow_steps",
            Self::PollActivityTasks => "poll_activity_tasks",
            Self::StartWorkflow => "start_workflow",
            Self::SendEventToWorkflow => "send_event_workflow",
            Self::QueryWorkflow => "query_workflow",
            Self::CancelWorkflow => "cancel_workflow",
            Self::CompleteWorkflowStep => "complete_workflow_step",
            Self::CompleteActivityTask => "complete_activity_task",
            Self::FailWorkflowStep => "fail_workflow_step",
            Self::FailActivityTask => "fail_activity_task",
            Self::UpdateWorkflow => "update_workflow",
        }
    }
}
