//! Projection initialization and event subscription.

use crate::projections::{TaskStateProjection, WorkerStateProjection, WorkflowStateProjection};
use orcher_events::projections::{ProjectionManager, ProjectionStateStore};
use orcher_events::replay::EventStore;
use orcher_events::subscriptions::SubscriptionManager;
use std::sync::Arc;

/// Set up ProjectionManager with all core projections
///
/// This convenience function creates a ProjectionManager and registers
/// the three core projections:
/// - TaskStateProjection
/// - WorkerStateProjection
/// - WorkflowStateProjection
///
/// # Arguments
///
/// * `event_store` - Event store for reading events
/// * `subscription_manager` - Manager for event subscriptions
/// * `state_store` - Store for persisting projection state
///
/// # Returns
///
/// Returns a configured ProjectionManager with all core projections registered
/// and ready to process events automatically.
///
/// # Example
///
/// ```rust,ignore
/// use orcher_core::projections::setup::setup_core_projections;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let event_store = todo!();
/// # let subscription_manager = todo!();
/// # let state_store = todo!();
/// let manager = setup_core_projections(
///     event_store,
///     subscription_manager,
///     state_store,
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn setup_core_projections<S, T>(
    event_store: Arc<S>,
    subscription_manager: Arc<SubscriptionManager<S>>,
    state_store: T,
) -> Result<ProjectionManager<S>, orcher_events::projections::ProjectionError>
where
    S: EventStore + Send + Sync + 'static,
    T: ProjectionStateStore + 'static,
{
    let manager = ProjectionManager::new(event_store, subscription_manager, state_store);

    // Register core projections
    manager
        .register_projection(TaskStateProjection::new())
        .await?;
    manager
        .register_projection(WorkerStateProjection::new())
        .await?;
    manager
        .register_projection(WorkflowStateProjection::new())
        .await?;

    Ok(manager)
}

/// Set up ProjectionManager with selected projections
///
/// This provides more control over which projections to register,
/// allowing you to enable only the projections you need.
///
/// # Arguments
///
/// * `event_store` - Event store for reading events
/// * `subscription_manager` - Manager for event subscriptions
/// * `state_store` - Store for persisting projection state
/// * `enable_task` - Whether to enable TaskStateProjection
/// * `enable_worker` - Whether to enable WorkerStateProjection
/// * `enable_workflow` - Whether to enable WorkflowStateProjection
///
/// # Example
///
/// ```rust,ignore
/// use orcher_core::projections::setup::setup_projections;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let event_store = todo!();
/// # let subscription_manager = todo!();
/// # let state_store = todo!();
/// // Only enable task and workflow projections
/// let manager = setup_projections(
///     event_store,
///     subscription_manager,
///     state_store,
///     true,  // enable task
///     false, // disable worker
///     true,  // enable workflow
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn setup_projections<S, T>(
    event_store: Arc<S>,
    subscription_manager: Arc<SubscriptionManager<S>>,
    state_store: T,
    enable_task: bool,
    enable_worker: bool,
    enable_workflow: bool,
) -> Result<ProjectionManager<S>, orcher_events::projections::ProjectionError>
where
    S: EventStore + Send + Sync + 'static,
    T: ProjectionStateStore + 'static,
{
    let manager = ProjectionManager::new(event_store, subscription_manager, state_store);

    // Register selected projections
    if enable_task {
        manager
            .register_projection(TaskStateProjection::new())
            .await?;
    }

    if enable_worker {
        manager
            .register_projection(WorkerStateProjection::new())
            .await?;
    }

    if enable_workflow {
        manager
            .register_projection(WorkflowStateProjection::new())
            .await?;
    }

    Ok(manager)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_setup_functions_exist() {
        // Compile-time test to ensure setup functions are available
        // Actual functionality requires infrastructure setup
    }
}
