//! Workflow and task lifecycle metrics, each with exactly one label set.
//!
//! A metric emitted from two places with different labels is two different
//! things to a metrics backend. With the Prometheus recorder installed, the
//! second shape fails registration and panics the task that emitted it — which
//! for `orcher_workflows_executing` was the workflow-completion RPC, on every
//! completion, surfacing to workers only as a torn-down stream. Without that
//! recorder the two shapes are simply separate series that never net against
//! each other. Five metrics had drifted that way.
//!
//! Every lifecycle metric is emitted through a function here, and each function
//! is the only definition of its metric's labels, so call sites cannot disagree.
//!
//! Labels are bounded on purpose: namespace, workflow type, task type. Task
//! *id* was used before, and a task id carries a sequence number, so a workflow
//! that looped a thousand times minted a thousand series.
//!
//! "Workflows executing" is deliberately not here. It was an up/down gauge held
//! in process memory for a quantity that lives in the database: it read zero
//! after every restart while workflows were still running, and only one of the
//! several ways an execution is created incremented it while every completion
//! decremented it. It is now counted from the database when metrics are scraped.

use super::names;

/// The label value for a dimension the caller could not determine. A fixed
/// value keeps the series count bounded rather than inventing one per failure.
pub const UNKNOWN: &str = "unknown";

/// A workflow reached a terminal state.
pub fn workflow_finished(namespace: &str, workflow_type: &str, succeeded: bool) {
    let name = if succeeded {
        names::workflows::COMPLETED_TOTAL
    } else {
        names::workflows::FAILED_TOTAL
    };
    metrics::counter!(name,
        "namespace" => namespace.to_string(),
        "workflow_type" => workflow_type.to_string()
    )
    .increment(1);
}

/// A workflow completion arrived for an activation that had been handed out
/// again, and was acknowledged without being applied. A steady rate means
/// workers are losing their claims mid-activation — claims timing out, or
/// workflows being woken while an activation is still running.
pub fn stale_completion(namespace: &str) {
    metrics::counter!(names::workflows::STALE_COMPLETIONS_TOTAL,
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

/// A workflow was submitted.
pub fn task_submitted(namespace: &str, workflow_type: &str) {
    metrics::counter!(names::tasks::SUBMITTED_TOTAL,
        "namespace" => namespace.to_string(),
        "workflow_type" => workflow_type.to_string()
    )
    .increment(1);
}

/// A task finished, successfully or not, after `duration_secs` if known.
pub fn task_finished(
    namespace: &str,
    task_type: &str,
    succeeded: bool,
    duration_secs: Option<f64>,
) {
    let name = if succeeded {
        names::tasks::COMPLETED_TOTAL
    } else {
        names::tasks::FAILED_TOTAL
    };
    metrics::counter!(name,
        "namespace" => namespace.to_string(),
        "task_type" => task_type.to_string()
    )
    .increment(1);

    if let Some(seconds) = duration_secs {
        metrics::histogram!(names::tasks::DURATION_SECONDS,
            "namespace" => namespace.to_string(),
            "task_type" => task_type.to_string()
        )
        .record(seconds);
    }
}

#[cfg(all(test, feature = "prometheus"))]
mod tests {
    use super::*;

    /// Every emission path, every outcome, through the recorder that panics on
    /// inconsistent labels. Before, completing a workflow panicked here.
    #[test]
    fn every_lifecycle_metric_has_one_label_set() {
        let registry = prometheus::Registry::new();
        let recorder = metrics_prometheus::Recorder::builder()
            .with_registry(registry.clone())
            .build();

        metrics::with_local_recorder(&recorder, || {
            for succeeded in [true, false] {
                workflow_finished("ns", "Checkout", succeeded);
                workflow_finished("ns", UNKNOWN, succeeded);
                task_finished("ns", "charge_card", succeeded, Some(0.25));
                task_finished("ns", "charge_card", succeeded, None);
            }
            task_submitted("ns", "Checkout");
        });

        let families: Vec<String> = registry
            .gather()
            .iter()
            .map(|f| f.get_name().to_string())
            .collect();
        for expected in [
            names::workflows::COMPLETED_TOTAL,
            names::workflows::FAILED_TOTAL,
            names::tasks::SUBMITTED_TOTAL,
            names::tasks::COMPLETED_TOTAL,
            names::tasks::FAILED_TOTAL,
            names::tasks::DURATION_SECONDS,
        ] {
            assert!(families.iter().any(|f| f == expected), "{expected} missing");
        }
    }

    /// Nothing emits the in-memory executing gauge any more; it is counted from
    /// the database at scrape time. A reappearance would duplicate that family
    /// in the scrape output, and Prometheus rejects the whole scrape for that.
    #[test]
    fn workflows_executing_is_not_emitted_from_memory() {
        let registry = prometheus::Registry::new();
        let recorder = metrics_prometheus::Recorder::builder()
            .with_registry(registry.clone())
            .build();
        metrics::with_local_recorder(&recorder, || {
            workflow_finished("ns", "Checkout", true);
            task_submitted("ns", "Checkout");
        });
        assert!(!registry
            .gather()
            .iter()
            .any(|f| f.get_name() == names::workflows::EXECUTING));
    }
}
