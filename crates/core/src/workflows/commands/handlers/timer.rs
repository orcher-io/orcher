//! Timer command handlers.

use orcher_common::{error::OrcherError, OrcherResult};
use orcher_proto::orcher::v1::Command;
use std::sync::Arc;
use tracing::{debug, info};

use crate::engine::WorkflowCoordinator;
use crate::workflows::JournalService;
use jiff::SignedDuration;

/// Handle StartTimer command
pub async fn handle_start_timer(
    journal_service: &Arc<JournalService>,
    coordinator: &Option<Arc<WorkflowCoordinator>>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling StartTimer command"
    );

    // Extract command attributes
    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::StartTimer(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing StartTimer attributes"))?;

    // Calculate duration from protobuf Duration
    let duration_ms = attrs
        .start_to_fire_timeout
        .as_ref()
        .map(|d| d.seconds * 1000 + d.nanos as i64 / 1_000_000)
        .unwrap_or(0);

    // Schedule timer via coordinator (creates timer queue entry for TimerProcessor)
    if let Some(coord) = coordinator {
        let wf_id = uuid::Uuid::parse_str(workflow_id)
            .map_err(|e| OrcherError::invalid_input(format!("Invalid workflow_id UUID: {}", e)))?;
        let duration = SignedDuration::from_millis(duration_ms);
        // A workflow replays while a timer is pending, and every replay emits
        // the same StartTimer again: after a task completes, after an event
        // arrives, after the claim-timeout reclaimer hands the parked workflow
        // back. Each of those used to schedule another copy of the timer. The
        // copies then fired one by one, every firing re-dispatched the
        // workflow, and the replay emitted its current wait's timer again, so
        // a loop of timed waits accumulated timers round after round. A start
        // for a timer this execution already has is a replay: nothing to
        // schedule, and nothing to journal unless the journal is missing it.
        let scheduled = coord
            .schedule_timer_once(wf_id, attrs.timer_id.clone(), duration)
            .await?;
        if scheduled.is_none() {
            debug!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                timer_id = %attrs.timer_id,
                "StartTimer for a timer already scheduled (replay); not scheduled again"
            );
        }
    } else {
        // Journaling the start without scheduling it left the workflow parked
        // on a timer nothing would ever fire, behind a warning. An engine
        // built without a coordinator cannot run timers; saying so fails the
        // activation, which is retried — so it works once the engine is
        // configured with one, instead of never.
        return Err(OrcherError::Configuration {
            key: "coordinator".to_string(),
            message: format!(
                "timer '{}' cannot be scheduled: this engine has no workflow coordinator",
                attrs.timer_id
            ),
        });
    }

    // Journal TimerStarted (for deterministic replay) once per timer — on a
    // replay as well as the first time. The timer row and this entry are not
    // written together, so an attempt that scheduled the timer and failed
    // before journaling it left a timer the journal never mentioned, and every
    // replay skipped the journal because the timer existed. Journaling here
    // whenever the entry is missing heals such a workflow on its next replay;
    // keyed by the timer id, it never journals the same timer twice.
    journal_service
        .append_timer_started_once(workflow_id, execution_id, &attrs.timer_id, duration_ms)
        .await?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        timer_id = %attrs.timer_id,
        duration_ms = %duration_ms,
        "Timer started and scheduled"
    );

    Ok(())
}

/// Handle CancelTimer command
///
/// Cancels a previously started timer by recording a TimerCanceled journal entry.
/// The timer service checks for cancellation before firing.
pub async fn handle_cancel_timer(
    journal_service: &Arc<JournalService>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling CancelTimer command"
    );

    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::CancelTimer(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing CancelTimer attributes"))?;

    // Append TimerCanceled journal entry (started_event_id 0 = lookup by timer_id)
    journal_service
        .append_timer_canceled(workflow_id, execution_id, &attrs.timer_id, 0)
        .await?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        timer_id = %attrs.timer_id,
        "Timer canceled"
    );

    Ok(())
}
