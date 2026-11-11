//! Declared value sets for columns guarded by a database CHECK constraint.
//!
//! These columns are written as plain strings from many call sites, so nothing
//! in the type system stops the engine emitting a value the database will
//! reject. That has shipped twice:
//!
//! - `restarted_fresh` — the engine set the status, the CHECK did not allow it,
//!   and a fresh restart could never persist (fixed by V026).
//! - `reset` — same shape, and a workflow reset could never persist (V033).
//!
//! Both were runtime failures on a path with no test coverage, found only by
//! exercising the feature end to end. The lists below are the single declared
//! source of truth, and the tests in this module assert they still match what
//! the migrations actually apply.
//!
//! **Adding a value:** add it here *and* in a new migration, in the same change.
//! The test will fail if you do only one.

/// Every value the engine may write to `workflow_executions.status`.
///
/// Must stay in step with the `workflow_exec_status_check` CHECK constraint.
pub const WORKFLOW_EXECUTION_STATUSES: &[&str] = &[
    "pending",
    "queued",
    "running",
    "paused",
    "completed",
    "failed",
    "cancelled",
    "terminated",
    "timed_out",
    "retrying",
    "waiting",
    // A workflow that continued as new: terminal, produced no result of its own,
    // the logical workflow carries on in a successor.
    "restarted_fresh",
    // A workflow that was reset: terminal, superseded by a successor that
    // replays the journal up to the target event.
    "reset",
];

/// Every value the engine may write to `workflow_executions.trigger_type`.
///
/// Must stay in step with the `workflow_exec_trigger_check` CHECK constraint.
///
/// Note this records the *mechanism*, not the reason. Engine-created successor
/// runs — a fresh restart, a workflow-level retry, a reset — all use `workflow`;
/// the reason is carried by `parent_execution_id` and the predecessor's status.
pub const WORKFLOW_EXECUTION_TRIGGER_TYPES: &[&str] = &[
    "manual",
    "api",
    "scheduled",
    "event",
    "webhook",
    "workflow",
    "cli",
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Pull the quoted values out of the most recent definition of `constraint`
    /// across the migration SQL, so the assertion reflects what actually gets
    /// applied rather than a value list copied by hand.
    fn constraint_values(sql: &str, constraint: &str) -> BTreeSet<String> {
        let start = sql
            .rfind(constraint)
            .unwrap_or_else(|| panic!("constraint {constraint} not found in the supplied SQL"));
        let tail = &sql[start..];

        // Take the `IN ( ... )` list belonging to this constraint. Scanning to
        // the matching close paren keeps a later constraint in the same file
        // from leaking its values in.
        let in_at = tail
            .find(" IN (")
            .unwrap_or_else(|| panic!("constraint {constraint} has no IN (...) list"));
        let body = &tail[in_at + " IN (".len()..];
        let end = body
            .find(')')
            .unwrap_or_else(|| panic!("constraint {constraint} has an unterminated IN list"));

        body[..end]
            .split('\'')
            .skip(1)
            .step_by(2)
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn declared_statuses_match_the_check_constraint() {
        let sql = crate::migrations::sql::postgres::V033_RESET_STATUS;
        let applied = constraint_values(sql, "workflow_exec_status_check");
        let declared: BTreeSet<String> = WORKFLOW_EXECUTION_STATUSES
            .iter()
            .map(|s| s.to_string())
            .collect();

        assert_eq!(
            declared, applied,
            "WORKFLOW_EXECUTION_STATUSES and the workflow_exec_status_check constraint have \
             diverged. A status the engine writes but the constraint rejects fails at runtime, \
             on whichever path happens to set it — that is how restarted_fresh (V026) and reset \
             (V033) both shipped broken. Add the value in BOTH places."
        );
    }

    #[test]
    fn declared_trigger_types_match_the_check_constraint() {
        let sql = crate::migrations::sql::postgres::V003_WORKFLOW_TABLES;
        let applied = constraint_values(sql, "workflow_exec_trigger_check");
        let declared: BTreeSet<String> = WORKFLOW_EXECUTION_TRIGGER_TYPES
            .iter()
            .map(|s| s.to_string())
            .collect();

        assert_eq!(
            declared, applied,
            "WORKFLOW_EXECUTION_TRIGGER_TYPES and the workflow_exec_trigger_check constraint \
             have diverged. Same failure mode as the status set: the insert is rejected at \
             runtime. Add the value in BOTH places."
        );
    }

    /// Guards the two values whose absence caused a shipped defect, so a future
    /// tidy-up cannot quietly drop them.
    #[test]
    fn successor_statuses_remain_permitted() {
        for status in ["restarted_fresh", "reset"] {
            assert!(
                WORKFLOW_EXECUTION_STATUSES.contains(&status),
                "'{status}' is a terminal status the engine writes for a superseded execution; \
                 removing it breaks that path at runtime"
            );
        }
    }
}
