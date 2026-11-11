//! # Journal Entry Domain Repository Module
//!
//! This module provides repository implementations for workflow execution journal entries.
//! Journal entries represent the event-sourced history of workflow executions, capturing
//! all state transitions and significant events in an immutable, append-only log.
//!
//! ## ORCHER Terminology
//!
//! - **Journal Entry**: A single event in a workflow's execution history
//! - **Execution Journal**: The complete sequence of journal entries for a workflow execution
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_storage::repositories::domains::journal::JournalRepository;
//! use orcher_storage::schema::types::journal::{JournalEntry, JournalQueryFilters};
//!
//! let repo = JournalRepository::new(pool);
//!
//! // Append a new journal entry
//! let entry = JournalEntry::new(
//!     "wf-123".to_string(),
//!     "exec-456".to_string(),
//!     1,
//!     "WorkflowExecutionStarted".to_string(),
//!     serde_json::json!({"input": "data"}),
//! );
//! // The event id is allocated in the insert's transaction.
//! let created = repo.append_next(entry).await?;
//!
//! // Get complete journal for execution
//! let journal = repo.get_journal("wf-123", "exec-456").await?;
//!
//! // Query with filters
//! let filters = JournalQueryFilters::for_execution(
//!     "wf-123".to_string(),
//!     "exec-456".to_string(),
//! ).paginate(10, 0);
//! let entries = repo.query(filters).await?;
//!
//! // Get statistics
//! let stats = repo.get_statistics("wf-123", "exec-456").await?;
//! println!("Total entries: {}", stats.total_entries);
//! println!("Max event ID: {}", stats.max_event_id);
//! ```
//!
//! ## Key Features
//!
//! - **Append-Only**: Journal entries are immutable once created
//! - **Ordered**: Entries are ordered by event_id for deterministic replay
//! - **Queryable**: Flexible querying with filters and pagination
//! - **Statistics**: Analytics on journal entries (counts, timing, categories)
//! - **Thread-Safe**: Event ID sequencing uses database function
//!
//! ## Relationship with V020 and V022
//!
//! This module complements existing event sourcing infrastructure:
//!
//! - **V020 events**: Platform-wide stream-based event store
//! - **V022 execution_log**: Step-level durable execution tracking
//! - **V024 journal**: Workflow-level event-sourced history (this module)

pub mod repository;

pub use repository::JournalRepository;
