//! Integration tests for event-driven component migrations
//!
//! **DEPRECATED - ALL TESTS DISABLED**
//!
//! This test file is obsolete. All tested components were removed in the worker module cleanup
//! (worker module removal) as part of the architecture migration from push-based to
//! pull-based (service-polling) orchestration.
//!
//! ## What Was Removed (January 2025)
//!
//! The entire `worker/` module was removed (7,031 lines), including:
//! - `HealthMonitor` - Worker health monitoring via heartbeats
//! - `TaskAssigner` - Server-side task assignment
//! - `FailoverHandler` - Failover coordination
//! - `WorkerRegistry` - Central worker registration
//! - `WorkerManager` - Worker lifecycle management
//!
//! ## Architecture Change
//!
//! **Before (Push-based)**:
//! ```text
//! Server → Discovers workers → Assigns tasks → Pushes to worker → Worker executes
//!     ↑
//!     └─ HealthMonitor, TaskAssigner, FailoverHandler (tested here)
//! ```
//!
//! **After (Pull-based)**:
//! ```text
//! Client → Server → OutboxQueue
//!                       ↑
//!                       │ (poll & claim)
//!                       │
//!          [SDK Service A, B, C]
//!          │
//!          └─ Services decide what to poll for
//!          └─ Services claim atomically
//!          └─ No server-side worker management needed
//! ```
//!
//! ## Replacement Architecture
//!
//! The functionality tested here is replaced by:
//! - `execution/polling/` - Server-side polling infrastructure
//!   - `Claimer` - Atomic work claiming with SKIP LOCKED
//!   - `Reclaimer` - Recovers work from crashed services
//!   - `Notifier` - Low-latency poll notifications
//!   - `PollingManager` - Long-polling coordination
//!
//! - `sdk-core/service/poller.rs` - SDK service polling
//!   - Services poll for workflow/task executions
//!   - Self-selection replaces task assignment
//!   - Claim timeouts replace health monitoring
//!   - Natural load balancing via polling frequency
//!
//! ## Why Pull-Based Is Better
//!
//! 1. **No Worker Discovery** - Services just start polling
//! 2. **No Task Assignment** - Services self-select work
//! 3. **No Health Monitoring** - Implicit via polling activity
//! 4. **No Failover Logic** - Claim timeout + reclaimer handles it
//! 5. **Simpler** - 7,031 lines removed, ~500 lines added
//! 6. **More Scalable** - Services scale independently
//! 7. **Self-Healing** - Crashed services' work auto-reclaimed
//!
//! ## References
//!
//! - `PHASE2_WORKER_MODULE_REVIEW.md` - Code review
//! - `PHASE2_WORKER_MODULE_CLEANUP_COMPLETE.md` - Completion report
//! - `CLEANUP_ACTION_PLAN.md` - Overall cleanup plan
//!
//! ## Test File Status
//!
//! This file is kept for historical reference but all tests are commented out
//! since the tested functionality no longer exists.
//!
//! **Date Deprecated**: January 20, 2025
//! **Reason**: Worker module removed during architecture cleanup
//! **Alternative**: See `execution/polling/` tests for new architecture

// All test code commented out - components no longer exist

/*

use orcher_common::types::time::Timestamptz;
use orcher_common::types::identifiers::{TaskId, WorkerId};
use orcher_core::events::{EventBridge, InMemoryEventStore};
use orcher_core::runtime::Orchestrator;
use orcher_core::prelude::{
    TaskPriority, TaskSubmission, WorkerHealthStatus, WorkerRegistration, WorkerStatus,
};
// REMOVED: use orcher_core::worker::{CapabilityMatcher, HealthMonitor, TaskAssigner, WorkerRegistry};
use orcher_events::bus::{EventBus, EventBusConfig};
use orcher_events::domain::{TaskEvent, WorkerEvent};
use std::sync::Arc;

// All tests below are obsolete - worker components removed

#[tokio::test]
async fn test_health_monitor_emits_events() {
    // HealthMonitor no longer exists
}

#[tokio::test]
async fn test_health_monitor_emits_health_changed_event() {
    // HealthMonitor no longer exists
}

#[tokio::test]
async fn test_task_assigner_listens_to_task_events() {
    // TaskAssigner no longer exists - replaced by service self-selection
}

#[tokio::test]
async fn test_failover_handler_listens_to_worker_events() {
    // FailoverHandler no longer exists - replaced by claim timeout + reclaimer
}

#[tokio::test]
async fn test_orchestrator_with_event_bridge() {
    // WorkerRegistry no longer exists
}

#[tokio::test]
async fn test_full_system_integration() {
    // All worker components removed
}

#[tokio::test]
async fn test_health_monitor_worker_offline_event() {
    // HealthMonitor no longer exists
}

#[tokio::test]
async fn test_multiple_subscribers_receive_events() {
    // WorkerRegistry no longer exists
}

*/
