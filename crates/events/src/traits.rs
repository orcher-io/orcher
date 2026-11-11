//! Base traits for the event type hierarchy.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Every event in the system implements this.
pub trait Event: Send + Sync + Clone + std::fmt::Debug {
    /// Unique ID for this particular event instance.
    fn event_id(&self) -> Uuid;

    /// When the event occurred.
    fn timestamp(&self) -> Timestamptz;

    /// Discriminator string, e.g. "TaskSubmitted".
    fn event_type(&self) -> &str;

    /// Optional bag of metadata (correlation IDs, trace context, etc).
    fn metadata(&self) -> Option<&std::collections::HashMap<String, String>> {
        None
    }
}

/// An event tied to an aggregate root (event-sourcing style).
pub trait DomainEvent: Event + Serialize + for<'de> Deserialize<'de> {
    /// Which aggregate this event belongs to.
    fn aggregate_id(&self) -> Uuid;

    /// Aggregate type name (e.g. "Task", "Workflow").
    fn aggregate_type(&self) -> &str;

    /// Schema version of this event.
    fn version(&self) -> i64;
}

/// An event meant for consumption by external systems.
pub trait IntegrationEvent: Event + Serialize + for<'de> Deserialize<'de> {
    /// Where this event originated.
    fn source(&self) -> &str;

    /// Where this event is headed, if known.
    fn destination(&self) -> Option<&str> {
        None
    }
}

/// Infrastructure-level event (health checks, metrics, alerts).
pub trait SystemEvent: Event + Serialize + for<'de> Deserialize<'de> {
    /// Which component emitted this.
    fn component(&self) -> &str;

    /// How urgent is this.
    fn severity(&self) -> EventSeverity;
}

/// Severity levels for system events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum EventSeverity {
    /// Noisy debug-level detail.
    Debug,
    /// Normal operational info.
    #[default]
    Info,
    /// Something looks off but isn't broken yet.
    Warning,
    /// Something failed.
    Error,
    /// Wake someone up.
    Critical,
}

impl std::fmt::Display for EventSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Debug => write!(f, "DEBUG"),
            Self::Info => write!(f, "INFO"),
            Self::Warning => write!(f, "WARNING"),
            Self::Error => write!(f, "ERROR"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_ordering() {
        assert!(EventSeverity::Debug < EventSeverity::Info);
        assert!(EventSeverity::Info < EventSeverity::Warning);
        assert!(EventSeverity::Warning < EventSeverity::Error);
        assert!(EventSeverity::Error < EventSeverity::Critical);
    }

    #[test]
    fn test_severity_display() {
        assert_eq!(EventSeverity::Debug.to_string(), "DEBUG");
        assert_eq!(EventSeverity::Critical.to_string(), "CRITICAL");
    }
}
