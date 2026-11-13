//! Worker lifecycle events: registration, heartbeats, health changes, deregistration.

use orcher_common::types::identifiers::WorkerId;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// Worker pool state transitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerEvent {
    /// Worker joined the pool with its declared capabilities.
    Registered {
        worker_id: WorkerId,
        /// Task types this worker can handle.
        capabilities: Vec<String>,
        registered_at: Timestamptz,
    },

    /// Periodic liveness signal from the worker.
    HeartbeatReceived {
        worker_id: WorkerId,
        /// 0.0 = idle, 1.0 = fully loaded.
        load: f64,
        timestamp: Timestamptz,
    },

    /// Worker transitioned between healthy and unhealthy.
    HealthChanged {
        worker_id: WorkerId,
        healthy: bool,
        reason: Option<String>,
        timestamp: Timestamptz,
    },

    /// Worker disconnected or missed heartbeat deadline.
    Offline {
        worker_id: WorkerId,
        reason: String,
        offline_at: Timestamptz,
    },

    /// Refreshed load/capacity snapshot for the worker.
    LoadUpdated {
        worker_id: WorkerId,
        /// 0.0 = idle, 1.0 = fully loaded.
        load: f64,
        /// 0.0 = no room, 1.0 = completely free.
        available_capacity: f64,
    },

    /// Graceful removal from the pool (terminal).
    Deregistered {
        worker_id: WorkerId,
        reason: String,
        deregistered_at: Timestamptz,
    },

    /// Max concurrent task slots changed.
    CapacityChanged {
        worker_id: WorkerId,
        new_capacity: u32,
        timestamp: Timestamptz,
    },

    /// Load exceeded the configured threshold.
    Overloaded {
        worker_id: WorkerId,
        current_load: f64,
        /// The threshold that was breached.
        threshold: f64,
        timestamp: Timestamptz,
    },
}

impl WorkerEvent {
    /// The worker this event is about.
    pub fn worker_id(&self) -> &WorkerId {
        match self {
            Self::Registered { worker_id, .. } => worker_id,
            Self::HeartbeatReceived { worker_id, .. } => worker_id,
            Self::HealthChanged { worker_id, .. } => worker_id,
            Self::Offline { worker_id, .. } => worker_id,
            Self::LoadUpdated { worker_id, .. } => worker_id,
            Self::Deregistered { worker_id, .. } => worker_id,
            Self::CapacityChanged { worker_id, .. } => worker_id,
            Self::Overloaded { worker_id, .. } => worker_id,
        }
    }

    /// When this event happened.
    pub fn timestamp(&self) -> &Timestamptz {
        match self {
            Self::Registered { registered_at, .. } => registered_at,
            Self::HeartbeatReceived { timestamp, .. } => timestamp,
            Self::HealthChanged { timestamp, .. } => timestamp,
            Self::Offline { offline_at, .. } => offline_at,
            Self::LoadUpdated { .. } => {
                // LoadUpdated doesn't have a timestamp field in the original
                // We use a static reference to avoid allocation
                static DEFAULT_TIME: std::sync::OnceLock<Timestamptz> = std::sync::OnceLock::new();
                DEFAULT_TIME.get_or_init(Timestamptz::now)
            }
            Self::Deregistered {
                deregistered_at, ..
            } => deregistered_at,
            Self::CapacityChanged { timestamp, .. } => timestamp,
            Self::Overloaded { timestamp, .. } => timestamp,
        }
    }

    /// Dotted type name, e.g. "worker.registered".
    pub fn event_type(&self) -> &str {
        match self {
            Self::Registered { .. } => "worker.registered",
            Self::HeartbeatReceived { .. } => "worker.heartbeat_received",
            Self::HealthChanged { .. } => "worker.health_changed",
            Self::Offline { .. } => "worker.offline",
            Self::LoadUpdated { .. } => "worker.load_updated",
            Self::Deregistered { .. } => "worker.deregistered",
            Self::CapacityChanged { .. } => "worker.capacity_changed",
            Self::Overloaded { .. } => "worker.overloaded",
        }
    }

    /// Whether this event implies the worker is in good shape.
    pub fn is_healthy(&self) -> bool {
        match self {
            Self::HealthChanged { healthy, .. } => *healthy,
            Self::Registered { .. } | Self::HeartbeatReceived { .. } => true,
            Self::Offline { .. } | Self::Overloaded { .. } => false,
            _ => true, // Neutral events
        }
    }

    /// False for Offline or Deregistered.
    pub fn is_available(&self) -> bool {
        !matches!(self, Self::Offline { .. } | Self::Deregistered { .. })
    }

    /// Current load if this is a load-related event.
    pub fn load(&self) -> Option<f64> {
        match self {
            Self::HeartbeatReceived { load, .. } => Some(*load),
            Self::LoadUpdated { load, .. } => Some(*load),
            Self::Overloaded { current_load, .. } => Some(*current_load),
            _ => None,
        }
    }

    /// True only for Deregistered.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Deregistered { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::Identifier;

    #[test]
    fn test_worker_event_registered() {
        let worker_id = WorkerId::generate();
        let registered_at = Timestamptz::now();
        let capabilities = vec!["python".to_string(), "nodejs".to_string()];

        let event = WorkerEvent::Registered {
            worker_id,
            capabilities: capabilities.clone(),
            registered_at,
        };

        assert_eq!(event.worker_id(), &worker_id);
        assert_eq!(event.timestamp(), &registered_at);
        assert_eq!(event.event_type(), "worker.registered");
        assert!(event.is_healthy());
        assert!(event.is_available());
        assert!(!event.is_terminal());
        assert_eq!(event.load(), None);
    }

    #[test]
    fn test_worker_event_heartbeat() {
        let worker_id = WorkerId::generate();
        let timestamp = Timestamptz::now();
        let load = 0.45;

        let event = WorkerEvent::HeartbeatReceived {
            worker_id,
            load,
            timestamp,
        };

        assert_eq!(event.worker_id(), &worker_id);
        assert_eq!(event.timestamp(), &timestamp);
        assert_eq!(event.event_type(), "worker.heartbeat_received");
        assert!(event.is_healthy());
        assert_eq!(event.load(), Some(load));
    }

    #[test]
    fn test_worker_event_health_changed() {
        let worker_id = WorkerId::generate();
        let timestamp = Timestamptz::now();

        // Healthy
        let healthy_event = WorkerEvent::HealthChanged {
            worker_id,
            healthy: true,
            reason: None,
            timestamp,
        };
        assert!(healthy_event.is_healthy());

        // Unhealthy
        let unhealthy_event = WorkerEvent::HealthChanged {
            worker_id,
            healthy: false,
            reason: Some("High CPU usage".to_string()),
            timestamp,
        };
        assert!(!unhealthy_event.is_healthy());
    }

    #[test]
    fn test_worker_event_offline() {
        let worker_id = WorkerId::generate();
        let offline_at = Timestamptz::now();

        let event = WorkerEvent::Offline {
            worker_id,
            reason: "Connection timeout".to_string(),
            offline_at,
        };

        assert_eq!(event.worker_id(), &worker_id);
        assert_eq!(event.timestamp(), &offline_at);
        assert_eq!(event.event_type(), "worker.offline");
        assert!(!event.is_healthy());
        assert!(!event.is_available());
    }

    #[test]
    fn test_worker_event_load_updated() {
        let worker_id = WorkerId::generate();
        let load = 0.65;
        let available_capacity = 0.35;

        let event = WorkerEvent::LoadUpdated {
            worker_id,
            load,
            available_capacity,
        };

        assert_eq!(event.worker_id(), &worker_id);
        assert_eq!(event.event_type(), "worker.load_updated");
        assert_eq!(event.load(), Some(load));
    }

    #[test]
    fn test_worker_event_deregistered() {
        let worker_id = WorkerId::generate();
        let deregistered_at = Timestamptz::now();

        let event = WorkerEvent::Deregistered {
            worker_id,
            reason: "Graceful shutdown".to_string(),
            deregistered_at,
        };

        assert_eq!(event.worker_id(), &worker_id);
        assert_eq!(event.event_type(), "worker.deregistered");
        assert!(!event.is_available());
        assert!(event.is_terminal());
    }

    #[test]
    fn test_worker_event_capacity_changed() {
        let worker_id = WorkerId::generate();
        let timestamp = Timestamptz::now();

        let event = WorkerEvent::CapacityChanged {
            worker_id,
            new_capacity: 10,
            timestamp,
        };

        assert_eq!(event.worker_id(), &worker_id);
        assert_eq!(event.event_type(), "worker.capacity_changed");
    }

    #[test]
    fn test_worker_event_overloaded() {
        let worker_id = WorkerId::generate();
        let timestamp = Timestamptz::now();

        let event = WorkerEvent::Overloaded {
            worker_id,
            current_load: 0.95,
            threshold: 0.80,
            timestamp,
        };

        assert_eq!(event.worker_id(), &worker_id);
        assert_eq!(event.event_type(), "worker.overloaded");
        assert!(!event.is_healthy());
        assert_eq!(event.load(), Some(0.95));
    }

    #[test]
    fn test_worker_event_serialization() {
        let event = WorkerEvent::Registered {
            worker_id: WorkerId::generate(),
            capabilities: vec!["python".to_string()],
            registered_at: Timestamptz::now(),
        };

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: WorkerEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_worker_event_clone() {
        let event = WorkerEvent::Registered {
            worker_id: WorkerId::generate(),
            capabilities: vec!["python".to_string()],
            registered_at: Timestamptz::now(),
        };

        let cloned = event.clone();
        assert_eq!(event, cloned);
    }

    #[test]
    fn test_all_worker_event_types() {
        let worker_id = WorkerId::generate();
        let now = Timestamptz::now();

        let events = vec![
            WorkerEvent::Registered {
                worker_id,
                capabilities: vec!["python".to_string()],
                registered_at: now,
            },
            WorkerEvent::HeartbeatReceived {
                worker_id,
                load: 0.5,
                timestamp: now,
            },
            WorkerEvent::HealthChanged {
                worker_id,
                healthy: true,
                reason: None,
                timestamp: now,
            },
            WorkerEvent::Offline {
                worker_id,
                reason: "timeout".to_string(),
                offline_at: now,
            },
            WorkerEvent::LoadUpdated {
                worker_id,
                load: 0.6,
                available_capacity: 0.4,
            },
            WorkerEvent::Deregistered {
                worker_id,
                reason: "shutdown".to_string(),
                deregistered_at: now,
            },
            WorkerEvent::CapacityChanged {
                worker_id,
                new_capacity: 10,
                timestamp: now,
            },
            WorkerEvent::Overloaded {
                worker_id,
                current_load: 0.95,
                threshold: 0.8,
                timestamp: now,
            },
        ];

        // All events should have the same worker_id
        for event in &events {
            assert_eq!(event.worker_id(), &worker_id);
        }

        // All events should have a valid event_type
        for event in &events {
            assert!(!event.event_type().is_empty());
        }
    }
}
