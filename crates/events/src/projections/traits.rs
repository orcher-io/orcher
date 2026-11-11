//! Traits for implementing projections.

use async_trait::async_trait;
use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::fmt;

use orcher_storage::schema::types::event::StoredEvent;

/// Transforms events into a read model.
#[async_trait]
pub trait Projection: Send + Sync {
    type State: Send + Sync;

    fn name(&self) -> &str;

    /// Called for each event. Update projection state based on the event data.
    async fn project(&mut self, event: &StoredEvent) -> Result<(), Box<dyn std::error::Error>>;

    async fn get_state(&self) -> Result<Self::State, Box<dyn std::error::Error>>;

    /// Resets to initial state. Called when rebuilding from scratch.
    async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    /// Return false to skip events this projection doesn't care about.
    fn handles_event_type(&self, event_type: &str) -> bool {
        let _ = event_type;
        true
    }

    /// Bump this when the projection schema changes to trigger a rebuild.
    fn version(&self) -> u32 {
        1
    }

    fn category(&self) -> Option<&str> {
        None
    }
}

/// Convenience trait for projections with serializable state.
#[async_trait]
pub trait TypedProjection: Send + Sync {
    type State: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone;

    fn name(&self) -> &str;
    async fn project(&mut self, event: &StoredEvent) -> super::Result<()>;
    async fn get_state(&self) -> Self::State;
    fn initial_state(&self) -> Self::State;

    async fn reset(&mut self) -> super::Result<()> {
        Ok(())
    }

    fn handles_event_type(&self, event_type: &str) -> bool {
        let _ = event_type;
        true
    }

    fn version(&self) -> u32 {
        1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionMetadata {
    pub name: String,
    pub last_processed_position: u64,
    pub last_update: Timestamptz,
    pub status: ProjectionStatus,
    pub error_count: u64,
    pub version: u32,
    pub created_at: Timestamptz,
    pub category: Option<String>,
}

impl ProjectionMetadata {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            last_processed_position: 0,
            last_update: Timestamptz::now(),
            status: ProjectionStatus::Building,
            error_count: 0,
            version: 1,
            created_at: Timestamptz::now(),
            category: None,
        }
    }

    pub fn update_position(&mut self, position: u64) {
        self.last_processed_position = position;
        self.last_update = Timestamptz::now();
    }

    pub fn increment_errors(&mut self) {
        self.error_count += 1;
        self.last_update = Timestamptz::now();
    }

    pub fn set_status(&mut self, status: ProjectionStatus) {
        self.status = status;
        self.last_update = Timestamptz::now();
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.status, ProjectionStatus::Running)
    }

    pub fn age(&self) -> SignedDuration {
        Timestamptz::now() - self.created_at
    }

    pub fn time_since_update(&self) -> SignedDuration {
        Timestamptz::now() - self.last_update
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProjectionStatus {
    Building,
    Running,
    Paused,
    Failed,
    Stopped,
}

impl fmt::Display for ProjectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Building => write!(f, "Building"),
            Self::Running => write!(f, "Running"),
            Self::Paused => write!(f, "Paused"),
            Self::Failed => write!(f, "Failed"),
            Self::Stopped => write!(f, "Stopped"),
        }
    }
}

impl ProjectionStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Building | Self::Running)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Failed | Self::Stopped)
    }

    pub fn can_process_events(&self) -> bool {
        matches!(self, Self::Building | Self::Running)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_projection_metadata_creation() {
        let metadata = ProjectionMetadata::new("test_projection");
        assert_eq!(metadata.name, "test_projection");
        assert_eq!(metadata.last_processed_position, 0);
        assert_eq!(metadata.error_count, 0);
        assert_eq!(metadata.status, ProjectionStatus::Building);
    }

    #[test]
    fn test_projection_metadata_update_position() {
        let mut metadata = ProjectionMetadata::new("test");
        let initial_update = metadata.last_update;

        // Small delay to ensure timestamp changes
        std::thread::sleep(std::time::Duration::from_millis(10));

        metadata.update_position(100);
        assert_eq!(metadata.last_processed_position, 100);
        assert!(metadata.last_update > initial_update);
    }

    #[test]
    fn test_projection_metadata_increment_errors() {
        let mut metadata = ProjectionMetadata::new("test");
        assert_eq!(metadata.error_count, 0);

        metadata.increment_errors();
        assert_eq!(metadata.error_count, 1);

        metadata.increment_errors();
        assert_eq!(metadata.error_count, 2);
    }

    #[test]
    fn test_projection_status_display() {
        assert_eq!(ProjectionStatus::Building.to_string(), "Building");
        assert_eq!(ProjectionStatus::Running.to_string(), "Running");
        assert_eq!(ProjectionStatus::Paused.to_string(), "Paused");
        assert_eq!(ProjectionStatus::Failed.to_string(), "Failed");
        assert_eq!(ProjectionStatus::Stopped.to_string(), "Stopped");
    }

    #[test]
    fn test_projection_status_checks() {
        assert!(ProjectionStatus::Building.is_active());
        assert!(ProjectionStatus::Running.is_active());
        assert!(!ProjectionStatus::Paused.is_active());
        assert!(!ProjectionStatus::Failed.is_active());

        assert!(ProjectionStatus::Failed.is_terminal());
        assert!(ProjectionStatus::Stopped.is_terminal());
        assert!(!ProjectionStatus::Running.is_terminal());

        assert!(ProjectionStatus::Building.can_process_events());
        assert!(ProjectionStatus::Running.can_process_events());
        assert!(!ProjectionStatus::Paused.can_process_events());
        assert!(!ProjectionStatus::Failed.can_process_events());
    }

    #[test]
    fn test_projection_metadata_is_healthy() {
        let mut metadata = ProjectionMetadata::new("test");

        metadata.set_status(ProjectionStatus::Running);
        assert!(metadata.is_healthy());

        metadata.set_status(ProjectionStatus::Failed);
        assert!(!metadata.is_healthy());

        metadata.set_status(ProjectionStatus::Building);
        assert!(!metadata.is_healthy());
    }

    #[test]
    fn test_projection_metadata_age() {
        let metadata = ProjectionMetadata::new("test");
        let age = metadata.age();

        // Should be very recent (within a second)
        assert!(age.as_secs() < 1);
    }

    #[test]
    fn test_projection_metadata_time_since_update() {
        let metadata = ProjectionMetadata::new("test");
        let time_since = metadata.time_since_update();

        // Should be very recent
        assert!(time_since.as_secs() < 1);
    }
}
