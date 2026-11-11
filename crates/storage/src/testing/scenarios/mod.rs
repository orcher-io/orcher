//! # Test Scenario Templates
//!
//! This module provides test scenario templates for common storage testing patterns,
//! integrating with orcher-common test utilities and storage test infrastructure.

#[cfg(test)]
pub mod integration_scenarios;

// Re-export commonly used scenarios
#[cfg(test)]
pub use integration_scenarios::*;
