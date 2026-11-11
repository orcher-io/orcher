//! # Test Data Generators
//!
//! This module provides test data generators for creating realistic
//! test data for storage testing, built on orcher-common utilities.

pub mod task_generator;
pub mod workflow_generator;

// Re-export commonly used generators
pub use task_generator::*;
pub use workflow_generator::*;

// Re-export orcher-common test data generator
pub use orcher_common::testing::TestDataGenerator;
