//! REST API v1 Validation
//!
//! This module contains validation rules for v1 REST API requests.
//! Each submodule corresponds to a specific domain or functionality.

/// Workflow validation rules
pub mod workflows;

/// Schedule validation rules
pub mod schedules;

/// User validation rules
pub mod users;

/// Authentication validation rules
pub mod auth;

/// Common validation utilities
pub mod common;
