//! REST API Version 1 (Current Stable)
//!
//! This module implements the current stable version of the REST API.
//! All endpoints communicate with the orchestrator service via gRPC clients.

/// Request handlers for v1 API endpoints
pub mod handlers;

/// Data models for v1 API (requests, responses, domain models)
pub mod models;

/// Validation rules for v1 API requests
pub mod validation;

/// Route definitions for v1 API
pub mod routes;
