//! Integration Module for Non-gRPC Crate Integrations
//!
//! This module handles direct integration with other ORCHER crates for functionality
//! that doesn't go through gRPC. This includes storage, security, monitoring, and
//! configuration integrations, as well as bridges between API protocols and gRPC.
//!
//! # Architecture
//!
//! Integration is organized into two main categories:
//!
//! ## Direct Crate Integrations
//!
//! These modules integrate directly with other ORCHER crates for infrastructure:
//! - **Storage**: Integration with `orcher-storage` for file and data persistence
//! - **Security**: Integration with `orcher-security` for auth, encryption, and RBAC
//! - **Monitoring**: Integration with `orcher-monitoring` for metrics and tracing
//! - **Config**: Integration with `orcher-config` for configuration management
//!
//! ## API-to-gRPC Bridges
//!
//! These modules transform API requests into gRPC calls:
//! - **REST Bridge**: Converts REST API requests/responses to/from gRPC messages
//! - **GraphQL Bridge**: Converts GraphQL queries/mutations to gRPC calls
//! - **WebSocket Bridge**: Bridges WebSocket messages to gRPC streaming
//!
//! # Important Note
//!
//! This module does NOT integrate with `crates/core` directly. All orchestration
//! and task management operations go through gRPC clients in the `grpc` module.
//! This maintains the three-service architecture:
//! - Gateway (this crate) → Orchestrator (via gRPC) → Workers

/// Integration with orcher-storage crate (file storage, data persistence)
pub mod storage;

/// Integration with orcher-security crate (authentication, authorization, encryption)
pub mod security;

/// Integration with orcher-monitoring crate (metrics, tracing, logging)
pub mod monitoring;

/// Integration with orcher-config crate (configuration management)
pub mod config;

/// API-to-gRPC transformation bridges
pub mod bridges;
