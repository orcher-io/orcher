//! The engine's REST API.
//!
//! The `orchestrator` binary serves it on its HTTP address. Its handlers call
//! the engine's gRPC services through gRPC clients, over an in-process
//! connection that passes the same authentication as the gRPC port, so a REST
//! caller is granted exactly what the same credential is granted over gRPC.

#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
// Allow missing docs in generated proto code
#![allow(missing_docs)]

/// Main HTTP server implementation
pub mod server;

/// The REST API
pub mod api;

/// gRPC client implementations for communicating with services
pub mod grpc;

/// Authentication provider trait (extensible by cloud/enterprise)
pub mod auth;

pub mod security;

/// Error types for the server crate
pub mod error;

/// Type definitions and common types
pub mod types;

/// Middleware stack for API gateway (auth, rate limiting, security, etc.)
pub mod middleware;

/// Business logic service layer (thin layer that calls gRPC clients)
pub mod services;

/// Application state management (dependency injection for services)
pub mod state;

/// Integration with other ORCHER crates and API-to-gRPC bridges
pub mod integration;

/// Configuration management for server components
pub mod config;
