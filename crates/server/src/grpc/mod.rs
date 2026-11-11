//! gRPC client implementations for communicating with ORCHER services
//!
//! This module provides gRPC clients for communicating with the orchestrator service.
//! It follows the three-service architecture pattern where the gateway (server) acts
//! as a gRPC client to the orchestrator service.
//!
//! # Architecture
//!
//! ```text
//! Gateway (crates/server) → [gRPC] → Orchestrator (crates/core)
//! ```
//!
//! # Module Structure
//!
//! - `clients/` - gRPC client implementations (orchestrator, worker registry, events)
//! - `connection/` - Connection management (pooling, retry, circuit breaker)
//! - `conversions` - Type conversions between REST/GraphQL and gRPC models
//!
//! # Usage
//!
//! ```no_run
//! use orcher_server::grpc::clients::orchestrator::OrchestratorClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = OrchestratorClient::connect("http://localhost:50051").await?;
//! # Ok(())
//! # }
//! ```

/// gRPC client implementations
pub mod clients;

/// Connection management (pooling, retry, circuit breaker)
pub mod connection;

/// Type conversions between API models and gRPC protobuf
pub mod conversions;

/// Streaming utilities for gRPC streams
pub mod streaming;

/// gRPC interceptors for authentication, tracing, and metrics
pub mod interceptors;
