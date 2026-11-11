//! API-to-gRPC Transformation Bridges
//!
//! This module provides transformation layers that convert API protocol-specific
//! requests and responses into gRPC messages and vice versa. These bridges enable
//! the gateway to accept multiple API protocols (REST, GraphQL, WebSocket) while
//! maintaining a consistent gRPC communication layer with backend services.
//!
//! # Bridge Architecture
//!
//! Each bridge handles bidirectional transformation:
//!
//! ```text
//! ┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
//! │  REST API   │────▶│  REST Bridge │────▶│  gRPC Client    │
//! │  Request    │     │  (Transform) │     │  (Orchestrator) │
//! └─────────────┘     └──────────────┘     └─────────────────┘
//!                            │
//!                            ▼
//!                     ┌──────────────┐
//!                     │ REST Response│
//!                     └──────────────┘
//! ```
//!
//! # Bridge Modules
//!
//! - **REST Bridge**: Transforms REST HTTP requests/responses to/from gRPC messages
//! - **GraphQL Bridge**: Transforms GraphQL queries/mutations to gRPC calls
//! - **WebSocket Bridge**: Bridges WebSocket bidirectional streams to gRPC streaming
//!
//! # Design Principles
//!
//! 1. **Protocol Independence**: Backend services (orchestrator) are protocol-agnostic
//! 2. **Type Safety**: Strong typing ensures correct transformations
//! 3. **Error Handling**: Protocol-specific errors are mapped to appropriate gRPC errors
//! 4. **Streaming Support**: Handles both unary and streaming gRPC calls
//! 5. **Async/Await**: All transformations are async for efficiency

/// REST API to gRPC transformation bridge
pub mod rest;

/// GraphQL to gRPC transformation bridge
pub mod graphql;

// WebSocket bridge disabled in open source (requires cloud auth)
