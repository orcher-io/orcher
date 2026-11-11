//! Type conversions between API models and gRPC protobuf types
//!
//! This module provides conversion functions between REST/GraphQL API models
//! and gRPC protobuf types used for communicating with the orchestrator service.

// NOTE: Legacy proto conversions (TaskId/WorkerId struct wrappers) removed.
// The modern execution service in orcher-proto uses string IDs directly.
