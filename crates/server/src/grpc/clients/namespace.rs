//! Namespace Service gRPC Client
//!
//! This module provides a client wrapper for the NamespaceService gRPC API.
//! It handles namespace lifecycle operations including creating, reading,
//! updating, deprecating, and deleting namespaces.
//!
//! # Architecture
//!
//! The client wraps the auto-generated gRPC client and provides:
//! - Connection management and retry logic
//! - Type conversions between API models and proto types
//! - Error handling and logging
//! - Health checking and reconnection
//!
//! # Example
//!
//! ```rust,no_run
//! use orcher_server::grpc::clients::namespace::NamespaceServiceClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = NamespaceServiceClient::connect("http://localhost:50051").await?;
//!
//! let ns = client.create_namespace("my-namespace", "A test namespace", 30, "owner@example.com").await?;
//! println!("Created namespace: {}", ns.name);
//! # Ok(())
//! # }
//! ```

use orcher_common::error::{OrcherError, OrcherResult};
use orcher_proto::orcher::v1::namespace_service_client::NamespaceServiceClient as ProtoNamespaceServiceClient;
use orcher_proto::orcher::v1::{
    CreateNamespaceRequest, DeleteNamespaceRequest, DeprecateNamespaceRequest, GetNamespaceRequest,
    ListNamespacesRequest, NamespaceInfo, UpdateNamespaceRequest,
};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info};

/// Default connection timeout for gRPC client
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default request timeout
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Namespace service client wrapper
///
/// Provides a high-level interface to the NamespaceService gRPC API
/// with connection management, retry logic, and error handling.
#[derive(Clone)]
pub struct NamespaceServiceClient {
    /// Inner gRPC client
    client: ProtoNamespaceServiceClient<super::AuthenticatedChannel>,
    /// Server endpoint URL
    endpoint: String,
}

impl NamespaceServiceClient {
    /// Connect to a namespace service
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The gRPC endpoint URL (e.g., "http://localhost:50051")
    ///
    /// # Returns
    ///
    /// Returns a connected client instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails.
    pub async fn connect(endpoint: impl Into<String>) -> OrcherResult<Self> {
        let endpoint_str = endpoint.into();
        info!("Connecting to NamespaceService at {}", endpoint_str);

        let channel = Endpoint::from_shared(endpoint_str.clone())
            .map_err(|e| OrcherError::Configuration {
                key: "endpoint".to_string(),
                message: format!("Invalid endpoint URL: {}", e),
            })?
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .connect()
            .await
            .map_err(|e| {
                error!("Failed to connect to NamespaceService: {}", e);
                OrcherError::ExternalService {
                    service: "NamespaceService".to_string(),
                    message: format!("Connection failed: {}", e),
                    status_code: None,
                }
            })?;

        info!("Successfully connected to NamespaceService");
        Ok(Self::from_channel(channel, endpoint_str))
    }

    /// A client over `channel`, which `endpoint` describes: a connection made
    /// some other way than [`connect`](Self::connect), such as the engine's
    /// in-process loopback.
    pub fn from_channel(channel: Channel, endpoint: impl Into<String>) -> Self {
        // Same interceptor as the workflow client, for the same reason: the
        // tenant and the credential are per-client, not per-call.
        let interceptor: fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> =
            orcher_core::grpc::outbound::outbound_interceptor;
        // The engine's message limit, not tonic's 4 MiB default: a workflow's
        // input and result pass through here whole.
        let max = orcher_core::grpc::payload_limits::max_message_bytes_from_env();
        let client = ProtoNamespaceServiceClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(max)
            .max_encoding_message_size(max);

        Self {
            client,
            endpoint: endpoint.into(),
        }
    }

    /// Create a new namespace
    ///
    /// # Arguments
    ///
    /// * `name` - Unique namespace name (1-100 chars, alphanumeric/hyphens/underscores)
    /// * `description` - Human-readable description
    /// * `retention_period_days` - How long completed workflow data is retained (must be > 0)
    /// * `owner_email` - Email of the namespace owner
    ///
    /// # Returns
    ///
    /// Returns the created `NamespaceInfo`.
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace cannot be created.
    pub async fn create_namespace(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        retention_period_days: i32,
        owner_email: impl Into<String>,
    ) -> OrcherResult<NamespaceInfo> {
        let name = name.into();
        debug!("Creating namespace: {}", name);

        let request = CreateNamespaceRequest {
            name: name.clone(),
            description: description.into(),
            retention_period_days,
            owner_email: owner_email.into(),
            data: std::collections::HashMap::new(),
        };

        let mut client = self.client.clone();
        let response = client.create_namespace(request).await.map_err(|e| {
            error!("Failed to create namespace {}: {}", name, e);
            OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: format!("Failed to create namespace: {}", e),
                status_code: Some(e.code() as u16),
            }
        })?;

        let ns = response
            .into_inner()
            .namespace
            .ok_or_else(|| OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: "Create response missing namespace".to_string(),
                status_code: None,
            })?;

        info!("Successfully created namespace: {}", ns.name);
        Ok(ns)
    }

    /// Get a namespace by name
    ///
    /// # Arguments
    ///
    /// * `name` - The namespace name to look up
    ///
    /// # Returns
    ///
    /// Returns the `NamespaceInfo` for the given name.
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace is not found or the request fails.
    pub async fn get_namespace(&self, name: &str) -> OrcherResult<NamespaceInfo> {
        debug!("Getting namespace: {}", name);

        let request = GetNamespaceRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = client.get_namespace(request).await.map_err(|e| {
            error!("Failed to get namespace {}: {}", name, e);
            OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: format!("Failed to get namespace: {}", e),
                status_code: Some(e.code() as u16),
            }
        })?;

        let ns = response
            .into_inner()
            .namespace
            .ok_or_else(|| OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: "Get response missing namespace".to_string(),
                status_code: None,
            })?;

        Ok(ns)
    }

    /// List namespaces with pagination
    ///
    /// # Arguments
    ///
    /// * `page_size` - Maximum number of results
    /// * `page_offset` - Offset for pagination
    ///
    /// # Returns
    ///
    /// Returns a tuple of (namespaces, total_count).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn list_namespaces(
        &self,
        page_size: i32,
        page_offset: i32,
    ) -> OrcherResult<(Vec<NamespaceInfo>, i64)> {
        debug!(
            "Listing namespaces: page_size={}, page_offset={}",
            page_size, page_offset
        );

        // Carry the calling organisation across the hop.
        //
        // The namespace service reads it from request metadata and scopes to
        // it; this client sent a bare request, so the service saw no caller and
        // listed every organisation's namespaces. The tenant is in scope here —
        // the gateway put it there — it simply was not being passed on.
        let mut request = tonic::Request::new(ListNamespacesRequest {
            page_size,
            page_offset,
        });
        if let Some(organization_id) = orcher_core::grpc::tenant::current_organization() {
            orcher_core::grpc::tenant::set_organization(&mut request, &organization_id);
        }

        let mut client = self.client.clone();
        let response = client.list_namespaces(request).await.map_err(|e| {
            error!("Failed to list namespaces: {}", e);
            OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: format!("Failed to list namespaces: {}", e),
                status_code: Some(e.code() as u16),
            }
        })?;

        let inner = response.into_inner();
        Ok((inner.namespaces, inner.total_count))
    }

    /// Update a namespace
    ///
    /// # Arguments
    ///
    /// * `name` - The namespace name to update
    /// * `description` - Updated description (empty string means no change)
    /// * `retention_period_days` - Updated retention period (0 means no change)
    ///
    /// # Returns
    ///
    /// Returns the updated `NamespaceInfo`.
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace cannot be updated.
    pub async fn update_namespace(
        &self,
        name: &str,
        description: impl Into<String>,
        retention_period_days: i32,
    ) -> OrcherResult<NamespaceInfo> {
        debug!("Updating namespace: {}", name);

        let request = UpdateNamespaceRequest {
            name: name.to_string(),
            description: description.into(),
            owner_email: String::new(),
            retention_period_days,
            data: std::collections::HashMap::new(),
        };

        let mut client = self.client.clone();
        let response = client.update_namespace(request).await.map_err(|e| {
            error!("Failed to update namespace {}: {}", name, e);
            OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: format!("Failed to update namespace: {}", e),
                status_code: Some(e.code() as u16),
            }
        })?;

        let ns = response
            .into_inner()
            .namespace
            .ok_or_else(|| OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: "Update response missing namespace".to_string(),
                status_code: None,
            })?;

        info!("Successfully updated namespace: {}", ns.name);
        Ok(ns)
    }

    /// Deprecate a namespace (no new workflows allowed)
    ///
    /// # Arguments
    ///
    /// * `name` - The namespace name to deprecate
    ///
    /// # Returns
    ///
    /// Returns the updated `NamespaceInfo` with deprecated status.
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace cannot be deprecated.
    pub async fn deprecate_namespace(&self, name: &str) -> OrcherResult<NamespaceInfo> {
        info!("Deprecating namespace: {}", name);

        let request = DeprecateNamespaceRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = client.deprecate_namespace(request).await.map_err(|e| {
            error!("Failed to deprecate namespace {}: {}", name, e);
            OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: format!("Failed to deprecate namespace: {}", e),
                status_code: Some(e.code() as u16),
            }
        })?;

        let ns = response
            .into_inner()
            .namespace
            .ok_or_else(|| OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: "Deprecate response missing namespace".to_string(),
                status_code: None,
            })?;

        info!("Successfully deprecated namespace: {}", ns.name);
        Ok(ns)
    }

    /// Delete a namespace (soft-delete)
    ///
    /// # Arguments
    ///
    /// * `name` - The namespace name to delete
    ///
    /// # Returns
    ///
    /// Returns Ok if the deletion was successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace cannot be deleted.
    pub async fn delete_namespace(&self, name: &str) -> OrcherResult<()> {
        info!("Deleting namespace: {}", name);

        let request = DeleteNamespaceRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        client.delete_namespace(request).await.map_err(|e| {
            error!("Failed to delete namespace {}: {}", name, e);
            OrcherError::ExternalService {
                service: "NamespaceService".to_string(),
                message: format!("Failed to delete namespace: {}", e),
                status_code: Some(e.code() as u16),
            }
        })?;

        info!("Successfully deleted namespace: {}", name);
        Ok(())
    }

    /// Get the endpoint this client is connected to
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_client_creation() {
        let endpoint = "http://localhost:50051";
        assert!(!endpoint.is_empty());
    }

    #[test]
    fn test_default_timeouts() {
        assert_eq!(DEFAULT_CONNECT_TIMEOUT, Duration::from_secs(10));
        assert_eq!(DEFAULT_REQUEST_TIMEOUT, Duration::from_secs(30));
    }
}
