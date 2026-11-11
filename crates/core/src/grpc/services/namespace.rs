//! Namespace management gRPC service.

use orcher_proto::orcher::v1::{
    namespace_service_server::{NamespaceService as NamespaceServiceTrait, NamespaceServiceServer},
    CreateNamespaceRequest, CreateNamespaceResponse, DeleteNamespaceRequest,
    DeleteNamespaceResponse, DeprecateNamespaceRequest, DeprecateNamespaceResponse,
    GetNamespaceRequest, GetNamespaceResponse, ListNamespacesRequest, ListNamespacesResponse,
    NamespaceInfo, UpdateNamespaceRequest, UpdateNamespaceResponse,
};
use orcher_proto::tonic::{Request, Response, Status};
use orcher_storage::repositories::domains::namespace::NamespaceRepository;
use orcher_storage::schema::types::Namespace;
use orcher_storage::traits::domain::NamespaceRepositoryTrait;
use std::sync::Arc;
use tracing::{debug, info};

use crate::grpc::tenant::organization_from_metadata;

/// NamespaceServiceImpl gRPC implementation
///
/// Handles CRUD operations for namespace management.
///
/// Every operation is scoped to the calling organisation when the request
/// names one. A namespace created by a tenant belongs to that tenant, and a
/// tenant listing or reading namespaces sees only its own. Without a tenant —
/// a single-tenant deployment — nothing is scoped and behaviour is as it was.
///
/// This is the same rule the workflow and execution services apply. It was
/// missing here, so a namespace created under one organisation was created
/// under none, and listed for everyone.
pub struct NamespaceServiceImpl {
    namespace_repository: Arc<NamespaceRepository>,
}

impl NamespaceServiceImpl {
    /// Create a new NamespaceServiceImpl
    pub fn new(namespace_repository: Arc<NamespaceRepository>) -> Self {
        Self {
            namespace_repository,
        }
    }

    /// Create a gRPC server for this service
    pub fn into_server(self) -> NamespaceServiceServer<Self> {
        NamespaceServiceServer::new(self)
    }
}

/// Validate a namespace name format
#[allow(clippy::result_large_err)]
fn validate_namespace_name(name: &str) -> Result<(), Status> {
    if name.is_empty() || name.len() > 100 {
        return Err(Status::invalid_argument(
            "Namespace name must be between 1 and 100 characters",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(Status::invalid_argument(
            "Namespace name can only contain alphanumeric characters, hyphens, and underscores",
        ));
    }
    Ok(())
}

/// Convert a storage Namespace to a proto NamespaceInfo
fn namespace_to_info(ns: &Namespace) -> NamespaceInfo {
    NamespaceInfo {
        name: ns.name.clone(),
        status: ns.status.clone(),
        description: ns.description.clone(),
        owner_email: ns.owner_email.clone().unwrap_or_default(),
        retention_period_days: ns.retention_period_days,
        data: serde_json::from_value::<std::collections::HashMap<String, String>>(ns.data.clone())
            .unwrap_or_default(),
        created_at: Some(prost_types::Timestamp {
            seconds: ns.created_at.timestamp(),
            nanos: ns.created_at.timestamp_subsec_nanos() as i32,
        }),
        updated_at: Some(prost_types::Timestamp {
            seconds: ns.updated_at.timestamp(),
            nanos: ns.updated_at.timestamp_subsec_nanos() as i32,
        }),
    }
}

#[orcher_proto::tonic::async_trait]
impl NamespaceServiceTrait for NamespaceServiceImpl {
    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        let organization_id = organization_from_metadata(request.metadata());
        let req = request.into_inner();
        debug!("CreateNamespace: name={}", req.name);

        validate_namespace_name(&req.name)?;

        if req.retention_period_days <= 0 {
            return Err(Status::invalid_argument(
                "Retention period must be a positive number of days",
            ));
        }

        // Unique within the organisation, not globally: two tenants may each
        // have a `development`.
        if self
            .namespace_repository
            .exists(&req.name, organization_id.as_ref())
            .await
            .map_err(|e| Status::internal(format!("Failed to check namespace: {}", e)))?
        {
            return Err(Status::already_exists(format!(
                "Namespace '{}' already exists",
                req.name
            )));
        }

        let mut ns = Namespace::new(req.name.clone(), req.retention_period_days);
        ns.organization_id = organization_id;
        ns.description = req.description;
        ns.owner_email = if req.owner_email.is_empty() {
            None
        } else {
            Some(req.owner_email)
        };
        if !req.data.is_empty() {
            ns.data = serde_json::to_value(&req.data)
                .map_err(|e| Status::internal(format!("Failed to serialize data: {}", e)))?;
        }

        let created = self
            .namespace_repository
            .create(ns)
            .await
            .map_err(|e| Status::internal(format!("Failed to create namespace: {}", e)))?;

        info!("Namespace created: name={}", req.name);

        Ok(Response::new(CreateNamespaceResponse {
            namespace: Some(namespace_to_info(&created)),
        }))
    }

    async fn get_namespace(
        &self,
        request: Request<GetNamespaceRequest>,
    ) -> Result<Response<GetNamespaceResponse>, Status> {
        let organization_id = organization_from_metadata(request.metadata());
        let req = request.into_inner();
        debug!("GetNamespace: name={}", req.name);

        let ns = self
            .namespace_repository
            .get_by_name(&req.name, organization_id.as_ref())
            .await
            .map_err(|e| Status::internal(format!("Failed to get namespace: {}", e)))?
            .ok_or_else(|| Status::not_found(format!("Namespace '{}' not found", req.name)))?;

        Ok(Response::new(GetNamespaceResponse {
            namespace: Some(namespace_to_info(&ns)),
        }))
    }

    async fn list_namespaces(
        &self,
        request: Request<ListNamespacesRequest>,
    ) -> Result<Response<ListNamespacesResponse>, Status> {
        let organization_id = organization_from_metadata(request.metadata());
        let req = request.into_inner();
        let limit = if req.page_size > 0 {
            req.page_size as i64
        } else {
            100
        };
        let offset = req.page_offset.max(0) as i64;

        debug!("ListNamespaces: limit={}, offset={}", limit, offset);

        let namespaces = self
            .namespace_repository
            .list(organization_id.as_ref(), limit, offset)
            .await
            .map_err(|e| Status::internal(format!("Failed to list namespaces: {}", e)))?;

        let total_count = self
            .namespace_repository
            .count(organization_id.as_ref())
            .await
            .map_err(|e| Status::internal(format!("Failed to count namespaces: {}", e)))?;

        Ok(Response::new(ListNamespacesResponse {
            namespaces: namespaces.iter().map(namespace_to_info).collect(),
            total_count,
        }))
    }

    async fn update_namespace(
        &self,
        request: Request<UpdateNamespaceRequest>,
    ) -> Result<Response<UpdateNamespaceResponse>, Status> {
        let organization_id = organization_from_metadata(request.metadata());
        let req = request.into_inner();
        debug!("UpdateNamespace: name={}", req.name);

        let mut ns = self
            .namespace_repository
            .get_by_name(&req.name, organization_id.as_ref())
            .await
            .map_err(|e| Status::internal(format!("Failed to get namespace: {}", e)))?
            .ok_or_else(|| Status::not_found(format!("Namespace '{}' not found", req.name)))?;

        // Apply updates (empty string = no change for string fields, 0 = no change for int)
        if !req.description.is_empty() {
            ns.description = req.description;
        }
        if !req.owner_email.is_empty() {
            ns.owner_email = Some(req.owner_email);
        }
        if req.retention_period_days > 0 {
            ns.retention_period_days = req.retention_period_days;
        }
        if !req.data.is_empty() {
            ns.data = serde_json::to_value(&req.data)
                .map_err(|e| Status::internal(format!("Failed to serialize data: {}", e)))?;
        }

        let updated = self
            .namespace_repository
            .update(ns)
            .await
            .map_err(|e| Status::internal(format!("Failed to update namespace: {}", e)))?;

        info!("Namespace updated: name={}", req.name);

        Ok(Response::new(UpdateNamespaceResponse {
            namespace: Some(namespace_to_info(&updated)),
        }))
    }

    async fn deprecate_namespace(
        &self,
        request: Request<DeprecateNamespaceRequest>,
    ) -> Result<Response<DeprecateNamespaceResponse>, Status> {
        let organization_id = organization_from_metadata(request.metadata());
        let req = request.into_inner();
        debug!("DeprecateNamespace: name={}", req.name);

        // Verify it exists
        let ns = self
            .namespace_repository
            .get_by_name(&req.name, organization_id.as_ref())
            .await
            .map_err(|e| Status::internal(format!("Failed to get namespace: {}", e)))?
            .ok_or_else(|| Status::not_found(format!("Namespace '{}' not found", req.name)))?;

        self.namespace_repository
            .update_status(&req.name, organization_id.as_ref(), "deprecated")
            .await
            .map_err(|e| Status::internal(format!("Failed to deprecate namespace: {}", e)))?;

        info!("Namespace deprecated: name={}", req.name);

        let mut info = namespace_to_info(&ns);
        info.status = "deprecated".to_string();

        Ok(Response::new(DeprecateNamespaceResponse {
            namespace: Some(info),
        }))
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let organization_id = organization_from_metadata(request.metadata());
        let req = request.into_inner();
        debug!("DeleteNamespace: name={}", req.name);

        // Verify it exists
        self.namespace_repository
            .get_by_name(&req.name, organization_id.as_ref())
            .await
            .map_err(|e| Status::internal(format!("Failed to get namespace: {}", e)))?
            .ok_or_else(|| Status::not_found(format!("Namespace '{}' not found", req.name)))?;

        // Prevent deleting the default namespace
        if req.name == "default" {
            return Err(Status::failed_precondition(
                "Cannot delete the default namespace",
            ));
        }

        self.namespace_repository
            .delete(&req.name, organization_id.as_ref())
            .await
            .map_err(|e| Status::internal(format!("Failed to delete namespace: {}", e)))?;

        info!("Namespace deleted: name={}", req.name);

        Ok(Response::new(DeleteNamespaceResponse {}))
    }
}
