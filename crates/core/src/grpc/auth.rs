//! The gRPC services' permission checks, on the caller the auth layer
//! attached to the request. Who the caller is is decided by the
//! [`Authenticator`](crate::auth::Authenticator).

use super::service_auth::ServiceIdentity;
use tonic::Status;

/// Check operation permission.
#[allow(clippy::result_large_err)]
pub fn check_operation(
    identity: &ServiceIdentity,
    operation: super::service_auth::ServiceOperation,
) -> Result<(), Status> {
    if operation.check_permission(&identity.permissions) {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "Service '{}' does not have permission for '{}'",
            identity.service_name,
            operation.as_str()
        )))
    }
}

/// Check task queue access.
#[allow(clippy::result_large_err)]
pub fn check_task_queue(identity: &ServiceIdentity, task_queue: &str) -> Result<(), Status> {
    if identity.can_access_task_queue(task_queue) {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "Service '{}' is not authorized for task queue '{}'",
            identity.service_name, task_queue
        )))
    }
}

/// Check namespace isolation.
///
/// An identity with an empty namespace is not pinned to one — the shape a
/// credential issued to an organisation has, since an organisation holds many
/// namespaces. Which namespaces such a caller may reach is decided where the
/// namespace is resolved, against the caller's organisation, not here.
#[allow(clippy::result_large_err)]
pub fn check_namespace(identity: &ServiceIdentity, namespace: &str) -> Result<(), Status> {
    if identity.namespace.is_empty() || identity.namespace == namespace {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "Service '{}' belongs to namespace '{}', cannot access '{}'",
            identity.service_name, identity.namespace, namespace
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::service_auth::ServicePermissions;

    fn identity_in(namespace: &str) -> ServiceIdentity {
        ServiceIdentity::new(
            "svc".into(),
            namespace.into(),
            Vec::new(),
            ServicePermissions::default(),
        )
    }

    #[test]
    fn a_namespaced_identity_is_held_to_its_namespace() {
        let identity = identity_in("payments");
        assert!(check_namespace(&identity, "payments").is_ok());
        assert!(check_namespace(&identity, "billing").is_err());
    }

    #[test]
    fn an_identity_without_a_namespace_is_not_refused_by_this_check() {
        // Scoping for such an identity happens where the namespace is
        // resolved against its organisation.
        let identity = identity_in("");
        assert!(check_namespace(&identity, "payments").is_ok());
        assert!(check_namespace(&identity, "billing").is_ok());
    }
}
