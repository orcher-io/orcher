//! Tower auth middleware layer for gRPC.

use crate::auth::{organization_for, AuthError, Authenticator, Credential, Tenancy};
use http::Response;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tower::{Layer, Service};
use tracing::debug;

/// Which organisation a request acts for, given what the credential says and
/// what the caller claimed.
///
/// The credential wins. A caller may repeat it, or say nothing, but may not
/// name a different organisation: that is not a request to be scoped
/// differently, it is a request to act as someone else, and is refused
/// rather than quietly corrected.
///
/// A credential with no organisation may name one only if it holds
/// [`can_act_for_any_organization`](crate::grpc::service_auth::ServicePermissions::can_act_for_any_organization)
/// — the trusted edge that has already resolved the tenant. Before, any
/// organisation-less key could, so one key stored without an owner was a key
/// to every tenant. Without the capability and without a claim, the request
/// is unscoped: the single-tenant case. The validator refuses such a key on a
/// deployment that holds organisations, so an unscoped request
/// from an ownerless key only ever happens where there is one tenant.
#[allow(clippy::result_large_err)]
pub fn effective_organization(
    from_credential: Option<uuid::Uuid>,
    may_delegate: bool,
    claimed: Option<uuid::Uuid>,
) -> Result<Option<uuid::Uuid>, tonic::Status> {
    match (from_credential, claimed) {
        (Some(credential), Some(claim)) if credential != claim => Err(
            tonic::Status::permission_denied("API key belongs to a different organization"),
        ),
        (Some(credential), _) => Ok(Some(credential)),
        (None, Some(_)) if !may_delegate => Err(tonic::Status::permission_denied(
            "API key is not permitted to act for an organization",
        )),
        (None, claim) => Ok(claim),
    }
}

/// Per-request tenant context extracted from the tenant header (see `tenant`).
///
/// Inserted into request extensions by `AuthLayer` regardless of auth mode,
/// allowing gRPC handlers to scope storage queries to a specific organization.
#[derive(Clone, Debug)]
pub struct TenantContext {
    /// Organization UUID parsed from the tenant header, or `None` if absent/invalid.
    pub organization_id: Option<uuid::Uuid>,
}

/// Authentication mode for the gRPC server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMode {
    /// No authentication (development mode)
    None,
    /// Require API key in authorization header
    ApiKey,
    /// Require mTLS client certificate (TLS-level enforcement)
    Mtls,
    /// Require both API key and mTLS
    ApiKeyAndMtls,
    /// Require a token the configured OpenID Connect provider issued
    Oidc,
}

impl Default for AuthMode {
    fn default() -> Self {
        AuthMode::None
    }
}

/// Tower Layer that wraps services with authentication.
#[derive(Clone)]
pub struct AuthLayer {
    authenticator: Arc<dyn Authenticator>,
    mode: AuthMode,
    tenancy: Option<Tenancy>,
}

impl AuthLayer {
    /// Create a new auth layer.
    pub fn new(authenticator: Arc<dyn Authenticator>, mode: AuthMode) -> Self {
        Self {
            authenticator,
            mode,
            tenancy: None,
        }
    }

    /// Resolve each caller's organisation for a deployment of `tenancy`
    /// ([`organization_for`]). Undeclared, the authenticator decides.
    pub fn with_tenancy(mut self, tenancy: Option<Tenancy>) -> Self {
        self.tenancy = tenancy;
        self
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            authenticator: self.authenticator.clone(),
            mode: self.mode.clone(),
            tenancy: self.tenancy,
        }
    }
}

/// Tower Service that performs authentication on each request.
#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    authenticator: Arc<dyn Authenticator>,
    mode: AuthMode,
    tenancy: Option<Tenancy>,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

impl<S, ReqBody> Service<http::Request<ReqBody>> for AuthService<S>
where
    S: Service<http::Request<ReqBody>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // Read the tenant header for all modes (borrowed before req is moved).
        // Which header that is lives in one place, `tenant`, shared with the
        // service handlers.
        let org_id: Option<uuid::Uuid> =
            crate::grpc::tenant::organization_from_headers(req.headers());

        // In None mode, pass through with tenant context only
        if self.mode == AuthMode::None {
            let mut req = req;
            req.extensions_mut().insert(TenantContext {
                organization_id: org_id,
            });
            let future = self.inner.call(req);
            return Box::pin(future);
        }

        let requires_api_key = matches!(
            self.mode,
            AuthMode::ApiKey | AuthMode::ApiKeyAndMtls | AuthMode::Oidc
        );

        if requires_api_key {
            // Extract the authorization header
            let api_key = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(|v| {
                    // Strip "Bearer " prefix if present
                    v.strip_prefix("Bearer ").unwrap_or(v).to_string()
                });

            match api_key {
                Some(key) => {
                    let authenticator = self.authenticator.clone();
                    let tenancy = self.tenancy;
                    let method = req.uri().path().to_string();
                    let mut inner = self.inner.clone();

                    Box::pin(async move {
                        // Validate the API key
                        match authenticator.authenticate(Credential::Bearer(&key)).await {
                            Ok(principal) => {
                                // What the call needs, against what the
                                // credential grants. A method the engine does
                                // not know needs a scope nothing has.
                                match crate::auth::scope::for_grpc_method(&method) {
                                    Some(scope) if principal.scopes.allows(scope) => {}
                                    Some(scope) => {
                                        return Ok(tonic::Status::permission_denied(format!(
                                            "The credential does not grant the '{scope}' scope \
                                             this call needs"
                                        ))
                                        .into_http());
                                    }
                                    None => {
                                        return Ok(tonic::Status::permission_denied(format!(
                                            "{method} is not open to credentials"
                                        ))
                                        .into_http());
                                    }
                                }
                                let identity = principal.service_identity();
                                debug!(
                                    service_id = %identity.service_id,
                                    service_name = %identity.service_name,
                                    namespace = %identity.namespace,
                                    "Request authenticated"
                                );

                                let organization_id =
                                    match organization_for(&principal, org_id, tenancy) {
                                        Ok(organization_id) => organization_id,
                                        Err(error) => {
                                            return Ok(tonic::Status::permission_denied(
                                                error.to_string(),
                                            )
                                            .into_http())
                                        }
                                    };

                                let mut req = req;
                                // The handlers read the tenant from the
                                // request metadata. Writing the authenticated
                                // organisation there means they need no second
                                // path for "what the credential says".
                                if let Some(organization_id) = organization_id {
                                    if let Ok(value) = organization_id.to_string().parse() {
                                        req.headers_mut().insert(
                                            crate::grpc::tenant::ORGANIZATION_METADATA_KEY,
                                            value,
                                        );
                                    }
                                }
                                req.extensions_mut().insert(identity);
                                req.extensions_mut().insert(principal);
                                req.extensions_mut()
                                    .insert(TenantContext { organization_id });

                                // Call the inner service
                                inner.call(req).await
                            }
                            Err(AuthError::Forbidden(message)) => {
                                Ok(tonic::Status::permission_denied(message).into_http())
                            }
                            Err(AuthError::Unauthenticated(message)) => {
                                Ok(tonic::Status::unauthenticated(format!(
                                    "Authentication failed: {message}"
                                ))
                                .into_http())
                            }
                        }
                    })
                }
                None => {
                    // No authorization header — return immediate error
                    let status = tonic::Status::unauthenticated(
                        "Missing authorization header. Provide an API key via 'Authorization: Bearer <key>'",
                    );
                    Box::pin(async move { Ok(status.into_http()) })
                }
            }
        } else {
            // mTLS-only mode: TLS layer handles cert validation,
            // we just pass through at the application layer with tenant context
            let mut req = req;
            req.extensions_mut().insert(TenantContext {
                organization_id: org_id,
            });
            let future = self.inner.call(req);
            Box::pin(future)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::effective_organization;
    use uuid::Uuid;

    #[test]
    fn the_credential_decides_the_organisation() {
        let org = Uuid::new_v4();
        assert_eq!(
            effective_organization(Some(org), false, None).unwrap(),
            Some(org)
        );
        assert_eq!(
            effective_organization(Some(org), false, Some(org)).unwrap(),
            Some(org)
        );
    }

    #[test]
    fn claiming_another_organisation_is_refused_not_corrected() {
        let status =
            effective_organization(Some(Uuid::new_v4()), false, Some(Uuid::new_v4())).unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    /// Delegation lets an organisation-less credential act *as* a tenant; it
    /// does not let an organisation's own key escape to another one.
    #[test]
    fn delegation_does_not_free_a_key_bound_to_an_organisation() {
        let status =
            effective_organization(Some(Uuid::new_v4()), true, Some(Uuid::new_v4())).unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    /// The trusted edge: it has resolved the tenant and says which.
    #[test]
    fn a_delegating_credential_acts_for_the_organisation_it_names() {
        let claim = Uuid::new_v4();
        assert_eq!(
            effective_organization(None, true, Some(claim)).unwrap(),
            Some(claim)
        );
    }

    /// The hole this closes. An organisation-less key used to be able to name
    /// any organisation, so one key stored without an owner could act as
    /// every tenant on the deployment.
    #[test]
    fn an_ownerless_key_without_delegation_cannot_name_an_organisation() {
        let status = effective_organization(None, false, Some(Uuid::new_v4())).unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    /// A deployment without tenancy: no owner, no claim, unscoped — unchanged.
    #[test]
    fn without_tenancy_the_request_stays_unscoped() {
        assert_eq!(effective_organization(None, false, None).unwrap(), None);
        assert_eq!(effective_organization(None, true, None).unwrap(), None);
    }
}
