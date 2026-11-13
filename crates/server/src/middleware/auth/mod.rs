//! Authentication middleware.

pub mod context;
pub mod errors;

// Re-export AuthenticatedUser from crate::auth (canonical location)
pub use crate::auth::{AuthMethod, AuthenticatedUser};
pub use errors::AuthError;

// Axum layer middleware
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

/// Authentication middleware.
///
/// With an [`Authenticator`](orcher_core::auth::Authenticator) set on the
/// state, the caller is identified with it, as the gRPC API identifies one:
/// see [`authenticate_with`]. Otherwise with the pluggable
/// [`AuthProvider`](crate::auth::AuthProvider). Either way the request carries
/// the caller as a [`Principal`](orcher_core::auth::Principal) and as an
/// [`AuthenticatedUser`].
pub async fn require_authentication(mut request: Request, next: Next) -> Response {
    use axum::http::header;
    use axum::response::IntoResponse;
    use tracing::{debug, warn};

    let state = if let Some(state) = request.extensions().get::<crate::state::AppState>() {
        state.clone()
    } else {
        warn!("AppState not found in request extensions");
        return crate::error::ServerError::internal(
            "Server configuration error: AppState not available",
        )
        .into_response();
    };

    if let Some(authenticator) = state.authenticator() {
        return authenticate_with(authenticator.as_ref(), state.tenancy(), request, next).await;
    }

    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|auth| auth.strip_prefix("Bearer "));

    if let Some(token_str) = token {
        match state.auth_provider().authenticate(token_str).await {
            Ok(user) => {
                debug!("Authentication successful for user: {}", user.user_id);
                if user.is_near_expiry() {
                    warn!("Token near expiry for user: {}", user.user_id);
                }
                request.extensions_mut().insert(principal_of(&user));
                request.extensions_mut().insert(user);
                next.run(request).await
            }
            Err(e) => {
                warn!("Authentication failed: {}", e);
                match e {
                    crate::auth::AuthError::TokenExpired => {
                        crate::error::ServerError::token_expired()
                    }
                    crate::auth::AuthError::MissingToken => {
                        crate::error::ServerError::missing_auth_token()
                    }
                    crate::auth::AuthError::InvalidToken(msg) => {
                        crate::error::ServerError::unauthorized(&msg)
                    }
                    crate::auth::AuthError::Internal(msg) => {
                        crate::error::ServerError::internal(msg)
                    }
                }
                .into_response()
            }
        }
    } else {
        match state.auth_provider().authenticate("").await {
            Ok(user) => {
                request.extensions_mut().insert(principal_of(&user));
                request.extensions_mut().insert(user);
                next.run(request).await
            }
            Err(_) => {
                warn!("Missing authentication token");
                crate::error::ServerError::missing_auth_token().into_response()
            }
        }
    }
}

/// The caller an [`AuthProvider`](crate::auth::AuthProvider) identified, as a
/// principal: a user, with no organisation of its own (a gateway that knows
/// it scopes the request itself) and the full permissions REST callers had.
fn principal_of(user: &AuthenticatedUser) -> orcher_core::auth::Principal {
    orcher_core::auth::Principal {
        subject: user.user_id.to_string(),
        name: user.user_id.to_string(),
        ..orcher_core::auth::Principal::anonymous()
    }
}

/// Identify the caller with `authenticator`, as the gRPC API does: a bearer
/// token is the credential, and the request acts for the credential's
/// organisation, or for the one the caller names when it may name one, on a
/// deployment of `tenancy` ([`organization_for`](orcher_core::auth::organization_for)).
/// A request that resolves to no organisation is left in whatever scope the
/// embedding application set.
async fn authenticate_with(
    authenticator: &dyn orcher_core::auth::Authenticator,
    tenancy: Option<orcher_core::auth::Tenancy>,
    mut request: Request,
    next: Next,
) -> Response {
    use axum::http::header;
    use axum::response::IntoResponse;
    use orcher_core::auth::{AuthError, Credential};

    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_string);
    let credential = token
        .as_deref()
        .map_or(Credential::None, Credential::Bearer);
    let principal = match authenticator.authenticate(credential).await {
        Ok(principal) => principal,
        Err(AuthError::Unauthenticated(_)) if token.is_none() => {
            return crate::error::ServerError::missing_auth_token().into_response();
        }
        Err(AuthError::Unauthenticated(message)) => {
            return crate::error::ServerError::unauthorized(message).into_response();
        }
        Err(AuthError::Forbidden(message)) => {
            return crate::error::ServerError::forbidden(message).into_response();
        }
    };

    let claimed = orcher_core::grpc::tenant::organization_from_headers(request.headers());
    let organization_id = match orcher_core::auth::organization_for(&principal, claimed, tenancy) {
        Ok(organization_id) => organization_id,
        Err(error) => {
            return crate::error::ServerError::forbidden(error).into_response();
        }
    };

    let scope = orcher_core::auth::scope::for_rest(request.method(), request.uri().path());
    if !principal.scopes.allows(scope) {
        return crate::error::ServerError::forbidden(format!(
            "The credential does not grant the '{scope}' scope this call needs"
        ))
        .into_response();
    }

    let mut user = AuthenticatedUser::anonymous();
    if principal != orcher_core::auth::Principal::anonymous() {
        user.auth_method = AuthMethod::ApiKey;
    }
    request.extensions_mut().insert(user);
    request.extensions_mut().insert(principal);
    // The calls this request makes to the engine carry the caller's own
    // credential, so the engine grants them what it grants the caller.
    let run = async move {
        match token {
            Some(token) => {
                orcher_core::grpc::outbound::with_caller_credential(token, next.run(request)).await
            }
            None => next.run(request).await,
        }
    };
    match organization_id {
        Some(organization_id) => {
            orcher_core::grpc::tenant::with_organization(Some(organization_id), run).await
        }
        None => run.await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use axum::routing::get;
    use axum::{Extension, Router};
    use orcher_core::auth::{
        AuthError, Authenticator, Credential, NoAuthenticator, Principal, Tenancy,
    };
    use std::sync::Arc;
    use tower::ServiceExt;
    use uuid::Uuid;

    /// `key-a` is organisation A's key, `key-edge` the trusted edge's, and
    /// `key-unbound` may not act; anything else is unknown.
    struct Keys {
        a: Uuid,
    }

    #[async_trait]
    impl Authenticator for Keys {
        async fn authenticate(&self, credential: Credential<'_>) -> Result<Principal, AuthError> {
            let principal = |organization_id, can_act_for_any_organization| Principal {
                subject: "key".into(),
                name: "key".into(),
                organization_id,
                can_act_for_any_organization,
                ..Principal::anonymous()
            };
            match credential {
                Credential::Bearer("key-a") => Ok(principal(Some(self.a), false)),
                Credential::Bearer("key-edge") => Ok(principal(None, true)),
                Credential::Bearer("key-unbound") => {
                    Err(AuthError::Forbidden("not bound to an organization".into()))
                }
                Credential::Bearer("key-ownerless") => Ok(principal(None, false)),
                Credential::Bearer("key-reader") => Ok(Principal {
                    scopes: orcher_core::auth::Scopes::parse(["workflows:read"]).0,
                    ..principal(Some(self.a), false)
                }),
                _ => Err(AuthError::Unauthenticated("invalid API key".into())),
            }
        }
    }

    /// A route answering with the organisation the request acts for and
    /// whether it saw the caller.
    fn app(authenticator: Arc<dyn Authenticator>) -> Router {
        app_of(authenticator, None)
    }

    fn app_of(authenticator: Arc<dyn Authenticator>, tenancy: Option<Tenancy>) -> Router {
        Router::new()
            .route(
                "/",
                get(
                    |Extension(principal): Extension<Principal>,
                     Extension(_user): Extension<AuthenticatedUser>| async move {
                        format!(
                            "{}|{}",
                            principal.subject,
                            orcher_core::grpc::tenant::current_organization()
                                .map(|o| o.to_string())
                                .unwrap_or_default()
                        )
                    },
                ),
            )
            .route(
                "/workflows/x/cancel",
                axum::routing::post(|| async { "cancelled" }),
            )
            .layer(axum::middleware::from_fn(move |request, next| {
                let authenticator = authenticator.clone();
                async move { authenticate_with(authenticator.as_ref(), tenancy, request, next).await }
            }))
    }

    async fn call(app: &Router, token: Option<&str>, claim: Option<Uuid>) -> (StatusCode, String) {
        let mut request = HttpRequest::builder().uri("/");
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        if let Some(claim) = claim {
            request = request.header("x-organization-id", claim.to_string());
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn without_authentication_every_caller_is_served_unscoped() {
        let app = app(Arc::new(NoAuthenticator));
        assert_eq!(
            call(&app, None, None).await,
            (StatusCode::OK, "anonymous|".into())
        );
        assert_eq!(
            call(&app, Some("anything"), None).await,
            (StatusCode::OK, "anonymous|".into())
        );
    }

    #[tokio::test]
    async fn a_key_acts_for_its_own_organisation_and_no_other() {
        let a = Uuid::new_v4();
        let app = app(Arc::new(Keys { a }));
        assert_eq!(
            call(&app, Some("key-a"), None).await,
            (StatusCode::OK, format!("key|{a}"))
        );
        assert_eq!(
            call(&app, Some("key-a"), Some(a)).await,
            (StatusCode::OK, format!("key|{a}"))
        );
        assert_eq!(
            call(&app, Some("key-a"), Some(Uuid::new_v4())).await.0,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn the_trusted_edge_acts_for_the_organisation_it_names() {
        let app = app(Arc::new(Keys { a: Uuid::new_v4() }));
        let named = Uuid::new_v4();
        assert_eq!(
            call(&app, Some("key-edge"), Some(named)).await,
            (StatusCode::OK, format!("key|{named}"))
        );
    }

    #[tokio::test]
    async fn a_missing_unknown_or_refused_credential_is_turned_away() {
        let app = app(Arc::new(Keys { a: Uuid::new_v4() }));
        assert_eq!(call(&app, None, None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            call(&app, Some("nope"), None).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(&app, Some("key-unbound"), None).await.0,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn a_credential_without_an_organisation_is_served_by_one_tenant_and_refused_by_many() {
        let keys = || Arc::new(Keys { a: Uuid::new_v4() });
        let single = app_of(keys(), Some(Tenancy::Single));
        assert_eq!(
            call(&single, Some("key-ownerless"), None).await,
            (StatusCode::OK, "key|".into())
        );
        let multi = app_of(keys(), Some(Tenancy::Multi));
        assert_eq!(
            call(&multi, Some("key-ownerless"), None).await.0,
            StatusCode::FORBIDDEN
        );
    }

    /// A route that changes something, beside the reading one.
    #[tokio::test]
    async fn a_credential_may_only_do_what_its_scopes_grant() {
        let app = app(Arc::new(Keys { a: Uuid::new_v4() }));
        assert_eq!(call(&app, Some("key-reader"), None).await.0, StatusCode::OK);
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/workflows/x/cancel")
                    .header("authorization", "Bearer key-reader")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/workflows/x/cancel")
                    .header("authorization", "Bearer key-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
