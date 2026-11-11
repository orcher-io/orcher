//! Who is calling, which organisation they act for, and what they may do.
//!
//! One [`Authenticator`] answers that for every API the engine serves: it
//! turns the credential a request carries into a [`Principal`]. The gRPC auth
//! layer and the REST middleware both ask it, so a caller is the same caller,
//! held to the same limits, whichever API it uses.
//!
//! The engine ships the answers most deployments need ([`NoAuthenticator`]
//! for local development, [`ApiKeyAuthenticator`] for keys it stores, and
//! [`CachedAuthenticator`] in front of either); anything else (an identity
//! provider, a deployment's own accounts) implements the trait.

mod api_key;
mod cached;
pub mod keys;
mod oidc;
pub mod scope;

pub use api_key::ApiKeyAuthenticator;
pub use cached::{AuthCacheConfig, CachedAuthenticator};
pub use oidc::{KeyOrToken, OidcAuthenticator, OidcConfig};
pub use scope::{Scope, Scopes};

use crate::grpc::service_auth::{ServiceIdentity, ServicePermissions, ServiceStatus};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Whether a deployment serves one tenant or many: what decides whether a
/// credential that belongs to no organisation may act.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Tenancy {
    /// One tenant. A credential without an organisation acts for it, and sees
    /// everything; one with an organisation is held to it.
    Single,
    /// Many tenants. Every credential acts for its own organisation, or, if it
    /// may act for any, for the one a request names. A credential with
    /// neither is refused: it would see every tenant's runs.
    Multi,
}

/// The organisation a request from `principal` acts for, when the caller
/// claimed `claimed` (the tenant header), on a deployment of `tenancy`.
///
/// `None` tenancy is a deployment that has not declared it, whose
/// authenticator decides instead (the API-key one infers it from whether any
/// organisation exists). The claim rule is the one every API applies:
/// [`effective_organization`](crate::grpc::auth_layer::effective_organization).
pub fn organization_for(
    principal: &Principal,
    claimed: Option<Uuid>,
    tenancy: Option<Tenancy>,
) -> Result<Option<Uuid>, AuthError> {
    if tenancy == Some(Tenancy::Multi)
        && principal.organization_id.is_none()
        && !principal.can_act_for_any_organization()
    {
        return Err(AuthError::Forbidden(
            "The credential is not bound to an organization; bind it to one".to_string(),
        ));
    }
    crate::grpc::auth_layer::effective_organization(
        principal.organization_id,
        principal.can_act_for_any_organization(),
        claimed,
    )
    .map_err(|status| AuthError::Forbidden(status.message().to_string()))
}

/// The credential a request carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Credential<'a> {
    /// None at all.
    None,
    /// A bearer token: an API key, or a token an identity provider issued.
    Bearer(&'a str),
}

/// A caller, as an [`Authenticator`] identified them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// Stable id of the caller: `anonymous`, `key_<id>`, or a token's subject.
    pub subject: String,
    /// What to call the caller in messages and logs.
    pub name: String,
    /// The organisation the credential belongs to, if it belongs to one. An
    /// opaque tenancy key: the engine compares it, and never looks it up.
    pub organization_id: Option<Uuid>,
    /// The one namespace the caller is held to, or `None` for any.
    pub namespace: Option<String>,
    /// The task queues the caller may poll; empty for any.
    pub task_queues: Vec<String>,
    /// What the caller may do.
    pub scopes: Scopes,
    /// Whether the caller may act for whichever organisation a request names:
    /// a trusted edge that has resolved the tenant itself. A capability of the
    /// credential, never a scope one can grant another: a credential with it
    /// can act as every tenant.
    pub can_act_for_any_organization: bool,
    /// When the credential stops being valid, if it says: a token's expiry.
    /// Nothing remembers the caller past it.
    pub valid_until: Option<std::time::SystemTime>,
}

impl Principal {
    /// The caller of a deployment that authenticates nobody: no organisation,
    /// no limits, but no right to act for an organisation it names either.
    pub fn anonymous() -> Self {
        Self {
            subject: "anonymous".to_string(),
            name: "anonymous".to_string(),
            organization_id: None,
            namespace: None,
            task_queues: Vec::new(),
            scopes: Scopes::all(),
            can_act_for_any_organization: false,
            valid_until: None,
        }
    }

    /// Whether the caller may act for whichever organisation a request names:
    /// a trusted edge that has resolved the tenant itself.
    pub fn can_act_for_any_organization(&self) -> bool {
        self.can_act_for_any_organization
    }

    /// The caller as the gRPC services' permission checks read it.
    pub fn service_identity(&self) -> ServiceIdentity {
        ServiceIdentity {
            service_id: self.subject.clone(),
            service_name: self.name.clone(),
            namespace: self.namespace.clone().unwrap_or_default(),
            allowed_task_queues: self.task_queues.clone(),
            permissions: ServicePermissions {
                can_poll_workflow_steps: self.scopes.allows(Scope::WorkersRun),
                can_poll_activity_tasks: self.scopes.allows(Scope::WorkersRun),
                can_start_workflows: self.scopes.allows(Scope::WorkflowsWrite),
                can_send_events_to_workflows: self.scopes.allows(Scope::WorkflowsWrite),
                can_query_workflows: self.scopes.allows(Scope::WorkflowsRead),
                can_cancel_workflows: self.scopes.allows(Scope::WorkflowsWrite),
                can_act_for_any_organization: self.can_act_for_any_organization,
            },
            metadata: std::collections::HashMap::new(),
            registered_at: orcher_common::types::time::Timestamptz::now(),
            last_seen_at: None,
            status: ServiceStatus::Active,
            organization_id: self.organization_id,
        }
    }
}

/// Why a credential was not accepted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// The credential is missing, unknown, revoked, expired or malformed.
    /// One kind for all of them: telling a caller which it was tells an
    /// attacker which credentials exist.
    #[error("{0}")]
    Unauthenticated(String),
    /// The caller is who it says, but may not act: for example a key with no
    /// organisation on a deployment that has organisations.
    #[error("{0}")]
    Forbidden(String),
}

/// Turns a request's credential into the caller it identifies.
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// The caller `credential` identifies, or why it is not accepted.
    async fn authenticate(&self, credential: Credential<'_>) -> Result<Principal, AuthError>;
}

/// Accepts every request, as [`Principal::anonymous`]: local development and
/// deployments that keep the engine on a private network.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoAuthenticator;

#[async_trait]
impl Authenticator for NoAuthenticator {
    async fn authenticate(&self, _credential: Credential<'_>) -> Result<Principal, AuthError> {
        Ok(Principal::anonymous())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn without_authentication_every_caller_is_anonymous() {
        for credential in [Credential::None, Credential::Bearer("anything")] {
            assert_eq!(
                NoAuthenticator.authenticate(credential).await.unwrap(),
                Principal::anonymous()
            );
        }
    }

    fn key(organization_id: Option<Uuid>, delegate: bool) -> Principal {
        Principal {
            subject: "key".into(),
            name: "key".into(),
            organization_id,
            can_act_for_any_organization: delegate,
            ..Principal::anonymous()
        }
    }

    #[test]
    fn a_single_tenant_deployment_serves_a_credential_without_an_organisation() {
        assert_eq!(
            organization_for(&key(None, false), None, Some(Tenancy::Single)),
            Ok(None)
        );
    }

    #[test]
    fn a_multi_tenant_deployment_refuses_a_credential_without_an_organisation() {
        assert!(matches!(
            organization_for(&key(None, false), None, Some(Tenancy::Multi)),
            Err(AuthError::Forbidden(_))
        ));
        // Naming one does not help: it may not.
        assert!(matches!(
            organization_for(
                &key(None, false),
                Some(Uuid::new_v4()),
                Some(Tenancy::Multi)
            ),
            Err(AuthError::Forbidden(_))
        ));
    }

    #[test]
    fn every_tenancy_holds_a_credential_to_its_organisation_and_lets_the_edge_name_one() {
        let (own, other) = (Uuid::new_v4(), Uuid::new_v4());
        for tenancy in [None, Some(Tenancy::Single), Some(Tenancy::Multi)] {
            assert_eq!(
                organization_for(&key(Some(own), false), None, tenancy),
                Ok(Some(own))
            );
            assert!(matches!(
                organization_for(&key(Some(own), false), Some(other), tenancy),
                Err(AuthError::Forbidden(_))
            ));
            assert_eq!(
                organization_for(&key(None, true), Some(other), tenancy),
                Ok(Some(other))
            );
        }
    }

    #[test]
    fn tenancy_is_written_in_lower_case() {
        assert_eq!(
            serde_json::from_str::<Tenancy>("\"multi\"").unwrap(),
            Tenancy::Multi
        );
        assert_eq!(
            serde_json::to_string(&Tenancy::Single).unwrap(),
            "\"single\""
        );
    }

    #[test]
    fn the_anonymous_caller_may_not_act_for_an_organisation_it_names() {
        assert!(!Principal::anonymous().can_act_for_any_organization());
    }

    #[test]
    fn a_principal_reads_as_the_identity_the_permission_checks_take() {
        let organization = Uuid::new_v4();
        let principal = Principal {
            subject: "key_1".into(),
            name: "worker key".into(),
            organization_id: Some(organization),
            namespace: Some("payments".into()),
            task_queues: vec!["orders".into()],
            scopes: Scopes::parse(["workers:run"]).0,
            can_act_for_any_organization: false,
            valid_until: None,
        };
        let identity = principal.service_identity();
        assert_eq!(identity.service_id, "key_1");
        assert_eq!(identity.service_name, "worker key");
        assert_eq!(identity.organization_id, Some(organization));
        assert_eq!(identity.namespace, "payments");
        assert_eq!(identity.allowed_task_queues, ["orders"]);
        // The scopes, as the handlers' own checks read them.
        assert_eq!(identity.permissions, ServicePermissions::worker());
        assert_eq!(
            Principal::anonymous().service_identity().permissions,
            ServicePermissions::default()
        );

        // No namespace reads as the empty one: not pinned to any.
        assert_eq!(Principal::anonymous().service_identity().namespace, "");
    }
}
