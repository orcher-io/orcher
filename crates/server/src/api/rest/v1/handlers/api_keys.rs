//! API key handlers: issue, list, read and revoke the engine's own keys.
//!
//! ```text
//! POST   /api/v1/api-keys        issue a key; the key is in this response only
//! GET    /api/v1/api-keys        the organisation's keys
//! GET    /api/v1/api-keys/:id    one key
//! DELETE /api/v1/api-keys/:id    revoke a key
//! ```
//!
//! Every route needs the `api-keys:manage` scope, and a caller manages the
//! keys of the organisation it acts for. A key is issued with scopes the
//! caller names, all of which the caller must hold itself, so no key can be
//! used to make a stronger one.
//!
//! These routes serve the engine's authentication seam
//! ([`AppState::set_authenticator`]). A deployment that identifies callers
//! some other way manages its keys there too, and these answer 404.

use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Duration, Utc};
use orcher_core::auth::keys::{ApiKeyInfo, ApiKeyStore, NewKey};
use orcher_core::auth::{Principal, Scope, Scopes};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The longest a key may be issued for.
const MAX_EXPIRY_DAYS: i64 = 3650;

/// Request body for issuing a key.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateApiKeyRequest {
    /// What the key is for, shown in listings.
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// What the key may do: scope names such as `workflows:read`, or `*`.
    /// Required, so a key grants only what was asked for.
    pub scopes: Vec<String>,
    /// Days until the key expires; never, when omitted.
    #[serde(default)]
    pub expires_in_days: Option<i64>,
}

/// A key as listed.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeyResponse {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    /// The key's first characters, to tell it apart from others.
    pub key_prefix: String,
    pub organization_id: Option<Uuid>,
    /// What the key may do; `["*"]` for a key issued before keys had scopes.
    pub scopes: Vec<String>,
    /// Whether the key is accepted: not revoked, not expired.
    pub is_active: bool,
    pub created_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub revoked_by: Option<String>,
}

impl From<ApiKeyInfo> for ApiKeyResponse {
    fn from(key: ApiKeyInfo) -> Self {
        let is_active = key.revoked_at.is_none() && key.expires_at.is_none_or(|at| at > Utc::now());
        Self {
            id: key.id,
            name: key.name,
            description: key.description,
            key_prefix: key.key_prefix,
            organization_id: key.organization_id,
            scopes: key.scopes.unwrap_or_else(|| vec!["*".to_string()]),
            is_active,
            created_by: key.created_by,
            created_at: key.created_at,
            expires_at: key.expires_at,
            last_used_at: key.last_used_at,
            revoked_at: key.revoked_at,
            revoked_by: key.revoked_by,
        }
    }
}

/// A key just issued.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateApiKeyResponse {
    #[serde(flatten)]
    pub api_key: ApiKeyResponse,
    /// The key itself. It is not stored and is not shown again.
    pub key: String,
}

/// The organisation's keys.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListApiKeysResponse {
    pub keys: Vec<ApiKeyResponse>,
    pub total: usize,
}

/// Issue a key. `POST /api/v1/api-keys`
pub async fn create_api_key(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Json(request): Json<CreateApiKeyRequest>,
) -> Result<(StatusCode, Json<CreateApiKeyResponse>)> {
    let (store, organization_id) = keys_of(&state, &caller)?;

    let name = request.name.trim();
    if name.is_empty() || name.chars().count() > 255 {
        return Err(ServerError::validation(
            "name must be between 1 and 255 characters",
        ));
    }
    let scopes = requested_scopes(&request.scopes)?;
    if !caller.scopes.covers(&scopes) {
        return Err(ServerError::forbidden(
            "A key may be given only scopes the caller holds itself",
        ));
    }
    let expires_at = match request.expires_in_days {
        None => None,
        Some(days) if (1..=MAX_EXPIRY_DAYS).contains(&days) => {
            Some(Utc::now() + Duration::days(days))
        }
        Some(_) => {
            return Err(ServerError::validation(format!(
                "expiresInDays must be between 1 and {MAX_EXPIRY_DAYS}"
            )))
        }
    };

    let issued = store
        .issue(NewKey {
            name: name.to_string(),
            description: request.description,
            organization_id,
            scopes,
            expires_at,
            created_by: caller.subject.clone(),
        })
        .await
        .map_err(storage_error)?;
    Ok((
        StatusCode::CREATED,
        Json(CreateApiKeyResponse {
            api_key: issued.info.into(),
            key: issued.key,
        }),
    ))
}

/// The organisation's keys, newest first. `GET /api/v1/api-keys`
pub async fn list_api_keys(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
) -> Result<Json<ListApiKeysResponse>> {
    let (store, organization_id) = keys_of(&state, &caller)?;
    let keys: Vec<ApiKeyResponse> = store
        .list(organization_id)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(Into::into)
        .collect();
    Ok(Json(ListApiKeysResponse {
        total: keys.len(),
        keys,
    }))
}

/// One key. `GET /api/v1/api-keys/:id`
pub async fn get_api_key(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<Uuid>,
) -> Result<Json<ApiKeyResponse>> {
    let (store, organization_id) = keys_of(&state, &caller)?;
    store
        .get(organization_id, id)
        .await
        .map_err(storage_error)?
        .map(|key| Json(key.into()))
        .ok_or_else(|| ServerError::not_found(format!("API key {id}")))
}

/// Revoke a key, at once on every engine. Revoking a revoked key answers it
/// unchanged. `DELETE /api/v1/api-keys/:id`
pub async fn revoke_api_key(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<Uuid>,
) -> Result<Json<ApiKeyResponse>> {
    let (store, organization_id) = keys_of(&state, &caller)?;
    store
        .revoke(organization_id, id, &caller.subject)
        .await
        .map_err(storage_error)?
        .map(|key| Json(key.into()))
        .ok_or_else(|| ServerError::not_found(format!("API key {id}")))
}

/// The key store, and the organisation whose keys the caller manages.
fn keys_of(state: &AppState, caller: &Principal) -> Result<(ApiKeyStore, Option<Uuid>)> {
    if state.authenticator().is_none() {
        return Err(ServerError::not_found(
            "API keys are managed by this deployment's account service",
        ));
    }
    // The organisation the request acts for, as the authentication
    // middleware resolved it.
    let organization_id = orcher_core::grpc::tenant::current_organization();
    if organization_id.is_none() && caller.can_act_for_any_organization() {
        // A platform credential acting for nobody would manage the keys that
        // belong to no organisation, which on a multi-tenant deployment are
        // keys to every tenant.
        return Err(ServerError::bad_request(
            "Name the organization whose keys to manage (x-organization-id)",
        ));
    }
    Ok((ApiKeyStore::new(state.pool().clone()), organization_id))
}

/// The scopes a request names, or why they cannot be granted.
fn requested_scopes(names: &[String]) -> Result<Scopes> {
    let (scopes, unknown) = Scopes::parse(names.iter().map(String::as_str));
    if !unknown.is_empty() {
        let known: Vec<&str> = Scope::ALL.iter().map(|scope| scope.as_str()).collect();
        return Err(ServerError::validation(format!(
            "Unknown scopes {unknown:?}; known scopes are {known:?} and \"*\""
        )));
    }
    if scopes == Scopes::none() {
        return Err(ServerError::validation(
            "scopes must name at least one scope",
        ));
    }
    Ok(scopes)
}

fn storage_error(error: sqlx::Error) -> ServerError {
    tracing::error!(%error, "API key storage failed");
    ServerError::internal("API key storage failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_must_be_asked_for_with_known_scopes() {
        assert!(requested_scopes(&["workflows:read".into()]).is_ok());
        assert_eq!(requested_scopes(&["*".into()]).unwrap(), Scopes::all());
        assert!(requested_scopes(&[]).is_err());
        let unknown = requested_scopes(&["workflows:teleport".into()]).unwrap_err();
        assert!(
            unknown.to_string().contains("workflows:teleport"),
            "{unknown}"
        );
    }
}
