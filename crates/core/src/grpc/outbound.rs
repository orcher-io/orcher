//! What every outgoing gRPC call carries.
//!
//! A process that calls the orchestrator on behalf of others — an API layer,
//! say — has two things to say on each call: which tenant it is acting for,
//! and who it is. The tenant is per request and ambient (see `tenant`). The
//! identity is per process: one credential, configured once at startup, sent
//! on every call. This module holds that credential and provides the one
//! interceptor a client should be built with, so neither is something a call
//! site can forget.
//!
//! The credential is optional. A deployment whose orchestrator does not
//! require authentication configures none, and nothing is sent.
//!
//! A process can instead act as each of its callers: when the engine serves
//! its own REST API, a REST request's calls carry the credential that request
//! sent ([`with_caller_credential`]), so the engine grants them exactly what it
//! would grant the caller and nothing more.

use std::sync::OnceLock;
use tonic::metadata::MetadataValue;

static OUTBOUND_API_KEY: OnceLock<String> = OnceLock::new();

tokio::task_local! {
    static CALLER_CREDENTIAL: String;
}

/// Run `future` with the calls it makes carrying `credential`, the bearer
/// token of the caller they are made for, in place of the process's own.
pub async fn with_caller_credential<F, R>(credential: String, future: F) -> R
where
    F: std::future::Future<Output = R>,
{
    CALLER_CREDENTIAL.scope(credential, future).await
}

/// Configure the credential sent on every outgoing call.
///
/// Set once, at startup, before any client is built. A second call with a
/// different value is an error rather than a silent no-op: two parts of a
/// process disagreeing about who it is should fail loudly.
pub fn set_outbound_api_key(api_key: impl Into<String>) -> Result<(), AlreadyConfigured> {
    let api_key = api_key.into();
    match OUTBOUND_API_KEY.set(api_key.clone()) {
        Ok(()) => Ok(()),
        Err(_) if OUTBOUND_API_KEY.get() == Some(&api_key) => Ok(()),
        Err(_) => Err(AlreadyConfigured),
    }
}

/// The outbound credential was already set to a different value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlreadyConfigured;

impl std::fmt::Display for AlreadyConfigured {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("outbound API key is already configured with a different value")
    }
}

impl std::error::Error for AlreadyConfigured {}

/// Stamp the tenant and the credential onto an outgoing request.
///
/// Built into every client by the server crate's client wrappers, so that
/// a new call site cannot omit either. Coercible to a fn pointer, which is
/// what the wrappers' field types name.
#[allow(clippy::result_large_err)]
pub fn outbound_interceptor(
    request: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    let request = super::tenant::tenant_interceptor(request)?;
    let caller = CALLER_CREDENTIAL.try_with(Clone::clone).ok();
    Ok(stamp_credential(
        request,
        caller
            .as_deref()
            .or_else(|| OUTBOUND_API_KEY.get().map(String::as_str)),
    ))
}

fn stamp_credential(mut request: tonic::Request<()>, api_key: Option<&str>) -> tonic::Request<()> {
    if let Some(api_key) = api_key {
        if let Ok(value) = MetadataValue::try_from(format!("Bearer {api_key}")) {
            request.metadata_mut().insert("authorization", value);
        }
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authorization(request: &tonic::Request<()>) -> Option<String> {
        request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }

    #[test]
    fn a_configured_credential_is_sent_as_a_bearer_token() {
        let request = stamp_credential(tonic::Request::new(()), Some("orch_abc"));
        assert_eq!(authorization(&request).as_deref(), Some("Bearer orch_abc"));
    }

    #[test]
    fn no_credential_means_nothing_is_sent() {
        let request = stamp_credential(tonic::Request::new(()), None);
        assert_eq!(authorization(&request), None);
    }

    #[test]
    fn a_credential_that_cannot_be_a_header_is_left_off_rather_than_panicking() {
        let request = stamp_credential(tonic::Request::new(()), Some("has\nnewline"));
        assert_eq!(authorization(&request), None);
    }

    #[tokio::test]
    async fn the_interceptor_carries_the_tenant_as_well() {
        let org = uuid::Uuid::new_v4();
        let stamped = super::super::tenant::with_organization(Some(org), async {
            outbound_interceptor(tonic::Request::new(())).unwrap()
        })
        .await;
        assert_eq!(
            super::super::tenant::organization_from_metadata(stamped.metadata()),
            Some(org)
        );
    }

    #[tokio::test]
    async fn a_call_made_for_a_caller_carries_the_callers_credential() {
        let stamped = with_caller_credential("orch_caller".to_string(), async {
            outbound_interceptor(tonic::Request::new(())).unwrap()
        })
        .await;
        assert_eq!(
            authorization(&stamped).as_deref(),
            Some("Bearer orch_caller")
        );
    }

    #[test]
    fn setting_the_same_credential_twice_is_fine_and_a_different_one_is_not() {
        // The global is process-wide, so this test owns the only writes to it.
        set_outbound_api_key("orch_once").unwrap();
        assert_eq!(set_outbound_api_key("orch_once"), Ok(()));
        assert_eq!(set_outbound_api_key("orch_other"), Err(AlreadyConfigured));
    }
}
