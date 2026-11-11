//! Carrying the calling tenant across a gRPC hop.
//!
//! Namespaces are already per-organisation in the schema — `namespaces` has an
//! `organization_id`, and `uq_namespace_org` makes the name unique only within
//! one. The repository has supported org-scoped lookup since it was written:
//!
//! ```ignore
//! async fn exists(&self, name: &str, org_id: Option<&Uuid>) -> OrcherResult<bool>;
//! ```
//!
//! Every caller passed `None`, and nothing carried the organisation from the
//! request down to it, so a namespace resolved by name for anybody. Any
//! deployment where two organisations each hold a namespace of the same name —
//! `development`, say — would resolve to whichever row came back first.
//!
//! This module is the missing carrier: one metadata key, written by a client
//! that knows the tenant and read by the service that needs it. Every reader
//! of that key — the auth layer on the raw HTTP request, the service handlers
//! on tonic metadata — goes through here, so there is exactly one answer to
//! "which header carries the tenant".
//!
//! **Optional on purpose.** Single-tenant deployments have no organisations at
//! all, and must keep working exactly as before: absent header means `None`
//! means the existing behaviour. That is also the risk — a multi-tenant caller
//! that forgets the header silently gets a global lookup rather than an error —
//! so the header is set centrally by the client wrapper rather than at each
//! call site, and a deployment that requires tenancy should enforce it where
//! identity is established, not here.

use tonic::metadata::MetadataMap;
use uuid::Uuid;

/// The header carrying the calling organisation.
///
/// This is the key the SDK workers already send, so it is the canonical one.
/// Lower-case: gRPC metadata keys are case-insensitive but tonic rejects
/// upper-case ASCII when constructing a key, which fails at runtime rather
/// than at compile time.
pub const ORGANIZATION_METADATA_KEY: &str = "x-organization-id";

/// Older spellings of the same header, still read so that a client built
/// against either keeps working. Written by nothing new; remove once no
/// deployed client sends them.
pub(crate) const LEGACY_ORGANIZATION_KEYS: [&str; 2] = ["x-orcher-organization-id", "x-org-id"];

/// Read an organisation id in any form a client has been handed one.
///
/// The API gives ids out as `org_<32 hex>`, and the SDKs pass that string
/// straight through, so that is the common case on the wire — not the bare
/// hyphenated UUID this used to insist on. A client configured with exactly
/// the id it was shown was silently treated as having declared no tenant.
fn parse_organization(raw: &str) -> Option<Uuid> {
    let raw = raw.trim();
    let bare = raw.strip_prefix("org_").unwrap_or(raw);
    Uuid::parse_str(bare).ok()
}

/// Read the calling organisation, if the caller declared one.
///
/// A malformed value is treated as absent rather than as an error. The
/// alternative — failing the RPC — turns a client bug into an outage for a
/// field that is optional by design, and the effect of returning `None` is
/// merely the unscoped behaviour that every caller had before this existed.
pub fn organization_from_metadata(metadata: &MetadataMap) -> Option<Uuid> {
    std::iter::once(ORGANIZATION_METADATA_KEY)
        .chain(LEGACY_ORGANIZATION_KEYS)
        .find_map(|key| {
            metadata
                .get(key)?
                .to_str()
                .ok()
                .and_then(parse_organization)
        })
}

/// The same reading, for layers that see the raw HTTP request rather than a
/// tonic one. Both must agree on which header carries the tenant, so both go
/// through this module.
pub fn organization_from_headers(headers: &http::HeaderMap) -> Option<Uuid> {
    std::iter::once(ORGANIZATION_METADATA_KEY)
        .chain(LEGACY_ORGANIZATION_KEYS)
        .find_map(|key| headers.get(key)?.to_str().ok().and_then(parse_organization))
}

/// Declare the calling organisation on an outgoing request.
pub fn set_organization<T>(request: &mut tonic::Request<T>, organization_id: &Uuid) {
    if let Ok(value) = organization_id.to_string().parse() {
        request
            .metadata_mut()
            .insert(ORGANIZATION_METADATA_KEY, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata_with(value: &str) -> MetadataMap {
        let mut md = MetadataMap::new();
        md.insert(ORGANIZATION_METADATA_KEY, value.parse().unwrap());
        md
    }

    #[test]
    fn reads_an_organisation_the_caller_declared() {
        let id = Uuid::new_v4();
        let md = metadata_with(&id.to_string());
        assert_eq!(organization_from_metadata(&md), Some(id));
    }

    #[test]
    fn absent_header_means_no_organisation() {
        // Single-tenant callers never set it, and must keep working.
        assert_eq!(organization_from_metadata(&MetadataMap::new()), None);
    }

    #[test]
    fn rubbish_is_treated_as_absent_rather_than_fatal() {
        assert_eq!(
            organization_from_metadata(&metadata_with("not-a-uuid")),
            None
        );
        assert_eq!(organization_from_metadata(&metadata_with("")), None);
    }

    #[test]
    fn the_prefixed_form_the_api_hands_out_is_read() {
        let id = Uuid::new_v4();
        // `org_` plus the 32-hex simple form, exactly as the API renders it.
        let md = metadata_with(&format!("org_{}", id.simple()));
        assert_eq!(organization_from_metadata(&md), Some(id));
        // And the hyphenated form with the prefix, in case a client builds it.
        let md = metadata_with(&format!("org_{id}"));
        assert_eq!(organization_from_metadata(&md), Some(id));
    }

    #[test]
    fn legacy_spellings_are_still_read() {
        let id = Uuid::new_v4();
        for key in LEGACY_ORGANIZATION_KEYS {
            let mut md = MetadataMap::new();
            md.insert(key, id.to_string().parse().unwrap());
            assert_eq!(organization_from_metadata(&md), Some(id), "{key}");
        }
    }

    #[test]
    fn the_canonical_key_wins_when_both_are_present() {
        let canonical = Uuid::new_v4();
        let mut md = metadata_with(&canonical.to_string());
        md.insert("x-org-id", Uuid::new_v4().to_string().parse().unwrap());
        assert_eq!(organization_from_metadata(&md), Some(canonical));
    }

    #[test]
    fn http_headers_and_grpc_metadata_read_the_same_way() {
        let id = Uuid::new_v4();
        let mut headers = http::HeaderMap::new();
        headers.insert(ORGANIZATION_METADATA_KEY, id.to_string().parse().unwrap());
        assert_eq!(organization_from_headers(&headers), Some(id));
        assert_eq!(organization_from_headers(&http::HeaderMap::new()), None);
    }

    #[test]
    fn survives_the_round_trip_a_client_and_server_actually_make() {
        let id = Uuid::new_v4();
        let mut request = tonic::Request::new(());
        set_organization(&mut request, &id);
        assert_eq!(organization_from_metadata(request.metadata()), Some(id));
    }
}

// ============================================================================
// Ambient tenant, for callers that cannot pass one explicitly
// ============================================================================

tokio::task_local! {
    /// The organisation on whose behalf the current task is working.
    static CURRENT_ORGANIZATION: Option<Uuid>;
}

/// Run `future` with `organization_id` attached to the current task.
///
/// This exists because the code that *knows* the tenant and the code that
/// *makes the gRPC call* sit in different layers, and deliberately so. An
/// embedding application authenticates the caller and knows which organisation
/// they act for; the handlers between that point and the outgoing call have no
/// use for an organisation, and threading one through their signatures would
/// make every layer carry a concern only the edges have.
///
/// So the tenant is ambient for the duration of a request: whatever establishes
/// identity sets it once, and the interceptor below reads it when a call goes
/// out. Nothing in between has to know.
///
/// Ambient state is worth being uneasy about — it is invisible at the call site
/// — which is why it is confined to exactly one value, set in exactly one
/// place, and read by exactly one interceptor.
pub async fn with_organization<F, R>(organization_id: Option<Uuid>, future: F) -> R
where
    F: std::future::Future<Output = R>,
{
    CURRENT_ORGANIZATION.scope(organization_id, future).await
}

/// The organisation attached to the current task, if any.
///
/// `None` outside a scope, which is the single-tenant case and every test.
pub fn current_organization() -> Option<Uuid> {
    CURRENT_ORGANIZATION.try_with(|org| *org).ok().flatten()
}

/// Whether a caller acting for `caller` may act on a row owned by `owner`.
///
/// The by-id paths — cancel, terminate, reset, send event, update, and the REST
/// equivalents — resolve a workflow by its id alone and act on whatever comes
/// back. An execution id was therefore sufficient to cancel, terminate or reset
/// another organisation's workflow, or to inject a signal into it.
///
/// A caller with no organisation is a single-tenant deployment and may act on
/// anything: scoping there would refuse the one tenant its own data.
///
/// A row with no owner is refused to a caller that has one. Rows predating
/// tenancy exist, and they are exactly the rows that were reachable by
/// everybody; a deployment upgrading with such rows should back-fill their
/// owner rather than have them stay reachable by anyone.
#[must_use]
pub fn may_act_on(owner: Option<Uuid>, caller: Option<Uuid>) -> bool {
    match caller {
        Some(caller) => owner == Some(caller),
        None => true,
    }
}

/// Stamp the current task's organisation onto an outgoing request.
///
/// Used as a tonic interceptor so that every call through a client carries the
/// tenant without each call site remembering to add it. A forgotten header is
/// an unscoped lookup — a silent widening — so this must not be something
/// callers opt into one method at a time.
pub fn tenant_interceptor(
    mut request: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    if let Some(organization_id) = current_organization() {
        set_organization(&mut request, &organization_id);
    }
    Ok(request)
}

#[cfg(test)]
mod ambient_tests {
    use super::*;

    #[tokio::test]
    async fn the_interceptor_stamps_the_ambient_organisation() {
        let id = Uuid::new_v4();

        let stamped = with_organization(Some(id), async {
            let request = tenant_interceptor(tonic::Request::new(())).unwrap();
            organization_from_metadata(request.metadata())
        })
        .await;

        assert_eq!(stamped, Some(id));
    }

    #[tokio::test]
    async fn outside_a_scope_nothing_is_stamped() {
        // The single-tenant case, and every test that never sets one.
        let request = tenant_interceptor(tonic::Request::new(())).unwrap();
        assert_eq!(organization_from_metadata(request.metadata()), None);
    }

    #[tokio::test]
    async fn a_scope_without_an_organisation_stamps_nothing() {
        let stamped = with_organization(None, async {
            let request = tenant_interceptor(tonic::Request::new(())).unwrap();
            organization_from_metadata(request.metadata())
        })
        .await;

        assert_eq!(stamped, None);
    }

    #[tokio::test]
    async fn the_scope_does_not_leak_past_the_request() {
        let id = Uuid::new_v4();
        with_organization(Some(id), async {
            assert_eq!(current_organization(), Some(id))
        })
        .await;
        assert_eq!(current_organization(), None);
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::may_act_on;
    use uuid::Uuid;

    #[test]
    fn a_caller_may_act_on_its_own_row() {
        let mine = Uuid::new_v4();
        assert!(may_act_on(Some(mine), Some(mine)));
    }

    /// The hole this closes: an execution id was enough to cancel, terminate or
    /// reset another organisation's workflow.
    #[test]
    fn a_caller_may_not_act_on_another_organisations_row() {
        assert!(!may_act_on(Some(Uuid::new_v4()), Some(Uuid::new_v4())));
    }

    /// An unowned row predates tenancy. It is refused rather than shared.
    #[test]
    fn an_unowned_row_is_refused_to_a_tenant() {
        assert!(!may_act_on(None, Some(Uuid::new_v4())));
    }

    #[test]
    fn a_single_tenant_deployment_may_act_on_anything() {
        assert!(may_act_on(Some(Uuid::new_v4()), None));
        assert!(may_act_on(None, None));
    }
}
