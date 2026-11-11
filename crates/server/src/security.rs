//! Security policy trait.
//!
//! Defines the interface between the core server and security enforcement,
//! mirroring the [`crate::auth::AuthProvider`] seam. The open source server
//! ships with [`DefaultSecurityPolicy`], which applies a standard set of
//! hardening response headers to every response. The cloud gateway — or any
//! self-hosted deployment — can plug in its own implementation to enforce a
//! different policy without forking the core server.
//!
//! # Open Source Default
//!
//! [`DefaultSecurityPolicy`] emits the widely-recommended security response
//! headers (HSTS, `X-Content-Type-Options`, `X-Frame-Options`,
//! `Referrer-Policy`, `Permissions-Policy`, and — off by default — a
//! Content-Security-Policy). Its behaviour is configurable via
//! [`SecurityHeaders`], so self-hosters who only need to tweak values do not
//! have to implement the trait at all.
//!
//! # Cloud / Self-Hosted Implementation
//!
//! Implement [`SecurityPolicy`] and inject it with
//! [`crate::state::AppState::set_security_policy`] to enforce bespoke policy
//! (e.g. per-tenant CSP, nonce-based script policies, environment-aware HSTS).

use axum::http::{HeaderMap, HeaderName, HeaderValue};

/// Applies security policy to outgoing responses.
///
/// Called by the server's security middleware for every response, after the
/// route handler has run. Implementations typically set response headers, but
/// may enforce any header-level policy.
pub trait SecurityPolicy: Send + Sync + 'static {
    /// Apply security-related headers to an outgoing response.
    fn apply_response_headers(&self, headers: &mut HeaderMap);
}

/// Configuration for [`DefaultSecurityPolicy`].
///
/// Every field is `Option`/`bool`; a `None` (or `false`) value means the
/// corresponding header is omitted. Defaults are safe for a self-hosted API
/// server behind TLS.
#[derive(Debug, Clone)]
pub struct SecurityHeaders {
    /// `Strict-Transport-Security` value (only honoured by browsers over HTTPS).
    /// `None` omits the header.
    pub hsts: Option<String>,
    /// `X-Frame-Options` value (e.g. `DENY`, `SAMEORIGIN`).
    pub frame_options: Option<String>,
    /// When `true`, sends `X-Content-Type-Options: nosniff`.
    pub content_type_options: bool,
    /// `Referrer-Policy` value.
    pub referrer_policy: Option<String>,
    /// `X-XSS-Protection` value. Modern guidance is `0` (disable the legacy,
    /// buggy filter and rely on CSP); kept configurable for compatibility.
    pub xss_protection: Option<String>,
    /// `Permissions-Policy` value.
    pub permissions_policy: Option<String>,
    /// `Content-Security-Policy` value. `None` by default: a CSP is
    /// deployment-specific and a wrong one silently breaks a dashboard, so it
    /// is opt-in rather than guessed.
    pub csp: Option<String>,
}

impl Default for SecurityHeaders {
    fn default() -> Self {
        Self {
            // 1 year, includeSubDomains. `preload` is intentionally omitted:
            // preloading is a hard-to-reverse commitment that should be an
            // explicit operator decision, not a silent default.
            hsts: Some("max-age=31536000; includeSubDomains".to_string()),
            frame_options: Some("DENY".to_string()),
            content_type_options: true,
            referrer_policy: Some("strict-origin-when-cross-origin".to_string()),
            // OWASP now recommends `0` over `1; mode=block` (the legacy filter
            // has known bypasses and can introduce vulnerabilities).
            xss_protection: Some("0".to_string()),
            permissions_policy: Some("geolocation=(), microphone=(), camera=()".to_string()),
            csp: None,
        }
    }
}

/// The open source default security policy: standard hardening response headers.
///
/// Construct with [`Default::default`] for sensible defaults, or
/// [`DefaultSecurityPolicy::new`] to customise the header values.
pub struct DefaultSecurityPolicy {
    headers: SecurityHeaders,
}

impl DefaultSecurityPolicy {
    /// Create a policy with the given header configuration.
    #[must_use]
    pub fn new(headers: SecurityHeaders) -> Self {
        Self { headers }
    }
}

impl Default for DefaultSecurityPolicy {
    fn default() -> Self {
        Self {
            headers: SecurityHeaders::default(),
        }
    }
}

impl SecurityPolicy for DefaultSecurityPolicy {
    fn apply_response_headers(&self, headers: &mut HeaderMap) {
        let h = &self.headers;
        set(headers, "strict-transport-security", h.hsts.as_deref());
        set(headers, "x-frame-options", h.frame_options.as_deref());
        if h.content_type_options {
            set(headers, "x-content-type-options", Some("nosniff"));
        }
        set(headers, "referrer-policy", h.referrer_policy.as_deref());
        set(headers, "x-xss-protection", h.xss_protection.as_deref());
        set(
            headers,
            "permissions-policy",
            h.permissions_policy.as_deref(),
        );
        set(headers, "content-security-policy", h.csp.as_deref());
    }
}

/// Insert a header, skipping absent values and values that are not valid
/// header contents (rather than panicking).
fn set(headers: &mut HeaderMap, name: &'static str, value: Option<&str>) {
    if let Some(v) = value {
        if let Ok(hv) = HeaderValue::from_str(v) {
            headers.insert(HeaderName::from_static(name), hv);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_sets_expected_headers() {
        let mut headers = HeaderMap::new();
        DefaultSecurityPolicy::default().apply_response_headers(&mut headers);

        assert_eq!(headers["x-content-type-options"], "nosniff");
        assert_eq!(headers["x-frame-options"], "DENY");
        assert_eq!(headers["x-xss-protection"], "0");
        assert_eq!(
            headers["referrer-policy"],
            "strict-origin-when-cross-origin"
        );
        assert!(headers["strict-transport-security"]
            .to_str()
            .unwrap()
            .contains("max-age=31536000"));
        // CSP is opt-in, so absent by default.
        assert!(!headers.contains_key("content-security-policy"));
    }

    #[test]
    fn omitted_values_skip_headers() {
        let policy = DefaultSecurityPolicy::new(SecurityHeaders {
            hsts: None,
            frame_options: None,
            content_type_options: false,
            referrer_policy: None,
            xss_protection: None,
            permissions_policy: None,
            csp: Some("default-src 'self'".to_string()),
        });
        let mut headers = HeaderMap::new();
        policy.apply_response_headers(&mut headers);

        assert!(!headers.contains_key("strict-transport-security"));
        assert!(!headers.contains_key("x-frame-options"));
        assert!(!headers.contains_key("x-content-type-options"));
        assert_eq!(headers["content-security-policy"], "default-src 'self'");
    }

    #[test]
    fn custom_policy_can_be_injected_as_trait_object() {
        // A self-hoster / cloud can supply any implementation.
        struct DenyEverything;
        impl SecurityPolicy for DenyEverything {
            fn apply_response_headers(&self, headers: &mut HeaderMap) {
                headers.insert(
                    "content-security-policy",
                    HeaderValue::from_static("default-src 'none'"),
                );
            }
        }
        let policy: std::sync::Arc<dyn SecurityPolicy> = std::sync::Arc::new(DenyEverything);
        let mut headers = HeaderMap::new();
        policy.apply_response_headers(&mut headers);
        assert_eq!(headers["content-security-policy"], "default-src 'none'");
    }
}
