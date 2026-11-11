//! Integration with orcher-security for automatic auth event tracking.
//!
//! This module provides utilities to automatically instrument security operations,
//! including authentication attempts, authorization checks, and threat detection.

use crate::metrics::security_names;
use metrics::counter;
use tracing::{info, instrument, warn};

/// Tracks an authentication attempt with automatic metrics collection.
///
/// This function records authentication attempts and emits appropriate metrics
/// for monitoring security events and access patterns.
#[instrument(skip(username))]
pub fn track_auth_attempt(username: &str, success: bool, method: &str) {
    counter!(security_names::AUTH_ATTEMPTS_TOTAL, "method" => method.to_string()).increment(1);

    if success {
        info!(username = %username, method = %method, "Authentication succeeded");
    } else {
        counter!(security_names::AUTH_FAILURES_TOTAL, "method" => method.to_string()).increment(1);
        warn!(username = %username, method = %method, "Authentication failed");
    }
}

/// Records an authorization check result.
pub fn track_authorization_check(user_id: &str, resource: &str, action: &str, allowed: bool) {
    let result = if allowed { "allowed" } else { "denied" };

    counter!(
        "security_authorization_checks_total",
        "user_id" => user_id.to_string(),
        "resource" => resource.to_string(),
        "action" => action.to_string(),
        "result" => result.to_string()
    )
    .increment(1);
}

/// Records a rate limit exceeded event.
pub fn track_rate_limit_exceeded(endpoint: &str, identifier: &str) {
    counter!(
        security_names::RATE_LIMIT_EXCEEDED_TOTAL,
        "endpoint" => endpoint.to_string(),
        "identifier" => identifier.to_string()
    )
    .increment(1);

    warn!(endpoint = %endpoint, identifier = %identifier, "Rate limit exceeded");
}

/// Records a detected security threat.
pub fn track_threat_detected(threat_type: &str, severity: &str, source: &str) {
    counter!(
        security_names::THREATS_DETECTED_TOTAL,
        "type" => threat_type.to_string(),
        "severity" => severity.to_string(),
        "source" => source.to_string()
    )
    .increment(1);

    warn!(
        threat_type = %threat_type,
        severity = %severity,
        source = %source,
        "Security threat detected"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_auth_attempt_success() {
        track_auth_attempt("test_user", true, "jwt");
    }

    #[test]
    fn test_track_auth_attempt_failure() {
        track_auth_attempt("test_user", false, "jwt");
    }

    #[test]
    fn test_track_authorization_check_allowed() {
        track_authorization_check("user123", "task:123", "read", true);
    }

    #[test]
    fn test_track_authorization_check_denied() {
        track_authorization_check("user123", "task:123", "delete", false);
    }

    #[test]
    fn test_track_rate_limit_exceeded() {
        track_rate_limit_exceeded("/api/tasks", "192.168.1.1");
    }

    #[test]
    fn test_track_threat_detected() {
        track_threat_detected("sql_injection", "high", "192.168.1.100");
    }
}
