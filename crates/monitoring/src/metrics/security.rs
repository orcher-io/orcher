//! Security metrics (auth, rate limiting, threat detection).

use metrics::counter;

pub fn track_auth_attempt(user_id: &str, success: bool) {
    counter!(
        super::names::security::AUTH_ATTEMPTS_TOTAL,
        "user_id" => user_id.to_string(),
        "result" => if success { "success" } else { "failure" }
    )
    .increment(1);

    if !success {
        counter!(super::names::security::AUTH_FAILURES_TOTAL).increment(1);
    }
}

pub fn track_rate_limit_exceeded(endpoint: &str) {
    counter!(
        super::names::security::RATE_LIMIT_EXCEEDED_TOTAL,
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

pub fn track_threat_detected(threat_type: &str) {
    counter!(
        super::names::security::THREATS_DETECTED_TOTAL,
        "threat_type" => threat_type.to_string()
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_auth_attempt_success() {
        track_auth_attempt("user123", true);
        track_auth_attempt("admin", true);
    }

    #[test]
    fn test_track_auth_attempt_failure() {
        track_auth_attempt("user456", false);
        track_auth_attempt("attacker", false);
    }

    #[test]
    fn test_track_rate_limit_exceeded() {
        track_rate_limit_exceeded("/api/tasks");
        track_rate_limit_exceeded("/api/workflows");
        track_rate_limit_exceeded("/api/users");
    }

    #[test]
    fn test_track_threat_detected() {
        track_threat_detected("sql_injection");
        track_threat_detected("xss");
        track_threat_detected("brute_force");
        track_threat_detected("csrf");
    }

    #[test]
    fn test_mixed_auth_events() {
        // Multiple successful attempts
        track_auth_attempt("user1", true);
        track_auth_attempt("user2", true);

        // Some failures
        track_auth_attempt("user3", false);
        track_auth_attempt("user1", false); // Same user failing

        // More successes
        track_auth_attempt("user3", true); // User3 succeeds after failure
    }

    #[test]
    fn test_security_incident_scenario() {
        // Simulate a security incident scenario

        // Rate limiting kicks in
        track_rate_limit_exceeded("/api/login");

        // Multiple failed auth attempts (brute force)
        for i in 0..10 {
            track_auth_attempt(&format!("attacker{}", i), false);
        }

        // Threat detected
        track_threat_detected("brute_force");

        // More rate limiting
        track_rate_limit_exceeded("/api/tasks");

        // SQL injection attempt
        track_threat_detected("sql_injection");
    }

    #[test]
    fn test_edge_case_inputs() {
        // Empty strings
        track_auth_attempt("", true);
        track_rate_limit_exceeded("");
        track_threat_detected("");

        // Very long strings
        let long_user_id = "a".repeat(1000);
        track_auth_attempt(&long_user_id, false);

        let long_endpoint = "/api/".to_string() + &"very_long_endpoint_name/".repeat(100);
        track_rate_limit_exceeded(&long_endpoint);

        let long_threat = "very_specific_threat_type_with_lots_of_details_".repeat(50);
        track_threat_detected(&long_threat);
    }
}
