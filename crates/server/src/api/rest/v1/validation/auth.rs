//! Authentication Validation
//!
//! This module provides validation rules and helpers specific to authentication operations.
//! These validators ensure that authentication-related inputs meet security requirements.

use lazy_static::lazy_static;
use regex::Regex;
use validator::ValidationError;

lazy_static! {
    /// Regex for validating MFA codes (6-8 digits)
    pub static ref MFA_CODE_REGEX: Regex = Regex::new(r"^[0-9]{6,8}$").unwrap();

    /// Regex for validating email or username identifiers
    /// Allows both email format and username format (alphanumeric + underscore)
    pub static ref IDENTIFIER_REGEX: Regex = Regex::new(r"^[a-zA-Z0-9._@+-]+$").unwrap();
}

/// Validate MFA code format
///
/// MFA codes should be 6-8 digit numbers (TOTP or backup codes).
///
/// # Arguments
///
/// * `code` - The MFA code to validate
///
/// # Returns
///
/// * `Ok(())` if valid
/// * `Err(ValidationError)` if invalid
///
/// # Example
///
/// ```
/// use orcher_server::api::rest::v1::validation::auth::validate_mfa_code;
///
/// assert!(validate_mfa_code("123456").is_ok());
/// assert!(validate_mfa_code("12345").is_err()); // Too short
/// assert!(validate_mfa_code("abc123").is_err()); // Not numeric
/// ```
pub fn validate_mfa_code(code: &str) -> Result<(), ValidationError> {
    if code.is_empty() {
        let mut error = ValidationError::new("mfa_code_empty");
        error.message = Some("MFA code cannot be empty".into());
        return Err(error);
    }

    if !MFA_CODE_REGEX.is_match(code) {
        let mut error = ValidationError::new("mfa_code_invalid");
        error.message = Some("MFA code must be 6-8 digits".into());
        return Err(error);
    }

    Ok(())
}

/// Validate login identifier (email or username)
///
/// Identifiers can be either:
/// - Valid email addresses
/// - Usernames (3-50 characters, alphanumeric + underscore)
///
/// # Arguments
///
/// * `identifier` - The identifier to validate
///
/// # Returns
///
/// * `Ok(())` if valid
/// * `Err(ValidationError)` if invalid
///
/// # Example
///
/// ```
/// use orcher_server::api::rest::v1::validation::auth::validate_identifier;
///
/// assert!(validate_identifier("user@example.com").is_ok());
/// assert!(validate_identifier("username").is_ok());
/// assert!(validate_identifier("").is_err()); // Empty
/// ```
pub fn validate_identifier(identifier: &str) -> Result<(), ValidationError> {
    if identifier.is_empty() {
        let mut error = ValidationError::new("identifier_empty");
        error.message = Some("Identifier cannot be empty".into());
        return Err(error);
    }

    if identifier.len() < 3 {
        let mut error = ValidationError::new("identifier_too_short");
        error.message = Some("Identifier must be at least 3 characters".into());
        return Err(error);
    }

    if identifier.len() > 255 {
        let mut error = ValidationError::new("identifier_too_long");
        error.message = Some("Identifier must not exceed 255 characters".into());
        return Err(error);
    }

    // Basic format check - allow common identifier characters
    if !IDENTIFIER_REGEX.is_match(identifier) {
        let mut error = ValidationError::new("identifier_invalid");
        error.message = Some(
            "Identifier can only contain letters, numbers, dots, underscores, @ and hyphens".into(),
        );
        return Err(error);
    }

    Ok(())
}

/// Validate refresh token format
///
/// Refresh tokens should be non-empty strings (actual JWT validation happens elsewhere).
///
/// # Arguments
///
/// * `token` - The refresh token to validate
///
/// # Returns
///
/// * `Ok(())` if valid
/// * `Err(ValidationError)` if invalid
pub fn validate_refresh_token(token: &str) -> Result<(), ValidationError> {
    if token.is_empty() {
        let mut error = ValidationError::new("refresh_token_empty");
        error.message = Some("Refresh token cannot be empty".into());
        return Err(error);
    }

    if token.len() < 20 {
        let mut error = ValidationError::new("refresh_token_invalid");
        error.message = Some("Refresh token appears to be invalid (too short)".into());
        return Err(error);
    }

    Ok(())
}

/// Validate password for login
///
/// For login, we only check that password is not empty.
/// (Password strength validation is only for registration/password changes)
///
/// # Arguments
///
/// * `password` - The password to validate
///
/// # Returns
///
/// * `Ok(())` if valid
/// * `Err(ValidationError)` if invalid
pub fn validate_login_password(password: &str) -> Result<(), ValidationError> {
    if password.is_empty() {
        let mut error = ValidationError::new("password_empty");
        error.message = Some("Password cannot be empty".into());
        return Err(error);
    }

    if password.len() > 128 {
        let mut error = ValidationError::new("password_too_long");
        error.message = Some("Password must not exceed 128 characters".into());
        return Err(error);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_mfa_code_valid() {
        assert!(validate_mfa_code("123456").is_ok());
        assert!(validate_mfa_code("12345678").is_ok());
        assert!(validate_mfa_code("000000").is_ok());
    }

    #[test]
    fn test_validate_mfa_code_invalid() {
        assert!(validate_mfa_code("").is_err());
        assert!(validate_mfa_code("12345").is_err()); // Too short
        assert!(validate_mfa_code("123456789").is_err()); // Too long
        assert!(validate_mfa_code("abc123").is_err()); // Not numeric
        assert!(validate_mfa_code("12 34 56").is_err()); // Contains spaces
    }

    #[test]
    fn test_validate_identifier_email() {
        assert!(validate_identifier("user@example.com").is_ok());
        assert!(validate_identifier("john.doe+tag@company.co.uk").is_ok());
    }

    #[test]
    fn test_validate_identifier_username() {
        assert!(validate_identifier("username").is_ok());
        assert!(validate_identifier("user_name").is_ok());
        assert!(validate_identifier("user123").is_ok());
    }

    #[test]
    fn test_validate_identifier_invalid() {
        assert!(validate_identifier("").is_err()); // Empty
        assert!(validate_identifier("ab").is_err()); // Too short
        assert!(validate_identifier(&"a".repeat(256)).is_err()); // Too long
    }

    #[test]
    fn test_validate_refresh_token_valid() {
        assert!(validate_refresh_token(
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0"
        )
        .is_ok());
        assert!(validate_refresh_token("a".repeat(50).as_str()).is_ok());
    }

    #[test]
    fn test_validate_refresh_token_invalid() {
        assert!(validate_refresh_token("").is_err());
        assert!(validate_refresh_token("short").is_err());
        assert!(validate_refresh_token("a".repeat(10).as_str()).is_err());
    }

    #[test]
    fn test_validate_login_password_valid() {
        assert!(validate_login_password("password").is_ok());
        assert!(validate_login_password("P@ssw0rd!").is_ok());
        assert!(validate_login_password("a".repeat(128).as_str()).is_ok());
    }

    #[test]
    fn test_validate_login_password_invalid() {
        assert!(validate_login_password("").is_err());
        assert!(validate_login_password(&"a".repeat(129)).is_err());
    }

    #[test]
    fn test_mfa_code_regex() {
        assert!(MFA_CODE_REGEX.is_match("123456"));
        assert!(MFA_CODE_REGEX.is_match("12345678"));
        assert!(!MFA_CODE_REGEX.is_match("12345"));
        assert!(!MFA_CODE_REGEX.is_match("123456789"));
        assert!(!MFA_CODE_REGEX.is_match("abc123"));
    }

    #[test]
    fn test_identifier_regex() {
        assert!(IDENTIFIER_REGEX.is_match("user@example.com"));
        assert!(IDENTIFIER_REGEX.is_match("username"));
        assert!(IDENTIFIER_REGEX.is_match("user_name"));
        assert!(IDENTIFIER_REGEX.is_match("user.name"));
        assert!(IDENTIFIER_REGEX.is_match("user+tag@example.com"));
        assert!(IDENTIFIER_REGEX.is_match("user-name"));
    }
}
