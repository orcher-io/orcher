//! # Custom Extractors for REST API
//!
//! Provides custom Axum extractors with improved error handling and validation.
//!
//! ## Features
//!
//! - Better JSON deserialization error messages
//! - Field-level validation errors
//! - User-friendly error responses
//! - Automatic validation using `validator` crate
//! - Request context extraction for audit logging

use axum::{
    async_trait,
    extract::{rejection::JsonRejection, ConnectInfo, FromRequest, FromRequestParts, Request},
    http::{header, request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json, RequestPartsExt,
};
use serde::de::DeserializeOwned;
use std::error::Error;
use std::net::SocketAddr;
use uuid::Uuid;
use validator::Validate;

use crate::error::{ErrorDetails, ErrorInfo, ErrorResponse};
use orcher_common::types::time::Timestamptz;

/// Audit context extracted from HTTP request
///
/// Captures metadata useful for audit logging, including IP address,
/// user agent, and request ID for correlation.
///
/// # Example
///
/// ```rust,ignore
/// use orcher_server::api::rest::extractors::AuditContext;
///
/// async fn handler(audit_ctx: AuditContext) {
///     println!("Request from: {:?}", audit_ctx.ip_address);
///     println!("User agent: {:?}", audit_ctx.user_agent);
///     println!("Request ID: {}", audit_ctx.request_id);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct AuditContext {
    /// IP address of the client making the request
    pub ip_address: Option<String>,
    /// User agent string from the request headers
    pub user_agent: Option<String>,
    /// Unique request ID for correlation
    pub request_id: Uuid,
}

impl Default for AuditContext {
    fn default() -> Self {
        Self {
            ip_address: None,
            user_agent: None,
            request_id: Uuid::nil(),
        }
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for AuditContext
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract IP address from ConnectInfo
        let ip_address = parts
            .extract::<ConnectInfo<SocketAddr>>()
            .await
            .ok()
            .map(|ConnectInfo(addr)| addr.ip().to_string())
            .or_else(|| {
                // Fallback to X-Forwarded-For header (if behind proxy)
                parts
                    .headers
                    .get("x-forwarded-for")
                    .and_then(|h| h.to_str().ok())
                    .and_then(|s| s.split(',').next())
                    .map(|s| s.trim().to_string())
            })
            .or_else(|| {
                // Fallback to X-Real-IP header
                parts
                    .headers
                    .get("x-real-ip")
                    .and_then(|h| h.to_str().ok())
                    .map(std::string::ToString::to_string)
            });

        // Extract user agent
        let user_agent = parts
            .headers
            .get(header::USER_AGENT)
            .and_then(|h| h.to_str().ok())
            .map(std::string::ToString::to_string);

        // Extract or generate request ID
        let request_id = parts
            .headers
            .get("x-request-id")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or_else(Uuid::new_v4);

        Ok(AuditContext {
            ip_address,
            user_agent,
            request_id,
        })
    }
}

/// Authenticated user extracted from request extensions
///
/// This extractor retrieves the authenticated user information that was
/// stored in the request extensions by the authentication middleware.
/// It will return an error if no authenticated user is found.
///
/// # Example
///
/// ```rust,ignore
/// use orcher_server::api::rest::extractors::AuthUser;
/// use orcher_server::middleware::auth::AuthenticatedUser;
///
/// async fn handler(AuthUser(user): AuthUser) {
///     println!("User ID: {}", user.user_id);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct AuthUser(pub crate::middleware::auth::AuthenticatedUser);

#[async_trait]
impl<S> FromRequestParts<S> for AuthUser
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<crate::middleware::auth::AuthenticatedUser>()
            .cloned()
            .map(AuthUser)
            .ok_or(StatusCode::UNAUTHORIZED)
    }
}

/// Custom JSON extractor with better error messages
///
/// This extractor wraps Axum's `Json` extractor and provides:
/// - More user-friendly error messages
/// - Field-level error information
/// - Automatic validation using the `validator` crate
///
/// # Example
///
/// ```rust,ignore
/// use axum::extract::State;
/// use orcher_server::api::rest::extractors::ValidatedJson;
/// use serde::Deserialize;
/// use validator::Validate;
///
/// #[derive(Debug, Deserialize, Validate)]
/// struct CreateUserRequest {
///     #[validate(email)]
///     email: String,
///     #[validate(length(min = 8))]
///     password: String,
/// }
///
/// async fn create_user(
///     ValidatedJson(payload): ValidatedJson<CreateUserRequest>
/// ) -> Result<(), ServerError> {
///     // payload is already validated
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct ValidatedJson<T>(pub T);

#[async_trait]
impl<T, S> FromRequest<S> for ValidatedJson<T>
where
    T: DeserializeOwned + Validate,
    S: Send + Sync,
{
    type Rejection = ValidatedJsonRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        // First, try to deserialize using Axum's Json extractor
        let Json(value) = Json::<T>::from_request(req, state)
            .await
            .map_err(|rejection| ValidatedJsonRejection {
                status: StatusCode::BAD_REQUEST,
                error: parse_json_rejection(rejection),
            })?;

        // Then validate the deserialized value
        value
            .validate()
            .map_err(|validation_errors| ValidatedJsonRejection {
                status: StatusCode::BAD_REQUEST,
                error: parse_validation_errors(validation_errors),
            })?;

        Ok(ValidatedJson(value))
    }
}

/// Rejection type for `ValidatedJson` extractor
#[derive(Debug)]
pub struct ValidatedJsonRejection {
    status: StatusCode,
    error: ErrorResponse,
}

impl IntoResponse for ValidatedJsonRejection {
    fn into_response(self) -> Response {
        (self.status, Json(self.error)).into_response()
    }
}

/// Parse Axum's `JsonRejection` into a user-friendly error response
fn parse_json_rejection(rejection: JsonRejection) -> ErrorResponse {
    use tracing::warn;

    let (message, field, suggestion) = match rejection {
        JsonRejection::JsonDataError(err) => {
            // Get the actual serde error from the source
            let error_msg = if let Some(source_err) = err.source() {
                source_err.to_string()
            } else {
                err.to_string()
            };

            // Log the detailed error for debugging
            warn!(
                "JSON deserialization error - Raw message: '{}' | Contains 'missing field': {} | Contains 'unknown field': {}",
                error_msg,
                error_msg.contains("missing field"),
                error_msg.contains("unknown field")
            );

            // Try to extract field name from error message
            let field = extract_field_from_error(&error_msg);

            // Provide specific suggestions based on error type
            let (message, suggestion) = if error_msg.contains("missing field") {
                let field_name = field.clone().unwrap_or_else(|| "unknown".to_string());

                // Try to extract all mentioned fields from the error message
                let all_fields = extract_all_fields_from_error(&error_msg);

                let suggestion_text = if all_fields.is_empty() {
                    format!(
                        "Please provide the '{field_name}' field in your request. Please check the API documentation for the complete list of required fields for this endpoint."
                    )
                } else {
                    format!(
                        "Please provide the '{}' field in your request. The following fields appear to be required: {}. Please check the API documentation for the complete list of required fields.",
                        field_name,
                        all_fields.join(", ")
                    )
                };

                (
                    format!("Missing required field: {field_name}"),
                    Some(suggestion_text),
                )
            } else if error_msg.contains("unknown field") {
                let field_name = field.clone().unwrap_or_else(|| "unknown".to_string());

                // Try to extract expected fields from "expected one of `field1`, `field2`" pattern
                let expected_fields =
                    if let Some(expected_start) = error_msg.find("expected one of") {
                        extract_all_fields_from_error(&error_msg[expected_start..])
                    } else {
                        Vec::new()
                    };

                let suggestion_text = if expected_fields.is_empty() {
                    format!(
                        "The field '{field_name}' is not recognized. Please check the API documentation for valid field names"
                    )
                } else {
                    format!(
                        "The field '{}' is not recognized. Valid fields for this request are: {}",
                        field_name,
                        expected_fields.join(", ")
                    )
                };

                (
                    format!("Unknown field: {field_name}"),
                    Some(suggestion_text),
                )
            } else if error_msg.contains("invalid type") {
                let field_name = field.clone().unwrap_or_else(|| "unknown".to_string());
                (
                    format!("Invalid data type for field: {field_name}"),
                    Some(format!(
                        "The field '{field_name}' has the wrong data type. Please check that all fields have the correct data type (string, number, boolean, etc.)"
                    ))
                )
            } else if error_msg.contains("duplicate field") {
                (
                    "Duplicate field in JSON".to_string(),
                    Some(
                        "Please ensure each field appears only once in the JSON object".to_string(),
                    ),
                )
            } else {
                // Fallback for errors we haven't specifically handled
                // Try to extract any field information anyway
                let field_name = field.clone();
                let all_fields = extract_all_fields_from_error(&error_msg);

                warn!(
                    "Unhandled JSON error type. Error: '{}', Extracted field: {:?}, All fields: {:?}",
                    error_msg, field_name, all_fields
                );

                let suggestion_text = if !all_fields.is_empty() {
                    format!(
                        "Request validation failed. Fields mentioned in error: {}. Please check your JSON structure and ensure all required fields are present with correct data types.",
                        all_fields.join(", ")
                    )
                } else if let Some(ref f) = field_name {
                    format!(
                        "Issue with field '{f}'. Please check your JSON structure and field values."
                    )
                } else {
                    "Please check your JSON structure and field values. Ensure all required fields are present.".to_string()
                };

                (
                    format!("Invalid JSON data: {error_msg}"),
                    Some(suggestion_text),
                )
            };

            (message, field, suggestion)
        }
        JsonRejection::JsonSyntaxError(err) => {
            warn!("JSON syntax error: {}", err);
            (
                format!("JSON syntax error: {err}"),
                None,
                Some("Please ensure your JSON is properly formatted with matching brackets, quotes, and commas".to_string())
            )
        }
        JsonRejection::MissingJsonContentType(_) => {
            warn!("Missing Content-Type header for JSON request");
            (
                "Missing or invalid Content-Type header".to_string(),
                None,
                Some("Please set Content-Type header to 'application/json'".to_string()),
            )
        }
        _ => {
            warn!("JSON parsing failed: {}", rejection);
            (
                format!("Failed to parse JSON: {rejection}"),
                None,
                Some("Please check your request body and try again".to_string()),
            )
        }
    };

    ErrorResponse {
        error: ErrorInfo {
            code: "INVALID_JSON".to_string(),
            status: StatusCode::BAD_REQUEST.as_u16(),
            message,
            details: Some(ErrorDetails {
                field,
                message: "The request body contains invalid JSON data".to_string(),
                suggestion,
                action: None,
            }),
            timestamp: Timestamptz::now().to_rfc3339(),
            request_id: None,
        },
    }
}

/// Parse validation errors into a user-friendly error response
fn parse_validation_errors(errors: validator::ValidationErrors) -> ErrorResponse {
    let mut field_errors = Vec::new();
    let mut messages = Vec::new();

    for (field, field_errors_list) in errors.field_errors() {
        for error in field_errors_list {
            let message = error
                .message
                .as_ref()
                .map(std::string::ToString::to_string)
                .unwrap_or_else(|| {
                    // Generate a friendly message based on validation code
                    match error.code.as_ref() {
                        "email" => format!("'{field}' must be a valid email address"),
                        "length" => {
                            if let Some(min) = error.params.get("min") {
                                format!("'{field}' must be at least {min} characters long")
                            } else if let Some(max) = error.params.get("max") {
                                format!("'{field}' must be at most {max} characters long")
                            } else {
                                format!("'{field}' has invalid length")
                            }
                        }
                        "range" => {
                            if let Some(min) = error.params.get("min") {
                                if let Some(max) = error.params.get("max") {
                                    format!("'{field}' must be between {min} and {max}")
                                } else {
                                    format!("'{field}' must be at least {min}")
                                }
                            } else if let Some(max) = error.params.get("max") {
                                format!("'{field}' must be at most {max}")
                            } else {
                                format!("'{field}' is out of range")
                            }
                        }
                        "url" => format!("'{field}' must be a valid URL"),
                        "required" => format!("'{field}' is required"),
                        "regex" => format!("'{field}' format is invalid"),
                        "must_match" => format!("'{field}' does not match required format"),
                        _ => format!("'{field}' is invalid"),
                    }
                });

            messages.push(message.clone());
            field_errors.push((field.to_string(), message));
        }
    }

    // Create a combined message
    let message = if field_errors.len() == 1 {
        field_errors[0].1.clone()
    } else {
        format!("Validation failed for {} fields", field_errors.len())
    };

    // Get the first field error for the details
    let (first_field, first_message) = field_errors
        .first()
        .cloned()
        .unwrap_or_else(|| ("unknown".to_string(), "Validation failed".to_string()));

    ErrorResponse {
        error: ErrorInfo {
            code: "VALIDATION_ERROR".to_string(),
            status: StatusCode::BAD_REQUEST.as_u16(),
            message,
            details: Some(ErrorDetails {
                field: Some(first_field),
                message: first_message,
                suggestion: Some("Please correct the validation errors and try again".to_string()),
                action: None,
            }),
            timestamp: Timestamptz::now().to_rfc3339(),
            request_id: None,
        },
    }
}

/// Extract field name from error message
///
/// Tries to extract the field name from Serde's error messages like:
/// - "missing field `email` at line 1 column 2"
/// - "invalid type: null, expected a string at line 1 column 10"
/// - "unknown field `extra_field`, expected one of `name`, `slug`, `description`, `plan`"
fn extract_all_fields_from_error(error_msg: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut search_pos = 0;

    // Look for all field names in backticks throughout the error message
    while let Some(start) = error_msg[search_pos..].find('`') {
        let actual_start = search_pos + start;
        let remaining = &error_msg[actual_start + 1..];

        if let Some(end) = remaining.find('`') {
            let field_name = remaining[..end].to_string();
            // Only add if it looks like a field name (alphanumeric + underscore)
            if !field_name.is_empty()
                && field_name.chars().all(|c| c.is_alphanumeric() || c == '_')
                && !fields.contains(&field_name)
            {
                fields.push(field_name);
            }
            search_pos = actual_start + end + 2;
        } else {
            break;
        }
    }

    fields
}

fn extract_field_from_error(error_msg: &str) -> Option<String> {
    // Pattern 1: "missing field `fieldname`"
    if let Some(start) = error_msg.find("field `") {
        let remaining = &error_msg[start + 7..];
        if let Some(end) = remaining.find('`') {
            return Some(remaining[..end].to_string());
        }
    }

    // Pattern 2: "unknown field `fieldname`"
    if let Some(start) = error_msg.find("unknown field `") {
        let remaining = &error_msg[start + 15..];
        if let Some(end) = remaining.find('`') {
            let field = remaining[..end].to_string();
            // Also extract expected fields if available
            if let Some(expected_start) = error_msg.find("expected one of") {
                use tracing::info;
                info!(
                    "Unknown field '{}', error details: {}",
                    field,
                    &error_msg[expected_start..]
                );
            }
            return Some(field);
        }
    }

    // Pattern 3: Look for field name in quotes
    if let Some(start) = error_msg.find('"') {
        let remaining = &error_msg[start + 1..];
        if let Some(end) = remaining.find('"') {
            return Some(remaining[..end].to_string());
        }
    }

    // Pattern 4: Try to find field name after "for field" or "in field"
    for pattern in &["for field ", "in field "] {
        if let Some(start) = error_msg.find(pattern) {
            let remaining = &error_msg[start + pattern.len()..];
            // Get the next word (field name)
            let field_name: String = remaining
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !field_name.is_empty() {
                return Some(field_name);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_field_from_error() {
        // Test missing field pattern
        let error = "missing field `email` at line 1 column 2";
        assert_eq!(extract_field_from_error(error), Some("email".to_string()));

        // Test quoted field pattern
        let error = "invalid type for field \"username\"";
        assert_eq!(
            extract_field_from_error(error),
            Some("username".to_string())
        );

        // Test no field found
        let error = "some other error";
        assert_eq!(extract_field_from_error(error), None);
    }

    #[test]
    fn test_audit_context_creation() {
        let ctx = AuditContext {
            ip_address: Some("192.168.1.1".to_string()),
            user_agent: Some("Mozilla/5.0".to_string()),
            request_id: Uuid::new_v4(),
        };

        assert!(ctx.ip_address.is_some());
        assert_eq!(ctx.ip_address.unwrap(), "192.168.1.1");
        assert!(ctx.user_agent.is_some());
    }
}
