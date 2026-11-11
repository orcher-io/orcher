//! # S3 Configuration
//!
//! Configuration types for S3 storage backend.

use orcher_common::{error::OrcherError, OrcherResult};
use serde::{Deserialize, Serialize};

/// S3 storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,

    /// AWS region (e.g., "us-east-1")
    pub region: String,

    /// AWS access key ID
    pub access_key: String,

    /// AWS secret access key
    pub secret_key: String,

    /// Optional custom endpoint URL (for S3-compatible services like MinIO)
    pub endpoint_url: Option<String>,
}

impl S3Config {
    /// Create a new S3 configuration
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `region` - AWS region
    /// * `access_key` - AWS access key ID
    /// * `secret_key` - AWS secret access key
    ///
    /// # Returns
    /// * `Ok(S3Config)` - Configuration created successfully
    /// * `Err(OrcherError)` - Validation error
    pub fn new(
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> OrcherResult<Self> {
        let config = Self {
            bucket: bucket.into(),
            region: region.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            endpoint_url: None,
        };

        config.validate()?;
        Ok(config)
    }

    /// Create S3 configuration with custom endpoint
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `region` - AWS region
    /// * `access_key` - AWS access key ID
    /// * `secret_key` - AWS secret access key
    /// * `endpoint_url` - Custom endpoint URL
    pub fn new_with_endpoint(
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        endpoint_url: impl Into<String>,
    ) -> OrcherResult<Self> {
        let config = Self {
            bucket: bucket.into(),
            region: region.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            endpoint_url: Some(endpoint_url.into()),
        };

        config.validate()?;
        Ok(config)
    }

    /// Create S3 configuration from environment variables
    ///
    /// Required environment variables:
    /// - `S3_BUCKET` - S3 bucket name
    /// - `S3_REGION` - AWS region
    /// - `AWS_ACCESS_KEY_ID` - AWS access key ID
    /// - `AWS_SECRET_ACCESS_KEY` - AWS secret access key
    ///
    /// Optional environment variables:
    /// - `S3_ENDPOINT_URL` - Custom endpoint URL
    ///
    /// # Returns
    /// * `Ok(S3Config)` - Configuration created from environment
    /// * `Err(OrcherError)` - Missing or invalid environment variable
    pub fn from_env() -> OrcherResult<Self> {
        let bucket = std::env::var("S3_BUCKET")
            .map_err(|_| OrcherError::invalid_input("S3_BUCKET environment variable not set"))?;

        let region = std::env::var("S3_REGION")
            .map_err(|_| OrcherError::invalid_input("S3_REGION environment variable not set"))?;

        let access_key = std::env::var("AWS_ACCESS_KEY_ID").map_err(|_| {
            OrcherError::invalid_input("AWS_ACCESS_KEY_ID environment variable not set")
        })?;

        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| {
            OrcherError::invalid_input("AWS_SECRET_ACCESS_KEY environment variable not set")
        })?;

        let endpoint_url = std::env::var("S3_ENDPOINT_URL").ok();

        let config = Self {
            bucket,
            region,
            access_key,
            secret_key,
            endpoint_url,
        };

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration
    ///
    /// # Returns
    /// * `Ok(())` - Configuration is valid
    /// * `Err(OrcherError)` - Validation error
    pub fn validate(&self) -> OrcherResult<()> {
        if self.bucket.is_empty() {
            return Err(OrcherError::invalid_input_field(
                "bucket cannot be empty",
                "bucket",
            ));
        }

        if self.region.is_empty() {
            return Err(OrcherError::invalid_input_field(
                "region cannot be empty",
                "region",
            ));
        }

        if self.access_key.is_empty() {
            return Err(OrcherError::invalid_input_field(
                "access_key cannot be empty",
                "access_key",
            ));
        }

        if self.secret_key.is_empty() {
            return Err(OrcherError::invalid_input_field(
                "secret_key cannot be empty",
                "secret_key",
            ));
        }

        // Validate endpoint URL format if provided
        if let Some(ref endpoint) = self.endpoint_url {
            if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                return Err(OrcherError::invalid_input_field(
                    "endpoint_url must start with http:// or https://",
                    "endpoint_url",
                ));
            }
        }

        Ok(())
    }

    /// Get the S3 endpoint URL
    ///
    /// Returns the custom endpoint URL if set, otherwise constructs the default AWS S3 endpoint.
    pub fn endpoint(&self) -> String {
        self.endpoint_url
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.region))
    }

    /// Check if using a custom endpoint (S3-compatible service)
    pub fn is_custom_endpoint(&self) -> bool {
        self.endpoint_url.is_some()
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: "orcher-workflows".to_string(),
            region: "us-east-1".to_string(),
            access_key: String::new(),
            secret_key: String::new(),
            endpoint_url: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_new() {
        let config = S3Config::new("test-bucket", "us-west-2", "AKIA123", "secret123").unwrap();

        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.region, "us-west-2");
        assert_eq!(config.access_key, "AKIA123");
        assert_eq!(config.secret_key, "secret123");
        assert_eq!(config.endpoint_url, None);
    }

    #[test]
    fn test_s3_config_with_endpoint() {
        let config = S3Config::new_with_endpoint(
            "test-bucket",
            "us-west-2",
            "AKIA123",
            "secret123",
            "http://localhost:9000",
        )
        .unwrap();

        assert_eq!(
            config.endpoint_url,
            Some("http://localhost:9000".to_string())
        );
        assert!(config.is_custom_endpoint());
    }

    #[test]
    fn test_s3_config_validation_empty_bucket() {
        let result = S3Config::new("", "us-west-2", "AKIA123", "secret123");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_config_validation_empty_region() {
        let result = S3Config::new("test-bucket", "", "AKIA123", "secret123");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_config_validation_empty_access_key() {
        let result = S3Config::new("test-bucket", "us-west-2", "", "secret123");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_config_validation_empty_secret_key() {
        let result = S3Config::new("test-bucket", "us-west-2", "AKIA123", "");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_config_endpoint() {
        let config = S3Config::new("test-bucket", "us-west-2", "AKIA123", "secret123").unwrap();

        assert_eq!(config.endpoint(), "https://s3.us-west-2.amazonaws.com");
    }

    #[test]
    fn test_s3_config_custom_endpoint() {
        let config = S3Config::new_with_endpoint(
            "test-bucket",
            "us-west-2",
            "AKIA123",
            "secret123",
            "http://localhost:9000",
        )
        .unwrap();

        assert_eq!(config.endpoint(), "http://localhost:9000");
    }

    #[test]
    fn test_s3_config_default() {
        let config = S3Config::default();
        assert_eq!(config.bucket, "orcher-workflows");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.endpoint_url, None);
    }

    #[test]
    fn test_s3_config_invalid_endpoint() {
        let result = S3Config::new_with_endpoint(
            "test-bucket",
            "us-west-2",
            "AKIA123",
            "secret123",
            "invalid-url",
        );
        assert!(result.is_err());
    }
}
