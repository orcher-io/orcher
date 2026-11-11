//! Security configuration (JWT, encryption, audit, access control).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SecurityConfig {
    /// JWT authentication configuration
    pub jwt: JwtConfig,

    /// Encryption configuration
    pub encryption: EncryptionConfig,

    /// Audit logging configuration
    pub audit: AuditConfig,

    /// Access control configuration
    pub access_control: AccessControlConfig,

    /// API security configuration
    pub api_security: ApiSecurityConfig,

    /// Session management configuration
    pub session: SessionConfig,
}

/// JWT authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    /// Enable JWT authentication
    pub enabled: bool,

    /// JWT secret key path
    pub secret_key_path: Option<PathBuf>,

    /// JWT algorithm
    pub algorithm: JwtAlgorithm,

    /// Token expiration time in seconds
    pub expiration_secs: u64,

    /// Refresh token expiration time in seconds
    pub refresh_expiration_secs: u64,

    /// JWT issuer
    pub issuer: String,

    /// JWT audience
    pub audience: Vec<String>,

    /// Allow refresh tokens
    pub allow_refresh: bool,

    /// Maximum refresh count
    pub max_refresh_count: u32,

    /// Include user claims in token
    pub include_user_claims: bool,
}

/// Encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// Enable field-level encryption
    pub field_encryption_enabled: bool,

    /// Encryption algorithm
    pub algorithm: EncryptionAlgorithm,

    /// Key derivation configuration
    pub key_derivation: KeyDerivationConfig,

    /// Encrypted field mappings
    pub encrypted_fields: HashMap<String, FieldEncryptionConfig>,

    /// Enable data masking
    pub data_masking_enabled: bool,

    /// Data masking rules
    pub masking_rules: Vec<MaskingRule>,
}

/// Audit logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Enable audit logging
    pub enabled: bool,

    /// Audit log level
    pub log_level: AuditLogLevel,

    /// Audit log destination
    pub destination: AuditDestination,

    /// Events to audit
    pub events: Vec<AuditEvent>,

    /// Include request/response bodies
    pub include_bodies: bool,

    /// Include user information
    pub include_user_info: bool,

    /// Include IP addresses
    pub include_ip_addresses: bool,

    /// Audit log retention days
    pub retention_days: u32,

    /// Sensitive fields to exclude
    pub exclude_fields: Vec<String>,
}

/// Access control configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessControlConfig {
    /// Enable role-based access control
    pub rbac_enabled: bool,

    /// Default roles
    pub default_roles: Vec<String>,

    /// Permission mapping
    pub permissions: HashMap<String, Vec<Permission>>,

    /// Resource-based permissions
    pub resource_permissions: HashMap<String, ResourcePermission>,

    /// Enable attribute-based access control
    pub abac_enabled: bool,

    /// Policy evaluation engine
    pub policy_engine: PolicyEngine,
}

/// API security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiSecurityConfig {
    /// Enable API key authentication
    pub api_keys_enabled: bool,

    /// API key header name
    pub api_key_header: String,

    /// Rate limiting per API key
    pub rate_limiting: ApiRateLimiting,

    /// Request signature validation
    pub signature_validation: SignatureValidation,

    /// IP allowlist
    pub ip_allowlist: Vec<String>,

    /// IP blocklist
    pub ip_blocklist: Vec<String>,

    /// Enable CORS security
    pub cors_security_enabled: bool,
}

/// Session management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    /// Enable session management
    pub enabled: bool,

    /// Session timeout in seconds
    pub timeout_secs: u64,

    /// Session storage type
    pub storage_type: SessionStorageType,

    /// Session cookie configuration
    pub cookie: SessionCookieConfig,

    /// Enable session rotation
    pub rotation_enabled: bool,

    /// Session rotation interval in seconds
    pub rotation_interval_secs: u64,
}

/// JWT algorithm enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JwtAlgorithm {
    /// HMAC SHA-256
    HS256,
    /// HMAC SHA-384
    HS384,
    /// HMAC SHA-512
    HS512,
    /// RSA SHA-256
    RS256,
    /// RSA SHA-384
    RS384,
    /// RSA SHA-512
    RS512,
    /// ECDSA SHA-256
    ES256,
    /// ECDSA SHA-384
    ES384,
    /// ECDSA SHA-512
    ES512,
}

/// Encryption algorithm enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM
    AES256GCM,
    /// AES-128-GCM
    AES128GCM,
    /// ChaCha20-Poly1305
    ChaCha20Poly1305,
    /// XChaCha20-Poly1305
    XChaCha20Poly1305,
}

/// Key derivation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDerivationConfig {
    /// Algorithm for key derivation
    pub algorithm: KeyDerivationAlgorithm,

    /// Salt length in bytes
    pub salt_length: u32,

    /// Number of iterations
    pub iterations: u32,

    /// Output key length in bytes
    pub key_length: u32,
}

/// Field encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldEncryptionConfig {
    /// Encryption enabled for this field
    pub enabled: bool,

    /// Field-specific algorithm
    pub algorithm: Option<EncryptionAlgorithm>,

    /// Field-specific key identifier
    pub key_id: Option<String>,
}

/// Data masking rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaskingRule {
    /// Field name to mask
    pub field_name: String,

    /// Masking type
    pub mask_type: MaskingType,

    /// Custom masking pattern
    pub pattern: Option<String>,
}

/// Audit log level enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditLogLevel {
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warning level
    Warning,
    /// Error level
    Error,
    /// Critical level
    Critical,
}

/// Audit destination enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditDestination {
    /// File-based logging
    File(PathBuf),
    /// Database logging
    Database,
    /// Syslog
    Syslog,
    /// External service
    External {
        /// External service URL
        url: String,
        /// Additional headers for external service
        headers: HashMap<String, String>,
    },
}

/// Audit event enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditEvent {
    /// Authentication events
    Authentication,
    /// Authorization events
    Authorization,
    /// Data access events
    DataAccess,
    /// Configuration changes
    ConfigurationChange,
    /// System events
    SystemEvent,
    /// User management events
    UserManagement,
    /// API calls
    ApiCall,
}

/// Permission enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Permission {
    /// Read permission
    Read,
    /// Write permission
    Write,
    /// Delete permission
    Delete,
    /// Execute permission
    Execute,
    /// Admin permission
    Admin,
    /// Custom permission
    Custom(String),
}

/// Resource permission configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcePermission {
    /// Resource identifier
    pub resource_id: String,

    /// Required permissions
    pub required_permissions: Vec<Permission>,

    /// Resource-specific policies
    pub policies: Vec<String>,
}

/// Policy engine enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyEngine {
    /// Simple rule-based engine
    RuleBased,
    /// Open Policy Agent
    OPA,
    /// Custom policy engine
    Custom(String),
}

/// API rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiRateLimiting {
    /// Enable rate limiting per API key
    pub enabled: bool,

    /// Requests per minute per API key
    pub requests_per_minute: u32,

    /// Burst capacity
    pub burst_capacity: u32,
}

/// Signature validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignatureValidation {
    /// Enable signature validation
    pub enabled: bool,

    /// Signature algorithm
    pub algorithm: SignatureAlgorithm,

    /// Signature header name
    pub header_name: String,

    /// Clock skew tolerance in seconds
    pub clock_skew_tolerance_secs: u64,
}

/// Session storage type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionStorageType {
    /// In-memory storage
    Memory,
    /// Redis storage
    Redis,
    /// Database storage
    Database,
}

/// Session cookie configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionCookieConfig {
    /// Cookie name
    pub name: String,

    /// Cookie domain
    pub domain: Option<String>,

    /// Cookie path
    pub path: String,

    /// Secure flag
    pub secure: bool,

    /// HTTP-only flag
    pub http_only: bool,

    /// SameSite policy
    pub same_site: SameSitePolicy,
}

/// Key derivation algorithm enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyDerivationAlgorithm {
    /// PBKDF2
    PBKDF2,
    /// Argon2
    Argon2,
    /// Scrypt
    Scrypt,
}

/// Masking type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaskingType {
    /// Replace with asterisks
    Asterisk,
    /// Replace with X characters
    X,
    /// Show only first N characters
    FirstN(u32),
    /// Show only last N characters
    LastN(u32),
    /// Custom pattern
    Custom,
}

/// Signature algorithm enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignatureAlgorithm {
    /// HMAC SHA-256
    HMACSHA256,
    /// HMAC SHA-512
    HMACSHA512,
    /// RSA SHA-256
    RSASHA256,
    /// ECDSA SHA-256
    ECDSASHA256,
}

/// SameSite policy enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SameSitePolicy {
    /// Strict policy
    Strict,
    /// Lax policy
    Lax,
    /// None policy
    None,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            secret_key_path: None,
            algorithm: JwtAlgorithm::HS256,
            expiration_secs: 3600,          // 1 hour
            refresh_expiration_secs: 86400, // 24 hours
            issuer: "orcher".to_string(),
            audience: vec!["orcher-api".to_string()],
            allow_refresh: true,
            max_refresh_count: 5,
            include_user_claims: true,
        }
    }
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            field_encryption_enabled: false,
            algorithm: EncryptionAlgorithm::AES256GCM,
            key_derivation: KeyDerivationConfig {
                algorithm: KeyDerivationAlgorithm::PBKDF2,
                salt_length: 32,
                iterations: 100_000,
                key_length: 32,
            },
            encrypted_fields: HashMap::new(),
            data_masking_enabled: true,
            masking_rules: vec![
                MaskingRule {
                    field_name: "password".to_string(),
                    mask_type: MaskingType::Asterisk,
                    pattern: None,
                },
                MaskingRule {
                    field_name: "email".to_string(),
                    mask_type: MaskingType::FirstN(3),
                    pattern: None,
                },
            ],
        }
    }
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_level: AuditLogLevel::Info,
            destination: AuditDestination::File(PathBuf::from("./logs/audit.log")),
            events: vec![
                AuditEvent::Authentication,
                AuditEvent::Authorization,
                AuditEvent::DataAccess,
                AuditEvent::ConfigurationChange,
            ],
            include_bodies: false,
            include_user_info: true,
            include_ip_addresses: true,
            retention_days: 90,
            exclude_fields: vec![
                "password".to_string(),
                "secret".to_string(),
                "token".to_string(),
            ],
        }
    }
}

impl Default for AccessControlConfig {
    fn default() -> Self {
        let mut permissions = HashMap::new();
        permissions.insert("admin".to_string(), vec![Permission::Admin]);
        permissions.insert(
            "user".to_string(),
            vec![Permission::Read, Permission::Write],
        );
        permissions.insert("viewer".to_string(), vec![Permission::Read]);

        Self {
            rbac_enabled: true,
            default_roles: vec!["user".to_string()],
            permissions,
            resource_permissions: HashMap::new(),
            abac_enabled: false,
            policy_engine: PolicyEngine::RuleBased,
        }
    }
}

impl Default for ApiSecurityConfig {
    fn default() -> Self {
        Self {
            api_keys_enabled: false,
            api_key_header: "X-API-Key".to_string(),
            rate_limiting: ApiRateLimiting {
                enabled: true,
                requests_per_minute: 1000,
                burst_capacity: 2000,
            },
            signature_validation: SignatureValidation {
                enabled: false,
                algorithm: SignatureAlgorithm::HMACSHA256,
                header_name: "X-Signature".to_string(),
                clock_skew_tolerance_secs: 300,
            },
            ip_allowlist: vec![],
            ip_blocklist: vec![],
            cors_security_enabled: true,
        }
    }
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout_secs: 1800, // 30 minutes
            storage_type: SessionStorageType::Memory,
            cookie: SessionCookieConfig {
                name: "orcher_session".to_string(),
                domain: None,
                path: "/".to_string(),
                secure: true,
                http_only: true,
                same_site: SameSitePolicy::Strict,
            },
            rotation_enabled: true,
            rotation_interval_secs: 3600, // 1 hour
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_security_config_default() {
        let config = SecurityConfig::default();
        assert!(config.jwt.enabled);
        assert!(!config.encryption.field_encryption_enabled);
        assert!(config.audit.enabled);
        assert!(config.access_control.rbac_enabled);
        assert!(!config.api_security.api_keys_enabled);
        assert!(config.session.enabled);
    }

    #[test]
    fn test_jwt_config_default() {
        let config = JwtConfig::default();
        assert!(config.enabled);
        assert_eq!(config.expiration_secs, 3600);
        assert_eq!(config.issuer, "orcher");
        assert!(matches!(config.algorithm, JwtAlgorithm::HS256));
        assert!(config.allow_refresh);
    }

    #[test]
    fn test_encryption_config_default() {
        let config = EncryptionConfig::default();
        assert!(!config.field_encryption_enabled);
        assert!(matches!(config.algorithm, EncryptionAlgorithm::AES256GCM));
        assert!(config.data_masking_enabled);
        assert_eq!(config.masking_rules.len(), 2);
    }

    #[test]
    fn test_audit_config_default() {
        let config = AuditConfig::default();
        assert!(config.enabled);
        assert!(matches!(config.log_level, AuditLogLevel::Info));
        assert_eq!(config.events.len(), 4);
        assert!(config.include_user_info);
        assert_eq!(config.retention_days, 90);
    }

    #[test]
    fn test_access_control_config_default() {
        let config = AccessControlConfig::default();
        assert!(config.rbac_enabled);
        assert!(!config.abac_enabled);
        assert_eq!(config.default_roles.len(), 1);
        assert!(config.permissions.contains_key("admin"));
        assert!(matches!(config.policy_engine, PolicyEngine::RuleBased));
    }

    #[test]
    fn test_api_security_config_default() {
        let config = ApiSecurityConfig::default();
        assert!(!config.api_keys_enabled);
        assert!(config.rate_limiting.enabled);
        assert_eq!(config.api_key_header, "X-API-Key");
        assert!(config.cors_security_enabled);
    }

    #[test]
    fn test_session_config_default() {
        let config = SessionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.timeout_secs, 1800);
        assert!(matches!(config.storage_type, SessionStorageType::Memory));
        assert!(config.cookie.secure);
        assert!(config.rotation_enabled);
    }

    #[test]
    fn test_jwt_algorithms() {
        let algorithms = vec![
            JwtAlgorithm::HS256,
            JwtAlgorithm::HS384,
            JwtAlgorithm::HS512,
            JwtAlgorithm::RS256,
            JwtAlgorithm::ES256,
        ];

        for algorithm in algorithms {
            let json = serde_json::to_string(&algorithm).unwrap();
            let deserialized: JwtAlgorithm = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                JwtAlgorithm::HS256
                    | JwtAlgorithm::HS384
                    | JwtAlgorithm::HS512
                    | JwtAlgorithm::RS256
                    | JwtAlgorithm::ES256
                    | JwtAlgorithm::RS384
                    | JwtAlgorithm::RS512
                    | JwtAlgorithm::ES384
                    | JwtAlgorithm::ES512
            ));
        }
    }

    #[test]
    fn test_audit_events() {
        let events = vec![
            AuditEvent::Authentication,
            AuditEvent::Authorization,
            AuditEvent::DataAccess,
            AuditEvent::ConfigurationChange,
            AuditEvent::SystemEvent,
        ];

        for event in events {
            let json = serde_json::to_string(&event).unwrap();
            let deserialized: AuditEvent = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                AuditEvent::Authentication
                    | AuditEvent::Authorization
                    | AuditEvent::DataAccess
                    | AuditEvent::ConfigurationChange
                    | AuditEvent::SystemEvent
                    | AuditEvent::UserManagement
                    | AuditEvent::ApiCall
            ));
        }
    }

    #[test]
    fn test_permissions() {
        let permissions = vec![
            Permission::Read,
            Permission::Write,
            Permission::Delete,
            Permission::Execute,
            Permission::Admin,
        ];

        for permission in permissions {
            let json = serde_json::to_string(&permission).unwrap();
            let deserialized: Permission = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                Permission::Read
                    | Permission::Write
                    | Permission::Delete
                    | Permission::Execute
                    | Permission::Admin
                    | Permission::Custom(_)
            ));
        }
    }

    #[test]
    fn test_masking_types() {
        let types = vec![
            MaskingType::Asterisk,
            MaskingType::X,
            MaskingType::FirstN(5),
            MaskingType::LastN(3),
            MaskingType::Custom,
        ];

        for mask_type in types {
            let json = serde_json::to_string(&mask_type).unwrap();
            let deserialized: MaskingType = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                MaskingType::Asterisk
                    | MaskingType::X
                    | MaskingType::FirstN(_)
                    | MaskingType::LastN(_)
                    | MaskingType::Custom
            ));
        }
    }

    #[test]
    fn test_config_serialization() {
        let config = SecurityConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("jwt"));
        assert!(json.contains("encryption"));
        assert!(json.contains("audit"));
        assert!(json.contains("access_control"));

        let deserialized: SecurityConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.jwt.enabled, deserialized.jwt.enabled);
        assert_eq!(config.audit.enabled, deserialized.audit.enabled);
        assert_eq!(
            config.access_control.rbac_enabled,
            deserialized.access_control.rbac_enabled
        );
    }
}
