//! Storage configuration (database, Redis, pooling, backup, encryption).

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageConfig {
    /// Database configuration
    pub database: DatabaseConfig,

    /// Redis configuration
    pub redis: RedisConfig,

    /// Connection pooling configuration
    pub pooling: ConnectionPoolingConfig,

    /// Backup configuration
    pub backup: BackupConfig,

    /// Storage encryption configuration
    pub encryption: StorageEncryptionConfig,

    /// Performance configuration
    pub performance: StoragePerformanceConfig,
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// PostgreSQL connection URL
    pub postgres_url: String,

    /// Database name
    pub database_name: String,

    /// Host address
    pub host: String,

    /// Port number
    pub port: u16,

    /// Username
    pub username: String,

    /// Password (will be masked in logs)
    pub password: String,

    /// SSL mode
    pub ssl_mode: SslMode,

    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,

    /// Query timeout in seconds
    pub query_timeout_secs: u64,

    /// Statement timeout in seconds
    pub statement_timeout_secs: u64,

    /// Application name for connection tracking
    pub application_name: String,
}

/// Redis configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Redis connection URL
    pub url: String,

    /// Redis host
    pub host: String,

    /// Redis port
    pub port: u16,

    /// Redis password
    pub password: Option<String>,

    /// Redis database number
    pub database: u8,

    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,

    /// Command timeout in seconds
    pub command_timeout_secs: u64,

    /// Enable Redis cluster mode
    pub cluster_mode: bool,

    /// Redis cluster nodes (if cluster mode is enabled)
    pub cluster_nodes: Vec<String>,

    /// Key prefix for all Redis keys
    pub key_prefix: String,
}

/// Connection pooling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolingConfig {
    /// Enable connection pooling
    pub enabled: bool,

    /// Minimum pool size
    pub min_connections: u32,

    /// Maximum pool size
    pub max_connections: u32,

    /// Connection idle timeout in seconds
    pub idle_timeout_secs: u64,

    /// Maximum connection lifetime in seconds
    pub max_lifetime_secs: u64,

    /// Connection acquisition timeout in seconds
    pub acquire_timeout_secs: u64,

    /// Pool validation query
    pub validation_query: Option<String>,

    /// Test connections on borrow
    pub test_on_borrow: bool,

    /// Test connections while idle
    pub test_while_idle: bool,

    /// Pool statistics tracking
    pub enable_statistics: bool,
}

/// Backup configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    /// Enable automated backups
    pub enabled: bool,

    /// Backup schedule (cron expression)
    pub schedule: String,

    /// Backup storage location
    pub storage_location: BackupStorageLocation,

    /// Backup retention policy
    pub retention: BackupRetentionPolicy,

    /// Backup compression
    pub compression: BackupCompressionConfig,

    /// Backup encryption
    pub encryption_enabled: bool,

    /// Backup encryption key
    pub encryption_key_path: Option<PathBuf>,

    /// Include data in backups
    pub include_data: bool,

    /// Include schema in backups
    pub include_schema: bool,

    /// Backup verification
    pub verify_backups: bool,
}

/// Storage encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageEncryptionConfig {
    /// Enable encryption at rest
    pub encryption_at_rest: bool,

    /// Encryption algorithm
    pub algorithm: EncryptionAlgorithm,

    /// Key management service
    pub key_management: KeyManagementConfig,

    /// Encrypt sensitive fields
    pub encrypt_sensitive_fields: bool,

    /// List of fields to encrypt
    pub encrypted_fields: Vec<String>,
}

/// Storage performance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoragePerformanceConfig {
    /// Enable query caching
    pub query_cache_enabled: bool,

    /// Query cache TTL in seconds
    pub query_cache_ttl_secs: u64,

    /// Query cache size limit
    pub query_cache_size_mb: u64,

    /// Enable connection monitoring
    pub connection_monitoring: bool,

    /// Slow query threshold in milliseconds
    pub slow_query_threshold_ms: u64,

    /// Enable query logging
    pub query_logging: bool,

    /// Batch operation settings
    pub batch_operations: BatchOperationConfig,
}

/// SSL mode enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SslMode {
    /// Disable SSL
    Disable,
    /// Allow SSL
    Allow,
    /// Prefer SSL
    Prefer,
    /// Require SSL
    Require,
    /// Verify CA certificate
    VerifyCA,
    /// Verify full certificate
    VerifyFull,
}

/// Backup storage location
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackupStorageLocation {
    /// Local file system
    Local(PathBuf),
    /// AWS S3
    S3 {
        /// S3 bucket name
        bucket: String,
        /// AWS region
        region: String,
        /// Optional prefix for backup files
        prefix: Option<String>,
    },
    /// Google Cloud Storage
    GCS {
        /// GCS bucket name
        bucket: String,
        /// Optional prefix for backup files
        prefix: Option<String>,
    },
    /// Azure Blob Storage
    Azure {
        /// Azure container name
        container: String,
        /// Optional prefix for backup files
        prefix: Option<String>,
    },
}

/// Backup retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRetentionPolicy {
    /// Keep daily backups for this many days
    pub daily_retention_days: u32,

    /// Keep weekly backups for this many weeks
    pub weekly_retention_weeks: u32,

    /// Keep monthly backups for this many months
    pub monthly_retention_months: u32,

    /// Keep yearly backups for this many years
    pub yearly_retention_years: u32,
}

/// Backup compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupCompressionConfig {
    /// Enable compression
    pub enabled: bool,

    /// Compression algorithm
    pub algorithm: CompressionAlgorithm,

    /// Compression level (1-9)
    pub level: u8,
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
}

/// Key management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyManagementConfig {
    /// Key management service type
    pub service_type: KeyManagementService,

    /// Key rotation interval in days
    pub rotation_interval_days: u32,

    /// Enable automatic key rotation
    pub auto_rotation: bool,
}

/// Key management service enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyManagementService {
    /// Local key management
    Local,
    /// AWS KMS
    AwsKms {
        /// AWS region
        region: String,
        /// KMS key ID
        key_id: String,
    },
    /// Azure Key Vault
    AzureKeyVault {
        /// Key vault URL
        vault_url: String,
        /// Key name in vault
        key_name: String,
    },
    /// Google Cloud KMS
    GoogleKms {
        /// Google Cloud project ID
        project_id: String,
        /// KMS location
        location: String,
        /// Key ring name
        key_ring: String,
        /// Key name in ring
        key_name: String,
    },
}

/// Compression algorithm enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// Gzip compression
    Gzip,
    /// LZ4 compression
    LZ4,
    /// Zstd compression
    Zstd,
}

/// Batch operation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOperationConfig {
    /// Enable batch operations
    pub enabled: bool,

    /// Default batch size
    pub default_batch_size: u32,

    /// Maximum batch size
    pub max_batch_size: u32,

    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            postgres_url: "postgresql://localhost:5432/orcher".to_string(),
            database_name: "orcher".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            username: "orcher".to_string(),
            password: String::new(),
            ssl_mode: SslMode::Prefer,
            connection_timeout_secs: 30,
            query_timeout_secs: 60,
            statement_timeout_secs: 300,
            application_name: "orcher-config".to_string(),
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            host: "localhost".to_string(),
            port: 6379,
            password: None,
            database: 0,
            connection_timeout_secs: 10,
            command_timeout_secs: 5,
            cluster_mode: false,
            cluster_nodes: vec![],
            key_prefix: "orcher:".to_string(),
        }
    }
}

impl Default for ConnectionPoolingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_connections: 10,      // Keep warm connections ready
            max_connections: 50,      // Support multi-worker scenarios
            idle_timeout_secs: 600,   // 10 minutes - recycle idle connections
            max_lifetime_secs: 1800,  // 30 minutes - prevent stale connections
            acquire_timeout_secs: 30, // Fail fast on pool exhaustion
            validation_query: Some("SELECT 1".to_string()),
            test_on_borrow: true,    // Validate connections before use
            test_while_idle: true,   // Proactively test idle connections
            enable_statistics: true, // Enable metrics collection
        }
    }
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            schedule: "0 2 * * *".to_string(), // Daily at 2 AM
            storage_location: BackupStorageLocation::Local(PathBuf::from("./backups")),
            retention: BackupRetentionPolicy {
                daily_retention_days: 7,
                weekly_retention_weeks: 4,
                monthly_retention_months: 12,
                yearly_retention_years: 5,
            },
            compression: BackupCompressionConfig {
                enabled: true,
                algorithm: CompressionAlgorithm::Gzip,
                level: 6,
            },
            encryption_enabled: false,
            encryption_key_path: None,
            include_data: true,
            include_schema: true,
            verify_backups: true,
        }
    }
}

impl Default for StorageEncryptionConfig {
    fn default() -> Self {
        Self {
            encryption_at_rest: false,
            algorithm: EncryptionAlgorithm::AES256GCM,
            key_management: KeyManagementConfig {
                service_type: KeyManagementService::Local,
                rotation_interval_days: 90,
                auto_rotation: false,
            },
            encrypt_sensitive_fields: true,
            encrypted_fields: vec![
                "password".to_string(),
                "api_key".to_string(),
                "secret".to_string(),
                "token".to_string(),
            ],
        }
    }
}

impl Default for StoragePerformanceConfig {
    fn default() -> Self {
        Self {
            query_cache_enabled: true,
            query_cache_ttl_secs: 300, // 5 minutes
            query_cache_size_mb: 100,
            connection_monitoring: true,
            slow_query_threshold_ms: 1000, // 1 second
            query_logging: false,
            batch_operations: BatchOperationConfig {
                enabled: true,
                default_batch_size: 1000,
                max_batch_size: 10000,
                batch_timeout_ms: 5000,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        assert!(config.database.postgres_url.contains("postgresql"));
        assert!(config.redis.url.contains("redis"));
        assert!(config.pooling.enabled);
        assert!(config.backup.enabled);
        assert!(!config.encryption.encryption_at_rest);
        assert!(config.performance.query_cache_enabled);
    }

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        assert_eq!(config.database_name, "orcher");
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert!(matches!(config.ssl_mode, SslMode::Prefer));
    }

    #[test]
    fn test_redis_config_default() {
        let config = RedisConfig::default();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 6379);
        assert_eq!(config.database, 0);
        assert!(!config.cluster_mode);
        assert_eq!(config.key_prefix, "orcher:");
    }

    #[test]
    fn test_connection_pooling_config_default() {
        let config = ConnectionPoolingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_connections, 10); // Increased from 5 for better connection availability
        assert_eq!(config.max_connections, 50); // Increased from 20 to support multi-worker scenarios
        assert!(config.test_on_borrow);
        assert!(config.enable_statistics);
    }

    #[test]
    fn test_backup_config_default() {
        let config = BackupConfig::default();
        assert!(config.enabled);
        assert_eq!(config.schedule, "0 2 * * *");
        assert!(config.include_data);
        assert!(config.include_schema);
        assert!(config.verify_backups);
    }

    #[test]
    fn test_storage_encryption_config_default() {
        let config = StorageEncryptionConfig::default();
        assert!(!config.encryption_at_rest);
        assert!(matches!(config.algorithm, EncryptionAlgorithm::AES256GCM));
        assert!(config.encrypt_sensitive_fields);
        assert!(config.encrypted_fields.contains(&"password".to_string()));
    }

    #[test]
    fn test_storage_performance_config_default() {
        let config = StoragePerformanceConfig::default();
        assert!(config.query_cache_enabled);
        assert_eq!(config.query_cache_ttl_secs, 300);
        assert!(config.connection_monitoring);
        assert_eq!(config.slow_query_threshold_ms, 1000);
    }

    #[test]
    fn test_ssl_modes() {
        let modes = vec![
            SslMode::Disable,
            SslMode::Allow,
            SslMode::Prefer,
            SslMode::Require,
            SslMode::VerifyCA,
            SslMode::VerifyFull,
        ];

        for mode in modes {
            let json = serde_json::to_string(&mode).unwrap();
            let deserialized: SslMode = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                SslMode::Disable
                    | SslMode::Allow
                    | SslMode::Prefer
                    | SslMode::Require
                    | SslMode::VerifyCA
                    | SslMode::VerifyFull
            ));
        }
    }

    #[test]
    fn test_backup_storage_locations() {
        let local = BackupStorageLocation::Local(PathBuf::from("/tmp"));
        let s3 = BackupStorageLocation::S3 {
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            prefix: Some("backups/".to_string()),
        };

        let local_json = serde_json::to_string(&local).unwrap();
        let s3_json = serde_json::to_string(&s3).unwrap();

        let local_deserialized: BackupStorageLocation = serde_json::from_str(&local_json).unwrap();
        let s3_deserialized: BackupStorageLocation = serde_json::from_str(&s3_json).unwrap();

        assert!(matches!(
            local_deserialized,
            BackupStorageLocation::Local(_)
        ));
        assert!(matches!(s3_deserialized, BackupStorageLocation::S3 { .. }));
    }

    #[test]
    fn test_encryption_algorithms() {
        let algorithms = vec![
            EncryptionAlgorithm::AES256GCM,
            EncryptionAlgorithm::AES128GCM,
            EncryptionAlgorithm::ChaCha20Poly1305,
        ];

        for algorithm in algorithms {
            let json = serde_json::to_string(&algorithm).unwrap();
            let deserialized: EncryptionAlgorithm = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                EncryptionAlgorithm::AES256GCM
                    | EncryptionAlgorithm::AES128GCM
                    | EncryptionAlgorithm::ChaCha20Poly1305
            ));
        }
    }

    #[test]
    fn test_key_management_services() {
        let local = KeyManagementService::Local;
        let aws = KeyManagementService::AwsKms {
            region: "us-east-1".to_string(),
            key_id: "key-123".to_string(),
        };

        let local_json = serde_json::to_string(&local).unwrap();
        let aws_json = serde_json::to_string(&aws).unwrap();

        let local_deserialized: KeyManagementService = serde_json::from_str(&local_json).unwrap();
        let aws_deserialized: KeyManagementService = serde_json::from_str(&aws_json).unwrap();

        assert!(matches!(local_deserialized, KeyManagementService::Local));
        assert!(matches!(
            aws_deserialized,
            KeyManagementService::AwsKms { .. }
        ));
    }

    #[test]
    fn test_compression_algorithms() {
        let algorithms = vec![
            CompressionAlgorithm::Gzip,
            CompressionAlgorithm::LZ4,
            CompressionAlgorithm::Zstd,
        ];

        for algorithm in algorithms {
            let json = serde_json::to_string(&algorithm).unwrap();
            let deserialized: CompressionAlgorithm = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                CompressionAlgorithm::Gzip | CompressionAlgorithm::LZ4 | CompressionAlgorithm::Zstd
            ));
        }
    }

    #[test]
    fn test_config_serialization() {
        let config = StorageConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("database"));
        assert!(json.contains("redis"));
        assert!(json.contains("pooling"));
        assert!(json.contains("backup"));

        let deserialized: StorageConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(
            config.database.database_name,
            deserialized.database.database_name
        );
        assert_eq!(config.redis.host, deserialized.redis.host);
        assert_eq!(config.pooling.enabled, deserialized.pooling.enabled);
        assert_eq!(config.backup.enabled, deserialized.backup.enabled);
    }
}
