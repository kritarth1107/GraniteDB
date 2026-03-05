// ============================================================================
// GraniteDB — Server & Engine Configuration
// ============================================================================
// All tunable knobs for GraniteDB live here. Supports sensible defaults
// with the ability to override via CLI flags or config files.
// ============================================================================

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Master configuration for the GraniteDB server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraniteConfig {
    /// Server configuration
    pub server: ServerConfig,
    /// Storage engine configuration
    pub storage: StorageConfig,
    /// Authentication / authorization configuration
    pub auth: AuthConfig,
    /// Replication configuration
    pub replication: ReplicationConfig,
    /// Sharding configuration
    pub sharding: ShardingConfig,
    /// Logging configuration
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Host address to bind to
    pub host: String,
    /// TCP port for the wire protocol
    pub port: u16,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,
    /// Worker thread count (0 = auto-detect)
    pub worker_threads: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Root data directory
    pub data_dir: PathBuf,
    /// WAL directory (defaults to `data_dir/wal`)
    pub wal_dir: PathBuf,
    /// Page size in bytes (default 16 KB)
    pub page_size: usize,
    /// Buffer pool size in pages
    pub buffer_pool_pages: usize,
    /// Enable WAL fsync for durability
    pub wal_fsync: bool,
    /// WAL segment max size in bytes (default 64 MB)
    pub wal_segment_size: usize,
    /// Enable encryption at rest
    pub encryption_at_rest: bool,
    /// Compaction interval in seconds
    pub compaction_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Enable authentication
    pub enabled: bool,
    /// Default admin username
    pub admin_user: String,
    /// Path to the users database file
    pub users_file: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Enable replication
    pub enabled: bool,
    /// Role: "primary" or "secondary"
    pub role: String,
    /// Primary host (for secondaries)
    pub primary_host: Option<String>,
    /// Primary port (for secondaries)
    pub primary_port: Option<u16>,
    /// Oplog max size in MB
    pub oplog_size_mb: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardingConfig {
    /// Enable sharding
    pub enabled: bool,
    /// Shard key field name
    pub shard_key: Option<String>,
    /// Number of virtual shards
    pub num_shards: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level: trace, debug, info, warn, error
    pub level: String,
    /// Log output: stdout, file, or both
    pub output: String,
    /// Log file path (if output includes "file")
    pub log_file: PathBuf,
    /// Enable JSON structured logging
    pub json_format: bool,
}

impl Default for GraniteConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            auth: AuthConfig::default(),
            replication: ReplicationConfig::default(),
            sharding: ShardingConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6380,
            max_connections: 10_000,
            connection_timeout_secs: 30,
            worker_threads: 0,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        let data_dir = PathBuf::from("./data/granite");
        let wal_dir = data_dir.join("wal");
        Self {
            data_dir,
            wal_dir,
            page_size: 16 * 1024, // 16 KB
            buffer_pool_pages: 4096,
            wal_fsync: true,
            wal_segment_size: 64 * 1024 * 1024, // 64 MB
            encryption_at_rest: false,
            compaction_interval_secs: 300,
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            admin_user: "admin".to_string(),
            users_file: PathBuf::from("./data/granite/users.json"),
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            role: "primary".to_string(),
            primary_host: None,
            primary_port: None,
            oplog_size_mb: 256,
        }
    }
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shard_key: None,
            num_shards: 4,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            output: "stdout".to_string(),
            log_file: PathBuf::from("./data/granite/granitedb.log"),
            json_format: false,
        }
    }
}

impl GraniteConfig {
    /// Load configuration from a JSON file, falling back to defaults.
    pub fn load_from_file(path: &std::path::Path) -> crate::error::GraniteResult<Self> {
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            let config: GraniteConfig =
                serde_json::from_str(&content).map_err(|e| {
                    crate::error::GraniteError::ConfigError(format!(
                        "Failed to parse config: {}",
                        e
                    ))
                })?;
            Ok(config)
        } else {
            Ok(Self::default())
        }
    }

    /// Save current configuration to a JSON file.
    pub fn save_to_file(&self, path: &std::path::Path) -> crate::error::GraniteResult<()> {
        let content = serde_json::to_string_pretty(self)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, content)?;
        Ok(())
    }
}
