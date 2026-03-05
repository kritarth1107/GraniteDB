// ============================================================================
// GraniteDB — Error Module
// ============================================================================
// Centralized error types for the entire database engine.
// Uses `thiserror` for ergonomic, typed error handling.
// ============================================================================

use thiserror::Error;

/// Top-level result type used across GraniteDB.
pub type GraniteResult<T> = Result<T, GraniteError>;

/// Primary error enum for GraniteDB.
#[derive(Error, Debug)]
pub enum GraniteError {
    // ── Storage Errors ───────────────────────────────────────────────
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Page corrupted: page_id={page_id}, expected_crc={expected_crc}, actual_crc={actual_crc}")]
    PageCorruption {
        page_id: u64,
        expected_crc: u32,
        actual_crc: u32,
    },

    #[error("Buffer pool exhausted: capacity={capacity}")]
    BufferPoolExhausted { capacity: usize },

    // ── Document Errors ──────────────────────────────────────────────
    #[error("Document not found: id={0}")]
    DocumentNotFound(String),

    #[error("Document validation failed: {0}")]
    ValidationError(String),

    #[error("Invalid BSON value: {0}")]
    InvalidBsonValue(String),

    #[error("Duplicate key: collection={collection}, key={key}")]
    DuplicateKey { collection: String, key: String },

    // ── Collection Errors ────────────────────────────────────────────
    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Collection already exists: {0}")]
    CollectionAlreadyExists(String),

    // ── Database Errors ──────────────────────────────────────────────
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),

    #[error("Database already exists: {0}")]
    DatabaseAlreadyExists(String),

    // ── Query Errors ─────────────────────────────────────────────────
    #[error("Query parse error: {0}")]
    QueryParseError(String),

    #[error("Query execution error: {0}")]
    QueryExecutionError(String),

    #[error("Invalid operator: {0}")]
    InvalidOperator(String),

    #[error("Type mismatch in query: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    // ── Index Errors ─────────────────────────────────────────────────
    #[error("Index not found: {0}")]
    IndexNotFound(String),

    #[error("Index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("Index build failed: {0}")]
    IndexBuildFailed(String),

    // ── Transaction Errors ───────────────────────────────────────────
    #[error("Transaction conflict: txn_id={0}")]
    TransactionConflict(String),

    #[error("Transaction aborted: {0}")]
    TransactionAborted(String),

    #[error("Transaction timeout: txn_id={0}")]
    TransactionTimeout(String),

    // ── Auth Errors ──────────────────────────────────────────────────
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Authorization denied: user={user}, action={action}")]
    AuthorizationDenied { user: String, action: String },

    #[error("User not found: {0}")]
    UserNotFound(String),

    #[error("User already exists: {0}")]
    UserAlreadyExists(String),

    // ── Network Errors ───────────────────────────────────────────────
    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Connection closed")]
    ConnectionClosed,

    // ── Replication / Sharding ───────────────────────────────────────
    #[error("Replication error: {0}")]
    ReplicationError(String),

    #[error("Shard not found: {0}")]
    ShardNotFound(String),

    // ── Encryption ───────────────────────────────────────────────────
    #[error("Encryption error: {0}")]
    EncryptionError(String),

    #[error("Decryption error: {0}")]
    DecryptionError(String),

    // ── Vector / AI Errors ───────────────────────────────────────────
    #[error("Invalid vector: {0}")]
    InvalidVector(String),

    #[error("Vector dimension mismatch: expected {expected}, got {actual}")]
    VectorDimensionMismatch { expected: usize, actual: usize },

    #[error("Embedding error: {0}")]
    EmbeddingError(String),

    #[error("Inference error: {0}")]
    InferenceError(String),

    // ── Search Errors ───────────────────────────────────────────────
    #[error("Search error: {0}")]
    SearchError(String),

    // ── Geospatial Errors ───────────────────────────────────────────
    #[error("Invalid coordinates: {0}")]
    InvalidCoordinates(String),

    // ── Compression Errors ──────────────────────────────────────────
    #[error("Compression error: {0}")]
    CompressionError(String),

    // ── Generic ──────────────────────────────────────────────────────
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

impl From<serde_json::Error> for GraniteError {
    fn from(e: serde_json::Error) -> Self {
        GraniteError::Serialization(e.to_string())
    }
}

impl From<bincode::Error> for GraniteError {
    fn from(e: bincode::Error) -> Self {
        GraniteError::Serialization(e.to_string())
    }
}
