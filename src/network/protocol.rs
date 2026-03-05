// ============================================================================
// GraniteDB — Wire Protocol
// ============================================================================
// JSON-based wire protocol for client-server communication over TCP.
// ============================================================================

use serde::{Deserialize, Serialize};

/// A request from a client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    /// Request ID (for correlating responses)
    pub request_id: String,
    /// The command to execute
    pub command: Command,
    /// Optional authentication token
    pub auth_token: Option<String>,
}

/// Supported commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Command {
    // ── Database operations ──
    #[serde(rename = "create_database")]
    CreateDatabase { name: String },
    #[serde(rename = "drop_database")]
    DropDatabase { name: String },
    #[serde(rename = "list_databases")]
    ListDatabases,

    // ── Collection operations ──
    #[serde(rename = "create_collection")]
    CreateCollection { database: String, name: String },
    #[serde(rename = "drop_collection")]
    DropCollection { database: String, name: String },
    #[serde(rename = "list_collections")]
    ListCollections { database: String },

    // ── CRUD operations ──
    #[serde(rename = "insert_one")]
    InsertOne {
        database: String,
        collection: String,
        document: serde_json::Value,
    },
    #[serde(rename = "insert_many")]
    InsertMany {
        database: String,
        collection: String,
        documents: Vec<serde_json::Value>,
    },
    #[serde(rename = "find")]
    Find {
        database: String,
        collection: String,
        filter: serde_json::Value,
        projection: Option<Vec<String>>,
        sort: Option<serde_json::Value>,
        skip: Option<usize>,
        limit: Option<usize>,
    },
    #[serde(rename = "find_one")]
    FindOne {
        database: String,
        collection: String,
        filter: serde_json::Value,
    },
    #[serde(rename = "update_one")]
    UpdateOne {
        database: String,
        collection: String,
        filter: serde_json::Value,
        update: serde_json::Value,
    },
    #[serde(rename = "update_many")]
    UpdateMany {
        database: String,
        collection: String,
        filter: serde_json::Value,
        update: serde_json::Value,
    },
    #[serde(rename = "delete_one")]
    DeleteOne {
        database: String,
        collection: String,
        filter: serde_json::Value,
    },
    #[serde(rename = "delete_many")]
    DeleteMany {
        database: String,
        collection: String,
        filter: serde_json::Value,
    },
    #[serde(rename = "count")]
    Count {
        database: String,
        collection: String,
        filter: serde_json::Value,
    },

    // ── Aggregation ──
    #[serde(rename = "aggregate")]
    Aggregate {
        database: String,
        collection: String,
        pipeline: Vec<serde_json::Value>,
    },

    // ── Index operations ──
    #[serde(rename = "create_index")]
    CreateIndex {
        database: String,
        collection: String,
        name: String,
        fields: Vec<String>,
        unique: bool,
        index_type: String,
    },
    #[serde(rename = "drop_index")]
    DropIndex {
        database: String,
        collection: String,
        name: String,
    },
    #[serde(rename = "list_indexes")]
    ListIndexes {
        database: String,
        collection: String,
    },

    // ── Transaction operations ──
    #[serde(rename = "txn_begin")]
    TxnBegin,
    #[serde(rename = "txn_commit")]
    TxnCommit { txn_id: String },
    #[serde(rename = "txn_abort")]
    TxnAbort { txn_id: String },

    // ── Auth operations ──
    #[serde(rename = "authenticate")]
    Authenticate { username: String, password: String },
    #[serde(rename = "create_user")]
    CreateUser {
        username: String,
        password: String,
        roles: Vec<String>,
    },

    // ── Admin operations ──
    #[serde(rename = "server_status")]
    ServerStatus,
    #[serde(rename = "ping")]
    Ping,
}

/// A response from the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// Correlates to request_id
    pub request_id: String,
    /// Whether the operation succeeded
    pub ok: bool,
    /// Result data (if successful)
    pub data: Option<serde_json::Value>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Execution time in microseconds
    pub execution_time_us: Option<u128>,
}

impl Response {
    pub fn success(request_id: &str, data: serde_json::Value) -> Self {
        Self {
            request_id: request_id.to_string(),
            ok: true,
            data: Some(data),
            error: None,
            execution_time_us: None,
        }
    }

    pub fn error(request_id: &str, msg: &str) -> Self {
        Self {
            request_id: request_id.to_string(),
            ok: false,
            data: None,
            error: Some(msg.to_string()),
            execution_time_us: None,
        }
    }

    pub fn with_timing(mut self, us: u128) -> Self {
        self.execution_time_us = Some(us);
        self
    }
}
