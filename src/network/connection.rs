// ============================================================================
// GraniteDB — Connection Handler
// ============================================================================

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Represents an active client connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    pub id: String,
    pub remote_addr: String,
    pub connected_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    pub authenticated_user: Option<String>,
    pub current_database: Option<String>,
    pub active_txn: Option<String>,
    pub request_count: u64,
}

impl Connection {
    pub fn new(remote_addr: &str) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            remote_addr: remote_addr.to_string(),
            connected_at: now,
            last_activity: now,
            authenticated_user: None,
            current_database: None,
            active_txn: None,
            request_count: 0,
        }
    }

    pub fn touch(&mut self) {
        self.last_activity = Utc::now();
        self.request_count += 1;
    }
}

/// Manages active connections.
pub struct ConnectionPool {
    connections: HashMap<String, Connection>,
    max_connections: usize,
}

impl ConnectionPool {
    pub fn new(max_connections: usize) -> Self {
        Self {
            connections: HashMap::new(),
            max_connections,
        }
    }

    pub fn add(&mut self, conn: Connection) -> Result<(), String> {
        if self.connections.len() >= self.max_connections {
            return Err("Max connections reached".to_string());
        }
        self.connections.insert(conn.id.clone(), conn);
        Ok(())
    }

    pub fn remove(&mut self, id: &str) {
        self.connections.remove(id);
    }

    pub fn get(&self, id: &str) -> Option<&Connection> {
        self.connections.get(id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut Connection> {
        self.connections.get_mut(id)
    }

    pub fn count(&self) -> usize {
        self.connections.len()
    }

    pub fn list(&self) -> Vec<&Connection> {
        self.connections.values().collect()
    }
}
