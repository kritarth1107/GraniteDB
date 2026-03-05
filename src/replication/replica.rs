// ============================================================================
// GraniteDB — Replica Set Management
// ============================================================================

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Replica set member role.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicaRole {
    Primary,
    Secondary,
    Arbiter,
}

/// State of a replica set member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaMember {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub role: ReplicaRole,
    pub last_heartbeat: Option<i64>,
    pub oplog_ts: u64,
    pub healthy: bool,
}

/// Manages a replica set.
pub struct ReplicaSet {
    pub name: String,
    pub members: HashMap<String, ReplicaMember>,
    pub local_id: String,
}

impl ReplicaSet {
    pub fn new(name: &str, local_id: &str) -> Self {
        Self {
            name: name.to_string(),
            members: HashMap::new(),
            local_id: local_id.to_string(),
        }
    }

    /// Add a member to the replica set.
    pub fn add_member(&mut self, member: ReplicaMember) {
        self.members.insert(member.id.clone(), member);
    }

    /// Remove a member.
    pub fn remove_member(&mut self, id: &str) {
        self.members.remove(id);
    }

    /// Get the current primary.
    pub fn primary(&self) -> Option<&ReplicaMember> {
        self.members.values().find(|m| m.role == ReplicaRole::Primary)
    }

    /// Get all secondaries.
    pub fn secondaries(&self) -> Vec<&ReplicaMember> {
        self.members
            .values()
            .filter(|m| m.role == ReplicaRole::Secondary)
            .collect()
    }

    /// Check if the local node is primary.
    pub fn is_primary(&self) -> bool {
        self.members
            .get(&self.local_id)
            .map(|m| m.role == ReplicaRole::Primary)
            .unwrap_or(false)
    }

    /// Update heartbeat for a member.
    pub fn update_heartbeat(&mut self, member_id: &str, oplog_ts: u64) {
        if let Some(member) = self.members.get_mut(member_id) {
            member.last_heartbeat = Some(chrono::Utc::now().timestamp());
            member.oplog_ts = oplog_ts;
            member.healthy = true;
        }
    }

    /// Get replica set status.
    pub fn status(&self) -> serde_json::Value {
        let members: Vec<serde_json::Value> = self
            .members
            .values()
            .map(|m| {
                serde_json::json!({
                    "id": m.id,
                    "host": format!("{}:{}", m.host, m.port),
                    "role": format!("{:?}", m.role),
                    "healthy": m.healthy,
                    "oplog_ts": m.oplog_ts,
                })
            })
            .collect();
        serde_json::json!({
            "set": self.name,
            "members": members,
        })
    }
}
