// ============================================================================
// GraniteDB — Shard Definition
// ============================================================================

use serde::{Deserialize, Serialize};

/// A shard represents a partition of data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub key_range_start: u64,
    pub key_range_end: u64,
    pub status: ShardStatus,
    pub document_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ShardStatus {
    Active,
    Migrating,
    Draining,
    Offline,
}

impl Shard {
    pub fn new(id: &str, host: &str, port: u16, range_start: u64, range_end: u64) -> Self {
        Self {
            id: id.to_string(),
            host: host.to_string(),
            port,
            key_range_start: range_start,
            key_range_end: range_end,
            status: ShardStatus::Active,
            document_count: 0,
        }
    }

    /// Check if a hash falls within this shard's key range.
    pub fn contains_hash(&self, hash: u64) -> bool {
        hash >= self.key_range_start && hash < self.key_range_end
    }
}
