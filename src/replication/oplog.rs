// ============================================================================
// GraniteDB — Operations Log (Oplog)
// ============================================================================
// The oplog records all write operations for replication to secondaries.
// ============================================================================

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// An oplog entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OplogEntry {
    /// Monotonically increasing timestamp
    pub ts: u64,
    /// Wall clock time
    pub wall: DateTime<Utc>,
    /// Namespace: "database.collection"
    pub ns: String,
    /// Operation type
    pub op: OpType,
    /// Operation data
    pub data: serde_json::Value,
}

/// Types of oplog operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpType {
    /// Insert
    Insert,
    /// Update
    Update,
    /// Delete
    Delete,
    /// Command (createCollection, dropCollection, etc.)
    Command,
    /// No-op (used for heartbeats)
    Noop,
}

/// The operations log — a capped ring buffer.
pub struct Oplog {
    entries: VecDeque<OplogEntry>,
    max_size: usize,
    next_ts: u64,
}

impl Oplog {
    pub fn new(max_size_mb: usize) -> Self {
        // Approximate: each entry ~500 bytes, so max entries = max_size_mb * 1024 * 1024 / 500
        let max_entries = (max_size_mb * 1024 * 1024) / 500;
        Self {
            entries: VecDeque::with_capacity(max_entries.min(1_000_000)),
            max_size: max_entries,
            next_ts: 1,
        }
    }

    /// Append an operation to the oplog.
    pub fn append(&mut self, ns: &str, op: OpType, data: serde_json::Value) -> u64 {
        let ts = self.next_ts;
        self.next_ts += 1;

        let entry = OplogEntry {
            ts,
            wall: Utc::now(),
            ns: ns.to_string(),
            op,
            data,
        };

        if self.entries.len() >= self.max_size {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
        ts
    }

    /// Get all entries after a given timestamp (for replication sync).
    pub fn entries_after(&self, ts: u64) -> Vec<&OplogEntry> {
        self.entries.iter().filter(|e| e.ts > ts).collect()
    }

    /// Get the latest timestamp.
    pub fn latest_ts(&self) -> u64 {
        self.entries.back().map(|e| e.ts).unwrap_or(0)
    }

    /// Get total entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
