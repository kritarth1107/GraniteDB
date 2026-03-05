// ============================================================================
// GraniteDB — MVCC (Multi-Version Concurrency Control)
// ============================================================================
// Tracks document versions for snapshot isolation. Each write creates a
// new version, and readers see a consistent snapshot of the data.
// ============================================================================

use crate::document::bson::BsonValue;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// A versioned entry for a document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedEntry {
    /// Version number (monotonically increasing)
    pub version: u64,
    /// Transaction ID that created this version
    pub created_by_txn: String,
    /// The document data at this version (None = deleted)
    pub data: Option<BTreeMap<String, BsonValue>>,
    /// Timestamp of this version
    pub timestamp: i64,
}

/// MVCC version store for a collection.
pub struct MvccStore {
    /// document_id -> list of versions (newest last)
    versions: HashMap<String, Vec<VersionedEntry>>,
    /// Global version counter
    next_version: u64,
}

impl MvccStore {
    pub fn new() -> Self {
        Self {
            versions: HashMap::new(),
            next_version: 1,
        }
    }

    /// Create a new version for a document.
    pub fn write(
        &mut self,
        doc_id: &str,
        txn_id: &str,
        data: Option<BTreeMap<String, BsonValue>>,
    ) -> u64 {
        let version = self.next_version;
        self.next_version += 1;

        let entry = VersionedEntry {
            version,
            created_by_txn: txn_id.to_string(),
            data,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        self.versions
            .entry(doc_id.to_string())
            .or_insert_with(Vec::new)
            .push(entry);

        version
    }

    /// Read the latest committed version of a document visible to a snapshot.
    pub fn read(
        &self,
        doc_id: &str,
        snapshot_version: u64,
        committed_txns: &[String],
    ) -> Option<&VersionedEntry> {
        self.versions.get(doc_id).and_then(|versions| {
            versions
                .iter()
                .rev()
                .find(|v| {
                    v.version <= snapshot_version
                        && committed_txns.contains(&v.created_by_txn)
                })
        })
    }

    /// Get all versions for a document (for debugging/history).
    pub fn history(&self, doc_id: &str) -> Vec<&VersionedEntry> {
        self.versions
            .get(doc_id)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    /// Garbage collect old versions that are no longer needed.
    pub fn gc(&mut self, min_active_version: u64) {
        for versions in self.versions.values_mut() {
            // Keep at least the latest version and any version >= min_active_version
            if versions.len() > 1 {
                let cutoff = versions
                    .iter()
                    .rposition(|v| v.version < min_active_version)
                    .unwrap_or(0);
                if cutoff > 0 {
                    versions.drain(..cutoff);
                }
            }
        }
    }

    /// Current version counter.
    pub fn current_version(&self) -> u64 {
        self.next_version - 1
    }
}
