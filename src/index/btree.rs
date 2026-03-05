// ============================================================================
// GraniteDB — B-Tree Index
// ============================================================================
// A simple in-memory B-Tree index for fast range queries and ordered access.
// Maps field values to sets of document IDs.
// ============================================================================

use crate::document::bson::BsonValue;
use crate::error::GraniteResult;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// An in-memory B-Tree index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeIndex {
    /// Index name
    pub name: String,
    /// Field(s) this index covers
    pub fields: Vec<String>,
    /// Whether this index enforces uniqueness
    pub unique: bool,
    /// The B-Tree: maps serialized BsonValue keys → set of document IDs
    entries: BTreeMap<Vec<u8>, BTreeSet<String>>,
    /// Number of entries
    count: usize,
}

impl BTreeIndex {
    /// Create a new B-Tree index.
    pub fn new(name: &str, fields: Vec<String>, unique: bool) -> Self {
        Self {
            name: name.to_string(),
            fields,
            unique,
            entries: BTreeMap::new(),
            count: 0,
        }
    }

    /// Generate a composite key from the given values.
    fn make_key(values: &[&BsonValue]) -> Vec<u8> {
        bincode::serialize(values).unwrap_or_default()
    }

    /// Insert a document into the index.
    pub fn insert(
        &mut self,
        doc_id: &str,
        field_values: &[&BsonValue],
    ) -> GraniteResult<()> {
        let key = Self::make_key(field_values);

        if self.unique {
            if let Some(existing) = self.entries.get(&key) {
                if !existing.is_empty() {
                    return Err(crate::error::GraniteError::DuplicateKey {
                        collection: self.name.clone(),
                        key: format!("{:?}", field_values),
                    });
                }
            }
        }

        let set = self.entries.entry(key).or_insert_with(BTreeSet::new);
        set.insert(doc_id.to_string());
        self.count += 1;
        Ok(())
    }

    /// Remove a document from the index.
    pub fn remove(&mut self, doc_id: &str, field_values: &[&BsonValue]) {
        let key = Self::make_key(field_values);
        if let Some(set) = self.entries.get_mut(&key) {
            set.remove(doc_id);
            if set.is_empty() {
                self.entries.remove(&key);
            }
            self.count = self.count.saturating_sub(1);
        }
    }

    /// Exact lookup: find all document IDs matching the given values.
    pub fn lookup(&self, field_values: &[&BsonValue]) -> Vec<String> {
        let key = Self::make_key(field_values);
        self.entries
            .get(&key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Range scan: find all document IDs where the indexed value is in [min, max].
    pub fn range_scan(
        &self,
        min_values: &[&BsonValue],
        max_values: &[&BsonValue],
    ) -> Vec<String> {
        let min_key = Self::make_key(min_values);
        let max_key = Self::make_key(max_values);

        let mut results = Vec::new();
        for (_, doc_ids) in self.entries.range(min_key..=max_key) {
            results.extend(doc_ids.iter().cloned());
        }
        results
    }

    /// Get all document IDs in the index (full scan).
    pub fn all_doc_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        for doc_ids in self.entries.values() {
            ids.extend(doc_ids.iter().cloned());
        }
        ids
    }

    /// Number of indexed entries.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Is the index empty?
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Clear the index.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.count = 0;
    }
}
