// ============================================================================
// GraniteDB — Hash Index
// ============================================================================
// A hash-based index for O(1) exact-match lookups.
// ============================================================================

use crate::document::bson::BsonValue;
use crate::error::GraniteResult;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// An in-memory hash index for fast equality lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashIndex {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
    entries: HashMap<Vec<u8>, HashSet<String>>,
    count: usize,
}

impl HashIndex {
    pub fn new(name: &str, fields: Vec<String>, unique: bool) -> Self {
        Self {
            name: name.to_string(),
            fields,
            unique,
            entries: HashMap::new(),
            count: 0,
        }
    }

    fn make_key(values: &[&BsonValue]) -> Vec<u8> {
        bincode::serialize(values).unwrap_or_default()
    }

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

        let set = self.entries.entry(key).or_insert_with(HashSet::new);
        set.insert(doc_id.to_string());
        self.count += 1;
        Ok(())
    }

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

    pub fn lookup(&self, field_values: &[&BsonValue]) -> Vec<String> {
        let key = Self::make_key(field_values);
        self.entries
            .get(&key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.count = 0;
    }
}
