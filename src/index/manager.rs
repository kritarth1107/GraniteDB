// ============================================================================
// GraniteDB — Index Manager
// ============================================================================
// Manages all indexes for a database. Handles index creation, deletion,
// and updating when documents change.
// ============================================================================

use crate::document::bson::BsonValue;
use crate::document::document::Document;
use crate::error::{GraniteError, GraniteResult};
use crate::index::btree::BTreeIndex;
use crate::index::hash_index::HashIndex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Type of index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexType {
    BTree,
    Hash,
}

/// Index definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub collection: String,
    pub fields: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
    pub sparse: bool,
}

/// Wraps either a BTree or Hash index.
pub enum IndexInstance {
    BTree(BTreeIndex),
    Hash(HashIndex),
}

/// Manages indexes across collections.
pub struct IndexManager {
    /// Index definitions keyed by "collection.index_name"
    definitions: HashMap<String, IndexDefinition>,
    /// Active index instances
    instances: HashMap<String, IndexInstance>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            definitions: HashMap::new(),
            instances: HashMap::new(),
        }
    }

    /// Create a new index.
    pub fn create_index(&mut self, def: IndexDefinition) -> GraniteResult<()> {
        let key = format!("{}.{}", def.collection, def.name);
        if self.definitions.contains_key(&key) {
            return Err(GraniteError::IndexAlreadyExists(key));
        }

        let instance = match def.index_type {
            IndexType::BTree => {
                IndexInstance::BTree(BTreeIndex::new(&def.name, def.fields.clone(), def.unique))
            }
            IndexType::Hash => {
                IndexInstance::Hash(HashIndex::new(&def.name, def.fields.clone(), def.unique))
            }
        };

        self.definitions.insert(key.clone(), def);
        self.instances.insert(key, instance);
        Ok(())
    }

    /// Drop an index.
    pub fn drop_index(&mut self, collection: &str, index_name: &str) -> GraniteResult<()> {
        let key = format!("{}.{}", collection, index_name);
        if self.definitions.remove(&key).is_none() {
            return Err(GraniteError::IndexNotFound(key));
        }
        self.instances.remove(&key);
        Ok(())
    }

    /// Index a document (call after insert).
    pub fn index_document(&mut self, collection: &str, doc: &Document) -> GraniteResult<()> {
        let collection_prefix = format!("{}.", collection);
        let keys: Vec<String> = self
            .definitions
            .keys()
            .filter(|k| k.starts_with(&collection_prefix))
            .cloned()
            .collect();

        for key in keys {
            if let Some(def) = self.definitions.get(&key) {
                let field_values: Vec<&BsonValue> = def
                    .fields
                    .iter()
                    .filter_map(|f| doc.data.get(f))
                    .collect();

                // Skip if sparse and no indexed fields are present
                if def.sparse && field_values.is_empty() {
                    continue;
                }

                if let Some(instance) = self.instances.get_mut(&key) {
                    match instance {
                        IndexInstance::BTree(idx) => idx.insert(&doc.id, &field_values)?,
                        IndexInstance::Hash(idx) => idx.insert(&doc.id, &field_values)?,
                    }
                }
            }
        }
        Ok(())
    }

    /// Remove a document from indexes (call before delete).
    pub fn unindex_document(&mut self, collection: &str, doc: &Document) {
        let collection_prefix = format!("{}.", collection);
        let keys: Vec<String> = self
            .definitions
            .keys()
            .filter(|k| k.starts_with(&collection_prefix))
            .cloned()
            .collect();

        for key in keys {
            if let Some(def) = self.definitions.get(&key) {
                let field_values: Vec<&BsonValue> = def
                    .fields
                    .iter()
                    .filter_map(|f| doc.data.get(f))
                    .collect();

                if let Some(instance) = self.instances.get_mut(&key) {
                    match instance {
                        IndexInstance::BTree(idx) => idx.remove(&doc.id, &field_values),
                        IndexInstance::Hash(idx) => idx.remove(&doc.id, &field_values),
                    }
                }
            }
        }
    }

    /// Lookup document IDs using an index.
    pub fn lookup(
        &self,
        collection: &str,
        index_name: &str,
        values: &[&BsonValue],
    ) -> GraniteResult<Vec<String>> {
        let key = format!("{}.{}", collection, index_name);
        match self.instances.get(&key) {
            Some(IndexInstance::BTree(idx)) => Ok(idx.lookup(values)),
            Some(IndexInstance::Hash(idx)) => Ok(idx.lookup(values)),
            None => Err(GraniteError::IndexNotFound(key)),
        }
    }

    /// List all indexes for a collection.
    pub fn list_indexes(&self, collection: &str) -> Vec<&IndexDefinition> {
        let prefix = format!("{}.", collection);
        self.definitions
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v)
            .collect()
    }

    /// Get stats for all indexes.
    pub fn stats(&self) -> Vec<IndexStats> {
        self.definitions
            .iter()
            .map(|(key, def)| {
                let count = match self.instances.get(key) {
                    Some(IndexInstance::BTree(idx)) => idx.len(),
                    Some(IndexInstance::Hash(idx)) => idx.len(),
                    None => 0,
                };
                IndexStats {
                    name: def.name.clone(),
                    collection: def.collection.clone(),
                    fields: def.fields.clone(),
                    unique: def.unique,
                    index_type: def.index_type.clone(),
                    entries: count,
                }
            })
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexStats {
    pub name: String,
    pub collection: String,
    pub fields: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
    pub entries: usize,
}
