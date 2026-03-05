// ============================================================================
// GraniteDB — Database
// ============================================================================
// A Database is a named container of Collections. The GraniteDB server can
// host multiple databases simultaneously. This is the top-level logical
// unit that users interact with.
// ============================================================================

use crate::collection::Collection;
use crate::document::bson::BsonValue;
use crate::document::document::Document;
use crate::error::{GraniteError, GraniteResult};
use crate::storage::engine::StorageEngine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Metadata for a database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    /// Database name
    pub name: String,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// List of collection names
    pub collections: Vec<String>,
}

/// A GraniteDB database instance.
pub struct Database {
    /// Database metadata
    pub metadata: DatabaseMetadata,
    /// Collections in this database
    collections: HashMap<String, Collection>,
}

impl Database {
    /// Create a new database.
    pub fn new(name: &str) -> Self {
        Self {
            metadata: DatabaseMetadata {
                name: name.to_string(),
                created_at: Utc::now(),
                collections: Vec::new(),
            },
            collections: HashMap::new(),
        }
    }

    /// Create a new collection in this database.
    pub fn create_collection(
        &mut self,
        name: &str,
        storage: &mut StorageEngine,
    ) -> GraniteResult<()> {
        if self.collections.contains_key(name) {
            return Err(GraniteError::CollectionAlreadyExists(name.to_string()));
        }

        let collection = Collection::new(&self.metadata.name, name);
        storage.create_collection(&self.metadata.name, name)?;
        self.collections.insert(name.to_string(), collection);
        self.metadata.collections.push(name.to_string());

        tracing::info!(
            db = %self.metadata.name,
            collection = name,
            "Created collection"
        );
        Ok(())
    }

    /// Drop a collection.
    pub fn drop_collection(
        &mut self,
        name: &str,
        storage: &mut StorageEngine,
    ) -> GraniteResult<()> {
        if !self.collections.contains_key(name) {
            return Err(GraniteError::CollectionNotFound(name.to_string()));
        }

        storage.drop_collection(&self.metadata.name, name)?;
        self.collections.remove(name);
        self.metadata.collections.retain(|c| c != name);

        tracing::info!(
            db = %self.metadata.name,
            collection = name,
            "Dropped collection"
        );
        Ok(())
    }

    /// Get a reference to a collection.
    pub fn collection(&self, name: &str) -> GraniteResult<&Collection> {
        self.collections
            .get(name)
            .ok_or_else(|| GraniteError::CollectionNotFound(name.to_string()))
    }

    /// Get a mutable reference to a collection.
    pub fn collection_mut(&mut self, name: &str) -> GraniteResult<&mut Collection> {
        self.collections
            .get_mut(name)
            .ok_or_else(|| GraniteError::CollectionNotFound(name.to_string()))
    }

    /// List all collection names.
    pub fn list_collections(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    /// Quick helper: insert a document into a collection.
    pub fn insert(
        &self,
        collection_name: &str,
        doc: Document,
        storage: &mut StorageEngine,
    ) -> GraniteResult<String> {
        let col = self.collection(collection_name)?;
        col.insert_one(storage, doc)
    }

    /// Quick helper: find documents in a collection.
    pub fn find(
        &self,
        collection_name: &str,
        filter: &BTreeMap<String, BsonValue>,
        storage: &StorageEngine,
    ) -> GraniteResult<Vec<Document>> {
        let col = self.collection(collection_name)?;
        col.find(storage, filter)
    }

    /// Quick helper: update documents in a collection.
    pub fn update(
        &self,
        collection_name: &str,
        filter: &BTreeMap<String, BsonValue>,
        update: &BTreeMap<String, BsonValue>,
        storage: &mut StorageEngine,
    ) -> GraniteResult<usize> {
        let col = self.collection(collection_name)?;
        col.update_many(storage, filter, update)
    }

    /// Quick helper: delete documents in a collection.
    pub fn delete(
        &self,
        collection_name: &str,
        filter: &BTreeMap<String, BsonValue>,
        storage: &mut StorageEngine,
    ) -> GraniteResult<usize> {
        let col = self.collection(collection_name)?;
        col.delete_many(storage, filter)
    }

    /// Get database stats.
    pub fn stats(&self, storage: &StorageEngine) -> DatabaseStats {
        let mut total_docs = 0;
        let mut collection_stats = Vec::new();
        for (name, col) in &self.collections {
            let count = storage.document_count(&col.full_name);
            total_docs += count;
            collection_stats.push(CollectionStats {
                name: name.clone(),
                document_count: count,
                index_count: col.metadata.indexes.len(),
            });
        }
        DatabaseStats {
            name: self.metadata.name.clone(),
            collection_count: self.collections.len(),
            total_documents: total_docs,
            collections: collection_stats,
        }
    }
}

/// Database statistics.
#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseStats {
    pub name: String,
    pub collection_count: usize,
    pub total_documents: usize,
    pub collections: Vec<CollectionStats>,
}

/// Per-collection statistics.
#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionStats {
    pub name: String,
    pub document_count: usize,
    pub index_count: usize,
}
