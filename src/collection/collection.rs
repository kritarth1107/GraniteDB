// ============================================================================
// GraniteDB — Collection
// ============================================================================
// A Collection is a logical grouping of Documents (like a table in SQL
// or a collection in MongoDB). It manages CRUD operations, indexing,
// schema validation, and document lifecycle.
// ============================================================================

use crate::document::bson::BsonValue;
use crate::document::document::Document;
use crate::document::validation::{Schema, SchemaValidator};
use crate::error::{GraniteError, GraniteResult};
use crate::storage::engine::StorageEngine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Metadata for a collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMetadata {
    /// Collection name
    pub name: String,
    /// Database this collection belongs to
    pub database: String,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Optional schema for validation
    pub schema: Option<Schema>,
    /// Whether documents should be capped (max count)
    pub capped: bool,
    /// Max number of documents (if capped)
    pub max_documents: Option<usize>,
    /// Max total size in bytes (if capped)
    pub max_size_bytes: Option<usize>,
    /// Index names on this collection
    pub indexes: Vec<String>,
}

/// A collection of documents.
pub struct Collection {
    /// Collection metadata
    pub metadata: CollectionMetadata,
    /// Fully qualified name: "database.collection"
    pub full_name: String,
}

impl Collection {
    /// Create a new collection.
    pub fn new(database: &str, name: &str) -> Self {
        let full_name = format!("{}.{}", database, name);
        Self {
            metadata: CollectionMetadata {
                name: name.to_string(),
                database: database.to_string(),
                created_at: Utc::now(),
                schema: None,
                capped: false,
                max_documents: None,
                max_size_bytes: None,
                indexes: vec!["_id_".to_string()], // Default _id index
            },
            full_name,
        }
    }

    /// Create a capped collection.
    pub fn new_capped(
        database: &str,
        name: &str,
        max_documents: Option<usize>,
        max_size_bytes: Option<usize>,
    ) -> Self {
        let mut col = Self::new(database, name);
        col.metadata.capped = true;
        col.metadata.max_documents = max_documents;
        col.metadata.max_size_bytes = max_size_bytes;
        col
    }

    /// Set a schema for validation.
    pub fn set_schema(&mut self, schema: Schema) {
        self.metadata.schema = Some(schema);
    }

    /// Insert a document into this collection.
    pub fn insert_one(
        &self,
        storage: &mut StorageEngine,
        doc: Document,
    ) -> GraniteResult<String> {
        // Validate against schema if present
        if let Some(schema) = &self.metadata.schema {
            SchemaValidator::validate(&doc.data, schema)?;
        }

        // Check capped collection limits
        if self.metadata.capped {
            if let Some(max) = self.metadata.max_documents {
                let count = storage.document_count(&self.full_name);
                if count >= max {
                    return Err(GraniteError::Storage(format!(
                        "Capped collection '{}' has reached max documents: {}",
                        self.full_name, max
                    )));
                }
            }
        }

        let doc_id = doc.id.clone();
        storage.insert(&self.full_name, &doc)?;
        Ok(doc_id)
    }

    /// Insert many documents.
    pub fn insert_many(
        &self,
        storage: &mut StorageEngine,
        docs: Vec<Document>,
    ) -> GraniteResult<Vec<String>> {
        let mut ids = Vec::with_capacity(docs.len());
        for doc in docs {
            ids.push(self.insert_one(storage, doc)?);
        }
        Ok(ids)
    }

    /// Find a document by ID.
    pub fn find_by_id(
        &self,
        storage: &StorageEngine,
        id: &str,
    ) -> GraniteResult<Option<Document>> {
        storage.get(&self.full_name, id)
    }

    /// Find documents matching a filter.
    pub fn find(
        &self,
        storage: &StorageEngine,
        filter: &BTreeMap<String, BsonValue>,
    ) -> GraniteResult<Vec<Document>> {
        let all_docs = storage.get_all(&self.full_name)?;

        if filter.is_empty() {
            return Ok(all_docs);
        }

        let mut results = Vec::new();
        for doc in all_docs {
            if self.matches_filter(&doc, filter) {
                results.push(doc);
            }
        }
        Ok(results)
    }

    /// Find the first document matching a filter.
    pub fn find_one(
        &self,
        storage: &StorageEngine,
        filter: &BTreeMap<String, BsonValue>,
    ) -> GraniteResult<Option<Document>> {
        let all_docs = storage.get_all(&self.full_name)?;

        for doc in all_docs {
            if self.matches_filter(&doc, filter) {
                return Ok(Some(doc));
            }
        }
        Ok(None)
    }

    /// Update a document by ID.
    pub fn update_one(
        &self,
        storage: &mut StorageEngine,
        id: &str,
        update: &BTreeMap<String, BsonValue>,
    ) -> GraniteResult<bool> {
        if let Some(mut doc) = storage.get(&self.full_name, id)? {
            doc.merge(update);

            // Re-validate
            if let Some(schema) = &self.metadata.schema {
                SchemaValidator::validate(&doc.data, schema)?;
            }

            storage.update(&self.full_name, &doc)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Update many documents matching a filter.
    pub fn update_many(
        &self,
        storage: &mut StorageEngine,
        filter: &BTreeMap<String, BsonValue>,
        update: &BTreeMap<String, BsonValue>,
    ) -> GraniteResult<usize> {
        let docs = self.find(storage, filter)?;
        let mut count = 0;
        for doc in docs {
            if self.update_one(storage, &doc.id, update)? {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Delete a document by ID.
    pub fn delete_one(&self, storage: &mut StorageEngine, id: &str) -> GraniteResult<bool> {
        if storage.get(&self.full_name, id)?.is_some() {
            storage.delete(&self.full_name, id)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Delete many documents matching a filter.
    pub fn delete_many(
        &self,
        storage: &mut StorageEngine,
        filter: &BTreeMap<String, BsonValue>,
    ) -> GraniteResult<usize> {
        let docs = self.find(storage, filter)?;
        let mut count = 0;
        for doc in docs {
            if self.delete_one(storage, &doc.id)? {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Count documents matching a filter.
    pub fn count(
        &self,
        storage: &StorageEngine,
        filter: &BTreeMap<String, BsonValue>,
    ) -> GraniteResult<usize> {
        if filter.is_empty() {
            Ok(storage.document_count(&self.full_name))
        } else {
            Ok(self.find(storage, filter)?.len())
        }
    }

    /// Check if a document matches a filter (simple equality matching).
    fn matches_filter(&self, doc: &Document, filter: &BTreeMap<String, BsonValue>) -> bool {
        for (key, expected) in filter {
            // Handle _id specially
            if key == "_id" {
                if let BsonValue::String(id) = expected {
                    if &doc.id != id {
                        return false;
                    }
                    continue;
                }
            }

            match doc.get_path(key) {
                Some(actual) => {
                    // Support operator queries (e.g., {"age": {"$gt": 18}})
                    if let BsonValue::Document(ops) = expected {
                        if !self.evaluate_operators(actual, ops) {
                            return false;
                        }
                    } else if actual != expected {
                        return false;
                    }
                }
                None => {
                    if *expected != BsonValue::Null {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Evaluate MongoDB-style comparison operators.
    fn evaluate_operators(
        &self,
        actual: &BsonValue,
        operators: &BTreeMap<String, BsonValue>,
    ) -> bool {
        for (op, value) in operators {
            let result = match op.as_str() {
                "$eq" => actual == value,
                "$ne" => actual != value,
                "$gt" => actual.partial_cmp(value) == Some(std::cmp::Ordering::Greater),
                "$gte" => matches!(
                    actual.partial_cmp(value),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                "$lt" => actual.partial_cmp(value) == Some(std::cmp::Ordering::Less),
                "$lte" => matches!(
                    actual.partial_cmp(value),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
                "$in" => {
                    if let BsonValue::Array(arr) = value {
                        arr.contains(actual)
                    } else {
                        false
                    }
                }
                "$nin" => {
                    if let BsonValue::Array(arr) = value {
                        !arr.contains(actual)
                    } else {
                        true
                    }
                }
                "$exists" => {
                    if let BsonValue::Boolean(should_exist) = value {
                        if *should_exist {
                            *actual != BsonValue::Null
                        } else {
                            *actual == BsonValue::Null
                        }
                    } else {
                        true
                    }
                }
                "$regex" => {
                    if let (BsonValue::String(text), BsonValue::String(pattern)) =
                        (actual, value)
                    {
                        regex::Regex::new(pattern)
                            .map(|re| re.is_match(text))
                            .unwrap_or(false)
                    } else {
                        false
                    }
                }
                _ => true, // Unknown operators are ignored
            };

            if !result {
                return false;
            }
        }
        true
    }
}
