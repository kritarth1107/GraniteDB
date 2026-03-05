// ============================================================================
// GraniteDB — Document
// ============================================================================
// The core data unit stored in GraniteDB collections.
// Each document has a unique `_id`, rich metadata, and a flexible data payload.
// ============================================================================

use crate::document::bson::BsonValue;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// A single document stored in a GraniteDB collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Unique document identifier (auto-generated UUID v4)
    pub id: String,
    /// The document's field data
    pub data: BTreeMap<String, BsonValue>,
    /// Metadata
    pub metadata: DocumentMetadata,
}

/// Metadata attached to every document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp
    pub updated_at: DateTime<Utc>,
    /// Document version (incremented on every update)
    pub version: u64,
    /// Size in bytes (serialized)
    pub size_bytes: usize,
    /// Optional TTL — if set, the document expires after this time
    pub expires_at: Option<DateTime<Utc>>,
}

impl Document {
    /// Create a new document from a `BsonValue::Document`.
    pub fn new(data: BTreeMap<String, BsonValue>) -> Self {
        let now = Utc::now();
        let id = Uuid::new_v4().to_string();
        let serialized_size = bincode::serialize(&data).map(|b| b.len()).unwrap_or(0);

        Self {
            id,
            data,
            metadata: DocumentMetadata {
                created_at: now,
                updated_at: now,
                version: 1,
                size_bytes: serialized_size,
                expires_at: None,
            },
        }
    }

    /// Create a document from a serde_json::Value (must be an Object).
    pub fn from_json(value: serde_json::Value) -> crate::error::GraniteResult<Self> {
        match value {
            serde_json::Value::Object(map) => {
                let mut data = BTreeMap::new();
                for (k, v) in map {
                    data.insert(k, BsonValue::from(v));
                }
                Ok(Self::new(data))
            }
            _ => Err(crate::error::GraniteError::ValidationError(
                "Document must be a JSON object".to_string(),
            )),
        }
    }

    /// Create a document with a specific ID (for restoring from disk).
    pub fn with_id(id: String, data: BTreeMap<String, BsonValue>, metadata: DocumentMetadata) -> Self {
        Self { id, data, metadata }
    }

    /// Get a field value by key.
    pub fn get(&self, key: &str) -> Option<&BsonValue> {
        // Support dot-notation for nested lookups
        if key.contains('.') {
            let bson_doc = BsonValue::Document(self.data.clone());
            // We need to return owned, so this is a workaround
            // For non-dot keys, do the simple path
            return self.data.get(key).or_else(|| {
                // Dot notation: traverse
                let parts: Vec<&str> = key.splitn(2, '.').collect();
                if parts.len() == 2 {
                    if let Some(BsonValue::Document(nested)) = self.data.get(parts[0]) {
                        let nested_doc = Document {
                            id: String::new(),
                            data: nested.clone(),
                            metadata: self.metadata.clone(),
                        };
                        // This recursion is bounded by dot depth
                        // For deep nesting we'd need a different approach
                        return None; // Simplified — full impl uses BsonValue::get_path
                    }
                }
                None
            });
        }
        self.data.get(key)
    }

    /// Get a field using full dot-notation path.
    pub fn get_path(&self, path: &str) -> Option<&BsonValue> {
        let doc_value = BsonValue::Document(self.data.clone());
        // We can't return a reference to a temporary, so we use data directly
        let parts: Vec<&str> = path.split('.').collect();
        let mut current_map = &self.data;
        for (i, part) in parts.iter().enumerate() {
            if let Some(val) = current_map.get(*part) {
                if i == parts.len() - 1 {
                    return current_map.get(*part);
                }
                match val {
                    BsonValue::Document(nested) => {
                        current_map = nested;
                    }
                    _ => return None,
                }
            } else {
                return None;
            }
        }
        None
    }

    /// Set a top-level field.
    pub fn set(&mut self, key: String, value: BsonValue) {
        self.data.insert(key, value);
        self.metadata.updated_at = Utc::now();
        self.metadata.version += 1;
    }

    /// Remove a field.
    pub fn remove(&mut self, key: &str) -> Option<BsonValue> {
        let removed = self.data.remove(key);
        if removed.is_some() {
            self.metadata.updated_at = Utc::now();
            self.metadata.version += 1;
        }
        removed
    }

    /// Merge another document's fields into this one (`$set`-like behavior).
    pub fn merge(&mut self, other: &BTreeMap<String, BsonValue>) {
        for (k, v) in other {
            self.data.insert(k.clone(), v.clone());
        }
        self.metadata.updated_at = Utc::now();
        self.metadata.version += 1;
    }

    /// Convert the document to a JSON value (including _id and metadata).
    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("_id".to_string(), serde_json::json!(self.id));
        for (k, v) in &self.data {
            map.insert(k.clone(), serde_json::Value::from(v.clone()));
        }
        map.insert(
            "_metadata".to_string(),
            serde_json::json!({
                "created_at": self.metadata.created_at.to_rfc3339(),
                "updated_at": self.metadata.updated_at.to_rfc3339(),
                "version": self.metadata.version,
                "size_bytes": self.metadata.size_bytes,
            }),
        );
        serde_json::Value::Object(map)
    }

    /// Return all top-level field names.
    pub fn keys(&self) -> Vec<&String> {
        self.data.keys().collect()
    }

    /// Check if the document has expired (if TTL is set).
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.metadata.expires_at {
            Utc::now() > expires_at
        } else {
            false
        }
    }

    /// Recalculate the serialized size.
    pub fn recalculate_size(&mut self) {
        self.metadata.size_bytes = bincode::serialize(&self.data).map(|b| b.len()).unwrap_or(0);
    }
}

impl std::fmt::Display for Document {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Document(_id: {}, version: {}, fields: {})",
            self.id,
            self.metadata.version,
            self.data.len()
        )
    }
}
