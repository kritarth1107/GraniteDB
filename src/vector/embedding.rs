// ============================================================================
// GraniteDB — Embedding Store
// ============================================================================
// Stores raw vector embeddings alongside document IDs. Supports batch
// operations for bulk import of embeddings from ML models.
// ============================================================================

use crate::error::{GraniteError, GraniteResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metadata about a stored embedding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingMeta {
    pub doc_id: String,
    pub collection: String,
    pub model: String,
    pub dimensions: usize,
    pub created_at: i64,
}

/// Stores embeddings and their metadata.
pub struct EmbeddingStore {
    /// Flat storage: doc_id -> (vector, metadata)
    embeddings: HashMap<String, (Vec<f32>, EmbeddingMeta)>,
    /// Collection -> list of doc_ids with embeddings
    collection_index: HashMap<String, Vec<String>>,
    /// Stats
    total_vectors: usize,
    total_dimensions: usize,
}

impl EmbeddingStore {
    pub fn new() -> Self {
        Self {
            embeddings: HashMap::new(),
            collection_index: HashMap::new(),
            total_vectors: 0,
            total_dimensions: 0,
        }
    }

    /// Store an embedding for a document.
    pub fn store(
        &mut self,
        doc_id: &str,
        collection: &str,
        model: &str,
        vector: Vec<f32>,
    ) -> GraniteResult<()> {
        let dims = vector.len();
        if dims == 0 {
            return Err(GraniteError::InvalidVector(
                "Empty vector".to_string(),
            ));
        }

        let meta = EmbeddingMeta {
            doc_id: doc_id.to_string(),
            collection: collection.to_string(),
            model: model.to_string(),
            dimensions: dims,
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        self.embeddings
            .insert(doc_id.to_string(), (vector, meta));

        self.collection_index
            .entry(collection.to_string())
            .or_default()
            .push(doc_id.to_string());

        self.total_vectors += 1;
        self.total_dimensions = dims;

        Ok(())
    }

    /// Store multiple embeddings in a batch.
    pub fn store_batch(
        &mut self,
        entries: Vec<(String, String, String, Vec<f32>)>,
    ) -> GraniteResult<usize> {
        let mut count = 0;
        for (doc_id, collection, model, vector) in entries {
            self.store(&doc_id, &collection, &model, vector)?;
            count += 1;
        }
        Ok(count)
    }

    /// Retrieve an embedding by document ID.
    pub fn get(&self, doc_id: &str) -> Option<(&[f32], &EmbeddingMeta)> {
        self.embeddings
            .get(doc_id)
            .map(|(v, m)| (v.as_slice(), m))
    }

    /// Get all embedding vectors for a collection (for batch operations).
    pub fn get_collection_vectors(&self, collection: &str) -> Vec<(&str, &[f32])> {
        self.collection_index
            .get(collection)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| {
                        self.embeddings
                            .get(id)
                            .map(|(v, _)| (id.as_str(), v.as_slice()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Remove an embedding.
    pub fn remove(&mut self, doc_id: &str) -> bool {
        if let Some((_, meta)) = self.embeddings.remove(doc_id) {
            if let Some(ids) = self.collection_index.get_mut(&meta.collection) {
                ids.retain(|id| id != doc_id);
            }
            self.total_vectors = self.total_vectors.saturating_sub(1);
            true
        } else {
            false
        }
    }

    /// Get total number of stored embeddings.
    pub fn len(&self) -> usize {
        self.total_vectors
    }

    pub fn is_empty(&self) -> bool {
        self.total_vectors == 0
    }

    /// Get memory estimate in bytes.
    pub fn memory_usage_bytes(&self) -> usize {
        self.total_vectors * self.total_dimensions * std::mem::size_of::<f32>()
    }

    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!({
            "total_vectors": self.total_vectors,
            "dimensions": self.total_dimensions,
            "memory_bytes": self.memory_usage_bytes(),
            "collections": self.collection_index.keys().collect::<Vec<_>>(),
        })
    }
}
