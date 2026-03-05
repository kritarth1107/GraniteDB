// ============================================================================
// GraniteDB — Vector Index Manager
// ============================================================================
// High-level manager that ties together HNSW indexes, embedding storage,
// and quantization for a complete vector search solution.
// ============================================================================

use crate::error::{GraniteError, GraniteResult};
use crate::vector::distance::DistanceMetric;
use crate::vector::embedding::EmbeddingStore;
use crate::vector::hnsw::{HnswConfig, HnswIndex, VectorSearchResult};
use crate::vector::quantizer::{PQConfig, ProductQuantizer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Definition of a vector index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexDef {
    pub name: String,
    pub collection: String,
    pub field: String,
    pub dimensions: usize,
    pub metric: DistanceMetric,
    pub hnsw_m: usize,
    pub hnsw_ef_construction: usize,
    pub quantize: bool,
}

/// Manages all vector indexes across the database.
pub struct VectorIndexManager {
    indexes: HashMap<String, HnswIndex>,
    definitions: HashMap<String, VectorIndexDef>,
    embeddings: EmbeddingStore,
    quantizers: HashMap<String, ProductQuantizer>,
}

impl VectorIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            definitions: HashMap::new(),
            embeddings: EmbeddingStore::new(),
            quantizers: HashMap::new(),
        }
    }

    /// Create a new vector index.
    pub fn create_index(&mut self, def: VectorIndexDef) -> GraniteResult<()> {
        let key = format!("{}.{}", def.collection, def.name);
        if self.indexes.contains_key(&key) {
            return Err(GraniteError::IndexAlreadyExists(key));
        }

        let config = HnswConfig {
            m: def.hnsw_m,
            m0: def.hnsw_m * 2,
            ef_construction: def.hnsw_ef_construction,
            ef_search: 100,
            metric: def.metric,
            dimensions: def.dimensions,
        };

        let index = HnswIndex::new(config);
        self.indexes.insert(key.clone(), index);

        if def.quantize && def.dimensions > 0 {
            let pq_config = PQConfig {
                num_subquantizers: (def.dimensions / 8).max(1),
                num_centroids: 256,
                dimensions: def.dimensions,
                training_iterations: 25,
            };
            self.quantizers
                .insert(key.clone(), ProductQuantizer::new(pq_config));
        }

        self.definitions.insert(key, def);
        Ok(())
    }

    /// Drop a vector index.
    pub fn drop_index(
        &mut self,
        collection: &str,
        index_name: &str,
    ) -> GraniteResult<()> {
        let key = format!("{}.{}", collection, index_name);
        self.indexes.remove(&key);
        self.definitions.remove(&key);
        self.quantizers.remove(&key);
        Ok(())
    }

    /// Index a vector for a document.
    pub fn index_vector(
        &mut self,
        collection: &str,
        index_name: &str,
        doc_id: &str,
        vector: Vec<f32>,
    ) -> GraniteResult<()> {
        let key = format!("{}.{}", collection, index_name);

        // Store in embedding store
        self.embeddings
            .store(doc_id, collection, index_name, vector.clone())?;

        // Add to HNSW index
        if let Some(index) = self.indexes.get_mut(&key) {
            index.insert(doc_id, vector);
            Ok(())
        } else {
            Err(GraniteError::IndexNotFound(key))
        }
    }

    /// Search for nearest neighbors.
    pub fn search(
        &self,
        collection: &str,
        index_name: &str,
        query: &[f32],
        k: usize,
    ) -> GraniteResult<Vec<VectorSearchResult>> {
        let key = format!("{}.{}", collection, index_name);
        if let Some(index) = self.indexes.get(&key) {
            Ok(index.search(query, k))
        } else {
            Err(GraniteError::IndexNotFound(key))
        }
    }

    /// Search with a minimum similarity threshold.
    pub fn search_with_filter(
        &self,
        collection: &str,
        index_name: &str,
        query: &[f32],
        k: usize,
        min_score: f32,
    ) -> GraniteResult<Vec<VectorSearchResult>> {
        let key = format!("{}.{}", collection, index_name);
        if let Some(index) = self.indexes.get(&key) {
            Ok(index.search_with_threshold(query, k, min_score))
        } else {
            Err(GraniteError::IndexNotFound(key))
        }
    }

    /// Remove a document from all vector indexes in a collection.
    pub fn remove_vector(
        &mut self,
        collection: &str,
        doc_id: &str,
    ) {
        let prefix = format!("{}.", collection);
        let keys: Vec<String> = self
            .indexes
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();

        for key in keys {
            if let Some(index) = self.indexes.get_mut(&key) {
                index.remove(doc_id);
            }
        }
        self.embeddings.remove(doc_id);
    }

    /// Set ef_search for runtime tuning.
    pub fn set_ef_search(
        &mut self,
        collection: &str,
        index_name: &str,
        ef: usize,
    ) {
        let key = format!("{}.{}", collection, index_name);
        if let Some(index) = self.indexes.get_mut(&key) {
            index.set_ef_search(ef);
        }
    }

    /// List all vector indexes for a collection.
    pub fn list_indexes(&self, collection: &str) -> Vec<&VectorIndexDef> {
        let prefix = format!("{}.", collection);
        self.definitions
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v)
            .collect()
    }

    /// Get combined stats.
    pub fn stats(&self) -> serde_json::Value {
        let index_stats: Vec<serde_json::Value> = self
            .indexes
            .iter()
            .map(|(name, idx)| {
                let mut s = idx.stats();
                s["name"] = serde_json::Value::String(name.clone());
                s
            })
            .collect();

        serde_json::json!({
            "indexes": index_stats,
            "embeddings": self.embeddings.stats(),
        })
    }
}
