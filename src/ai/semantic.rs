// ============================================================================
// GraniteDB — Semantic Search Engine
// ============================================================================
// Combines embedding generation, vector indexing, and full-text search
// into a unified semantic search experience. Supports hybrid search
// (vector + keyword) for the best of both worlds.
// ============================================================================

use crate::ai::embedding_pipeline::EmbeddingPipeline;
use crate::vector::distance::DistanceMetric;
use crate::vector::hnsw::{HnswConfig, HnswIndex, VectorSearchResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Semantic search configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticSearchConfig {
    pub dimensions: usize,
    pub metric: DistanceMetric,
    /// Weight for vector similarity in hybrid search (0.0 - 1.0)
    pub vector_weight: f64,
    /// Weight for keyword (BM25) score in hybrid search
    pub keyword_weight: f64,
    /// Number of candidates to fetch from vector search before re-ranking
    pub num_candidates: usize,
}

impl Default for SemanticSearchConfig {
    fn default() -> Self {
        Self {
            dimensions: 384,
            metric: DistanceMetric::Cosine,
            vector_weight: 0.7,
            keyword_weight: 0.3,
            num_candidates: 100,
        }
    }
}

/// A hybrid search result combining vector and keyword scores.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchResult {
    pub doc_id: String,
    /// Vector similarity score (0-1)
    pub vector_score: f64,
    /// Keyword relevance score (BM25)
    pub keyword_score: f64,
    /// Combined hybrid score
    pub hybrid_score: f64,
    /// Distance from query vector
    pub distance: f32,
}

/// The semantic search engine.
pub struct SemanticSearchEngine {
    config: SemanticSearchConfig,
    /// Collection-specific HNSW indexes
    indexes: HashMap<String, HnswIndex>,
    /// Collection-specific embedding pipelines
    pipelines: HashMap<String, EmbeddingPipeline>,
}

impl SemanticSearchEngine {
    pub fn new(config: SemanticSearchConfig) -> Self {
        Self {
            config,
            indexes: HashMap::new(),
            pipelines: HashMap::new(),
        }
    }

    /// Register an embedding pipeline for a collection.
    pub fn register_pipeline(
        &mut self,
        collection: &str,
        pipeline: EmbeddingPipeline,
    ) {
        let hnsw_config = HnswConfig {
            m: 16,
            m0: 32,
            ef_construction: 200,
            ef_search: self.config.num_candidates,
            metric: self.config.metric,
            dimensions: pipeline.dimensions(),
        };

        self.indexes
            .insert(collection.to_string(), HnswIndex::new(hnsw_config));
        self.pipelines
            .insert(collection.to_string(), pipeline);
    }

    /// Index a document with auto-generated embeddings.
    pub fn index_document(
        &mut self,
        collection: &str,
        doc_id: &str,
        doc: &serde_json::Value,
    ) {
        if let Some(pipeline) = self.pipelines.get(collection) {
            let text = pipeline.extract_text(doc);
            let embedding = pipeline.generate_embedding(&text);

            if let Some(index) = self.indexes.get_mut(collection) {
                index.insert(doc_id, embedding);
            }
        }
    }

    /// Pure vector search — find semantically similar documents.
    pub fn vector_search(
        &self,
        collection: &str,
        query: &str,
        k: usize,
    ) -> Vec<VectorSearchResult> {
        if let (Some(pipeline), Some(index)) = (
            self.pipelines.get(collection),
            self.indexes.get(collection),
        ) {
            let query_embedding = pipeline.generate_embedding(query);
            index.search(&query_embedding, k)
        } else {
            Vec::new()
        }
    }

    /// Hybrid search — combine vector similarity with keyword scores.
    pub fn hybrid_search(
        &self,
        collection: &str,
        query: &str,
        k: usize,
        keyword_scores: &HashMap<String, f64>,
    ) -> Vec<HybridSearchResult> {
        let vector_results = self.vector_search(
            collection,
            query,
            self.config.num_candidates,
        );

        let mut hybrid_results: Vec<HybridSearchResult> = vector_results
            .into_iter()
            .map(|vr| {
                let vs = vr.score as f64;
                let ks = keyword_scores.get(&vr.doc_id).copied().unwrap_or(0.0);

                // Reciprocal Rank Fusion (RRF) inspired combination
                let hybrid = self.config.vector_weight * vs
                    + self.config.keyword_weight * ks;

                HybridSearchResult {
                    doc_id: vr.doc_id,
                    vector_score: vs,
                    keyword_score: ks,
                    hybrid_score: hybrid,
                    distance: vr.distance,
                }
            })
            .collect();

        // Sort by hybrid score descending
        hybrid_results
            .sort_by(|a, b| b.hybrid_score.partial_cmp(&a.hybrid_score).unwrap_or(std::cmp::Ordering::Equal));

        hybrid_results.truncate(k);
        hybrid_results
    }

    /// Find similar documents to a given document.
    pub fn find_similar(
        &self,
        collection: &str,
        doc_id: &str,
        k: usize,
    ) -> Vec<VectorSearchResult> {
        // Would look up the document's embedding and search for neighbors
        // For now, return empty (needs embedding store integration)
        Vec::new()
    }

    /// Get stats for a collection's semantic index.
    pub fn stats(&self, collection: &str) -> serde_json::Value {
        if let Some(index) = self.indexes.get(collection) {
            let mut s = index.stats();
            if let Some(pipeline) = self.pipelines.get(collection) {
                s["model"] = pipeline.model_info();
            }
            s["hybrid_config"] = serde_json::json!({
                "vector_weight": self.config.vector_weight,
                "keyword_weight": self.config.keyword_weight,
                "num_candidates": self.config.num_candidates,
            });
            s
        } else {
            serde_json::json!({"error": "No index for collection"})
        }
    }
}
