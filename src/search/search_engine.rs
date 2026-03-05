// ============================================================================
// GraniteDB — Full-Text Search Engine
// ============================================================================
// High-level search engine combining analyzer, inverted index, and
// BM25 scoring into a unified search API.
// ============================================================================

use crate::document::document::Document;
use crate::error::{GraniteError, GraniteResult};
use crate::search::analyzer::{AnalyzerType, TextAnalyzer};
use crate::search::inverted_index::InvertedIndex;
use crate::search::scoring::{Bm25Scorer, ScoredResult};
use std::collections::HashMap;

/// Configuration for a text search index.
#[derive(Debug, Clone)]
pub struct TextIndexConfig {
    pub name: String,
    pub collection: String,
    pub fields: Vec<String>,
    pub analyzer: AnalyzerType,
    pub field_weights: HashMap<String, f64>,
}

/// The full-text search engine.
pub struct FullTextSearchEngine {
    /// One inverted index per text index
    indexes: HashMap<String, InvertedIndex>,
    /// Associated configs
    configs: HashMap<String, TextIndexConfig>,
    /// Analyzers
    analyzers: HashMap<String, TextAnalyzer>,
    /// BM25 scorer
    scorer: Bm25Scorer,
}

impl FullTextSearchEngine {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            configs: HashMap::new(),
            analyzers: HashMap::new(),
            scorer: Bm25Scorer::default(),
        }
    }

    /// Create a text search index.
    pub fn create_index(&mut self, config: TextIndexConfig) -> GraniteResult<()> {
        let key = format!("{}.{}", config.collection, config.name);
        if self.indexes.contains_key(&key) {
            return Err(GraniteError::IndexAlreadyExists(key));
        }

        let analyzer = TextAnalyzer::new(config.analyzer.clone());
        self.indexes.insert(key.clone(), InvertedIndex::new());
        self.analyzers.insert(key.clone(), analyzer);
        self.configs.insert(key, config);
        Ok(())
    }

    /// Index a document for full-text search.
    pub fn index_document(
        &mut self,
        collection: &str,
        doc: &Document,
    ) -> GraniteResult<()> {
        let prefix = format!("{}.", collection);
        let keys: Vec<String> = self
            .configs
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();

        for key in keys {
            if let Some(config) = self.configs.get(&key) {
                let fields = config.fields.clone();
                for field in &fields {
                    if let Some(text) = doc.get_string(field) {
                        if let Some(analyzer) = self.analyzers.get(&key) {
                            let tokens = analyzer.analyze(text);
                            if let Some(index) = self.indexes.get_mut(&key) {
                                index.index_document(&doc.id, field, &tokens);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Remove a document from all text indexes.
    pub fn remove_document(&mut self, collection: &str, doc_id: &str) {
        let prefix = format!("{}.", collection);
        let keys: Vec<String> = self
            .indexes
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();

        for key in keys {
            if let Some(index) = self.indexes.get_mut(&key) {
                index.remove_document(doc_id);
            }
        }
    }

    /// Search across a text index.
    pub fn search(
        &self,
        collection: &str,
        index_name: &str,
        query: &str,
        limit: usize,
    ) -> GraniteResult<Vec<ScoredResult>> {
        let key = format!("{}.{}", collection, index_name);

        let analyzer = self
            .analyzers
            .get(&key)
            .ok_or_else(|| GraniteError::IndexNotFound(key.clone()))?;
        let index = self
            .indexes
            .get(&key)
            .ok_or_else(|| GraniteError::IndexNotFound(key.clone()))?;

        let query_tokens = analyzer.analyze(query);
        if query_tokens.is_empty() {
            return Ok(Vec::new());
        }

        // Get all candidate documents
        let candidate_docs = index.boolean_or(&query_tokens);

        // Score each candidate
        let mut results: Vec<ScoredResult> = candidate_docs
            .iter()
            .map(|doc_id| {
                let score = self.scorer.score_document(doc_id, &query_tokens, index);
                let matched: Vec<String> = query_tokens
                    .iter()
                    .filter(|t| {
                        index
                            .get_postings(t)
                            .iter()
                            .any(|p| p.doc_id == *doc_id)
                    })
                    .cloned()
                    .collect();

                ScoredResult {
                    doc_id: doc_id.clone(),
                    score,
                    matched_terms: matched,
                }
            })
            .collect();

        // Sort by score descending
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        // Limit results
        results.truncate(limit);

        Ok(results)
    }

    /// Phrase search (exact phrase matching).
    pub fn phrase_search(
        &self,
        collection: &str,
        index_name: &str,
        phrase: &str,
        limit: usize,
    ) -> GraniteResult<Vec<ScoredResult>> {
        let key = format!("{}.{}", collection, index_name);

        let analyzer = self
            .analyzers
            .get(&key)
            .ok_or_else(|| GraniteError::IndexNotFound(key.clone()))?;
        let index = self
            .indexes
            .get(&key)
            .ok_or_else(|| GraniteError::IndexNotFound(key.clone()))?;

        let tokens = analyzer.analyze(phrase);
        let matching_docs = index.phrase_query(&tokens);

        let mut results: Vec<ScoredResult> = matching_docs
            .iter()
            .map(|doc_id| {
                let score = self.scorer.score_document(doc_id, &tokens, index);
                ScoredResult {
                    doc_id: doc_id.clone(),
                    score,
                    matched_terms: tokens.clone(),
                }
            })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        Ok(results)
    }

    /// Autocomplete / suggest based on prefix.
    pub fn autocomplete(
        &self,
        collection: &str,
        index_name: &str,
        prefix: &str,
        limit: usize,
    ) -> GraniteResult<Vec<ScoredResult>> {
        let key = format!("{}.{}", collection, index_name);

        let index = self
            .indexes
            .get(&key)
            .ok_or_else(|| GraniteError::IndexNotFound(key.clone()))?;

        // Create edge n-gram analyzer for the prefix
        let prefix_analyzer = TextAnalyzer::new(AnalyzerType::EdgeNgram {
            min_gram: 2,
            max_gram: prefix.len(),
        });
        let tokens = prefix_analyzer.analyze(prefix);

        let candidate_docs = index.boolean_or(&tokens);

        let mut results: Vec<ScoredResult> = candidate_docs
            .into_iter()
            .map(|doc_id| ScoredResult {
                doc_id,
                score: 1.0,
                matched_terms: tokens.clone(),
            })
            .collect();

        results.truncate(limit);
        Ok(results)
    }

    /// Get stats for a text index.
    pub fn stats(
        &self,
        collection: &str,
        index_name: &str,
    ) -> GraniteResult<serde_json::Value> {
        let key = format!("{}.{}", collection, index_name);
        let index = self
            .indexes
            .get(&key)
            .ok_or_else(|| GraniteError::IndexNotFound(key.clone()))?;

        Ok(serde_json::json!({
            "vocabulary_size": index.vocabulary_size(),
            "indexed_documents": index.num_docs(),
            "avg_doc_length": index.avg_doc_length(),
        }))
    }
}
