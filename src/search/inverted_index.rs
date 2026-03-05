// ============================================================================
// GraniteDB — Inverted Index
// ============================================================================
// Maps terms to the documents (and positions) they appear in.
// Core data structure for full-text search.
// ============================================================================

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

/// A posting in the inverted index: one term occurrence in one document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posting {
    pub doc_id: String,
    /// Term frequency in this document
    pub term_frequency: u32,
    /// Positions where the term appears (for phrase queries)
    pub positions: Vec<u32>,
    /// Field the term was found in
    pub field: String,
}

/// The inverted index: term → list of postings.
pub struct InvertedIndex {
    /// term → postings list (sorted by doc_id)
    index: BTreeMap<String, Vec<Posting>>,
    /// doc_id → document length (number of tokens)
    doc_lengths: HashMap<String, u32>,
    /// Total number of documents
    num_docs: usize,
    /// Average document length
    avg_doc_length: f64,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            index: BTreeMap::new(),
            doc_lengths: HashMap::new(),
            num_docs: 0,
            avg_doc_length: 0.0,
        }
    }

    /// Index a document: add its tokens to the inverted index.
    pub fn index_document(
        &mut self,
        doc_id: &str,
        field: &str,
        tokens: &[String],
    ) {
        // Remove previous entry (if re-indexing)
        self.remove_document(doc_id);

        let doc_len = tokens.len() as u32;
        self.doc_lengths.insert(doc_id.to_string(), doc_len);
        self.num_docs += 1;
        self.update_avg_doc_length();

        // Build term → (frequency, positions) map
        let mut term_data: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_data.entry(token.as_str()).or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        // Add postings
        for (term, (tf, positions)) in term_data {
            let posting = Posting {
                doc_id: doc_id.to_string(),
                term_frequency: tf,
                positions,
                field: field.to_string(),
            };
            self.index
                .entry(term.to_string())
                .or_default()
                .push(posting);
        }
    }

    /// Remove a document from the index.
    pub fn remove_document(&mut self, doc_id: &str) {
        if self.doc_lengths.remove(doc_id).is_some() {
            self.num_docs = self.num_docs.saturating_sub(1);
            self.update_avg_doc_length();

            // Remove from all postings lists
            for postings in self.index.values_mut() {
                postings.retain(|p| p.doc_id != doc_id);
            }
            // Clean up empty terms
            self.index.retain(|_, postings| !postings.is_empty());
        }
    }

    /// Look up a term's postings.
    pub fn get_postings(&self, term: &str) -> &[Posting] {
        self.index.get(term).map(|p| p.as_slice()).unwrap_or(&[])
    }

    /// Get the document frequency of a term (number of docs containing it).
    pub fn doc_frequency(&self, term: &str) -> usize {
        self.get_postings(term).len()
    }

    /// Get total number of indexed documents.
    pub fn num_docs(&self) -> usize {
        self.num_docs
    }

    /// Get the length of a document.
    pub fn doc_length(&self, doc_id: &str) -> u32 {
        self.doc_lengths.get(doc_id).copied().unwrap_or(0)
    }

    /// Get the average document length.
    pub fn avg_doc_length(&self) -> f64 {
        self.avg_doc_length
    }

    fn update_avg_doc_length(&mut self) {
        if self.num_docs == 0 {
            self.avg_doc_length = 0.0;
        } else {
            let total: u64 = self.doc_lengths.values().map(|&l| l as u64).sum();
            self.avg_doc_length = total as f64 / self.num_docs as f64;
        }
    }

    /// Boolean AND: documents containing ALL given terms.
    pub fn boolean_and(&self, terms: &[String]) -> Vec<String> {
        if terms.is_empty() {
            return Vec::new();
        }

        let mut result: HashSet<String> = self
            .get_postings(&terms[0])
            .iter()
            .map(|p| p.doc_id.clone())
            .collect();

        for term in &terms[1..] {
            let docs: HashSet<String> = self
                .get_postings(term)
                .iter()
                .map(|p| p.doc_id.clone())
                .collect();
            result = result.intersection(&docs).cloned().collect();
        }

        result.into_iter().collect()
    }

    /// Boolean OR: documents containing ANY of the given terms.
    pub fn boolean_or(&self, terms: &[String]) -> Vec<String> {
        let mut result: HashSet<String> = HashSet::new();
        for term in terms {
            for posting in self.get_postings(term) {
                result.insert(posting.doc_id.clone());
            }
        }
        result.into_iter().collect()
    }

    /// Phrase query: find documents where terms appear consecutively.
    pub fn phrase_query(&self, terms: &[String]) -> Vec<String> {
        if terms.is_empty() {
            return Vec::new();
        }
        if terms.len() == 1 {
            return self
                .get_postings(&terms[0])
                .iter()
                .map(|p| p.doc_id.clone())
                .collect();
        }

        // Get docs that contain ALL terms
        let candidate_docs = self.boolean_and(terms);
        let mut results = Vec::new();

        for doc_id in &candidate_docs {
            // Get positions for each term in this doc
            let positions: Vec<Vec<u32>> = terms
                .iter()
                .map(|term| {
                    self.get_postings(term)
                        .iter()
                        .find(|p| p.doc_id == *doc_id)
                        .map(|p| p.positions.clone())
                        .unwrap_or_default()
                })
                .collect();

            // Check for consecutive positions
            if Self::has_consecutive_positions(&positions) {
                results.push(doc_id.clone());
            }
        }

        results
    }

    fn has_consecutive_positions(positions: &[Vec<u32>]) -> bool {
        if positions.is_empty() || positions[0].is_empty() {
            return false;
        }

        for &start_pos in &positions[0] {
            let mut found = true;
            for (i, term_positions) in positions.iter().enumerate().skip(1) {
                let expected_pos = start_pos + i as u32;
                if !term_positions.contains(&expected_pos) {
                    found = false;
                    break;
                }
            }
            if found {
                return true;
            }
        }
        false
    }

    /// Get vocabulary size (number of unique terms).
    pub fn vocabulary_size(&self) -> usize {
        self.index.len()
    }
}
