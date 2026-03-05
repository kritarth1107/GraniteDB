// ============================================================================
// GraniteDB — Search Scoring (TF-IDF + BM25)
// ============================================================================
// Relevance scoring algorithms for full-text search ranking.
// BM25 is the industry-standard ranking function used by Elasticsearch,
// Lucene, and most modern search engines.
// ============================================================================

use crate::search::inverted_index::InvertedIndex;

/// TF-IDF scoring.
pub struct TfIdfScorer;

impl TfIdfScorer {
    /// Compute TF-IDF score for a term in a document.
    /// TF = term_frequency / doc_length
    /// IDF = log(N / df)
    pub fn score(
        term_frequency: u32,
        doc_length: u32,
        doc_frequency: usize,
        total_docs: usize,
    ) -> f64 {
        if doc_length == 0 || doc_frequency == 0 || total_docs == 0 {
            return 0.0;
        }

        let tf = term_frequency as f64 / doc_length as f64;
        let idf = (total_docs as f64 / doc_frequency as f64).ln();
        tf * idf
    }
}

/// BM25 scoring (Okapi BM25).
/// The gold standard for document ranking in information retrieval.
pub struct Bm25Scorer {
    /// Term frequency saturation parameter (typically 1.2-2.0)
    pub k1: f64,
    /// Document length normalization (typically 0.75)
    pub b: f64,
}

impl Default for Bm25Scorer {
    fn default() -> Self {
        Self { k1: 1.5, b: 0.75 }
    }
}

impl Bm25Scorer {
    pub fn new(k1: f64, b: f64) -> Self {
        Self { k1, b }
    }

    /// Compute BM25 score for a single term in a document.
    ///
    /// score(D, Q) = IDF(q) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
    ///
    /// Where:
    ///   tf    = term frequency in document
    ///   dl    = document length
    ///   avgdl = average document length
    ///   N     = total documents
    ///   df    = document frequency of the term
    pub fn score_term(
        &self,
        term_frequency: u32,
        doc_length: u32,
        avg_doc_length: f64,
        doc_frequency: usize,
        total_docs: usize,
    ) -> f64 {
        if total_docs == 0 || doc_frequency == 0 {
            return 0.0;
        }

        let tf = term_frequency as f64;
        let dl = doc_length as f64;

        // IDF with smoothing to avoid negative values
        let idf = ((total_docs as f64 - doc_frequency as f64 + 0.5)
            / (doc_frequency as f64 + 0.5)
            + 1.0)
            .ln();

        let tf_norm = (tf * (self.k1 + 1.0))
            / (tf + self.k1 * (1.0 - self.b + self.b * dl / avg_doc_length));

        idf * tf_norm
    }

    /// Score a document against a multi-term query.
    pub fn score_document(
        &self,
        doc_id: &str,
        query_terms: &[String],
        index: &InvertedIndex,
    ) -> f64 {
        let total_docs = index.num_docs();
        let avg_dl = index.avg_doc_length();
        let doc_len = index.doc_length(doc_id);

        let mut total_score = 0.0;

        for term in query_terms {
            let postings = index.get_postings(term);
            let df = postings.len();

            if let Some(posting) = postings.iter().find(|p| p.doc_id == doc_id) {
                total_score += self.score_term(
                    posting.term_frequency,
                    doc_len,
                    avg_dl,
                    df,
                    total_docs,
                );
            }
        }

        total_score
    }
}

/// A scored search result.
#[derive(Debug, Clone)]
pub struct ScoredResult {
    pub doc_id: String,
    pub score: f64,
    /// Which query terms matched
    pub matched_terms: Vec<String>,
}
