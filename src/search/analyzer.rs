// ============================================================================
// GraniteDB — Text Analyzer / Tokenizer
// ============================================================================
// Breaks text into tokens for full-text indexing. Supports multiple
// analysis strategies: standard, whitespace, keyword, and language-aware.
// ============================================================================

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Analyzer type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AnalyzerType {
    /// Standard: lowercase + whitespace + punctuation removal + stop words
    Standard,
    /// Whitespace only
    Whitespace,
    /// Entire field as a single token
    Keyword,
    /// N-gram tokenization
    Ngram { min_gram: usize, max_gram: usize },
    /// Edge n-grams (for autocomplete)
    EdgeNgram { min_gram: usize, max_gram: usize },
}

/// A text analyzer that produces tokens from text.
pub struct TextAnalyzer {
    analyzer_type: AnalyzerType,
    stop_words: HashSet<String>,
}

impl TextAnalyzer {
    pub fn new(analyzer_type: AnalyzerType) -> Self {
        let stop_words = Self::default_stop_words();
        Self {
            analyzer_type,
            stop_words,
        }
    }

    /// Analyze text into tokens.
    pub fn analyze(&self, text: &str) -> Vec<String> {
        match &self.analyzer_type {
            AnalyzerType::Standard => self.standard_analyze(text),
            AnalyzerType::Whitespace => self.whitespace_analyze(text),
            AnalyzerType::Keyword => vec![text.to_string()],
            AnalyzerType::Ngram { min_gram, max_gram } => {
                self.ngram_analyze(text, *min_gram, *max_gram)
            }
            AnalyzerType::EdgeNgram { min_gram, max_gram } => {
                self.edge_ngram_analyze(text, *min_gram, *max_gram)
            }
        }
    }

    fn standard_analyze(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .filter(|s| s.len() >= 2)
            .filter(|s| !self.stop_words.contains(*s))
            .map(|s| self.stem(s))
            .collect()
    }

    fn whitespace_analyze(&self, text: &str) -> Vec<String> {
        text.split_whitespace()
            .map(|s| s.to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    }

    fn ngram_analyze(&self, text: &str, min: usize, max: usize) -> Vec<String> {
        let text = text.to_lowercase();
        let chars: Vec<char> = text.chars().collect();
        let mut tokens = Vec::new();

        for n in min..=max {
            for window in chars.windows(n) {
                let token: String = window.iter().collect();
                if !token.trim().is_empty() {
                    tokens.push(token);
                }
            }
        }
        tokens
    }

    fn edge_ngram_analyze(
        &self,
        text: &str,
        min: usize,
        max: usize,
    ) -> Vec<String> {
        let words: Vec<&str> = text.split_whitespace().collect();
        let mut tokens = Vec::new();

        for word in words {
            let lower = word.to_lowercase();
            let chars: Vec<char> = lower.chars().collect();
            for n in min..=max.min(chars.len()) {
                let token: String = chars[..n].iter().collect();
                tokens.push(token);
            }
        }
        tokens
    }

    /// Very simple English stemmer (suffix stripping).
    fn stem(&self, word: &str) -> String {
        let mut w = word.to_string();
        // Simple suffix rules
        for suffix in &["ingly", "tion", "sion", "ment", "ness", "able", "ible", "ful", "less", "ous", "ive", "ing", "ies", "ied", "ly", "ed", "er", "es", "al", "en", "s"] {
            if w.len() > suffix.len() + 2 && w.ends_with(suffix) {
                w.truncate(w.len() - suffix.len());
                break;
            }
        }
        w
    }

    fn default_stop_words() -> HashSet<String> {
        let words = [
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "from", "had", "has", "have", "he", "her", "his", "how", "i",
            "if", "in", "into", "is", "it", "its", "just", "me", "more",
            "my", "no", "not", "of", "on", "one", "or", "our", "out",
            "over", "own", "she", "so", "some", "than", "that", "the",
            "their", "them", "then", "there", "these", "they", "this",
            "to", "too", "up", "us", "very", "was", "we", "were", "what",
            "when", "where", "which", "while", "who", "whom", "why",
            "will", "with", "you", "your",
        ];
        words.iter().map(|s| s.to_string()).collect()
    }
}
