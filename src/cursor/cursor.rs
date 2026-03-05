// ============================================================================
// GraniteDB — Cursor
// ============================================================================

use crate::document::document::Document;
use uuid::Uuid;

/// A cursor for iterating over query results.
pub struct Cursor {
    pub id: String,
    documents: Vec<Document>,
    position: usize,
    batch_size: usize,
    exhausted: bool,
}

impl Cursor {
    pub fn new(documents: Vec<Document>, batch_size: usize) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            documents,
            position: 0,
            batch_size,
            exhausted: false,
        }
    }

    /// Get the next batch of documents.
    pub fn next_batch(&mut self) -> Vec<&Document> {
        if self.exhausted {
            return Vec::new();
        }

        let end = (self.position + self.batch_size).min(self.documents.len());
        let batch: Vec<&Document> = self.documents[self.position..end].iter().collect();
        self.position = end;

        if self.position >= self.documents.len() {
            self.exhausted = true;
        }

        batch
    }

    /// Check if the cursor has more results.
    pub fn has_next(&self) -> bool {
        !self.exhausted
    }

    /// Get the total number of documents.
    pub fn total(&self) -> usize {
        self.documents.len()
    }

    /// Get remaining documents.
    pub fn remaining(&self) -> usize {
        self.documents.len().saturating_sub(self.position)
    }

    /// Reset the cursor to the beginning.
    pub fn rewind(&mut self) {
        self.position = 0;
        self.exhausted = false;
    }

    /// Collect all remaining documents.
    pub fn collect_all(&mut self) -> Vec<&Document> {
        let all: Vec<&Document> = self.documents[self.position..].iter().collect();
        self.position = self.documents.len();
        self.exhausted = true;
        all
    }
}
