use crate::document::Document;
use std::collections::HashMap;
// use serde_json::Value;

pub struct Database {
    pub data: HashMap<String, Document>, // Simple in-memory storage using a HashMap
}

impl Database {
    pub fn new() -> Self {
        Database {
            data: HashMap::new(),
        }
    }

    // Insert a new document into the database
    pub fn insert(&mut self, document: Document) {
        self.data.insert(document.id.clone(), document);
    }

    // Retrieve a document by its ID
    pub fn get(&self, id: &str) -> Option<&Document> {
        self.data.get(id)
    }
}
