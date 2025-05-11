use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Document {
    pub id: String,
    pub data: serde_json::Value, // This is where the actual document data goes
}

impl Document {
    pub fn new(data: serde_json::Value) -> Self {
        Document {
            id: Uuid::new_v4().to_string(), // Generate a unique ID for each document
            data,
        }
    }
}
