mod db;         // Declare the db module
mod document;   // Declare the document module

use db::Database;
use document::Document;

#[tokio::main]
async fn main() {
    println!("Welcome to GraniteDB!");

    // Create a new database instance
    let mut db = Database::new();

    // Create a sample document
    let doc = Document::new(serde_json::json!({"name": "John", "age": 30}));

    // Insert the document into the database
    db.insert(doc.clone());

    // Retrieve and display the document
    match db.get(&doc.id) {
        Some(fetched_doc) => println!("Found: {:?}", fetched_doc),
        None => println!("Document not found."),
    }
}
