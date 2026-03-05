// ============================================================================
// GraniteDB — Request Handler
// ============================================================================
// Routes incoming protocol commands to the appropriate database operations.
// ============================================================================

use crate::database::Database;
use crate::document::document::Document;
use crate::error::GraniteResult;
use crate::network::protocol::{Command, Response};
use crate::storage::engine::StorageEngine;
use std::collections::HashMap;

/// Handles incoming commands against the database engine.
pub struct RequestHandler {
    databases: HashMap<String, Database>,
}

impl RequestHandler {
    pub fn new() -> Self {
        Self {
            databases: HashMap::new(),
        }
    }

    /// Handle a single command.
    pub fn handle(
        &mut self,
        command: &Command,
        request_id: &str,
        storage: &mut StorageEngine,
    ) -> Response {
        let start = std::time::Instant::now();

        let result = match command {
            Command::Ping => Ok(serde_json::json!({ "pong": true })),

            Command::ServerStatus => {
                Ok(serde_json::json!({
                    "status": "running",
                    "databases": self.databases.keys().collect::<Vec<_>>(),
                    "wal_lsn": storage.current_lsn(),
                    "collections": storage.list_collections(),
                }))
            }

            Command::CreateDatabase { name } => {
                if self.databases.contains_key(name) {
                    Err(crate::error::GraniteError::DatabaseAlreadyExists(name.clone()))
                } else {
                    self.databases.insert(name.clone(), Database::new(name));
                    Ok(serde_json::json!({ "created": name }))
                }
            }

            Command::DropDatabase { name } => {
                self.databases.remove(name);
                Ok(serde_json::json!({ "dropped": name }))
            }

            Command::ListDatabases => {
                let names: Vec<_> = self.databases.keys().collect();
                Ok(serde_json::json!({ "databases": names }))
            }

            Command::CreateCollection { database, name } => {
                let db = self.get_or_create_db(database);
                match db.create_collection(name, storage) {
                    Ok(_) => Ok(serde_json::json!({ "created": name })),
                    Err(e) => Err(e),
                }
            }

            Command::DropCollection { database, name } => {
                if let Some(db) = self.databases.get_mut(database) {
                    match db.drop_collection(name, storage) {
                        Ok(_) => Ok(serde_json::json!({ "dropped": name })),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(crate::error::GraniteError::DatabaseNotFound(database.clone()))
                }
            }

            Command::ListCollections { database } => {
                if let Some(db) = self.databases.get(database) {
                    Ok(serde_json::json!({ "collections": db.list_collections() }))
                } else {
                    Ok(serde_json::json!({ "collections": [] }))
                }
            }

            Command::InsertOne {
                database,
                collection,
                document,
            } => {
                let db = self.get_or_create_db(database);
                if db.collection(collection).is_err() {
                    let _ = db.create_collection(collection, storage);
                }
                match Document::from_json(document.clone()) {
                    Ok(doc) => match db.insert(collection, doc, storage) {
                        Ok(id) => Ok(serde_json::json!({ "inserted_id": id })),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                }
            }

            Command::InsertMany {
                database,
                collection,
                documents,
            } => {
                let db = self.get_or_create_db(database);
                if db.collection(collection).is_err() {
                    let _ = db.create_collection(collection, storage);
                }
                let mut ids = Vec::new();
                for doc_json in documents {
                    match Document::from_json(doc_json.clone()) {
                        Ok(doc) => match db.insert(collection, doc, storage) {
                            Ok(id) => ids.push(id),
                            Err(e) => return Response::error(request_id, &e.to_string()),
                        },
                        Err(e) => return Response::error(request_id, &e.to_string()),
                    }
                }
                Ok(serde_json::json!({ "inserted_ids": ids, "count": ids.len() }))
            }

            Command::Find {
                database,
                collection,
                filter,
                sort,
                skip,
                limit,
                ..
            } => {
                if let Some(db) = self.databases.get(database) {
                    let filter_map = Self::json_to_bson_map(filter);
                    match db.find(collection, &filter_map, storage) {
                        Ok(docs) => {
                            let mut json_docs: Vec<serde_json::Value> =
                                docs.iter().map(|d| d.to_json()).collect();

                            // Apply skip
                            if let Some(s) = skip {
                                json_docs = json_docs.into_iter().skip(*s).collect();
                            }
                            // Apply limit
                            if let Some(l) = limit {
                                json_docs = json_docs.into_iter().take(*l).collect();
                            }

                            Ok(serde_json::json!({
                                "documents": json_docs,
                                "count": json_docs.len()
                            }))
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(serde_json::json!({ "documents": [], "count": 0 }))
                }
            }

            Command::FindOne {
                database,
                collection,
                filter,
            } => {
                if let Some(db) = self.databases.get(database) {
                    let filter_map = Self::json_to_bson_map(filter);
                    match db.find(collection, &filter_map, storage) {
                        Ok(docs) => {
                            let doc = docs.first().map(|d| d.to_json());
                            Ok(serde_json::json!({ "document": doc }))
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(serde_json::json!({ "document": null }))
                }
            }

            Command::DeleteMany {
                database,
                collection,
                filter,
            } => {
                if let Some(db) = self.databases.get(database) {
                    let filter_map = Self::json_to_bson_map(filter);
                    match db.delete(collection, &filter_map, storage) {
                        Ok(count) => Ok(serde_json::json!({ "deleted_count": count })),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(crate::error::GraniteError::DatabaseNotFound(database.clone()))
                }
            }

            Command::Count {
                database,
                collection,
                filter,
            } => {
                if let Some(db) = self.databases.get(database) {
                    let filter_map = Self::json_to_bson_map(filter);
                    match db.find(collection, &filter_map, storage) {
                        Ok(docs) => Ok(serde_json::json!({ "count": docs.len() })),
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(serde_json::json!({ "count": 0 }))
                }
            }

            _ => Ok(serde_json::json!({ "message": "Command not yet implemented" })),
        };

        let elapsed = start.elapsed().as_micros();

        match result {
            Ok(data) => Response::success(request_id, data).with_timing(elapsed),
            Err(e) => Response::error(request_id, &e.to_string()).with_timing(elapsed),
        }
    }

    fn get_or_create_db(&mut self, name: &str) -> &mut Database {
        if !self.databases.contains_key(name) {
            self.databases.insert(name.to_string(), Database::new(name));
        }
        self.databases.get_mut(name).unwrap()
    }

    fn json_to_bson_map(
        val: &serde_json::Value,
    ) -> std::collections::BTreeMap<String, crate::document::bson::BsonValue> {
        let mut map = std::collections::BTreeMap::new();
        if let serde_json::Value::Object(obj) = val {
            for (k, v) in obj {
                map.insert(k.clone(), crate::document::bson::BsonValue::from(v.clone()));
            }
        }
        map
    }
}
