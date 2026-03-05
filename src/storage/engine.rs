// ============================================================================
// GraniteDB — Storage Engine
// ============================================================================
// The storage engine ties together the WAL, disk manager, and buffer pool
// into a coherent persistence layer. It handles document serialization,
// page allocation, and crash recovery.
// ============================================================================

use crate::document::Document;
use crate::error::{GraniteError, GraniteResult};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::disk::DiskManager;
use crate::storage::page::Page;
use crate::storage::wal::{WalOperation, WriteAheadLog};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// The unified storage engine.
pub struct StorageEngine {
    /// Base data directory
    data_dir: PathBuf,
    /// Write-Ahead Log
    wal: WriteAheadLog,
    /// Buffer pool for page caching
    buffer_pool: BufferPool,
    /// Per-collection disk managers
    disk_managers: HashMap<String, DiskManager>,
    /// Page size in bytes
    page_size: usize,
    /// In-memory document store (for fast lookups before full page-based storage)
    /// Maps collection_name -> (document_id -> serialized bytes)
    mem_store: HashMap<String, HashMap<String, Vec<u8>>>,
}

impl StorageEngine {
    /// Initialize the storage engine.
    pub fn open(
        data_dir: &Path,
        wal_dir: &Path,
        page_size: usize,
        buffer_pool_pages: usize,
        wal_segment_size: usize,
        wal_fsync: bool,
    ) -> GraniteResult<Self> {
        std::fs::create_dir_all(data_dir)?;
        std::fs::create_dir_all(wal_dir)?;

        let wal = WriteAheadLog::open(wal_dir, wal_segment_size, wal_fsync)?;
        let buffer_pool = BufferPool::new(buffer_pool_pages);

        let mut engine = Self {
            data_dir: data_dir.to_path_buf(),
            wal,
            buffer_pool,
            disk_managers: HashMap::new(),
            page_size,
            mem_store: HashMap::new(),
        };

        // Recover from WAL
        engine.recover(wal_dir)?;

        Ok(engine)
    }

    /// Ensure a disk manager exists for the given collection.
    fn ensure_disk_manager(&mut self, collection: &str) -> GraniteResult<()> {
        if !self.disk_managers.contains_key(collection) {
            let file_path = self.data_dir.join(format!("{}.gdb", collection));
            let dm = DiskManager::open(&file_path, self.page_size)?;
            self.disk_managers.insert(collection.to_string(), dm);
        }
        Ok(())
    }

    /// Insert a document into storage. Writes to WAL first, then memory.
    pub fn insert(&mut self, collection: &str, doc: &Document) -> GraniteResult<()> {
        let serialized = bincode::serialize(doc)
            .map_err(|e| GraniteError::Serialization(e.to_string()))?;

        // WAL first
        self.wal.append(WalOperation::Insert {
            collection: collection.to_string(),
            document_id: doc.id.clone(),
            data: serialized.clone(),
        })?;

        // Memory store
        let col_store = self
            .mem_store
            .entry(collection.to_string())
            .or_insert_with(HashMap::new);
        col_store.insert(doc.id.clone(), serialized);

        Ok(())
    }

    /// Update a document in storage.
    pub fn update(&mut self, collection: &str, doc: &Document) -> GraniteResult<()> {
        let serialized = bincode::serialize(doc)
            .map_err(|e| GraniteError::Serialization(e.to_string()))?;

        self.wal.append(WalOperation::Update {
            collection: collection.to_string(),
            document_id: doc.id.clone(),
            data: serialized.clone(),
        })?;

        let col_store = self
            .mem_store
            .entry(collection.to_string())
            .or_insert_with(HashMap::new);
        col_store.insert(doc.id.clone(), serialized);

        Ok(())
    }

    /// Delete a document from storage.
    pub fn delete(&mut self, collection: &str, document_id: &str) -> GraniteResult<()> {
        self.wal.append(WalOperation::Delete {
            collection: collection.to_string(),
            document_id: document_id.to_string(),
        })?;

        if let Some(col_store) = self.mem_store.get_mut(collection) {
            col_store.remove(document_id);
        }

        Ok(())
    }

    /// Retrieve a document by ID from the in-memory store.
    pub fn get(&self, collection: &str, document_id: &str) -> GraniteResult<Option<Document>> {
        if let Some(col_store) = self.mem_store.get(collection) {
            if let Some(bytes) = col_store.get(document_id) {
                let doc: Document = bincode::deserialize(bytes)
                    .map_err(|e| GraniteError::Serialization(e.to_string()))?;
                return Ok(Some(doc));
            }
        }
        Ok(None)
    }

    /// Get all documents in a collection.
    pub fn get_all(&self, collection: &str) -> GraniteResult<Vec<Document>> {
        let mut docs = Vec::new();
        if let Some(col_store) = self.mem_store.get(collection) {
            for bytes in col_store.values() {
                let doc: Document = bincode::deserialize(bytes)
                    .map_err(|e| GraniteError::Serialization(e.to_string()))?;
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    /// Check if a collection has any data.
    pub fn collection_exists(&self, collection: &str) -> bool {
        self.mem_store.contains_key(collection)
    }

    /// Get the count of documents in a collection.
    pub fn document_count(&self, collection: &str) -> usize {
        self.mem_store
            .get(collection)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// Create a collection entry (WAL-logged).
    pub fn create_collection(&mut self, database: &str, collection: &str) -> GraniteResult<()> {
        self.wal.append(WalOperation::CreateCollection {
            database: database.to_string(),
            collection: collection.to_string(),
        })?;
        self.mem_store
            .entry(collection.to_string())
            .or_insert_with(HashMap::new);
        self.ensure_disk_manager(collection)?;
        Ok(())
    }

    /// Drop a collection (WAL-logged).
    pub fn drop_collection(&mut self, database: &str, collection: &str) -> GraniteResult<()> {
        self.wal.append(WalOperation::DropCollection {
            database: database.to_string(),
            collection: collection.to_string(),
        })?;
        self.mem_store.remove(collection);
        self.disk_managers.remove(collection);
        // Remove data file
        let file_path = self.data_dir.join(format!("{}.gdb", collection));
        if file_path.exists() {
            std::fs::remove_file(&file_path)?;
        }
        Ok(())
    }

    /// Flush the WAL to disk.
    pub fn flush_wal(&mut self) -> GraniteResult<()> {
        self.wal.flush()
    }

    /// Flush all dirty pages from the buffer pool to disk.
    pub fn flush_dirty_pages(&mut self) -> GraniteResult<()> {
        let dirty_pages = self.buffer_pool.flush_all_dirty();
        for page in &dirty_pages {
            // Determine which disk manager to use based on collection_id
            // For now, we just write to any available manager
            for dm in self.disk_managers.values() {
                dm.write_page(page)?;
            }
        }
        Ok(())
    }

    /// Checkpoint: flush everything and write a checkpoint marker.
    pub fn checkpoint(&mut self) -> GraniteResult<()> {
        self.flush_dirty_pages()?;
        self.wal.checkpoint()?;
        self.wal.flush()?;
        tracing::info!("Checkpoint completed at LSN {}", self.wal.current_lsn());
        Ok(())
    }

    /// Recover state by replaying the WAL.
    fn recover(&mut self, wal_dir: &Path) -> GraniteResult<()> {
        let entries = WriteAheadLog::read_all_entries(wal_dir)?;
        if entries.is_empty() {
            return Ok(());
        }

        tracing::info!(
            "Recovering from WAL: {} entries to replay",
            entries.len()
        );

        for entry in entries {
            match entry.operation {
                WalOperation::Insert {
                    collection,
                    document_id,
                    data,
                } => {
                    let col_store = self
                        .mem_store
                        .entry(collection)
                        .or_insert_with(HashMap::new);
                    col_store.insert(document_id, data);
                }
                WalOperation::Update {
                    collection,
                    document_id,
                    data,
                } => {
                    let col_store = self
                        .mem_store
                        .entry(collection)
                        .or_insert_with(HashMap::new);
                    col_store.insert(document_id, data);
                }
                WalOperation::Delete {
                    collection,
                    document_id,
                } => {
                    if let Some(col_store) = self.mem_store.get_mut(&collection) {
                        col_store.remove(&document_id);
                    }
                }
                WalOperation::CreateCollection {
                    collection, ..
                } => {
                    self.mem_store
                        .entry(collection.clone())
                        .or_insert_with(HashMap::new);
                    let _ = self.ensure_disk_manager(&collection);
                }
                WalOperation::DropCollection {
                    collection, ..
                } => {
                    self.mem_store.remove(&collection);
                    self.disk_managers.remove(&collection);
                }
                WalOperation::Checkpoint { .. } => {
                    // Checkpoints are for bookkeeping only
                }
                _ => {
                    // Other operations (TxnBegin, etc.) are handled by the txn manager
                }
            }
        }

        tracing::info!("WAL recovery complete");
        Ok(())
    }

    /// Get a reference to the buffer pool.
    pub fn buffer_pool(&self) -> &BufferPool {
        &self.buffer_pool
    }

    /// Get the current WAL LSN.
    pub fn current_lsn(&self) -> u64 {
        self.wal.current_lsn()
    }

    /// List all collections in the store.
    pub fn list_collections(&self) -> Vec<String> {
        self.mem_store.keys().cloned().collect()
    }
}
