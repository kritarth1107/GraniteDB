// ============================================================================
// GraniteDB — Write-Ahead Log (WAL)
// ============================================================================
// The WAL guarantees durability by writing every mutation to a sequential
// log file BEFORE applying it to the in-memory state. On crash recovery,
// the WAL is replayed to restore the database to a consistent state.
// ============================================================================

use crate::error::{GraniteError, GraniteResult};
use chrono::Utc;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Types of operations recorded in the WAL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WalOperation {
    /// Insert a new document
    Insert {
        collection: String,
        document_id: String,
        data: Vec<u8>,
    },
    /// Update an existing document
    Update {
        collection: String,
        document_id: String,
        data: Vec<u8>,
    },
    /// Delete a document
    Delete {
        collection: String,
        document_id: String,
    },
    /// Create a new collection
    CreateCollection {
        database: String,
        collection: String,
    },
    /// Drop a collection
    DropCollection {
        database: String,
        collection: String,
    },
    /// Create an index
    CreateIndex {
        collection: String,
        index_name: String,
        fields: Vec<String>,
    },
    /// Drop an index
    DropIndex {
        collection: String,
        index_name: String,
    },
    /// Begin transaction
    TxnBegin { txn_id: String },
    /// Commit transaction
    TxnCommit { txn_id: String },
    /// Abort transaction
    TxnAbort { txn_id: String },
    /// Checkpoint marker
    Checkpoint { timestamp: i64 },
}

/// A single WAL log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Log Sequence Number — monotonically increasing
    pub lsn: u64,
    /// The operation
    pub operation: WalOperation,
    /// Timestamp (UTC epoch millis)
    pub timestamp: i64,
    /// CRC32 of the serialized operation bytes
    pub checksum: u32,
}

/// The Write-Ahead Log.
pub struct WriteAheadLog {
    /// Directory containing WAL segment files
    wal_dir: PathBuf,
    /// Current active segment file writer
    writer: Option<BufWriter<File>>,
    /// Current LSN counter
    current_lsn: u64,
    /// Current segment file index
    current_segment: u64,
    /// Max segment size in bytes
    max_segment_size: usize,
    /// Bytes written to the current segment
    current_segment_bytes: usize,
    /// Whether to fsync after each write
    fsync: bool,
}

impl WriteAheadLog {
    /// Create or open a WAL in the specified directory.
    pub fn open(wal_dir: &Path, max_segment_size: usize, fsync: bool) -> GraniteResult<Self> {
        std::fs::create_dir_all(wal_dir)?;

        // Find the latest segment and LSN
        let (latest_segment, latest_lsn) = Self::find_latest_state(wal_dir)?;

        let mut wal = Self {
            wal_dir: wal_dir.to_path_buf(),
            writer: None,
            current_lsn: latest_lsn,
            current_segment: latest_segment,
            max_segment_size,
            current_segment_bytes: 0,
            fsync,
        };

        wal.open_segment(latest_segment)?;
        Ok(wal)
    }

    /// Append an operation to the WAL. Returns the assigned LSN.
    pub fn append(&mut self, operation: WalOperation) -> GraniteResult<u64> {
        self.current_lsn += 1;
        let lsn = self.current_lsn;

        let op_bytes = bincode::serialize(&operation)
            .map_err(|e| GraniteError::Wal(format!("Serialization failed: {}", e)))?;

        let mut hasher = Hasher::new();
        hasher.update(&op_bytes);
        let checksum = hasher.finalize();

        let entry = WalEntry {
            lsn,
            operation,
            timestamp: Utc::now().timestamp_millis(),
            checksum,
        };

        let entry_bytes = bincode::serialize(&entry)
            .map_err(|e| GraniteError::Wal(format!("Entry serialization failed: {}", e)))?;

        // Write: [4-byte length][entry bytes]
        let len = entry_bytes.len() as u32;
        let len_bytes = len.to_le_bytes();

        if let Some(writer) = &mut self.writer {
            writer
                .write_all(&len_bytes)
                .map_err(|e| GraniteError::Wal(format!("Write len failed: {}", e)))?;
            writer
                .write_all(&entry_bytes)
                .map_err(|e| GraniteError::Wal(format!("Write entry failed: {}", e)))?;

            if self.fsync {
                writer
                    .flush()
                    .map_err(|e| GraniteError::Wal(format!("Flush failed: {}", e)))?;
            }

            self.current_segment_bytes += 4 + entry_bytes.len();

            // Roll segment if needed
            if self.current_segment_bytes >= self.max_segment_size {
                self.roll_segment()?;
            }
        } else {
            return Err(GraniteError::Wal("WAL writer not initialized".to_string()));
        }

        Ok(lsn)
    }

    /// Write a checkpoint marker.
    pub fn checkpoint(&mut self) -> GraniteResult<u64> {
        let ts = Utc::now().timestamp();
        self.append(WalOperation::Checkpoint { timestamp: ts })
    }

    /// Read all entries from all WAL segments (for recovery).
    pub fn read_all_entries(wal_dir: &Path) -> GraniteResult<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut segments = Self::list_segments(wal_dir)?;
        segments.sort();

        for segment_path in segments {
            let mut segment_entries = Self::read_segment(&segment_path)?;
            entries.append(&mut segment_entries);
        }

        Ok(entries)
    }

    /// Read entries from a single segment file.
    fn read_segment(path: &Path) -> GraniteResult<Vec<WalEntry>> {
        let file = File::open(path).map_err(|e| {
            GraniteError::Wal(format!("Failed to open segment {:?}: {}", path, e))
        })?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            // Read 4-byte length prefix
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(GraniteError::Wal(format!(
                        "Failed to read entry length: {}",
                        e
                    )));
                }
            }

            let len = u32::from_le_bytes(len_buf) as usize;
            let mut entry_buf = vec![0u8; len];
            reader.read_exact(&mut entry_buf).map_err(|e| {
                GraniteError::Wal(format!("Failed to read entry data: {}", e))
            })?;

            let entry: WalEntry = bincode::deserialize(&entry_buf).map_err(|e| {
                GraniteError::Wal(format!("Failed to deserialize entry: {}", e))
            })?;

            // Verify checksum
            let op_bytes = bincode::serialize(&entry.operation).unwrap_or_default();
            let mut hasher = Hasher::new();
            hasher.update(&op_bytes);
            if hasher.finalize() != entry.checksum {
                tracing::warn!(lsn = entry.lsn, "WAL entry checksum mismatch — skipping");
                continue;
            }

            entries.push(entry);
        }

        Ok(entries)
    }

    /// List all WAL segment files in the directory.
    fn list_segments(wal_dir: &Path) -> GraniteResult<Vec<PathBuf>> {
        let mut segments = Vec::new();
        if wal_dir.exists() {
            for entry in std::fs::read_dir(wal_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("wal") {
                    segments.push(path);
                }
            }
        }
        Ok(segments)
    }

    /// Open (or create) a segment file for writing.
    fn open_segment(&mut self, segment_index: u64) -> GraniteResult<()> {
        let path = self
            .wal_dir
            .join(format!("segment_{:08}.wal", segment_index));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                GraniteError::Wal(format!("Failed to open segment {:?}: {}", path, e))
            })?;
        self.writer = Some(BufWriter::new(file));
        self.current_segment = segment_index;

        // Calculate current byte count
        self.current_segment_bytes = std::fs::metadata(&path)
            .map(|m| m.len() as usize)
            .unwrap_or(0);

        Ok(())
    }

    /// Roll to a new segment file.
    fn roll_segment(&mut self) -> GraniteResult<()> {
        // Flush and close current writer
        if let Some(writer) = &mut self.writer {
            writer
                .flush()
                .map_err(|e| GraniteError::Wal(format!("Flush failed: {}", e)))?;
        }
        self.writer = None;

        self.current_segment += 1;
        self.open_segment(self.current_segment)?;
        Ok(())
    }

    /// Find the latest segment index and LSN.
    fn find_latest_state(wal_dir: &Path) -> GraniteResult<(u64, u64)> {
        let segments = Self::list_segments(wal_dir)?;
        if segments.is_empty() {
            return Ok((0, 0));
        }

        let mut max_segment: u64 = 0;
        let mut max_lsn: u64 = 0;

        for seg_path in &segments {
            // Extract segment index from filename
            if let Some(stem) = seg_path.file_stem().and_then(|s| s.to_str()) {
                if let Some(idx_str) = stem.strip_prefix("segment_") {
                    if let Ok(idx) = idx_str.parse::<u64>() {
                        if idx > max_segment {
                            max_segment = idx;
                        }
                    }
                }
            }

            // Find max LSN
            if let Ok(entries) = Self::read_segment(seg_path) {
                if let Some(last) = entries.last() {
                    if last.lsn > max_lsn {
                        max_lsn = last.lsn;
                    }
                }
            }
        }

        Ok((max_segment, max_lsn))
    }

    /// Get the current LSN.
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Flush the WAL to disk.
    pub fn flush(&mut self) -> GraniteResult<()> {
        if let Some(writer) = &mut self.writer {
            writer
                .flush()
                .map_err(|e| GraniteError::Wal(format!("Flush failed: {}", e)))?;
        }
        Ok(())
    }
}
