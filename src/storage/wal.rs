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
    /// A `MemoryTree` mutation. `op` is bincode of `memory::wal::TreeOp`.
    MemoryTree { op: Vec<u8> },
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
        wal.trim_torn_tail()?;
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

            writer
                .flush()
                .map_err(|e| GraniteError::Wal(format!("Flush failed: {}", e)))?;
            if self.fsync {
                writer
                    .get_ref()
                    .sync_all()
                    .map_err(|e| GraniteError::Wal(format!("fsync failed: {}", e)))?;
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

    /// Read entries from a single segment file. A torn tail is ignored.
    fn read_segment(path: &Path) -> GraniteResult<Vec<WalEntry>> {
        Ok(Self::read_segment_with_tail(path)?.0)
    }

    /// Read a segment and the byte offset of the last complete record.
    fn read_segment_with_tail(path: &Path) -> GraniteResult<(Vec<WalEntry>, u64)> {
        let file = File::open(path)
            .map_err(|e| GraniteError::Wal(format!("Failed to open segment {:?}: {}", path, e)))?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut last_good: u64 = 0;

        loop {
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
            if len > 64 * 1024 * 1024 {
                break;
            }
            let mut entry_buf = vec![0u8; len];
            match reader.read_exact(&mut entry_buf) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(GraniteError::Wal(format!(
                        "Failed to read entry data: {}",
                        e
                    )));
                }
            }

            let Ok(entry) = bincode::deserialize::<WalEntry>(&entry_buf) else {
                break;
            };

            let op_bytes = bincode::serialize(&entry.operation).unwrap_or_default();
            let mut hasher = Hasher::new();
            hasher.update(&op_bytes);
            if hasher.finalize() != entry.checksum {
                break;
            }

            entries.push(entry);
            last_good += 4 + len as u64;
        }

        Ok((entries, last_good))
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
            .truncate(false)
            .open(&path)
            .map_err(|e| GraniteError::Wal(format!("Failed to open segment {:?}: {}", path, e)))?;
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

    /// Flush the WAL to disk. When `fsync` is on, also `sync_all`s the file.
    pub fn flush(&mut self) -> GraniteResult<()> {
        if let Some(writer) = &mut self.writer {
            writer
                .flush()
                .map_err(|e| GraniteError::Wal(format!("Flush failed: {}", e)))?;
            if self.fsync {
                writer
                    .get_ref()
                    .sync_all()
                    .map_err(|e| GraniteError::Wal(format!("fsync failed: {}", e)))?;
            }
        }
        Ok(())
    }

    /// Raise the LSN so replay after a snapshot does not reuse sequence numbers.
    pub fn set_current_lsn(&mut self, lsn: u64) {
        if lsn > self.current_lsn {
            self.current_lsn = lsn;
        }
    }

    /// Directory that holds `segment_*.wal` files.
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Drop every segment after a successful snapshot. Keeps the in-memory LSN.
    pub fn reset_segments(&mut self) -> GraniteResult<()> {
        if let Some(writer) = &mut self.writer {
            writer
                .flush()
                .map_err(|e| GraniteError::Wal(format!("Flush failed: {}", e)))?;
        }
        self.writer = None;
        for path in Self::list_segments(&self.wal_dir)? {
            std::fs::remove_file(&path)
                .map_err(|e| GraniteError::Wal(format!("remove segment {:?}: {}", path, e)))?;
        }
        self.current_segment = 0;
        self.current_segment_bytes = 0;
        self.open_segment(0)?;
        if self.fsync {
            self.flush()?;
            sync_dir(&self.wal_dir)?;
        }
        Ok(())
    }

    fn segment_path(&self, index: u64) -> PathBuf {
        self.wal_dir.join(format!("segment_{:08}.wal", index))
    }

    fn trim_torn_tail(&mut self) -> GraniteResult<()> {
        let path = self.segment_path(self.current_segment);
        if !path.exists() {
            return Ok(());
        }
        let last_good = Self::read_segment_with_tail(&path)?.1;
        let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        if last_good >= len {
            return Ok(());
        }
        self.writer = None;
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| GraniteError::Wal(format!("open for trim {:?}: {}", path, e)))?;
        file.set_len(last_good)
            .map_err(|e| GraniteError::Wal(format!("trim {:?}: {}", path, e)))?;
        if self.fsync {
            file.sync_all()
                .map_err(|e| GraniteError::Wal(format!("fsync trim: {}", e)))?;
        }
        self.open_segment(self.current_segment)?;
        Ok(())
    }
}

/// `fsync` a directory (needed after rename/unlink of WAL segments or snapshots).
pub fn sync_dir(dir: &Path) -> GraniteResult<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| GraniteError::Wal(format!("fsync dir {}: {e}", dir.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_replay_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WriteAheadLog::open(dir.path(), 1024 * 1024, false).unwrap();
        wal.append(WalOperation::Checkpoint { timestamp: 1 })
            .unwrap();
        wal.append(WalOperation::MemoryTree { op: vec![1, 2, 3] })
            .unwrap();
        wal.flush().unwrap();
        drop(wal);

        let seg = dir.path().join("segment_00000000.wal");
        {
            let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
            f.write_all(&[8, 0, 0, 0, 9, 9, 9]).unwrap();
        }

        let wal = WriteAheadLog::open(dir.path(), 1024 * 1024, false).unwrap();
        let entries = WriteAheadLog::read_all_entries(dir.path()).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(matches!(
            entries[1].operation,
            WalOperation::MemoryTree { .. }
        ));
        assert_eq!(wal.current_lsn(), 2);
        let len = std::fs::metadata(&seg).unwrap().len();
        assert!(len > 0);
        assert!(len < 8 + 10000);
    }
}
