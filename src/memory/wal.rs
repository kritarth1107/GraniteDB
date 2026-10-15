use super::id::NodeId;
use super::node::{Node, Payload};
use crate::error::{GraniteError, GraniteResult};
use chrono::{DateTime, Utc};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub(crate) const WAL_MAGIC: &[u8; 4] = b"GTWL";
const WAL_VERSION: u16 = 1;
const WAL_HEADER_LEN: usize = 24; // magic(4)+ver(2)+typ(2)+lsn(8)+len(4)+crc(4)

/// Logged mutation. Replay must not consult the wall clock.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum TreeOp {
    Insert(Box<Node>),
    Update {
        id: NodeId,
        payload: Payload,
        summary: Option<String>,
        expires_at: Option<DateTime<Utc>>,
        decay: Option<f32>,
        updated_at: DateTime<Utc>,
    },
    Move {
        id: NodeId,
        new_parent: NodeId,
        updated_at: DateTime<Utc>,
    },
    Rename {
        id: NodeId,
        new_name: String,
        updated_at: DateTime<Utc>,
    },
    DeleteSubtree {
        id: NodeId,
    },
}

/// Crash-safe, length-prefixed, checksummed write-ahead log.
pub(crate) struct TreeWal {
    path: PathBuf,
    file: File,
    lsn: u64,
    fsync: bool,
}

impl TreeWal {
    /// Open (or create) the WAL, truncate a torn tail, and return committed ops.
    pub(crate) fn open(path: &Path, fsync: bool) -> GraniteResult<(Self, Vec<(u64, TreeOp)>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| GraniteError::Wal(format!("open {}: {e}", path.display())))?;

        let recovered = read_records(&mut file)?;
        let (ops, last_good, max_lsn) = (recovered.ops, recovered.last_good, recovered.max_lsn);
        file.set_len(last_good)
            .map_err(|e| GraniteError::Wal(format!("truncate torn tail: {e}")))?;
        file.seek(SeekFrom::Start(last_good))
            .map_err(|e| GraniteError::Wal(format!("seek wal: {e}")))?;
        if fsync {
            file.sync_all()
                .map_err(|e| GraniteError::Wal(format!("fsync after trim: {e}")))?;
        }

        Ok((
            Self {
                path: path.to_path_buf(),
                file,
                lsn: max_lsn,
                fsync,
            },
            ops,
        ))
    }

    pub(crate) fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Continue the LSN sequence after a snapshot truncated the log file.
    pub(crate) fn set_current_lsn(&mut self, lsn: u64) {
        if lsn > self.lsn {
            self.lsn = lsn;
        }
    }

    /// Append an op, fsync if configured, return the new LSN.
    pub(crate) fn append(&mut self, op: &TreeOp) -> GraniteResult<u64> {
        self.lsn += 1;
        let lsn = self.lsn;
        let bytes = encode_record(lsn, op)?;
        self.file
            .write_all(&bytes)
            .map_err(|e| GraniteError::Wal(format!("write: {e}")))?;
        self.file
            .flush()
            .map_err(|e| GraniteError::Wal(format!("flush: {e}")))?;
        if self.fsync {
            self.file
                .sync_all()
                .map_err(|e| GraniteError::Wal(format!("fsync: {e}")))?;
        }
        Ok(lsn)
    }

    pub(crate) fn flush(&mut self) -> GraniteResult<()> {
        self.file
            .flush()
            .map_err(|e| GraniteError::Wal(format!("flush: {e}")))?;
        if self.fsync {
            self.file
                .sync_all()
                .map_err(|e| GraniteError::Wal(format!("fsync: {e}")))?;
        }
        Ok(())
    }

    /// Drop all records but keep the in-memory LSN (after a snapshot).
    pub(crate) fn truncate(&mut self) -> GraniteResult<()> {
        self.file
            .set_len(0)
            .map_err(|e| GraniteError::Wal(format!("truncate: {e}")))?;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| GraniteError::Wal(format!("seek: {e}")))?;
        if self.fsync {
            self.file
                .sync_all()
                .map_err(|e| GraniteError::Wal(format!("fsync truncate: {e}")))?;
            sync_dir_of(&self.path)?;
        }
        Ok(())
    }
}

pub(crate) fn sync_dir_of(file: &Path) -> GraniteResult<()> {
    let Some(dir) = file.parent() else {
        return Ok(());
    };
    sync_dir(dir)
}

pub(crate) fn sync_dir(dir: &Path) -> GraniteResult<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| GraniteError::Wal(format!("fsync dir {}: {e}", dir.display())))
}

fn encode_record(lsn: u64, op: &TreeOp) -> GraniteResult<Vec<u8>> {
    let payload =
        bincode::serialize(op).map_err(|e| GraniteError::Wal(format!("serialize op: {e}")))?;
    let mut buf = Vec::with_capacity(WAL_HEADER_LEN + payload.len());
    buf.extend_from_slice(WAL_MAGIC);
    buf.extend_from_slice(&WAL_VERSION.to_le_bytes());
    buf.extend_from_slice(&1u16.to_le_bytes()); // rec_type = op
    buf.extend_from_slice(&lsn.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    let mut hasher = Hasher::new();
    hasher.update(&buf[4..]);
    hasher.update(&payload);
    buf.extend_from_slice(&hasher.finalize().to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

struct RecoveredWal {
    ops: Vec<(u64, TreeOp)>,
    last_good: u64,
    max_lsn: u64,
}

fn read_records(file: &mut File) -> GraniteResult<RecoveredWal> {
    file.seek(SeekFrom::Start(0))
        .map_err(|e| GraniteError::Wal(format!("seek start: {e}")))?;
    let mut ops = Vec::new();
    let mut last_good: u64 = 0;
    let mut max_lsn: u64 = 0;
    loop {
        let start = last_good;
        let mut header = [0u8; WAL_HEADER_LEN];
        match read_exact_or_eof(file, &mut header)? {
            ReadOutcome::Eof => break,
            ReadOutcome::Partial => {
                // Torn header at end of file: trim back to last_good.
                break;
            }
            ReadOutcome::Full => {}
        }
        if header[0..4] != WAL_MAGIC[..] {
            if start == 0 {
                return Err(GraniteError::Wal(
                    "WAL file does not start with GTWL magic".to_string(),
                ));
            }
            return Err(GraniteError::Wal(format!(
                "corrupt WAL magic at offset {start}"
            )));
        }
        let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
        if version != WAL_VERSION {
            return Err(GraniteError::Wal(format!(
                "unsupported WAL version {version}"
            )));
        }
        let lsn = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let payload_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(header[20..24].try_into().unwrap());
        let mut payload = vec![0u8; payload_len];
        match read_exact_or_eof(file, &mut payload)? {
            ReadOutcome::Full => {}
            ReadOutcome::Eof | ReadOutcome::Partial => break,
        }
        let mut hasher = Hasher::new();
        hasher.update(&header[4..20]);
        hasher.update(&payload);
        if hasher.finalize() != crc {
            // Bad CRC at the tail is treated as a torn write.
            break;
        }
        let op: TreeOp = bincode::deserialize(&payload)
            .map_err(|e| GraniteError::Wal(format!("deserialize LSN {lsn}: {e}")))?;
        ops.push((lsn, op));
        max_lsn = max_lsn.max(lsn);
        last_good = start + WAL_HEADER_LEN as u64 + payload_len as u64;
    }
    Ok(RecoveredWal {
        ops,
        last_good,
        max_lsn,
    })
}

enum ReadOutcome {
    Full,
    Partial,
    Eof,
}

fn read_exact_or_eof(file: &mut File, buf: &mut [u8]) -> GraniteResult<ReadOutcome> {
    let mut filled = 0usize;
    while filled < buf.len() {
        match file.read(&mut buf[filled..]) {
            Ok(0) if filled == 0 => return Ok(ReadOutcome::Eof),
            Ok(0) => return Ok(ReadOutcome::Partial),
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(GraniteError::Wal(format!("read: {e}"))),
        }
    }
    Ok(ReadOutcome::Full)
}
