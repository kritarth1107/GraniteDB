//! Tree mutations logged through the shared [`crate::storage::WriteAheadLog`].

use super::id::NodeId;
use super::node::{Node, Payload};
use crate::error::{GraniteError, GraniteResult};
use crate::storage::wal::{WalOperation, WriteAheadLog};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

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

/// Thin adapter: tree ops ride on the crate's segmented WAL.
pub(crate) struct TreeWal {
    inner: WriteAheadLog,
}

impl TreeWal {
    /// Open `<dir>` as a WAL directory (`segment_*.wal`), replay MemoryTree records.
    pub(crate) fn open(wal_dir: &Path, fsync: bool) -> GraniteResult<(Self, Vec<(u64, TreeOp)>)> {
        const SEGMENT: usize = 64 * 1024 * 1024;
        let inner = WriteAheadLog::open(wal_dir, SEGMENT, fsync)?;
        let mut ops = Vec::new();
        for entry in WriteAheadLog::read_all_entries(wal_dir)? {
            if let WalOperation::MemoryTree { op } = entry.operation {
                let tree_op: TreeOp = bincode::deserialize(&op).map_err(|e| {
                    GraniteError::Wal(format!("decode tree op LSN {}: {e}", entry.lsn))
                })?;
                ops.push((entry.lsn, tree_op));
            }
        }
        Ok((Self { inner }, ops))
    }

    pub(crate) fn lsn(&self) -> u64 {
        self.inner.current_lsn()
    }

    pub(crate) fn set_current_lsn(&mut self, lsn: u64) {
        self.inner.set_current_lsn(lsn);
    }

    pub(crate) fn append(&mut self, op: &TreeOp) -> GraniteResult<u64> {
        let bytes = bincode::serialize(op)
            .map_err(|e| GraniteError::Wal(format!("serialize tree op: {e}")))?;
        self.inner.append(WalOperation::MemoryTree { op: bytes })
    }

    pub(crate) fn flush(&mut self) -> GraniteResult<()> {
        self.inner.flush()
    }

    pub(crate) fn truncate(&mut self) -> GraniteResult<()> {
        self.inner.reset_segments()
    }
}
