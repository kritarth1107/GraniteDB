use super::id::NodeId;
use super::node::Node;
use crate::error::{GraniteError, GraniteResult};
use crate::storage::wal::sync_dir;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

const SNAP_MAGIC: &[u8; 4] = b"GTSN";
const SNAP_VERSION: u16 = 1;

/// Point-in-time dump of a memory tree, taken at a WAL LSN.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TreeSnapshot {
    pub format_version: u32,
    pub lsn: u64,
    pub root_id: NodeId,
    pub next_id: u64,
    pub nodes: HashMap<NodeId, Node>,
}

pub(crate) fn save(dir: &Path, snap: &TreeSnapshot) -> GraniteResult<()> {
    let payload = bincode::serialize(snap)
        .map_err(|e| GraniteError::Storage(format!("serialize snapshot: {e}")))?;
    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let crc = hasher.finalize();

    let mut buf = Vec::with_capacity(4 + 2 + 8 + 8 + 4 + payload.len());
    buf.extend_from_slice(SNAP_MAGIC);
    buf.extend_from_slice(&SNAP_VERSION.to_le_bytes());
    buf.extend_from_slice(&snap.lsn.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&payload);

    let tmp = dir.join("snapshot.bin.tmp");
    let dest = dir.join("snapshot.bin");
    {
        let mut file = File::create(&tmp)
            .map_err(|e| GraniteError::Storage(format!("create snapshot tmp: {e}")))?;
        file.write_all(&buf)
            .map_err(|e| GraniteError::Storage(format!("write snapshot: {e}")))?;
        file.sync_all()
            .map_err(|e| GraniteError::Storage(format!("fsync snapshot: {e}")))?;
    }
    std::fs::rename(&tmp, &dest)
        .map_err(|e| GraniteError::Storage(format!("rename snapshot: {e}")))?;
    sync_dir(dir)?;
    Ok(())
}

pub(crate) fn load(dir: &Path) -> GraniteResult<Option<TreeSnapshot>> {
    let path = dir.join("snapshot.bin");
    if !path.exists() {
        return Ok(None);
    }
    let mut file =
        File::open(&path).map_err(|e| GraniteError::Storage(format!("open snapshot: {e}")))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|e| GraniteError::Storage(format!("read snapshot: {e}")))?;
    if buf.len() < 26 {
        return Err(GraniteError::Storage(
            "snapshot.bin is too short".to_string(),
        ));
    }
    if buf[0..4] != SNAP_MAGIC[..] {
        return Err(GraniteError::Storage(
            "snapshot.bin does not start with GTSN magic".to_string(),
        ));
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version != SNAP_VERSION {
        return Err(GraniteError::Storage(format!(
            "unsupported snapshot version {version}"
        )));
    }
    let payload_len = u64::from_le_bytes(buf[14..22].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf[22..26].try_into().unwrap());
    if buf.len() != 26 + payload_len {
        return Err(GraniteError::Storage(format!(
            "snapshot length mismatch: file={} expected={}",
            buf.len(),
            26 + payload_len
        )));
    }
    let payload = &buf[26..];
    let mut hasher = Hasher::new();
    hasher.update(payload);
    if hasher.finalize() != crc {
        return Err(GraniteError::Storage(
            "snapshot checksum mismatch".to_string(),
        ));
    }
    let snap: TreeSnapshot = bincode::deserialize(payload)
        .map_err(|e| GraniteError::Storage(format!("deserialize snapshot: {e}")))?;
    Ok(Some(snap))
}
