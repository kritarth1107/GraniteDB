// ============================================================================
// GraniteDB — Page Management
// ============================================================================
// Pages are the fundamental unit of storage. Each page is a fixed-size block
// on disk (default 16KB). Pages contain serialized documents and include
// a CRC32 checksum for integrity verification.
// ============================================================================

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

/// Fixed page header size in bytes.
pub const PAGE_HEADER_SIZE: usize = 64;

/// Page status flags.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PageStatus {
    /// Page is free and can be allocated
    Free,
    /// Page is in use and contains data
    InUse,
    /// Page is marked for compaction
    PendingCompaction,
    /// Page is an overflow page (for large documents)
    Overflow,
}

/// Header of a data page.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    /// Unique page identifier
    pub page_id: u64,
    /// Status of this page
    pub status: PageStatus,
    /// Number of items stored in this page
    pub item_count: u32,
    /// Bytes used in the data section
    pub used_bytes: u32,
    /// CRC32 checksum of the data section
    pub checksum: u32,
    /// ID of the next overflow page (0 = none)
    pub overflow_page_id: u64,
    /// The collection this page belongs to
    pub collection_id: u64,
    /// Last modification timestamp (unix epoch seconds)
    pub last_modified: u64,
}

/// A single data page (fixed size).
#[derive(Debug, Clone)]
pub struct Page {
    /// Page header
    pub header: PageHeader,
    /// Raw data bytes (page_size - header_size)
    pub data: Vec<u8>,
    /// Whether this page has been modified in memory (dirty flag)
    pub dirty: bool,
    /// Pin count — how many operations hold a reference
    pub pin_count: u32,
}

/// Represents an item stored within a page.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageSlot {
    /// Offset within the page data section
    pub offset: u32,
    /// Length of the serialized item
    pub length: u32,
    /// Document ID this slot belongs to
    pub document_id: String,
}

impl Page {
    /// Create a new empty page with the given size.
    pub fn new(page_id: u64, page_size: usize, collection_id: u64) -> Self {
        let data_size = page_size.saturating_sub(PAGE_HEADER_SIZE);
        Self {
            header: PageHeader {
                page_id,
                status: PageStatus::Free,
                item_count: 0,
                used_bytes: 0,
                checksum: 0,
                overflow_page_id: 0,
                collection_id,
                last_modified: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            },
            data: vec![0u8; data_size],
            dirty: false,
            pin_count: 0,
        }
    }

    /// Write data to this page at the given offset.
    pub fn write(&mut self, offset: usize, bytes: &[u8]) -> crate::error::GraniteResult<()> {
        if offset + bytes.len() > self.data.len() {
            return Err(crate::error::GraniteError::Storage(format!(
                "Page {} overflow: offset={} len={} capacity={}",
                self.header.page_id,
                offset,
                bytes.len(),
                self.data.len()
            )));
        }
        self.data[offset..offset + bytes.len()].copy_from_slice(bytes);
        self.header.used_bytes = (offset + bytes.len()) as u32;
        self.dirty = true;
        self.update_checksum();
        Ok(())
    }

    /// Read data from this page.
    pub fn read(&self, offset: usize, length: usize) -> crate::error::GraniteResult<&[u8]> {
        if offset + length > self.data.len() {
            return Err(crate::error::GraniteError::Storage(format!(
                "Page {} read out of bounds: offset={} len={} capacity={}",
                self.header.page_id,
                offset,
                length,
                self.data.len()
            )));
        }
        Ok(&self.data[offset..offset + length])
    }

    /// Compute and store the CRC32 checksum of the data.
    pub fn update_checksum(&mut self) {
        let mut hasher = Hasher::new();
        hasher.update(&self.data[..self.header.used_bytes as usize]);
        self.header.checksum = hasher.finalize();
    }

    /// Verify the page's integrity.
    pub fn verify_checksum(&self) -> bool {
        let mut hasher = Hasher::new();
        hasher.update(&self.data[..self.header.used_bytes as usize]);
        hasher.finalize() == self.header.checksum
    }

    /// Returns the free space available.
    pub fn free_space(&self) -> usize {
        self.data.len() - self.header.used_bytes as usize
    }

    /// Is this page dirty (modified but not flushed to disk)?
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Mark the page as clean (just flushed to disk).
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }

    /// Pin this page (incrementing the reference count).
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    /// Unpin this page.
    pub fn unpin(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
    }

    /// Whether this page can be evicted from the buffer pool.
    pub fn is_evictable(&self) -> bool {
        self.pin_count == 0
    }
}

impl PageHeader {
    /// Serialize the header to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize a header from bytes.
    pub fn from_bytes(bytes: &[u8]) -> crate::error::GraniteResult<Self> {
        bincode::deserialize(bytes).map_err(|e| {
            crate::error::GraniteError::Storage(format!("Failed to deserialize page header: {}", e))
        })
    }
}
