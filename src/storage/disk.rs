// ============================================================================
// GraniteDB — Disk I/O
// ============================================================================
// Low-level disk operations for reading / writing pages and data files.
// ============================================================================

use crate::error::{GraniteError, GraniteResult};
use crate::storage::page::{Page, PageHeader, PAGE_HEADER_SIZE};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Manages file I/O for a single data file (collection-level).
pub struct DiskManager {
    /// Path to the data file
    file_path: PathBuf,
    /// Page size in bytes
    page_size: usize,
    /// Next page ID to allocate
    next_page_id: u64,
}

impl DiskManager {
    /// Open or create a data file.
    pub fn open(file_path: &Path, page_size: usize) -> GraniteResult<Self> {
        // Ensure parent directory exists
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create file if it doesn't exist
        if !file_path.exists() {
            File::create(file_path)?;
        }

        let file_size = std::fs::metadata(file_path)
            .map(|m| m.len())
            .unwrap_or(0);
        let next_page_id = if page_size > 0 {
            file_size / page_size as u64
        } else {
            0
        };

        Ok(Self {
            file_path: file_path.to_path_buf(),
            page_size,
            next_page_id,
        })
    }

    /// Allocate a new page and return its ID.
    pub fn allocate_page(&mut self) -> u64 {
        let id = self.next_page_id;
        self.next_page_id += 1;
        id
    }

    /// Write a page to disk at the position determined by its page_id.
    pub fn write_page(&self, page: &Page) -> GraniteResult<()> {
        let offset = page.header.page_id * self.page_size as u64;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.file_path)?;

        file.seek(SeekFrom::Start(offset))?;

        // Write header
        let header_bytes = page.header.to_bytes();
        let mut header_buf = vec![0u8; PAGE_HEADER_SIZE];
        let copy_len = header_bytes.len().min(PAGE_HEADER_SIZE);
        header_buf[..copy_len].copy_from_slice(&header_bytes[..copy_len]);
        file.write_all(&header_buf)?;

        // Write data section
        let data_size = self.page_size - PAGE_HEADER_SIZE;
        if page.data.len() >= data_size {
            file.write_all(&page.data[..data_size])?;
        } else {
            file.write_all(&page.data)?;
            // Pad remaining
            let pad = vec![0u8; data_size - page.data.len()];
            file.write_all(&pad)?;
        }

        file.flush()?;
        Ok(())
    }

    /// Read a page from disk by its page_id.
    pub fn read_page(&self, page_id: u64) -> GraniteResult<Page> {
        let offset = page_id * self.page_size as u64;
        let mut file = File::open(&self.file_path).map_err(|e| {
            GraniteError::Storage(format!("Failed to open data file: {}", e))
        })?;

        file.seek(SeekFrom::Start(offset))?;

        // Read header
        let mut header_buf = vec![0u8; PAGE_HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = PageHeader::from_bytes(&header_buf)?;

        // Read data
        let data_size = self.page_size - PAGE_HEADER_SIZE;
        let mut data = vec![0u8; data_size];
        file.read_exact(&mut data)?;

        Ok(Page {
            header,
            data,
            dirty: false,
            pin_count: 0,
        })
    }

    /// Get the number of pages currently in the file.
    pub fn page_count(&self) -> u64 {
        self.next_page_id
    }

    /// Get the file path.
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Sync the file to disk (fsync).
    pub fn sync(&self) -> GraniteResult<()> {
        let file = File::open(&self.file_path)?;
        file.sync_all()?;
        Ok(())
    }
}
