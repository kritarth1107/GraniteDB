// ============================================================================
// GraniteDB — Buffer Pool
// ============================================================================
// An LRU-based buffer pool that caches frequently-accessed pages in memory.
// Implements pin/unpin semantics and dirty page flushing.
// ============================================================================

use crate::error::{GraniteError, GraniteResult};
use crate::storage::page::Page;
use std::collections::{HashMap, VecDeque};

/// LRU-based buffer pool for caching disk pages in memory.
pub struct BufferPool {
    /// Map from page_id → Page
    pages: HashMap<u64, Page>,
    /// LRU order: front = least recently used
    lru_order: VecDeque<u64>,
    /// Maximum number of pages in the pool
    capacity: usize,
    /// Statistics
    pub stats: BufferPoolStats,
}

/// Performance counters for the buffer pool.
#[derive(Debug, Default, Clone)]
pub struct BufferPoolStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub dirty_writes: u64,
}

impl BufferPool {
    /// Create a new buffer pool with the given capacity (number of pages).
    pub fn new(capacity: usize) -> Self {
        Self {
            pages: HashMap::with_capacity(capacity),
            lru_order: VecDeque::with_capacity(capacity),
            capacity,
            stats: BufferPoolStats::default(),
        }
    }

    /// Fetch a page from the pool (returns None if not cached).
    pub fn get(&mut self, page_id: u64) -> Option<&Page> {
        if self.pages.contains_key(&page_id) {
            self.stats.hits += 1;
            self.touch(page_id);
            self.pages.get(&page_id)
        } else {
            self.stats.misses += 1;
            None
        }
    }

    /// Fetch a mutable reference to a page.
    pub fn get_mut(&mut self, page_id: u64) -> Option<&mut Page> {
        if self.pages.contains_key(&page_id) {
            self.stats.hits += 1;
            self.touch(page_id);
            self.pages.get_mut(&page_id)
        } else {
            self.stats.misses += 1;
            None
        }
    }

    /// Insert a page into the pool. If the pool is full, evict the LRU page.
    /// Returns the evicted page if one was evicted (so the caller can flush it).
    pub fn insert(&mut self, page: Page) -> GraniteResult<Option<Page>> {
        let page_id = page.header.page_id;
        let mut evicted = None;

        // If already present, just replace
        if self.pages.contains_key(&page_id) {
            self.pages.insert(page_id, page);
            self.touch(page_id);
            return Ok(None);
        }

        // Evict if at capacity
        if self.pages.len() >= self.capacity {
            evicted = self.evict_one()?;
        }

        self.pages.insert(page_id, page);
        self.lru_order.push_back(page_id);

        Ok(evicted)
    }

    /// Remove a specific page from the pool.
    pub fn remove(&mut self, page_id: u64) -> Option<Page> {
        self.lru_order.retain(|&id| id != page_id);
        self.pages.remove(&page_id)
    }

    /// Evict the least-recently-used, unpinned page.
    fn evict_one(&mut self) -> GraniteResult<Option<Page>> {
        // Walk the LRU list from front (oldest) and find an evictable page
        let mut evict_idx = None;
        for (i, &page_id) in self.lru_order.iter().enumerate() {
            if let Some(page) = self.pages.get(&page_id) {
                if page.is_evictable() {
                    evict_idx = Some(i);
                    break;
                }
            }
        }

        if let Some(idx) = evict_idx {
            let page_id = self.lru_order.remove(idx).unwrap();
            let page = self.pages.remove(&page_id);
            self.stats.evictions += 1;
            if let Some(ref p) = page {
                if p.is_dirty() {
                    self.stats.dirty_writes += 1;
                }
            }
            Ok(page)
        } else {
            Err(GraniteError::BufferPoolExhausted {
                capacity: self.capacity,
            })
        }
    }

    /// Move a page_id to the back of the LRU list (most recently used).
    fn touch(&mut self, page_id: u64) {
        self.lru_order.retain(|&id| id != page_id);
        self.lru_order.push_back(page_id);
    }

    /// Collect all dirty pages (for flushing to disk).
    pub fn dirty_pages(&self) -> Vec<u64> {
        self.pages
            .iter()
            .filter(|(_, p)| p.is_dirty())
            .map(|(&id, _)| id)
            .collect()
    }

    /// Mark a page as clean after it has been written to disk.
    pub fn mark_clean(&mut self, page_id: u64) {
        if let Some(page) = self.pages.get_mut(&page_id) {
            page.mark_clean();
        }
    }

    /// Returns the current number of pages in the pool.
    pub fn size(&self) -> usize {
        self.pages.len()
    }

    /// Returns the capacity of the pool.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns a snapshot of the stats.
    pub fn stats(&self) -> &BufferPoolStats {
        &self.stats
    }

    /// Flush all dirty pages. Returns the list of dirty pages for the caller to write.
    pub fn flush_all_dirty(&mut self) -> Vec<Page> {
        let dirty_ids = self.dirty_pages();
        let mut flushed = Vec::new();
        for id in dirty_ids {
            if let Some(page) = self.pages.get(&id) {
                flushed.push(page.clone());
            }
            self.mark_clean(id);
        }
        flushed
    }
}
