// ============================================================================
// GraniteDB — Metrics Collector
// ============================================================================
// Collects and exposes performance metrics for monitoring.
// ============================================================================

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Global metrics collector.
#[derive(Debug)]
pub struct MetricsCollector {
    pub queries_total: Arc<AtomicU64>,
    pub inserts_total: Arc<AtomicU64>,
    pub updates_total: Arc<AtomicU64>,
    pub deletes_total: Arc<AtomicU64>,
    pub connections_total: Arc<AtomicU64>,
    pub active_connections: Arc<AtomicU64>,
    pub bytes_read: Arc<AtomicU64>,
    pub bytes_written: Arc<AtomicU64>,
    pub errors_total: Arc<AtomicU64>,
    pub wal_writes: Arc<AtomicU64>,
    pub buffer_pool_hits: Arc<AtomicU64>,
    pub buffer_pool_misses: Arc<AtomicU64>,
    pub index_lookups: Arc<AtomicU64>,
    pub collection_scans: Arc<AtomicU64>,
    pub transactions_started: Arc<AtomicU64>,
    pub transactions_committed: Arc<AtomicU64>,
    pub transactions_aborted: Arc<AtomicU64>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            queries_total: Arc::new(AtomicU64::new(0)),
            inserts_total: Arc::new(AtomicU64::new(0)),
            updates_total: Arc::new(AtomicU64::new(0)),
            deletes_total: Arc::new(AtomicU64::new(0)),
            connections_total: Arc::new(AtomicU64::new(0)),
            active_connections: Arc::new(AtomicU64::new(0)),
            bytes_read: Arc::new(AtomicU64::new(0)),
            bytes_written: Arc::new(AtomicU64::new(0)),
            errors_total: Arc::new(AtomicU64::new(0)),
            wal_writes: Arc::new(AtomicU64::new(0)),
            buffer_pool_hits: Arc::new(AtomicU64::new(0)),
            buffer_pool_misses: Arc::new(AtomicU64::new(0)),
            index_lookups: Arc::new(AtomicU64::new(0)),
            collection_scans: Arc::new(AtomicU64::new(0)),
            transactions_started: Arc::new(AtomicU64::new(0)),
            transactions_committed: Arc::new(AtomicU64::new(0)),
            transactions_aborted: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn inc_queries(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_inserts(&self) {
        self.inserts_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_updates(&self) {
        self.updates_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_deletes(&self) {
        self.deletes_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_errors(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_connections(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Export all metrics as JSON.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "queries_total": self.queries_total.load(Ordering::Relaxed),
            "inserts_total": self.inserts_total.load(Ordering::Relaxed),
            "updates_total": self.updates_total.load(Ordering::Relaxed),
            "deletes_total": self.deletes_total.load(Ordering::Relaxed),
            "connections_total": self.connections_total.load(Ordering::Relaxed),
            "active_connections": self.active_connections.load(Ordering::Relaxed),
            "bytes_read": self.bytes_read.load(Ordering::Relaxed),
            "bytes_written": self.bytes_written.load(Ordering::Relaxed),
            "errors_total": self.errors_total.load(Ordering::Relaxed),
            "wal_writes": self.wal_writes.load(Ordering::Relaxed),
            "buffer_pool_hits": self.buffer_pool_hits.load(Ordering::Relaxed),
            "buffer_pool_misses": self.buffer_pool_misses.load(Ordering::Relaxed),
            "index_lookups": self.index_lookups.load(Ordering::Relaxed),
            "collection_scans": self.collection_scans.load(Ordering::Relaxed),
            "transactions_started": self.transactions_started.load(Ordering::Relaxed),
            "transactions_committed": self.transactions_committed.load(Ordering::Relaxed),
            "transactions_aborted": self.transactions_aborted.load(Ordering::Relaxed),
        })
    }

    /// Reset all counters.
    pub fn reset(&self) {
        self.queries_total.store(0, Ordering::Relaxed);
        self.inserts_total.store(0, Ordering::Relaxed);
        self.updates_total.store(0, Ordering::Relaxed);
        self.deletes_total.store(0, Ordering::Relaxed);
        self.errors_total.store(0, Ordering::Relaxed);
        self.wal_writes.store(0, Ordering::Relaxed);
        self.buffer_pool_hits.store(0, Ordering::Relaxed);
        self.buffer_pool_misses.store(0, Ordering::Relaxed);
        self.index_lookups.store(0, Ordering::Relaxed);
        self.collection_scans.store(0, Ordering::Relaxed);
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}
