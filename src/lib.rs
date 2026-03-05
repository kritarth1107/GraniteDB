// ============================================================================
// GraniteDB — Library Root
// ============================================================================
// Re-exports all modules for the GraniteDB engine.
// ============================================================================

pub mod aggregation;
pub mod ai;
pub mod auth;
pub mod cache;
pub mod collection;
pub mod compression;
pub mod config;
pub mod cursor;
pub mod database;
pub mod document;
pub mod error;
pub mod geo;
pub mod index;
pub mod metrics;
pub mod network;
pub mod query;
pub mod replication;
pub mod search;
pub mod sharding;
pub mod storage;
pub mod stream;
pub mod transaction;
pub mod utils;
pub mod vector;

// ── Convenience re-exports ──────────────────────────────────────────────────

pub use config::GraniteConfig;
pub use database::Database;
pub use document::{BsonValue, Document};
pub use error::{GraniteError, GraniteResult};
pub use network::GraniteServer;
pub use storage::StorageEngine;
pub use vector::HnswIndex;
pub use search::FullTextSearchEngine;
pub use ai::SemanticSearchEngine;
