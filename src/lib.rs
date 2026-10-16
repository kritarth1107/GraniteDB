// ============================================================================
// GraniteDB — Library Root
// ============================================================================
// Re-exports all modules for the GraniteDB engine.
// ============================================================================

/// Document-oriented sketch kept for later mining; not the product API.
/// Clippy is held to `-D warnings` on [`memory`]; these modules are pre-pivot.
macro_rules! legacy_mod {
    ($name:ident) => {
        #[allow(
            dead_code,
            unused_imports,
            unused_variables,
            unused_mut,
            unused_assignments,
            clippy::all
        )]
        pub mod $name;
    };
}

legacy_mod!(aggregation);
legacy_mod!(ai);
legacy_mod!(auth);
legacy_mod!(cache);
legacy_mod!(collection);
legacy_mod!(compression);
legacy_mod!(config);
legacy_mod!(cursor);
legacy_mod!(database);
legacy_mod!(document);
legacy_mod!(geo);
legacy_mod!(index);
legacy_mod!(metrics);
legacy_mod!(network);
legacy_mod!(query);
legacy_mod!(replication);
legacy_mod!(search);
legacy_mod!(sharding);
legacy_mod!(stream);
legacy_mod!(transaction);
legacy_mod!(utils);

pub mod storage;
pub mod vector;

pub mod error;
pub mod memory;

// ── Convenience re-exports ──────────────────────────────────────────────────

pub use ai::SemanticSearchEngine;
pub use config::GraniteConfig;
pub use database::Database;
pub use document::{BsonValue, Document};
pub use error::{GraniteError, GraniteResult};
pub use memory::{
    DfsInterval, GlobSeg, MemoryTree, Node, NodeId, NodeKind, NodePath, NodeSpec, PathGlob,
    Payload, Rollup, RollupView, SearchHit, SubtreeQuery, TreeOpenOptions,
};
pub use network::GraniteServer;
pub use search::FullTextSearchEngine;
pub use storage::StorageEngine;
pub use storage::wal::WriteAheadLog;
pub use vector::{DistanceMetric, HnswIndex, PQConfig, ProductQuantizer};
