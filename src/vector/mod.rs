// ============================================================================
// GraniteDB — Vector Search Module
// ============================================================================
// High-performance vector similarity search for AI/ML workloads.
// Supports HNSW (Hierarchical Navigable Small World) index for
// approximate nearest neighbor queries at scale.
// ============================================================================

pub mod distance;
pub mod embedding;
pub mod hnsw;
pub mod quantizer;
pub mod vector_index;

pub use distance::DistanceMetric;
pub use embedding::EmbeddingStore;
pub use hnsw::HnswIndex;
pub use vector_index::VectorIndexManager;
