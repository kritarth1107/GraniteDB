//! Hierarchical memory tree: the primary GraniteDB engine API.
//!
//! Agents store memories as nodes under a single root. Each node has a stable
//! [`NodeId`], a canonical [`NodePath`], typed [`NodeKind`], and a [`Payload`]
//! that may include an embedding for later subtree vector search.

mod id;
mod node;
mod path;
mod snapshot;
mod tree;
mod wal;

pub use id::NodeId;
pub use node::{Node, NodeKind, NodeSpec, Payload};
pub use path::NodePath;
pub use tree::{DfsInterval, MemoryTree, SearchHit, SubtreeIter, TreeOpenOptions};
