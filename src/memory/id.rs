use serde::{Deserialize, Serialize};
use std::fmt;

/// Stable identifier of a node in a [`super::MemoryTree`].
///
/// Root is always [`NodeId::ROOT`] (`0`). Subsequent ids are assigned
/// monotonically and are never reused within a tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    /// The well-known root node.
    pub const ROOT: Self = Self(0);

    /// Wrap a raw integer. Used by persistence and tests.
    pub const fn from_raw(id: u64) -> Self {
        Self(id)
    }

    /// Unwrap to the raw integer.
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Whether this is the root id.
    pub const fn is_root(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for NodeId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<NodeId> for u64 {
    fn from(value: NodeId) -> Self {
        value.0
    }
}
