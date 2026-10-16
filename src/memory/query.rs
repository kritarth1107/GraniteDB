//! Query builder over a subtree of a [`MemoryTree`].
//!
//! Start with [`MemoryTree::query`] and add filters in later methods. The
//! candidate set is the Euler interval of the subtree root.

use super::id::NodeId;
use super::node::Node;
use super::tree::MemoryTree;
use crate::error::GraniteResult;

/// Builder that selects nodes under a subtree root.
pub struct SubtreeQuery<'a> {
    tree: &'a MemoryTree,
    root: NodeId,
}

impl<'a> SubtreeQuery<'a> {
    pub(crate) fn new(tree: &'a MemoryTree, root: NodeId) -> Self {
        Self { tree, root }
    }

    /// Subtree root this query is scoped to.
    pub fn root(&self) -> NodeId {
        self.root
    }

    /// Nodes in the subtree, in Euler-tour (`dfs_in` / preorder) order.
    pub fn collect(self) -> GraniteResult<Vec<&'a Node>> {
        Ok(self.tree.subtree(self.root)?.collect())
    }
}
