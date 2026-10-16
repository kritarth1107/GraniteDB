//! Query builder over a subtree of a [`MemoryTree`].
//!
//! Start with [`MemoryTree::query`]. The candidate set is the Euler interval
//! of the subtree root. Depth bounds are relative to that root (the root
//! itself has relative depth 0).

use super::id::NodeId;
use super::node::{Node, NodeKind};
use super::tree::MemoryTree;
use crate::error::GraniteResult;
use std::collections::HashSet;

/// Builder that selects nodes under a subtree root.
pub struct SubtreeQuery<'a> {
    tree: &'a MemoryTree,
    root: NodeId,
    kinds: Option<HashSet<NodeKind>>,
    min_depth: Option<usize>,
    max_depth: Option<usize>,
}

impl<'a> SubtreeQuery<'a> {
    pub(crate) fn new(tree: &'a MemoryTree, root: NodeId) -> Self {
        Self {
            tree,
            root,
            kinds: None,
            min_depth: None,
            max_depth: None,
        }
    }

    /// Subtree root this query is scoped to.
    pub fn root(&self) -> NodeId {
        self.root
    }

    /// Keep nodes of this [`NodeKind`]. Repeated calls OR the kinds together.
    pub fn kind(mut self, kind: NodeKind) -> Self {
        self.kinds.get_or_insert_with(HashSet::new).insert(kind);
        self
    }

    /// Keep nodes whose kind is in `kinds`.
    pub fn kinds<I>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = NodeKind>,
    {
        let set = self.kinds.get_or_insert_with(HashSet::new);
        set.extend(kinds);
        self
    }

    /// Inclusive lower bound on depth relative to the subtree root.
    pub fn min_depth(mut self, depth: usize) -> Self {
        self.min_depth = Some(depth);
        self
    }

    /// Inclusive upper bound on depth relative to the subtree root.
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = Some(depth);
        self
    }

    /// Exact relative depth (equivalent to `min_depth(d).max_depth(d)`).
    pub fn depth(self, depth: usize) -> Self {
        self.min_depth(depth).max_depth(depth)
    }

    /// Nodes in the subtree that pass the current filters, in `dfs_in` order.
    pub fn collect(self) -> GraniteResult<Vec<&'a Node>> {
        let root = self.tree.get(self.root)?;
        let root_depth = root.depth();
        Ok(self
            .tree
            .subtree(self.root)?
            .filter(|node| self.matches(node, root_depth))
            .collect())
    }

    fn matches(&self, node: &Node, root_depth: usize) -> bool {
        let rel = node.depth().saturating_sub(root_depth);
        if let Some(min) = self.min_depth {
            if rel < min {
                return false;
            }
        }
        if let Some(max) = self.max_depth {
            if rel > max {
                return false;
            }
        }
        if let Some(kinds) = &self.kinds {
            if !kinds.contains(&node.kind) {
                return false;
            }
        }
        true
    }
}
