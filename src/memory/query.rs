//! Query builder over a subtree of a [`MemoryTree`].
//!
//! Start with [`MemoryTree::query`]. The candidate set is the Euler interval
//! of the subtree root. Depth bounds are relative to that root (the root
//! itself has relative depth 0).

use super::id::NodeId;
use super::node::{Node, NodeKind};
use super::tree::MemoryTree;
use crate::error::GraniteResult;
use serde_json::Value;
use std::collections::HashSet;

#[derive(Clone, Debug)]
enum PayloadPred {
    Eq { field: String, value: Value },
    Exists { field: String },
    Contains { field: String, needle: String },
}

/// Builder that selects nodes under a subtree root.
pub struct SubtreeQuery<'a> {
    tree: &'a MemoryTree,
    root: NodeId,
    kinds: Option<HashSet<NodeKind>>,
    min_depth: Option<usize>,
    max_depth: Option<usize>,
    predicates: Vec<PayloadPred>,
    most_recent: Option<usize>,
    limit: Option<usize>,
}

impl<'a> SubtreeQuery<'a> {
    pub(crate) fn new(tree: &'a MemoryTree, root: NodeId) -> Self {
        Self {
            tree,
            root,
            kinds: None,
            min_depth: None,
            max_depth: None,
            predicates: Vec::new(),
            most_recent: None,
            limit: None,
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

    /// Keep nodes whose JSON payload field equals `value`.
    ///
    /// `field` is a `.`-separated object path (`text`, `meta.role`).
    pub fn payload_eq(mut self, field: impl Into<String>, value: Value) -> Self {
        self.predicates.push(PayloadPred::Eq {
            field: field.into(),
            value,
        });
        self
    }

    /// Keep nodes that have `field` in their JSON payload (including `null`).
    pub fn payload_exists(mut self, field: impl Into<String>) -> Self {
        self.predicates.push(PayloadPred::Exists {
            field: field.into(),
        });
        self
    }

    /// Keep nodes whose string payload field contains `needle`.
    ///
    /// Non-string fields do not match. Comparison is case-sensitive.
    pub fn payload_contains(mut self, field: impl Into<String>, needle: impl Into<String>) -> Self {
        self.predicates.push(PayloadPred::Contains {
            field: field.into(),
            needle: needle.into(),
        });
        self
    }

    /// Keep the `n` nodes with the latest `updated_at` after other filters.
    ///
    /// Ties break by `NodeId`. The result is newest-first. `n == 0` is empty.
    pub fn most_recent(mut self, n: usize) -> Self {
        self.most_recent = Some(n);
        self
    }

    /// Cap the result at `n` nodes (applied after [`Self::most_recent`]).
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Nodes in the subtree that pass the current filters.
    ///
    /// Order is Euler-tour (`dfs_in` / preorder) unless [`Self::most_recent`]
    /// is set, in which case the hits are newest-`updated_at` first.
    pub fn collect(self) -> GraniteResult<Vec<&'a Node>> {
        let root = self.tree.get(self.root)?;
        let root_depth = root.depth();
        let mut hits: Vec<&'a Node> = self
            .tree
            .subtree(self.root)?
            .filter(|node| self.matches(node, root_depth))
            .collect();
        if let Some(n) = self.most_recent {
            hits.sort_by(|a, b| {
                b.updated_at
                    .cmp(&a.updated_at)
                    .then_with(|| a.id.cmp(&b.id))
            });
            hits.truncate(n);
        }
        if let Some(n) = self.limit {
            hits.truncate(n);
        }
        Ok(hits)
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
        self.predicates
            .iter()
            .all(|pred| pred.matches(&node.payload.data))
    }
}

impl PayloadPred {
    fn matches(&self, data: &Value) -> bool {
        match self {
            Self::Eq { field, value } => lookup_field(data, field) == Some(value),
            Self::Exists { field } => lookup_field(data, field).is_some(),
            Self::Contains { field, needle } => lookup_field(data, field)
                .and_then(Value::as_str)
                .is_some_and(|s| s.contains(needle.as_str())),
        }
    }
}

/// Walk a `.`-separated object path. Empty segments do not match.
fn lookup_field<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    if field.is_empty() {
        return None;
    }
    let mut cur = value;
    for part in field.split('.') {
        if part.is_empty() {
            return None;
        }
        cur = cur.get(part)?;
    }
    Some(cur)
}
