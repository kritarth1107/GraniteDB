//! Pluggable subtree roll-ups.
//!
//! A [`Rollup`] turns a parent and its descendants into a compact text
//! summary. [`MemoryTree`](super::MemoryTree) holds one implementation
//! (the default is extractive) and feeds it a [`RollupView`] whenever a
//! parent summary is computed.

use super::node::Node;
use crate::error::GraniteResult;

/// A strategy that produces a parent node's summary from its subtree.
///
/// The default engine implementation is extractive concatenation. Agents
/// can supply an LLM-backed type later; this trait is object-safe so the
/// tree can store `Box<dyn Rollup>`.
pub trait Rollup: Send + Sync {
    /// Summarize `view.parent` from its descendants.
    ///
    /// Return `Ok(None)` when there is nothing to say (no descendant
    /// text, empty subtree). Implementations that call a model may
    /// return a [`crate::error::GraniteError`].
    fn summarize(&self, view: &RollupView<'_>) -> GraniteResult<Option<String>>;
}

/// Read-only snapshot of a parent and its preorder subtree.
///
/// `subtree()` yields the parent first, then descendants in the same
/// order as [`super::MemoryTree::subtree`]. `descendants()` skips the
/// parent. Node payloads and cached child summaries are visible so an
/// LLM-backed [`Rollup`] can choose its own inputs later.
pub struct RollupView<'a> {
    parent: &'a Node,
    subtree: Vec<&'a Node>,
}

impl<'a> RollupView<'a> {
    pub(crate) fn from_parts(parent: &'a Node, subtree: Vec<&'a Node>) -> Self {
        Self { parent, subtree }
    }

    /// The node whose summary is being produced.
    pub fn parent(&self) -> &'a Node {
        self.parent
    }

    /// Parent plus descendants, preorder (self first).
    pub fn subtree(&self) -> impl Iterator<Item = &'a Node> + '_ {
        self.subtree.iter().copied()
    }

    /// Descendants only, preorder (parent excluded).
    pub fn descendants(&self) -> impl Iterator<Item = &'a Node> + '_ {
        self.subtree.iter().copied().skip(1)
    }

    /// Number of nodes in the view, including the parent.
    pub fn len(&self) -> usize {
        self.subtree.len()
    }

    /// True when the view contains only the parent.
    pub fn is_empty(&self) -> bool {
        self.subtree.len() <= 1
    }
}
