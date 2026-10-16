//! Pluggable subtree roll-ups.
//!
//! A [`Rollup`] turns a parent and its descendants into a compact text
//! summary. [`MemoryTree`](super::MemoryTree) holds one implementation
//! (the default is extractive) and feeds it a [`RollupView`] whenever a
//! parent summary is computed.

use super::node::{Node, Payload};
use crate::error::GraniteResult;
use serde_json::Value;

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

/// Default roll-up: concatenate descendant payload text in preorder.
///
/// No network, no model. Each descendant with extractable text becomes a
/// `{name}: {text}` line. The joined result is truncated to `max_chars`
/// on a Unicode scalar boundary (default 2048). Empty input yields `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtractiveRollup {
    max_chars: usize,
}

impl Default for ExtractiveRollup {
    fn default() -> Self {
        Self { max_chars: 2048 }
    }
}

impl ExtractiveRollup {
    /// Default budget of 2048 characters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Truncate the joined summary to at most `max_chars` characters.
    pub fn with_max_chars(mut self, max_chars: usize) -> Self {
        self.max_chars = max_chars;
        self
    }

    /// Character budget applied after joining lines.
    pub fn max_chars(&self) -> usize {
        self.max_chars
    }
}

/// Pull a single text string out of a payload, if one exists.
///
/// A JSON string is used as-is. An object contributes its `text` field
/// when that field is a string. Surrounding whitespace is trimmed;
/// empty results are dropped. Other JSON shapes are ignored so the
/// default roll-up stays deterministic and free of dumped structure.
pub fn extract_payload_text(payload: &Payload) -> Option<String> {
    match &payload.data {
        Value::String(s) => trim_nonempty(s),
        Value::Object(map) => match map.get("text") {
            Some(Value::String(s)) => trim_nonempty(s),
            _ => None,
        },
        _ => None,
    }
}

fn trim_nonempty(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

/// Keep at most `max` Unicode scalars, ending with `…` when truncated.
fn truncate_chars(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        return s.to_string();
    }
    if max == 0 {
        return String::new();
    }
    let take = max.saturating_sub(1);
    let mut out: String = s.chars().take(take).collect();
    out.push('…');
    out
}

impl Rollup for ExtractiveRollup {
    fn summarize(&self, view: &RollupView<'_>) -> GraniteResult<Option<String>> {
        let mut parts: Vec<String> = Vec::new();
        for node in view.descendants() {
            let Some(text) = extract_payload_text(&node.payload) else {
                continue;
            };
            if node.name.is_empty() {
                parts.push(text);
            } else {
                parts.push(format!("{}: {text}", node.name));
            }
        }
        if parts.is_empty() {
            return Ok(None);
        }
        Ok(Some(truncate_chars(&parts.join("\n"), self.max_chars)))
    }
}
