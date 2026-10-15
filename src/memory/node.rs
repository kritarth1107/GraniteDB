use super::id::NodeId;
use super::path::NodePath;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Classification of a memory node. Agents typically use
/// `Root → Project → Session → Episode → Memory`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum NodeKind {
    /// The single tree root.
    Root,
    /// A long-lived project or agent workspace.
    Project,
    /// A conversation or run.
    Session,
    /// A bounded stretch of a session.
    Episode,
    /// A leaf memory (fact, note, tool result, …).
    #[default]
    Memory,
    /// A rolled-up summary of siblings / a subtree.
    Summary,
    /// Escape hatch for application-defined kinds.
    Custom(String),
}

/// serde_json::Value uses `deserialize_any`, which bincode cannot handle.
/// Persist the document as a JSON string and parse it back.
mod json_via_string {
    use serde::{Deserialize, Deserializer, Serializer};
    use serde_json::Value;

    pub fn serialize<S: Serializer>(value: &Value, serializer: S) -> Result<S::Ok, S::Error> {
        let encoded = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        serde_json::from_str(&encoded).map_err(serde::de::Error::custom)
    }
}

/// Application data stored on a node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct Payload {
    /// Arbitrary JSON document.
    #[serde(with = "json_via_string")]
    pub data: Value,
    /// Optional embedding for later subtree vector search.
    pub embedding: Option<Vec<f32>>,
}

impl Payload {
    /// Empty JSON object, no embedding.
    pub fn empty() -> Self {
        Self {
            data: Value::Object(serde_json::Map::new()),
            embedding: None,
        }
    }

    /// Wrap a JSON value.
    pub fn json(data: Value) -> Self {
        Self {
            data,
            embedding: None,
        }
    }

    /// Convenience: `{"text": ...}`.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            data: serde_json::json!({ "text": text.into() }),
            embedding: None,
        }
    }

    /// Attach an embedding.
    pub fn with_embedding(mut self, embedding: Vec<f32>) -> Self {
        self.embedding = Some(embedding);
        self
    }
}

/// A node in the memory tree.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Stable id.
    pub id: NodeId,
    /// Parent id; `None` for root.
    pub parent_id: Option<NodeId>,
    /// Canonical path.
    pub path: NodePath,
    /// Last path segment. Empty for root.
    pub name: String,
    /// Node kind.
    pub kind: NodeKind,
    /// User payload.
    pub payload: Payload,
    /// Optional roll-up summary (populated by later roll-up jobs).
    pub summary: Option<String>,
    /// Creation time (UTC).
    pub created_at: DateTime<Utc>,
    /// Last mutation time (UTC).
    pub updated_at: DateTime<Utc>,
    /// Optional expiry instant.
    pub expires_at: Option<DateTime<Utc>>,
    /// Optional decay score in `[0, 1]`.
    pub decay: Option<f32>,
    pub(crate) children: Vec<NodeId>,
}

impl Node {
    /// Direct child ids, in insertion order.
    pub fn child_ids(&self) -> &[NodeId] {
        &self.children
    }

    /// Depth below root (root has depth 0).
    pub fn depth(&self) -> usize {
        self.path.depth()
    }

    /// True for the root node.
    pub fn is_root(&self) -> bool {
        self.parent_id.is_none()
    }

    /// True when `expires_at` is set and `now` is at or after it.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.map(|t| now >= t).unwrap_or(false)
    }
}

/// Builder for [`super::MemoryTree::insert_with`].
#[derive(Clone, Debug)]
pub struct NodeSpec {
    pub(crate) name: String,
    pub(crate) kind: NodeKind,
    pub(crate) payload: Payload,
    pub(crate) summary: Option<String>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) decay: Option<f32>,
}

impl NodeSpec {
    /// Start a spec with `kind = Memory` and an empty payload.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: NodeKind::Memory,
            payload: Payload::empty(),
            summary: None,
            expires_at: None,
            decay: None,
        }
    }

    /// Set kind.
    pub fn kind(mut self, kind: NodeKind) -> Self {
        self.kind = kind;
        self
    }

    /// Set payload.
    pub fn payload(mut self, payload: Payload) -> Self {
        self.payload = payload;
        self
    }

    /// Set payload to [`Payload::text`].
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.payload = Payload::text(text);
        self
    }

    /// Set payload to a JSON value.
    pub fn json(mut self, data: Value) -> Self {
        self.payload = Payload::json(data);
        self
    }

    /// Set an embedding on the payload.
    pub fn embedding(mut self, embedding: Vec<f32>) -> Self {
        self.payload.embedding = Some(embedding);
        self
    }

    /// Set a summary string.
    pub fn summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }

    /// Expire at an absolute timestamp.
    pub fn expires_at(mut self, when: DateTime<Utc>) -> Self {
        self.expires_at = Some(when);
        self
    }

    /// Expire after `ttl` from now.
    pub fn ttl(mut self, ttl: chrono::Duration) -> Self {
        self.expires_at = Some(Utc::now() + ttl);
        self
    }

    /// Set a decay score.
    pub fn decay(mut self, decay: f32) -> Self {
        self.decay = Some(decay);
        self
    }
}
