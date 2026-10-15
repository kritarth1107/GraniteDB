use crate::error::{GraniteError, GraniteResult};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Canonical hierarchical path of a node (`/`, `/project`, `/project/session`).
///
/// Segments are stored without slashes. The root path has zero segments and
/// displays as `/`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodePath {
    segments: Vec<String>,
}

impl NodePath {
    /// The path of the tree root.
    pub fn root() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// Parse a `/`-separated path. `""` and `"/"` are root.
    pub fn parse(raw: &str) -> GraniteResult<Self> {
        if raw.is_empty() || raw == "/" {
            return Ok(Self::root());
        }
        let rest = raw.strip_prefix('/').ok_or_else(|| {
            GraniteError::InvalidNodePath(format!("path must start with '/': {raw}"))
        })?;
        if rest.is_empty() {
            return Ok(Self::root());
        }
        if rest.ends_with('/') {
            return Err(GraniteError::InvalidNodePath(format!(
                "path must not end with '/': {raw}"
            )));
        }
        let mut segments = Vec::new();
        for part in rest.split('/') {
            validate_name(part)?;
            segments.push(part.to_string());
        }
        Ok(Self { segments })
    }

    /// True when this is the root path.
    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }

    /// Number of segments below root.
    pub fn depth(&self) -> usize {
        self.segments.len()
    }

    /// Path segments, root-first.
    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    /// Last segment (node name). `None` for root.
    pub fn name(&self) -> Option<&str> {
        self.segments.last().map(String::as_str)
    }

    /// Parent path. `None` for root.
    pub fn parent(&self) -> Option<NodePath> {
        if self.is_root() {
            None
        } else {
            let mut segments = self.segments.clone();
            segments.pop();
            Some(Self { segments })
        }
    }

    /// Append a child name.
    pub fn join(&self, name: &str) -> GraniteResult<Self> {
        validate_name(name)?;
        let mut segments = self.segments.clone();
        segments.push(name.to_string());
        Ok(Self { segments })
    }

    /// True if `other` is this path or a descendant.
    pub fn contains(&self, other: &NodePath) -> bool {
        other.segments.starts_with(&self.segments)
    }
}

impl fmt::Display for NodePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.segments.is_empty() {
            f.write_str("/")
        } else {
            write!(f, "/{}", self.segments.join("/"))
        }
    }
}

impl FromStr for NodePath {
    type Err = GraniteError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Rules for a single path segment / node name.
pub(crate) fn validate_name(name: &str) -> GraniteResult<()> {
    if name.is_empty() {
        return Err(GraniteError::InvalidNodeName {
            name: name.to_string(),
            reason: "name must not be empty".to_string(),
        });
    }
    if name == "." || name == ".." {
        return Err(GraniteError::InvalidNodeName {
            name: name.to_string(),
            reason: "'.' and '..' are reserved".to_string(),
        });
    }
    if name.len() > 255 {
        return Err(GraniteError::InvalidNodeName {
            name: name.to_string(),
            reason: "name longer than 255 bytes".to_string(),
        });
    }
    if name.contains('/') || name.contains('\0') {
        return Err(GraniteError::InvalidNodeName {
            name: name.to_string(),
            reason: "name must not contain '/' or NUL".to_string(),
        });
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(GraniteError::InvalidNodeName {
            name: name.to_string(),
            reason: "name must not contain control characters".to_string(),
        });
    }
    Ok(())
}
