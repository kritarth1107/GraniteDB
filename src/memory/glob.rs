//! Path globs over [`NodePath`](super::path::NodePath).
//!
//! Two wildcards are recognized:
//! - `*` matches exactly one path segment
//! - `**` matches zero or more path segments
//!
//! A leading literal prefix (`/proj` in `/proj/**/memory`) is exposed so the
//! tree can start at that node via the path index instead of scanning.

use super::path::{NodePath, validate_name};
use crate::error::{GraniteError, GraniteResult};
use std::fmt;

/// One segment of a [`PathGlob`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GlobSeg {
    /// Exact segment match.
    Literal(String),
    /// Exactly one segment (`*`).
    Star,
    /// Zero or more segments (`**`).
    StarStar,
}

/// A `/`-separated path pattern with `*` and `**` wildcards.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathGlob {
    segments: Vec<GlobSeg>,
}

impl PathGlob {
    /// Parse a glob. The pattern must start with `/`, like a [`NodePath`].
    pub fn parse(raw: &str) -> GraniteResult<Self> {
        if raw.is_empty() {
            return Err(GraniteError::InvalidPathGlob(
                "glob must not be empty".to_string(),
            ));
        }
        if raw == "/" {
            return Ok(Self {
                segments: Vec::new(),
            });
        }
        let rest = raw.strip_prefix('/').ok_or_else(|| {
            GraniteError::InvalidPathGlob(format!("glob must start with '/': {raw}"))
        })?;
        if rest.is_empty() {
            return Ok(Self {
                segments: Vec::new(),
            });
        }
        if rest.ends_with('/') {
            return Err(GraniteError::InvalidPathGlob(format!(
                "glob must not end with '/': {raw}"
            )));
        }
        let mut segments = Vec::new();
        for part in rest.split('/') {
            if part.is_empty() {
                return Err(GraniteError::InvalidPathGlob(format!(
                    "glob has an empty segment: {raw}"
                )));
            }
            let seg = match part {
                "*" => GlobSeg::Star,
                "**" => GlobSeg::StarStar,
                other if other.chars().all(|c| c == '*') => {
                    return Err(GraniteError::InvalidPathGlob(format!(
                        "unknown wildcard '{other}'; only '*' and '**' are supported"
                    )));
                }
                other => {
                    validate_name(other).map_err(|e| {
                        GraniteError::InvalidPathGlob(format!("invalid glob segment: {e}"))
                    })?;
                    GlobSeg::Literal(other.to_string())
                }
            };
            // Collapse consecutive `**` — they match the same language.
            if matches!(seg, GlobSeg::StarStar)
                && matches!(segments.last(), Some(GlobSeg::StarStar))
            {
                continue;
            }
            segments.push(seg);
        }
        Ok(Self { segments })
    }

    /// Pattern segments, root-first.
    pub fn segments(&self) -> &[GlobSeg] {
        &self.segments
    }

    /// Longest leading run of literal segments, as a [`NodePath`].
    ///
    /// Root (`/`) when the pattern starts with a wildcard or is `/` itself.
    pub fn literal_prefix(&self) -> NodePath {
        let mut path = NodePath::root();
        for seg in &self.segments {
            match seg {
                GlobSeg::Literal(name) => {
                    path = path.join(name).expect("glob literals are validated names");
                }
                GlobSeg::Star | GlobSeg::StarStar => break,
            }
        }
        path
    }

    /// True when every segment is a literal (no `*` or `**`).
    pub fn is_literal(&self) -> bool {
        self.segments
            .iter()
            .all(|s| matches!(s, GlobSeg::Literal(_)))
    }

    /// True when this matches only the tree root.
    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }

    /// True when `path` matches this pattern.
    pub fn matches(&self, path: &NodePath) -> bool {
        match_segs(&self.segments, path.segments())
    }
}

impl fmt::Display for PathGlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.segments.is_empty() {
            return f.write_str("/");
        }
        for seg in &self.segments {
            f.write_str("/")?;
            match seg {
                GlobSeg::Literal(name) => f.write_str(name)?,
                GlobSeg::Star => f.write_str("*")?,
                GlobSeg::StarStar => f.write_str("**")?,
            }
        }
        Ok(())
    }
}

fn match_segs(pat: &[GlobSeg], segs: &[String]) -> bool {
    match pat.first() {
        None => segs.is_empty(),
        Some(GlobSeg::Literal(lit)) => match segs.split_first() {
            Some((head, rest)) if head == lit => match_segs(&pat[1..], rest),
            _ => false,
        },
        Some(GlobSeg::Star) => match segs.split_first() {
            Some((_, rest)) => match_segs(&pat[1..], rest),
            None => false,
        },
        Some(GlobSeg::StarStar) => {
            if pat.len() == 1 {
                return true;
            }
            for i in 0..=segs.len() {
                if match_segs(&pat[1..], &segs[i..]) {
                    return true;
                }
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> NodePath {
        NodePath::parse(s).unwrap()
    }

    fn g(s: &str) -> PathGlob {
        PathGlob::parse(s).unwrap()
    }

    #[test]
    fn parse_root_and_literals() {
        assert!(PathGlob::parse("/").unwrap().is_root());
        assert!(PathGlob::parse("/proj/sess").unwrap().is_literal());
        assert_eq!(
            PathGlob::parse("/proj/sess")
                .unwrap()
                .literal_prefix()
                .to_string(),
            "/proj/sess"
        );
    }

    #[test]
    fn parse_rejects_bad_patterns() {
        assert!(PathGlob::parse("").is_err());
        assert!(PathGlob::parse("proj/*").is_err());
        assert!(PathGlob::parse("/proj/").is_err());
        assert!(PathGlob::parse("/proj/***").is_err());
        assert!(PathGlob::parse("//a").is_err());
        assert!(PathGlob::parse("/has/slash/no").ok().is_some()); // valid
        assert!(PathGlob::parse("/.").is_err());
    }

    #[test]
    fn star_matches_one_segment() {
        let glob = g("/proj/*/memory");
        assert!(glob.matches(&p("/proj/s1/memory")));
        assert!(!glob.matches(&p("/proj/memory")));
        assert!(!glob.matches(&p("/proj/s1/s2/memory")));
        assert!(!glob.matches(&p("/proj/s1/other")));
        assert_eq!(glob.literal_prefix().to_string(), "/proj");
    }

    #[test]
    fn starstar_matches_zero_or_more() {
        let glob = g("/proj/**/memory");
        assert!(glob.matches(&p("/proj/memory")));
        assert!(glob.matches(&p("/proj/s1/memory")));
        assert!(glob.matches(&p("/proj/a/b/c/memory")));
        assert!(!glob.matches(&p("/proj/s1/other")));
        assert!(!glob.matches(&p("/other/memory")));
    }

    #[test]
    fn starstar_alone_matches_everything() {
        let glob = g("/**");
        assert!(glob.matches(&p("/")));
        assert!(glob.matches(&p("/a")));
        assert!(glob.matches(&p("/a/b/c")));
        assert!(glob.literal_prefix().is_root());
    }

    #[test]
    fn star_on_root_is_direct_children() {
        let glob = g("/*");
        assert!(glob.matches(&p("/a")));
        assert!(!glob.matches(&p("/")));
        assert!(!glob.matches(&p("/a/b")));
    }

    #[test]
    fn consecutive_starstar_collapses() {
        let glob = g("/a/**/**/b");
        assert_eq!(glob.to_string(), "/a/**/b");
        assert!(glob.matches(&p("/a/b")));
        assert!(glob.matches(&p("/a/x/y/b")));
    }

    #[test]
    fn mixed_wildcards() {
        let glob = g("/proj/**/sess/*/memory");
        assert!(glob.matches(&p("/proj/sess/e1/memory")));
        assert!(glob.matches(&p("/proj/x/y/sess/e1/memory")));
        assert!(!glob.matches(&p("/proj/sess/memory")));
        assert!(!glob.matches(&p("/proj/sess/e1/e2/memory")));
    }
}
