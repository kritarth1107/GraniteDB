// ============================================================================
// GraniteDB — BSON-like Value System
// ============================================================================
// GraniteDB uses a rich value type system inspired by BSON / JSON.
// This allows flexible, schema-optional document storage while providing
// strong typing for queries and indexing.
// ============================================================================

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// Core value type for GraniteDB documents.
/// Supports all BSON-like types plus extensions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BsonValue {
    /// Null / missing value
    Null,
    /// Boolean
    Boolean(bool),
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 64-bit floating point
    Double(f64),
    /// UTF-8 string
    String(String),
    /// Ordered map of key-value pairs (embedded document)
    Document(BTreeMap<String, BsonValue>),
    /// Ordered array of values
    Array(Vec<BsonValue>),
    /// Binary data with subtype
    Binary(Vec<u8>),
    /// UUID stored as a string
    ObjectId(String),
    /// UTC datetime
    DateTime(DateTime<Utc>),
    /// Timestamp (seconds, increment) — for internal replication ordering
    Timestamp { seconds: u64, increment: u32 },
    /// Regular expression pattern
    Regex { pattern: String, options: String },
}

impl BsonValue {
    /// Returns the type name as a string.
    pub fn type_name(&self) -> &'static str {
        match self {
            BsonValue::Null => "null",
            BsonValue::Boolean(_) => "boolean",
            BsonValue::Int32(_) => "int32",
            BsonValue::Int64(_) => "int64",
            BsonValue::Double(_) => "double",
            BsonValue::String(_) => "string",
            BsonValue::Document(_) => "document",
            BsonValue::Array(_) => "array",
            BsonValue::Binary(_) => "binary",
            BsonValue::ObjectId(_) => "objectId",
            BsonValue::DateTime(_) => "datetime",
            BsonValue::Timestamp { .. } => "timestamp",
            BsonValue::Regex { .. } => "regex",
        }
    }

    /// Check if this value is "truthy" (non-null, non-false, non-zero).
    pub fn is_truthy(&self) -> bool {
        match self {
            BsonValue::Null => false,
            BsonValue::Boolean(b) => *b,
            BsonValue::Int32(n) => *n != 0,
            BsonValue::Int64(n) => *n != 0,
            BsonValue::Double(n) => *n != 0.0,
            BsonValue::String(s) => !s.is_empty(),
            BsonValue::Array(a) => !a.is_empty(),
            BsonValue::Document(d) => !d.is_empty(),
            _ => true,
        }
    }

    /// Attempt to coerce this value to a f64 for comparisons.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            BsonValue::Int32(n) => Some(*n as f64),
            BsonValue::Int64(n) => Some(*n as f64),
            BsonValue::Double(n) => Some(*n),
            _ => None,
        }
    }

    /// Attempt to get this value as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            BsonValue::String(s) => Some(s),
            BsonValue::ObjectId(s) => Some(s),
            _ => None,
        }
    }

    /// Get a nested value using dot-notation path (e.g., "address.city").
    pub fn get_path(&self, path: &str) -> Option<&BsonValue> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = self;
        for part in parts {
            match current {
                BsonValue::Document(map) => {
                    current = map.get(part)?;
                }
                BsonValue::Array(arr) => {
                    let index: usize = part.parse().ok()?;
                    current = arr.get(index)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    /// Set a value at a dot-notation path. Creates intermediate documents as needed.
    pub fn set_path(&mut self, path: &str, value: BsonValue) {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = self;
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                if let BsonValue::Document(map) = current {
                    map.insert(part.to_string(), value);
                    return;
                }
            } else {
                if let BsonValue::Document(map) = current {
                    current = map
                        .entry(part.to_string())
                        .or_insert_with(|| BsonValue::Document(BTreeMap::new()));
                } else {
                    return;
                }
            }
        }
    }
}

// ── Conversions from serde_json::Value ───────────────────────────────────────

impl From<serde_json::Value> for BsonValue {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => BsonValue::Null,
            serde_json::Value::Bool(b) => BsonValue::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        BsonValue::Int32(i as i32)
                    } else {
                        BsonValue::Int64(i)
                    }
                } else if let Some(f) = n.as_f64() {
                    BsonValue::Double(f)
                } else {
                    BsonValue::Null
                }
            }
            serde_json::Value::String(s) => BsonValue::String(s),
            serde_json::Value::Array(a) => {
                BsonValue::Array(a.into_iter().map(BsonValue::from).collect())
            }
            serde_json::Value::Object(o) => {
                let mut map = BTreeMap::new();
                for (k, v) in o {
                    map.insert(k, BsonValue::from(v));
                }
                BsonValue::Document(map)
            }
        }
    }
}

impl From<BsonValue> for serde_json::Value {
    fn from(v: BsonValue) -> Self {
        match v {
            BsonValue::Null => serde_json::Value::Null,
            BsonValue::Boolean(b) => serde_json::Value::Bool(b),
            BsonValue::Int32(n) => serde_json::json!(n),
            BsonValue::Int64(n) => serde_json::json!(n),
            BsonValue::Double(n) => serde_json::json!(n),
            BsonValue::String(s) => serde_json::Value::String(s),
            BsonValue::ObjectId(s) => serde_json::json!({ "$oid": s }),
            BsonValue::DateTime(dt) => serde_json::json!({ "$date": dt.to_rfc3339() }),
            BsonValue::Timestamp { seconds, increment } => {
                serde_json::json!({ "$timestamp": { "t": seconds, "i": increment } })
            }
            BsonValue::Binary(b) => {
                serde_json::json!({ "$binary": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &b) })
            }
            BsonValue::Regex { pattern, options } => {
                serde_json::json!({ "$regex": pattern, "$options": options })
            }
            BsonValue::Array(a) => {
                serde_json::Value::Array(a.into_iter().map(serde_json::Value::from).collect())
            }
            BsonValue::Document(map) => {
                let obj: serde_json::Map<String, serde_json::Value> = map
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect();
                serde_json::Value::Object(obj)
            }
        }
    }
}

impl fmt::Display for BsonValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BsonValue::Null => write!(f, "null"),
            BsonValue::Boolean(b) => write!(f, "{}", b),
            BsonValue::Int32(n) => write!(f, "{}", n),
            BsonValue::Int64(n) => write!(f, "{}", n),
            BsonValue::Double(n) => write!(f, "{}", n),
            BsonValue::String(s) => write!(f, "\"{}\"", s),
            BsonValue::ObjectId(s) => write!(f, "ObjectId(\"{}\")", s),
            BsonValue::DateTime(dt) => write!(f, "DateTime(\"{}\")", dt),
            BsonValue::Document(_) => write!(f, "{{...}}"),
            BsonValue::Array(a) => write!(f, "[{} elements]", a.len()),
            BsonValue::Binary(b) => write!(f, "Binary({} bytes)", b.len()),
            BsonValue::Timestamp { seconds, increment } => {
                write!(f, "Timestamp({}, {})", seconds, increment)
            }
            BsonValue::Regex { pattern, options } => {
                write!(f, "/{}/{}", pattern, options)
            }
        }
    }
}

impl PartialOrd for BsonValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (BsonValue::Null, BsonValue::Null) => Some(std::cmp::Ordering::Equal),
            (BsonValue::Boolean(a), BsonValue::Boolean(b)) => a.partial_cmp(b),
            (BsonValue::Int32(a), BsonValue::Int32(b)) => a.partial_cmp(b),
            (BsonValue::Int64(a), BsonValue::Int64(b)) => a.partial_cmp(b),
            (BsonValue::Double(a), BsonValue::Double(b)) => a.partial_cmp(b),
            (BsonValue::String(a), BsonValue::String(b)) => a.partial_cmp(b),
            (BsonValue::DateTime(a), BsonValue::DateTime(b)) => a.partial_cmp(b),
            // Cross-numeric comparisons
            (BsonValue::Int32(a), BsonValue::Int64(b)) => (*a as i64).partial_cmp(b),
            (BsonValue::Int64(a), BsonValue::Int32(b)) => a.partial_cmp(&(*b as i64)),
            (BsonValue::Int32(a), BsonValue::Double(b)) => (*a as f64).partial_cmp(b),
            (BsonValue::Double(a), BsonValue::Int32(b)) => a.partial_cmp(&(*b as f64)),
            (BsonValue::Int64(a), BsonValue::Double(b)) => (*a as f64).partial_cmp(b),
            (BsonValue::Double(a), BsonValue::Int64(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }
}
