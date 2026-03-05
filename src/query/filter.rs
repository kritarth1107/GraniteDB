// ============================================================================
// GraniteDB — Query Filter
// ============================================================================
// Structured representation of query filters, supporting MongoDB-style
// comparison and logical operators.
// ============================================================================

use crate::document::bson::BsonValue;
use serde::{Deserialize, Serialize};

/// A query filter expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterExpr {
    /// Field equality: { "field": value }
    Eq { field: String, value: BsonValue },
    /// Not equal: { "field": { "$ne": value } }
    Ne { field: String, value: BsonValue },
    /// Greater than
    Gt { field: String, value: BsonValue },
    /// Greater than or equal
    Gte { field: String, value: BsonValue },
    /// Less than
    Lt { field: String, value: BsonValue },
    /// Less than or equal
    Lte { field: String, value: BsonValue },
    /// In array: { "field": { "$in": [v1, v2] } }
    In { field: String, values: Vec<BsonValue> },
    /// Not in array
    Nin { field: String, values: Vec<BsonValue> },
    /// Exists check
    Exists { field: String, exists: bool },
    /// Regex match
    Regex { field: String, pattern: String },
    /// Type check
    Type { field: String, bson_type: String },
    /// Element match (for arrays of documents)
    ElemMatch { field: String, filter: Box<FilterExpr> },
    /// Logical AND
    And(Vec<FilterExpr>),
    /// Logical OR
    Or(Vec<FilterExpr>),
    /// Logical NOT
    Not(Box<FilterExpr>),
    /// Logical NOR
    Nor(Vec<FilterExpr>),
    /// Always true (empty filter)
    All,
}

impl FilterExpr {
    /// Evaluate this filter against a document's data.
    pub fn matches(&self, data: &std::collections::BTreeMap<String, BsonValue>, doc_id: &str) -> bool {
        match self {
            FilterExpr::All => true,

            FilterExpr::Eq { field, value } => {
                if field == "_id" {
                    if let BsonValue::String(id) = value {
                        return doc_id == id;
                    }
                }
                let doc = BsonValue::Document(data.clone());
                doc.get_path(field)
                    .map(|v| v == value)
                    .unwrap_or(false)
            }

            FilterExpr::Ne { field, value } => {
                let doc = BsonValue::Document(data.clone());
                doc.get_path(field)
                    .map(|v| v != value)
                    .unwrap_or(true)
            }

            FilterExpr::Gt { field, value } => {
                let doc = BsonValue::Document(data.clone());
                doc.get_path(field)
                    .and_then(|v| v.partial_cmp(value))
                    .map(|ord| ord == std::cmp::Ordering::Greater)
                    .unwrap_or(false)
            }

            FilterExpr::Gte { field, value } => {
                let doc = BsonValue::Document(data.clone());
                doc.get_path(field)
                    .and_then(|v| v.partial_cmp(value))
                    .map(|ord| matches!(ord, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                    .unwrap_or(false)
            }

            FilterExpr::Lt { field, value } => {
                let doc = BsonValue::Document(data.clone());
                doc.get_path(field)
                    .and_then(|v| v.partial_cmp(value))
                    .map(|ord| ord == std::cmp::Ordering::Less)
                    .unwrap_or(false)
            }

            FilterExpr::Lte { field, value } => {
                let doc = BsonValue::Document(data.clone());
                doc.get_path(field)
                    .and_then(|v| v.partial_cmp(value))
                    .map(|ord| matches!(ord, std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
                    .unwrap_or(false)
            }

            FilterExpr::In { field, values } => {
                let doc = BsonValue::Document(data.clone());
                doc.get_path(field)
                    .map(|v| values.contains(v))
                    .unwrap_or(false)
            }

            FilterExpr::Nin { field, values } => {
                let doc = BsonValue::Document(data.clone());
                doc.get_path(field)
                    .map(|v| !values.contains(v))
                    .unwrap_or(true)
            }

            FilterExpr::Exists { field, exists } => {
                let doc = BsonValue::Document(data.clone());
                let has_field = doc.get_path(field).is_some();
                has_field == *exists
            }

            FilterExpr::Regex { field, pattern } => {
                let doc = BsonValue::Document(data.clone());
                if let Some(BsonValue::String(s)) = doc.get_path(field) {
                    regex::Regex::new(pattern)
                        .map(|re| re.is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            FilterExpr::Type { field, bson_type } => {
                let doc = BsonValue::Document(data.clone());
                doc.get_path(field)
                    .map(|v| v.type_name() == bson_type.as_str())
                    .unwrap_or(false)
            }

            FilterExpr::ElemMatch { field, filter } => {
                let doc = BsonValue::Document(data.clone());
                if let Some(BsonValue::Array(arr)) = doc.get_path(field) {
                    arr.iter().any(|elem| {
                        if let BsonValue::Document(elem_data) = elem {
                            filter.matches(elem_data, "")
                        } else {
                            false
                        }
                    })
                } else {
                    false
                }
            }

            FilterExpr::And(exprs) => exprs.iter().all(|e| e.matches(data, doc_id)),
            FilterExpr::Or(exprs) => exprs.iter().any(|e| e.matches(data, doc_id)),
            FilterExpr::Not(expr) => !expr.matches(data, doc_id),
            FilterExpr::Nor(exprs) => !exprs.iter().any(|e| e.matches(data, doc_id)),
        }
    }
}
