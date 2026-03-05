// ============================================================================
// GraniteDB — Query Parser
// ============================================================================
// Parses JSON query objects into structured FilterExpr trees.
// Supports MongoDB-style query syntax.
// ============================================================================

use crate::document::bson::BsonValue;
use crate::error::{GraniteError, GraniteResult};
use crate::query::filter::FilterExpr;
use std::collections::BTreeMap;

/// Parses query documents into FilterExpr trees.
pub struct QueryParser;

impl QueryParser {
    /// Parse a JSON query object into a FilterExpr.
    pub fn parse(query: &serde_json::Value) -> GraniteResult<FilterExpr> {
        match query {
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    return Ok(FilterExpr::All);
                }
                Self::parse_object(map)
            }
            _ => Err(GraniteError::QueryParseError(
                "Query must be a JSON object".to_string(),
            )),
        }
    }

    fn parse_object(map: &serde_json::Map<String, serde_json::Value>) -> GraniteResult<FilterExpr> {
        let mut filters = Vec::new();

        for (key, value) in map {
            match key.as_str() {
                // Logical operators
                "$and" => {
                    if let serde_json::Value::Array(arr) = value {
                        let mut sub = Vec::new();
                        for item in arr {
                            sub.push(Self::parse(item)?);
                        }
                        filters.push(FilterExpr::And(sub));
                    } else {
                        return Err(GraniteError::QueryParseError(
                            "$and requires an array".to_string(),
                        ));
                    }
                }
                "$or" => {
                    if let serde_json::Value::Array(arr) = value {
                        let mut sub = Vec::new();
                        for item in arr {
                            sub.push(Self::parse(item)?);
                        }
                        filters.push(FilterExpr::Or(sub));
                    } else {
                        return Err(GraniteError::QueryParseError(
                            "$or requires an array".to_string(),
                        ));
                    }
                }
                "$not" => {
                    let sub = Self::parse(value)?;
                    filters.push(FilterExpr::Not(Box::new(sub)));
                }
                "$nor" => {
                    if let serde_json::Value::Array(arr) = value {
                        let mut sub = Vec::new();
                        for item in arr {
                            sub.push(Self::parse(item)?);
                        }
                        filters.push(FilterExpr::Nor(sub));
                    } else {
                        return Err(GraniteError::QueryParseError(
                            "$nor requires an array".to_string(),
                        ));
                    }
                }
                // Field-level filter
                field => {
                    let field_filter = Self::parse_field_filter(field, value)?;
                    filters.push(field_filter);
                }
            }
        }

        if filters.len() == 1 {
            Ok(filters.into_iter().next().unwrap())
        } else {
            Ok(FilterExpr::And(filters))
        }
    }

    fn parse_field_filter(field: &str, value: &serde_json::Value) -> GraniteResult<FilterExpr> {
        match value {
            // If the value is an object, check for operators
            serde_json::Value::Object(ops) => {
                let mut filters = Vec::new();
                for (op, op_value) in ops {
                    let bson_val = BsonValue::from(op_value.clone());
                    let filter = match op.as_str() {
                        "$eq" => FilterExpr::Eq {
                            field: field.to_string(),
                            value: bson_val,
                        },
                        "$ne" => FilterExpr::Ne {
                            field: field.to_string(),
                            value: bson_val,
                        },
                        "$gt" => FilterExpr::Gt {
                            field: field.to_string(),
                            value: bson_val,
                        },
                        "$gte" => FilterExpr::Gte {
                            field: field.to_string(),
                            value: bson_val,
                        },
                        "$lt" => FilterExpr::Lt {
                            field: field.to_string(),
                            value: bson_val,
                        },
                        "$lte" => FilterExpr::Lte {
                            field: field.to_string(),
                            value: bson_val,
                        },
                        "$in" => {
                            if let BsonValue::Array(arr) = bson_val {
                                FilterExpr::In {
                                    field: field.to_string(),
                                    values: arr,
                                }
                            } else {
                                return Err(GraniteError::QueryParseError(
                                    "$in requires an array".to_string(),
                                ));
                            }
                        }
                        "$nin" => {
                            if let BsonValue::Array(arr) = bson_val {
                                FilterExpr::Nin {
                                    field: field.to_string(),
                                    values: arr,
                                }
                            } else {
                                return Err(GraniteError::QueryParseError(
                                    "$nin requires an array".to_string(),
                                ));
                            }
                        }
                        "$exists" => {
                            if let BsonValue::Boolean(b) = bson_val {
                                FilterExpr::Exists {
                                    field: field.to_string(),
                                    exists: b,
                                }
                            } else {
                                return Err(GraniteError::QueryParseError(
                                    "$exists requires a boolean".to_string(),
                                ));
                            }
                        }
                        "$regex" => {
                            if let BsonValue::String(pattern) = bson_val {
                                FilterExpr::Regex {
                                    field: field.to_string(),
                                    pattern,
                                }
                            } else {
                                return Err(GraniteError::QueryParseError(
                                    "$regex requires a string".to_string(),
                                ));
                            }
                        }
                        "$type" => {
                            if let BsonValue::String(t) = bson_val {
                                FilterExpr::Type {
                                    field: field.to_string(),
                                    bson_type: t,
                                }
                            } else {
                                return Err(GraniteError::QueryParseError(
                                    "$type requires a string".to_string(),
                                ));
                            }
                        }
                        "$elemMatch" => {
                            let sub = Self::parse(op_value)?;
                            FilterExpr::ElemMatch {
                                field: field.to_string(),
                                filter: Box::new(sub),
                            }
                        }
                        unknown => {
                            // Not an operator, treat the whole object as an equality match
                            return Ok(FilterExpr::Eq {
                                field: field.to_string(),
                                value: BsonValue::from(serde_json::Value::Object(ops.clone())),
                            });
                        }
                    };
                    filters.push(filter);
                }

                if filters.len() == 1 {
                    Ok(filters.into_iter().next().unwrap())
                } else {
                    Ok(FilterExpr::And(filters))
                }
            }
            // Simple equality match
            _ => Ok(FilterExpr::Eq {
                field: field.to_string(),
                value: BsonValue::from(value.clone()),
            }),
        }
    }

    /// Parse an update document (supports $set, $unset, $inc, $push, $pull).
    pub fn parse_update(
        update: &serde_json::Value,
    ) -> GraniteResult<Vec<UpdateOperation>> {
        let mut ops = Vec::new();
        match update {
            serde_json::Value::Object(map) => {
                for (key, value) in map {
                    match key.as_str() {
                        "$set" => {
                            if let serde_json::Value::Object(fields) = value {
                                for (field, val) in fields {
                                    ops.push(UpdateOperation::Set {
                                        field: field.clone(),
                                        value: BsonValue::from(val.clone()),
                                    });
                                }
                            }
                        }
                        "$unset" => {
                            if let serde_json::Value::Object(fields) = value {
                                for (field, _) in fields {
                                    ops.push(UpdateOperation::Unset {
                                        field: field.clone(),
                                    });
                                }
                            }
                        }
                        "$inc" => {
                            if let serde_json::Value::Object(fields) = value {
                                for (field, val) in fields {
                                    ops.push(UpdateOperation::Inc {
                                        field: field.clone(),
                                        value: BsonValue::from(val.clone()),
                                    });
                                }
                            }
                        }
                        "$push" => {
                            if let serde_json::Value::Object(fields) = value {
                                for (field, val) in fields {
                                    ops.push(UpdateOperation::Push {
                                        field: field.clone(),
                                        value: BsonValue::from(val.clone()),
                                    });
                                }
                            }
                        }
                        "$pull" => {
                            if let serde_json::Value::Object(fields) = value {
                                for (field, val) in fields {
                                    ops.push(UpdateOperation::Pull {
                                        field: field.clone(),
                                        value: BsonValue::from(val.clone()),
                                    });
                                }
                            }
                        }
                        "$rename" => {
                            if let serde_json::Value::Object(fields) = value {
                                for (old_name, new_name) in fields {
                                    if let serde_json::Value::String(new) = new_name {
                                        ops.push(UpdateOperation::Rename {
                                            old_field: old_name.clone(),
                                            new_field: new.clone(),
                                        });
                                    }
                                }
                            }
                        }
                        _ => {
                            // Treat as a $set-style replacement
                            ops.push(UpdateOperation::Set {
                                field: key.clone(),
                                value: BsonValue::from(value.clone()),
                            });
                        }
                    }
                }
            }
            _ => {
                return Err(GraniteError::QueryParseError(
                    "Update document must be a JSON object".to_string(),
                ));
            }
        }
        Ok(ops)
    }
}

/// An update operation to apply to a document.
#[derive(Debug, Clone)]
pub enum UpdateOperation {
    Set { field: String, value: BsonValue },
    Unset { field: String },
    Inc { field: String, value: BsonValue },
    Push { field: String, value: BsonValue },
    Pull { field: String, value: BsonValue },
    Rename { old_field: String, new_field: String },
}
