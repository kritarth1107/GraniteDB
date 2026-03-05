// ============================================================================
// GraniteDB — Schema Validation
// ============================================================================
// Optional schema validation for collections. Supports JSON-Schema-like
// rules: required fields, type checking, min/max values, regex patterns, etc.
// ============================================================================

use crate::document::bson::BsonValue;
use crate::error::{GraniteError, GraniteResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Supported field types for schema validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FieldType {
    Null,
    Boolean,
    Int32,
    Int64,
    Double,
    String,
    Document,
    Array,
    Binary,
    ObjectId,
    DateTime,
    Any,
}

/// Validation rule for a single field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldRule {
    /// Expected type
    pub field_type: FieldType,
    /// Is this field required?
    pub required: bool,
    /// Minimum value (for numeric types)
    pub min: Option<f64>,
    /// Maximum value (for numeric types)
    pub max: Option<f64>,
    /// Minimum string/array length
    pub min_length: Option<usize>,
    /// Maximum string/array length
    pub max_length: Option<usize>,
    /// Regex pattern (for strings)
    pub pattern: Option<String>,
    /// Allowed values (enum)
    pub allowed_values: Option<Vec<BsonValue>>,
    /// Nested schema (for document fields)
    pub nested_schema: Option<Box<Schema>>,
    /// Default value if the field is missing
    pub default: Option<BsonValue>,
    /// Custom description for error messages
    pub description: Option<String>,
}

/// A collection-level schema defining field rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Field rules keyed by field name
    pub fields: HashMap<String, FieldRule>,
    /// Allow extra fields not defined in the schema?
    pub allow_additional_fields: bool,
}

/// Validates documents against a schema.
pub struct SchemaValidator;

impl SchemaValidator {
    /// Validate a document's data against a schema.
    pub fn validate(
        data: &std::collections::BTreeMap<String, BsonValue>,
        schema: &Schema,
    ) -> GraniteResult<()> {
        let mut errors: Vec<String> = Vec::new();

        // Check required fields exist
        for (field_name, rule) in &schema.fields {
            if rule.required && !data.contains_key(field_name) {
                if rule.default.is_none() {
                    errors.push(format!("Missing required field: '{}'", field_name));
                }
            }
        }

        // Check each field in the data
        for (field_name, value) in data {
            if let Some(rule) = schema.fields.get(field_name) {
                Self::validate_field(field_name, value, rule, &mut errors);
            } else if !schema.allow_additional_fields {
                errors.push(format!(
                    "Unknown field '{}' — additional fields not allowed",
                    field_name
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(GraniteError::ValidationError(errors.join("; ")))
        }
    }

    fn validate_field(
        field_name: &str,
        value: &BsonValue,
        rule: &FieldRule,
        errors: &mut Vec<String>,
    ) {
        // Type check
        if rule.field_type != FieldType::Any && !Self::type_matches(value, &rule.field_type) {
            errors.push(format!(
                "Field '{}': expected type {:?}, got '{}'",
                field_name,
                rule.field_type,
                value.type_name()
            ));
            return;
        }

        // Numeric range checks
        if let Some(min) = rule.min {
            if let Some(num) = value.as_f64() {
                if num < min {
                    errors.push(format!(
                        "Field '{}': value {} is below minimum {}",
                        field_name, num, min
                    ));
                }
            }
        }
        if let Some(max) = rule.max {
            if let Some(num) = value.as_f64() {
                if num > max {
                    errors.push(format!(
                        "Field '{}': value {} exceeds maximum {}",
                        field_name, num, max
                    ));
                }
            }
        }

        // Length checks
        match value {
            BsonValue::String(s) => {
                if let Some(min_len) = rule.min_length {
                    if s.len() < min_len {
                        errors.push(format!(
                            "Field '{}': string length {} is below minimum {}",
                            field_name,
                            s.len(),
                            min_len
                        ));
                    }
                }
                if let Some(max_len) = rule.max_length {
                    if s.len() > max_len {
                        errors.push(format!(
                            "Field '{}': string length {} exceeds maximum {}",
                            field_name,
                            s.len(),
                            max_len
                        ));
                    }
                }
                // Regex pattern
                if let Some(pattern) = &rule.pattern {
                    if let Ok(re) = regex::Regex::new(pattern) {
                        if !re.is_match(s) {
                            errors.push(format!(
                                "Field '{}': value does not match pattern '{}'",
                                field_name, pattern
                            ));
                        }
                    }
                }
            }
            BsonValue::Array(arr) => {
                if let Some(min_len) = rule.min_length {
                    if arr.len() < min_len {
                        errors.push(format!(
                            "Field '{}': array length {} is below minimum {}",
                            field_name,
                            arr.len(),
                            min_len
                        ));
                    }
                }
                if let Some(max_len) = rule.max_length {
                    if arr.len() > max_len {
                        errors.push(format!(
                            "Field '{}': array length {} exceeds maximum {}",
                            field_name,
                            arr.len(),
                            max_len
                        ));
                    }
                }
            }
            _ => {}
        }

        // Enum check
        if let Some(allowed) = &rule.allowed_values {
            if !allowed.contains(value) {
                errors.push(format!(
                    "Field '{}': value not in allowed set",
                    field_name
                ));
            }
        }

        // Nested schema
        if let Some(nested_schema) = &rule.nested_schema {
            if let BsonValue::Document(nested_data) = value {
                let nested_btree: std::collections::BTreeMap<String, BsonValue> =
                    nested_data.clone();
                if let Err(e) = Self::validate(&nested_btree, nested_schema) {
                    errors.push(format!("Field '{}' nested error: {}", field_name, e));
                }
            }
        }
    }

    fn type_matches(value: &BsonValue, expected: &FieldType) -> bool {
        matches!(
            (value, expected),
            (BsonValue::Null, FieldType::Null)
                | (BsonValue::Boolean(_), FieldType::Boolean)
                | (BsonValue::Int32(_), FieldType::Int32)
                | (BsonValue::Int64(_), FieldType::Int64)
                | (BsonValue::Double(_), FieldType::Double)
                | (BsonValue::String(_), FieldType::String)
                | (BsonValue::Document(_), FieldType::Document)
                | (BsonValue::Array(_), FieldType::Array)
                | (BsonValue::Binary(_), FieldType::Binary)
                | (BsonValue::ObjectId(_), FieldType::ObjectId)
                | (BsonValue::DateTime(_), FieldType::DateTime)
                | (_, FieldType::Any)
        )
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            fields: HashMap::new(),
            allow_additional_fields: true,
        }
    }
}

impl Default for FieldRule {
    fn default() -> Self {
        Self {
            field_type: FieldType::Any,
            required: false,
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            pattern: None,
            allowed_values: None,
            nested_schema: None,
            default: None,
            description: None,
        }
    }
}
