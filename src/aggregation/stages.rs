// ============================================================================
// GraniteDB — Aggregation Stages
// ============================================================================
// MongoDB-style aggregation pipeline stages. Each stage transforms the
// document stream flowing through the pipeline.
// ============================================================================

use crate::document::bson::BsonValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// An aggregation pipeline stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Stage {
    /// Filter documents (like $match)
    Match(BTreeMap<String, BsonValue>),

    /// Project (include/exclude) fields (like $project)
    Project(BTreeMap<String, ProjectSpec>),

    /// Group documents by a key and apply accumulators (like $group)
    Group {
        /// The grouping key expression (field path starting with $)
        key: GroupKey,
        /// Accumulator operations: output_field -> accumulator
        accumulators: BTreeMap<String, Accumulator>,
    },

    /// Sort documents (like $sort)
    Sort(Vec<(String, SortDirection)>),

    /// Limit number of documents (like $limit)
    Limit(usize),

    /// Skip documents (like $skip)
    Skip(usize),

    /// Unwind an array field into multiple documents (like $unwind)
    Unwind {
        path: String,
        preserve_null: bool,
    },

    /// Count documents (like $count)
    Count(String),

    /// Add computed fields (like $addFields)
    AddFields(BTreeMap<String, BsonValue>),

    /// Replace root document (like $replaceRoot)
    ReplaceRoot(String),

    /// Lookup (join) with another collection (like $lookup)
    Lookup {
        from: String,
        local_field: String,
        foreign_field: String,
        alias: String,
    },

    /// Output to a collection (like $out)
    Out(String),
}

/// How to project a field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProjectSpec {
    /// Include the field (1)
    Include,
    /// Exclude the field (0)
    Exclude,
    /// Rename/compute from another field
    Expression(String),
}

/// Grouping key options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupKey {
    /// Group by a single field (e.g., "$status")
    Field(String),
    /// Group by null (all documents into one group)
    Null,
    /// Group by a compound key
    Compound(BTreeMap<String, String>),
}

/// Sort direction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Accumulator operations for $group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Accumulator {
    /// Sum a field
    Sum(AccumulatorExpr),
    /// Average a field
    Avg(AccumulatorExpr),
    /// Minimum value
    Min(AccumulatorExpr),
    /// Maximum value
    Max(AccumulatorExpr),
    /// Count
    Count,
    /// Push values into an array
    Push(AccumulatorExpr),
    /// Add unique values to a set
    AddToSet(AccumulatorExpr),
    /// First value in the group
    First(AccumulatorExpr),
    /// Last value in the group
    Last(AccumulatorExpr),
}

/// Expression for an accumulator — either a field reference or a literal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccumulatorExpr {
    /// Field reference (e.g., "$price")
    Field(String),
    /// Literal value
    Literal(BsonValue),
}
