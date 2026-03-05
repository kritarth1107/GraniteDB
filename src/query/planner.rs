// ============================================================================
// GraniteDB — Query Planner
// ============================================================================
// The query planner analyzes a parsed query and determines the optimal
// execution strategy, including index selection and sort ordering.
// ============================================================================

use crate::query::filter::FilterExpr;
use serde::{Deserialize, Serialize};

/// Describes how a query should be executed.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// The collection to query
    pub collection: String,
    /// The database
    pub database: String,
    /// Filter expression
    pub filter: FilterExpr,
    /// Fields to project (empty = all fields)
    pub projection: Vec<String>,
    /// Sort specification: (field, ascending?)
    pub sort: Vec<(String, bool)>,
    /// Number of documents to skip
    pub skip: usize,
    /// Maximum number of documents to return (0 = unlimited)
    pub limit: usize,
    /// Which index to use (None = collection scan)
    pub index_hint: Option<String>,
    /// Execution strategy
    pub strategy: ExecutionStrategy,
}

/// Execution strategy choices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStrategy {
    /// Full collection scan — no index
    CollectionScan,
    /// Index scan using a specific index
    IndexScan { index_name: String },
    /// Covered query — all fields available from the index
    CoveredIndex { index_name: String },
    /// ID-based lookup — fastest path
    IdLookup,
}

/// The query planner.
pub struct QueryPlanner;

impl QueryPlanner {
    /// Create an execution plan from query parameters.
    pub fn plan(
        database: &str,
        collection: &str,
        filter: FilterExpr,
        projection: Vec<String>,
        sort: Vec<(String, bool)>,
        skip: usize,
        limit: usize,
        available_indexes: &[IndexInfo],
    ) -> QueryPlan {
        // Determine execution strategy
        let strategy = Self::choose_strategy(&filter, available_indexes);
        let index_hint = match &strategy {
            ExecutionStrategy::IndexScan { index_name } => Some(index_name.clone()),
            ExecutionStrategy::CoveredIndex { index_name } => Some(index_name.clone()),
            _ => None,
        };

        QueryPlan {
            collection: collection.to_string(),
            database: database.to_string(),
            filter,
            projection,
            sort,
            skip,
            limit,
            index_hint,
            strategy,
        }
    }

    fn choose_strategy(
        filter: &FilterExpr,
        available_indexes: &[IndexInfo],
    ) -> ExecutionStrategy {
        // Check for ID lookup (fastest)
        if Self::is_id_lookup(filter) {
            return ExecutionStrategy::IdLookup;
        }

        // Check for matching indexes
        let filter_fields = Self::extract_fields(filter);
        for index in available_indexes {
            // Check if the index covers the query fields
            let covers = filter_fields
                .iter()
                .all(|f| index.fields.contains(f));
            if covers {
                return ExecutionStrategy::IndexScan {
                    index_name: index.name.clone(),
                };
            }
        }

        // Fallback: collection scan
        ExecutionStrategy::CollectionScan
    }

    fn is_id_lookup(filter: &FilterExpr) -> bool {
        matches!(filter, FilterExpr::Eq { field, .. } if field == "_id")
    }

    fn extract_fields(filter: &FilterExpr) -> Vec<String> {
        let mut fields = Vec::new();
        match filter {
            FilterExpr::Eq { field, .. }
            | FilterExpr::Ne { field, .. }
            | FilterExpr::Gt { field, .. }
            | FilterExpr::Gte { field, .. }
            | FilterExpr::Lt { field, .. }
            | FilterExpr::Lte { field, .. }
            | FilterExpr::In { field, .. }
            | FilterExpr::Nin { field, .. }
            | FilterExpr::Exists { field, .. }
            | FilterExpr::Regex { field, .. }
            | FilterExpr::Type { field, .. }
            | FilterExpr::ElemMatch { field, .. } => {
                fields.push(field.clone());
            }
            FilterExpr::And(exprs)
            | FilterExpr::Or(exprs)
            | FilterExpr::Nor(exprs) => {
                for expr in exprs {
                    fields.extend(Self::extract_fields(expr));
                }
            }
            FilterExpr::Not(expr) => {
                fields.extend(Self::extract_fields(expr));
            }
            FilterExpr::All => {}
        }
        fields
    }

    /// Explain a query plan as JSON (for debugging/profiling).
    pub fn explain(plan: &QueryPlan) -> serde_json::Value {
        serde_json::json!({
            "database": plan.database,
            "collection": plan.collection,
            "strategy": format!("{:?}", plan.strategy),
            "index_hint": plan.index_hint,
            "projection_fields": plan.projection.len(),
            "sort_fields": plan.sort.len(),
            "skip": plan.skip,
            "limit": plan.limit,
        })
    }
}

/// Information about an available index (used by the planner).
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
}
