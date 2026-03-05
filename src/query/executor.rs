// ============================================================================
// GraniteDB — Query Executor
// ============================================================================
// Executes a query plan against the storage engine, producing results
// with filtering, projection, sorting, skip, and limit support.
// ============================================================================

use crate::document::bson::BsonValue;
use crate::document::document::Document;
use crate::error::GraniteResult;
use crate::query::filter::FilterExpr;
use crate::query::planner::{ExecutionStrategy, QueryPlan};
use crate::storage::engine::StorageEngine;
use std::collections::BTreeMap;

/// Query result set.
#[derive(Debug)]
pub struct QueryResult {
    /// Matched documents
    pub documents: Vec<Document>,
    /// Number of documents scanned
    pub scanned: usize,
    /// Number of documents matched
    pub matched: usize,
    /// Execution time in microseconds
    pub execution_time_us: u128,
    /// Strategy used
    pub strategy: String,
}

/// Executes query plans.
pub struct QueryExecutor;

impl QueryExecutor {
    /// Execute a query plan and return results.
    pub fn execute(plan: &QueryPlan, storage: &StorageEngine) -> GraniteResult<QueryResult> {
        let start = std::time::Instant::now();
        let full_name = format!("{}.{}", plan.database, plan.collection);

        let (documents, scanned) = match &plan.strategy {
            ExecutionStrategy::IdLookup => {
                Self::execute_id_lookup(&full_name, &plan.filter, storage)?
            }
            ExecutionStrategy::CollectionScan
            | ExecutionStrategy::IndexScan { .. }
            | ExecutionStrategy::CoveredIndex { .. } => {
                Self::execute_collection_scan(&full_name, &plan.filter, storage)?
            }
        };

        let matched = documents.len();

        // Apply sort
        let mut sorted = documents;
        if !plan.sort.is_empty() {
            Self::sort_documents(&mut sorted, &plan.sort);
        }

        // Apply skip
        let skipped: Vec<Document> = if plan.skip > 0 {
            sorted.into_iter().skip(plan.skip).collect()
        } else {
            sorted
        };

        // Apply limit
        let limited: Vec<Document> = if plan.limit > 0 {
            skipped.into_iter().take(plan.limit).collect()
        } else {
            skipped
        };

        // Apply projection
        let projected = if !plan.projection.is_empty() {
            Self::apply_projection(limited, &plan.projection)
        } else {
            limited
        };

        let execution_time_us = start.elapsed().as_micros();

        Ok(QueryResult {
            documents: projected,
            scanned,
            matched,
            execution_time_us,
            strategy: format!("{:?}", plan.strategy),
        })
    }

    fn execute_id_lookup(
        full_name: &str,
        filter: &FilterExpr,
        storage: &StorageEngine,
    ) -> GraniteResult<(Vec<Document>, usize)> {
        if let FilterExpr::Eq {
            field,
            value: BsonValue::String(id),
        } = filter
        {
            if field == "_id" {
                if let Some(doc) = storage.get(full_name, id)? {
                    return Ok((vec![doc], 1));
                }
            }
        }
        Ok((Vec::new(), 0))
    }

    fn execute_collection_scan(
        full_name: &str,
        filter: &FilterExpr,
        storage: &StorageEngine,
    ) -> GraniteResult<(Vec<Document>, usize)> {
        let all_docs = storage.get_all(full_name)?;
        let scanned = all_docs.len();
        let mut matched = Vec::new();

        for doc in all_docs {
            if filter.matches(&doc.data, &doc.id) {
                matched.push(doc);
            }
        }

        Ok((matched, scanned))
    }

    fn sort_documents(docs: &mut [Document], sort_spec: &[(String, bool)]) {
        docs.sort_by(|a, b| {
            for (field, ascending) in sort_spec {
                let val_a = a.get_path(field);
                let val_b = b.get_path(field);

                let ordering = match (val_a, val_b) {
                    (Some(va), Some(vb)) => va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                let ordering = if *ascending {
                    ordering
                } else {
                    ordering.reverse()
                };

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn apply_projection(docs: Vec<Document>, fields: &[String]) -> Vec<Document> {
        docs.into_iter()
            .map(|doc| {
                let mut projected_data = BTreeMap::new();
                for field in fields {
                    if let Some(val) = doc.data.get(field) {
                        projected_data.insert(field.clone(), val.clone());
                    }
                }
                Document::with_id(doc.id, projected_data, doc.metadata)
            })
            .collect()
    }
}
