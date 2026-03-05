// ============================================================================
// GraniteDB — Aggregation Pipeline
// ============================================================================
// Executes a sequence of stages against a document stream, producing
// aggregated results. Supports all major MongoDB-style stages.
// ============================================================================

use crate::aggregation::stages::*;
use crate::document::bson::BsonValue;
use crate::document::document::Document;
use crate::error::{GraniteError, GraniteResult};
use std::collections::BTreeMap;

/// An aggregation pipeline consisting of ordered stages.
pub struct AggregationPipeline {
    pub stages: Vec<Stage>,
}

impl AggregationPipeline {
    pub fn new(stages: Vec<Stage>) -> Self {
        Self { stages }
    }

    /// Execute the pipeline against a set of input documents.
    pub fn execute(&self, input: Vec<Document>) -> GraniteResult<Vec<Document>> {
        let mut docs = input;

        for stage in &self.stages {
            docs = self.execute_stage(stage, docs)?;
        }

        Ok(docs)
    }

    fn execute_stage(&self, stage: &Stage, docs: Vec<Document>) -> GraniteResult<Vec<Document>> {
        match stage {
            Stage::Match(filter) => self.stage_match(docs, filter),
            Stage::Project(spec) => self.stage_project(docs, spec),
            Stage::Group { key, accumulators } => self.stage_group(docs, key, accumulators),
            Stage::Sort(sort_spec) => self.stage_sort(docs, sort_spec),
            Stage::Limit(n) => Ok(docs.into_iter().take(*n).collect()),
            Stage::Skip(n) => Ok(docs.into_iter().skip(*n).collect()),
            Stage::Unwind { path, preserve_null } => {
                self.stage_unwind(docs, path, *preserve_null)
            }
            Stage::Count(field_name) => self.stage_count(docs, field_name),
            Stage::AddFields(fields) => self.stage_add_fields(docs, fields),
            _ => Ok(docs), // $lookup, $out, $replaceRoot handled elsewhere
        }
    }

    fn stage_match(
        &self,
        docs: Vec<Document>,
        filter: &BTreeMap<String, BsonValue>,
    ) -> GraniteResult<Vec<Document>> {
        let mut results = Vec::new();
        for doc in docs {
            let mut matches = true;
            for (key, expected) in filter {
                match doc.data.get(key) {
                    Some(actual) if actual == expected => {}
                    _ => {
                        matches = false;
                        break;
                    }
                }
            }
            if matches {
                results.push(doc);
            }
        }
        Ok(results)
    }

    fn stage_project(
        &self,
        docs: Vec<Document>,
        spec: &BTreeMap<String, ProjectSpec>,
    ) -> GraniteResult<Vec<Document>> {
        let mut results = Vec::new();
        for doc in docs {
            let mut new_data = BTreeMap::new();
            for (field, proj) in spec {
                match proj {
                    ProjectSpec::Include => {
                        if let Some(val) = doc.data.get(field) {
                            new_data.insert(field.clone(), val.clone());
                        }
                    }
                    ProjectSpec::Exclude => {
                        // Copy everything except excluded
                        for (k, v) in &doc.data {
                            if k != field {
                                new_data.insert(k.clone(), v.clone());
                            }
                        }
                    }
                    ProjectSpec::Expression(expr) => {
                        // Resolve field reference (e.g. "$name")
                        let source = expr.trim_start_matches('$');
                        if let Some(val) = doc.data.get(source) {
                            new_data.insert(field.clone(), val.clone());
                        }
                    }
                }
            }
            results.push(Document::with_id(doc.id, new_data, doc.metadata));
        }
        Ok(results)
    }

    fn stage_group(
        &self,
        docs: Vec<Document>,
        key: &GroupKey,
        accumulators: &BTreeMap<String, Accumulator>,
    ) -> GraniteResult<Vec<Document>> {
        // Group documents by key
        let mut groups: BTreeMap<String, Vec<&Document>> = BTreeMap::new();

        for doc in &docs {
            let group_key = match key {
                GroupKey::Null => "__null__".to_string(),
                GroupKey::Field(field) => {
                    let f = field.trim_start_matches('$');
                    doc.data
                        .get(f)
                        .map(|v| format!("{}", v))
                        .unwrap_or_else(|| "__null__".to_string())
                }
                GroupKey::Compound(fields) => {
                    let mut parts = Vec::new();
                    for (_, src) in fields {
                        let f = src.trim_start_matches('$');
                        let val = doc
                            .data
                            .get(f)
                            .map(|v| format!("{}", v))
                            .unwrap_or_else(|| "null".to_string());
                        parts.push(val);
                    }
                    parts.join("|")
                }
            };
            groups.entry(group_key).or_default().push(doc);
        }

        // Apply accumulators to each group
        let mut results = Vec::new();
        for (group_key, group_docs) in &groups {
            let mut result_data = BTreeMap::new();
            result_data.insert(
                "_id".to_string(),
                if group_key == "__null__" {
                    BsonValue::Null
                } else {
                    BsonValue::String(group_key.clone())
                },
            );

            for (output_field, acc) in accumulators {
                let value = self.apply_accumulator(acc, group_docs);
                result_data.insert(output_field.clone(), value);
            }

            results.push(Document::new(result_data));
        }

        Ok(results)
    }

    fn apply_accumulator(&self, acc: &Accumulator, docs: &[&Document]) -> BsonValue {
        match acc {
            Accumulator::Count => BsonValue::Int64(docs.len() as i64),

            Accumulator::Sum(expr) => {
                let sum: f64 = docs
                    .iter()
                    .filter_map(|d| self.resolve_expr(expr, d))
                    .filter_map(|v| v.as_f64())
                    .sum();
                BsonValue::Double(sum)
            }

            Accumulator::Avg(expr) => {
                let values: Vec<f64> = docs
                    .iter()
                    .filter_map(|d| self.resolve_expr(expr, d))
                    .filter_map(|v| v.as_f64())
                    .collect();
                if values.is_empty() {
                    BsonValue::Null
                } else {
                    let avg = values.iter().sum::<f64>() / values.len() as f64;
                    BsonValue::Double(avg)
                }
            }

            Accumulator::Min(expr) => {
                docs.iter()
                    .filter_map(|d| self.resolve_expr(expr, d))
                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap_or(BsonValue::Null)
            }

            Accumulator::Max(expr) => {
                docs.iter()
                    .filter_map(|d| self.resolve_expr(expr, d))
                    .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap_or(BsonValue::Null)
            }

            Accumulator::Push(expr) => {
                let values: Vec<BsonValue> = docs
                    .iter()
                    .filter_map(|d| self.resolve_expr(expr, d))
                    .collect();
                BsonValue::Array(values)
            }

            Accumulator::AddToSet(expr) => {
                let mut seen = Vec::new();
                for doc in docs {
                    if let Some(val) = self.resolve_expr(expr, doc) {
                        if !seen.contains(&val) {
                            seen.push(val);
                        }
                    }
                }
                BsonValue::Array(seen)
            }

            Accumulator::First(expr) => docs
                .first()
                .and_then(|d| self.resolve_expr(expr, d))
                .unwrap_or(BsonValue::Null),

            Accumulator::Last(expr) => docs
                .last()
                .and_then(|d| self.resolve_expr(expr, d))
                .unwrap_or(BsonValue::Null),
        }
    }

    fn resolve_expr(&self, expr: &AccumulatorExpr, doc: &Document) -> Option<BsonValue> {
        match expr {
            AccumulatorExpr::Field(field) => {
                let f = field.trim_start_matches('$');
                doc.data.get(f).cloned()
            }
            AccumulatorExpr::Literal(val) => Some(val.clone()),
        }
    }

    fn stage_sort(
        &self,
        mut docs: Vec<Document>,
        sort_spec: &[(String, SortDirection)],
    ) -> GraniteResult<Vec<Document>> {
        docs.sort_by(|a, b| {
            for (field, direction) in sort_spec {
                let val_a = a.data.get(field);
                let val_b = b.data.get(field);
                let ord = match (val_a, val_b) {
                    (Some(va), Some(vb)) => va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                let ord = if *direction == SortDirection::Descending {
                    ord.reverse()
                } else {
                    ord
                };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
        Ok(docs)
    }

    fn stage_unwind(
        &self,
        docs: Vec<Document>,
        path: &str,
        preserve_null: bool,
    ) -> GraniteResult<Vec<Document>> {
        let field = path.trim_start_matches('$');
        let mut results = Vec::new();

        for doc in docs {
            match doc.data.get(field) {
                Some(BsonValue::Array(arr)) => {
                    if arr.is_empty() && preserve_null {
                        results.push(doc);
                    } else {
                        for item in arr {
                            let mut new_data = doc.data.clone();
                            new_data.insert(field.to_string(), item.clone());
                            results.push(Document::with_id(
                                doc.id.clone(),
                                new_data,
                                doc.metadata.clone(),
                            ));
                        }
                    }
                }
                None if preserve_null => {
                    results.push(doc);
                }
                _ => {
                    if preserve_null {
                        results.push(doc);
                    }
                }
            }
        }
        Ok(results)
    }

    fn stage_count(
        &self,
        docs: Vec<Document>,
        field_name: &str,
    ) -> GraniteResult<Vec<Document>> {
        let count = docs.len() as i64;
        let mut data = BTreeMap::new();
        data.insert(field_name.to_string(), BsonValue::Int64(count));
        Ok(vec![Document::new(data)])
    }

    fn stage_add_fields(
        &self,
        docs: Vec<Document>,
        fields: &BTreeMap<String, BsonValue>,
    ) -> GraniteResult<Vec<Document>> {
        let mut results = Vec::new();
        for mut doc in docs {
            for (key, value) in fields {
                doc.data.insert(key.clone(), value.clone());
            }
            results.push(doc);
        }
        Ok(results)
    }
}

/// Parse a JSON aggregation pipeline.
pub fn parse_pipeline(stages_json: &[serde_json::Value]) -> GraniteResult<Vec<Stage>> {
    let mut stages = Vec::new();

    for stage_json in stages_json {
        if let serde_json::Value::Object(map) = stage_json {
            for (stage_name, stage_value) in map {
                let stage = match stage_name.as_str() {
                    "$match" => {
                        let filter: BTreeMap<String, BsonValue> =
                            if let serde_json::Value::Object(obj) = stage_value {
                                obj.iter()
                                    .map(|(k, v)| (k.clone(), BsonValue::from(v.clone())))
                                    .collect()
                            } else {
                                BTreeMap::new()
                            };
                        Stage::Match(filter)
                    }
                    "$sort" => {
                        let mut sort_spec = Vec::new();
                        if let serde_json::Value::Object(obj) = stage_value {
                            for (field, dir) in obj {
                                let direction = if dir.as_i64() == Some(-1) {
                                    SortDirection::Descending
                                } else {
                                    SortDirection::Ascending
                                };
                                sort_spec.push((field.clone(), direction));
                            }
                        }
                        Stage::Sort(sort_spec)
                    }
                    "$limit" => Stage::Limit(stage_value.as_u64().unwrap_or(100) as usize),
                    "$skip" => Stage::Skip(stage_value.as_u64().unwrap_or(0) as usize),
                    "$count" => Stage::Count(
                        stage_value
                            .as_str()
                            .unwrap_or("count")
                            .to_string(),
                    ),
                    "$unwind" => {
                        let (path, preserve) = match stage_value {
                            serde_json::Value::String(s) => (s.clone(), false),
                            serde_json::Value::Object(obj) => {
                                let path = obj
                                    .get("path")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let preserve = obj
                                    .get("preserveNullAndEmptyArrays")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);
                                (path, preserve)
                            }
                            _ => (String::new(), false),
                        };
                        Stage::Unwind {
                            path,
                            preserve_null: preserve,
                        }
                    }
                    "$group" => {
                        if let serde_json::Value::Object(obj) = stage_value {
                            let key = match obj.get("_id") {
                                Some(serde_json::Value::Null) => GroupKey::Null,
                                Some(serde_json::Value::String(s)) => GroupKey::Field(s.clone()),
                                Some(serde_json::Value::Object(compound)) => {
                                    let fields: BTreeMap<String, String> = compound
                                        .iter()
                                        .map(|(k, v)| {
                                            (k.clone(), v.as_str().unwrap_or("").to_string())
                                        })
                                        .collect();
                                    GroupKey::Compound(fields)
                                }
                                _ => GroupKey::Null,
                            };

                            let mut accumulators = BTreeMap::new();
                            for (field, acc_spec) in obj {
                                if field == "_id" {
                                    continue;
                                }
                                if let serde_json::Value::Object(acc_obj) = acc_spec {
                                    for (acc_name, acc_val) in acc_obj {
                                        let expr = match acc_val {
                                            serde_json::Value::String(s) => {
                                                AccumulatorExpr::Field(s.clone())
                                            }
                                            _ => AccumulatorExpr::Literal(BsonValue::from(
                                                acc_val.clone(),
                                            )),
                                        };
                                        let acc = match acc_name.as_str() {
                                            "$sum" => Accumulator::Sum(expr),
                                            "$avg" => Accumulator::Avg(expr),
                                            "$min" => Accumulator::Min(expr),
                                            "$max" => Accumulator::Max(expr),
                                            "$first" => Accumulator::First(expr),
                                            "$last" => Accumulator::Last(expr),
                                            "$push" => Accumulator::Push(expr),
                                            "$addToSet" => Accumulator::AddToSet(expr),
                                            "$count" => Accumulator::Count,
                                            _ => continue,
                                        };
                                        accumulators.insert(field.clone(), acc);
                                    }
                                }
                            }

                            Stage::Group { key, accumulators }
                        } else {
                            continue;
                        }
                    }
                    "$lookup" => {
                        if let serde_json::Value::Object(obj) = stage_value {
                            Stage::Lookup {
                                from: obj
                                    .get("from")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                local_field: obj
                                    .get("localField")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                foreign_field: obj
                                    .get("foreignField")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                alias: obj
                                    .get("as")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                            }
                        } else {
                            continue;
                        }
                    }
                    "$out" => Stage::Out(
                        stage_value
                            .as_str()
                            .unwrap_or("")
                            .to_string(),
                    ),
                    _ => continue,
                };
                stages.push(stage);
            }
        }
    }

    Ok(stages)
}
