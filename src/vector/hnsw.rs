// ============================================================================
// GraniteDB — HNSW (Hierarchical Navigable Small World) Index
// ============================================================================
// Approximate nearest neighbor (ANN) graph keyed by a caller-chosen id
// (`NodeId` in the memory tree, document id in the legacy sketch).
//
// Deleted vectors are tombstoned in place so neighbor indices stay valid.
// Search, greedy descent, and the entry point skip those slots. A later
// commit compact-rebuilds the graph when the tombstone ratio grows.
//
// Key parameters:
//   M           — max connections per node per layer (default 16)
//   ef_construction — beam width during build (default 200)
//   ef_search   — beam width during search (default 100)
// ============================================================================

use crate::vector::distance::DistanceMetric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// A node in the HNSW graph. `id == None` means the slot is a tombstone.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnswNode<K> {
    id: Option<K>,
    vector: Vec<f32>,
    /// Connections at each layer: layer -> list of neighbor indices
    connections: Vec<Vec<usize>>,
    /// The highest layer this node appears in
    max_layer: usize,
}

/// A candidate during search (min-heap by distance).
#[derive(Debug, Clone)]
struct Candidate {
    distance: f32,
    index: usize,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Far candidate for max-heap (furthest neighbor tracking).
#[derive(Debug, Clone)]
struct FarCandidate {
    distance: f32,
    index: usize,
}

impl PartialEq for FarCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}
impl Eq for FarCandidate {}
impl Ord for FarCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
    }
}
impl PartialOrd for FarCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// HNSW configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Max connections per node per layer
    pub m: usize,
    /// Max connections at layer 0 (typically 2*M)
    pub m0: usize,
    /// Beam width during construction
    pub ef_construction: usize,
    /// Beam width during search
    pub ef_search: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Vector dimensionality
    pub dimensions: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m0: 32,
            ef_construction: 200,
            ef_search: 100,
            metric: DistanceMetric::Cosine,
            dimensions: 0, // Set at first insert
        }
    }
}

/// The HNSW index, keyed by `K` (`NodeId` or a document id string).
pub struct HnswIndex<K = String> {
    config: HnswConfig,
    nodes: Vec<HnswNode<K>>,
    entry_point: Option<usize>,
    max_level: usize,
    /// Mapping from live id to node index
    id_to_node: HashMap<K, usize>,
    /// Level multiplier for random level generation (1/ln(M))
    level_mult: f64,
}

impl<K> HnswIndex<K>
where
    K: Clone + Eq + Hash + Ord,
{
    pub fn new(config: HnswConfig) -> Self {
        let level_mult = if config.m <= 1 {
            1.0
        } else {
            1.0 / (config.m as f64).ln()
        };
        Self {
            config,
            nodes: Vec::new(),
            entry_point: None,
            max_level: 0,
            id_to_node: HashMap::new(),
            level_mult,
        }
    }

    /// Create with default config and specified dimensions + metric.
    pub fn with_dimensions(dimensions: usize, metric: DistanceMetric) -> Self {
        let config = HnswConfig {
            dimensions,
            metric,
            ..HnswConfig::default()
        };
        Self::new(config)
    }

    /// Distance metric the graph was built with.
    pub fn metric(&self) -> DistanceMetric {
        self.config.metric
    }

    /// Configured search beam width.
    pub fn ef_search(&self) -> usize {
        self.config.ef_search
    }

    /// Dimensionality, or `0` before the first live insert.
    pub fn dimensions(&self) -> usize {
        self.config.dimensions
    }

    fn seed_for(id: &K) -> u64 {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        hasher.finish()
    }

    /// Level for a new node. Seeded from the id so a rebuild with the same
    /// insertion order reconstructs the same layers.
    fn random_level(&self, id: &K) -> usize {
        let mut rng = StdRng::seed_from_u64(Self::seed_for(id));
        let r: f64 = rng.r#gen::<f64>().clamp(1e-12, 1.0);
        (-r.ln() * self.level_mult).floor() as usize
    }

    fn is_live(&self, idx: usize) -> bool {
        self.nodes.get(idx).is_some_and(|n| n.id.is_some())
    }

    /// Live graph node used as the search / insert entry point.
    fn pick_entry(&self) -> Option<usize> {
        if let Some(ep) = self.entry_point {
            if self.is_live(ep) {
                return Some(ep);
            }
        }
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.id.is_some())
            .max_by_key(|(i, n)| (n.max_layer, core::cmp::Reverse(*i)))
            .map(|(i, _)| i)
    }

    /// Insert a vector into the index. An existing id is replaced.
    pub fn insert(&mut self, id: impl Into<K>, vector: Vec<f32>) {
        let id = id.into();
        if self.config.dimensions == 0 {
            self.config.dimensions = vector.len();
        }
        assert_eq!(
            vector.len(),
            self.config.dimensions,
            "Vector dimension mismatch: expected {}, got {}",
            self.config.dimensions,
            vector.len()
        );

        if self.id_to_node.contains_key(&id) {
            self.remove(&id);
        }

        let level = self.random_level(&id);
        let node_idx = self.nodes.len();

        let mut connections = Vec::with_capacity(level + 1);
        for _ in 0..=level {
            connections.push(Vec::new());
        }

        let node = HnswNode {
            id: Some(id.clone()),
            vector,
            connections,
            max_layer: level,
        };

        self.nodes.push(node);
        self.id_to_node.insert(id, node_idx);

        if self.id_to_node.len() == 1 {
            self.entry_point = Some(node_idx);
            self.max_level = level;
            return;
        }

        let Some(ep) = self.pick_entry() else {
            self.entry_point = Some(node_idx);
            self.max_level = level;
            return;
        };
        let mut current_ep = ep;

        let node_vec = self.nodes[node_idx].vector.clone();
        for lev in (level + 1..=self.max_level).rev() {
            current_ep = self.greedy_closest(current_ep, &node_vec, lev);
        }

        let ef = self.config.ef_construction;
        for lev in (0..=level.min(self.max_level)).rev() {
            let neighbors = self.search_layer(&node_vec, current_ep, ef, lev);

            let m = if lev == 0 {
                self.config.m0
            } else {
                self.config.m
            };
            let selected: Vec<usize> = neighbors
                .iter()
                .filter(|c| c.index != node_idx && self.is_live(c.index))
                .take(m)
                .map(|c| c.index)
                .collect();

            self.nodes[node_idx].connections[lev] = selected.clone();

            for &neighbor_idx in &selected {
                if lev <= self.nodes[neighbor_idx].max_layer {
                    let max_conn = if lev == 0 {
                        self.config.m0
                    } else {
                        self.config.m
                    };
                    self.nodes[neighbor_idx].connections[lev].push(node_idx);

                    if self.nodes[neighbor_idx].connections[lev].len() > max_conn {
                        let nv = self.nodes[neighbor_idx].vector.clone();
                        let mut scored: Vec<(f32, usize)> = self.nodes[neighbor_idx].connections
                            [lev]
                            .iter()
                            .copied()
                            .filter(|&idx| self.is_live(idx))
                            .map(|idx| {
                                let dist = self.config.metric.compute(&nv, &self.nodes[idx].vector);
                                (dist, idx)
                            })
                            .collect();
                        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                        self.nodes[neighbor_idx].connections[lev] = scored
                            .into_iter()
                            .take(max_conn)
                            .map(|(_, idx)| idx)
                            .collect();
                    }
                }
            }

            if !neighbors.is_empty() {
                current_ep = neighbors[0].index;
            }
        }

        if level > self.max_level {
            self.max_level = level;
            self.entry_point = Some(node_idx);
        }
    }

    /// Greedy search for the single closest live node at a given layer.
    fn greedy_closest(&self, start: usize, query: &[f32], level: usize) -> usize {
        let mut current = if self.is_live(start) {
            start
        } else if let Some(ep) = self.pick_entry() {
            ep
        } else {
            return start;
        };
        let mut current_dist = self
            .config
            .metric
            .compute(query, &self.nodes[current].vector);

        loop {
            let mut changed = false;
            if level <= self.nodes[current].max_layer {
                for &neighbor in &self.nodes[current].connections[level] {
                    if !self.is_live(neighbor) {
                        continue;
                    }
                    let dist = self
                        .config
                        .metric
                        .compute(query, &self.nodes[neighbor].vector);
                    if dist < current_dist {
                        current = neighbor;
                        current_dist = dist;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        current
    }

    /// Search within a single layer using beam search. Tombstones are skipped.
    fn search_layer(&self, query: &[f32], entry: usize, ef: usize, level: usize) -> Vec<Candidate> {
        let entry = if self.is_live(entry) {
            entry
        } else if let Some(ep) = self.pick_entry() {
            ep
        } else {
            return Vec::new();
        };
        let entry_dist = self.config.metric.compute(query, &self.nodes[entry].vector);

        let mut candidates: BinaryHeap<Candidate> = BinaryHeap::new();
        let mut results: BinaryHeap<FarCandidate> = BinaryHeap::new();
        let mut visited: HashSet<usize> = HashSet::new();

        candidates.push(Candidate {
            distance: entry_dist,
            index: entry,
        });
        results.push(FarCandidate {
            distance: entry_dist,
            index: entry,
        });
        visited.insert(entry);

        while let Some(current) = candidates.pop() {
            if let Some(worst) = results.peek() {
                if current.distance > worst.distance && results.len() >= ef {
                    break;
                }
            }

            if level <= self.nodes[current.index].max_layer {
                for &neighbor in &self.nodes[current.index].connections[level] {
                    if !self.is_live(neighbor) {
                        continue;
                    }
                    if visited.insert(neighbor) {
                        let dist = self
                            .config
                            .metric
                            .compute(query, &self.nodes[neighbor].vector);

                        let should_add = results.len() < ef
                            || dist < results.peek().map(|r| r.distance).unwrap_or(f32::MAX);

                        if should_add {
                            candidates.push(Candidate {
                                distance: dist,
                                index: neighbor,
                            });
                            results.push(FarCandidate {
                                distance: dist,
                                index: neighbor,
                            });
                            if results.len() > ef {
                                results.pop();
                            }
                        }
                    }
                }
            }
        }

        let mut final_results: Vec<Candidate> = results
            .into_iter()
            .filter(|fc| self.is_live(fc.index))
            .map(|fc| Candidate {
                distance: fc.distance,
                index: fc.index,
            })
            .collect();
        final_results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| self.nodes[a.index].id.cmp(&self.nodes[b.index].id))
        });
        final_results
    }

    fn hits_from_candidates(
        &self,
        candidates: Vec<Candidate>,
        k: usize,
    ) -> Vec<VectorSearchResult<K>> {
        candidates
            .into_iter()
            .filter(|c| c.distance.is_finite() && self.is_live(c.index))
            .filter_map(|c| {
                let id = self.nodes[c.index].id.clone()?;
                Some(VectorSearchResult {
                    id,
                    distance: c.distance,
                    score: 1.0 - c.distance,
                })
            })
            .take(k)
            .collect()
    }

    /// Search for the K nearest live neighbors to a query vector.
    ///
    /// Ties break by the stored id (ascending) so results are deterministic.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<VectorSearchResult<K>> {
        self.search_with_ef(query, k, self.config.ef_search)
    }

    /// Search with an explicit beam width (`ef`).
    pub fn search_with_ef(&self, query: &[f32], k: usize, ef: usize) -> Vec<VectorSearchResult<K>> {
        if k == 0 || self.id_to_node.is_empty() {
            return Vec::new();
        }
        if self.config.dimensions != 0 && query.len() != self.config.dimensions {
            return Vec::new();
        }

        let Some(ep) = self.pick_entry() else {
            return Vec::new();
        };
        let mut current_ep = ep;

        for lev in (1..=self.max_level).rev() {
            current_ep = self.greedy_closest(current_ep, query, lev);
        }

        let ef = ef.max(k);
        let candidates = self.search_layer(query, current_ep, ef, 0);
        self.hits_from_candidates(candidates, k)
    }

    /// Search with a minimum similarity threshold.
    pub fn search_with_threshold(
        &self,
        query: &[f32],
        k: usize,
        min_score: f32,
    ) -> Vec<VectorSearchResult<K>> {
        self.search(query, k)
            .into_iter()
            .filter(|r| r.score >= min_score)
            .collect()
    }

    /// Remove a vector. The graph slot is tombstoned so neighbor indices stay
    /// valid; search skips it.
    pub fn remove<Q>(&mut self, id: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let Some(&node_idx) = self.id_to_node.get(id) else {
            return false;
        };
        for lev in 0..=self.nodes[node_idx].max_layer {
            let neighbors: Vec<usize> = self.nodes[node_idx].connections[lev].clone();
            for neighbor_idx in neighbors {
                if neighbor_idx < self.nodes.len() && lev <= self.nodes[neighbor_idx].max_layer {
                    self.nodes[neighbor_idx].connections[lev].retain(|&idx| idx != node_idx);
                }
            }
        }
        self.nodes[node_idx].connections.clear();
        self.nodes[node_idx].id = None;
        self.id_to_node.remove(id);

        if self.entry_point == Some(node_idx) {
            self.entry_point = self.pick_entry();
            self.max_level = self
                .entry_point
                .map(|i| self.nodes[i].max_layer)
                .unwrap_or(0);
        }
        true
    }

    /// Whether `id` is a live (not tombstoned) member of the graph.
    pub fn contains<Q>(&self, id: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.id_to_node.contains_key(id)
    }

    /// Number of live vectors in the index.
    pub fn len(&self) -> usize {
        self.id_to_node.len()
    }

    /// Is the index empty of live vectors?
    pub fn is_empty(&self) -> bool {
        self.id_to_node.is_empty()
    }

    /// Get index statistics.
    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!({
            "total_vectors": self.len(),
            "dimensions": self.config.dimensions,
            "max_level": self.max_level,
            "metric": format!("{:?}", self.config.metric),
            "m": self.config.m,
            "ef_construction": self.config.ef_construction,
            "ef_search": self.config.ef_search,
        })
    }

    /// Set the ef_search parameter for runtime tuning.
    pub fn set_ef_search(&mut self, ef: usize) {
        self.config.ef_search = ef;
    }
}

/// A vector search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult<K = String> {
    /// Indexed id (`NodeId` in the memory tree).
    pub id: K,
    /// Distance from query (lower = more similar)
    pub distance: f32,
    /// Similarity score (higher = more similar, typically 1 - distance)
    pub score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f32, b: f32) -> bool {
        (a - b).abs() <= 1e-4
    }

    #[test]
    fn search_returns_nearest_and_skips_tombstones() {
        let mut idx = HnswIndex::<String>::with_dimensions(2, DistanceMetric::Euclidean);
        idx.insert("a", vec![0.0, 0.0]);
        idx.insert("b", vec![1.0, 0.0]);
        idx.insert("c", vec![10.0, 0.0]);
        let hits = idx.search(&[0.1, 0.0], 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, "a");
        assert_eq!(hits[1].id, "b");
        assert!(idx.remove("a"));
        assert!(!idx.contains("a"));
        let hits = idx.search(&[0.1, 0.0], 2);
        assert_eq!(hits[0].id, "b");
        assert!(hits.iter().all(|h| h.id != "a"));
    }

    #[test]
    fn equal_distances_break_ties_by_id() {
        let mut idx = HnswIndex::<String>::with_dimensions(1, DistanceMetric::Euclidean);
        idx.insert("b", vec![1.0]);
        idx.insert("a", vec![1.0]);
        let hits = idx.search(&[1.0], 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, "a");
        assert_eq!(hits[1].id, "b");
        assert!(close(hits[0].distance, hits[1].distance));
    }

    #[test]
    fn deleting_the_entry_point_keeps_search_working() {
        let mut idx = HnswIndex::<u64>::with_dimensions(2, DistanceMetric::Cosine);
        idx.insert(1u64, vec![1.0, 0.0]);
        idx.insert(2u64, vec![0.0, 1.0]);
        idx.insert(3u64, vec![0.7, 0.7]);
        assert!(idx.remove(&1u64));
        let hits = idx.search(&[0.0, 1.0], 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, 2);
    }
}
