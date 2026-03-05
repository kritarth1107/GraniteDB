// ============================================================================
// GraniteDB — HNSW (Hierarchical Navigable Small World) Index
// ============================================================================
// A production-grade approximate nearest neighbor (ANN) index.
// HNSW provides O(log n) search time with high recall, making it
// the gold standard for vector search at scale (millions of vectors).
//
// Key parameters:
//   M           — max connections per node per layer (default 16)
//   ef_construction — beam width during build (default 200)
//   ef_search   — beam width during search (default 100)
//   max_level   — computed dynamically based on data size
// ============================================================================

use crate::vector::distance::DistanceMetric;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// A node in the HNSW graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnswNode {
    /// Document ID this node belongs to
    doc_id: String,
    /// The vector data
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

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Reverse for min-heap behavior in BinaryHeap (which is max-heap)
        other.distance.partial_cmp(&self.distance)
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
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
impl PartialOrd for FarCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.distance.partial_cmp(&other.distance)
    }
}
impl Ord for FarCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
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

/// The HNSW index.
pub struct HnswIndex {
    config: HnswConfig,
    nodes: Vec<HnswNode>,
    entry_point: Option<usize>,
    max_level: usize,
    /// Mapping from doc_id to node index
    doc_to_node: HashMap<String, usize>,
    /// Level multiplier for random level generation (1/ln(M))
    level_mult: f64,
}

impl HnswIndex {
    pub fn new(config: HnswConfig) -> Self {
        let level_mult = 1.0 / (config.m as f64).ln();
        Self {
            config,
            nodes: Vec::new(),
            entry_point: None,
            max_level: 0,
            doc_to_node: HashMap::new(),
            level_mult,
        }
    }

    /// Create with default config and specified dimensions + metric.
    pub fn with_dimensions(dimensions: usize, metric: DistanceMetric) -> Self {
        let mut config = HnswConfig::default();
        config.dimensions = dimensions;
        config.metric = metric;
        Self::new(config)
    }

    /// Generate a random level for a new node.
    fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let r: f64 = rng.gen();
        (-r.ln() * self.level_mult).floor() as usize
    }

    /// Insert a vector into the index.
    pub fn insert(&mut self, doc_id: &str, vector: Vec<f32>) {
        // Auto-detect dimensions from first insert
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

        let level = self.random_level();
        let node_idx = self.nodes.len();

        // Create connections for each layer
        let mut connections = Vec::with_capacity(level + 1);
        for _ in 0..=level {
            connections.push(Vec::new());
        }

        let node = HnswNode {
            doc_id: doc_id.to_string(),
            vector,
            connections,
            max_layer: level,
        };

        self.nodes.push(node);
        self.doc_to_node.insert(doc_id.to_string(), node_idx);

        if self.nodes.len() == 1 {
            // First node — set as entry point
            self.entry_point = Some(0);
            self.max_level = level;
            return;
        }

        let ep = self.entry_point.unwrap();
        let mut current_ep = ep;

        // Phase 1: Traverse from top layer down to level+1 (greedy search)
        let node_vec = self.nodes[node_idx].vector.clone();
        for lev in (level + 1..=self.max_level).rev() {
            current_ep = self.greedy_closest(current_ep, &node_vec, lev);
        }

        // Phase 2: For layers level down to 0, find neighbors and connect
        let ef = self.config.ef_construction;
        for lev in (0..=level.min(self.max_level)).rev() {
            let neighbors = self.search_layer(&node_vec, current_ep, ef, lev);

            // Select M best neighbors
            let m = if lev == 0 { self.config.m0 } else { self.config.m };
            let selected: Vec<usize> = neighbors
                .iter()
                .take(m)
                .map(|c| c.index)
                .collect();

            // Connect node to selected neighbors
            self.nodes[node_idx].connections[lev] = selected.clone();

            // Connect neighbors back to this node (bidirectional)
            for &neighbor_idx in &selected {
                if lev <= self.nodes[neighbor_idx].max_layer {
                    let max_conn = if lev == 0 { self.config.m0 } else { self.config.m };
                    self.nodes[neighbor_idx].connections[lev].push(node_idx);

                    // Prune if too many connections
                    if self.nodes[neighbor_idx].connections[lev].len() > max_conn {
                        let nv = self.nodes[neighbor_idx].vector.clone();
                        let mut scored: Vec<(f32, usize)> = self.nodes[neighbor_idx]
                            .connections[lev]
                            .iter()
                            .map(|&idx| {
                                let dist =
                                    self.config.metric.compute(&nv, &self.nodes[idx].vector);
                                (dist, idx)
                            })
                            .collect();
                        scored.sort_by(|a, b| {
                            a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal)
                        });
                        self.nodes[neighbor_idx].connections[lev] =
                            scored.into_iter().take(max_conn).map(|(_, idx)| idx).collect();
                    }
                }
            }

            if !neighbors.is_empty() {
                current_ep = neighbors[0].index;
            }
        }

        // Update entry point if new node has higher level
        if level > self.max_level {
            self.max_level = level;
            self.entry_point = Some(node_idx);
        }
    }

    /// Greedy search for the single closest node at a given layer.
    fn greedy_closest(&self, start: usize, query: &[f32], level: usize) -> usize {
        let mut current = start;
        let mut current_dist = self.config.metric.compute(query, &self.nodes[current].vector);

        loop {
            let mut changed = false;
            if level <= self.nodes[current].max_layer {
                for &neighbor in &self.nodes[current].connections[level] {
                    let dist =
                        self.config.metric.compute(query, &self.nodes[neighbor].vector);
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

    /// Search within a single layer using beam search.
    fn search_layer(
        &self,
        query: &[f32],
        entry: usize,
        ef: usize,
        level: usize,
    ) -> Vec<Candidate> {
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
            // If current candidate is further than the worst result, stop
            if let Some(worst) = results.peek() {
                if current.distance > worst.distance && results.len() >= ef {
                    break;
                }
            }

            if level <= self.nodes[current.index].max_layer {
                for &neighbor in &self.nodes[current.index].connections[level] {
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
                                results.pop(); // Remove furthest
                            }
                        }
                    }
                }
            }
        }

        // Convert results to sorted candidates
        let mut final_results: Vec<Candidate> = results
            .into_iter()
            .map(|fc| Candidate {
                distance: fc.distance,
                index: fc.index,
            })
            .collect();
        final_results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal));
        final_results
    }

    /// Search for the K nearest neighbors to a query vector.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<VectorSearchResult> {
        if self.nodes.is_empty() {
            return Vec::new();
        }

        let ep = self.entry_point.unwrap();
        let mut current_ep = ep;

        // Traverse from top layer to layer 1
        for lev in (1..=self.max_level).rev() {
            current_ep = self.greedy_closest(current_ep, query, lev);
        }

        // Search at layer 0 with ef_search
        let ef = self.config.ef_search.max(k);
        let candidates = self.search_layer(query, current_ep, ef, 0);

        // Return top-k
        candidates
            .into_iter()
            .take(k)
            .map(|c| VectorSearchResult {
                doc_id: self.nodes[c.index].doc_id.clone(),
                distance: c.distance,
                score: 1.0 - c.distance, // Similarity score
            })
            .collect()
    }

    /// Search with a minimum similarity threshold.
    pub fn search_with_threshold(
        &self,
        query: &[f32],
        k: usize,
        min_score: f32,
    ) -> Vec<VectorSearchResult> {
        self.search(query, k)
            .into_iter()
            .filter(|r| r.score >= min_score)
            .collect()
    }

    /// Remove a document from the index.
    pub fn remove(&mut self, doc_id: &str) -> bool {
        if let Some(&node_idx) = self.doc_to_node.get(doc_id) {
            // Remove connections to this node from all neighbors
            for lev in 0..=self.nodes[node_idx].max_layer {
                let neighbors: Vec<usize> = self.nodes[node_idx].connections[lev].clone();
                for neighbor_idx in neighbors {
                    if neighbor_idx < self.nodes.len()
                        && lev <= self.nodes[neighbor_idx].max_layer
                    {
                        self.nodes[neighbor_idx].connections[lev]
                            .retain(|&idx| idx != node_idx);
                    }
                }
            }
            // Mark node as deleted (but don't remove to preserve indices)
            self.nodes[node_idx].connections.clear();
            self.nodes[node_idx].doc_id = String::new();
            self.doc_to_node.remove(doc_id);
            true
        } else {
            false
        }
    }

    /// Get the number of vectors in the index.
    pub fn len(&self) -> usize {
        self.doc_to_node.len()
    }

    /// Is the index empty?
    pub fn is_empty(&self) -> bool {
        self.doc_to_node.is_empty()
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
pub struct VectorSearchResult {
    /// Document ID
    pub doc_id: String,
    /// Distance from query (lower = more similar)
    pub distance: f32,
    /// Similarity score (higher = more similar, typically 1 - distance)
    pub score: f32,
}
