// ============================================================================
// GraniteDB — Product Quantization (PQ)
// ============================================================================
// Compresses high-dimensional vectors into compact codes for memory-
// efficient approximate nearest neighbor search. Enables searching
// billions of vectors that wouldn't fit in RAM as full f32 arrays.
//
// Approach: Split each vector into M sub-vectors, cluster each subspace
// with K centroids, and store only the centroid indices (1 byte each).
// A 768-dim vector (3072 bytes as f32) becomes 96 bytes with M=96.
// ============================================================================

use crate::vector::distance::DistanceMetric;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

/// Product Quantizer config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PQConfig {
    /// Number of sub-quantizers (must divide dimensions evenly)
    pub num_subquantizers: usize,
    /// Number of centroids per sub-quantizer (typically 256 for u8 codes)
    pub num_centroids: usize,
    /// Vector dimensionality
    pub dimensions: usize,
    /// Training iterations for k-means
    pub training_iterations: usize,
}

impl Default for PQConfig {
    fn default() -> Self {
        Self {
            num_subquantizers: 8,
            num_centroids: 256,
            dimensions: 0,
            training_iterations: 25,
        }
    }
}

/// A trained Product Quantizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantizer {
    config: PQConfig,
    /// Codebook: [subquantizer][centroid][sub_dimension]
    codebook: Vec<Vec<Vec<f32>>>,
    /// Sub-vector dimensionality
    sub_dim: usize,
    /// Whether the quantizer has been trained
    trained: bool,
}

impl ProductQuantizer {
    pub fn new(config: PQConfig) -> Self {
        let sub_dim = if config.dimensions > 0 {
            config.dimensions / config.num_subquantizers
        } else {
            0
        };
        Self {
            config,
            codebook: Vec::new(),
            sub_dim,
            trained: false,
        }
    }

    /// Train the quantizer on a set of vectors using k-means.
    pub fn train(&mut self, vectors: &[Vec<f32>]) {
        if vectors.is_empty() {
            return;
        }

        let dims = vectors[0].len();
        self.config.dimensions = dims;
        self.sub_dim = dims / self.config.num_subquantizers;
        assert_eq!(
            dims % self.config.num_subquantizers,
            0,
            "Dimensions must be divisible by num_subquantizers"
        );

        self.codebook =
            Vec::with_capacity(self.config.num_subquantizers);

        for sq in 0..self.config.num_subquantizers {
            let start = sq * self.sub_dim;
            let end = start + self.sub_dim;

            // Extract sub-vectors
            let sub_vectors: Vec<Vec<f32>> = vectors
                .iter()
                .map(|v| v[start..end].to_vec())
                .collect();

            // Run k-means on this subspace
            let centroids =
                self.kmeans(&sub_vectors, self.config.num_centroids);
            self.codebook.push(centroids);
        }

        self.trained = true;
    }

    /// Simple k-means clustering.
    fn kmeans(&self, data: &[Vec<f32>], k: usize) -> Vec<Vec<f32>> {
        let dim = data[0].len();
        let k = k.min(data.len());

        // Initialize centroids by random sampling
        let mut rng = rand::thread_rng();
        let mut indices: Vec<usize> = (0..data.len()).collect();
        indices.shuffle(&mut rng);
        let mut centroids: Vec<Vec<f32>> =
            indices[..k].iter().map(|&i| data[i].clone()).collect();

        for _iter in 0..self.config.training_iterations {
            // Assign each point to nearest centroid
            let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); k];

            for (i, point) in data.iter().enumerate() {
                let mut best_c = 0;
                let mut best_dist = f32::MAX;
                for (c, centroid) in centroids.iter().enumerate() {
                    let dist =
                        DistanceMetric::Euclidean.compute(point, centroid);
                    if dist < best_dist {
                        best_dist = dist;
                        best_c = c;
                    }
                }
                assignments[best_c].push(i);
            }

            // Update centroids
            for (c, assigned) in assignments.iter().enumerate() {
                if assigned.is_empty() {
                    continue;
                }
                let mut new_centroid = vec![0.0f32; dim];
                for &idx in assigned {
                    for (j, val) in data[idx].iter().enumerate() {
                        new_centroid[j] += val;
                    }
                }
                let n = assigned.len() as f32;
                for val in &mut new_centroid {
                    *val /= n;
                }
                centroids[c] = new_centroid;
            }
        }

        centroids
    }

    /// Encode a vector into a compact PQ code (vector of centroid indices).
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        assert!(self.trained, "Quantizer must be trained first");
        let mut code = Vec::with_capacity(self.config.num_subquantizers);

        for sq in 0..self.config.num_subquantizers {
            let start = sq * self.sub_dim;
            let end = start + self.sub_dim;
            let sub_vec = &vector[start..end];

            let mut best_c = 0u8;
            let mut best_dist = f32::MAX;
            for (c, centroid) in self.codebook[sq].iter().enumerate() {
                let dist =
                    DistanceMetric::Euclidean.compute(sub_vec, centroid);
                if dist < best_dist {
                    best_dist = dist;
                    best_c = c as u8;
                }
            }
            code.push(best_c);
        }

        code
    }

    /// Decode a PQ code back to an approximate vector.
    pub fn decode(&self, code: &[u8]) -> Vec<f32> {
        assert!(self.trained, "Quantizer must be trained first");
        let mut vector = Vec::with_capacity(self.config.dimensions);

        for (sq, &centroid_idx) in code.iter().enumerate() {
            vector.extend_from_slice(
                &self.codebook[sq][centroid_idx as usize],
            );
        }

        vector
    }

    /// Compute asymmetric distance between a raw query and a PQ code.
    /// This avoids decoding the code, providing faster search.
    pub fn asymmetric_distance(
        &self,
        query: &[f32],
        code: &[u8],
    ) -> f32 {
        let mut total_dist = 0.0f32;

        for (sq, &centroid_idx) in code.iter().enumerate() {
            let start = sq * self.sub_dim;
            let end = start + self.sub_dim;
            let sub_query = &query[start..end];
            let centroid = &self.codebook[sq][centroid_idx as usize];
            total_dist +=
                DistanceMetric::Euclidean.compute(sub_query, centroid);
        }

        total_dist
    }

    /// Compression ratio achieved.
    pub fn compression_ratio(&self) -> f32 {
        let original_size =
            self.config.dimensions * std::mem::size_of::<f32>();
        let compressed_size = self.config.num_subquantizers; // 1 byte each
        original_size as f32 / compressed_size as f32
    }

    pub fn is_trained(&self) -> bool {
        self.trained
    }
}
