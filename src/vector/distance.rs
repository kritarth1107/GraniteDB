// ============================================================================
// GraniteDB — Distance / Similarity Functions
// ============================================================================
// SIMD-friendly distance computation for vector similarity search.
// Supports cosine similarity, euclidean (L2), dot product, and Manhattan (L1).
// Optimized for millions of high-dimensional vectors.
// ============================================================================

use serde::{Deserialize, Serialize};

/// Supported distance / similarity metrics.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine_sim → distance)
    Cosine,
    /// Euclidean / L2 distance
    Euclidean,
    /// Dot product (higher = more similar, negated for distance)
    DotProduct,
    /// Manhattan / L1 distance
    Manhattan,
}

impl DistanceMetric {
    /// Compute the distance between two vectors.
    #[inline]
    pub fn compute(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        match self {
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::Euclidean => euclidean_distance(a, b),
            DistanceMetric::DotProduct => dot_product_distance(a, b),
            DistanceMetric::Manhattan => manhattan_distance(a, b),
        }
    }
}

/// Cosine distance: 1.0 - cosine_similarity
/// cosine_similarity = dot(a,b) / (||a|| * ||b||)
#[inline]
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;

    // Process in chunks of 4 for better instruction-level parallelism
    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    for i in 0..chunks {
        let base = i * 4;
        let a0 = a[base] as f64;
        let a1 = a[base + 1] as f64;
        let a2 = a[base + 2] as f64;
        let a3 = a[base + 3] as f64;
        let b0 = b[base] as f64;
        let b1 = b[base + 1] as f64;
        let b2 = b[base + 2] as f64;
        let b3 = b[base + 3] as f64;

        dot += a0 * b0 + a1 * b1 + a2 * b2 + a3 * b3;
        norm_a += a0 * a0 + a1 * a1 + a2 * a2 + a3 * a3;
        norm_b += b0 * b0 + b1 * b1 + b2 * b2 + b3 * b3;
    }

    let base = chunks * 4;
    for i in 0..remainder {
        let av = a[base + i] as f64;
        let bv = b[base + i] as f64;
        dot += av * bv;
        norm_a += av * av;
        norm_b += bv * bv;
    }

    let denom = (norm_a.sqrt() * norm_b.sqrt()).max(1e-10);
    (1.0 - (dot / denom)) as f32
}

/// Euclidean (L2) distance: sqrt(sum((a_i - b_i)^2))
#[inline]
fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f64;

    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    for i in 0..chunks {
        let base = i * 4;
        let d0 = (a[base] - b[base]) as f64;
        let d1 = (a[base + 1] - b[base + 1]) as f64;
        let d2 = (a[base + 2] - b[base + 2]) as f64;
        let d3 = (a[base + 3] - b[base + 3]) as f64;
        sum += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3;
    }

    let base = chunks * 4;
    for i in 0..remainder {
        let d = (a[base + i] - b[base + i]) as f64;
        sum += d * d;
    }

    sum.sqrt() as f32
}

/// Dot product distance: negative dot product (smaller = more similar).
#[inline]
fn dot_product_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f64;

    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    for i in 0..chunks {
        let base = i * 4;
        dot += (a[base] as f64) * (b[base] as f64)
            + (a[base + 1] as f64) * (b[base + 1] as f64)
            + (a[base + 2] as f64) * (b[base + 2] as f64)
            + (a[base + 3] as f64) * (b[base + 3] as f64);
    }

    let base = chunks * 4;
    for i in 0..remainder {
        dot += (a[base + i] as f64) * (b[base + i] as f64);
    }

    -(dot as f32)
}

/// Manhattan (L1) distance: sum(|a_i - b_i|)
#[inline]
fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f64;

    for (av, bv) in a.iter().zip(b.iter()) {
        sum += (*av as f64 - *bv as f64).abs();
    }

    sum as f32
}

/// Normalize a vector to unit length (for cosine similarity preprocessing).
pub fn normalize(v: &mut [f32]) {
    let norm: f64 = v.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();
    if norm > 1e-10 {
        for x in v.iter_mut() {
            *x = (*x as f64 / norm) as f32;
        }
    }
}

/// Compute the magnitude (L2 norm) of a vector.
pub fn magnitude(v: &[f32]) -> f32 {
    v.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt() as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let dist = cosine_distance(&a, &a);
        assert!(dist.abs() < 1e-5);
    }

    #[test]
    fn test_euclidean_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let dist = euclidean_distance(&a, &a);
        assert!(dist.abs() < 1e-5);
    }

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let dist = dot_product_distance(&a, &b);
        assert!((dist - 0.0).abs() < 1e-5); // orthogonal → dot=0
    }

    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0];
        normalize(&mut v);
        assert!((magnitude(&v) - 1.0).abs() < 1e-5);
    }
}
