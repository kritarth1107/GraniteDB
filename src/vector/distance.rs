//! Distance / similarity metrics used by exact and approximate vector search.
//!
//! [`DistanceMetric::compute`] dispatches to the auto-vectorized kernels in
//! [`crate::vector::kernels`]. A scalar `f64` reference lives in
//! [`crate::vector::scalar`] for correctness tests.

use serde::{Deserialize, Serialize};

use super::kernels;
use super::scalar;

/// Supported distance / similarity metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Cosine similarity (`1 - cosine_sim` → distance).
    Cosine,
    /// Euclidean / L2 distance.
    Euclidean,
    /// Dot product (higher = more similar, negated for distance).
    DotProduct,
    /// Manhattan / L1 distance.
    Manhattan,
}

impl DistanceMetric {
    /// Every metric `search_subtree` accepts.
    pub const ALL: [Self; 4] = [
        Self::Cosine,
        Self::Euclidean,
        Self::DotProduct,
        Self::Manhattan,
    ];

    /// Compute the distance between two vectors (SIMD-friendly kernel).
    #[inline]
    pub fn compute(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        kernels::distance(*self, a, b)
    }

    /// Scalar `f64` reference. Used by tests; not the search hot path.
    #[inline]
    pub fn compute_scalar(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
        scalar::distance(*self, a, b)
    }
}

/// Normalize a vector to unit length (for cosine similarity preprocessing).
pub fn normalize(v: &mut [f32]) {
    let norm = kernels::l2_norm(v);
    if norm > 1e-10 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

/// Compute the magnitude (L2 norm) of a vector.
pub fn magnitude(v: &[f32]) -> f32 {
    kernels::l2_norm(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let dist = DistanceMetric::Cosine.compute(&a, &a);
        assert!(dist.abs() < 1e-5);
    }

    #[test]
    fn test_euclidean_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let dist = DistanceMetric::Euclidean.compute(&a, &a);
        assert!(dist.abs() < 1e-5);
    }

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let dist = DistanceMetric::DotProduct.compute(&a, &b);
        assert!((dist - 0.0).abs() < 1e-5); // orthogonal → dot=0
    }

    #[test]
    fn test_manhattan_identical() {
        let a = vec![1.0, -2.0, 3.0];
        let dist = DistanceMetric::Manhattan.compute(&a, &a);
        assert!(dist.abs() < 1e-5);
    }

    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0];
        normalize(&mut v);
        assert!((magnitude(&v) - 1.0).abs() < 1e-5);
    }
}
