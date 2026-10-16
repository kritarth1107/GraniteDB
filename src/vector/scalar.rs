//! Scalar reference distance kernels.
//!
//! These implementations use a plain loop and `f64` accumulators. They are the
//! correctness oracle for the auto-vectorized kernels in
//! [`crate::vector::kernels`]. Exact subtree search does not call this module.

use super::distance::DistanceMetric;

/// Floor under a cosine denominator so a zero vector does not produce NaN.
const COSINE_EPS: f64 = 1e-10;

/// Pairwise distance using the scalar reference for `metric`.
#[inline]
pub fn distance(metric: DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    match metric {
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::Euclidean => euclidean_distance(a, b),
        DistanceMetric::DotProduct => dot_product_distance(a, b),
        DistanceMetric::Manhattan => manhattan_distance(a, b),
    }
}

/// Cosine distance: `1 - dot(a, b) / (||a|| * ||b||)`.
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;
    for (av, bv) in a.iter().zip(b.iter()) {
        let x = f64::from(*av);
        let y = f64::from(*bv);
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let denom = (norm_a.sqrt() * norm_b.sqrt()).max(COSINE_EPS);
    (1.0 - (dot / denom)) as f32
}

/// Euclidean (L2) distance: `sqrt(sum((a_i - b_i)^2))`.
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    let mut sum = 0.0f64;
    for (av, bv) in a.iter().zip(b.iter()) {
        let d = f64::from(*av) - f64::from(*bv);
        sum += d * d;
    }
    sum.sqrt() as f32
}

/// Dot-product distance: `-dot(a, b)` so smaller is closer.
#[inline]
pub fn dot_product_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    let mut dot = 0.0f64;
    for (av, bv) in a.iter().zip(b.iter()) {
        dot += f64::from(*av) * f64::from(*bv);
    }
    -(dot as f32)
}

/// Manhattan (L1) distance: `sum(|a_i - b_i|)`.
#[inline]
pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    let mut sum = 0.0f64;
    for (av, bv) in a.iter().zip(b.iter()) {
        sum += (f64::from(*av) - f64::from(*bv)).abs();
    }
    sum as f32
}
