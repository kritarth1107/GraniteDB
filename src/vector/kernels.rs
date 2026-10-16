//! Auto-vectorized f32 distance kernels.
//!
//! Inner loops stay in `f32` and process eight lanes per iteration so LLVM can
//! emit SIMD without a nightly `std::simd` dependency. The old kernels cast
//! every lane to `f64`, which blocked auto-vectorization.
//!
//! These functions are the production path for [`DistanceMetric::compute`].
//! No throughput numbers are claimed here; Criterion is a later day.

use super::distance::DistanceMetric;

/// Floor under a cosine denominator so a zero vector does not produce NaN.
const COSINE_EPS: f32 = 1e-10;

/// Lanes processed per inner-loop trip. Eight is a natural SSE/NEON width
/// and a half-width for AVX, so the same source auto-vectorizes on both CI
/// targets (x86_64 and aarch64).
const LANES: usize = 8;

/// Pairwise distance using the SIMD-friendly kernel for `metric`.
#[inline]
pub fn distance(metric: DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    match metric {
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::Euclidean => euclidean_distance(a, b),
        DistanceMetric::DotProduct => dot_product_distance(a, b),
        DistanceMetric::Manhattan => super::scalar::manhattan_distance(a, b),
    }
}

/// Cosine distance: `1 - dot(a, b) / (||a|| * ||b||)`.
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let (dot, na, nb) = cosine_components(a, b);
    let denom = (na.sqrt() * nb.sqrt()).max(COSINE_EPS);
    1.0 - dot / denom
}

/// Euclidean (L2) distance: `sqrt(sum((a_i - b_i)^2))`.
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    l2_squared(a, b).sqrt()
}

/// Dot-product distance: `-dot(a, b)` so smaller is closer.
#[inline]
pub fn dot_product_distance(a: &[f32], b: &[f32]) -> f32 {
    -dot_sum(a, b)
}

/// L2 norm of one vector.
#[inline]
pub fn l2_norm(v: &[f32]) -> f32 {
    norm_squared(v).sqrt()
}

/// Fused `dot`, `||a||^2`, `||b||^2` in one 8-wide pass.
#[inline(always)]
fn cosine_components(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    let n = a.len();
    let mut d0 = 0.0f32;
    let mut d1 = 0.0f32;
    let mut d2 = 0.0f32;
    let mut d3 = 0.0f32;
    let mut na0 = 0.0f32;
    let mut na1 = 0.0f32;
    let mut na2 = 0.0f32;
    let mut na3 = 0.0f32;
    let mut nb0 = 0.0f32;
    let mut nb1 = 0.0f32;
    let mut nb2 = 0.0f32;
    let mut nb3 = 0.0f32;
    let mut i = 0;
    while i + LANES <= n {
        let a0 = a[i];
        let a1 = a[i + 1];
        let a2 = a[i + 2];
        let a3 = a[i + 3];
        let a4 = a[i + 4];
        let a5 = a[i + 5];
        let a6 = a[i + 6];
        let a7 = a[i + 7];
        let b0 = b[i];
        let b1 = b[i + 1];
        let b2 = b[i + 2];
        let b3 = b[i + 3];
        let b4 = b[i + 4];
        let b5 = b[i + 5];
        let b6 = b[i + 6];
        let b7 = b[i + 7];
        d0 += a0 * b0 + a1 * b1;
        d1 += a2 * b2 + a3 * b3;
        d2 += a4 * b4 + a5 * b5;
        d3 += a6 * b6 + a7 * b7;
        na0 += a0 * a0 + a1 * a1;
        na1 += a2 * a2 + a3 * a3;
        na2 += a4 * a4 + a5 * a5;
        na3 += a6 * a6 + a7 * a7;
        nb0 += b0 * b0 + b1 * b1;
        nb1 += b2 * b2 + b3 * b3;
        nb2 += b4 * b4 + b5 * b5;
        nb3 += b6 * b6 + b7 * b7;
        i += LANES;
    }
    let mut dot = d0 + d1 + d2 + d3;
    let mut na = na0 + na1 + na2 + na3;
    let mut nb = nb0 + nb1 + nb2 + nb3;
    while i < n {
        let av = a[i];
        let bv = b[i];
        dot += av * bv;
        na += av * av;
        nb += bv * bv;
        i += 1;
    }
    (dot, na, nb)
}

/// 8-wide auto-vectorized dot product.
#[inline(always)]
pub fn dot_sum(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    let n = a.len();
    let mut s0 = 0.0f32;
    let mut s1 = 0.0f32;
    let mut s2 = 0.0f32;
    let mut s3 = 0.0f32;
    let mut i = 0;
    while i + LANES <= n {
        s0 += a[i] * b[i] + a[i + 1] * b[i + 1];
        s1 += a[i + 2] * b[i + 2] + a[i + 3] * b[i + 3];
        s2 += a[i + 4] * b[i + 4] + a[i + 5] * b[i + 5];
        s3 += a[i + 6] * b[i + 6] + a[i + 7] * b[i + 7];
        i += LANES;
    }
    let mut sum = s0 + s1 + s2 + s3;
    while i < n {
        sum += a[i] * b[i];
        i += 1;
    }
    sum
}

/// 8-wide auto-vectorized squared L2.
#[inline(always)]
fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    let n = a.len();
    let mut s0 = 0.0f32;
    let mut s1 = 0.0f32;
    let mut s2 = 0.0f32;
    let mut s3 = 0.0f32;
    let mut i = 0;
    while i + LANES <= n {
        let d0 = a[i] - b[i];
        let d1 = a[i + 1] - b[i + 1];
        let d2 = a[i + 2] - b[i + 2];
        let d3 = a[i + 3] - b[i + 3];
        let d4 = a[i + 4] - b[i + 4];
        let d5 = a[i + 5] - b[i + 5];
        let d6 = a[i + 6] - b[i + 6];
        let d7 = a[i + 7] - b[i + 7];
        s0 += d0 * d0 + d1 * d1;
        s1 += d2 * d2 + d3 * d3;
        s2 += d4 * d4 + d5 * d5;
        s3 += d6 * d6 + d7 * d7;
        i += LANES;
    }
    let mut sum = s0 + s1 + s2 + s3;
    while i < n {
        let d = a[i] - b[i];
        sum += d * d;
        i += 1;
    }
    sum
}

/// 8-wide auto-vectorized `||v||^2`.
#[inline(always)]
fn norm_squared(v: &[f32]) -> f32 {
    let n = v.len();
    let mut s0 = 0.0f32;
    let mut s1 = 0.0f32;
    let mut s2 = 0.0f32;
    let mut s3 = 0.0f32;
    let mut i = 0;
    while i + LANES <= n {
        s0 += v[i] * v[i] + v[i + 1] * v[i + 1];
        s1 += v[i + 2] * v[i + 2] + v[i + 3] * v[i + 3];
        s2 += v[i + 4] * v[i + 4] + v[i + 5] * v[i + 5];
        s3 += v[i + 6] * v[i + 6] + v[i + 7] * v[i + 7];
        i += LANES;
    }
    let mut sum = s0 + s1 + s2 + s3;
    while i < n {
        sum += v[i] * v[i];
        i += 1;
    }
    sum
}
