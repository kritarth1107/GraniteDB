//! SIMD-friendly kernels must match the scalar f64 reference.

use granitedb::DistanceMetric;
use granitedb::vector::{QueryKernel, scan_distances};
use proptest::prelude::*;

fn close(a: f32, b: f32) -> bool {
    if a == b {
        return true;
    }
    if !a.is_finite() && !b.is_finite() {
        return true;
    }
    let diff = (a - b).abs();
    let scale = a.abs().max(b.abs()).max(1.0);
    diff <= 1e-3 * scale
}

fn assert_close(metric: DistanceMetric, a: &[f32], b: &[f32]) {
    let simd = metric.compute(a, b);
    let scalar = metric.compute_scalar(a, b);
    assert!(
        close(simd, scalar),
        "{metric:?} dim={} simd={simd} scalar={scalar} a={a:?} b={b:?}",
        a.len()
    );
}

fn assert_kernel_close(metric: DistanceMetric, a: &[f32], b: &[f32]) {
    let pairwise = metric.compute(a, b);
    let via_query = QueryKernel::new(metric, a).distance(b);
    assert!(
        close(pairwise, via_query),
        "{metric:?} QueryKernel={via_query} pairwise={pairwise}"
    );
}

const DIMS: &[usize] = &[0, 1, 3, 7, 8, 9, 15, 16, 17, 31, 32, 63, 64, 128, 384];

fn unit(n: usize, axis: usize) -> Vec<f32> {
    let mut v = vec![0.0; n];
    if n > 0 {
        v[axis % n] = 1.0;
    }
    v
}

fn ramp(n: usize, start: f32, step: f32) -> Vec<f32> {
    (0..n).map(|i| start + step * i as f32).collect()
}

#[test]
fn identical_vectors_are_nearest() {
    for &dim in DIMS {
        let a = ramp(dim, 0.25, 0.05);
        for metric in DistanceMetric::ALL {
            if dim == 0 && metric == DistanceMetric::Cosine {
                // empty cosine uses the denom floor; both paths agree.
            }
            assert_close(metric, &a, &a);
            if metric != DistanceMetric::DotProduct {
                let d = metric.compute(&a, &a);
                if dim > 0 && a.iter().any(|x| *x != 0.0) {
                    assert!(d.abs() < 1e-4, "{metric:?} identical dim={dim} d={d}");
                }
            }
        }
    }
}

#[test]
fn orthogonal_unit_vectors() {
    for &dim in &[2usize, 3, 8, 16, 64] {
        let a = unit(dim, 0);
        let b = unit(dim, 1);
        assert_close(DistanceMetric::Cosine, &a, &b);
        assert_close(DistanceMetric::DotProduct, &a, &b);
        assert_close(DistanceMetric::Euclidean, &a, &b);
        assert_close(DistanceMetric::Manhattan, &a, &b);
        let dot = DistanceMetric::DotProduct.compute(&a, &b);
        assert!(dot.abs() < 1e-5, "orthogonal dot distance {dot}");
        let cos = DistanceMetric::Cosine.compute(&a, &b);
        assert!((cos - 1.0).abs() < 1e-5, "orthogonal cosine {cos}");
    }
}

#[test]
fn known_small_values() {
    let a = [1.0f32, 0.0, 0.0];
    let b = [0.0f32, 1.0, 0.0];
    let c = [0.6f32, 0.8, 0.0];
    for metric in DistanceMetric::ALL {
        assert_close(metric, &a, &b);
        assert_close(metric, &a, &c);
        assert_close(metric, &c, &c);
    }
    let l2 = DistanceMetric::Euclidean.compute(&[0.0, 0.0], &[3.0, 4.0]);
    assert!((l2 - 5.0).abs() < 1e-5);
    let l1 = DistanceMetric::Manhattan.compute(&[1.0, -2.0], &[4.0, 2.0]);
    assert!((l1 - 7.0).abs() < 1e-5);
}

#[test]
fn query_kernel_matches_pairwise() {
    for &dim in DIMS {
        let q = ramp(dim, -0.5, 0.03);
        let v = ramp(dim, 0.2, -0.01);
        for metric in DistanceMetric::ALL {
            assert_kernel_close(metric, &q, &v);
        }
    }
}

#[test]
fn scan_distances_matches_scalar_batch() {
    let query = ramp(16, 0.1, 0.05);
    let vecs: Vec<Vec<f32>> = (0..12).map(|i| ramp(16, i as f32 * 0.02, 0.01)).collect();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
    for metric in DistanceMetric::ALL {
        let mut out = Vec::new();
        scan_distances(metric, &query, refs.iter().copied(), &mut out);
        assert_eq!(out.len(), vecs.len());
        for (got, v) in out.iter().zip(vecs.iter()) {
            let expect = metric.compute_scalar(&query, v);
            assert!(close(*got, expect), "{metric:?} scan={got} scalar={expect}");
        }
    }
}

fn arb_metric() -> impl Strategy<Value = DistanceMetric> {
    prop_oneof![
        Just(DistanceMetric::Cosine),
        Just(DistanceMetric::Euclidean),
        Just(DistanceMetric::DotProduct),
        Just(DistanceMetric::Manhattan),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        ..ProptestConfig::default()
    })]

    #[test]
    fn kernel_matches_scalar_random(
        metric in arb_metric(),
        dim in 1usize..=257,
        seed in any::<u64>(),
    ) {
        let mut a = vec![0.0f32; dim];
        let mut b = vec![0.0f32; dim];
        let mut x = seed;
        for i in 0..dim {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
            a[i] = ((x >> 33) as i32 as f32) / (i32::MAX as f32) * 2.0;
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
            b[i] = ((x >> 33) as i32 as f32) / (i32::MAX as f32) * 2.0;
        }
        let simd = metric.compute(&a, &b);
        let scalar = metric.compute_scalar(&a, &b);
        prop_assert!(
            close(simd, scalar),
            "{metric:?} dim={dim} simd={simd} scalar={scalar}"
        );
        let via_query = QueryKernel::new(metric, &a).distance(&b);
        prop_assert!(
            close(simd, via_query),
            "{metric:?} QueryKernel={via_query} pairwise={simd}"
        );
    }
}
