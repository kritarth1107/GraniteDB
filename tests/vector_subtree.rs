//! Subtree search must use the crate's distance functions and HNSW index.

use granitedb::{
    DistanceMetric, GraniteError, MemoryTree, NodeId, NodeKind, NodeSpec, Payload,
    ProductQuantizer, SearchHit,
};

fn embed(name: &str, v: Vec<f32>) -> NodeSpec {
    NodeSpec::new(name)
        .kind(NodeKind::Memory)
        .payload(Payload::text(name).with_embedding(v))
}

#[test]
fn brute_force_uses_distance_metric() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let a = tree
        .insert_with(root, embed("a", vec![1.0, 0.0, 0.0]))
        .unwrap();
    tree.insert_with(root, embed("b", vec![0.0, 1.0, 0.0]))
        .unwrap();
    tree.insert_with(root, embed("c", vec![0.9, 0.1, 0.0]))
        .unwrap();

    let hits = tree
        .search_subtree(root, &[1.0, 0.0, 0.0], 2, DistanceMetric::Cosine)
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].id, a);
    assert!(hits[0].distance < hits[1].distance);
}

#[test]
fn search_is_scoped_to_subtree() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let left = tree
        .insert_with(root, NodeSpec::new("left").kind(NodeKind::Project))
        .unwrap();
    let right = tree
        .insert_with(root, NodeSpec::new("right").kind(NodeKind::Project))
        .unwrap();
    tree.insert_with(left, embed("l1", vec![1.0, 0.0])).unwrap();
    let r1 = tree
        .insert_with(right, embed("r1", vec![1.0, 0.0]))
        .unwrap();

    let hits = tree
        .search_subtree(right, &[1.0, 0.0], 4, DistanceMetric::DotProduct)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, r1);
}

#[test]
fn hnsw_ann_agrees_with_brute_force_on_tiny_set() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    for (name, v) in [
        ("n0", vec![1.0, 0.0, 0.0]),
        ("n1", vec![0.0, 1.0, 0.0]),
        ("n2", vec![0.0, 0.0, 1.0]),
        ("n3", vec![0.7, 0.7, 0.0]),
    ] {
        tree.insert_with(root, embed(name, v)).unwrap();
    }
    let query = [1.0, 0.0, 0.0];
    let exact = tree
        .search_subtree(root, &query, 2, DistanceMetric::Cosine)
        .unwrap();
    let ann = tree.search_subtree_ann(root, &query, 2).unwrap();
    assert_eq!(exact[0].id, ann[0].id);
}

#[test]
fn product_quantizer_encodes_node_embeddings() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let mut vectors = Vec::new();
    for i in 0..8 {
        let v = vec![i as f32, (i + 1) as f32, 0.0, 1.0];
        tree.insert_with(root, embed(&format!("v{i}"), v.clone()))
            .unwrap();
        vectors.push(v);
    }
    let mut pq = ProductQuantizer::new(granitedb::PQConfig {
        num_subquantizers: 2,
        num_centroids: 4,
        dimensions: 4,
        training_iterations: 5,
    });
    pq.train(&vectors);
    let codes = pq.encode(&vectors[0]);
    assert_eq!(codes.len(), 2);
}

fn close(a: f32, b: f32) -> bool {
    let diff = (a - b).abs();
    let scale = a.abs().max(b.abs()).max(1.0);
    diff <= 1e-3 * scale
}

/// Independent oracle: Euler membership via `in_subtree`, distances via scalar.
fn scalar_knn(
    tree: &MemoryTree,
    root: NodeId,
    query: &[f32],
    k: usize,
    metric: DistanceMetric,
) -> Vec<SearchHit> {
    let mut hits = Vec::new();
    for node in tree.iter() {
        if !tree.in_subtree(root, node.id).unwrap() {
            continue;
        }
        let Some(emb) = node.payload.embedding.as_deref() else {
            continue;
        };
        if emb.len() != query.len() || !emb.iter().copied().all(f32::is_finite) {
            continue;
        }
        let distance = metric.compute_scalar(query, emb);
        if !distance.is_finite() {
            continue;
        }
        hits.push(SearchHit {
            id: node.id,
            distance,
        });
    }
    hits.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.id.cmp(&b.id))
    });
    hits.truncate(k);
    hits
}

fn assert_hits_match(got: &[SearchHit], expect: &[SearchHit], metric: DistanceMetric) {
    assert_eq!(
        got.len(),
        expect.len(),
        "{metric:?} len got={} expect={}",
        got.len(),
        expect.len()
    );
    for (g, e) in got.iter().zip(expect.iter()) {
        assert_eq!(g.id, e.id, "{metric:?} id order {got:?} vs {expect:?}");
        assert!(
            close(g.distance, e.distance),
            "{metric:?} id={} simd={} scalar={}",
            g.id,
            g.distance,
            e.distance
        );
    }
}

fn knn_tree() -> (MemoryTree, NodeId, NodeId, NodeId) {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let left = tree
        .insert_with(root, NodeSpec::new("left").kind(NodeKind::Project))
        .unwrap();
    let right = tree
        .insert_with(root, NodeSpec::new("right").kind(NodeKind::Project))
        .unwrap();
    tree.insert_with(left, embed("l-near", vec![1.0, 0.0, 0.0]))
        .unwrap();
    tree.insert_with(left, embed("l-mid", vec![0.8, 0.2, 0.0]))
        .unwrap();
    tree.insert_with(left, embed("l-far", vec![0.0, 1.0, 0.0]))
        .unwrap();
    tree.insert_with(right, embed("r-near", vec![1.0, 0.0, 0.0]))
        .unwrap();
    tree.insert_with(right, embed("r-out", vec![0.99, 0.01, 0.0]))
        .unwrap();
    tree.insert_with(left, NodeSpec::new("no-vec").kind(NodeKind::Memory))
        .unwrap();
    (tree, root, left, right)
}

#[test]
fn exact_knn_matches_scalar_for_every_metric() {
    let (tree, root, left, right) = knn_tree();
    let query = [1.0f32, 0.0, 0.0];
    for metric in DistanceMetric::ALL {
        for (scope, k) in [(root, 3), (left, 2), (right, 4), (left, 8)] {
            let got = tree.search_subtree(scope, &query, k, metric).unwrap();
            let expect = scalar_knn(&tree, scope, &query, k, metric);
            assert_hits_match(&got, &expect, metric);
        }
    }
}

#[test]
fn closer_vector_outside_subtree_is_excluded() {
    let (tree, _root, left, right) = knn_tree();
    let query = [0.99f32, 0.01, 0.0];
    for metric in DistanceMetric::ALL {
        let hits = tree.search_subtree(left, &query, 8, metric).unwrap();
        for hit in &hits {
            assert!(
                tree.in_subtree(left, hit.id).unwrap(),
                "{metric:?} leaked {}",
                hit.id
            );
            assert!(!tree.in_subtree(right, hit.id).unwrap() || hit.id == left);
        }
        assert!(!hits.is_empty());
    }
}

#[test]
fn k_zero_is_empty_and_k_larger_than_pool_returns_all() {
    let (tree, _, left, _) = knn_tree();
    let query = [1.0f32, 0.0, 0.0];
    for metric in DistanceMetric::ALL {
        let empty = tree.search_subtree(left, &query, 0, metric).unwrap();
        assert!(empty.is_empty(), "{metric:?}");
        let all = tree.search_subtree(left, &query, 64, metric).unwrap();
        let expect = scalar_knn(&tree, left, &query, 64, metric);
        assert_hits_match(&all, &expect, metric);
        assert_eq!(all.len(), 3, "three finite embeddings under left");
    }
}

#[test]
fn non_finite_embeddings_are_skipped() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let good = tree
        .insert_with(root, embed("good", vec![1.0, 0.0]))
        .unwrap();
    tree.insert_with(root, embed("nan", vec![f32::NAN, 0.0]))
        .unwrap();
    tree.insert_with(root, embed("inf", vec![f32::INFINITY, 0.0]))
        .unwrap();
    let hits = tree
        .search_subtree(root, &[1.0, 0.0], 8, DistanceMetric::Euclidean)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, good);
}

#[test]
fn dimension_mismatch_is_an_error() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    tree.insert_with(root, embed("d2", vec![1.0, 0.0])).unwrap();
    let err = tree
        .search_subtree(root, &[1.0, 0.0, 0.0], 1, DistanceMetric::Cosine)
        .unwrap_err();
    assert!(matches!(
        err,
        GraniteError::VectorDimensionMismatch {
            expected: 3,
            actual: 2
        }
    ));
}

#[test]
fn equal_distances_break_ties_by_node_id() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let a = tree.insert_with(root, embed("a", vec![1.0, 0.0])).unwrap();
    let b = tree.insert_with(root, embed("b", vec![1.0, 0.0])).unwrap();
    let hits = tree
        .search_subtree(root, &[1.0, 0.0], 2, DistanceMetric::Euclidean)
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(a < b);
    assert_eq!(hits[0].id, a);
    assert_eq!(hits[1].id, b);
    assert!(close(hits[0].distance, hits[1].distance));
}

#[test]
fn move_updates_euler_scope_for_exact_search() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let left = tree
        .insert_with(root, NodeSpec::new("left").kind(NodeKind::Project))
        .unwrap();
    let right = tree
        .insert_with(root, NodeSpec::new("right").kind(NodeKind::Project))
        .unwrap();
    let moving = tree.insert_with(left, embed("m", vec![1.0, 0.0])).unwrap();
    tree.insert_with(right, embed("stay", vec![0.0, 1.0]))
        .unwrap();

    let before = tree
        .search_subtree(right, &[1.0, 0.0], 4, DistanceMetric::Cosine)
        .unwrap();
    assert!(before.iter().all(|h| h.id != moving));

    tree.move_node(moving, right).unwrap();
    let after = tree
        .search_subtree(right, &[1.0, 0.0], 4, DistanceMetric::Cosine)
        .unwrap();
    assert_eq!(after[0].id, moving);
    let left_hits = tree
        .search_subtree(left, &[1.0, 0.0], 4, DistanceMetric::Cosine)
        .unwrap();
    assert!(left_hits.iter().all(|h| h.id != moving));
}
