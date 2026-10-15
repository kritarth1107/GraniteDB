//! Subtree search must use the crate's distance functions and HNSW index.

use granitedb::{DistanceMetric, MemoryTree, NodeKind, NodeSpec, Payload, ProductQuantizer};

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
