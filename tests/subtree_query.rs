//! Unit tests for DFS intervals, path globs, and the subtree query builder.

use granitedb::{
    GraniteError, MemoryTree, NodeId, NodeKind, NodeSpec, PathGlob, Payload, SubtreeQuery,
};
use serde_json::json;

fn build_sample() -> (
    MemoryTree,
    NodeId,
    NodeId,
    NodeId,
    NodeId,
    NodeId,
    NodeId,
    NodeId,
) {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let proj = tree
        .insert_with(root, NodeSpec::new("proj").kind(NodeKind::Project))
        .unwrap();
    let other = tree
        .insert_with(root, NodeSpec::new("other").kind(NodeKind::Project))
        .unwrap();
    let s1 = tree
        .insert_with(proj, NodeSpec::new("s1").kind(NodeKind::Session))
        .unwrap();
    let s2 = tree
        .insert_with(proj, NodeSpec::new("s2").kind(NodeKind::Session))
        .unwrap();
    let ep = tree
        .insert_with(s1, NodeSpec::new("ep").kind(NodeKind::Episode))
        .unwrap();
    let m_s1 = tree
        .insert(
            s1,
            "memory",
            Payload::json(json!({"text": "prefers rust", "role": "user", "meta": {"k": 1}})),
        )
        .unwrap();
    let m_ep = tree
        .insert(ep, "memory", Payload::text("nested rust note"))
        .unwrap();
    let m_s2 = tree
        .insert(s2, "memory", Payload::text("session two"))
        .unwrap();
    let m_other = tree
        .insert(other, "memory", Payload::text("other project"))
        .unwrap();
    let _ = (m_s2, m_other);
    (tree, root, proj, s1, ep, m_s1, m_ep, other)
}

fn ids_of(nodes: &[&granitedb::Node]) -> Vec<NodeId> {
    nodes.iter().map(|n| n.id).collect()
}

fn touch(tree: &mut MemoryTree, id: NodeId) {
    let before = tree.get(id).unwrap().updated_at;
    loop {
        let payload = tree.get(id).unwrap().payload.clone();
        tree.set_payload(id, payload).unwrap();
        if tree.get(id).unwrap().updated_at > before {
            break;
        }
    }
}

#[test]
fn intervals_nest_after_insert() {
    let (tree, root, proj, s1, ep, m_s1, m_ep, _) = build_sample();
    tree.validate().unwrap();
    let root_iv = tree.interval(root).unwrap();
    let proj_iv = tree.interval(proj).unwrap();
    let s1_iv = tree.interval(s1).unwrap();
    assert!(root_iv.contains(proj_iv));
    assert!(proj_iv.contains(s1_iv));
    assert!(s1_iv.contains(tree.interval(m_s1).unwrap()));
    assert!(s1_iv.contains(tree.interval(ep).unwrap()));
    assert!(
        tree.interval(ep)
            .unwrap()
            .contains(tree.interval(m_ep).unwrap())
    );
    assert!(tree.in_subtree(proj, m_ep).unwrap());
    assert!(
        !tree
            .in_subtree(s1, tree.get_by_path_str("/proj/s2").unwrap().id)
            .unwrap()
    );
}

#[test]
fn move_rebuilds_intervals() {
    let (mut tree, _, proj, s1, _, _, _, other) = build_sample();
    tree.move_node(s1, other).unwrap();
    tree.validate().unwrap();
    assert!(tree.in_subtree(other, s1).unwrap());
    assert!(!tree.in_subtree(proj, s1).unwrap());
    assert!(
        tree.in_subtree(s1, tree.get_by_path_str("/other/s1/memory").unwrap().id)
            .unwrap()
    );
}

#[test]
fn rename_preserves_interval_numbers() {
    let (mut tree, _, proj, _, _, _, _, _) = build_sample();
    let before: Vec<_> = tree
        .iter()
        .map(|n| (n.id, tree.interval(n.id).unwrap()))
        .collect();
    tree.rename(proj, "renamed").unwrap();
    tree.validate().unwrap();
    let after: Vec<_> = tree
        .iter()
        .map(|n| (n.id, tree.interval(n.id).unwrap()))
        .collect();
    assert_eq!(before, after);
}

#[test]
fn delete_subtree_rebuilds_remaining_intervals() {
    let (mut tree, root, proj, s1, _, _, _, _) = build_sample();
    tree.delete_subtree(s1).unwrap();
    tree.validate().unwrap();
    assert!(tree.get(s1).is_err());
    assert!(tree.in_subtree(proj, s1).is_err());
    assert!(tree.in_subtree(root, proj).unwrap());
    assert!(matches!(tree.query(s1), Err(GraniteError::NodeNotFound(_))));
}

#[test]
fn glob_star_is_one_segment() {
    let (tree, _, _, _, _, m_s1, _, _) = build_sample();
    let hits = tree.glob("/proj/*/memory").unwrap();
    let got = ids_of(&hits);
    assert!(got.contains(&m_s1));
    assert_eq!(got.len(), 2, "s1/memory and s2/memory, not ep/memory");
    assert!(
        !hits
            .iter()
            .any(|n| n.path.to_string() == "/proj/s1/ep/memory")
    );
}

#[test]
fn glob_starstar_crosses_segments() {
    let (tree, _, _, _, _, m_s1, m_ep, _) = build_sample();
    let hits = tree.glob("/proj/**/memory").unwrap();
    let got = ids_of(&hits);
    assert!(got.contains(&m_s1));
    assert!(got.contains(&m_ep));
    assert_eq!(got.len(), 3);
    assert!(
        !hits
            .iter()
            .any(|n| n.path.to_string().starts_with("/other"))
    );
}

#[test]
fn glob_literal_and_missing_prefix() {
    let (tree, _, _, _, _, m_s1, _, _) = build_sample();
    let lit = tree.glob("/proj/s1/memory").unwrap();
    assert_eq!(ids_of(&lit), vec![m_s1]);
    assert!(tree.glob("/nope/**/memory").unwrap().is_empty());
    assert!(tree.glob("/proj/missing/*").unwrap().is_empty());
    let root_only = tree.glob("/").unwrap();
    assert_eq!(root_only.len(), 1);
    assert!(root_only[0].is_root());
}

#[test]
fn glob_star_and_starstar_on_root() {
    let (tree, _, proj, _, _, _, _, other) = build_sample();
    let kids = tree.glob("/*").unwrap();
    assert_eq!(ids_of(&kids), vec![proj, other]);
    let all = tree.glob("/**").unwrap();
    assert_eq!(all.len(), tree.len());
    let named = tree.glob("/**/memory").unwrap();
    assert_eq!(named.len(), 4);
}

#[test]
fn glob_rejects_bad_patterns() {
    let tree = MemoryTree::ephemeral();
    assert!(matches!(
        tree.glob("proj/*").unwrap_err(),
        GraniteError::InvalidPathGlob(_)
    ));
    assert!(PathGlob::parse("/proj/***").is_err());
}

#[test]
fn query_kind_and_kinds() {
    let (tree, _, proj, _, _, _, _, _) = build_sample();
    let sessions = tree
        .query(proj)
        .unwrap()
        .kind(NodeKind::Session)
        .collect()
        .unwrap();
    assert_eq!(sessions.len(), 2);
    assert!(sessions.iter().all(|n| n.kind == NodeKind::Session));
    let mixed = tree
        .query(proj)
        .unwrap()
        .kinds([NodeKind::Episode, NodeKind::Memory])
        .collect()
        .unwrap();
    assert!(mixed.iter().any(|n| n.kind == NodeKind::Episode));
    assert!(mixed.iter().any(|n| n.kind == NodeKind::Memory));
    assert!(!mixed.iter().any(|n| n.kind == NodeKind::Session));
}

#[test]
fn query_relative_depth_bounds() {
    let (tree, _, proj, s1, ep, m_s1, _, _) = build_sample();
    let only_root = tree.query(proj).unwrap().depth(0).collect().unwrap();
    assert_eq!(ids_of(&only_root), vec![proj]);
    let children = tree
        .query(proj)
        .unwrap()
        .min_depth(1)
        .max_depth(1)
        .collect()
        .unwrap();
    assert_eq!(
        ids_of(&children),
        vec![s1, tree.get_by_path_str("/proj/s2").unwrap().id]
    );
    let deep = tree.query(proj).unwrap().min_depth(3).collect().unwrap();
    assert_eq!(
        ids_of(&deep),
        vec![tree.get_by_path_str("/proj/s1/ep/memory").unwrap().id]
    );
    let mid = tree.query(proj).unwrap().depth(2).collect().unwrap();
    assert_eq!(
        ids_of(&mid),
        vec![
            ep,
            m_s1,
            tree.get_by_path_str("/proj/s2/memory").unwrap().id
        ]
    );
    let from_s1 = tree.query(s1).unwrap().depth(2).collect().unwrap();
    assert_eq!(from_s1.len(), 1);
    assert_eq!(
        from_s1[0].id,
        tree.get_by_path_str("/proj/s1/ep/memory").unwrap().id
    );
}

#[test]
fn query_payload_eq_exists_contains() {
    let (tree, _, proj, _, _, m_s1, m_ep, _) = build_sample();
    let eq = tree
        .query(proj)
        .unwrap()
        .payload_eq("role", json!("user"))
        .collect()
        .unwrap();
    assert_eq!(ids_of(&eq), vec![m_s1]);
    let nested = tree
        .query(proj)
        .unwrap()
        .payload_eq("meta.k", json!(1))
        .collect()
        .unwrap();
    assert_eq!(ids_of(&nested), vec![m_s1]);
    let exists = tree
        .query(proj)
        .unwrap()
        .payload_exists("role")
        .collect()
        .unwrap();
    assert_eq!(ids_of(&exists), vec![m_s1]);
    let missing = tree
        .query(proj)
        .unwrap()
        .payload_exists("nope")
        .collect()
        .unwrap();
    assert!(missing.is_empty());
    let contains = tree
        .query(proj)
        .unwrap()
        .payload_contains("text", "rust")
        .collect()
        .unwrap();
    assert_eq!(ids_of(&contains), vec![m_ep, m_s1]);
    let case = tree
        .query(proj)
        .unwrap()
        .payload_contains("text", "Rust")
        .collect()
        .unwrap();
    assert!(case.is_empty());
    let empty_field = tree
        .query(proj)
        .unwrap()
        .payload_exists("")
        .collect()
        .unwrap();
    assert!(empty_field.is_empty());
}

#[test]
fn query_recency_and_limit() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let a = tree.insert(root, "a", Payload::text("a")).unwrap();
    let b = tree.insert(root, "b", Payload::text("b")).unwrap();
    let c = tree.insert(root, "c", Payload::text("c")).unwrap();
    touch(&mut tree, a);
    touch(&mut tree, b);
    touch(&mut tree, c);
    let recent = tree
        .query(root)
        .unwrap()
        .kind(NodeKind::Memory)
        .most_recent(2)
        .collect()
        .unwrap();
    assert_eq!(ids_of(&recent), vec![c, b]);
    let limited = tree
        .query(root)
        .unwrap()
        .kind(NodeKind::Memory)
        .most_recent(3)
        .limit(1)
        .collect()
        .unwrap();
    assert_eq!(ids_of(&limited), vec![c]);
    let none = tree.query(root).unwrap().most_recent(0).collect().unwrap();
    assert!(none.is_empty());
    let zero_lim = tree.query(root).unwrap().limit(0).collect().unwrap();
    assert!(zero_lim.is_empty());
}

#[test]
fn query_missing_root_is_an_error() {
    let tree = MemoryTree::ephemeral();
    assert!(matches!(
        tree.query(NodeId::from_raw(99)),
        Err(GraniteError::NodeNotFound(_))
    ));
}

#[test]
fn query_builder_root_accessor() {
    let (tree, _, proj, _, _, _, _, _) = build_sample();
    let q: SubtreeQuery<'_> = tree.query(proj).unwrap();
    assert_eq!(q.root(), proj);
}

#[test]
fn combined_filters() {
    let (tree, _, proj, _, _, m_s1, _, _) = build_sample();
    let hits = tree
        .query(proj)
        .unwrap()
        .kind(NodeKind::Memory)
        .min_depth(2)
        .max_depth(2)
        .payload_contains("text", "rust")
        .limit(5)
        .collect()
        .unwrap();
    assert_eq!(ids_of(&hits), vec![m_s1]);
}
