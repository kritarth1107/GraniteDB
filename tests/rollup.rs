//! Unit tests for the default extractive [`granitedb::Rollup`].

use granitedb::{
    ExtractiveRollup, MemoryTree, NodeId, NodeKind, NodeSpec, Payload, Rollup, extract_payload_text,
};

fn sample() -> (MemoryTree, NodeId, NodeId, NodeId, NodeId) {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let project = tree
        .insert_with(root, NodeSpec::new("agent").kind(NodeKind::Project))
        .unwrap();
    let session = tree
        .insert_with(project, NodeSpec::new("s1").kind(NodeKind::Session))
        .unwrap();
    let rust = tree
        .insert(session, "prefers-rust", Payload::text("user likes rust"))
        .unwrap();
    let friday = tree
        .insert(session, "deadline", Payload::text("Friday"))
        .unwrap();
    (tree, project, session, rust, friday)
}

fn extractive(tree: &MemoryTree, id: NodeId) -> Option<String> {
    ExtractiveRollup::default()
        .summarize(&tree.rollup_view(id).unwrap())
        .unwrap()
}

#[test]
fn extract_payload_text_reads_text_field_and_string() {
    assert_eq!(
        extract_payload_text(&Payload::text("  hello  ")).as_deref(),
        Some("hello")
    );
    assert_eq!(
        extract_payload_text(&Payload::json(serde_json::json!("raw"))).as_deref(),
        Some("raw")
    );
    assert_eq!(extract_payload_text(&Payload::text("   ")), None);
    assert_eq!(extract_payload_text(&Payload::empty()), None);
    assert_eq!(
        extract_payload_text(&Payload::json(serde_json::json!({"n": 1}))),
        None
    );
    assert_eq!(
        extract_payload_text(&Payload::json(serde_json::json!({"text": 3}))),
        None
    );
}

#[test]
fn extractive_joins_descendant_text_in_preorder() {
    let (tree, project, session, _, _) = sample();
    let session_text = extractive(&tree, session).unwrap();
    assert_eq!(
        session_text,
        "prefers-rust: user likes rust\ndeadline: Friday"
    );
    let project_text = extractive(&tree, project).unwrap();
    assert_eq!(
        project_text,
        "prefers-rust: user likes rust\ndeadline: Friday"
    );
}

#[test]
fn extractive_skips_the_parent_payload() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let parent = tree
        .insert(root, "parent", Payload::text("do not include me"))
        .unwrap();
    tree.insert(parent, "child", Payload::text("keep")).unwrap();
    assert_eq!(extractive(&tree, parent).as_deref(), Some("child: keep"));
}

#[test]
fn extractive_skips_nodes_without_text() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let parent = tree.insert(root, "p", Payload::empty()).unwrap();
    tree.insert(parent, "silent", Payload::empty()).unwrap();
    tree.insert(parent, "loud", Payload::text("noise")).unwrap();
    tree.insert(
        parent,
        "json-only",
        Payload::json(serde_json::json!({"n": 2})),
    )
    .unwrap();
    assert_eq!(extractive(&tree, parent).as_deref(), Some("loud: noise"));
}

#[test]
fn extractive_empty_subtree_is_none() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let leaf = tree.insert(root, "leaf", Payload::text("only me")).unwrap();
    assert_eq!(extractive(&tree, leaf), None);
    assert_eq!(
        extractive(&tree, tree.root_id()).as_deref(),
        Some("leaf: only me")
    );
}

#[test]
fn extractive_truncates_at_max_chars() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    tree.insert(root, "a", Payload::text("abcdefghij")).unwrap();
    let view = tree.rollup_view(root).unwrap();
    let short = ExtractiveRollup::new()
        .with_max_chars(8)
        .summarize(&view)
        .unwrap()
        .unwrap();
    assert_eq!(short.chars().count(), 8);
    assert!(short.ends_with('…'), "{short}");
    assert_eq!(short, "a: abcd…");
}

#[test]
fn extractive_is_deterministic() {
    let (tree, project, _, _, _) = sample();
    let first = extractive(&tree, project);
    let second = extractive(&tree, project);
    assert_eq!(first, second);
    let mut again = MemoryTree::ephemeral();
    let root = again.root_id();
    let project2 = again
        .insert_with(root, NodeSpec::new("agent").kind(NodeKind::Project))
        .unwrap();
    let session2 = again
        .insert_with(project2, NodeSpec::new("s1").kind(NodeKind::Session))
        .unwrap();
    again
        .insert(session2, "prefers-rust", Payload::text("user likes rust"))
        .unwrap();
    again
        .insert(session2, "deadline", Payload::text("Friday"))
        .unwrap();
    assert_eq!(extractive(&again, project2), first);
}

#[test]
fn rollup_view_lists_parent_then_preorder_descendants() {
    let (tree, _, session, rust, friday) = sample();
    let view = tree.rollup_view(session).unwrap();
    assert_eq!(view.parent().id, session);
    let ids: Vec<_> = view.subtree().map(|n| n.id).collect();
    assert_eq!(ids, vec![session, rust, friday]);
    let desc: Vec<_> = view.descendants().map(|n| n.id).collect();
    assert_eq!(desc, vec![rust, friday]);
    assert!(!view.is_empty());
    assert_eq!(view.len(), 3);
}
