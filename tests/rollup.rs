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

fn deep_tree() -> (MemoryTree, NodeId, NodeId, NodeId, NodeId, NodeId) {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let project = tree
        .insert_with(root, NodeSpec::new("agent").kind(NodeKind::Project))
        .unwrap();
    let session = tree
        .insert_with(project, NodeSpec::new("s1").kind(NodeKind::Session))
        .unwrap();
    let mem = tree
        .insert(session, "prefers-rust", Payload::text("user likes rust"))
        .unwrap();
    let other = tree
        .insert_with(root, NodeSpec::new("other").kind(NodeKind::Project))
        .unwrap();
    tree.insert(other, "side", Payload::text("unrelated"))
        .unwrap();
    (tree, project, session, mem, other, root)
}

#[test]
fn summary_is_cached_on_the_parent() {
    let (mut tree, _, session, _, _, _) = deep_tree();
    assert!(!tree.has_fresh_summary(session).unwrap());
    let first = tree.summary(session).unwrap().map(str::to_string);
    assert_eq!(first.as_deref(), Some("prefers-rust: user likes rust"));
    assert!(tree.has_fresh_summary(session).unwrap());
    assert_eq!(
        tree.get(session).unwrap().summary.as_deref(),
        first.as_deref()
    );
    let second = tree.summary(session).unwrap().map(str::to_string);
    assert_eq!(first, second);
}

#[test]
fn insert_invalidates_parent_summary() {
    let (mut tree, _, session, _, _, _) = deep_tree();
    let _ = tree.summary(session).unwrap();
    assert!(tree.has_fresh_summary(session).unwrap());
    tree.insert(session, "deadline", Payload::text("Friday"))
        .unwrap();
    assert!(!tree.has_fresh_summary(session).unwrap());
    assert!(tree.get(session).unwrap().summary.is_none());
    assert_eq!(
        tree.summary(session).unwrap(),
        Some("prefers-rust: user likes rust\ndeadline: Friday")
    );
}

#[test]
fn update_invalidates_parent_summary() {
    let (mut tree, _, session, mem, _, _) = deep_tree();
    let _ = tree.summary(session).unwrap();
    tree.set_payload(mem, Payload::text("user likes zig"))
        .unwrap();
    assert!(!tree.has_fresh_summary(session).unwrap());
    assert_eq!(
        tree.summary(session).unwrap(),
        Some("prefers-rust: user likes zig")
    );
}

#[test]
fn move_invalidates_old_and_new_parent() {
    let (mut tree, _, session, mem, other, _) = deep_tree();
    let _ = tree.summary(session).unwrap();
    let _ = tree.summary(other).unwrap();
    assert!(tree.has_fresh_summary(session).unwrap());
    assert!(tree.has_fresh_summary(other).unwrap());
    tree.move_node(mem, other).unwrap();
    assert!(!tree.has_fresh_summary(session).unwrap());
    assert!(!tree.has_fresh_summary(other).unwrap());
    assert_eq!(tree.summary(session).unwrap(), None);
    let other_text = tree.summary(other).unwrap().unwrap();
    assert!(other_text.contains("prefers-rust: user likes rust"));
    assert!(other_text.contains("side: unrelated"));
}

#[test]
fn delete_invalidates_parent_summary() {
    let (mut tree, _, session, mem, _, _) = deep_tree();
    tree.insert(session, "deadline", Payload::text("Friday"))
        .unwrap();
    let _ = tree.summary(session).unwrap();
    tree.delete_subtree(mem).unwrap();
    assert!(!tree.has_fresh_summary(session).unwrap());
    assert_eq!(tree.summary(session).unwrap(), Some("deadline: Friday"));
}

#[test]
fn invalidation_propagates_to_ancestors() {
    let (mut tree, project, session, _, other, root) = deep_tree();
    let _ = tree.summary(session).unwrap();
    let _ = tree.summary(project).unwrap();
    let _ = tree.summary(root).unwrap();
    let _ = tree.summary(other).unwrap();
    tree.insert(session, "deadline", Payload::text("Friday"))
        .unwrap();
    assert!(!tree.has_fresh_summary(session).unwrap());
    assert!(!tree.has_fresh_summary(project).unwrap());
    assert!(!tree.has_fresh_summary(root).unwrap());
    assert!(
        tree.has_fresh_summary(other).unwrap(),
        "unrelated sibling branch must keep its cache"
    );
    assert!(tree.get(other).unwrap().summary.is_some());
}

#[test]
fn custom_rollup_is_used_and_replaces_extractive() {
    struct CountRollup;
    impl Rollup for CountRollup {
        fn summarize(
            &self,
            view: &granitedb::RollupView<'_>,
        ) -> granitedb::GraniteResult<Option<String>> {
            Ok(Some(format!("n={}", view.descendants().count())))
        }
    }

    let (mut tree, _, session, _, _, _) = deep_tree();
    let extractive = tree.summary(session).unwrap().map(str::to_string);
    assert_eq!(extractive.as_deref(), Some("prefers-rust: user likes rust"));
    tree.set_rollup(CountRollup);
    assert!(!tree.has_fresh_summary(session).unwrap());
    assert!(tree.get(session).unwrap().summary.is_none());
    assert_eq!(tree.summary(session).unwrap(), Some("n=1"));
    tree.insert(session, "deadline", Payload::text("Friday"))
        .unwrap();
    assert_eq!(tree.summary(session).unwrap(), Some("n=2"));
}

#[test]
fn reopen_after_checkpoint_serves_the_cached_summary() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = MemoryTree::open(dir.path()).unwrap();
    let root = tree.root_id();
    let session = tree
        .insert_with(root, NodeSpec::new("s").kind(NodeKind::Session))
        .unwrap();
    tree.insert(session, "fact", Payload::text("remember"))
        .unwrap();
    let live = tree.summary(session).unwrap().map(str::to_string);
    assert_eq!(live.as_deref(), Some("fact: remember"));
    tree.checkpoint().unwrap();
    drop(tree);

    let mut tree = MemoryTree::open(dir.path()).unwrap();
    assert!(tree.has_fresh_summary(session).unwrap());
    assert_eq!(
        tree.get(session).unwrap().summary.as_deref(),
        live.as_deref()
    );
    assert_eq!(tree.summary(session).unwrap(), live.as_deref());
}

#[test]
fn reopen_without_checkpoint_recomputes_the_same_text() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = granitedb::TreeOpenOptions::new()
        .fsync(false)
        .auto_checkpoint_every(None)
        .open(dir.path())
        .unwrap();
    let root = tree.root_id();
    let session = tree
        .insert_with(root, NodeSpec::new("s").kind(NodeKind::Session))
        .unwrap();
    tree.insert(session, "fact", Payload::text("remember"))
        .unwrap();
    let live = tree.summary(session).unwrap().map(str::to_string);
    drop(tree);

    let mut tree = MemoryTree::open(dir.path()).unwrap();
    assert!(!tree.has_fresh_summary(session).unwrap());
    assert!(tree.get(session).unwrap().summary.is_none());
    assert_eq!(tree.summary(session).unwrap(), live.as_deref());
    assert!(tree.has_fresh_summary(session).unwrap());
}

#[test]
fn reopen_replays_invalidation_then_matches_extractive() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = granitedb::TreeOpenOptions::new()
        .fsync(false)
        .auto_checkpoint_every(None)
        .open(dir.path())
        .unwrap();
    let root = tree.root_id();
    let session = tree
        .insert_with(root, NodeSpec::new("s").kind(NodeKind::Session))
        .unwrap();
    tree.insert(session, "fact", Payload::text("remember"))
        .unwrap();
    let _ = tree.summary(session).unwrap();
    tree.checkpoint().unwrap();
    tree.insert(session, "later", Payload::text("new fact"))
        .unwrap();
    assert!(!tree.has_fresh_summary(session).unwrap());
    drop(tree);

    let mut tree = MemoryTree::open(dir.path()).unwrap();
    assert!(!tree.has_fresh_summary(session).unwrap());
    assert_eq!(
        tree.summary(session).unwrap(),
        Some("fact: remember\nlater: new fact")
    );
}
