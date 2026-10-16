//! Crash-safety tests: reopen, snapshot, torn WAL tail.

use granitedb::{MemoryTree, NodeId, NodeKind, NodeSpec, Payload, TreeOpenOptions};
use std::fs::OpenOptions;
use std::io::Write;

fn build_tree(dir: &std::path::Path) -> (u64, u64, u64) {
    let mut tree = MemoryTree::open(dir).unwrap();
    let root = tree.root_id();
    let project = tree
        .insert_with(root, NodeSpec::new("proj").kind(NodeKind::Project))
        .unwrap();
    let session = tree
        .insert_with(project, NodeSpec::new("sess").kind(NodeKind::Session))
        .unwrap();
    let mem = tree
        .insert(session, "fact", Payload::text("remember this"))
        .unwrap();
    drop(tree);
    (project.raw(), session.raw(), mem.raw())
}

#[test]
fn reopen_replays_wal() {
    let dir = tempfile::tempdir().unwrap();
    let (project, session, mem) = build_tree(dir.path());
    let tree = MemoryTree::open(dir.path()).unwrap();
    tree.validate().unwrap();
    assert_eq!(tree.len(), 4);
    assert_eq!(
        tree.get_by_path_str("/proj/sess/fact").unwrap().id.raw(),
        mem
    );
    assert_eq!(
        tree.get(granitedb::NodeId::from_raw(session))
            .unwrap()
            .parent_id
            .unwrap()
            .raw(),
        project
    );
}

#[test]
fn checkpoint_then_reopen_matches() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = MemoryTree::open(dir.path()).unwrap();
    let root = tree.root_id();
    tree.insert(root, "a", Payload::text("alpha")).unwrap();
    tree.insert(root, "b", Payload::text("beta")).unwrap();
    tree.checkpoint().unwrap();
    // WAL should be empty after checkpoint; reopen still has both nodes.
    drop(tree);

    let wal = dir.path().join("wal").join("segment_00000000.wal");
    assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);

    let tree = MemoryTree::open(dir.path()).unwrap();
    tree.validate().unwrap();
    assert_eq!(tree.len(), 3);
    assert_eq!(
        tree.get_by_path_str("/a").unwrap().payload.data["text"],
        serde_json::json!("alpha")
    );
}

#[test]
fn mutations_after_checkpoint_replay() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = MemoryTree::open(dir.path()).unwrap();
    let root = tree.root_id();
    let a = tree.insert(root, "a", Payload::empty()).unwrap();
    tree.checkpoint().unwrap();
    tree.insert(a, "child", Payload::text("after snap"))
        .unwrap();
    drop(tree);

    let tree = MemoryTree::open(dir.path()).unwrap();
    tree.validate().unwrap();
    assert_eq!(
        tree.get_by_path_str("/a/child").unwrap().payload.data["text"],
        serde_json::json!("after snap")
    );
}

#[test]
fn torn_wal_tail_is_truncated_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = MemoryTree::open(dir.path()).unwrap();
    let root = tree.root_id();
    tree.insert(root, "keep", Payload::text("durable")).unwrap();
    drop(tree);

    let wal_path = dir.path().join("wal").join("segment_00000000.wal");
    let mut wal = OpenOptions::new().append(true).open(&wal_path).unwrap();
    wal.write_all(b"\x10\x00\x00\x00this is a torn record")
        .unwrap();
    wal.flush().unwrap();
    drop(wal);

    let tree = MemoryTree::open(dir.path()).unwrap();
    tree.validate().unwrap();
    assert_eq!(tree.len(), 2);
    assert_eq!(
        tree.get_by_path_str("/keep").unwrap().payload.data["text"],
        serde_json::json!("durable")
    );

    // A new insert after recovery must succeed and survive another reopen.
    let mut tree = tree;
    tree.insert(tree.root_id(), "after-crash", Payload::empty())
        .unwrap();
    drop(tree);
    let tree = MemoryTree::open(dir.path()).unwrap();
    assert!(tree.get_by_path_str("/after-crash").is_ok());
}

#[test]
fn move_and_delete_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = MemoryTree::open(dir.path()).unwrap();
    let root = tree.root_id();
    let p1 = tree.insert(root, "p1", Payload::empty()).unwrap();
    let p2 = tree.insert(root, "p2", Payload::empty()).unwrap();
    let leaf = tree.insert(p1, "leaf", Payload::text("x")).unwrap();
    tree.move_node(leaf, p2).unwrap();
    tree.delete_subtree(p1).unwrap();
    drop(tree);

    let tree = MemoryTree::open(dir.path()).unwrap();
    tree.validate().unwrap();
    assert!(tree.get_by_path_str("/p1").is_err());
    assert_eq!(tree.get_by_path_str("/p2/leaf").unwrap().id, leaf);
}

fn interval_table(tree: &MemoryTree) -> Vec<(u64, u32, u32)> {
    let mut rows: Vec<_> = tree
        .iter()
        .map(|node| {
            let iv = tree.interval(node.id).unwrap();
            (node.id.raw(), iv.dfs_in, iv.dfs_out)
        })
        .collect();
    rows.sort_unstable();
    rows
}

fn query_memory_ids(tree: &MemoryTree, root: NodeId) -> Vec<u64> {
    tree.query(root)
        .unwrap()
        .kind(NodeKind::Memory)
        .payload_contains("text", "keep")
        .collect()
        .unwrap()
        .into_iter()
        .map(|n| n.id.raw())
        .collect()
}

fn glob_fact_ids(tree: &MemoryTree) -> Vec<u64> {
    tree.glob("/beta/**/fact")
        .unwrap()
        .into_iter()
        .map(|n| n.id.raw())
        .collect()
}

fn populate_query_tree(tree: &mut MemoryTree) -> NodeId {
    let root = tree.root_id();
    let alpha = tree
        .insert_with(root, NodeSpec::new("alpha").kind(NodeKind::Project))
        .unwrap();
    let sess = tree
        .insert_with(alpha, NodeSpec::new("sess").kind(NodeKind::Session))
        .unwrap();
    tree.insert(sess, "fact", Payload::text("keep this rust note"))
        .unwrap();
    tree.insert(sess, "noise", Payload::text("ignore me"))
        .unwrap();
    let beta = tree
        .insert_with(root, NodeSpec::new("beta").kind(NodeKind::Project))
        .unwrap();
    tree.insert(beta, "fact", Payload::text("keep elsewhere"))
        .unwrap();
    tree.move_node(sess, beta).unwrap();
    tree.rename(alpha, "alpha2").unwrap();
    alpha
}

#[test]
fn intervals_and_queries_survive_wal_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = TreeOpenOptions::new()
        .fsync(false)
        .auto_checkpoint_every(None)
        .open(dir.path())
        .unwrap();
    let alpha = populate_query_tree(&mut tree);
    tree.validate().unwrap();
    let intervals = interval_table(&tree);
    let queried = query_memory_ids(&tree, tree.root_id());
    let globbed = glob_fact_ids(&tree);
    assert!(!queried.is_empty());
    assert!(!globbed.is_empty());
    let alpha_still = tree.get(alpha).unwrap().path.to_string();
    drop(tree);

    let tree = MemoryTree::open(dir.path()).unwrap();
    tree.validate().unwrap();
    assert_eq!(interval_table(&tree), intervals);
    assert_eq!(query_memory_ids(&tree, tree.root_id()), queried);
    assert_eq!(glob_fact_ids(&tree), globbed);
    assert_eq!(tree.get(alpha).unwrap().path.to_string(), alpha_still);
}

#[test]
fn intervals_and_queries_survive_snapshot_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = MemoryTree::open(dir.path()).unwrap();
    populate_query_tree(&mut tree);
    tree.checkpoint().unwrap();
    let intervals = interval_table(&tree);
    let queried = query_memory_ids(&tree, tree.root_id());
    let globbed = glob_fact_ids(&tree);
    drop(tree);

    let tree = MemoryTree::open(dir.path()).unwrap();
    tree.validate().unwrap();
    assert_eq!(interval_table(&tree), intervals);
    assert_eq!(query_memory_ids(&tree, tree.root_id()), queried);
    assert_eq!(glob_fact_ids(&tree), globbed);
}

#[test]
fn fsync_option_open_works() {
    let dir = tempfile::tempdir().unwrap();
    let mut tree = TreeOpenOptions::new()
        .fsync(false)
        .auto_checkpoint_every(None)
        .open(dir.path())
        .unwrap();
    tree.insert(tree.root_id(), "n", Payload::empty()).unwrap();
    tree.flush().unwrap();
    drop(tree);
    let tree = MemoryTree::open(dir.path()).unwrap();
    assert!(tree.get_by_path_str("/n").is_ok());
}
