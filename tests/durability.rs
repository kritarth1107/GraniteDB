//! Crash-safety tests: reopen, snapshot, torn WAL tail.

use granitedb::{MemoryTree, NodeKind, NodeSpec, Payload, TreeOpenOptions};
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
