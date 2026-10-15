//! Unit tests for MemoryTree structural operations.

use granitedb::{GraniteError, MemoryTree, NodeId, NodeKind, NodePath, NodeSpec, Payload};

fn sample_tree() -> (MemoryTree, NodeId, NodeId, NodeId) {
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
    (tree, project, session, mem)
}

#[test]
fn root_exists_on_empty_tree() {
    let tree = MemoryTree::ephemeral();
    assert_eq!(tree.len(), 1);
    assert!(tree.is_root_only());
    let root = tree.get(tree.root_id()).unwrap();
    assert!(root.is_root());
    assert_eq!(root.path.to_string(), "/");
    assert_eq!(root.kind, NodeKind::Root);
    tree.validate().unwrap();
}

#[test]
fn insert_get_and_path_index() {
    let (tree, project, session, mem) = sample_tree();
    assert_eq!(tree.get_by_path_str("/agent").unwrap().id, project);
    assert_eq!(tree.get_by_path_str("/agent/s1").unwrap().id, session);
    assert_eq!(
        tree.get_by_path_str("/agent/s1/prefers-rust").unwrap().id,
        mem
    );
    assert_eq!(
        tree.get(mem).unwrap().payload.data["text"],
        serde_json::json!("user likes rust")
    );
    tree.validate().unwrap();
}

#[test]
fn duplicate_sibling_name_is_rejected() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    tree.insert(root, "a", Payload::empty()).unwrap();
    let err = tree.insert(root, "a", Payload::empty()).unwrap_err();
    assert!(matches!(err, GraniteError::DuplicateChildName { .. }));
}

#[test]
fn invalid_names_are_rejected() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    assert!(tree.insert(root, "", Payload::empty()).is_err());
    assert!(tree.insert(root, "..", Payload::empty()).is_err());
    assert!(tree.insert(root, "has/slash", Payload::empty()).is_err());
    assert!(NodePath::parse("no-leading-slash").is_err());
    assert_eq!(NodePath::parse("/").unwrap().to_string(), "/");
    assert_eq!(
        NodePath::parse("/a/b")
            .unwrap()
            .join("c")
            .unwrap()
            .to_string(),
        "/a/b/c"
    );
}

#[test]
fn children_are_in_insertion_order() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let a = tree.insert(root, "a", Payload::empty()).unwrap();
    let b = tree.insert(root, "b", Payload::empty()).unwrap();
    let c = tree.insert(root, "c", Payload::empty()).unwrap();
    let ids: Vec<_> = tree.children(root).unwrap().iter().map(|n| n.id).collect();
    assert_eq!(ids, vec![a, b, c]);
}

#[test]
fn ancestors_walk_to_root() {
    let (tree, project, session, mem) = sample_tree();
    let ids: Vec<_> = tree.ancestors(mem).unwrap().iter().map(|n| n.id).collect();
    assert_eq!(ids, vec![session, project, tree.root_id()]);
    assert!(tree.ancestors(tree.root_id()).unwrap().is_empty());
}

#[test]
fn subtree_is_preorder_and_includes_self() {
    let (tree, project, session, mem) = sample_tree();
    let ids: Vec<_> = tree.subtree(project).unwrap().map(|n| n.id).collect();
    assert_eq!(ids, vec![project, session, mem]);
    assert_eq!(tree.subtree(mem).unwrap().count(), 1);
}

#[test]
fn move_rewrites_descendant_paths() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let p1 = tree
        .insert_with(root, NodeSpec::new("p1").kind(NodeKind::Project))
        .unwrap();
    let p2 = tree
        .insert_with(root, NodeSpec::new("p2").kind(NodeKind::Project))
        .unwrap();
    let session = tree
        .insert_with(p1, NodeSpec::new("s").kind(NodeKind::Session))
        .unwrap();
    let mem = tree.insert(session, "fact", Payload::text("x")).unwrap();

    tree.move_node(session, p2).unwrap();
    assert_eq!(tree.get(session).unwrap().parent_id, Some(p2));
    assert_eq!(tree.get(session).unwrap().path.to_string(), "/p2/s");
    assert_eq!(tree.get(mem).unwrap().path.to_string(), "/p2/s/fact");
    assert!(tree.get_by_path_str("/p1/s").is_err());
    assert_eq!(tree.children(p1).unwrap().len(), 0);
    assert_eq!(tree.children(p2).unwrap()[0].id, session);
    tree.validate().unwrap();
}

#[test]
fn move_into_own_subtree_is_rejected() {
    let (mut tree, project, session, _) = sample_tree();
    let err = tree.move_node(project, session).unwrap_err();
    assert!(matches!(err, GraniteError::CycleDetected(_)));
}

#[test]
fn cannot_move_or_delete_root() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let child = tree.insert(root, "c", Payload::empty()).unwrap();
    assert!(matches!(
        tree.move_node(root, child).unwrap_err(),
        GraniteError::CannotMoveRoot
    ));
    assert!(matches!(
        tree.delete_subtree(root).unwrap_err(),
        GraniteError::CannotDeleteRoot
    ));
    assert!(matches!(
        tree.rename(root, "nope").unwrap_err(),
        GraniteError::CannotRenameRoot
    ));
}

#[test]
fn rename_updates_path() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let a = tree.insert(root, "old", Payload::empty()).unwrap();
    tree.insert(a, "leaf", Payload::empty()).unwrap();
    tree.rename(a, "new").unwrap();
    assert_eq!(tree.get(a).unwrap().path.to_string(), "/new");
    assert_eq!(
        tree.get_by_path_str("/new/leaf").unwrap().parent_id,
        Some(a)
    );
    tree.validate().unwrap();
}

#[test]
fn delete_subtree_removes_descendants() {
    let (mut tree, project, session, mem) = sample_tree();
    let removed = tree.delete_subtree(session).unwrap();
    assert_eq!(removed, 2);
    assert!(tree.get(session).is_err());
    assert!(tree.get(mem).is_err());
    assert!(tree.get(project).is_ok());
    assert!(tree.children(project).unwrap().is_empty());
    tree.validate().unwrap();
}

#[test]
fn update_payload_and_summary() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let id = tree.insert(root, "n", Payload::text("one")).unwrap();
    tree.set_payload(id, Payload::text("two")).unwrap();
    tree.set_summary(id, Some("rolled up".into())).unwrap();
    let node = tree.get(id).unwrap();
    assert_eq!(node.payload.data["text"], serde_json::json!("two"));
    assert_eq!(node.summary.as_deref(), Some("rolled up"));
}

#[test]
fn missing_node_is_an_error() {
    let tree = MemoryTree::ephemeral();
    let err = tree.get(NodeId::from_raw(99)).unwrap_err();
    assert!(matches!(err, GraniteError::NodeNotFound(_)));
}

#[test]
fn ttl_and_decay_are_stored() {
    let mut tree = MemoryTree::ephemeral();
    let root = tree.root_id();
    let expires = chrono::Utc::now() + chrono::Duration::hours(1);
    let id = tree
        .insert_with(
            root,
            NodeSpec::new("ephemeral-fact")
                .expires_at(expires)
                .decay(0.4)
                .embedding(vec![0.1, 0.2, 0.3]),
        )
        .unwrap();
    let node = tree.get(id).unwrap();
    assert!(!node.is_expired(chrono::Utc::now()));
    assert_eq!(node.decay, Some(0.4));
    assert_eq!(node.payload.embedding.as_ref().unwrap().len(), 3);
}

#[test]
fn iter_visits_every_node() {
    let (tree, _, _, _) = sample_tree();
    assert_eq!(tree.iter().count(), tree.len());
}
