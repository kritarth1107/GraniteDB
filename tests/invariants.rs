//! Property tests: random trees keep invariants and round-trip through disk.

use granitedb::{MemoryTree, NodeId, Payload};
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum RawOp {
    Insert { parent_slot: u8, name: String },
    Move { node_slot: u8, parent_slot: u8 },
    Rename { node_slot: u8, name: String },
    Delete { node_slot: u8 },
}

fn arb_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9]{0,5}"
}

fn arb_ops(max: usize) -> impl Strategy<Value = Vec<RawOp>> {
    let insert = (any::<u8>(), arb_name())
        .prop_map(|(parent_slot, name)| RawOp::Insert { parent_slot, name });
    let mv = (any::<u8>(), any::<u8>()).prop_map(|(node_slot, parent_slot)| RawOp::Move {
        node_slot,
        parent_slot,
    });
    let rename =
        (any::<u8>(), arb_name()).prop_map(|(node_slot, name)| RawOp::Rename { node_slot, name });
    let delete = any::<u8>().prop_map(|node_slot| RawOp::Delete { node_slot });
    proptest::collection::vec(prop_oneof![insert, mv, rename, delete], 1..=max)
}

fn live_ids(tree: &MemoryTree) -> Vec<NodeId> {
    tree.iter().map(|n| n.id).collect()
}

/// Ancestor-walk membership: `cand` is `root` or has `root` as an ancestor.
fn walk_in_subtree(tree: &MemoryTree, root: NodeId, cand: NodeId) -> bool {
    if root == cand {
        return true;
    }
    tree.ancestors(cand)
        .unwrap()
        .iter()
        .any(|node| node.id == root)
}

fn apply_raw(tree: &mut MemoryTree, op: &RawOp) {
    let ids = live_ids(tree);
    match op {
        RawOp::Insert { parent_slot, name } => {
            let parent = ids[(*parent_slot as usize) % ids.len()];
            let _ = tree.insert(parent, name.clone(), Payload::empty());
        }
        RawOp::Move {
            node_slot,
            parent_slot,
        } => {
            let node = ids[(*node_slot as usize) % ids.len()];
            let parent = ids[(*parent_slot as usize) % ids.len()];
            let _ = tree.move_node(node, parent);
        }
        RawOp::Rename { node_slot, name } => {
            let node = ids[(*node_slot as usize) % ids.len()];
            let _ = tree.rename(node, name.clone());
        }
        RawOp::Delete { node_slot } => {
            let node = ids[(*node_slot as usize) % ids.len()];
            let _ = tree.delete_subtree(node);
        }
    }
}

fn apply_all(ops: &[RawOp]) -> MemoryTree {
    let mut tree = MemoryTree::ephemeral();
    for op in ops {
        apply_raw(&mut tree, op);
        tree.validate()
            .unwrap_or_else(|e| panic!("invariant broken after {op:?}: {e}"));
    }
    tree
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn random_ops_preserve_invariants(ops in arb_ops(40)) {
        let tree = apply_all(&ops);
        prop_assert!(tree.get(tree.root_id()).is_ok());
        prop_assert_eq!(tree.iter().count(), tree.len());
        // Every non-root node is reachable from root.
        let from_root: Vec<_> = tree.subtree(tree.root_id()).unwrap().map(|n| n.id).collect();
        prop_assert_eq!(from_root.len(), tree.len());
    }

    #[test]
    fn interval_membership_matches_ancestor_walk(ops in arb_ops(40)) {
        let tree = apply_all(&ops);
        let ids = live_ids(&tree);
        for &root in &ids {
            for &cand in &ids {
                let by_interval = tree.in_subtree(root, cand).expect("both ids live");
                let by_walk = walk_in_subtree(&tree, root, cand);
                prop_assert_eq!(
                    by_interval,
                    by_walk,
                    "interval vs walk mismatch for root={} cand={}",
                    root,
                    cand
                );
            }
        }
    }

    #[test]
    fn durable_reopen_equals_ephemeral(ops in arb_ops(24)) {
        let ephemeral = apply_all(&ops);
        let dir = tempfile::tempdir().unwrap();
        let mut durable = granitedb::TreeOpenOptions::new()
            .fsync(false)
            .auto_checkpoint_every(None)
            .open(dir.path())
            .unwrap();
        for op in &ops {
            apply_raw(&mut durable, op);
        }
        durable.validate().unwrap();
        drop(durable);

        let reopened = MemoryTree::open(dir.path()).unwrap();
        reopened.validate().unwrap();
        prop_assert_eq!(reopened.len(), ephemeral.len());
        for node in ephemeral.iter() {
            let got = reopened.get(node.id).expect("id missing after reopen");
            prop_assert_eq!(&got.path, &node.path);
            prop_assert_eq!(&got.name, &node.name);
            prop_assert_eq!(got.parent_id, node.parent_id);
            prop_assert_eq!(got.child_ids(), node.child_ids());
        }
    }
}
