//! Minimal agent-memory example: a project, a session, and one fact.

use granitedb::{MemoryTree, NodeKind, NodeSpec, Payload};

fn main() -> granitedb::GraniteResult<()> {
    let dir = std::env::temp_dir().join("granitedb-agent-memory-example");
    let mut tree = MemoryTree::open(&dir)?;

    let root = tree.root_id();
    let project = tree.insert_with(root, NodeSpec::new("demo").kind(NodeKind::Project))?;
    let session = tree.insert_with(project, NodeSpec::new("s1").kind(NodeKind::Session))?;
    tree.insert(
        session,
        "prefers-rust",
        Payload::text("The user prefers Rust for systems work"),
    )?;

    println!("nodes: {}", tree.len());
    for node in tree.subtree(project)? {
        println!("{}  {}", node.id, node.path);
    }

    tree.checkpoint()?;
    Ok(())
}
