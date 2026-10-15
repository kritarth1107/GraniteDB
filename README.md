# GraniteDB

**A tree-architecture memory database for AI agents**, written in Rust.

Agents store memories in a hierarchy — for example `root → project → session → episode → memory` — and query them as subtrees. GraniteDB keeps that tree crash-safe on disk (write-ahead log + snapshots) and exposes a small, explicit Rust API.

[![CI](https://github.com/kritarth1107/GraniteDB/actions/workflows/ci.yml/badge.svg)](https://github.com/kritarth1107/GraniteDB/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-beta-yellow.svg)](https://github.com/kritarth1107/GraniteDB/releases)

> **Beta.** Current releases are tagged `*-beta.*` and published as GitHub prereleases. The engine is not production-grade yet. Do not use it as the only copy of data you cannot rebuild.

---

## What it is

| You have | GraniteDB gives you |
|---|---|
| Agent / project / session / episode memories | Nodes with stable ids and canonical paths (`/agent/session-12/fact-3`) |
| “Everything under this session” | `children`, `ancestors`, and preorder `subtree` walks |
| Process crash | WAL (`tree.wal`) plus atomic `snapshot.bin` |
| Embeddings (later) | Optional `Vec<f32>` on each payload; subtree vector search is on the roadmap |
| Parent summaries (later) | `summary` field on every node; roll-up jobs are on the roadmap |

This crate used to be described as a document NoSQL database. That sketch is still in the repository; **the product is the memory tree.** See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design and the 15-day plan.

No latency or throughput numbers are published here. Benches land later; claims will follow measurements.

---

## Status (0.1.0-beta.1)

**In this release**

- `MemoryTree`: insert, get (id and path), move, rename, delete-subtree, children, ancestors, subtree iteration
- Node model: `NodeId`, `NodePath`, `NodeKind`, `Payload` (JSON + optional embedding), TTL / decay fields
- Durable open: crash-safe WAL with CRC32 and `fsync`, atomic snapshots, torn-tail recovery
- Tests: unit tests, durability tests, a proptest over random insert/move/rename/delete
- CI on `stable` for Ubuntu and macOS (`fmt`, `clippy -D warnings`, `test`)

**Not in this release** (roadmap)

- Vector similarity search inside a subtree (brute-force SIMD, then HNSW)
- Automatic summary roll-ups
- TTL eviction / decay sweeper
- Criterion benches and a kill-9 crash suite
- Dedicated tree server and Python bindings

---

## Quick start

Requires **Rust 1.85+** (edition 2024).

```bash
git clone https://github.com/kritarth1107/GraniteDB.git
cd GraniteDB
cargo test
cargo run --example agent_memory
```

### Library

```rust
use granitedb::{MemoryTree, NodeKind, NodeSpec, Payload};

fn main() -> granitedb::GraniteResult<()> {
    let mut tree = MemoryTree::open("./granite-data")?;

    let root = tree.root_id();
    let project = tree.insert_with(
        root,
        NodeSpec::new("research-bot").kind(NodeKind::Project),
    )?;
    let session = tree.insert_with(
        project,
        NodeSpec::new("session-42").kind(NodeKind::Session),
    )?;
    tree.insert(
        session,
        "prefers-rust",
        Payload::text("The user prefers Rust for systems work"),
    )?;

    for node in tree.subtree(project)? {
        println!("{} {}", node.id, node.path);
    }

    tree.checkpoint()?;
    Ok(())
}
```

`MemoryTree::ephemeral()` builds the same tree in RAM only (useful in tests). `TreeOpenOptions` controls `fsync` and auto-checkpoint interval.

On disk a data directory looks like:

```
granite-data/
  snapshot.bin    # last checkpoint (atomic rename)
  tree.wal        # CRC-framed operations since that checkpoint
```

---

## Roadmap

Full write-up: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

1. **Day 1 (this tag)** — node model, tree ops, WAL + snapshot, tests, CI, beta release
2. Subtree query API (kind / depth / path glob)
3. Brute-force SIMD vector search inside a subtree
4. HNSW, still subtree-scoped
5. Summary roll-ups
6. TTL and decay eviction
7. WAL rotation and compaction
8. Criterion benches
9. Crash-recovery fault-injection suite
10. Single-writer concurrent handle
11. Tree-oriented server
12. Python (PyO3) bindings
13. Agent recall helper
14. Layout / SIMD performance pass
15. Beta hardening — still a beta tag unless days 8–9 say otherwise

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
