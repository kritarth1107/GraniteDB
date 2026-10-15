# GraniteDB Architecture

GraniteDB is being rebuilt as an **embedded tree-architecture memory database** for AI agents. Agents store memories as nodes in a hierarchy (for example `root → project → session → episode → memory`). The engine’s job is to make that tree durable, fast to query as a subtree, and searchable by vector similarity *inside any subtree*.

This document is the design source of truth for the 15-day push. It is honest about what exists today and what is not yet built. No performance numbers are claimed until they are measured with Criterion.

---

## 1. Audit of the existing crate (day 0)

The repository on `master` was a document-oriented NoSQL sketch in the MongoDB style:

| Area | What is there | What it actually is |
|---|---|---|
| Data model | BSON-like `Document` + collections | Flat documents, no hierarchy, no paths |
| Persistence | `WriteAheadLog` + in-memory `HashMap` | WAL does not `fsync` the file (only `BufWriter::flush`). Recovery replays every entry. No atomic snapshot of the data set |
| Pages / buffer pool | Page headers, LRU pool, disk manager | Wired loosely; documents are not actually page-packed |
| Query / aggregation | Mongo-style operators and pipeline stages | In-memory filters over collections |
| Vector | HNSW + distance metrics | Document-id keyed, not subtree-scoped; not SIMD brute-force first |
| Transactions, RBAC, replication, sharding, TCP server | Modules exist | Early sketches. The crate did not compile on Rust 2024 (`gen` keyword, argon2 0.5 API, RLE parse, borrow errors) |
| Tests | Two tiny distance unit tests | No tree tests, no crash-recovery suite, no property tests, no CI |

**Keep.** Hierarchical *ideas* in comments, CRC32 length-prefixed WAL framing, HNSW as a later ANN backend, Apache-2.0 license, the `granitedb` crate name.

**Replace as the primary API.** Collections-of-documents as the thing users program against. The new primary type is `MemoryTree`.

**Defer.** Network server, Python bindings, HNSW-in-subtree, roll-up jobs, TTL eviction, Criterion numbers. They are on the roadmap, not day 1.

The legacy modules stay in the tree so later days can mine them (distance functions, HNSW, config). They are not the product.

---

## 2. Goals

An AI project should be able to:

1. **Insert** a memory under any parent (project, session, episode, …).
2. **Get** a node by id or by path (`/agent/session-12/episode-3/fact-7`).
3. **Move** a node (and its descendants) to a new parent without breaking paths.
4. **Delete a subtree** in one operation.
5. **Walk** children, ancestors, and the full subtree (preorder).
6. **Search by vector** inside a subtree (brute-force SIMD first, HNSW later).
7. **Roll summaries up** so a parent holds a compact view of its children.
8. **Survive a crash** via a write-ahead log plus atomic snapshots.
9. **Expire** stale memories with TTL and optional decay scores.
10. Expose an **excellent Rust API** first; a server and Python bindings after the engine is real.

Day 1 delivers (1)–(5) and (8), plus the data fields needed by (6), (7), and (9).

---

## 3. Node model

Every node has:

| Field | Role |
|---|---|
| `id` | Monotonic `NodeId` (`u64` newtype). Root is `NodeId(0)` |
| `parent_id` | `None` only for root |
| `path` | Canonical `NodePath` (`/` + `/`-separated names) |
| `name` | Last path segment; unique among siblings |
| `kind` | `Root`, `Project`, `Session`, `Episode`, `Memory`, `Summary`, or `Custom(String)` |
| `payload` | JSON value plus optional embedding (`Vec<f32>`) |
| `summary` | Optional rolled-up text (written now, computed later) |
| `created_at` / `updated_at` | UTC timestamps |
| `expires_at` | Optional TTL instant |
| `decay` | Optional score in `[0, 1]` for later eviction |
| `children` | Child ids in insertion order |

**Invariants** (checked by `MemoryTree::validate`):

- Exactly one root; it cannot be moved or deleted.
- `path_index[node.path] == node.id` for every node.
- A non-root node’s path is `parent.path.join(name)`.
- Parent/child lists agree in both directions.
- No cycles.
- Sibling names are unique.
- `next_id` is greater than every existing id.

Paths reject empty names, `.`, `..`, `/`, and NUL. Unicode is allowed so agents can use human labels.

Typical agent layout:

```
/                          (root)
└── research-bot           (project)
    └── session-2026-10-15 (session)
        ├── episode-1      (episode)
        │   ├── user-prefers-rust     (memory)
        │   └── deadline-is-friday    (memory)
        └── episode-1-summary         (summary, roll-up)
```

---

## 4. Engine layout

```
┌─────────────────────────────────────────────┐
│                 MemoryTree                  │
│  HashMap<NodeId, Node>                      │
│  HashMap<NodePath, NodeId>   (path index)   │
│  children stored on each node               │
└───────────────┬─────────────────────────────┘
                │ commit(op): validate → WAL → apply
        ┌───────┴────────┐
        │                │
        ▼                ▼
┌──────────────┐  ┌─────────────────┐
│  tree.wal    │  │  snapshot.bin   │
│  GTWL recs   │  │  atomic rename  │
│  CRC + fsync │  │  CRC + fsync    │
└──────────────┘  └─────────────────┘
```

Day 1 is single-writer, in-process. Concurrency (RwLock / single-writer queue) is a later day.

### 4.1 Write-ahead log

File: `<data_dir>/tree.wal`.

Record:

```
magic[4] = b"GTWL"
version u16le
rec_type u16le
lsn u64le
payload_len u32le
crc32le          # version..len + payload
payload          # bincode(TreeOp)
```

After each append the engine `write`s, `flush`es, and (by default) `sync_all`s the file. A torn tail (short read or bad CRC at end-of-file) is truncated on open; that is a crash, not corruption. A bad CRC in the middle of the file is corruption and fails open.

Logged ops: `Insert`, `Update`, `Move`, `Rename`, `DeleteSubtree`. Each carries the timestamps needed so replay does not call `now()`.

### 4.2 Snapshots

File: `<data_dir>/snapshot.bin`, written as `snapshot.bin.tmp` → `fsync` → `rename` → `fsync` directory.

Payload is a versioned `TreeSnapshot` (`lsn`, `root_id`, `next_id`, all nodes) with a CRC32. Recovery is: load snapshot if present, then replay WAL entries with `lsn > snapshot.lsn`. After a successful snapshot the WAL file is truncated. Replay is idempotent enough that a crash between snapshot and truncate simply skips already-snapshotted LSNs.

Auto-checkpoint every 256 mutations by default; callers can also call `checkpoint()` explicitly.

### 4.3 Indexes (roadmap)

| Index | Day 1 | Later |
|---|---|---|
| Primary (`NodeId`) | `HashMap` | same, then packed slab |
| Path | `HashMap<NodePath, NodeId>` | prefix/trie for glob |
| Subtree | walk child lists | DFS interval (Euler tour `in`/`out`) so “is in subtree?” is O(1) and range scans work |
| Vector | embedding stored on node | brute-force SIMD over subtree, then HNSW per tree / per hot subtree |

---

## 5. Query shapes

**Structural (day 1):** `get`, `get_by_path`, `children`, `ancestors`, `subtree` (preorder iterator).

**Later structural:** path globs (`/proj/**/memory`), filter by `kind`, depth bounds, payload predicates, “recent N in this subtree”.

**Vector (later):** `search_subtree(id, query_vec, k, metric)` — first exact inner-loop SIMD (dot / L2 / cosine), then HNSW built from the same embeddings, still *scoped to a subtree* (filter by Euler interval or per-subtree graphs).

**Roll-ups (later):** a parent may store a `summary`. A pluggable `Rollup` trait produces text (extractive concat first, optional LLM hook). Invalidated on child mutate; refreshed lazily or on `rollup(id)`.

**TTL / decay (later):** `expires_at` and `decay` are stored now. A sweeper will drop expired nodes and optionally decay scores on read. No silent data loss on day 1 — expired nodes remain until eviction is implemented and tested.

---

## 6. Rust API (target)

```rust
let mut tree = MemoryTree::open("./granite-data")?;
let root = tree.root_id();
let project = tree.insert_with(root, NodeSpec::new("agent").kind(NodeKind::Project))?;
tree.insert(project, "note", Payload::text("hello"))?;
tree.move_node(note_id, other_parent)?;
tree.delete_subtree(session_id)?;
for node in tree.subtree(project)? { /* … */ }
tree.checkpoint()?;
```

Ephemeral trees (`MemoryTree::ephemeral()`) skip the disk and are for tests.

---

## 7. Durability and testing policy

- **Unit tests** for every structural op, including reject cases (duplicate sibling name, move into own subtree, delete root).
- **At least one property test:** random insert/move/delete sequences preserve invariants; durable reopen equals in-memory state.
- **Crash suite (later day):** torn WAL tail, kill after WAL-before-snapshot, kill after snapshot-before-truncate, directory fsync, large trees.
- **Criterion (later day):** insert, subtree walk, reopen, vector search. Publish numbers only from those benches.
- **CI:** `fmt --check`, `clippy -D warnings`, `test`, on stable, Ubuntu and macOS.
- Releases stay **beta** (`0.x.y-beta.N`, GitHub prerelease) until crash tests, benches, and a real workload say otherwise.

---

## 8. Fifteen-day roadmap

Dates are sequencing, not a calendar promise. Each day should land on `master` with tests.

| Day | Theme | Deliverable |
|---|---|---|
| **1** | Core tree + durability + project spine | `MemoryTree` ops, WAL + snapshot, unit + property tests, CI, release workflow, README, `0.1.0-beta.1` |
| **2** | Subtree query API | Filters by kind/depth/payload, path glob, DFS interval numbers |
| **3** | Brute-force SIMD vectors | `search_subtree` with cosine / L2 / dot, auto-vectorized inner loop, correctness tests vs scalar |
| **4** | HNSW in a subtree | Incremental insert/delete, recall tests vs brute-force, scoped to Euler interval |
| **5** | Roll-ups | `Rollup` trait, extractive default, invalidate-on-write, `summary` on parents |
| **6** | TTL and decay | Sweeper, lazy expiry on read, decay function, tests that expired nodes vanish only after sweep |
| **7** | Storage hardening | WAL segment rotation, snapshot catalog, compaction of deleted ids, checksum report |
| **8** | Criterion | Benches for insert, walk, recovery, vector k-NN; CI job stores artifacts; still no slogan numbers in the README unless measured |
| **9** | Crash-recovery suite | Fault injection: torn writes, truncated snapshots, `kill` mid-checkpoint; must reopen cleanly |
| **10** | Concurrency | Single-writer `MemoryTreeHandle`, `parking_lot` RwLock for readers, no data races (loom or thread tests) |
| **11** | Server | Tokio server, tree-oriented JSON protocol (`insert`, `subtree`, `search`), keep the old document protocol out of the default path |
| **12** | Python | PyO3 module, numpy embeddings, context-manager open/close |
| **13** | Agent recall API | “what do we know under this project?”, recency + similarity mix, example agent |
| **14** | Performance pass | Node slab/arena, cache-friendly walk, mmap snapshot read, SIMD kernels |
| **15** | Beta hardening | Docs, examples, production checklist. Still a beta tag unless day 8–9 results justify otherwise |

Out of scope for these 15 days: multi-raft replication, sharding, a Mongo compatibility layer, and claiming “production-grade.”

---

## 9. Day-1 file map

```
docs/ARCHITECTURE.md          this file
src/memory/mod.rs             module root
src/memory/id.rs              NodeId
src/memory/path.rs            NodePath
src/memory/node.rs            Node, NodeKind, Payload, NodeSpec
src/memory/wal.rs             crash-safe WAL
src/memory/snapshot.rs        atomic snapshot
src/memory/tree.rs            MemoryTree
tests/memory_tree.rs          unit tests
tests/durability.rs           reopen / torn-tail tests
tests/invariants.rs           proptest
.github/workflows/ci.yml
.github/workflows/release.yml
```

---

## 10. Non-goals for day 1

- Network protocol and CLI against the tree (the old TCP server binary still boots the document sketch).
- Vector search and HNSW wiring into `MemoryTree`.
- Background roll-up / TTL threads.
- Benchmark claims.
- Multi-threaded writers.
