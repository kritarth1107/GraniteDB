# 🪨 GraniteDB

<div align="center">

**A blazing-fast, document-oriented NoSQL database engine built from scratch in Rust.**

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)
[![Status](https://img.shields.io/badge/status-alpha-yellow.svg)]()

*MongoDB-like flexibility · Next-gen performance · 100% open source*

</div>

---

## ✨ Features

### 🔥 Core Engine
- **Document-Based Data Model** — Rich BSON-like value types (strings, numbers, arrays, embedded documents, dates, binary, regex, and more)
- **Schema Validation** — Optional JSON-Schema-like validation with type checks, range/length constraints, regex patterns, enum values, and nested schemas
- **Write-Ahead Logging (WAL)** — Segmented WAL with CRC32 checksums per entry, LSN tracking, and automatic segment rolling for crash-proof durability
- **Buffer Pool** — LRU-based page cache with pin/unpin eviction and performance counters
- **Page-Based Storage** — Fixed-size pages with integrity verification and overflow support

### 🔍 Query Engine
- **MongoDB-Style Queries** — Full support for `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$regex`, `$type`, `$elemMatch`
- **Logical Operators** — `$and`, `$or`, `$not`, `$nor` for complex filter compositions
- **Query Planner** — Automatic strategy selection: ID lookup → Index scan → Collection scan
- **Projections** — Return only the fields you need
- **Sort, Skip & Limit** — Full pagination support
- **Update Operators** — `$set`, `$unset`, `$inc`, `$push`, `$pull`, `$rename`
- **Explain Plans** — Debug and optimize your queries

### 📊 Aggregation Pipeline
- **12 Pipeline Stages** — `$match`, `$project`, `$group`, `$sort`, `$limit`, `$skip`, `$unwind`, `$count`, `$addFields`, `$replaceRoot`, `$lookup`, `$out`
- **9 Accumulators** — `$sum`, `$avg`, `$min`, `$max`, `$count`, `$push`, `$addToSet`, `$first`, `$last`
- **JSON Pipeline Parser** — Build pipelines from JSON, just like MongoDB

### 🗂️ Indexing
- **B-Tree Indexes** — Ordered indexes for range queries with composite key support
- **Hash Indexes** — O(1) exact-match lookups
- **Unique Constraints** — Enforce uniqueness across indexed fields
- **Sparse Indexes** — Skip documents missing the indexed field
- **Index Manager** — Create, drop, and auto-maintain indexes on insert/update/delete

### 🔒 Security
- **Role-Based Access Control (RBAC)** — 5 built-in roles (`read`, `readWrite`, `dbAdmin`, `userAdmin`, `root`) + custom roles with 11 action types
- **Argon2 Password Hashing** — Industry-standard password security
- **AES-256-GCM Encryption at Rest** — Transparent data encryption with random nonce generation
- **User Management** — Create, authenticate, grant/revoke roles, enable/disable accounts

### 🔄 Transactions
- **ACID Transactions** — Begin/commit/abort with automatic conflict detection
- **Multiple Isolation Levels** — Read Uncommitted, Read Committed, Repeatable Read, Snapshot, Serializable
- **MVCC** — Multi-Version Concurrency Control with snapshot reads and garbage collection
- **Timeout Enforcement** — Automatic transaction abort on timeout

### 🌐 Networking
- **Async TCP Server** — Built on Tokio with per-connection task spawning
- **JSON Wire Protocol** — 25+ command types covering all database operations
- **Connection Pool** — Configurable max connections with tracking and lifecycle management
- **Interactive CLI** — MongoDB-like shell with commands: `use`, `insert`, `find`, `count`, `delete`, etc.

### 📡 Replication & Sharding
- **Oplog-Based Replication** — Capped operations log with timestamp-based sync queries
- **Replica Sets** — Primary/Secondary/Arbiter roles with heartbeat monitoring
- **Consistent Hashing** — Shard router with virtual nodes for even data distribution
- **Key Range Sharding** — Automatic routing based on shard key values

### 📈 Monitoring
- **17 Atomic Metrics** — Queries, inserts, updates, deletes, connections, bytes I/O, WAL writes, buffer pool hits/misses, index lookups, collection scans, transactions
- **Server Status** — Real-time statistics via the wire protocol

---

## 📁 Project Structure

```
GraniteDB/
├── Cargo.toml                      # Dependencies & build config
├── src/
│   ├── lib.rs                      # Library root (17 module re-exports)
│   ├── main.rs                     # Server entry point (CLI + boot)
│   │
│   ├── config/                     # ⚙️ Configuration
│   │   ├── mod.rs
│   │   └── settings.rs             # Server, storage, auth, replication settings
│   │
│   ├── error/                      # ❌ Error Types
│   │   └── mod.rs                  # 30+ typed error variants
│   │
│   ├── document/                   # 📄 Document Model
│   │   ├── mod.rs
│   │   ├── bson.rs                 # BSON-like value types with comparisons
│   │   ├── document.rs             # Document struct with metadata & TTL
│   │   └── validation.rs           # Schema validation engine
│   │
│   ├── storage/                    # 💾 Storage Engine
│   │   ├── mod.rs
│   │   ├── engine.rs               # Unified storage (WAL + memory + disk)
│   │   ├── wal.rs                  # Write-Ahead Log (segmented, checksummed)
│   │   ├── page.rs                 # Fixed-size page management
│   │   ├── buffer_pool.rs          # LRU page cache
│   │   └── disk.rs                 # Low-level disk I/O
│   │
│   ├── collection/                 # 📦 Collections
│   │   ├── mod.rs
│   │   └── collection.rs           # CRUD + operator queries + capped collections
│   │
│   ├── database/                   # 🗄️ Database Management
│   │   ├── mod.rs
│   │   └── database.rs             # Multi-collection container + stats
│   │
│   ├── query/                      # 🔍 Query Engine
│   │   ├── mod.rs
│   │   ├── parser.rs               # JSON → FilterExpr + UpdateOperation
│   │   ├── filter.rs               # Filter expression tree
│   │   ├── planner.rs              # Strategy selection + explain
│   │   └── executor.rs             # Execution with sort/skip/limit/projection
│   │
│   ├── index/                      # 🗂️ Indexing
│   │   ├── mod.rs
│   │   ├── btree.rs                # B-Tree index (range + composite)
│   │   ├── hash_index.rs           # Hash index (O(1) lookups)
│   │   └── manager.rs              # Index lifecycle management
│   │
│   ├── aggregation/                # 📊 Aggregation
│   │   ├── mod.rs
│   │   ├── stages.rs               # Stage & accumulator definitions
│   │   └── pipeline.rs             # Pipeline executor + JSON parser
│   │
│   ├── transaction/                # 🔄 Transactions
│   │   ├── mod.rs
│   │   ├── manager.rs              # Begin/commit/abort + conflict detection
│   │   └── mvcc.rs                 # Multi-Version Concurrency Control
│   │
│   ├── auth/                       # 🔒 Authentication & Security
│   │   ├── mod.rs
│   │   ├── user.rs                 # User management (Argon2 hashing)
│   │   ├── rbac.rs                 # Role-Based Access Control
│   │   └── encryption.rs           # AES-256-GCM encryption at rest
│   │
│   ├── network/                    # 🌐 Networking
│   │   ├── mod.rs
│   │   ├── server.rs               # Async TCP server (Tokio)
│   │   ├── protocol.rs             # JSON wire protocol (25+ commands)
│   │   ├── handler.rs              # Request routing & execution
│   │   └── connection.rs           # Connection tracking & pool
│   │
│   ├── replication/                # 📡 Replication
│   │   ├── mod.rs
│   │   ├── oplog.rs                # Capped operations log
│   │   └── replica.rs              # Replica set management
│   │
│   ├── sharding/                   # 🔀 Sharding
│   │   ├── mod.rs
│   │   ├── shard.rs                # Shard definition + key ranges
│   │   └── router.rs               # Consistent hashing router
│   │
│   ├── cursor/                     # 📜 Cursors
│   │   ├── mod.rs
│   │   └── cursor.rs               # Batch iteration over results
│   │
│   ├── metrics/                    # 📈 Monitoring
│   │   ├── mod.rs
│   │   └── collector.rs            # 17 atomic performance counters
│   │
│   ├── utils/                      # 🔧 Utilities
│   │   ├── mod.rs
│   │   └── helpers.rs              # ID gen, hashing, formatting
│   │
│   └── cli/                        # 💻 CLI Client
│       └── main.rs                 # Interactive shell client
```

---

## 🚀 Quick Start

### Prerequisites
- **Rust** ≥ 1.85 (2024 edition)

### Build & Run

```bash
# Clone
git clone https://github.com/kritarth1107/GraniteDB.git
cd GraniteDB

# Build
cargo build --release

# Start the server
cargo run --release -- --port 6380 --data-dir ./data

# In another terminal, connect with the CLI
cargo run --release --bin granite-cli
```

### CLI Usage

```
granite:default> use mydb
Switched to database: mydb

granite:mydb> createcol users

granite:mydb> insert users {"name": "John", "age": 30, "email": "john@example.com"}

granite:mydb> find users {"age": {"$gt": 25}}

granite:mydb> count users

granite:mydb> delete users {"name": "John"}
```

### Wire Protocol (TCP)

Connect via TCP and send JSON commands:

```json
{
  "request_id": "abc-123",
  "command": {
    "type": "insert_one",
    "database": "mydb",
    "collection": "users",
    "document": {"name": "Alice", "age": 28}
  }
}
```

---

## 📋 Configuration

Create a `granitedb.json` file:

```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 6380,
    "max_connections": 10000
  },
  "storage": {
    "data_dir": "./data/granite",
    "page_size": 16384,
    "buffer_pool_pages": 4096,
    "wal_fsync": true
  },
  "auth": {
    "enabled": false
  },
  "logging": {
    "level": "info"
  }
}
```

---

## 🏗️ Architecture

```
                    ┌──────────────┐
                    │  CLI Client  │
                    └──────┬───────┘
                           │ TCP/JSON
                    ┌──────▼───────┐
                    │  TCP Server  │   ← Tokio async, connection pool
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │   Handler    │   ← Request routing, auth checks
                    └──────┬───────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
     ┌──────▼──────┐ ┌────▼────┐ ┌───────▼──────┐
     │   Database  │ │  Query  │ │  Aggregation │
     │  Manager    │ │ Engine  │ │   Pipeline   │
     └──────┬──────┘ └────┬────┘ └───────┬──────┘
            │              │              │
     ┌──────▼──────────────▼──────────────▼──────┐
     │              Collection Manager            │
     │         (CRUD + Schema Validation)         │
     └─────────────────┬─────────────────────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
  ┌──────▼──────┐ ┌───▼────┐ ┌──────▼──────┐
  │   Storage   │ │  Index │ │ Transaction │
  │   Engine    │ │Manager │ │   Manager   │
  └──────┬──────┘ └────────┘ └─────────────┘
         │
    ┌────┼────┐
    │    │    │
  ┌─▼─┐┌▼──┐┌▼────────┐
  │WAL││Buf ││  Disk   │
  │   ││Pool││ Manager │
  └───┘└────┘└─────────┘
```

---

## 🤝 Contributing

We welcome contributions! GraniteDB is 100% open source under the Apache 2.0 license.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📜 License

This project is licensed under the **Apache License 2.0** — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Built with ❤️ and Rust 🦀**

*GraniteDB — The database that's solid as granite.*

</div>