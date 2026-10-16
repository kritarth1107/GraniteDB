use super::glob::PathGlob;
use super::id::NodeId;
use super::node::{Node, NodeKind, NodeSpec, Payload};
use super::path::NodePath;
use super::query::SubtreeQuery;
use super::snapshot::{self, TreeSnapshot};
use super::wal::{TreeOp, TreeWal};
use crate::error::{GraniteError, GraniteResult};
use crate::vector::distance::DistanceMetric;
use crate::vector::hnsw::{HnswConfig, HnswIndex};
use crate::vector::kernels::QueryKernel;
use chrono::Utc;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::path::{Path, PathBuf};

/// Euler-tour enter/leave times for one node.
///
/// Assigned by a DFS walk that writes `dfs_in` on enter and `dfs_out` on
/// leave. `X` is in the subtree of `Y` when
/// `Y.dfs_in <= X.dfs_in && X.dfs_out <= Y.dfs_out`. The numbers are derived
/// from child order and are rebuilt after insert, move, and delete_subtree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DfsInterval {
    /// Tick when the walk entered this node.
    pub dfs_in: u32,
    /// Tick when the walk left this node.
    pub dfs_out: u32,
}

impl DfsInterval {
    /// True when `other` is this interval or a nested descendant interval.
    pub fn contains(self, other: Self) -> bool {
        self.dfs_in <= other.dfs_in && other.dfs_out <= self.dfs_out
    }
}

/// One neighbor from [`MemoryTree::search_subtree`] / [`MemoryTree::search_subtree_ann`].
#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
    pub id: NodeId,
    pub distance: f32,
}

/// Max-heap entry for exact k-NN: worst (largest distance, then largest id) on top.
#[derive(Clone, Copy, Debug)]
struct HeapHit {
    distance: f32,
    id: NodeId,
}

impl PartialEq for HeapHit {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapHit {}

impl PartialOrd for HeapHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
            .then(self.id.cmp(&other.id))
    }
}

/// Options for opening a durable [`MemoryTree`].
#[derive(Clone, Debug)]
pub struct TreeOpenOptions {
    fsync: bool,
    auto_checkpoint_every: Option<u64>,
}

impl Default for TreeOpenOptions {
    fn default() -> Self {
        Self {
            fsync: true,
            auto_checkpoint_every: Some(256),
        }
    }
}

impl TreeOpenOptions {
    /// Default options: fsync on, auto-checkpoint every 256 ops.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether each WAL append calls `sync_all` (default `true`).
    pub fn fsync(mut self, yes: bool) -> Self {
        self.fsync = yes;
        self
    }

    /// Take a snapshot every `n` mutations. `None` disables auto-checkpoint.
    pub fn auto_checkpoint_every(mut self, n: Option<u64>) -> Self {
        self.auto_checkpoint_every = n;
        self
    }

    /// Open (or create) a durable tree at `path`.
    pub fn open(self, path: impl AsRef<Path>) -> GraniteResult<MemoryTree> {
        MemoryTree::open_with(path.as_ref(), self)
    }
}

/// An in-memory tree of memory nodes, optionally backed by a WAL + snapshot.
pub struct MemoryTree {
    nodes: HashMap<NodeId, Node>,
    path_index: HashMap<NodePath, NodeId>,
    /// Euler-tour intervals, keyed by node id. Rebuilt after structural ops.
    intervals: HashMap<NodeId, DfsInterval>,
    /// `dfs_in → NodeId` so a subtree is the range `[root.dfs_in, root.dfs_out]`.
    interval_index: BTreeMap<u32, NodeId>,
    root_id: NodeId,
    next_id: u64,
    wal: Option<TreeWal>,
    hnsw: HnswIndex,
    data_dir: Option<PathBuf>,
    ops_since_checkpoint: u64,
    auto_checkpoint_every: Option<u64>,
}

impl MemoryTree {
    /// In-memory tree with no durability.
    pub fn ephemeral() -> Self {
        Self::empty()
    }

    /// Open a durable tree, creating `path` if needed. Fsyncs by default.
    pub fn open(path: impl AsRef<Path>) -> GraniteResult<Self> {
        TreeOpenOptions::default().open(path)
    }

    fn empty() -> Self {
        let mut tree = Self {
            nodes: HashMap::new(),
            path_index: HashMap::new(),
            intervals: HashMap::new(),
            interval_index: BTreeMap::new(),
            root_id: NodeId::ROOT,
            next_id: 1,
            wal: None,
            hnsw: HnswIndex::new(HnswConfig::default()),
            data_dir: None,
            ops_since_checkpoint: 0,
            auto_checkpoint_every: None,
        };
        tree.install_root();
        tree
    }

    fn install_root(&mut self) {
        let now = Utc::now();
        let root = Node {
            id: NodeId::ROOT,
            parent_id: None,
            path: NodePath::root(),
            name: String::new(),
            kind: NodeKind::Root,
            payload: Payload::empty(),
            summary: None,
            created_at: now,
            updated_at: now,
            expires_at: None,
            decay: None,
            children: Vec::new(),
        };
        self.path_index.insert(root.path.clone(), root.id);
        self.nodes.insert(root.id, root);
        self.root_id = NodeId::ROOT;
        self.next_id = self.next_id.max(1);
        self.rebuild_intervals();
    }

    fn open_with(dir: &Path, opts: TreeOpenOptions) -> GraniteResult<Self> {
        std::fs::create_dir_all(dir)?;
        let mut tree = Self::empty();
        tree.data_dir = Some(dir.to_path_buf());
        tree.auto_checkpoint_every = opts.auto_checkpoint_every;

        let mut restored_lsn = 0u64;
        if let Some(snap) = snapshot::load(dir)? {
            restored_lsn = snap.lsn;
            tree.restore_snapshot(snap)?;
        }

        let wal_dir = dir.join("wal");
        let (mut wal, records) = TreeWal::open(&wal_dir, opts.fsync)?;
        for (lsn, op) in records {
            if lsn <= restored_lsn {
                continue;
            }
            tree.apply_op(&op)?;
        }

        wal.set_current_lsn(restored_lsn);
        tree.wal = Some(wal);
        tree.rebuild_hnsw();
        Ok(tree)
    }

    fn restore_snapshot(&mut self, snap: TreeSnapshot) -> GraniteResult<()> {
        self.nodes = snap.nodes;
        self.root_id = snap.root_id;
        self.next_id = snap.next_id;
        self.path_index.clear();
        for (id, node) in &self.nodes {
            self.path_index.insert(node.path.clone(), *id);
        }
        if !self.nodes.contains_key(&self.root_id) {
            return Err(GraniteError::Storage(
                "snapshot is missing the root node".to_string(),
            ));
        }
        self.rebuild_intervals();
        Ok(())
    }

    /// Root node id (always present).
    pub fn root_id(&self) -> NodeId {
        self.root_id
    }

    /// Number of nodes, including root.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// True when the tree has no nodes besides root.
    pub fn is_empty(&self) -> bool {
        self.nodes.len() <= 1
    }

    /// True when only the root node exists.
    pub fn is_root_only(&self) -> bool {
        self.is_empty()
    }

    /// Current WAL LSN (0 for ephemeral trees).
    pub fn lsn(&self) -> u64 {
        self.wal.as_ref().map(TreeWal::lsn).unwrap_or(0)
    }

    /// Fetch a node by id.
    pub fn get(&self, id: NodeId) -> GraniteResult<&Node> {
        self.nodes.get(&id).ok_or(GraniteError::NodeNotFound(id))
    }

    /// Fetch a node by canonical path.
    pub fn get_by_path(&self, path: &NodePath) -> GraniteResult<&Node> {
        let id = self
            .path_index
            .get(path)
            .copied()
            .ok_or_else(|| GraniteError::InvalidNodePath(format!("no node at {path}")))?;
        self.get(id)
    }

    /// Fetch a node by a path string (`/a/b`).
    pub fn get_by_path_str(&self, path: &str) -> GraniteResult<&Node> {
        self.get_by_path(&NodePath::parse(path)?)
    }

    /// Euler-tour interval for `id`.
    pub fn interval(&self, id: NodeId) -> GraniteResult<DfsInterval> {
        let _ = self.get(id)?;
        self.intervals
            .get(&id)
            .copied()
            .ok_or_else(|| GraniteError::Internal(format!("missing DFS interval for {id}")))
    }

    /// True when `id` is `root` or a descendant of `root` (O(1) interval test).
    pub fn in_subtree(&self, root: NodeId, id: NodeId) -> GraniteResult<bool> {
        let outer = self.interval(root)?;
        let inner = self.interval(id)?;
        Ok(outer.contains(inner))
    }

    /// Nodes whose path matches `pattern` (`*` one segment, `**` zero or more).
    ///
    /// When the pattern has a leading literal prefix that exists in the path
    /// index, only that subtree is considered. A missing prefix yields no
    /// hits. A fully literal pattern is a single path-index lookup.
    pub fn glob(&self, pattern: &str) -> GraniteResult<Vec<&Node>> {
        self.glob_path(&PathGlob::parse(pattern)?)
    }

    /// Same as [`MemoryTree::glob`] for an already-parsed pattern.
    pub fn glob_path(&self, glob: &PathGlob) -> GraniteResult<Vec<&Node>> {
        let prefix = glob.literal_prefix();
        if glob.is_literal() {
            return match self.path_index.get(&prefix) {
                Some(id) => self.get(*id).map(|n| vec![n]),
                None => Ok(Vec::new()),
            };
        }
        let start = if prefix.is_root() {
            self.root_id
        } else {
            match self.path_index.get(&prefix).copied() {
                Some(id) => id,
                None => return Ok(Vec::new()),
            }
        };
        Ok(self
            .subtree(start)?
            .filter(|node| glob.matches(&node.path))
            .collect())
    }

    /// Start a query over `root` and its descendants.
    pub fn query(&self, root: NodeId) -> GraniteResult<SubtreeQuery<'_>> {
        let _ = self.get(root)?;
        Ok(SubtreeQuery::new(self, root))
    }

    /// Insert `name` under `parent` with the given payload (kind = Memory).
    pub fn insert(
        &mut self,
        parent: NodeId,
        name: impl Into<String>,
        payload: Payload,
    ) -> GraniteResult<NodeId> {
        self.insert_with(parent, NodeSpec::new(name).payload(payload))
    }

    /// Insert a node described by `spec` under `parent`.
    pub fn insert_with(&mut self, parent: NodeId, spec: NodeSpec) -> GraniteResult<NodeId> {
        self.validate_insert(parent, &spec)?;
        let parent_node = self.get(parent)?;
        let path = parent_node.path.join(&spec.name)?;
        let now = Utc::now();
        let id = NodeId::from_raw(self.next_id);
        let node = Node {
            id,
            parent_id: Some(parent),
            path,
            name: spec.name,
            kind: spec.kind,
            payload: spec.payload,
            summary: spec.summary,
            created_at: now,
            updated_at: now,
            expires_at: spec.expires_at,
            decay: spec.decay,
            children: Vec::new(),
        };
        self.commit(TreeOp::Insert(Box::new(node)))?;
        Ok(id)
    }

    /// Replace payload, summary, expiry, and decay of an existing node.
    pub fn update(
        &mut self,
        id: NodeId,
        payload: Payload,
        summary: Option<String>,
        expires_at: Option<chrono::DateTime<Utc>>,
        decay: Option<f32>,
    ) -> GraniteResult<()> {
        let _ = self.get(id)?;
        self.commit(TreeOp::Update {
            id,
            payload,
            summary,
            expires_at,
            decay,
            updated_at: Utc::now(),
        })
    }

    /// Replace only the payload.
    pub fn set_payload(&mut self, id: NodeId, payload: Payload) -> GraniteResult<()> {
        let node = self.get(id)?;
        self.update(
            id,
            payload,
            node.summary.clone(),
            node.expires_at,
            node.decay,
        )
    }

    /// Set or clear the roll-up summary.
    pub fn set_summary(&mut self, id: NodeId, summary: Option<String>) -> GraniteResult<()> {
        let node = self.get(id)?;
        self.update(
            id,
            node.payload.clone(),
            summary,
            node.expires_at,
            node.decay,
        )
    }

    /// Move `id` under `new_parent`. Paths of the whole subtree are rewritten.
    pub fn move_node(&mut self, id: NodeId, new_parent: NodeId) -> GraniteResult<()> {
        self.validate_move(id, new_parent)?;
        if self.get(id)?.parent_id == Some(new_parent) {
            return Ok(());
        }
        self.commit(TreeOp::Move {
            id,
            new_parent,
            updated_at: Utc::now(),
        })
    }

    /// Rename a node (sibling uniqueness is enforced). Root cannot be renamed.
    pub fn rename(&mut self, id: NodeId, new_name: impl Into<String>) -> GraniteResult<()> {
        let new_name = new_name.into();
        self.validate_rename(id, &new_name)?;
        if self.get(id)?.name == new_name {
            return Ok(());
        }
        self.commit(TreeOp::Rename {
            id,
            new_name,
            updated_at: Utc::now(),
        })
    }

    /// Delete `id` and every descendant. Returns the number of nodes removed.
    /// Root cannot be deleted.
    pub fn delete_subtree(&mut self, id: NodeId) -> GraniteResult<usize> {
        self.validate_delete(id)?;
        let count = self.subtree_ids(id).len();
        self.commit(TreeOp::DeleteSubtree { id })?;
        Ok(count)
    }

    /// Direct children, in insertion order.
    pub fn children(&self, id: NodeId) -> GraniteResult<Vec<&Node>> {
        let ids = self.get(id)?.children.clone();
        ids.into_iter().map(|cid| self.get(cid)).collect()
    }

    /// Ancestors from nearest parent to root. Empty for root.
    pub fn ancestors(&self, id: NodeId) -> GraniteResult<Vec<&Node>> {
        let mut out = Vec::new();
        let mut current = self.get(id)?.parent_id;
        while let Some(pid) = current {
            let node = self.get(pid)?;
            current = node.parent_id;
            out.push(node);
        }
        Ok(out)
    }

    /// Preorder walk of `id` and its descendants (self first).
    pub fn subtree(&self, id: NodeId) -> GraniteResult<SubtreeIter<'_>> {
        let _ = self.get(id)?;
        Ok(SubtreeIter {
            tree: self,
            ids: self.subtree_ids(id),
            idx: 0,
        })
    }

    /// Exact k-NN over embeddings in `root`'s subtree, using [`DistanceMetric`].
    ///
    /// Membership is the Euler interval of `root` (`nodes_in_interval`). The
    /// walk does not follow child lists. Distances use the SIMD-friendly
    /// [`QueryKernel`] (cosine reuses `||q||`). Embeddings that are missing,
    /// empty of finite values, or produce a non-finite distance are skipped.
    /// A dimension mismatch is still an error.
    pub fn search_subtree(
        &self,
        root: NodeId,
        query: &[f32],
        k: usize,
        metric: DistanceMetric,
    ) -> GraniteResult<Vec<SearchHit>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let iv = self.interval(root)?;
        let kernel = QueryKernel::new(metric, query);
        let mut heap: BinaryHeap<HeapHit> = BinaryHeap::with_capacity(k);
        for node in self.nodes_in_interval(iv.dfs_in, iv.dfs_out) {
            let Some(emb) = node.payload.embedding.as_deref() else {
                continue;
            };
            if emb.len() != query.len() {
                return Err(GraniteError::VectorDimensionMismatch {
                    expected: query.len(),
                    actual: emb.len(),
                });
            }
            if !emb.iter().copied().all(f32::is_finite) {
                continue;
            }
            let distance = kernel.distance(emb);
            if !distance.is_finite() {
                continue;
            }
            let hit = HeapHit {
                id: node.id,
                distance,
            };
            if heap.len() < k {
                heap.push(hit);
            } else if heap.peek().is_some_and(|worst| hit < *worst) {
                heap.pop();
                heap.push(hit);
            }
        }
        let mut hits: Vec<SearchHit> = heap
            .into_iter()
            .map(|h| SearchHit {
                id: h.id,
                distance: h.distance,
            })
            .collect();
        hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then(a.id.cmp(&b.id))
        });
        Ok(hits)
    }

    /// Approximate k-NN via the shared [`HnswIndex`], then keep hits in `root`'s subtree.
    pub fn search_subtree_ann(
        &self,
        root: NodeId,
        query: &[f32],
        k: usize,
    ) -> GraniteResult<Vec<SearchHit>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let iv = self.interval(root)?;
        let fetch = k.saturating_mul(8).max(k);
        Ok(self
            .hnsw
            .search(query, fetch)
            .into_iter()
            .filter_map(|hit| {
                let id = NodeId::from_raw(hit.id.parse::<u64>().ok()?);
                let node_iv = self.intervals.get(&id)?;
                if iv.contains(*node_iv) {
                    Some(SearchHit {
                        id,
                        distance: hit.distance,
                    })
                } else {
                    None
                }
            })
            .take(k)
            .collect())
    }

    /// Preorder walk from root.
    pub fn iter(&self) -> SubtreeIter<'_> {
        self.subtree(self.root_id).expect("root always exists")
    }

    /// Child ids of `id`.
    pub fn child_ids(&self, id: NodeId) -> GraniteResult<&[NodeId]> {
        Ok(self.get(id)?.child_ids())
    }

    /// Flush the WAL (no-op for ephemeral trees).
    pub fn flush(&mut self) -> GraniteResult<()> {
        if let Some(wal) = &mut self.wal {
            wal.flush()?;
        }
        Ok(())
    }

    /// Write a snapshot and truncate the WAL. No-op for ephemeral trees.
    pub fn checkpoint(&mut self) -> GraniteResult<()> {
        let Some(dir) = self.data_dir.clone() else {
            return Ok(());
        };
        if let Some(wal) = &mut self.wal {
            wal.flush()?;
        }
        let lsn = self.lsn();
        let snap = TreeSnapshot {
            format_version: 1,
            lsn,
            root_id: self.root_id,
            next_id: self.next_id,
            nodes: self.nodes.clone(),
        };
        snapshot::save(&dir, &snap)?;
        if let Some(wal) = &mut self.wal {
            wal.truncate()?;
        }
        self.ops_since_checkpoint = 0;
        Ok(())
    }

    /// Expensive consistency check. Used by tests and diagnostics.
    pub fn validate(&self) -> GraniteResult<()> {
        if !self.nodes.contains_key(&self.root_id) {
            return Err(GraniteError::Internal("missing root".into()));
        }
        let root = self.get(self.root_id)?;
        if root.parent_id.is_some() || !root.path.is_root() || root.kind != NodeKind::Root {
            return Err(GraniteError::Internal("root invariant violated".into()));
        }
        if self.path_index.len() != self.nodes.len() {
            return Err(GraniteError::Internal("path index size mismatch".into()));
        }
        for (id, node) in &self.nodes {
            if node.id != *id {
                return Err(GraniteError::Internal(format!(
                    "node map key {id} != node.id {}",
                    node.id
                )));
            }
            if self.path_index.get(&node.path) != Some(id) {
                return Err(GraniteError::Internal(format!(
                    "path index missing {}",
                    node.path
                )));
            }
            if let Some(pid) = node.parent_id {
                let parent = self
                    .nodes
                    .get(&pid)
                    .ok_or_else(|| GraniteError::Internal(format!("{id} parent {pid} missing")))?;
                if !parent.children.contains(id) {
                    return Err(GraniteError::Internal(format!(
                        "{id} not in parent {pid} children"
                    )));
                }
                let expected = parent.path.join(&node.name)?;
                if expected != node.path {
                    return Err(GraniteError::Internal(format!(
                        "path {} != expected {expected}",
                        node.path
                    )));
                }
            } else if *id != self.root_id {
                return Err(GraniteError::Internal(format!("{id} has no parent")));
            }
            for cid in &node.children {
                let child = self
                    .nodes
                    .get(cid)
                    .ok_or_else(|| GraniteError::Internal(format!("child {cid} missing")))?;
                if child.parent_id != Some(*id) {
                    return Err(GraniteError::Internal(format!(
                        "child {cid} parent mismatch"
                    )));
                }
            }
            if node.id.raw() >= self.next_id {
                return Err(GraniteError::Internal("next_id too small".into()));
            }
        }
        if self.has_cycle() {
            return Err(GraniteError::Internal("cycle detected".into()));
        }
        self.validate_intervals()?;
        Ok(())
    }

    fn validate_intervals(&self) -> GraniteResult<()> {
        if self.intervals.len() != self.nodes.len() || self.interval_index.len() != self.nodes.len()
        {
            return Err(GraniteError::Internal(
                "DFS interval index size mismatch".into(),
            ));
        }
        let mut tick = 0u32;
        let mut expected = HashMap::with_capacity(self.nodes.len());
        self.assign_intervals(self.root_id, &mut tick, &mut expected);
        if expected.len() != self.nodes.len() {
            return Err(GraniteError::Internal(
                "DFS assignment missed nodes (disconnected from root?)".into(),
            ));
        }
        for (id, node) in &self.nodes {
            let Some(got) = self.intervals.get(id) else {
                return Err(GraniteError::Internal(format!(
                    "{id} has no stored DFS interval"
                )));
            };
            let Some(want) = expected.get(id) else {
                return Err(GraniteError::Internal(format!(
                    "{id} was not reached by the Euler tour"
                )));
            };
            if got != want {
                return Err(GraniteError::Internal(format!(
                    "{id} interval [{},{}] != expected [{},{}]",
                    got.dfs_in, got.dfs_out, want.dfs_in, want.dfs_out
                )));
            }
            if got.dfs_in >= got.dfs_out {
                return Err(GraniteError::Internal(format!(
                    "{id} has an empty DFS interval"
                )));
            }
            if self.interval_index.get(&got.dfs_in) != Some(id) {
                return Err(GraniteError::Internal(format!(
                    "interval index missing dfs_in {}",
                    got.dfs_in
                )));
            }
            if let Some(pid) = node.parent_id {
                let parent = self.intervals.get(&pid).ok_or_else(|| {
                    GraniteError::Internal(format!("parent {pid} of {id} has no interval"))
                })?;
                if !(parent.dfs_in < got.dfs_in && got.dfs_out < parent.dfs_out) {
                    return Err(GraniteError::Internal(format!(
                        "{id} interval not nested in parent {pid}"
                    )));
                }
            }
            for pair in node.children.windows(2) {
                let left = self.intervals.get(&pair[0]).ok_or_else(|| {
                    GraniteError::Internal(format!("child {} of {id} has no interval", pair[0]))
                })?;
                let right = self.intervals.get(&pair[1]).ok_or_else(|| {
                    GraniteError::Internal(format!("child {} of {id} has no interval", pair[1]))
                })?;
                if left.dfs_out >= right.dfs_in {
                    return Err(GraniteError::Internal(format!(
                        "sibling intervals overlap under {id}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn has_cycle(&self) -> bool {
        for start in self.nodes.keys().copied() {
            let mut seen = HashMap::new();
            let mut cur = Some(start);
            while let Some(id) = cur {
                if seen.insert(id, ()).is_some() {
                    return true;
                }
                cur = self.nodes.get(&id).and_then(|n| n.parent_id);
            }
        }
        false
    }

    fn nodes_in_interval(&self, lo: u32, hi: u32) -> impl Iterator<Item = &Node> {
        self.interval_index
            .range(lo..=hi)
            .filter_map(|(_, id)| self.nodes.get(id))
    }

    fn subtree_ids(&self, id: NodeId) -> Vec<NodeId> {
        if let Some(iv) = self.intervals.get(&id) {
            return self
                .interval_index
                .range(iv.dfs_in..=iv.dfs_out)
                .map(|(_, nid)| *nid)
                .collect();
        }
        // Fallback walk if intervals have not been assigned yet.
        let mut ids = vec![id];
        let mut i = 0;
        while i < ids.len() {
            if let Some(node) = self.nodes.get(&ids[i]) {
                ids.extend_from_slice(&node.children);
            }
            i += 1;
        }
        ids
    }

    fn is_in_subtree(&self, root: NodeId, candidate: NodeId) -> bool {
        match (self.intervals.get(&root), self.intervals.get(&candidate)) {
            (Some(r), Some(c)) => r.contains(*c),
            _ => self.subtree_ids(root).contains(&candidate),
        }
    }

    fn rebuild_intervals(&mut self) {
        let mut tick = 0u32;
        let mut assigned = HashMap::with_capacity(self.nodes.len());
        if self.nodes.contains_key(&self.root_id) {
            self.assign_intervals(self.root_id, &mut tick, &mut assigned);
        }
        self.intervals.clear();
        self.interval_index.clear();
        for (id, iv) in assigned {
            self.interval_index.insert(iv.dfs_in, id);
            self.intervals.insert(id, iv);
        }
    }

    fn assign_intervals(
        &self,
        id: NodeId,
        tick: &mut u32,
        assigned: &mut HashMap<NodeId, DfsInterval>,
    ) {
        let dfs_in = *tick;
        *tick = tick.saturating_add(1);
        if let Some(node) = self.nodes.get(&id) {
            let children = node.children.clone();
            for child in children {
                if self.nodes.contains_key(&child) {
                    self.assign_intervals(child, tick, assigned);
                }
            }
        }
        let dfs_out = *tick;
        *tick = tick.saturating_add(1);
        assigned.insert(id, DfsInterval { dfs_in, dfs_out });
    }

    fn validate_insert(&self, parent: NodeId, spec: &NodeSpec) -> GraniteResult<()> {
        super::path::validate_name(&spec.name)?;
        if spec.kind == NodeKind::Root {
            return Err(GraniteError::InvalidNodeName {
                name: spec.name.clone(),
                reason: "NodeKind::Root is reserved for the tree root".to_string(),
            });
        }
        let parent_node = self.get(parent)?;
        for cid in &parent_node.children {
            if let Some(child) = self.nodes.get(cid) {
                if child.name == spec.name {
                    return Err(GraniteError::DuplicateChildName {
                        parent,
                        name: spec.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_move(&self, id: NodeId, new_parent: NodeId) -> GraniteResult<()> {
        if id.is_root() {
            return Err(GraniteError::CannotMoveRoot);
        }
        let node = self.get(id)?;
        let _ = self.get(new_parent)?;
        if self.is_in_subtree(id, new_parent) {
            return Err(GraniteError::CycleDetected(id));
        }
        let parent = self.get(new_parent)?;
        for cid in &parent.children {
            if *cid == id {
                continue;
            }
            if let Some(child) = self.nodes.get(cid) {
                if child.name == node.name {
                    return Err(GraniteError::DuplicateChildName {
                        parent: new_parent,
                        name: node.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_rename(&self, id: NodeId, new_name: &str) -> GraniteResult<()> {
        if id.is_root() {
            return Err(GraniteError::CannotRenameRoot);
        }
        super::path::validate_name(new_name)?;
        let node = self.get(id)?;
        let parent_id = node.parent_id.ok_or(GraniteError::CannotMoveRoot)?;
        let parent = self.get(parent_id)?;
        for cid in &parent.children {
            if *cid == id {
                continue;
            }
            if let Some(child) = self.nodes.get(cid) {
                if child.name == new_name {
                    return Err(GraniteError::DuplicateChildName {
                        parent: parent_id,
                        name: new_name.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_delete(&self, id: NodeId) -> GraniteResult<()> {
        if id.is_root() {
            return Err(GraniteError::CannotDeleteRoot);
        }
        let _ = self.get(id)?;
        Ok(())
    }

    fn commit(&mut self, op: TreeOp) -> GraniteResult<()> {
        if let Some(wal) = &mut self.wal {
            wal.append(&op)?;
        }
        self.apply_op(&op)?;
        self.ops_since_checkpoint += 1;
        if let Some(n) = self.auto_checkpoint_every {
            if self.ops_since_checkpoint >= n {
                if let Err(e) = self.checkpoint() {
                    tracing::warn!(error = %e, "auto-checkpoint failed");
                }
            }
        }
        Ok(())
    }

    fn apply_op(&mut self, op: &TreeOp) -> GraniteResult<()> {
        match op {
            TreeOp::Insert(node) => self.apply_insert(node.as_ref().clone()),
            TreeOp::Update {
                id,
                payload,
                summary,
                expires_at,
                decay,
                updated_at,
            } => {
                {
                    let node = self
                        .nodes
                        .get_mut(id)
                        .ok_or(GraniteError::NodeNotFound(*id))?;
                    node.payload = payload.clone();
                    node.summary = summary.clone();
                    node.expires_at = *expires_at;
                    node.decay = *decay;
                    node.updated_at = *updated_at;
                }
                let embedding = self.nodes.get(id).and_then(|n| n.payload.embedding.clone());
                self.hnsw_upsert(*id, embedding.as_deref());
                Ok(())
            }
            TreeOp::Move {
                id,
                new_parent,
                updated_at,
            } => self.apply_move(*id, *new_parent, *updated_at),
            TreeOp::Rename {
                id,
                new_name,
                updated_at,
            } => self.apply_rename(*id, new_name, *updated_at),
            TreeOp::DeleteSubtree { id } => self.apply_delete(*id),
        }
    }

    fn apply_insert(&mut self, node: Node) -> GraniteResult<()> {
        let id = node.id;
        let parent_id = node
            .parent_id
            .ok_or_else(|| GraniteError::Internal("logged insert must have a parent".into()))?;
        {
            let parent = self
                .nodes
                .get_mut(&parent_id)
                .ok_or(GraniteError::NodeNotFound(parent_id))?;
            if !parent.children.contains(&id) {
                parent.children.push(id);
            }
        }
        self.path_index.insert(node.path.clone(), id);
        self.next_id = self.next_id.max(id.raw().saturating_add(1));
        let embedding = node.payload.embedding.clone();
        self.nodes.insert(id, node);
        self.hnsw_upsert(id, embedding.as_deref());
        self.rebuild_intervals();
        Ok(())
    }

    fn apply_move(
        &mut self,
        id: NodeId,
        new_parent: NodeId,
        updated_at: chrono::DateTime<Utc>,
    ) -> GraniteResult<()> {
        let old_parent = self
            .nodes
            .get(&id)
            .and_then(|n| n.parent_id)
            .ok_or(GraniteError::CannotMoveRoot)?;
        if old_parent == new_parent {
            return Ok(());
        }
        if let Some(parent) = self.nodes.get_mut(&old_parent) {
            parent.children.retain(|c| *c != id);
        }
        if let Some(parent) = self.nodes.get_mut(&new_parent) {
            if !parent.children.contains(&id) {
                parent.children.push(id);
            }
        }
        if let Some(node) = self.nodes.get_mut(&id) {
            node.parent_id = Some(new_parent);
            node.updated_at = updated_at;
        }
        self.repath_subtree(id)?;
        self.rebuild_intervals();
        Ok(())
    }

    fn apply_rename(
        &mut self,
        id: NodeId,
        new_name: &str,
        updated_at: chrono::DateTime<Utc>,
    ) -> GraniteResult<()> {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.name = new_name.to_string();
            node.updated_at = updated_at;
        }
        self.repath_subtree(id)?;
        Ok(())
    }

    fn apply_delete(&mut self, id: NodeId) -> GraniteResult<()> {
        let ids = self.subtree_ids(id);
        let parent_id = self
            .nodes
            .get(&id)
            .and_then(|n| n.parent_id)
            .ok_or(GraniteError::CannotDeleteRoot)?;
        if let Some(parent) = self.nodes.get_mut(&parent_id) {
            parent.children.retain(|c| *c != id);
        }
        for rid in ids {
            if let Some(node) = self.nodes.remove(&rid) {
                self.path_index.remove(&node.path);
                self.hnsw.remove(&rid.to_string());
            }
        }
        self.rebuild_intervals();
        Ok(())
    }

    fn repath_subtree(&mut self, id: NodeId) -> GraniteResult<()> {
        let (parent_path, name) = {
            let node = self.get(id)?;
            let parent_path = match node.parent_id {
                Some(pid) => self.get(pid)?.path.clone(),
                None => NodePath::root(),
            };
            (parent_path, node.name.clone())
        };
        let new_path = if id.is_root() {
            NodePath::root()
        } else {
            parent_path.join(&name)?
        };
        let old_path = self.get(id)?.path.clone();
        if old_path != new_path {
            self.path_index.remove(&old_path);
            self.path_index.insert(new_path.clone(), id);
            if let Some(node) = self.nodes.get_mut(&id) {
                node.path = new_path;
            }
        }
        let children = self.get(id)?.children.clone();
        for child in children {
            self.repath_subtree(child)?;
        }
        Ok(())
    }

    fn rebuild_hnsw(&mut self) {
        self.hnsw = HnswIndex::new(HnswConfig::default());
        let pairs: Vec<(NodeId, Vec<f32>)> = self
            .nodes
            .iter()
            .filter_map(|(id, node)| {
                node.payload
                    .embedding
                    .as_ref()
                    .map(|emb| (*id, emb.clone()))
            })
            .collect();
        for (id, emb) in pairs {
            self.hnsw.insert(id.to_string(), emb);
        }
    }

    fn hnsw_upsert(&mut self, id: NodeId, embedding: Option<&[f32]>) {
        let key = id.to_string();
        self.hnsw.remove(&key);
        if let Some(emb) = embedding {
            if !emb.is_empty() {
                self.hnsw.insert(key, emb.to_vec());
            }
        }
    }
}

impl Drop for MemoryTree {
    fn drop(&mut self) {
        if let Some(wal) = &mut self.wal {
            let _ = wal.flush();
        }
    }
}

impl PartialEq for MemoryTree {
    fn eq(&self, other: &Self) -> bool {
        self.root_id == other.root_id && self.next_id == other.next_id && self.nodes == other.nodes
    }
}

/// Preorder iterator over a subtree.
pub struct SubtreeIter<'a> {
    tree: &'a MemoryTree,
    ids: Vec<NodeId>,
    idx: usize,
}

impl<'a> Iterator for SubtreeIter<'a> {
    type Item = &'a Node;

    fn next(&mut self) -> Option<Self::Item> {
        let id = *self.ids.get(self.idx)?;
        self.idx += 1;
        self.tree.nodes.get(&id)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.ids.len().saturating_sub(self.idx);
        (left, Some(left))
    }
}

impl ExactSizeIterator for SubtreeIter<'_> {}
