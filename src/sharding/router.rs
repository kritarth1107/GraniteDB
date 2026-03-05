// ============================================================================
// GraniteDB — Shard Router
// ============================================================================
// Routes operations to the correct shard based on the shard key hash.
// Uses consistent hashing for minimal data movement during resharding.
// ============================================================================

use crate::sharding::shard::Shard;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Routes requests to the appropriate shard.
pub struct ShardRouter {
    /// Sorted map of virtual shard boundaries to shard IDs
    ring: BTreeMap<u64, String>,
    /// All registered shards
    shards: Vec<Shard>,
    /// Number of virtual nodes per shard (for even distribution)
    virtual_nodes: usize,
}

impl ShardRouter {
    pub fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            shards: Vec::new(),
            virtual_nodes,
        }
    }

    /// Add a shard to the hash ring.
    pub fn add_shard(&mut self, shard: Shard) {
        for i in 0..self.virtual_nodes {
            let key = format!("{}:{}", shard.id, i);
            let hash = Self::hash_key(&key);
            self.ring.insert(hash, shard.id.clone());
        }
        self.shards.push(shard);
    }

    /// Remove a shard from the hash ring.
    pub fn remove_shard(&mut self, shard_id: &str) {
        for i in 0..self.virtual_nodes {
            let key = format!("{}:{}", shard_id, i);
            let hash = Self::hash_key(&key);
            self.ring.remove(&hash);
        }
        self.shards.retain(|s| s.id != shard_id);
    }

    /// Route a shard key value to the appropriate shard ID.
    pub fn route(&self, shard_key_value: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = Self::hash_key(shard_key_value);

        // Find the first node on the ring whose position is >= hash
        let shard_id = self
            .ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next()) // Wrap around
            .map(|(_, id)| id.as_str());

        shard_id
    }

    /// Get a shard by ID.
    pub fn get_shard(&self, shard_id: &str) -> Option<&Shard> {
        self.shards.iter().find(|s| s.id == shard_id)
    }

    /// List all shards.
    pub fn list_shards(&self) -> &[Shard] {
        &self.shards
    }

    /// Hash a key using SHA-256 and return the first 8 bytes as u64.
    fn hash_key(key: &str) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        let result = hasher.finalize();
        u64::from_be_bytes(result[..8].try_into().unwrap())
    }
}
