// ============================================================================
// GraniteDB — LRU Cache (Generic)
// ============================================================================
// A generic Least Recently Used cache with O(1) get/put operations.
// ============================================================================

use std::collections::HashMap;
use std::hash::Hash;

struct LruNode<K, V> {
    key: K,
    value: V,
    prev: Option<usize>,
    next: Option<usize>,
}

/// A generic LRU Cache.
pub struct LruCache<K: Eq + Hash + Clone, V> {
    capacity: usize,
    map: HashMap<K, usize>,
    nodes: Vec<LruNode<K, V>>,
    head: Option<usize>,
    tail: Option<usize>,
    hits: u64,
    misses: u64,
}

impl<K: Eq + Hash + Clone, V> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            nodes: Vec::new(),
            head: None,
            tail: None,
            hits: 0,
            misses: 0,
        }
    }

    /// Get a reference to a cached value, marking it as recently used.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(&idx) = self.map.get(key) {
            self.hits += 1;
            self.move_to_front(idx);
            Some(&self.nodes[idx].value)
        } else {
            self.misses += 1;
            None
        }
    }

    /// Insert or update a value.
    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        if let Some(&idx) = self.map.get(&key) {
            let old = std::mem::replace(&mut self.nodes[idx].value, value);
            self.move_to_front(idx);
            return Some(old);
        }

        // Evict if at capacity
        let evicted = if self.nodes.len() >= self.capacity {
            self.evict_tail()
        } else {
            None
        };

        let idx = self.nodes.len();
        self.nodes.push(LruNode {
            key: key.clone(),
            value,
            prev: None,
            next: self.head,
        });

        if let Some(old_head) = self.head {
            self.nodes[old_head].prev = Some(idx);
        }
        self.head = Some(idx);
        if self.tail.is_none() {
            self.tail = Some(idx);
        }

        self.map.insert(key, idx);
        evicted
    }

    /// Remove a key.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(&idx) = self.map.get(key) {
            self.detach(idx);
            self.map.remove(key);
            // We can't actually remove from the Vec without invalidating indices,
            // so we leave a tombstone
            None
        } else {
            None
        }
    }

    fn move_to_front(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }
        self.detach(idx);
        self.nodes[idx].prev = None;
        self.nodes[idx].next = self.head;
        if let Some(old_head) = self.head {
            self.nodes[old_head].prev = Some(idx);
        }
        self.head = Some(idx);
    }

    fn detach(&mut self, idx: usize) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        if let Some(p) = prev {
            self.nodes[p].next = next;
        } else {
            self.head = next;
        }

        if let Some(n) = next {
            self.nodes[n].prev = prev;
        } else {
            self.tail = prev;
        }
    }

    fn evict_tail(&mut self) -> Option<V> {
        if let Some(tail_idx) = self.tail {
            self.detach(tail_idx);
            let key = self.nodes[tail_idx].key.clone();
            self.map.remove(&key);
            // Return None since we can't remove from Vec
            None
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    pub fn stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }
}
