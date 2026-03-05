// ============================================================================
// GraniteDB — Bloom Filter
// ============================================================================
// Space-efficient probabilistic data structure for fast negative lookups.
// False positives are possible, but false negatives are not.
// Used to avoid unnecessary disk reads: if the bloom filter says "no",
// the key definitely doesn't exist.
// ============================================================================

use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A Bloom filter for fast set membership testing.
pub struct BloomFilter {
    /// Bit array
    bits: Vec<bool>,
    /// Size of the bit array
    size: usize,
    /// Number of hash functions
    num_hashes: usize,
    /// Number of inserted items
    count: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with the given expected number of items
    /// and desired false positive rate.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let size = Self::optimal_size(expected_items, false_positive_rate);
        let num_hashes = Self::optimal_hashes(size, expected_items);

        Self {
            bits: vec![false; size],
            size,
            num_hashes,
            count: 0,
        }
    }

    /// Create with explicit parameters.
    pub fn with_params(size: usize, num_hashes: usize) -> Self {
        Self {
            bits: vec![false; size],
            size,
            num_hashes,
            count: 0,
        }
    }

    /// Calculate optimal bit array size.
    /// m = -(n * ln(p)) / (ln(2)^2)
    fn optimal_size(n: usize, p: f64) -> usize {
        let m = -(n as f64 * p.ln()) / (2.0_f64.ln().powi(2));
        m.ceil() as usize
    }

    /// Calculate optimal number of hash functions.
    /// k = (m / n) * ln(2)
    fn optimal_hashes(m: usize, n: usize) -> usize {
        let k = (m as f64 / n as f64) * 2.0_f64.ln();
        k.ceil().max(1.0) as usize
    }

    /// Generate hash values for a key.
    fn hash_values(&self, key: &[u8]) -> Vec<usize> {
        // Use double hashing: h(i) = h1 + i * h2
        let mut hasher1 = DefaultHasher::new();
        key.hash(&mut hasher1);
        let h1 = hasher1.finish();

        let mut hasher2 = Sha256::new();
        hasher2.update(key);
        let result = hasher2.finalize();
        let h2 = u64::from_be_bytes(result[..8].try_into().unwrap());

        (0..self.num_hashes)
            .map(|i| {
                let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
                (hash % self.size as u64) as usize
            })
            .collect()
    }

    /// Insert a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        let positions = self.hash_values(key);
        for pos in positions {
            self.bits[pos] = true;
        }
        self.count += 1;
    }

    /// Insert a string key.
    pub fn insert_str(&mut self, key: &str) {
        self.insert(key.as_bytes());
    }

    /// Check if a key might be in the set.
    /// Returns true if the key MIGHT exist (possible false positive).
    /// Returns false if the key DEFINITELY does not exist.
    pub fn might_contain(&self, key: &[u8]) -> bool {
        let positions = self.hash_values(key);
        positions.iter().all(|&pos| self.bits[pos])
    }

    /// Check a string key.
    pub fn might_contain_str(&self, key: &str) -> bool {
        self.might_contain(key.as_bytes())
    }

    /// Number of inserted items.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Estimated false positive rate at current load.
    pub fn false_positive_rate(&self) -> f64 {
        let ones: usize = self.bits.iter().filter(|&&b| b).count();
        let fill_ratio = ones as f64 / self.size as f64;
        fill_ratio.powi(self.num_hashes as i32)
    }

    /// Reset the filter.
    pub fn clear(&mut self) {
        self.bits.fill(false);
        self.count = 0;
    }

    /// Memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.size // 1 byte per bool (could be optimized to bit-packing)
    }
}
