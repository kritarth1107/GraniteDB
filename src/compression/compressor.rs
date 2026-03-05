// ============================================================================
// GraniteDB — Compression Engine
// ============================================================================
// Provides transparent data compression using multiple algorithms.
// Reduces storage footprint and I/O bandwidth at the cost of CPU.
// ============================================================================

use serde::{Deserialize, Serialize};

/// Supported compression algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// Run-Length Encoding (fast, simple)
    Rle,
    /// LZ77-style compression (good ratio, moderate speed)
    Lz77,
    /// Snappy-compatible (very fast, decent ratio)
    SnappyLike,
}

/// Compression engine with algorithm selection.
pub struct CompressionEngine {
    algorithm: CompressionAlgorithm,
    /// Stats
    total_compressed: u64,
    total_uncompressed: u64,
}

impl CompressionEngine {
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self {
            algorithm,
            total_compressed: 0,
            total_uncompressed: 0,
        }
    }

    /// Compress data.
    pub fn compress(&mut self, data: &[u8]) -> Vec<u8> {
        self.total_uncompressed += data.len() as u64;

        let compressed = match self.algorithm {
            CompressionAlgorithm::None => data.to_vec(),
            CompressionAlgorithm::Rle => self.rle_compress(data),
            CompressionAlgorithm::Lz77 => self.lz77_compress(data),
            CompressionAlgorithm::SnappyLike => self.snappy_compress(data),
        };

        self.total_compressed += compressed.len() as u64;
        compressed
    }

    /// Decompress data.
    pub fn decompress(&self, data: &[u8]) -> Vec<u8> {
        match self.algorithm {
            CompressionAlgorithm::None => data.to_vec(),
            CompressionAlgorithm::Rle => self.rle_decompress(data),
            CompressionAlgorithm::Lz77 => self.lz77_decompress(data),
            CompressionAlgorithm::SnappyLike => self.snappy_decompress(data),
        }
    }

    // ── RLE (Run-Length Encoding) ─────────────────────────────

    fn rle_compress(&self, data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();
        let mut i = 0;

        while i < data.len() {
            let byte = data[i];
            let mut count = 1u8;

            while i + count as usize < data.len()
                && data[i + count as usize] == byte
                && count < 255
            {
                count += 1;
            }

            result.push(count);
            result.push(byte);
            i += count as usize;
        }

        result
    }

    fn rle_decompress(&self, data: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();
        let mut i = 0;

        while i + 1 < data.len() {
            let count = data[i] as usize;
            let byte = data[i + 1];
            for _ in 0..count {
                result.push(byte);
            }
            i += 2;
        }

        result
    }

    // ── LZ77-style ────────────────────────────────────────────

    fn lz77_compress(&self, data: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();
        let mut pos = 0;
        let window_size = 255usize;
        let max_match = 255usize;

        while pos < data.len() {
            let mut best_offset = 0u8;
            let mut best_length = 0u8;

            let search_start = pos.saturating_sub(window_size);

            for offset in 1..=(pos - search_start) {
                let start = pos - offset;
                let mut length = 0usize;

                while pos + length < data.len()
                    && length < max_match
                    && data[start + (length % offset)] == data[pos + length]
                {
                    length += 1;
                }

                if length > best_length as usize {
                    best_offset = offset as u8;
                    best_length = length as u8;
                }
            }

            if best_length >= 3 {
                // Encode as (offset, length) back-reference
                result.push(0xFF); // Marker
                result.push(best_offset);
                result.push(best_length);
                pos += best_length as usize;
            } else {
                // Literal byte
                let byte = data[pos];
                if byte == 0xFF {
                    result.push(0xFF);
                    result.push(0);
                    result.push(0);
                } else {
                    result.push(byte);
                }
                pos += 1;
            }
        }

        result
    }

    fn lz77_decompress(&self, data: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();
        let mut i = 0;

        while i < data.len() {
            if data[i] == 0xFF && i + 2 < data.len() {
                let offset = data[i + 1] as usize;
                let length = data[i + 2] as usize;

                if offset == 0 && length == 0 {
                    result.push(0xFF);
                } else {
                    let start = result.len() - offset;
                    for j in 0..length {
                        let byte = result[start + (j % offset)];
                        result.push(byte);
                    }
                }
                i += 3;
            } else {
                result.push(data[i]);
                i += 1;
            }
        }

        result
    }

    // ── Snappy-like (fast, simple) ────────────────────────────

    fn snappy_compress(&self, data: &[u8]) -> Vec<u8> {
        // Simple block-based compression
        let mut result = Vec::new();

        // Write original length as 4 bytes (little-endian)
        let len = data.len() as u32;
        result.extend_from_slice(&len.to_le_bytes());

        let mut pos = 0;
        while pos < data.len() {
            // Try to find a match in the last 256 bytes
            let mut best_offset = 0u16;
            let mut best_length = 0u16;

            let search_start = pos.saturating_sub(65535);
            for offset in 1..=(pos - search_start).min(65535) {
                let start = pos - offset;
                let mut length = 0usize;

                while pos + length < data.len()
                    && length < 64
                    && data[start + length] == data[pos + length]
                {
                    length += 1;
                }

                if length > best_length as usize && length >= 4 {
                    best_offset = offset as u16;
                    best_length = length as u16;
                }
            }

            if best_length >= 4 {
                // Copy tag: 01 | (length-4)<<2 in first byte
                result.push(0x01 | ((best_length.min(7) as u8 - 1) << 2));
                result.extend_from_slice(&best_offset.to_le_bytes());
                pos += best_length as usize;
            } else {
                // Literal tag: 00 | (length-1)<<2
                let literal_len = 1usize;
                result.push(0x00);
                result.push(data[pos]);
                pos += literal_len;
            }
        }

        result
    }

    fn snappy_decompress(&self, data: &[u8]) -> Vec<u8> {
        if data.len() < 4 {
            return Vec::new();
        }

        let original_len =
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut result = Vec::with_capacity(original_len);
        let mut i = 4;

        while i < data.len() && result.len() < original_len {
            let tag = data[i];
            let tag_type = tag & 0x03;

            if tag_type == 0x00 {
                // Literal
                i += 1;
                if i < data.len() {
                    result.push(data[i]);
                    i += 1;
                }
            } else if tag_type == 0x01 {
                // Copy
                let length = ((tag >> 2) & 0x07) as usize + 1;
                i += 1;
                if i + 1 < data.len() {
                    let offset =
                        u16::from_le_bytes([data[i], data[i + 1]]) as usize;
                    i += 2;
                    let start = result.len().saturating_sub(offset);
                    for j in 0..length {
                        if start + j < result.len() {
                            let byte = result[start + j];
                            result.push(byte);
                        }
                    }
                }
            } else {
                i += 1;
            }
        }

        result.truncate(original_len);
        result
    }

    /// Get compression ratio.
    pub fn ratio(&self) -> f64 {
        if self.total_uncompressed == 0 {
            1.0
        } else {
            self.total_compressed as f64 / self.total_uncompressed as f64
        }
    }

    /// Get savings percentage.
    pub fn savings_percent(&self) -> f64 {
        (1.0 - self.ratio()) * 100.0
    }

    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!({
            "algorithm": format!("{:?}", self.algorithm),
            "total_uncompressed_bytes": self.total_uncompressed,
            "total_compressed_bytes": self.total_compressed,
            "compression_ratio": self.ratio(),
            "savings_percent": self.savings_percent(),
        })
    }
}
