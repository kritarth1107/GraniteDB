// ============================================================================
// GraniteDB — Helper Utilities
// ============================================================================

use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Generate a new UUID v4 string.
pub fn generate_id() -> String {
    Uuid::new_v4().to_string()
}

/// SHA-256 hash of a byte slice, returned as a hex string.
pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex_encode(&result)
}

/// Encode bytes as a hex string.
pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Format a byte count as a human-readable string.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a duration in microseconds as a human-readable string.
pub fn format_duration_us(us: u128) -> String {
    if us >= 1_000_000 {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    } else if us >= 1_000 {
        format!("{:.2}ms", us as f64 / 1_000.0)
    } else {
        format!("{}µs", us)
    }
}

/// Sanitize a collection or database name.
pub fn sanitize_name(name: &str) -> Result<String, String> {
    if name.is_empty() {
        return Err("Name cannot be empty".to_string());
    }
    if name.len() > 128 {
        return Err("Name too long (max 128 chars)".to_string());
    }
    if name.contains(|c: char| !c.is_alphanumeric() && c != '_' && c != '-') {
        return Err("Name contains invalid characters (only alphanumeric, _, -)".to_string());
    }
    Ok(name.to_string())
}

/// Get the current timestamp in milliseconds.
pub fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
