// ============================================================================
// GraniteDB — Encryption at Rest
// ============================================================================

use crate::error::{GraniteError, GraniteResult};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use rand::RngCore;

/// AES-256-GCM encryption engine for data at rest.
pub struct EncryptionEngine {
    cipher: Aes256Gcm,
}

impl EncryptionEngine {
    /// Create a new encryption engine with a 256-bit key.
    pub fn new(key: &[u8; 32]) -> Self {
        let cipher = Aes256Gcm::new_from_slice(key).expect("Invalid key length");
        Self { cipher }
    }

    /// Generate a random 256-bit encryption key.
    pub fn generate_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        key
    }

    /// Encrypt data. Returns nonce + ciphertext.
    pub fn encrypt(&self, plaintext: &[u8]) -> GraniteResult<Vec<u8>> {
        let mut nonce_bytes = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| GraniteError::EncryptionError(format!("Encryption failed: {}", e)))?;

        // Prepend nonce to ciphertext
        let mut result = Vec::with_capacity(12 + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);
        Ok(result)
    }

    /// Decrypt data. Expects nonce + ciphertext.
    pub fn decrypt(&self, data: &[u8]) -> GraniteResult<Vec<u8>> {
        if data.len() < 12 {
            return Err(GraniteError::DecryptionError(
                "Data too short for nonce".to_string(),
            ));
        }

        let (nonce_bytes, ciphertext) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| GraniteError::DecryptionError(format!("Decryption failed: {}", e)))
    }
}
