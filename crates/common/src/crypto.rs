//! Cryptography traits for the ORCHER platform.
//!
//! Traits live here to break circular dependencies between crates.
//! Implementations are provided by `orcher-security`.

use crate::error::OrcherResult;

/// Token encryptor trait for encrypting sensitive tokens (OAuth tokens, API keys, etc.).
///
/// Implementations MUST:
/// - Use authenticated encryption (AEAD)
/// - Use unique nonces/IVs for each encryption operation
/// - Use at least 256-bit keys
/// - Be tamper-evident (authentication tag verification)
/// - Return base64-encoded strings for storage
pub trait TokenEncryptor: Send + Sync {
    /// Encrypt a token string.
    ///
    /// Must use a unique nonce per call and include an authentication tag in the output.
    /// Plaintext must never be leaked in error messages.
    fn encrypt(&self, plaintext: &str) -> OrcherResult<String>;

    /// Decrypt a token string.
    ///
    /// Must verify the authentication tag before returning plaintext, return an error
    /// (not panic) on auth failure, and not leak timing information.
    fn decrypt(&self, ciphertext: &str) -> OrcherResult<String>;

    /// Round-trip encrypt/decrypt self-test.
    fn test(&self) -> OrcherResult<()> {
        let test_data = "test_token_verification_12345";
        let encrypted = self.encrypt(test_data)?;
        let decrypted = self.decrypt(&encrypted)?;

        if decrypted == test_data {
            Ok(())
        } else {
            Err(crate::error::OrcherError::Internal {
                message: "TokenEncryptor round-trip test failed".to_string(),
                source: None,
            })
        }
    }
}

/// Factory for creating `TokenEncryptor` instances from configuration.
#[allow(clippy::wrong_self_convention)]
pub trait TokenEncryptorFactory: Send + Sync {
    /// Create a token encryptor from an environment variable containing a base64-encoded key.
    fn from_env(&self, env_var: &str) -> OrcherResult<Box<dyn TokenEncryptor>>;

    /// Create a token encryptor from a base64-encoded key.
    fn from_base64_key(&self, key_b64: &str) -> OrcherResult<Box<dyn TokenEncryptor>>;

    /// Generate a new base64-encoded encryption key.
    fn generate_key(&self) -> String;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock encryptor for testing (simple ROT13-like transform)
    struct MockEncryptor;

    impl TokenEncryptor for MockEncryptor {
        fn encrypt(&self, plaintext: &str) -> OrcherResult<String> {
            // Simple mock: prefix with "ENCRYPTED:" and reverse
            let reversed: String = plaintext.chars().rev().collect();
            Ok(format!("ENCRYPTED:{}", reversed))
        }

        fn decrypt(&self, ciphertext: &str) -> OrcherResult<String> {
            // Simple mock: remove prefix and reverse back
            if let Some(encoded) = ciphertext.strip_prefix("ENCRYPTED:") {
                Ok(encoded.chars().rev().collect())
            } else {
                Err(crate::error::OrcherError::Internal {
                    message: "Invalid mock ciphertext format".to_string(),
                    source: None,
                })
            }
        }
    }

    #[test]
    fn test_token_encryptor_trait() {
        let encryptor = MockEncryptor;
        let plaintext = "test_token";

        let encrypted = encryptor.encrypt(plaintext).unwrap();
        assert_ne!(encrypted, plaintext);
        assert!(encrypted.starts_with("ENCRYPTED:"));

        let decrypted = encryptor.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_token_encryptor_test_method() {
        let encryptor = MockEncryptor;
        assert!(encryptor.test().is_ok());
    }
}
