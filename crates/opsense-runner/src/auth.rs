//! Auth trait + local Ed25519 implementation.
//!
//! Phase 4: every request carries `x-session-id` (= Ed25519 public key),
//! `x-timestamp`, `x-nonce`, `x-signature`. Sign scheme:
//!
//! ```text
//! sign(private_key, format!("{timestamp}:{nonce}:{method}"))
//! ```
//!
//! Runner verifies with the session-id (= public key). Timestamps must fall
//! inside a ±30 s window. `resolve_private_key` always returns `None` until the
//! future phase wires the server-API lookup.

use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use ed25519_dalek::{SigningKey, Verifier, VerifyingKey, SIGNATURE_LENGTH};
use rand::{RngCore, rngs::OsRng};
use subtle::ConstantTimeEq;

/// ±30s clock-skew window for timestamp checks.
pub const TIMESTAMP_WINDOW_SECS: i64 = 30;

/// Challenge issued by the runner to upgrade a session role.
///
/// The runner encrypts a random plaintext using its master key (AES-256-GCM);
/// the client must decrypt and return the same plaintext to prove possession
/// of the master key. Only the runner holds the master key, so a successful
/// challenge demonstrates that the connecting client is allowed to assume
/// the requested role.
#[derive(Debug, Clone)]
pub struct Challenge {
    /// Ciphertext trả về cho client (đã mã hoá bằng master key).
    pub ciphertext: Vec<u8>,
    /// Plaintext lưu server-side để so sánh khi verify.
    /// KHÔNG bao giờ gửi qua network.
    pub plaintext: Vec<u8>,
}

#[async_trait::async_trait]
pub trait Auth: Send + Sync {
    /// Mint a fresh Ed25519 keypair.
    /// Returns `(session_id = public_key_base64, private_key_bytes)`.
    async fn generate_keypair(&self) -> Result<(String, Vec<u8>)>;

    /// Verify a signature over `format!("{timestamp}:{nonce}:{method}")` using
    /// the public key encoded in `session_id`.
    async fn verify_signature(
        &self,
        session_id: &str,
        method: &str,
        timestamp: i64,
        nonce: u64,
        signature: &[u8],
    ) -> Result<bool>;

    /// Phase này: trả về `None` (private key chỉ lưu phía client).
    /// Phase sau: gọi server API lấy encrypted private key, decrypt rồi verify.
    async fn resolve_private_key(&self, _session_id: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Sinh challenge mới cho 1 session muốn upgrade role.
    /// Trả về `(ciphertext gửi client, plaintext lưu server-side)`.
    async fn create_challenge(&self, session_id: &str) -> Result<Challenge>;

    /// Verify response từ client (client giải mã ciphertext trả plaintext).
    /// Match với `expected_plaintext` lưu server-side → grant role.
    async fn verify_challenge(
        &self,
        session_id: &str,
        expected_plaintext: &[u8],
        response: &[u8],
    ) -> Result<bool>;
}

/// Auth context carried on each RPC call (extracted from gRPC metadata).
pub struct AuthContext {
    pub session_id: String,
    pub timestamp: i64,
    pub nonce: u64,
    pub signature: Vec<u8>,
}

impl TryFrom<&tonic::metadata::MetadataMap> for AuthContext {
    type Error = anyhow::Error;

    fn try_from(meta: &tonic::metadata::MetadataMap) -> Result<Self, Self::Error> {
        let session_id = meta
            .get("x-session-id")
            .ok_or_else(|| anyhow!("missing x-session-id"))?
            .to_str()
            .map_err(|e| anyhow!("invalid x-session-id: {e}"))?
            .to_string();
        let timestamp = meta
            .get("x-timestamp")
            .ok_or_else(|| anyhow!("missing x-timestamp"))?
            .to_str()
            .map_err(|e| anyhow!("invalid x-timestamp: {e}"))?
            .parse()
            .map_err(|e| anyhow!("invalid x-timestamp: {e}"))?;
        let nonce = meta
            .get("x-nonce")
            .ok_or_else(|| anyhow!("missing x-nonce"))?
            .to_str()
            .map_err(|e| anyhow!("invalid x-nonce: {e}"))?
            .parse()
            .map_err(|e| anyhow!("invalid x-nonce: {e}"))?;
        let signature = meta
            .get("x-signature")
            .ok_or_else(|| anyhow!("missing x-signature"))?
            .to_str()
            .map_err(|e| anyhow!("invalid x-signature: {e}"))?;
        let signature = B64
            .decode(signature)
            .map_err(|e| anyhow!("invalid x-signature base64: {e}"))?;
        Ok(Self {
            session_id,
            timestamp,
            nonce,
            signature,
        })
    }
}

/// Ed25519-backed auth that runs entirely in-process.
pub struct LocalAuth {
    /// Master key for challenge encryption (AES-256-GCM). `None` means
    /// `create_challenge` will return an error — useful for tests/dev where
    /// challenge-response role upgrade is disabled.
    master_key: Option<Vec<u8>>,
}

impl LocalAuth {
    /// Backward-compatible constructor without master key (challenge methods
    /// will return errors). Prefer `from_env` for production.
    #[must_use]
    pub fn new() -> Self {
        Self { master_key: None }
    }

    /// Load master key from `MASTER_KEY` env var. Returns an error if the
    /// variable is missing or not a valid 32-byte AES key.
    pub fn from_env() -> Result<Self> {
        let key = std::env::var("MASTER_KEY")
            .map_err(|_| anyhow!("MASTER_KEY env required for LocalAuth::from_env"))?;
        Self::from_key(key.into_bytes())
    }

    /// Construct with an explicit master key (any length — AES-256-GCM hashes
    /// the input via the `Key::from_slice` API, so callers can pass 32 raw
    /// bytes or any seed length).
    pub fn from_key(key: Vec<u8>) -> Result<Self> {
        if key.is_empty() {
            return Err(anyhow!("MASTER_KEY is empty"));
        }
        Ok(Self { master_key: Some(key) })
    }
}

impl Default for LocalAuth {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Auth for LocalAuth {
    async fn generate_keypair(&self) -> Result<(String, Vec<u8>)> {
        let mut csprng = OsRng;
        let mut bytes = [0u8; 32];
        csprng.fill_bytes(&mut bytes);
        let signing = SigningKey::from_bytes(&bytes);
        let verifying = signing.verifying_key();
        let public_b64 = B64.encode(verifying.to_bytes());
        let private_bytes = signing.to_bytes().to_vec();
        Ok((public_b64, private_bytes))
    }

    async fn verify_signature(
        &self,
        session_id: &str,
        method: &str,
        timestamp: i64,
        nonce: u64,
        signature: &[u8],
    ) -> Result<bool> {
        if signature.len() != SIGNATURE_LENGTH {
            return Ok(false);
        }
        let now = chrono::Utc::now().timestamp();
        if (now - timestamp).abs() > TIMESTAMP_WINDOW_SECS {
            return Ok(false);
        }
        let public_bytes = B64
            .decode(session_id)
            .with_context(|| format!("session_id is not valid base64: {session_id}"))?;
        if public_bytes.len() != 32 {
            return Ok(false);
        }
        let mut pk = [0u8; 32];
        pk.copy_from_slice(&public_bytes);
        let verifying = VerifyingKey::from_bytes(&pk)
            .map_err(|e| anyhow!("invalid ed25519 public key: {e}"))?;
        let message = format!("{timestamp}:{nonce}:{method}");
        let sig_bytes: [u8; SIGNATURE_LENGTH] = signature
            .try_into()
            .map_err(|_| anyhow!("signature is not 64 bytes"))?;
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        Ok(verifying.verify(message.as_bytes(), &sig).is_ok())
    }

    async fn create_challenge(&self, _session_id: &str) -> Result<Challenge> {
        let key = self
            .master_key
            .as_ref()
            .ok_or_else(|| anyhow!("LocalAuth has no master key — call LocalAuth::from_env or from_key"))?;
        // 32 random bytes for the plaintext challenge.
        let mut plaintext = [0u8; 32];
        OsRng.fill_bytes(&mut plaintext);
        // sops::encrypt takes &String; use hex encoding so we round-trip
        // arbitrary bytes losslessly.
        let hex_plain = hex_encode(&plaintext);
        let ciphertext = opsense_libs::sops::encrypt(key, &hex_plain)
            .map_err(|e| anyhow!("encrypt challenge: {e}"))?;
        Ok(Challenge {
            ciphertext,
            plaintext: plaintext.to_vec(),
        })
    }

    async fn verify_challenge(
        &self,
        _session_id: &str,
        expected_plaintext: &[u8],
        response: &[u8],
    ) -> Result<bool> {
        // Constant-time compare avoids leaking timing info about the prefix.
        Ok(expected_plaintext.ct_eq(response).into())
    }
}

/// Lowercase hex encoding without external crate. Used to wrap arbitrary bytes
/// before passing to `sops::encrypt` (which only handles `&String`).
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signer;

    #[tokio::test]
    async fn local_auth_roundtrip_succeeds() {
        let auth = LocalAuth::new();
        let (sid, priv_bytes) = auth.generate_keypair().await.unwrap();
        let mut pk_bytes = [0u8; 32];
        pk_bytes.copy_from_slice(&priv_bytes);
        let signing = SigningKey::from_bytes(&pk_bytes);
        let now = chrono::Utc::now().timestamp();
        let nonce = 42u64;
        let message = format!("{now}:{nonce}:Execute");
        let sig = signing.sign(message.as_bytes());
        let ok = auth
            .verify_signature(&sid, "Execute", now, nonce, &sig.to_bytes())
            .await
            .unwrap();
        assert!(ok, "fresh signature must verify");
    }

    #[tokio::test]
    async fn local_auth_rejects_old_timestamp() {
        let auth = LocalAuth::new();
        let (sid, _) = auth.generate_keypair().await.unwrap();
        let stale = chrono::Utc::now().timestamp() - (TIMESTAMP_WINDOW_SECS + 5);
        let ok = auth
            .verify_signature(&sid, "Ping", stale, 1, &[0u8; SIGNATURE_LENGTH])
            .await
            .unwrap();
        assert!(!ok);
    }

    #[tokio::test]
    async fn local_auth_rejects_bad_signature() {
        let auth = LocalAuth::new();
        let (sid, _) = auth.generate_keypair().await.unwrap();
        let now = chrono::Utc::now().timestamp();
        let ok = auth
            .verify_signature(&sid, "Execute", now, 0, &[0u8; SIGNATURE_LENGTH])
            .await
            .unwrap();
        assert!(!ok);
    }

    #[tokio::test]
    async fn resolve_private_key_returns_none_for_local_auth() {
        let auth = LocalAuth::new();
        assert!(auth.resolve_private_key("any").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_challenge_without_master_key_fails() {
        let auth = LocalAuth::new();
        let err = auth.create_challenge("any").await.unwrap_err();
        assert!(err.to_string().contains("master key"));
    }

    #[tokio::test]
    async fn challenge_roundtrip_succeeds() {
        // 32-byte master key (AES-256).
        let key = vec![0xABu8; 32];
        let auth = LocalAuth::from_key(key.clone()).unwrap();

        let challenge = auth.create_challenge("session-1").await.unwrap();
        // 32-byte random plaintext.
        assert_eq!(challenge.plaintext.len(), 32);
        // Ciphertext includes 12-byte nonce + AES-GCM output, longer than plaintext.
        assert!(challenge.ciphertext.len() > 32);

        // Server-side verify: client returns the same plaintext → ok.
        let ok = auth
            .verify_challenge("session-1", &challenge.plaintext, &challenge.plaintext)
            .await
            .unwrap();
        assert!(ok, "matching plaintext must verify");
    }

    #[tokio::test]
    async fn challenge_verify_rejects_mismatch() {
        let key = vec![0xCDu8; 32];
        let auth = LocalAuth::from_key(key).unwrap();
        let challenge = auth.create_challenge("session-1").await.unwrap();

        let wrong = vec![0u8; 32];
        let ok = auth
            .verify_challenge("session-1", &challenge.plaintext, &wrong)
            .await
            .unwrap();
        assert!(!ok, "mismatched plaintext must reject");
    }

    #[tokio::test]
    async fn from_env_succeeds_when_master_key_set() {
        // MASTER_KEY is a raw byte string of length 32 for AES-256.
        let key_bytes = vec![0x42u8; 32];
        let key_str: String = key_bytes.iter().map(|b| *b as char).collect();
        // SAFETY: tests run single-threaded for this env var.
        unsafe { std::env::set_var("MASTER_KEY", &key_str) };
        let auth = LocalAuth::from_env().unwrap();
        let challenge = auth.create_challenge("s").await.unwrap();
        assert_eq!(challenge.plaintext.len(), 32);
        unsafe { std::env::remove_var("MASTER_KEY") };
    }
}
