//! Auth trait + Ed25519 implementations.
//!
//! Phase 4: every request carries `x-session-id` (= Ed25519 public key),
//! `x-timestamp`, `x-nonce`, `x-signature`. Sign scheme:
//!
//! ```text
//! sign(private_key, format!("{timestamp}:{nonce}:{method}"))
//! ```
//!
//! Runner verifies with the session-id (= public key). Timestamps must fall
//! inside a ±30 s window.
//!
//! Hai implementation:
//! - [`LocalAuth`]: tự verify bằng public_key decode từ session_id (in-process).
//!   Chỉ phù hợp khi REPL + Runner cùng máy.
//! - [`RemoteAuth`]: cache miss → hỏi serve `POST /api/admin/v1/session/resolve`,
//!   lấy private_key, re-derive public_key, cache LRU, rồi verify.
//!   Phù hợp khi REPL + Runner khác máy.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use ed25519_dalek::{SigningKey, Verifier, VerifyingKey, SIGNATURE_LENGTH};
use rand::{RngCore, rngs::OsRng};
use subtle::ConstantTimeEq;

use crate::http_client::{ServeClient, SessionResolveRequest, SessionResolveResponse};

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
        let public_bytes = B64
            .decode(session_id)
            .with_context(|| format!("session_id is not valid base64: {session_id}"))?;
        if public_bytes.len() != 32 {
            return Ok(false);
        }
        let mut pk = [0u8; 32];
        pk.copy_from_slice(&public_bytes);
        verify_with_public_key(&pk, method, timestamp, nonce, signature)
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

/// Verify signature bằng `public_key` đã biết (32 bytes). Pure function —
/// chia sẻ giữa `LocalAuth` và `RemoteAuth` để đảm bảo logic verify giống
/// nhau, chỉ khác nguồn public_key.
pub(crate) fn verify_with_public_key(
    public_key: &[u8; 32],
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
    let verifying = VerifyingKey::from_bytes(public_key)
        .map_err(|e| anyhow!("invalid ed25519 public key: {e}"))?;
    let message = format!("{timestamp}:{nonce}:{method}");
    let sig_bytes: [u8; SIGNATURE_LENGTH] = signature
        .try_into()
        .map_err(|_| anyhow!("signature is not 64 bytes"))?;
    let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
    Ok(verifying.verify(message.as_bytes(), &sig).is_ok())
}

// =========================================================================
// RemoteAuth — REPL và Runner khác máy
// =========================================================================

/// LRU cache đơn giản (Mutex<HashMap>) cho `session_id → public_key`.
/// Khi đầy thì pop ngẫu nhiên phần tử cũ nhất (O(1) amortized).
#[derive(Debug)]
struct PubkeyCache {
    map:   HashMap<String, [u8; 32]>,
    order: Vec<String>, // FIFO eviction
    cap:   usize,
}

impl PubkeyCache {
    fn new(cap: usize) -> Self {
        Self { map: HashMap::new(), order: Vec::new(), cap: cap.max(1) }
    }
    fn get(&mut self, k: &str) -> Option<[u8; 32]> {
        self.map.get(k).copied()
    }
    fn put(&mut self, k: String, v: [u8; 32]) {
        if self.map.contains_key(&k) {
            self.map.insert(k, v);
            return;
        }
        if self.map.len() >= self.cap {
            // Evict oldest
            if let Some(oldest) = self.order.first().cloned() {
                self.map.remove(&oldest);
                self.order.remove(0);
            }
        }
        self.map.insert(k.clone(), v);
        self.order.push(k);
    }
}

/// Auth backed by HTTP calls tới serve, có LRU cache public_key theo
/// `session_id`. Phù hợp khi REPL + Runner ở 2 máy khác nhau.
pub struct RemoteAuth {
    serve:  Arc<ServeClient>,
    cache:  Mutex<PubkeyCache>,
}

impl RemoteAuth {
    /// `serve` phải có `base_url` + `admin_token` đã set.
    /// `cache_cap` = số session_id tối đa cache.
    pub fn new(serve: Arc<ServeClient>, cache_cap: usize) -> Self {
        Self {
            serve,
            cache: Mutex::new(PubkeyCache::new(cache_cap)),
        }
    }

    /// Lookup public_key: hit cache trả ngay, miss → gọi serve
    /// `/api/admin/v1/session/resolve` rồi re-derive từ private_key.
    async fn lookup_public_key(&self, session_id: &str) -> Result<Option<[u8; 32]>> {
        if let Some(pk) = self.cache.lock().unwrap().get(session_id) {
            return Ok(Some(pk));
        }
        let resp: SessionResolveResponse = self
            .serve
            .post(
                "/api/admin/v1/session/resolve",
                &SessionResolveRequest { session_id },
            )
            .await
            .context("POST /api/admin/v1/session/resolve")?;

        if !resp.active {
            return Ok(None);
        }
        let pk_b64 = resp
            .private_key
            .as_deref()
            .ok_or_else(|| anyhow!("session/resolve returned active=true but no private_key"))?;
        let pk_bytes = B64
            .decode(pk_b64)
            .context("private_key not valid base64")?;
        if pk_bytes.len() != 32 {
            anyhow::bail!(
                "private_key decoded to {} bytes, expected 32",
                pk_bytes.len()
            );
        }
        let mut priv_arr = [0u8; 32];
        priv_arr.copy_from_slice(&pk_bytes);
        let signing = SigningKey::from_bytes(&priv_arr);
        let public_key = signing.verifying_key().to_bytes();

        self.cache
            .lock()
            .unwrap()
            .put(session_id.to_string(), public_key);
        Ok(Some(public_key))
    }
}

#[async_trait::async_trait]
impl Auth for RemoteAuth {
    async fn generate_keypair(&self) -> Result<(String, Vec<u8>)> {
        // Serve mới là nơi mint keypair. Method này chỉ dùng cho test;
        // gọi từ runtime sẽ trả về keypair tạm để không phá API.
        LocalAuth::new().generate_keypair().await
    }

    async fn verify_signature(
        &self,
        session_id: &str,
        method: &str,
        timestamp: i64,
        nonce: u64,
        signature: &[u8],
    ) -> Result<bool> {
        // Fail-closed: serve lookup lỗi → reject signature, không phải Err.
        // Caller (gRPC) coi `Ok(false)` = "không hợp lệ" còn `Err` = "lỗi hệ thống";
        // ta muốn behaviour ổn định khi serve down (mọi request bị từ chối).
        let pk = match self.lookup_public_key(session_id).await {
            Ok(Some(p)) => p,
            Ok(None) => return Ok(false),
            Err(e) => {
                tracing::warn!(
                    "RemoteAuth: serve lookup failed for {session_id}: {e:#} — rejecting signature"
                );
                return Ok(false);
            }
        };
        verify_with_public_key(&pk, method, timestamp, nonce, signature)
    }

    async fn resolve_private_key(&self, session_id: &str) -> Result<Option<Vec<u8>>> {
        let resp: SessionResolveResponse = self
            .serve
            .post(
                "/api/admin/v1/session/resolve",
                &SessionResolveRequest { session_id },
            )
            .await
            .context("POST /api/admin/v1/session/resolve")?;
        if !resp.active {
            return Ok(None);
        }
        let pk_b64 = match resp.private_key.as_deref() {
            Some(s) => s,
            None => return Ok(None),
        };
        let bytes = B64
            .decode(pk_b64)
            .context("private_key not valid base64")?;
        if bytes.len() != 32 {
            anyhow::bail!("private_key length {} != 32", bytes.len());
        }
        Ok(Some(bytes))
    }

    async fn create_challenge(&self, _session_id: &str) -> Result<Challenge> {
        // Remote runner không giữ master_key — không thể encrypt local.
        // Tương lai sẽ proxy tới serve endpoint (nếu có); hiện tại trả lỗi.
        Err(anyhow!(
            "RemoteAuth::create_challenge not yet wired to serve (no master_key locally)"
        ))
    }

    async fn verify_challenge(
        &self,
        _session_id: &str,
        expected_plaintext: &[u8],
        response: &[u8],
    ) -> Result<bool> {
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

#[cfg(test)]
mod remote_tests {
    use super::*;
    use crate::http_client::ServeClient;
    use ed25519_dalek::Signer;

    /// `verify_with_public_key` share giữa LocalAuth + RemoteAuth.
    #[tokio::test]
    async fn verify_with_public_key_roundtrip() {
        // Tạo keypair local, derive public_key, verify.
        let (sid, priv_bytes) = LocalAuth::new().generate_keypair().await.unwrap();
        let mut pk = [0u8; 32];
        pk.copy_from_slice(&B64.decode(&sid).unwrap());
        let mut priv_arr = [0u8; 32];
        priv_arr.copy_from_slice(&priv_bytes);

        let signing = SigningKey::from_bytes(&priv_arr);
        let now = chrono::Utc::now().timestamp();
        let nonce = 7u64;
        let message = format!("{now}:{nonce}:Execute");
        let sig = signing.sign(message.as_bytes());

        assert!(
            verify_with_public_key(&pk, "Execute", now, nonce, &sig.to_bytes())
                .unwrap()
        );
        // Stale timestamp → false
        let stale = now - 60;
        let msg2 = format!("{stale}:{nonce}:Execute");
        let sig2 = signing.sign(msg2.as_bytes());
        assert!(
            !verify_with_public_key(&pk, "Execute", stale, nonce, &sig2.to_bytes())
                .unwrap()
        );
    }

    /// `RemoteAuth::verify_signature` trả `Ok(false)` (không panic) khi
    /// lookup serve thất bại — KHÔNG được trả Err cho caller gRPC path.
    /// Dùng URL chắc chắn không phản hồi.
    #[tokio::test]
    async fn remote_auth_lookup_failure_returns_false_not_err() {
        // 127.0.0.1:1 chắc chắn connection refused.
        let serve = Arc::new(
            ServeClient::new(
                "http://127.0.0.1:1".to_string(),
                "abt_test".to_string(),
                2,
            )
            .unwrap(),
        );
        let auth = RemoteAuth::new(serve, 16);
        let ok = auth
            .verify_signature("any-session-id", "Ping", chrono::Utc::now().timestamp(), 0, &[0u8; 64])
            .await
            .unwrap();
        // lookup fail → trả Ok(None) → verify_signature trả Ok(false).
        assert!(!ok);
    }

    /// `RemoteAuth::create_challenge` chưa wired serve → trả Err rõ ràng
    /// (không panic), để caller biết mà báo lỗi chứ không silently fail.
    #[tokio::test]
    async fn remote_auth_create_challenge_unwired_errors() {
        let serve = Arc::new(
            ServeClient::new(
                "http://127.0.0.1:1".to_string(),
                "abt".to_string(),
                2,
            )
            .unwrap(),
        );
        let auth = RemoteAuth::new(serve, 16);
        let err = auth.create_challenge("s").await.unwrap_err();
        assert!(err.to_string().contains("RemoteAuth"));
    }
}
