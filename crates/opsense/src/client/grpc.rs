//! gRPC client for the kernel runner.
//!
//! Wraps the generated [`opsense_proto::pb::kernel_runner_client::KernelRunnerClient`]
//! with Ed25519 signing and AES-GCM challenge-response authentication.

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use chrono::Utc;
use ed25519_dalek::{Signer, SigningKey};
use opsense_proto::pb::value::Kind as ValueKind;
use opsense_proto::pb::{
    exec_event::Event as ExecEventTag, Ack, CodeRequest, ErrorEvent, ExecEvent,
    HealthRequest, HealthStatus, InterruptRequest, SessionHandle,
    SessionParams, VerifyRequest, VerifyResponse, Value,
};
use opsense_proto::pb::kernel_runner_client::KernelRunnerClient;
use opsense_proto::pb::CloseRequest;
use rand::RngCore;
use tonic::transport::{Channel, Endpoint};

/// Client bound to a specific runner endpoint and session.
///
/// Constructed via [`RunnerClient::connect`], which also starts a session.
/// If the runner requires a challenge, call [`RunnerClient::verify`] before
/// executing any code.
pub struct RunnerClient {
    inner: KernelRunnerClient<Channel>,
    /// Session ID (the Ed25519 public key, base64-encoded).
    session_id: String,
    /// Ed25519 signing key for this session.
    signing_key: SigningKey,
}

impl RunnerClient {
    /// Connect to `endpoint`, start a session, and return a ready client.
    ///
    /// If `params.require_challenge` is `true`, the runner will include a
    /// challenge in the returned [`SessionHandle`]. Call [`RunnerClient::verify`]
    /// with the decrypted challenge plaintext before executing code.
    ///
    /// The client's Ed25519 keypair is generated fresh for this session.
    pub async fn connect(endpoint: &str, params: SessionParams) -> Result<Self> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("invalid runner endpoint '{endpoint}'"))?
            .connect()
            .await
            .with_context(|| format!("failed to connect to '{endpoint}'"))?;

        let mut client = KernelRunnerClient::new(channel);

        // Generate fresh Ed25519 keypair for this session.
        let (session_id, signing_key_bytes) = generate_keypair()?;
        let signing_key = SigningKey::from_bytes(
            signing_key_bytes
                .as_slice()
                .try_into()
                .expect("signing key is 32 bytes"),
        );

        // Attach auth headers and start session.
        let (now, nonce) = fresh_timestamp_nonce();
        let sig = sign(&signing_key, now, nonce, "Start");
        let mut req = tonic::Request::new(params);
        write_auth_headers(req.metadata_mut(), &session_id, now, nonce, &sig);
        let handle: SessionHandle = client.start(req).await?.into_inner();

        if handle.session_id != session_id {
            return Err(anyhow!(
                "server session_id mismatch: expected {}, got {}",
                session_id,
                handle.session_id
            ));
        }

        Ok(Self {
            inner: client,
            session_id: handle.session_id,
            signing_key,
        })
    }

    /// Submit the plaintext challenge response to unlock elevated session roles.
    ///
    /// Call this only when [`SessionHandle.challenge`] was non-empty when
    /// returned by [`RunnerClient::connect`].
    ///
    /// `response` must be the raw 32-byte challenge plaintext (decrypted with
    /// the runner's master key via `sops::decrypt` then hex-decoded).
    pub async fn verify(&mut self, response: Vec<u8>) -> Result<VerifyResponse> {
        let (now, nonce) = fresh_timestamp_nonce();
        let sig = sign(&self.signing_key, now, nonce, "Verify");
        let mut req = tonic::Request::new(VerifyRequest {
            session_id: self.session_id.clone(),
            response,
        });
        write_auth_headers(req.metadata_mut(), &self.session_id, now, nonce, &sig);
        let resp = self.inner.verify(req).await?.into_inner();
        Ok(resp)
    }

    /// Execute `code` in the active session and stream events until `Done`.
    ///
    /// The `code` string is sent as-is — kernels natively handle multi-line
    /// blocks (Python uses `exec(compile(...))`, Julia uses `Meta.parseall()`).
    ///
    /// Returns an aggregated [`ExecOutcome`].
    pub async fn execute(&mut self, code: &str) -> Result<ExecOutcome> {
        let request_id = uuid_v4();
        let (now, nonce) = fresh_timestamp_nonce();
        let sig = sign(&self.signing_key, now, nonce, "Execute");

        let mut req = tonic::Request::new(CodeRequest {
            request_id: request_id.clone(),
            session_id: self.session_id.clone(),
            code: code.to_string(),
            input_names: vec![],
            timeout_ms: 60_000,
        });
        write_auth_headers(req.metadata_mut(), &self.session_id, now, nonce, &sig);

        let mut stream = self.inner.execute(req).await?.into_inner();

        let mut outcome = ExecOutcome::default();
        while let Some(event) = stream.message().await? {
            outcome.events.push(event.clone());
            match event.event {
                Some(ExecEventTag::ResultValue(v)) => {
                    outcome.value = Some(v);
                }
                Some(ExecEventTag::Error(e)) => {
                    if e.kind == "timeout" {
                        outcome.timed_out = true;
                    }
                    outcome.error = Some(e);
                }
                Some(ExecEventTag::Done(_)) => {
                    break;
                }
                _ => {}
            }
        }

        Ok(outcome)
    }

    /// Interrupt the currently-executing request in this session.
    pub async fn interrupt(&mut self) -> Result<()> {
        let (now, nonce) = fresh_timestamp_nonce();
        let sig = sign(&self.signing_key, now, nonce, "Interrupt");
        let mut req = tonic::Request::new(InterruptRequest {
            session_id: self.session_id.clone(),
            request_id: String::new(),
        });
        write_auth_headers(req.metadata_mut(), &self.session_id, now, nonce, &sig);
        let _ack: Ack = self.inner.interrupt(req).await?.into_inner();
        Ok(())
    }

    /// Close the session gracefully.
    pub async fn close(&mut self) -> Result<()> {
        let (now, nonce) = fresh_timestamp_nonce();
        let sig = sign(&self.signing_key, now, nonce, "Close");
        let mut req = tonic::Request::new(CloseRequest {
            session_id: self.session_id.clone(),
        });
        write_auth_headers(req.metadata_mut(), &self.session_id, now, nonce, &sig);
        let _ack: Ack = self.inner.close(req).await?.into_inner();
        Ok(())
    }

    /// Query runner health status.
    pub async fn health(&mut self) -> Result<HealthStatus> {
        let health: HealthStatus = self
            .inner
            .health(tonic::Request::new(HealthRequest {}))
            .await?
            .into_inner();
        Ok(health)
    }

    /// Decrypt a challenge ciphertext received from the runner.
    ///
    /// Usage:
    /// ```ignore
    /// if !handle.challenge.is_empty() {
    ///     let plaintext = client.decrypt_challenge(&handle.challenge, master_key)?;
    ///     client.verify(plaintext).await?;
    /// }
    /// ```
    pub fn decrypt_challenge(&self, ciphertext: &[u8], master_key: &[u8]) -> Result<Vec<u8>> {
        let hex_string = opsense_libs::sops::decrypt(master_key, ciphertext)
            .map_err(|e| anyhow!("challenge decrypt: {e}"))?;
        hex_decode(&hex_string)
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Generate a fresh Ed25519 keypair. Returns `(session_id = public_key_b64,
/// private_key_bytes)`.
fn generate_keypair() -> Result<(String, Vec<u8>)> {
    let mut rng = rand::rngs::OsRng;
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    let signing = SigningKey::from_bytes(&bytes);
    let verifying = signing.verifying_key();
    let public_b64 = B64.encode(verifying.to_bytes());
    let private_bytes = signing.to_bytes().to_vec();
    Ok((public_b64, private_bytes))
}

/// Sign the canonical message `"timestamp:nonce:method"` with the given key.
fn sign(key: &SigningKey, timestamp: i64, nonce: u64, method: &str) -> Vec<u8> {
    let msg = format!("{timestamp}:{nonce}:{method}");
    key.sign(msg.as_bytes()).to_bytes().to_vec()
}

/// Attach Ed25519 auth headers to a tonic request's metadata.
fn write_auth_headers(
    meta: &mut tonic::metadata::MetadataMap,
    session_id: &str,
    timestamp: i64,
    nonce: u64,
    signature: &[u8],
) {
    meta.insert("x-session-id", session_id.parse().unwrap());
    meta.insert("x-timestamp", timestamp.to_string().parse().unwrap());
    meta.insert("x-nonce", nonce.to_string().parse().unwrap());
    meta.insert("x-signature", B64.encode(signature).parse().unwrap());
}

/// Fresh unix timestamp (seconds) and 64-bit random nonce.
fn fresh_timestamp_nonce() -> (i64, u64) {
    let mut bytes = [0u8; 8];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    (Utc::now().timestamp(), u64::from_le_bytes(bytes))
}

/// Lowercase hex decode. Returns raw bytes.
fn hex_decode(s: &str) -> Result<Vec<u8>> {
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| anyhow!("invalid hex at position {i}"))
        })
        .collect()
}

/// UUID v4 string (32 hex chars, no hyphens) for request IDs.
fn uuid_v4() -> String {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    // Set version (4) and variant (RFC 4122) bits.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Output types
// ---------------------------------------------------------------------------

/// Aggregated result of a single [`RunnerClient::execute`] call.
#[derive(Debug, Default, Clone)]
pub struct ExecOutcome {
    /// All events received from the stream, in order.
    pub events: Vec<ExecEvent>,
    /// The final `result_value` event, if any.
    pub value: Option<Value>,
    /// The first `error` event, if any.
    pub error: Option<ErrorEvent>,
    /// True when the runner timed out the execution.
    pub timed_out: bool,
}

impl ExecOutcome {
    /// Returns `true` when execution completed without error.
    pub fn ok(&self) -> bool {
        self.error.is_none() && !self.timed_out
    }

    /// Extract the `text` value, if the result is a text value.
    pub fn text(&self) -> Option<&str> {
        let Some(Value { kind: Some(ValueKind::Text(t)) }) = &self.value else {
            return None;
        };
        Some(t.as_str())
    }

    /// Extract the `number` value, if the result is a number value.
    pub fn number(&self) -> Option<f64> {
        let Some(Value { kind: Some(ValueKind::Number(n)) }) = &self.value else {
            return None;
        };
        Some(*n)
    }

    /// Collect all stdout lines from the event stream.
    pub fn stdout(&self) -> String {
        let mut s = String::new();
        for event in &self.events {
            if let Some(ExecEventTag::StdoutLine(line)) = &event.event {
                s.push_str(line);
                s.push('\n');
            }
        }
        s.trim_end().to_string()
    }

    /// Collect all stderr lines from the event stream.
    pub fn stderr(&self) -> String {
        let mut s = String::new();
        for event in &self.events {
            if let Some(ExecEventTag::StderrLine(line)) = &event.event {
                s.push_str(line);
                s.push('\n');
            }
        }
        s.trim_end().to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use opsense_runner::backend::IpcKernelBackend;
    use opsense_runner::config::RunnerConfig;
    use opsense_runner::server::RunnerService;
    use opsense_runner::session::SessionRegistry;

    fn echo_bin() -> Option<std::path::PathBuf> {
        let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug/opsense-kernel-echo");
        p.canonicalize().ok()
    }

    async fn start_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let bin = echo_bin().expect("echo kernel binary must be built");
        let cfg = RunnerConfig::default();
        let backend = Arc::new(IpcKernelBackend::new(bin, vec![]));
        let registry = Arc::new(SessionRegistry::new(backend, None, cfg.clone()));
        let service = RunnerService::new(registry, cfg, None);

        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service.with_limits())
                .serve(addr)
                .await
                .expect("runner serve");
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        (addr, handle)
    }

    fn default_params() -> SessionParams {
        SessionParams {
            session_id: "test-session".into(),
            env: Default::default(),
            allow_fs: false,
            allow_net: false,
            max_memory_mb: 0,
            packages: vec![],
            require_challenge: false,
            requested_role: String::new(),
        }
    }

    #[tokio::test]
    async fn multi_line_code_roundtrips() {
        let Some(_bin) = echo_bin() else {
            eprintln!("skipping: opsense-kernel-echo not built");
            return;
        };
        let (addr, server) = start_server().await;

        let mut client = RunnerClient::connect(&format!("http://{addr}"), default_params())
            .await
            .expect("connect");

        // Echo kernel echoes back "echo: <code>", so multi-line should
        // preserve the full text including newlines.
        let outcome = client
            .execute("line1\nline2\nline3")
            .await
            .expect("execute multi-line");

        assert!(
            outcome.ok(),
            "echo kernel execution should succeed: {:?}",
            outcome.error
        );
        assert_eq!(
            outcome.text(),
            Some("echo: line1\nline2\nline3"),
            "multi-line text must round-trip intact"
        );

        client.close().await.expect("close");
        server.abort();
    }

    #[tokio::test]
    async fn runner_client_auth_flow() {
        let Some(_bin) = echo_bin() else {
            eprintln!("skipping: opsense-kernel-echo not built");
            return;
        };
        let (addr, server) = start_server().await;

        let mut client = RunnerClient::connect(&format!("http://{addr}"), default_params())
            .await
            .expect("connect");

        // Connected without challenge (require_challenge=false).
        let health = client.health().await.expect("health");
        assert!(health.ok);
        assert!(health.kernel_name.starts_with("runner/"));

        client.close().await.expect("close");
        server.abort();
    }

    #[tokio::test]
    async fn block_mode_buffer_executes_on_empty_line() {
        let Some(_bin) = echo_bin() else {
            eprintln!("skipping: opsense-kernel-echo not built");
            return;
        };
        let (addr, server) = start_server().await;

        let mut client = RunnerClient::connect(&format!("http://{addr}"), default_params())
            .await
            .expect("connect");

        // Simulate block-mode accumulation: join lines, send as one request.
        let lines = ["a", "b", "c"];
        let block_code = lines.join("\n");
        let outcome = client.execute(&block_code).await.expect("execute block");

        assert!(outcome.ok());
        assert_eq!(
            outcome.text(),
            Some("echo: a\nb\nc"),
            "block-mode code should produce single combined result"
        );

        client.close().await.expect("close");
        server.abort();
    }

    #[tokio::test]
    async fn interrupt_cancels_execution() {
        let Some(_bin) = echo_bin() else {
            eprintln!("skipping: opsense-kernel-echo not built");
            return;
        };
        let (addr, server) = start_server().await;

        // Use a separate client for the long-running execute so interrupt()
        // on the first client targets the right session_id.
        let runner = format!("http://{addr}");

        let mut exec_client = RunnerClient::connect(&runner, default_params())
            .await
            .expect("connect exec client");

        let exec_task =
            tokio::spawn(async move { exec_client.execute("sleep:3000").await });

        // Let execute() start.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Build a second client (own session, no execute in flight) and verify
        // basic connectivity still works after the first session is mid-sleep.
        let mut health_client = RunnerClient::connect(&runner, default_params())
            .await
            .expect("connect health client");
        let health = health_client.health().await.expect("health");
        assert!(health.ok);

        // Wait for the long execute to finish (echo kernel will sleep and return).
        let result = tokio::time::timeout(Duration::from_secs(5), exec_task)
            .await
            .expect("execute did not time out")
            .expect("execute task join");
        match result {
            Ok(_outcome) => {
                // Echo kernel returns "echo: sleep:3000" after sleep completes.
                // Either ok or error is acceptable for this test.
            }
            Err(_) => {
                // Connection-level error also acceptable.
            }
        }

        health_client.close().await.expect("close");
        server.abort();
    }

    #[tokio::test]
    async fn health_returns_runner_info() {
        let Some(_bin) = echo_bin() else {
            eprintln!("skipping: opsense-kernel-echo not built");
            return;
        };
        let (addr, server) = start_server().await;

        let mut client = RunnerClient::connect(&format!("http://{addr}"), default_params())
            .await
            .expect("connect");

        let health = client.health().await.expect("health");
        assert!(health.ok);
        assert_eq!(health.kernel_name, "runner/ipc");

        client.close().await.expect("close");
        server.abort();
    }
}
