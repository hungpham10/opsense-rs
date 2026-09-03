//! Session registry + idle sweeper.
//!
//! Every RPC that carries a `session_id` implicitly calls `touch()` so
//! callers never need an explicit `:session ping` (implicit keepalive).
//! A background task sweeps expired sessions every `sweep_interval_secs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use opsense_proto::pb::{CodeRequest, SessionParams};

use crate::auth::{AuthContext, Auth};
use crate::backend::{ExecOutcome, HealthInfo, KernelBackend};
use crate::config::RunnerConfig;

/// Metadata tracked for every live session.
///
/// The underlying process + `KernelConnection` live inside the
/// [`KernelBackend`]; the registry owns only the bookkeeping and the
/// idle-sweep decision.
#[derive(Debug)]
pub struct SessionMeta {
    pub last_active: Instant,
    pub started_at: DateTime<Utc>,
    pub labels: HashMap<String, String>,
    /// Role currently granted (None = unprivileged). Set after a successful
    /// challenge-response via [`SessionRegistry::verify`].
    pub role: Option<String>,
    /// Role the client asked for in `SessionParams.requested_role`. Held
    /// until verify completes; promoted to `role` on success.
    pub requested_role: Option<String>,
    /// Plaintext challenge issued by the runner; server compares it against
    /// the response from `Verify`. Cleared after a successful verify.
    pub pending_challenge: Option<Vec<u8>>,
}

/// Registry of live sessions. Layers auth + idle sweep on top of a
/// [`KernelBackend`].
pub struct SessionRegistry {
    backend: Arc<dyn KernelBackend>,
    auth: Option<Arc<dyn Auth>>,
    cfg: RunnerConfig,
    sessions: Arc<Mutex<HashMap<String, SessionMeta>>>,
    sweeper: Option<JoinHandle<()>>,
}

impl SessionRegistry {
    #[must_use]
    pub fn new(
        backend: Arc<dyn KernelBackend>,
        auth: Option<Arc<dyn Auth>>,
        cfg: RunnerConfig,
    ) -> Self {
        let sessions = Arc::new(Mutex::new(HashMap::<String, SessionMeta>::new()));
        let interval = Duration::from_secs(cfg.sweep_interval_secs);
        let idle = Duration::from_secs(cfg.idle_timeout_secs);
        let sweeper = tokio::spawn(Self::sweep_loop(
            Arc::clone(&sessions),
            Arc::clone(&backend),
            interval,
            idle,
        ));
        Self {
            backend,
            auth,
            cfg,
            sessions,
            sweeper: Some(sweeper),
        }
    }

    async fn sweep_loop(
        sessions: Arc<Mutex<HashMap<String, SessionMeta>>>,
        backend: Arc<dyn KernelBackend>,
        interval: Duration,
        idle: Duration,
    ) {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let expired: Vec<String> = sessions
                .lock()
                .await
                .iter()
                .filter(|(_, h)| Instant::now().duration_since(h.last_active) > idle)
                .map(|(id, _)| id.clone())
                .collect();
            for id in expired {
                tracing::info!("closing idle session {id}");
                if let Err(e) = backend.close(&id).await {
                    tracing::warn!("sweep close {id}: {e}");
                }
                sessions.lock().await.remove(&id);
            }
        }
    }

    /// Spawn a session: verify auth (if enabled), delegate to backend,
    /// insert metadata into map.
    ///
    /// # Errors
    /// Backend spawn/handshake, auth verification, or duplicate id.
    pub async fn start(
        &self,
        params: SessionParams,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<String> {
        if let Some(auth) = &self.auth {
            let ctx = auth_ctx.ok_or_else(|| {
                anyhow::anyhow!("auth required but no context supplied")
            })?;
            if !auth
                .verify_signature(&ctx.session_id, "Start", ctx.timestamp, ctx.nonce, &ctx.signature)
                .await?
            {
                return Err(anyhow::anyhow!("auth verification failed"));
            }
        }
        let id = self.backend.start(params).await?;
        self.sessions.lock().await.insert(
            id.clone(),
            SessionMeta {
                last_active: Instant::now(),
                started_at: Utc::now(),
                labels: HashMap::new(),
                role: None,
                requested_role: None,
                pending_challenge: None,
            },
        );
        Ok(id)
    }

    /// Attach a challenge to an existing session (called by server when
    /// `Start(SessionParams { require_challenge: true })`).
    pub async fn attach_challenge(
        &self,
        session_id: &str,
        plaintext: Vec<u8>,
        requested_role: Option<String>,
    ) {
        let mut sessions = self.sessions.lock().await;
        if let Some(meta) = sessions.get_mut(session_id) {
            meta.pending_challenge = Some(plaintext);
            meta.requested_role = requested_role;
            meta.last_active = Instant::now();
        }
    }

    /// Verify a challenge response from the client. On success, promotes
    /// `requested_role` → `role` and clears the pending challenge.
    ///
    /// # Errors
    /// Session not found, no pending challenge, or auth backend rejects.
    pub async fn verify_challenge(
        &self,
        session_id: &str,
        response: &[u8],
    ) -> Result<Option<String>> {
        let plaintext = {
            let mut sessions = self.sessions.lock().await;
            let meta = sessions
                .get_mut(session_id)
                .ok_or_else(|| anyhow::anyhow!("session {session_id} not found"))?;
            meta.last_active = Instant::now();
            meta.pending_challenge
                .take()
                .ok_or_else(|| anyhow::anyhow!("no pending challenge for {session_id}"))?
        };

        let auth = self
            .auth
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("auth backend not configured"))?;
        let ok = auth
            .verify_challenge(session_id, &plaintext, response)
            .await?;
        if !ok {
            // Restore the challenge so the client can retry (or we drop on
            // sweep — single-attempt semantics would also be valid).
            self.sessions.lock().await.get_mut(session_id).map(|m| {
                m.pending_challenge = Some(plaintext);
            });
            return Ok(None);
        }

        // Promote role.
        let mut sessions = self.sessions.lock().await;
        if let Some(meta) = sessions.get_mut(session_id) {
            meta.role = meta.requested_role.take();
            return Ok(meta.role.clone());
        }
        Ok(None)
    }

    pub async fn touch(&self, session_id: &str) {
        if let Some(h) = self.sessions.lock().await.get_mut(session_id) {
            h.last_active = Instant::now();
        }
    }

    /// Health snapshot for the underlying backend.
    pub async fn health(&self) -> Result<HealthInfo> {
        self.backend.health().await
    }

    /// Kind label of the underlying backend (e.g. "ipc", "echo").
    pub fn backend_kind(&self) -> &'static str {
        self.backend.kind()
    }

    /// Close one session and remove from registry.
    pub async fn close(&self, session_id: &str, auth_ctx: Option<&AuthContext>) -> Result<()> {
        if let Some(auth) = &self.auth {
            let ctx = auth_ctx.ok_or_else(|| anyhow::anyhow!("auth required"))?;
            if !auth
                .verify_signature(&ctx.session_id, "Close", ctx.timestamp, ctx.nonce, &ctx.signature)
                .await?
            {
                return Err(anyhow::anyhow!("auth verification failed"));
            }
        }
        self.touch(session_id).await;
        self.backend.close(session_id).await?;
        self.sessions.lock().await.remove(session_id);
        Ok(())
    }

    /// Execute: touch + delegate.
    pub async fn execute(
        &self,
        session_id: &str,
        req: CodeRequest,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<ExecOutcome> {
        self.verify_or_skip(auth_ctx, session_id, "Execute").await?;
        self.touch(session_id).await;
        self.backend.execute(session_id, req).await
    }

    /// Interrupt: touch + delegate.
    pub async fn interrupt(
        &self,
        session_id: &str,
        request_id: &str,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<()> {
        self.verify_or_skip(auth_ctx, session_id, "Interrupt").await?;
        self.touch(session_id).await;
        self.backend.interrupt(session_id, request_id).await
    }

    /// Close all sessions and shut down the backend.
    pub async fn close_all(&self) {
        let ids: Vec<String> = self.sessions.lock().await.keys().cloned().collect();
        for id in ids {
            let _ = self.backend.close(&id).await;
        }
        let _ = self.backend.shutdown().await;
    }

    pub async fn list_sessions(&self) -> Vec<String> {
        self.sessions.lock().await.keys().cloned().collect()
    }

    async fn verify_or_skip(
        &self,
        auth_ctx: Option<&AuthContext>,
        session_id: &str,
        method: &str,
    ) -> Result<()> {
        if let Some(auth) = &self.auth {
            let ctx = auth_ctx.ok_or_else(|| anyhow::anyhow!("auth required"))?;
            if !auth
                .verify_signature(&ctx.session_id, method, ctx.timestamp, ctx.nonce, &ctx.signature)
                .await?
            {
                return Err(anyhow::anyhow!("auth verification failed for {session_id}"));
            }
        }
        Ok(())
    }

    pub fn abort_sweeper(&mut self) {
        if let Some(jh) = self.sweeper.take() {
            jh.abort();
        }
    }
}

impl Drop for SessionRegistry {
    fn drop(&mut self) {
        self.abort_sweeper();
    }
}
