//! Host side of the framed kernel protocol ([`KernelConnection`]).
//!
//! Generic over any `AsyncRead` + `AsyncWrite` pair so the same driver serves
//! the local transport (child-process stdio) and the runner's kernel sockets.
//! One connection drives one kernel process; requests are strictly
// sequential — concurrent sessions own separate processes.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::frame::{Frame, FrameCodec};
use crate::pb::{envelope, exec_event};
use crate::pb::{
    Ack, CodeRequest, DatasetAck, DatasetHeader, Envelope, ExecEvent, HealthRequest, HealthStatus,
    Hello, InterruptRequest, SessionHandle, SessionParams, Shutdown, Welcome,
};

/// Driver for one kernel connection (typically a spawned process's stdio).
pub struct KernelConnection<R, W> {
    source: FramedRead<R, FrameCodec>,
    sink: FramedWrite<W, FrameCodec>,
}

/// Everything that came back from one [`KernelConnection::execute`] run.
#[derive(Debug, Default)]
pub struct ExecOutcome {
    /// Streaming events in arrival order (stdout/stderr/artifacts/dataframes).
    pub events: Vec<ExecEvent>,
    /// Captured `result` value, if the kernel produced one.
    pub value: Option<crate::pb::Value>,
    /// Kernel-reported failure (`ErrorEvent` before `done`).
    pub error: Option<crate::pb::ErrorEvent>,
    /// Host-side deadline elapsed; the kernel was asked to interrupt.
    pub timed_out: bool,
}

impl ExecOutcome {
    #[must_use]
    pub fn ok(&self) -> bool {
        self.error.is_none() && !self.timed_out
    }

    /// Concatenated stdout lines, trimmed trailing newline.
    #[must_use]
    pub fn stdout(&self) -> String {
        self.events
            .iter()
            .filter_map(|e| match &e.event {
                Some(exec_event::Event::StdoutLine(line)) => Some(line.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl<R: tokio::io::AsyncRead + Unpin, W: tokio::io::AsyncWrite + Unpin> KernelConnection<R, W> {
    #[must_use]
    pub fn new(read: R, write: W) -> Self {
        Self {
            source: FramedRead::new(read, FrameCodec),
            sink: FramedWrite::new(write, FrameCodec),
        }
    }

    /// Send one control envelope.
    ///
    /// # Errors
    /// Write failures mean the kernel pipe is broken.
    pub async fn send(&mut self, msg: &Envelope) -> Result<()> {
        self.sink.send(Frame::control(msg)).await?;
        Ok(())
    }

    /// Receive the next frame as an envelope; `None` on EOF (the kernel
    /// exited or closed its stdout).
    pub async fn recv(&mut self) -> Option<Result<Envelope>> {
        loop {
            match self.source.next().await {
                Some(Ok(frame)) => match frame.tag {
                    crate::frame::FrameTag::Arrow => continue, // stray data frame
                    crate::frame::FrameTag::Control => {
                        return Some(frame.envelope());
                    }
                },
                Some(Err(err)) => return Some(Err(anyhow::Error::from(err))),
                None => return None,
            }
        }
    }

    async fn expect<T>(
        &mut self,
        what: &'static str,
        extract: impl Fn(envelope::Msg) -> Option<T>,
    ) -> Result<T> {
        let env = self
            .recv()
            .await
            .ok_or_else(|| anyhow!("kernel closed connection while waiting for {what}"))
            .and_then(|r| r)
            .with_context(|| format!("waiting for {what}"))?;
        let msg = env
            .msg
            .ok_or_else(|| anyhow!("empty envelope for {what}"))?;
        match extract(msg.clone()) {
            Some(v) => Ok(v),
            None => Err(anyhow!(
                "unexpected envelope while waiting for {what}: {msg:?}"
            )),
        }
    }

    async fn expect_ack(&mut self, what: &'static str) -> Result<Ack> {
        self.expect(what, |msg| match msg {
            envelope::Msg::Ack(ack) => Some(ack),
            _ => None,
        })
        .await
    }

    /// Handshake: send [`Hello`], require a matching-protocol [`Welcome`].
    ///
    /// # Errors
    /// EOF, protocol mismatch, or unexpected reply.
    pub async fn handshake(&mut self) -> Result<Welcome> {
        self.send(&Envelope {
            msg: Some(envelope::Msg::Hello(Hello {
                protocol_version: crate::PROTOCOL_VERSION,
                client: format!("opsense-{}", env!("CARGO_PKG_VERSION")),
            })),
        })
        .await?;
        let welcome = self
            .expect("welcome", |msg| match msg {
                envelope::Msg::Welcome(w) => Some(w),
                _ => None,
            })
            .await?;
        anyhow::ensure!(
            welcome.protocol_version == crate::PROTOCOL_VERSION,
            "kernel speaks protocol v{} but host speaks v{}",
            welcome.protocol_version,
            crate::PROTOCOL_VERSION
        );
        Ok(welcome)
    }

    /// # Errors
    /// Transport or kernel failures.
    pub async fn health(&mut self) -> Result<HealthStatus> {
        self.send(&Envelope {
            msg: Some(envelope::Msg::HealthRequest(HealthRequest {})),
        })
        .await?;
        self.expect("health status", |msg| match msg {
            envelope::Msg::HealthStatus(status) => Some(status),
            _ => None,
        })
        .await
    }

    /// Start a session; returns the session id echoed by the kernel.
    ///
    /// # Errors
    /// Transport or kernel failures.
    pub async fn start_session(&mut self, params: SessionParams) -> Result<String> {
        self.send(&Envelope {
            msg: Some(envelope::Msg::StartSession(params)),
        })
        .await?;
        let handle: SessionHandle = self
            .expect("session handle", |msg| match msg {
                envelope::Msg::SessionHandle(h) => Some(h),
                _ => None,
            })
            .await?;
        Ok(handle.session_id)
    }

    /// Close a session inside the kernel (state discarded, process lives).
    ///
    /// # Errors
    /// Transport failures or a negative ack.
    pub async fn close_session(&mut self, session_id: &str) -> Result<()> {
        self.send(&Envelope {
            msg: Some(envelope::Msg::CloseRequest(crate::pb::CloseRequest {
                session_id: session_id.to_string(),
            })),
        })
        .await?;
        let ack = self.expect_ack("close ack").await?;
        anyhow::ensure!(ack.ok, "close failed: {}", ack.error);
        Ok(())
    }

    /// Ask the kernel to shut down gracefully and wait for its ack.
    ///
    /// # Errors
    /// Transport failures; a missing ack is tolerated (process may already
    /// have exited).
    pub async fn shutdown(&mut self) -> Result<()> {
        let sent = self
            .send(&Envelope {
                msg: Some(envelope::Msg::Shutdown(Shutdown {})),
            })
            .await;
        if let Err(err) = sent {
            tracing::debug!(error = %err, "shutdown send failed; kernel likely gone");
            return Ok(());
        }
        let _ = self.expect_ack("shutdown ack").await;
        Ok(())
    }

    /// Interrupt the in-flight request (if any). While an Execute is active
    /// the kernel answers through that stream (`ErrorEvent{cancelled}` +
    /// `done`) instead of an explicit ack; when idle it answers `Ack`.
    ///
    /// # Errors
    /// Transport failures.
    pub async fn interrupt(&mut self, session_id: &str, request_id: &str) -> Result<()> {
        self.send(&Envelope {
            msg: Some(envelope::Msg::InterruptRequest(InterruptRequest {
                session_id: session_id.to_string(),
                request_id: request_id.to_string(),
            })),
        })
        .await
    }

    /// Push a dataset: N ARROW frames (one per chunk) followed by a
    /// `DatasetHeader` terminator, then wait for the kernel's `DatasetAck`.
    /// Chunks must already be complete Arrow IPC stream segments.
    ///
    /// # Errors
    /// Transport failures or a negative ack.
    pub async fn send_dataset(
        &mut self,
        header: DatasetHeader,
        chunks: Vec<Bytes>,
    ) -> Result<DatasetAck> {
        anyhow::ensure!(!chunks.is_empty(), "send_dataset requires >= 1 chunk");
        for chunk in chunks {
            self.sink.send(Frame::arrow(chunk)).await?;
        }
        self.send(&Envelope {
            msg: Some(envelope::Msg::DatasetHeader(header)),
        })
        .await?;
        self.expect("dataset ack", |msg| match msg {
            envelope::Msg::DatasetAck(ack) => Some(ack),
            _ => None,
        })
        .await
    }

    /// Run one [`CodeRequest`] to completion. Enforces `req.timeout_ms` (>0)
    /// host-side; on expiry fires [`Self::interrupt`] and reports
    /// [`ExecOutcome::timed_out`].
    ///
    /// # Errors
    /// Only transport failures; execution failures arrive as
    /// [`ExecOutcome::error`].
    pub async fn execute(&mut self, req: CodeRequest) -> Result<ExecOutcome> {
        self.send(&Envelope {
            msg: Some(envelope::Msg::CodeRequest(req.clone())),
        })
        .await?;

        let timeout = if req.timeout_ms > 0 {
            Duration::from_millis(req.timeout_ms)
        } else {
            Duration::MAX
        };

        let drain = async {
            let mut outcome = ExecOutcome::default();
            loop {
                let env = match self.recv().await {
                    Some(env) => env.context("reading exec events")?,
                    None => return Err(anyhow!("kernel closed during execute")),
                };
                match env.msg {
                    Some(envelope::Msg::ExecEvent(event)) => {
                        let done = matches!(event.event, Some(exec_event::Event::Done(true)));
                        if let Some(exec_event::Event::Error(err)) = &event.event {
                            outcome.error = Some(err.clone());
                        }
                        if let Some(exec_event::Event::ResultValue(value)) = &event.event {
                            outcome.value = Some(value.clone());
                        }
                        if !matches!(event.event, Some(exec_event::Event::ResultValue(_))) {
                            outcome.events.push(event);
                        }
                        if done {
                            return Ok(outcome);
                        }
                    }
                    other => {
                        return Err(anyhow!("unexpected envelope during execute: {other:?}"));
                    }
                }
            }
        };

        let drained = tokio::time::timeout(timeout, drain).await;
        match drained {
            Ok(result) => result,
            Err(_) => {
                let _ = self.interrupt(&req.session_id, &req.request_id).await;
                Ok(ExecOutcome {
                    timed_out: true,
                    ..ExecOutcome::default()
                })
            }
        }
    }
}
