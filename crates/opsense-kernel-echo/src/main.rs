//! Reference opsense kernel speaking the framed stdio protocol
//! ([`opsense_proto`]). Exists to lock the wire contract down without any
//! Python: every behaviour here is mirrored by `opsense-kernel-python`.
//!
//! Code directives understood by [`exec_code`]:
//!
//! | code              | behaviour                                            |
//! |-------------------|------------------------------------------------------|
//! | `sleep:<ms>`      | sleep (interruptible via InterruptRequest)           |
//! | `print:<text>`    | emit one stdout event                                |
//! | `err:<kind>:<msg>`| emit an ErrorEvent, request fails                    |
//! | anything else     | text result `echo: <code>`                           |

use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use opsense_proto::frame::{Frame, FrameCodec, FrameTag};
use opsense_proto::pb::{
    Ack, CodeRequest, Envelope, ExecEvent, HealthStatus, Welcome, envelope, exec_event, value,
};
use tokio::io::{stdin, stdout};
use tokio_util::codec::{FramedRead, FramedWrite};

const KERNEL_NAME: &str = "echo";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut source = FramedRead::new(stdin(), FrameCodec);
    let mut sink = FramedWrite::new(stdout(), FrameCodec);
    // Frames that arrived while a long op was in flight and still need
    // handling once it finishes.
    let mut inbox: Vec<Frame> = Vec::new();

    loop {
        let frame = match inbox.is_empty() {
            true => match source.next().await {
                Some(frame) => frame.context("reading frame")?,
                None => return Ok(()),
            },
            false => inbox.remove(0),
        };
        match frame.tag {
            FrameTag::Control => {
                let env = frame.envelope()?;
                if handle(env, &mut source, &mut sink, &mut inbox).await? {
                    return Ok(());
                }
            }
            // Stray ARROW frame with no dataset protocol in this kernel:
            // drop it on the floor rather than deadlock on next read.
            FrameTag::Arrow => {}
        }
    }
}

/// Handle one control envelope. Returns `true` when the kernel should exit.
async fn handle(
    env: Envelope,
    source: &mut FramedRead<tokio::io::Stdin, FrameCodec>,
    sink: &mut FramedWrite<tokio::io::Stdout, FrameCodec>,
    inbox: &mut Vec<Frame>,
) -> Result<bool> {
    match env.msg {
        Some(envelope::Msg::Hello(hello)) => {
            if hello.protocol_version != opsense_proto::PROTOCOL_VERSION {
                bail!(
                    "host speaks protocol v{}, kernel speaks v{}",
                    hello.protocol_version,
                    opsense_proto::PROTOCOL_VERSION
                );
            }
            send(
                sink,
                &Envelope {
                    msg: Some(envelope::Msg::Welcome(Welcome {
                        protocol_version: opsense_proto::PROTOCOL_VERSION,
                        kernel_name: KERNEL_NAME.into(),
                        kernel_version: env!("CARGO_PKG_VERSION").into(),
                    })),
                },
            )
            .await?;
            Ok(false)
        }
        Some(envelope::Msg::HealthRequest(_)) => {
            send(
                sink,
                &Envelope {
                    msg: Some(envelope::Msg::HealthStatus(HealthStatus {
                        ok: true,
                        kernel_name: KERNEL_NAME.into(),
                        kernel_version: env!("CARGO_PKG_VERSION").into(),
                        packages: vec![],
                        detail: "reference echo kernel".into(),
                    })),
                },
            )
            .await?;
            Ok(false)
        }
        Some(envelope::Msg::StartSession(params)) => {
            send(
                sink,
                &Envelope {
                    msg: Some(envelope::Msg::SessionHandle(
                        opsense_proto::pb::SessionHandle {
                            session_id: params.session_id,
                            // The echo kernel does not implement challenge/role
                            // auth; advertise no challenge to the host.
                            challenge: vec![],
                        },
                    )),
                },
            )
            .await?;
            Ok(false)
        }
        Some(envelope::Msg::CloseRequest(_)) => {
            ack(sink, true, "").await?;
            Ok(false)
        }
        Some(envelope::Msg::CodeRequest(req)) => {
            exec_code(req, source, sink, inbox).await?;
            Ok(false)
        }
        Some(envelope::Msg::InterruptRequest(_)) => {
            // Idle interrupt: nothing to cancel, acknowledge directly. While
            // an Execute is in flight the interrupt is consumed inside
            // `exec_code` and surfaces as ErrorEvent{cancelled} instead.
            ack(sink, true, "").await?;
            Ok(false)
        }
        Some(envelope::Msg::Shutdown(_)) => {
            ack(sink, true, "").await?;
            Ok(true)
        }
        other => {
            ack(sink, false, &format!("unexpected envelope {other:?}")).await?;
            Ok(false)
        }
    }
}

/// Run one code request; always terminates its stream with `done`.
async fn exec_code(
    req: CodeRequest,
    source: &mut FramedRead<tokio::io::Stdin, FrameCodec>,
    sink: &mut FramedWrite<tokio::io::Stdout, FrameCodec>,
    inbox: &mut Vec<Frame>,
) -> Result<()> {
    let code = req.code.trim();

    let outcome: Result<Option<ExecEvent>> = if let Some(rest) = code.strip_prefix("sleep:") {
        let ms: u64 = rest.trim().parse().context("sleep:<ms>")?;
        sleep_interruptible(ms, source, inbox)
            .await
            .map(|cancelled| {
                cancelled.then(|| error_event(&req, "cancelled", "interrupted by host"))
            })
    } else if let Some(text) = code.strip_prefix("print:") {
        Ok(Some(event(
            &req.request_id,
            exec_event::Event::StdoutLine(text.to_string()),
        )))
    } else if let Some(rest) = code.strip_prefix("err:") {
        let (kind, message) = rest.split_once(':').unwrap_or((rest, ""));
        Ok(Some(error_event(&req, kind, message)))
    } else {
        Ok(Some(event(
            &req.request_id,
            exec_event::Event::ResultValue(opsense_proto::pb::Value {
                kind: Some(value::Kind::Text(format!("echo: {code}"))),
            }),
        )))
    };

    match outcome {
        Ok(Some(ev)) => emit(sink, &[ev], &req).await,
        Ok(None) => emit(sink, &[], &req).await,
        Err(err) => {
            emit(
                sink,
                &[error_event(&req, "bad_request", &format!("{err:#}"))],
                &req,
            )
            .await
        }
    }
}

/// Sleep for `ms`, watching incoming frames; an `InterruptRequest` cancels
/// the sleep and is consumed (it answers through this stream instead of an
/// ack). Any other control frame is parked in `inbox` for later processing.
async fn sleep_interruptible(
    ms: u64,
    source: &mut FramedRead<tokio::io::Stdin, FrameCodec>,
    inbox: &mut Vec<Frame>,
) -> Result<bool> {
    let sleep = tokio::time::sleep(Duration::from_millis(ms));
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return Ok(false),
            item = source.next() => {
                let frame = match item {
                    Some(frame) => frame.context("reading during sleep")?,
                    None => bail!("stdin closed during execute"),
                };
                if frame.tag == FrameTag::Arrow {
                    continue;
                }
                let interrupted = matches!(
                    frame.envelope()?.msg,
                    Some(envelope::Msg::InterruptRequest(_))
                );
                if interrupted {
                    return Ok(true);
                }
                inbox.push(frame);
            }
        }
    }
}

fn event(request_id: &str, ev: exec_event::Event) -> ExecEvent {
    ExecEvent {
        request_id: request_id.to_string(),
        event: Some(ev),
    }
}

fn error_event(req: &CodeRequest, kind: &str, message: &str) -> ExecEvent {
    event(
        &req.request_id,
        exec_event::Event::Error(opsense_proto::pb::ErrorEvent {
            kind: kind.to_string(),
            message: message.to_string(),
        }),
    )
}

async fn emit(
    sink: &mut FramedWrite<tokio::io::Stdout, FrameCodec>,
    events: &[ExecEvent],
    req: &CodeRequest,
) -> Result<()> {
    for ev in events {
        send(
            sink,
            &Envelope {
                msg: Some(envelope::Msg::ExecEvent(ev.clone())),
            },
        )
        .await?;
    }
    send(
        sink,
        &Envelope {
            msg: Some(envelope::Msg::ExecEvent(event(
                &req.request_id,
                exec_event::Event::Done(true),
            ))),
        },
    )
    .await
}

async fn ack(
    sink: &mut FramedWrite<tokio::io::Stdout, FrameCodec>,
    ok: bool,
    error: &str,
) -> Result<()> {
    send(
        sink,
        &Envelope {
            msg: Some(envelope::Msg::Ack(Ack {
                ok,
                error: error.to_string(),
            })),
        },
    )
    .await
}

async fn send(sink: &mut FramedWrite<tokio::io::Stdout, FrameCodec>, msg: &Envelope) -> Result<()> {
    sink.send(Frame::control(msg)).await?;
    Ok(())
}