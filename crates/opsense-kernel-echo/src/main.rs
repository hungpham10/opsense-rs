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
//! | `df`              | return the last received dataset as a DataFrame      |
//! | anything else     | text result `echo: <code>`                           |

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use opsense_proto::frame::{Frame, FrameCodec, FrameTag};
use opsense_proto::pb::{
    envelope, exec_event, value, Ack, CodeRequest, DatasetAck, Envelope, ExecEvent, HealthStatus,
    Welcome,
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
    let mut datasets: HashMap<String, Dataset> = HashMap::new();
    let mut arrows: Vec<Bytes> = Vec::new();
    // Most recently acked dataset ref (the `df` directive echoes it back).
    let mut last_ref = String::new();

    loop {
        let frame = match inbox.is_empty() {
            true => match source.next().await {
                Some(frame) => frame.context("reading frame")?,
                None => return Ok(()),
            },
            false => inbox.remove(0),
        };
        match frame.tag {
            FrameTag::Arrow => arrows.push(frame.payload),
            FrameTag::Control => {
                let env = frame.envelope()?;
                if handle(
                    env,
                    &mut source,
                    &mut sink,
                    &mut inbox,
                    &mut datasets,
                    &mut arrows,
                    &mut last_ref,
                )
                .await?
                {
                    return Ok(());
                }
            }
        }
    }
}

struct Dataset {
    segments: Vec<Bytes>,
    rows: i64,
    cols: i64,
    columns: Vec<String>,
}

/// Handle one control envelope. Returns `true` when the kernel should exit.
#[allow(clippy::too_many_arguments)]
async fn handle(
    env: Envelope,
    source: &mut FramedRead<tokio::io::Stdin, FrameCodec>,
    sink: &mut FramedWrite<tokio::io::Stdout, FrameCodec>,
    inbox: &mut Vec<Frame>,
    datasets: &mut HashMap<String, Dataset>,
    arrows: &mut Vec<Bytes>,
    last_ref: &mut String,
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
                        },
                    )),
                },
            )
            .await?;
            Ok(false)
        }
        Some(envelope::Msg::CloseRequest(close)) => {
            datasets.remove(&close.session_id);
            ack(sink, true, "").await?;
            Ok(false)
        }
        Some(envelope::Msg::DatasetHeader(header)) => {
            let rows: i64 = arrows
                .iter()
                .map(|s| segment_rows(s))
                .sum::<Result<u64>>()? as i64;
            let first = arrows.first().map(|s| segment_info(s));
            let dataset = Dataset {
                segments: std::mem::take(arrows),
                rows,
                cols: first.as_ref().map_or(0, |i| i.1),
                columns: first.map(|i| i.2).unwrap_or_default(),
            };
            *last_ref = header.dataset_ref.clone();
            send(
                sink,
                &Envelope {
                    msg: Some(envelope::Msg::DatasetAck(DatasetAck {
                        dataset_ref: header.dataset_ref.clone(),
                        rows,
                        ok: true,
                        error: String::new(),
                    })),
                },
            )
            .await?;
            datasets.insert(header.dataset_ref, dataset);
            Ok(false)
        }
        Some(envelope::Msg::CodeRequest(req)) => {
            exec_code(req, source, sink, inbox, datasets, last_ref).await?;
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
    datasets: &HashMap<String, Dataset>,
    last_ref: &str,
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
    } else if code == "df" {
        echo_dataframe(datasets, last_ref).map(|value| {
            Some(event(
                &req.request_id,
                exec_event::Event::ResultValue(value),
            ))
        })
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

fn echo_dataframe(
    datasets: &HashMap<String, Dataset>,
    last_ref: &str,
) -> Result<opsense_proto::pb::Value> {
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;

    let last = datasets.get(last_ref).ok_or_else(|| {
        anyhow::anyhow!("no dataset received yet — send one with SendDataset first")
    })?;

    let mut batches = Vec::new();
    let mut schema = None;
    for segment in &last.segments {
        let reader = StreamReader::try_new(std::io::Cursor::new(segment.as_ref()), None)
            .context("decoding arrow segment")?;
        schema = Some(reader.schema().clone());
        for batch in reader {
            batches.push(batch.context("reading batch")?);
        }
    }

    let mut out = Vec::new();
    if let Some(schema) = &schema {
        let mut writer = StreamWriter::try_new(&mut out, schema)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
    }

    Ok(opsense_proto::pb::Value {
        kind: Some(value::Kind::Dataframe(opsense_proto::pb::DataFrame {
            arrow_ipc: out,
            rows: last.rows,
            cols: last.cols,
            columns: last.columns.clone(),
        })),
    })
}

/// `(rows, cols, column names)` of one Arrow IPC stream segment.
fn segment_info(segment: &[u8]) -> (u64, i64, Vec<String>) {
    let Ok(reader) = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(segment), None)
    else {
        return (0, 0, vec![]);
    };
    let cols = reader.schema().fields().len() as i64;
    let columns = reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    (0, cols, columns)
}

/// Row count of one Arrow IPC stream segment; 0 when undecodable (the ack
/// reports what actually landed).
fn segment_rows(segment: &[u8]) -> Result<u64> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(segment), None)
        .context("decoding arrow segment")?;
    let mut rows = 0u64;
    for batch in reader {
        rows += batch?.num_rows() as u64;
    }
    Ok(rows)
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
