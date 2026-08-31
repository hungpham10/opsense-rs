//! # BacktestExecutor — transform component chạy job (phía runner)
//!
//! Nhận `BacktestJobSpec` từ pipeline vector, chạy backtest bằng `Portfolio`,
//! thu mọi biến cố lệnh qua `NotifyFn` → đẩy ra data plane (RedisSink ghi vào
//! `backtest:job:{job_id}:events`), ghi status qua control plane (Redis).
//! Logic protocol + redis key nằm ở `protocol.rs`.

use std::collections::HashMap;
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

use crate::playground::SharpeScore;
use crate::qlib::calendar::{CryptoCalendar, StockCalendar};
use crate::qlib::data_loader::FromQueryCandleSticks;
use crate::qlib::fee::SimpleFixedFee;
use crate::qlib::jobs::protocol::{
    BacktestJobSpec, JobEvent, JobEventKind, JobStatus, is_cancelled, release_user_slot,
    try_acquire_user_slot, write_status,
};
use crate::qlib::portfolio::{Order, Report};
use crate::qlib::{Calendar, DataLoader, Fee, OrderEvent, Portfolio, Score};
use models::resolver::Resolver;
use vector_config_macro::transform;
use vector_runtime::{Component, Event, Identify, Message, Outbound};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn default_max_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn default_guard_ttl() -> u64 {
    24 * 60 * 60
}

#[transform]
pub struct BacktestExecutor {
    pub id: String,
    pub inputs: Vec<String>,

    /// Số backtest chạy song song tối đa (semaphore).
    #[serde(default = "default_max_workers")]
    pub max_workers: usize,

    /// TTL của user guard (chống kẹt vĩnh viễn nếu runner crash).
    #[serde(default = "default_guard_ttl")]
    pub guard_ttl_secs: u64,
}

/// Kết quả đầy đủ của job — dump lên S3.
#[derive(Serialize)]
struct JobResult<'a> {
    job_id: &'a str,
    tenant_id: i64,
    user_id: &'a str,
    broker: &'a str,
    symbol: &'a str,
    from: u64,
    to: u64,
    capital: f64,
    score: f64,
    report: &'a Report,
    open_orders: &'a [Order],
    order_history: &'a [Order],
}

async fn run_backtest(
    spec: &BacktestJobSpec,
    streams: Vec<mpsc::Sender<Message>>,
) -> Result<(f64, Report, Vec<Order>, Vec<Order>), Error> {
    let loader: Arc<dyn DataLoader> = spec.loader.clone().unwrap_or_else(|| {
        Arc::new(FromQueryCandleSticks::new(
            spec.broker.clone(),
            spec.symbol.clone(),
            None,
        ))
    });
    let calendar: Arc<dyn Calendar> = spec.calendar.clone().unwrap_or_else(|| {
        if spec.broker == "simplefx" {
            Arc::new(CryptoCalendar)
        } else {
            Arc::new(StockCalendar)
        }
    });
    let fee: Arc<dyn Fee> = spec
        .fee
        .clone()
        .unwrap_or_else(|| Arc::new(SimpleFixedFee::new(0.001)));
    let score: Arc<dyn Score> = spec.score.clone().unwrap_or_else(|| Arc::new(SharpeScore));

    let portfolio = Portfolio::new(
        loader,
        spec.strategy.clone(),
        fee,
        score,
        calendar,
        spec.resolution_for_rebuild.clone(),
        spec.resolution_for_test.clone(),
        crate::qlib::portfolio::DEFAULT_SETTLEMENT_CANDLES,
    )?;

    // Thu thập mọi biến cố lệnh qua notify callback → đẩy ra data plane
    let tenant_id = spec.tenant_id;
    let job_id = spec.job_id.clone();
    let notify_streams = streams;
    let mut notify =
        move |event: OrderEvent| -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> {
            let streams = notify_streams.clone();
            let job_id = job_id.clone();
            Box::pin(async move {
                emit_event(
                    &streams,
                    &JobEvent {
                        job_id,
                        tenant_id,
                        ts: now_secs(),
                        kind: JobEventKind::from(event),
                    },
                );
                Ok(())
            })
        };

    let mut orders = Vec::new();
    let mut history = Vec::new();
    let (final_score, report) = portfolio
        .evaluate(
            &mut orders,
            &mut history,
            spec.lookback,
            spec.from,
            spec.to,
            Some(&mut notify),
        )
        .await?;

    Ok((final_score, report, orders, history))
}

async fn dump_to_s3(resolver: &Resolver, result: &JobResult<'_>) -> Result<String, Error> {
    let bucket = std::env::var("S3_BUCKET").map_err(|_| {
        Error::new(
            ErrorKind::NotFound,
            "Missing S3_BUCKET env for job result dump",
        )
    })?;
    let key = format!("backtest/{}/{}.json", result.tenant_id, result.job_id);
    let body = serde_json::to_vec(result)
        .map_err(|e| Error::other(format!("Failed to serialize job result: {e}")))?;

    resolver
        .s3()
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(aws_sdk_s3::primitives::ByteStream::from(body))
        .content_type("application/json")
        .send()
        .await
        .map_err(|e| Error::other(format!("Failed to upload job result to S3: {e}")))?;

    Ok(key)
}

/// Đẩy event ra data plane (qua streams → RedisSink).
fn emit_event(streams: &[mpsc::Sender<Message>], event: &JobEvent) {
    if let Ok(payload) = serde_json::to_value(event) {
        let message = Message { payload };
        for sender in streams {
            let _ = sender.try_send(message.clone());
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_job(
    spec: BacktestJobSpec,
    resolver: &Resolver,
    streams: Vec<mpsc::Sender<Message>>,
    semaphore: &Arc<Semaphore>,
    guard_ttl_secs: u64,
) {
    let job_id = spec.job_id.clone();
    let tenant_id = spec.tenant_id;

    // Chờ slot worker
    let _permit = match semaphore.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(error) => {
            let _ = write_status(
                resolver,
                tenant_id,
                &job_id,
                &JobStatus {
                    status: "failed".into(),
                    error: Some(format!("semaphore closed: {error}")),
                    ..Default::default()
                },
            )
            .await;
            return;
        }
    };

    // Giới hạn 1 job / user (double-check phía runner, gateway đã check khi submit)
    match try_acquire_user_slot(resolver, tenant_id, &spec.user_id, &job_id, guard_ttl_secs).await {
        Ok(true) => {}
        Ok(false) => {
            let _ = write_status(
                resolver,
                tenant_id,
                &job_id,
                &JobStatus {
                    status: "failed".into(),
                    error: Some(format!(
                        "user {} already has a running backtest",
                        spec.user_id
                    )),
                    submitted_at: Some(spec.submitted_at),
                    finished_at: Some(now_secs()),
                    ..Default::default()
                },
            )
            .await;
            return;
        }
        Err(error) => {
            let _ = write_status(
                resolver,
                tenant_id,
                &job_id,
                &JobStatus {
                    status: "failed".into(),
                    error: Some(error.to_string()),
                    ..Default::default()
                },
            )
            .await;
            return;
        }
    }

    // Status = running + event Started
    let _ = write_status(
        resolver,
        tenant_id,
        &job_id,
        &JobStatus {
            status: "running".into(),
            progress: Some(0.0),
            submitted_at: Some(spec.submitted_at),
            started_at: Some(now_secs()),
            ..Default::default()
        },
    )
    .await;
    emit_event(
        &streams,
        &JobEvent {
            job_id: job_id.clone(),
            tenant_id,
            ts: now_secs(),
            kind: JobEventKind::Started,
        },
    );

    // Chạy backtest trong task riêng để có thể abort khi có cancel flag.
    // Events từng lệnh được thu thập qua notify callback bên trong run_backtest.
    let eval_spec = spec.clone();
    let eval_streams = streams.clone();
    let mut eval = tokio::spawn(async move { run_backtest(&eval_spec, eval_streams).await });

    let outcome = tokio::select! {
        result = &mut eval => match result {
            Ok(inner) => inner.map_err(|e| Error::other(format!("backtest failed: {e}"))),
            Err(join_error) => Err(Error::other(format!("backtest task panicked: {join_error}"))),
        },
        _ = async {
            loop {
                if is_cancelled(resolver, tenant_id, &job_id).await {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        } => {
            eval.abort();
            Err(Error::new(ErrorKind::Interrupted, "cancelled"))
        }
    };

    let _ = release_user_slot(resolver, tenant_id, &spec.user_id, &job_id).await;

    match outcome {
        Ok((final_score, report, orders, history)) => {
            let result = JobResult {
                job_id: &job_id,
                tenant_id,
                user_id: &spec.user_id,
                broker: &spec.broker,
                symbol: &spec.symbol,
                from: spec.from,
                to: spec.to,
                capital: spec.capital,
                score: final_score,
                report: &report,
                open_orders: &orders,
                order_history: &history,
            };

            let s3_key = match dump_to_s3(resolver, &result).await {
                Ok(key) => Some(key),
                Err(error) => {
                    tracing::warn!(job = %job_id, "Failed to dump job result to S3: {error}");
                    None
                }
            };

            let _ = write_status(
                resolver,
                tenant_id,
                &job_id,
                &JobStatus {
                    status: "finished".into(),
                    progress: Some(100.0),
                    score: Some(final_score),
                    s3_key,
                    submitted_at: Some(spec.submitted_at),
                    started_at: Some(now_secs()),
                    finished_at: Some(now_secs()),
                    ..Default::default()
                },
            )
            .await;
            emit_event(
                &streams,
                &JobEvent {
                    job_id,
                    tenant_id,
                    ts: now_secs(),
                    kind: JobEventKind::Finished { score: final_score },
                },
            );
        }
        Err(error) if error.kind() == ErrorKind::Interrupted => {
            let _ = write_status(
                resolver,
                tenant_id,
                &job_id,
                &JobStatus {
                    status: "cancelled".into(),
                    submitted_at: Some(spec.submitted_at),
                    finished_at: Some(now_secs()),
                    ..Default::default()
                },
            )
            .await;
            emit_event(
                &streams,
                &JobEvent {
                    job_id,
                    tenant_id,
                    ts: now_secs(),
                    kind: JobEventKind::Cancelled,
                },
            );
        }
        Err(error) => {
            let _ = write_status(
                resolver,
                tenant_id,
                &job_id,
                &JobStatus {
                    status: "failed".into(),
                    error: Some(error.to_string()),
                    submitted_at: Some(spec.submitted_at),
                    finished_at: Some(now_secs()),
                    ..Default::default()
                },
            )
            .await;
            emit_event(
                &streams,
                &JobEvent {
                    job_id,
                    tenant_id,
                    ts: now_secs(),
                    kind: JobEventKind::Failed {
                        reason: error.to_string(),
                    },
                },
            );
        }
    }
}

impl_backtest_executor!(
    async fn run(
        &self,
        _: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let semaphore = Arc::new(Semaphore::new(self.max_workers.max(1)));
        let mut tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

        while let Some(message) = rx.recv().await {
            let spec: BacktestJobSpec = match serde_json::from_value(message.payload) {
                Ok(spec) => spec,
                Err(error) => {
                    let _ = tx
                        .event
                        .send(Event::Minor((
                            0,
                            Error::new(
                                ErrorKind::InvalidData,
                                format!("Invalid backtest job spec: {error}"),
                            ),
                        )))
                        .await;
                    continue;
                }
            };

            // Dọn task đã kết thúc
            tasks.retain(|_, handle| !handle.is_finished());

            let Some(ctx) = tx.ctx.clone() else {
                let _ = tx
                    .event
                    .send(Event::Minor((
                        0,
                        Error::other("BacktestExecutor requires Resolver runtime context"),
                    )))
                    .await;
                continue;
            };

            let job_id = spec.job_id.clone();
            let streams = tx.streams.clone();
            let semaphore = semaphore.clone();
            let guard_ttl = self.guard_ttl_secs;

            let handle = tokio::spawn(async move {
                let Some(resolver) = ctx.as_any().downcast_ref::<Resolver>() else {
                    tracing::error!("BacktestExecutor: runtime context is not a Resolver");
                    return;
                };
                execute_job(spec, resolver, streams, &semaphore, guard_ttl).await;
            });
            tasks.insert(job_id, handle);
        }

        // Chờ mọi job đang chạy kết thúc khi pipeline shutdown
        for (_, handle) in tasks {
            let _ = handle.await;
        }

        Ok(())
    }
);
