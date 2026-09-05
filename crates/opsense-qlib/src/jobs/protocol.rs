//! # Backtest Job Protocol — contract chung giữa gateway (MCP) và runner
//!
//! Chứa mọi loại dữ liệu trao đổi qua Redis (spec / status / event), layout
//! key Redis, và các hàm control-plane (đọc/ghi status, guard 1 job/user,
//! cancel). Cả gateway lẫn runner đều import từ đây — không đụng tới executor.

use std::io::Error;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::qlib::portfolio::{Order, OrderType};
use crate::qlib::{Calendar, DataLoader, Fee, OrderEvent, Score, Strategy};
use models::resolver::Resolver;

// ═══════════════════════════════════════════════════════════════════════════
// Redis key layout
// ═══════════════════════════════════════════════════════════════════════════
pub const JOB_STREAM_KEY: &str = "backtest:jobs";

pub fn job_status_key(job_id: &str) -> String {
    format!("backtest:job:{job_id}")
}

pub fn job_events_key(job_id: &str) -> String {
    format!("backtest:job:{job_id}:events")
}

pub fn job_cancel_key(job_id: &str) -> String {
    format!("backtest:job:{job_id}:cancel")
}

pub fn user_guard_key(tenant_id: i64, user_id: &str) -> String {
    format!("backtest:user:{tenant_id}:{user_id}:active")
}

// ═══════════════════════════════════════════════════════════════════════════
// Protocol types
// ═══════════════════════════════════════════════════════════════════════════
fn default_capital() -> f64 {
    100_000.0
}

/// Mặc định lookback 30 ngày (giây) — khung dữ liệu cần prefetch trước khi chạy.
fn default_lookback() -> u64 {
    30 * 24 * 60 * 60
}

fn default_resolution_rebuild() -> String {
    "1H".into()
}

fn default_resolution_test() -> String {
    "5m".into()
}

/// Job spec — payload trong stream `backtest:jobs`. Strategy/loader/fee/
/// calendar là config typetag serde (giống vector component).
#[derive(Serialize, Deserialize, Clone)]
pub struct BacktestJobSpec {
    pub job_id: String,
    pub tenant_id: i64,
    pub user_id: String,
    pub broker: String,
    pub symbol: String,
    pub from: u64,
    pub to: u64,

    #[serde(default = "default_capital")]
    pub capital: f64,

    #[serde(default = "default_lookback")]
    pub lookback: u64,

    #[serde(default = "default_resolution_rebuild")]
    pub resolution_for_rebuild: String,

    #[serde(default = "default_resolution_test")]
    pub resolution_for_test: String,

    /// Bắt buộc: config typetag của strategy (grid / volatility_adaptive_grid / dag…)
    pub strategy: Arc<dyn Strategy>,

    #[serde(default)]
    pub loader: Option<Arc<dyn DataLoader>>,

    #[serde(default)]
    pub fee: Option<Arc<dyn Fee>>,

    #[serde(default)]
    pub calendar: Option<Arc<dyn Calendar>>,

    #[serde(default)]
    pub score: Option<Arc<dyn Score>>,

    #[serde(default)]
    pub submitted_at: u64,
}

impl From<BacktestJobSpec> for Value {
    fn from(spec: BacktestJobSpec) -> Value {
        serde_json::to_value(spec).unwrap_or(Value::Null)
    }
}

/// Trạng thái job — lưu JSON trong field `status` của hash `backtest:job:{id}`.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct JobStatus {
    /// queued | running | finished | failed | cancelled
    pub status: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_key: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_at: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<u64>,
}

/// Event của một job — data plane, RedisSink ghi vào list theo template
/// `backtest:job:{job_id}:events` (job_id phải ở top-level).
#[derive(Serialize, Deserialize, Clone)]
pub struct JobEvent {
    pub job_id: String,
    pub tenant_id: i64,
    pub ts: u64,

    #[serde(flatten)]
    pub kind: JobEventKind,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobEventKind {
    Started,
    Progress {
        percent: f64,
    },
    Finished {
        score: f64,
    },
    Failed {
        reason: String,
    },
    Cancelled,
    /// Chuyển tiếp từ event sink của Portfolio (review vì sao lệnh sai)
    OrderPlaced {
        ts: u64,
        grid: usize,
        level: usize,
        long: bool,
        entry_price: f64,
        size: f64,
        sl_price: f64,
        tp_price: f64,
        unlock_seq: u64,
    },
    OrderClosed {
        ts: u64,
        grid: usize,
        level: usize,
        long: bool,
        entry_price: f64,
        exit_price: f64,
        pnl_pct: f64,
        reason: String,
    },
    OrderRejected {
        ts: u64,
        grid: usize,
        level: usize,
        reason: String,
    },
    /// Strategy rebuild plan — snapshot dải giá grid sau rebuild (grid history)
    GridRebuilt {
        ts: u64,
        grids: Vec<crate::qlib::GridSnapshot>,
    },
}

/// Suy ra lý do đóng lệnh từ exit price so với SL (khớp logic check_order_exit).
fn close_reason(order: &Order) -> String {
    let exit = order.exit_price.unwrap_or(0.0);
    let stop_loss = match order.dtype {
        OrderType::Long => exit <= order.sl_price,
        OrderType::Unknown => false,
        OrderType::Short => exit >= order.sl_price,
    };
    if stop_loss {
        "stop_loss".into()
    } else {
        "take_profit".into()
    }
}

impl From<OrderEvent> for JobEventKind {
    fn from(event: OrderEvent) -> Self {
        match event {
            OrderEvent::Placed { ts, order } => JobEventKind::OrderPlaced {
                ts,
                grid: order.grid_index,
                level: order.level_index,
                long: order.dtype == OrderType::Long,
                entry_price: order.entry_price,
                size: order.size,
                sl_price: order.sl_price,
                tp_price: order.tp_price,
                unlock_seq: order.unlock_seq,
            },
            OrderEvent::Closed { ts, order } => JobEventKind::OrderClosed {
                ts,
                grid: order.grid_index,
                level: order.level_index,
                long: order.dtype == OrderType::Long,
                entry_price: order.entry_price,
                exit_price: order.exit_price.unwrap_or(0.0),
                pnl_pct: order.pnl_pct.unwrap_or(0.0),
                reason: close_reason(&order),
            },
            OrderEvent::Rejected {
                ts,
                grid,
                level,
                reason,
            } => JobEventKind::OrderRejected {
                ts,
                grid,
                level,
                reason,
            },
            OrderEvent::Rebuilt { ts, grids } => JobEventKind::GridRebuilt { ts, grids },
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Control plane (Redis qua Resolver)
// ═══════════════════════════════════════════════════════════════════════════
pub async fn write_status(
    resolver: &Resolver,
    tenant_id: i64,
    job_id: &str,
    status: &JobStatus,
) -> Result<(), Error> {
    let raw = serde_json::to_string(status)
        .map_err(|e| Error::other(format!("Failed to serialize job status: {e}")))?;
    let mut conn = resolver.cache(tenant_id);
    redis::cmd("HSET")
        .arg(job_status_key(job_id))
        .arg("status")
        .arg(raw)
        .query_async::<()>(&mut conn)
        .await
        .map_err(|e| Error::other(format!("Failed to write job status: {e}")))
}

pub async fn read_status(
    resolver: &Resolver,
    tenant_id: i64,
    job_id: &str,
) -> Result<Option<JobStatus>, Error> {
    let mut conn = resolver.cache(tenant_id);
    let raw: Option<String> = redis::cmd("HGET")
        .arg(job_status_key(job_id))
        .arg("status")
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::other(format!("Failed to read job status: {e}")))?;

    match raw {
        Some(raw) => serde_json::from_str(&raw)
            .map(Some)
            .map_err(|e| Error::other(format!("Malformed job status: {e}"))),
        None => Ok(None),
    }
}

/// Đọc events của job (mới nhất trước) — dùng bởi runner API và MCP.
pub async fn read_events(
    resolver: &Resolver,
    tenant_id: i64,
    job_id: &str,
    limit: usize,
) -> Result<Vec<Value>, Error> {
    let mut conn = resolver.cache(tenant_id);
    let raws: Vec<String> = redis::cmd("LRANGE")
        .arg(job_events_key(job_id))
        .arg(0)
        .arg(limit.saturating_sub(1) as i64)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::other(format!("Failed to read job events: {e}")))?;

    Ok(raws
        .iter()
        .filter_map(|raw| serde_json::from_str(raw).ok())
        .collect())
}

/// Giới hạn 1 job đang chạy / user. Trả về false nếu user đã có job active.
pub async fn try_acquire_user_slot(
    resolver: &Resolver,
    tenant_id: i64,
    user_id: &str,
    job_id: &str,
    ttl_secs: u64,
) -> Result<bool, Error> {
    let mut conn = resolver.cache(tenant_id);
    redis::cmd("SET")
        .arg(user_guard_key(tenant_id, user_id))
        .arg(job_id)
        .arg("NX")
        .arg("EX")
        .arg(ttl_secs)
        .query_async::<Option<String>>(&mut conn)
        .await
        .map(|v| v.is_some())
        .map_err(|e| Error::other(format!("Failed to acquire user slot: {e}")))
}

/// Chỉ xoá guard nếu đang giữ bởi đúng job này (tránh đè job mới của user).
pub async fn release_user_slot(
    resolver: &Resolver,
    tenant_id: i64,
    user_id: &str,
    job_id: &str,
) -> Result<(), Error> {
    let mut conn = resolver.cache(tenant_id);
    let current: Option<String> = redis::cmd("GET")
        .arg(user_guard_key(tenant_id, user_id))
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::other(format!("Failed to read user slot: {e}")))?;

    if current.as_deref() == Some(job_id) {
        let _: Result<(), redis::RedisError> = redis::cmd("DEL")
            .arg(user_guard_key(tenant_id, user_id))
            .query_async(&mut conn)
            .await;
    }

    Ok(())
}

/// Cancellation là best-effort: set flag, executor poll và abort task.
pub async fn request_cancel(
    resolver: &Resolver,
    tenant_id: i64,
    job_id: &str,
) -> Result<(), Error> {
    let mut conn = resolver.cache(tenant_id);
    redis::cmd("SET")
        .arg(job_cancel_key(job_id))
        .arg("1")
        .query_async::<()>(&mut conn)
        .await
        .map_err(|e| Error::other(format!("Failed to request cancel: {e}")))
}

pub async fn is_cancelled(resolver: &Resolver, tenant_id: i64, job_id: &str) -> bool {
    let mut conn = resolver.cache(tenant_id);
    redis::cmd("EXISTS")
        .arg(job_cancel_key(job_id))
        .query_async::<i64>(&mut conn)
        .await
        .map(|v| v > 0)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_event_keeps_job_id_top_level() {
        let event = JobEvent {
            job_id: "j1".into(),
            tenant_id: 7,
            ts: 100,
            kind: JobEventKind::Started,
        };
        let value = serde_json::to_value(&event).unwrap();
        assert_eq!(value["job_id"], "j1");
        assert_eq!(value["type"], "started");
        assert_eq!(value["tenant_id"], 7);
    }

    #[test]
    fn test_order_event_conversion() {
        let event = OrderEvent::Rejected {
            ts: 1,
            grid: 0,
            level: 2,
            reason: "fee".into(),
        };
        let kind = JobEventKind::from(event);
        let value = serde_json::to_value(JobEvent {
            job_id: "j2".into(),
            tenant_id: 1,
            ts: 0,
            kind,
        })
        .unwrap();
        assert_eq!(value["type"], "order_rejected");
        assert_eq!(value["reason"], "fee");
        assert_eq!(value["level"], 2);
    }

    #[test]
    fn test_closed_event_reason_inference() {
        let order = Order {
            dtype: OrderType::Long,
            entry_price: 100.0,
            sl_price: 95.0,
            tp_price: 110.0,
            exit_price: Some(94.0),
            pnl_pct: Some(-0.05),
            ..Default::default()
        };
        let kind = JobEventKind::from(OrderEvent::Closed { ts: 5, order });
        let value = serde_json::to_value(JobEvent {
            job_id: "j4".into(),
            tenant_id: 1,
            ts: 0,
            kind,
        })
        .unwrap();
        assert_eq!(value["type"], "order_closed");
        assert_eq!(value["reason"], "stop_loss");
        assert_eq!(value["entry_price"], 100.0);
    }

    #[test]
    fn test_spec_deserialize_with_grid_strategy() {
        let raw = serde_json::json!({
            "job_id": "j3",
            "tenant_id": 1,
            "user_id": "u1",
            "broker": "simplefx",
            "symbol": "BTCUSD",
            "from": 0,
            "to": 86_400,
            "strategy": {
                "type": "grid",
                "grid_levels": 17,
                "sl_pct": 0.02,
                "lookback_secs": 3600,
                "review_interval_secs": 3600,
                "smoothing_k": 0.5,
                "trading_candle_secs": 300
            }
        });
        let spec: BacktestJobSpec = serde_json::from_value(raw).unwrap();
        assert_eq!(spec.job_id, "j3");
        assert_eq!(spec.capital, 100_000.0);
        assert_eq!(spec.resolution_for_test, "5m");
        // Strategy deserialize qua typetag thành công
        assert_eq!(spec.strategy.init().len(), 5);
    }
}
