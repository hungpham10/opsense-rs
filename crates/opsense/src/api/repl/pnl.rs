//! PnL aggregation from `signal = "order"` observations.
//!
//! Thuần sync, không async, không DB — dùng chung cho GraphQL resolver (`v1.rs`),
//! MCP tool (`mcp/tools.rs`), REPL (`repl/commands.rs`), CLI (`cli.rs`).
//! Tách ra file riêng để test được logic aggregation mà không cần dựng server.

use opsense_model::events::Observation;

/// Parse `interval` string → bucket width in seconds.
///
/// Hợp lệ: `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`, `1w`, `1M`, `0`.
/// - `0` = 1 bucket cho toàn bộ cửa sổ (không chia).
/// - Không fallback âm thầm: interval sai → `Err` kèm danh sách hợp lệ.
///
/// Không dùng `bucket_secs` từ `tick2candle.rs` vì hàm đó `_ => 60`, nên
/// `--interval 1M` hay gõ nhầm `--interval 1x` sẽ **âm thầm** thành "1 phút"
/// và trả số ngày sai.
pub fn parse_bucket_secs(interval: &str) -> Result<i64, String> {
    let s = interval.trim();
    if s.is_empty() {
        return Err(
            "interval rỗng; hợp lệ: 1m,5m,15m,30m,1h,4h,1d,1w,1M,0".into(),
        );
    }
    match s {
        "0" => return Ok(0),
        "1m" => return Ok(60),
        "5m" => return Ok(300),
        "15m" => return Ok(900),
        "30m" => return Ok(1800),
        "1h" => return Ok(3_600),
        "4h" => return Ok(14_400),
        "1d" => return Ok(86_400),
        "1w" => return Ok(604_800),
        "1M" => return Ok(2_592_000), // 30 days
        _ => {}
    }
    Err(format!(
        "interval '{s}' không hợp lệ; chấp nhận: 1m,5m,15m,30m,1h,4h,1d,1w,1M,0"
    ))
}

/// Align timestamp to bucket boundary (floor).
///
/// `ts.div_euclid(bucket_secs) * bucket_secs` — dùng `div_euclid` chứ
/// không phải `/` để đúng với cả timestamp âm (không xảy ra thực tế nhưng
/// an toàn). `bucket_secs = 0` ⇒ trả `0` (1 bucket duy nhất).
fn align_bucket(ts: i64, bucket_secs: i64) -> i64 {
    if bucket_secs == 0 {
        return 0;
    }
    ts.div_euclid(bucket_secs) * bucket_secs
}

/// Deduplicate orders by `order_id`, keeping the **latest** row per id.
///
/// Dịch từ `v1.rs:490-513` để `orders()` và aggregation dùng chung một
/// hàm — sửa một chỗ, hai chỗ đúng. Cursor rows (`kind = trading_step`)
/// không có `order_id` nên luôn được giữ.
///
/// Trả về vector reference trỏ vào slice gốc, không clone.
pub fn dedup_orders<'a>(rows: &'a [&'a Observation]) -> Vec<&'a Observation> {
    use std::collections::HashMap;

    let mut latest: HashMap<&str, (i64, usize)> = Default::default();
    for (i, o) in rows.iter().enumerate() {
        let Some(id) = o.labels.get("order_id").map(String::as_str) else {
            // Cursor hoặc observation không có order_id → luôn giữ
            continue;
        };
        let ts = o.ts;
        match latest.get(id) {
            Some(&(prev_ts, _)) if prev_ts > ts => {}
            _ => {
                latest.insert(id, (ts, i));
            }
        }
    }
    let keep: std::collections::HashSet<usize> = latest.values().map(|(_, i)| *i).collect();
    rows.iter()
        .enumerate()
        .filter(|(i, o)| {
            let is_cursor = o.labels.get("order_id").is_none();
            is_cursor || keep.contains(i)
        })
        .map(|(_, o)| *o)
        .collect()
}

/// A single PnL bucket (time window).
#[derive(Debug, Clone, Default)]
pub struct PnlBucket {
    pub bucket_ts: i64,
    pub trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub win_rate: f64,
    pub net_pnl_abs: f64,
    pub net_pnl_pct: f64,
    pub gross_profit_abs: f64,
    pub gross_loss_abs: f64,
    pub notional: f64,
    pub avg_win_pct: f64,
    pub avg_loss_pct: f64,
    pub long_trades: usize,
    pub short_trades: usize,
    pub open_count: usize,
    pub open_notional: f64,
}

impl PnlBucket {
    fn new(bucket_ts: i64) -> Self {
        Self {
            bucket_ts,
            ..Default::default()
        }
    }

    fn add_closed(&mut self, size: f64, pnl_pct: f64, dtype: &str) {
        self.trades += 1;
        self.net_pnl_abs += size * pnl_pct;
        self.net_pnl_pct += pnl_pct;
        self.notional += size;

        if pnl_pct > 0.0 {
            self.wins += 1;
            self.gross_profit_abs += size * pnl_pct;
        } else {
            self.losses += 1;
            self.gross_loss_abs += size * pnl_pct;
        }

        match dtype {
            "long" => self.long_trades += 1,
            "short" => self.short_trades += 1,
            _ => {}
        }
    }

    fn add_open(&mut self, size: f64) {
        self.open_count += 1;
        self.open_notional += size;
    }

    fn finalize(&mut self) {
        if self.trades > 0 {
            self.win_rate = self.wins as f64 / self.trades as f64;
            self.net_pnl_pct /= self.trades as f64;
            if self.wins > 0 {
                self.avg_win_pct = self.gross_profit_abs / self.wins as f64;
            }
            if self.losses > 0 {
                self.avg_loss_pct = self.gross_loss_abs / self.losses as f64;
            }
        }
    }
}

/// Complete PnL summary for a query window.
#[derive(Debug, Clone, Default)]
pub struct PnlSummary {
    pub interval: String,
    pub bucket_secs: i64,
    pub from_ts: i64,
    pub to_ts: i64,
    pub complete: bool,
    pub trades: usize,
    pub zero_size_rows: usize,
    pub net_pnl_abs: f64,
    pub net_pnl_pct: f64,
    pub gross_profit_abs: f64,
    pub gross_loss_abs: f64,
    pub notional: f64,
    pub wins: usize,
    pub losses: usize,
    pub win_rate: f64,
    pub avg_win_pct: f64,
    pub avg_loss_pct: f64,
    pub long_trades: usize,
    pub short_trades: usize,
    pub open_count: usize,
    pub open_notional: f64,
    pub unrealized_abs: f64,
    pub mark_price: Option<f64>,
    pub total: PnlBucket,
    pub buckets: Vec<PnlBucket>,
}

/// Aggregate deduplicated orders into PnL buckets.
///
/// - `rows` = đã dedup qua `dedup_orders`, chỉ chứa `signal == Order`
/// - `bucket_secs` = 0 ⇒ 1 bucket total; > 0 ⇒ chia theo interval
/// - `mark_price` = giá để tính unrealized PnL của lệnh đang mở; `None` = bỏ qua
/// - `fee_roundtrip` = phí khứ hồi (entry + exit), mặc định `0.0004` (0.04%)
/// - `pnl_pct` trong observation **đã trừ phí** (xem `qlib/portfolio.rs:1279`), **không** trừ lần 2
pub fn aggregate(
    rows: &[&Observation],
    bucket_secs: i64,
    mark_price: Option<f64>,
    fee_roundtrip: f64,
) -> PnlSummary {
    let mut summary = PnlSummary {
        interval: match bucket_secs {
            0 => "0".to_string(),
            60 => "1m".to_string(),
            300 => "5m".to_string(),
            900 => "15m".to_string(),
            1800 => "30m".to_string(),
            3_600 => "1h".to_string(),
            14_400 => "4h".to_string(),
            86_400 => "1d".to_string(),
            604_800 => "1w".to_string(),
            2_592_000 => "1M".to_string(),
            _ => format!("{}s", bucket_secs),
        },
        bucket_secs,
        from_ts: rows.first().map(|o| o.ts).unwrap_or(0),
        to_ts: rows.last().map(|o| o.ts).unwrap_or(0),
        complete: true,
        ..Default::default()
    };

    if rows.is_empty() {
        return summary;
    }

    use std::collections::BTreeMap;
    let mut buckets: BTreeMap<i64, PnlBucket> = BTreeMap::new();

    for o in rows {
        let dtype = o.labels.get("dtype").map(String::as_str).unwrap_or("");
        let size = o.labels.get("size").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let status = o.labels.get("status").map(String::as_str).unwrap_or("");
        let entry = o.value;

        let bucket_ts = align_bucket(o.ts, bucket_secs);
        let bucket = buckets.entry(bucket_ts).or_insert_with(|| PnlBucket::new(bucket_ts));

        match status {
            "closed" => {
                let pnl_pct = o
                    .labels
                    .get("pnl_pct")
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);

                // size = 0 vẫn đếm là 1 trade (để biết có lệnh), nhưng không cộng PnL
                if size == 0.0 {
                    summary.zero_size_rows += 1;
                }
                bucket.add_closed(size, pnl_pct, dtype);

                // Unrealized: lệnh mở không có pnl_pct, tính tại đây nếu có mark_price
            }
            "open" => {
                if size > 0.0 {
                    bucket.add_open(size);
                }
                // Unrealized sẽ tính sau khi biết mark_price
                let _ = entry; // unused when no mark_price
            }
            _ => {}
        }
    }

    // Tính unrealized cho từng bucket nếu có mark_price
    if let Some(mark) = mark_price {
        for o in rows {
            let status = o.labels.get("status").map(String::as_str).unwrap_or("");
            if status != "open" {
                continue;
            }
            let size = o.labels.get("size").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            if size == 0.0 {
                continue;
            }
            let dtype = o.labels.get("dtype").map(String::as_str).unwrap_or("");
            let entry = o.value;
            let pnl_pct = match dtype {
                "long" => (mark - entry) / entry - fee_roundtrip,
                "short" => (entry - mark) / entry - fee_roundtrip,
                _ => 0.0,
            };
            let bucket_ts = align_bucket(o.ts, bucket_secs);
            if let Some(bucket) = buckets.get_mut(&bucket_ts) {
                bucket.net_pnl_abs += size * pnl_pct;
                bucket.net_pnl_pct += pnl_pct; // unrealized cộng vào net_pnl_pct của bucket
                bucket.notional += size;
                if pnl_pct > 0.0 {
                    bucket.gross_profit_abs += size * pnl_pct;
                } else {
                    bucket.gross_loss_abs += size * pnl_pct;
                }
            }
            summary.unrealized_abs += size * pnl_pct;
        }
    }

    // Finalize buckets + roll up to total
    let mut total = PnlBucket::new(0);

    if bucket_secs > 0 && !buckets.is_empty() {
        // Fill gap buckets: create empty buckets for the entire range
        let min_bucket = *buckets.keys().min().unwrap();
        let max_bucket = *buckets.keys().max().unwrap();
        let mut current = min_bucket;
        while current <= max_bucket {
            let bucket = buckets.entry(current).or_insert_with(|| PnlBucket::new(current));
            bucket.finalize();
            summary.trades += bucket.trades;
            summary.net_pnl_abs += bucket.net_pnl_abs;
            summary.net_pnl_pct += bucket.net_pnl_pct;
            summary.gross_profit_abs += bucket.gross_profit_abs;
            summary.gross_loss_abs += bucket.gross_loss_abs;
            summary.notional += bucket.notional;
            summary.wins += bucket.wins;
            summary.losses += bucket.losses;
            summary.long_trades += bucket.long_trades;
            summary.short_trades += bucket.short_trades;
            summary.open_count += bucket.open_count;
            summary.open_notional += bucket.open_notional;

            total.trades += bucket.trades;
            total.net_pnl_abs += bucket.net_pnl_abs;
            total.net_pnl_pct += bucket.net_pnl_pct;
            total.gross_profit_abs += bucket.gross_profit_abs;
            total.gross_loss_abs += bucket.gross_loss_abs;
            total.notional += bucket.notional;
            total.wins += bucket.wins;
            total.losses += bucket.losses;
            total.long_trades += bucket.long_trades;
            total.short_trades += bucket.short_trades;
            total.open_count += bucket.open_count;
            total.open_notional += bucket.open_notional;

            summary.buckets.push(bucket.clone());
            current += bucket_secs;
        }
    } else {
        for (_, mut bucket) in buckets {
            bucket.finalize();
            summary.trades += bucket.trades;
            summary.zero_size_rows += 0; // đã cộng ở trên
            summary.net_pnl_abs += bucket.net_pnl_abs;
            summary.net_pnl_pct += bucket.net_pnl_pct;
            summary.gross_profit_abs += bucket.gross_profit_abs;
            summary.gross_loss_abs += bucket.gross_loss_abs;
            summary.notional += bucket.notional;
            summary.wins += bucket.wins;
            summary.losses += bucket.losses;
            summary.long_trades += bucket.long_trades;
            summary.short_trades += bucket.short_trades;
            summary.open_count += bucket.open_count;
            summary.open_notional += bucket.open_notional;

            total.trades += bucket.trades;
            total.net_pnl_abs += bucket.net_pnl_abs;
            total.net_pnl_pct += bucket.net_pnl_pct;
            total.gross_profit_abs += bucket.gross_profit_abs;
            total.gross_loss_abs += bucket.gross_loss_abs;
            total.notional += bucket.notional;
            total.wins += bucket.wins;
            total.losses += bucket.losses;
            total.long_trades += bucket.long_trades;
            total.short_trades += bucket.short_trades;
            total.open_count += bucket.open_count;
            total.open_notional += bucket.open_notional;

            summary.buckets.push(bucket);
        }
    }

    total.finalize();
    summary.total = total;

    if summary.trades > 0 {
        summary.win_rate = summary.wins as f64 / summary.trades as f64;
        summary.net_pnl_pct /= summary.trades as f64;
        if summary.wins > 0 {
            summary.avg_win_pct = summary.gross_profit_abs / summary.wins as f64;
        }
        if summary.losses > 0 {
            summary.avg_loss_pct = summary.gross_loss_abs / summary.losses as f64;
        }
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_model::events::{Signal, TelemetryKind};

    fn order_obs(
        ts: i64,
        entry: f64,
        dtype: &str,
        size: f64,
        status: &str,
        pnl_pct: Option<f64>,
        labels: &[(&str, &str)],
    ) -> Observation {
        let mut o = Observation::new(ts, "BTCUSDT".into(), TelemetryKind::Metric, Signal::Order, entry);
        o.labels.insert("dtype".into(), dtype.into());
        o.labels.insert("size".into(), size.to_string());
        o.labels.insert("status".into(), status.into());
        if let Some(p) = pnl_pct {
            o.labels.insert("pnl_pct".into(), p.to_string());
        }
        for (k, v) in labels {
            o.labels.insert((*k).into(), (*v).into());
        }
        o
    }

    #[test]
    fn parse_bucket_secs_valid() {
        assert_eq!(parse_bucket_secs("1m"), Ok(60));
        assert_eq!(parse_bucket_secs("5m"), Ok(300));
        assert_eq!(parse_bucket_secs("15m"), Ok(900));
        assert_eq!(parse_bucket_secs("30m"), Ok(1800));
        assert_eq!(parse_bucket_secs("1h"), Ok(3600));
        assert_eq!(parse_bucket_secs("4h"), Ok(14400));
        assert_eq!(parse_bucket_secs("1d"), Ok(86400));
        assert_eq!(parse_bucket_secs("1w"), Ok(604800));
        assert_eq!(parse_bucket_secs("1M"), Ok(2_592_000));
        assert_eq!(parse_bucket_secs("0"), Ok(0));
    }

    #[test]
    fn parse_bucket_secs_invalid_is_rejected() {
        assert!(parse_bucket_secs("1x").is_err());
        assert!(parse_bucket_secs("1M2").is_err());
        assert!(parse_bucket_secs("").is_err());
        assert!(parse_bucket_secs("  ").is_err());
    }

    #[test]
    fn dedup_orders_keeps_latest_per_id() {
        let now = 1_000_000;
        let rows = vec![
            order_obs(now - 200, 100.0, "long", 100.0, "open", None, &[("order_id", "o1")]),
            order_obs(now - 100, 101.0, "long", 100.0, "closed", Some(0.01), &[("order_id", "o1")]),
            order_obs(now, 102.0, "short", 200.0, "open", None, &[("order_id", "o2")]),
        ];
        let rows_ref: Vec<_> = rows.iter().collect();
        let deduped = dedup_orders(&rows_ref);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].ts, now - 100); // o1 latest
        assert_eq!(deduped[1].ts, now); // o2
    }

    #[test]
    fn dedup_orders_keeps_cursor_rows() {
        let now = 1_000_000;
        let cursor = Observation::new(now - 50, "BTCUSDT".into(), TelemetryKind::Metric, Signal::Summary, 1.0)
            .with_label("kind", "trading_step");
        let rows = vec![
            cursor.clone(),
            order_obs(now, 100.0, "long", 100.0, "closed", Some(0.01), &[("order_id", "o1")]),
        ];
        let rows_ref: Vec<_> = rows.iter().collect();
        let deduped = dedup_orders(&rows_ref);
        assert_eq!(deduped.len(), 2);
    }

    #[test]
    fn closed_rows_count_as_trades_not_open_rows() {
        let now = 1_000_000;
        let rows = vec![
            order_obs(now - 100, 100.0, "long", 100.0, "open", None, &[("order_id", "o1")]),
            order_obs(now, 101.0, "long", 100.0, "closed", Some(0.01), &[("order_id", "o1")]),
        ];
        let rows_ref: Vec<_> = rows.iter().collect();
        let deduped = dedup_orders(&rows_ref);
        let sum = aggregate(&deduped, 0, None, 0.0004);
        assert_eq!(sum.trades, 1, "một lệnh có open+closed = 1 trade");
    }

    #[test]
    fn size_times_pnl_pct_is_not_double_charging_fees() {
        let now = 1_000_000;
        let rows = vec![order_obs(now, 100.0, "long", 100.0, "closed", Some(0.0011759), &[("order_id", "o1")])];
        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 0, None, 0.0004);
        // net_pnl_abs = 100 * 0.0011759 = 0.11759 — KHÔNG trừ thêm 0.0004
        assert!((sum.net_pnl_abs - 0.11759).abs() < 1e-9);
    }

    #[test]
    fn trades_and_buckets_fill_empty_gaps() {
        let now = 3_600_000; // align to 1h buckets
        let rows = vec![
            order_obs(now, 100.0, "long", 100.0, "closed", Some(0.01), &[("order_id", "o1")]),
            order_obs(now + 4 * 3600, 100.0, "long", 100.0, "closed", Some(0.01), &[("order_id", "o2")]),
        ];
        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 3600, None, 0.0004); // 1h buckets
        // Buckets 0,1,2,3,4 = 5 buckets, 2 có trade, 3 zero
        assert_eq!(sum.buckets.len(), 5);
        assert_eq!(sum.buckets[0].trades, 1);
        assert_eq!(sum.buckets[1].trades, 0);
        assert_eq!(sum.buckets[4].trades, 1);
    }

    #[test]
    fn cursor_rows_are_not_trades() {
        let now = 1_000_000;
        let cursor = Observation::new(now, "BTCUSDT".into(), TelemetryKind::Metric, Signal::Summary, 1.0)
            .with_label("kind", "trading_step");
        let rows = vec![cursor, order_obs(now, 100.0, "long", 100.0, "closed", Some(0.01), &[("order_id", "o1")])];
        let rows_ref: Vec<_> = rows.iter().collect();
        let deduped = dedup_orders(&rows_ref);
        let sum = aggregate(&deduped, 0, None, 0.0004);
        assert_eq!(sum.trades, 1);
    }

    #[test]
    fn total_is_window_wide_not_limit_tied() {
        let now = 1_000_000;
        // 10 trades nhưng bucket_secs = 0 (total only)
        let mut rows = Vec::new();
        for i in 0..10 {
            rows.push(order_obs(now + i, 100.0, "long", 100.0, "closed", Some(0.01), &[("order_id", &format!("o{}", i))]));
        }
        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 0, None, 0.0004);
        assert_eq!(sum.trades, 10);
        assert_eq!(sum.total.trades, 10);
    }

    #[test]
    fn unrealized_is_marked_to_mark_price() {
        let now = 1_000_000;
        let rows = vec![
            order_obs(now, 100.0, "long", 100.0, "open", None, &[("order_id", "o1")]),
            order_obs(now, 100.0, "short", 100.0, "open", None, &[("order_id", "o2")]),
        ];
        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 0, Some(101.0), 0.0004);
        // long: (101-100)/100 - 0.0004 = 0.01 - 0.0004 = 0.0096
        // short: (100-101)/100 - 0.0004 = -0.01 - 0.0004 = -0.0104
        assert!((sum.unrealized_abs - (100.0 * 0.0096 + 100.0 * (-0.0104))).abs() < 1e-9);
        // openCount = 2
        assert_eq!(sum.open_count, 2);
    }

    #[test]
    fn unrealized_is_zero_without_mark_price() {
        let now = 1_000_000;
        let rows = vec![order_obs(now, 100.0, "long", 100.0, "open", None, &[("order_id", "o1")])];
        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 0, None, 0.0004);
        assert_eq!(sum.unrealized_abs, 0.0);
        assert_eq!(sum.mark_price, None);
    }

    #[test]
    fn mark_price_uses_node_fee_rate() {
        let now = 1_000_000;
        let rows = vec![order_obs(now, 100.0, "long", 100.0, "open", None, &[("order_id", "o1")])];
        // fee_roundtrip = 0.0006 (0.03% * 2)
        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 0, Some(101.0), 0.0006);
        // (101-100)/100 - 0.0006 = 0.01 - 0.0006 = 0.0094
        assert!((sum.unrealized_abs - 100.0 * 0.0094).abs() < 1e-9);
    }

    #[test]
    fn bucket_aligns_on_positive_and_negative_ts() {
        assert_eq!(align_bucket(100, 60), 60);
        assert_eq!(align_bucket(119, 60), 60);
        assert_eq!(align_bucket(120, 60), 120);
        assert_eq!(align_bucket(-1, 60), -60);
        assert_eq!(align_bucket(-60, 60), -60);
        assert_eq!(align_bucket(-61, 60), -120);
        assert_eq!(align_bucket(0, 0), 0);
        assert_eq!(align_bucket(1000, 0), 0);
    }

    #[test]
    fn empty_window_yields_zero_buckets_not_error() {
        let rows: Vec<&Observation> = vec![];
        let sum = aggregate(&rows, 3600, None, 0.0004);
        assert_eq!(sum.trades, 0);
        assert_eq!(sum.buckets.len(), 0);
        assert_eq!(sum.total.trades, 0);
    }
}