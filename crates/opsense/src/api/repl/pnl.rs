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
///
/// `net_pnl_pct`, `avg_win_pct`, `avg_loss_pct` là **trung bình trên mỗi lệnh**
/// (`%`), còn `net_pnl_abs`, `gross_*_abs`, `notional` là **tổng USD**. Ba field
/// `*_sum` ở cuối là tích luỹ thô nội bộ để `finalize` chia ra — không lộ ra
/// GraphQL (v1.rs convert tay từng field).
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
    // ── nội bộ: tích luỹ thô, `finalize` mới đổi thành trung bình ──
    pub pnl_pct_sum: f64,
    pub win_pct_sum: f64,
    pub loss_pct_sum: f64,
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
        self.pnl_pct_sum += pnl_pct;
        self.notional += size;

        if pnl_pct > 0.0 {
            self.wins += 1;
            self.gross_profit_abs += size * pnl_pct;
            self.win_pct_sum += pnl_pct;
        } else {
            self.losses += 1;
            self.gross_loss_abs += size * pnl_pct;
            self.loss_pct_sum += pnl_pct;
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

    /// Cộng dồn **thô** từ bucket khác (trước khi bất kỳ phép chia nào).
    /// Không gộp `bucket_ts`.
    fn roll_up_raw(&mut self, other: &PnlBucket) {
        self.trades += other.trades;
        self.wins += other.wins;
        self.losses += other.losses;
        self.long_trades += other.long_trades;
        self.short_trades += other.short_trades;
        self.open_count += other.open_count;

        self.net_pnl_abs += other.net_pnl_abs;
        self.pnl_pct_sum += other.pnl_pct_sum;
        self.gross_profit_abs += other.gross_profit_abs;
        self.gross_loss_abs += other.gross_loss_abs;
        self.win_pct_sum += other.win_pct_sum;
        self.loss_pct_sum += other.loss_pct_sum;
        self.notional += other.notional;
        self.open_notional += other.open_notional;
    }

    /// Đổi tích luỹ thô thành trung bình. Gọi **sau khi** mọi `roll_up_raw`.
    fn finalize(&mut self) {
        if self.trades > 0 {
            self.win_rate = self.wins as f64 / self.trades as f64;
            self.net_pnl_pct = self.pnl_pct_sum / self.trades as f64;
        }
        if self.wins > 0 {
            self.avg_win_pct = self.win_pct_sum / self.wins as f64;
        }
        if self.losses > 0 {
            self.avg_loss_pct = self.loss_pct_sum / self.losses as f64;
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
///
/// ## Realized vs unrealized
///
/// Mọi số **trừ** `unrealized_abs` là **realized only**: `trades`, `wins`,
/// `gross_profit_abs`/`gross_loss_abs`, `avg_win_pct`/`avg_loss_pct`,
/// `net_pnl_abs`, `notional`. Unrealized (lệnh mở) chỉ nằm một mình trong
/// `unrealized_abs`, không lẫn vào `gross_*` hay `avg_*`.
///
/// Lý do: nếu trộn unrealized vào `gross_profit_abs` thì `avg_win_pct` (chia cho
/// `wins`) bị pha bởi một khoản **không phải win** — và vì unrealized không
/// tăng `wins`, tử số có thêm mà mẫu số không đổi ⇒ trung bình lệnh thắng bị
/// kéo lệch. Cộng 2 field ra vẫn rõ ràng hơn là để một con số nói dối.
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
    // Chỉ bucket nào **có dòng** mới nằm ở đây; gap bucket lấp đủ ở bước sau.
    let mut buckets: BTreeMap<i64, PnlBucket> = BTreeMap::new();

    for o in rows {
        let status = o.labels.get("status").map(String::as_str).unwrap_or("");
        if status != "closed" && status != "open" {
            continue;
        }
        let dtype = o.labels.get("dtype").map(String::as_str).unwrap_or("");
        let size = o.labels.get("size").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let bucket_ts = align_bucket(o.ts, bucket_secs);
        let bucket = buckets.entry(bucket_ts).or_insert_with(|| PnlBucket::new(bucket_ts));

        if status == "closed" {
            // `pnl_pct` đã net-of-fee ⇒ không trừ fee lần 2 (xem comment hàm).
            let pnl_pct = o
                .labels
                .get("pnl_pct")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            // `size = 0` vẫn là 1 trade (để biết có lệnh) nhưng không đóng góp USD.
            if size == 0.0 {
                summary.zero_size_rows += 1;
            }
            bucket.add_closed(size, pnl_pct, dtype);
        } else if size > 0.0 {
            bucket.add_open(size);
        }
    }

    // Unrealized: tính riêng, KHÔNG lẫn vào bucket.
    //
    // Lý do không ghi vào bucket: `gross_profit_abs`/`avg_win_pct` chỉ mô tả
    // realized (xem doc hàm). Một lệnh mở đang lời không phải "win" — chưa chốt.
    if let Some(mark) = mark_price {
        for o in rows {
            if o.labels.get("status").map(String::as_str) != Some("open") {
                continue;
            }
            let size = o.labels.get("size").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            if size == 0.0 {
                continue;
            }
            let dtype = o.labels.get("dtype").map(String::as_str).unwrap_or("");
            let entry = o.value;
            if entry == 0.0 {
                continue;
            }
            let pnl_pct = match dtype {
                "long" => (mark - entry) / entry - fee_roundtrip,
                "short" => (entry - mark) / entry - fee_roundtrip,
                _ => 0.0,
            };
            summary.unrealized_abs += size * pnl_pct;
        }
    }

    // Lấp gap bucket. Dùng `BTreeMap` nên `keys()` đã sort — bucket ra luôn asc.
    if bucket_secs > 0 && !buckets.is_empty() {
        let min_bucket = *buckets.keys().next().expect("không rỗng");
        let max_bucket = *buckets.keys().next_back().expect("không rỗng");
        let mut current = min_bucket;
        while current <= max_bucket {
            buckets
                .entry(current)
                .or_insert_with(|| PnlBucket::new(current));
            current += bucket_secs;
        }
    }

    // Roll up **thô** trước, `finalize` (chia ra trung bình) sau cùng. Làm ngược
    // — finalize từng bucket rồi mới cộng — sẽ chia 2 lần và cho ra trung bình
    // của trung bình, sai khi các bucket có số lệnh lệch nhau.
    let mut total = PnlBucket::new(0);
    for (_, bucket) in &buckets {
        total.roll_up_raw(bucket);
    }

    for (_, mut bucket) in buckets {
        bucket.finalize();
        summary.buckets.push(bucket);
    }

    total.finalize();
    summary.trades = total.trades;
    summary.wins = total.wins;
    summary.losses = total.losses;
    summary.win_rate = total.win_rate;
    summary.net_pnl_abs = total.net_pnl_abs;
    summary.net_pnl_pct = total.net_pnl_pct;
    summary.gross_profit_abs = total.gross_profit_abs;
    summary.gross_loss_abs = total.gross_loss_abs;
    summary.notional = total.notional;
    summary.avg_win_pct = total.avg_win_pct;
    summary.avg_loss_pct = total.avg_loss_pct;
    summary.long_trades = total.long_trades;
    summary.short_trades = total.short_trades;
    summary.open_count = total.open_count;
    summary.open_notional = total.open_notional;
    summary.total = total;

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

    /// `net_pnl_pct` phải là trung bình **trên mỗi lệnh thật**, không phải trung
    /// bình của trung bình theo bucket.
    ///
    /// Đây là bug đã gặp: cộng dồn theo giá trị **đã chia** của từng bucket rồi
    /// chia lại cho tổng lệnh ⇒ chia 2 lần. 3 lệnh ở giờ A và 1 lệnh ở giờ B
    /// là ca khó nhất vì hai bucket lệch nhau 3× về số lệnh.
    ///
    /// Dữ liệu cố ý **không đối xứng** (tổng ≠ 0): với dữ liệu đối xứng cả bản
    /// đúng lẫn bản sai đều cho 0 — test sẽ xanh ngầm.
    ///
    ///   đúng   = Σ pnl_pct / total_trades  = 0.01 / 4 = 0.0025
    ///   bug A  = (avgA + avgB) / total     = −0.02 / 4 = −0.005  (mean-of-means)
    ///   bug B  = dùng avg chưa finalize    = 0 / 4    = 0
    #[test]
    fn net_pnl_pct_is_true_mean_not_mean_of_means() {
        let base = 3_600_000; // thẳng hàng bucket 1h
        let mut rows = Vec::new();
        // Giờ A: 3 lệnh mỗi lệnh +1% ⇒ sum 0.03
        for i in 0..3 {
            rows.push(order_obs(base + i, 100.0, "long", 100.0, "closed", Some(0.01), &[("order_id", &format!("a{i}"))]));
        }
        // Giờ B: 1 lệnh −2% ⇒ sum −0.02
        rows.push(order_obs(base + 3600 + 5, 100.0, "long", 100.0, "closed", Some(-0.02), &[("order_id", "b0")]));

        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 3600, None, 0.0004);
        assert_eq!(sum.trades, 4);

        let expected = (3.0 * 0.01 - 0.02) / 4.0; // 0.0025
        assert!(
            (sum.total.net_pnl_pct - expected).abs() < 1e-12,
            "total.netPnlPct phải là mean thật {expected}, được {}",
            sum.total.net_pnl_pct
        );
        assert!(
            (sum.net_pnl_pct - expected).abs() < 1e-12,
            "summary.netPnlPct phải là mean thật {expected}, được {}",
            sum.net_pnl_pct
        );
        // Hai dạng sai đều bị loại. Thiếu 2 assert này thì test không phân biệt
        // được đúng/sai khi kết quả đúng tình cờ gần 0.
        assert!(
            (sum.total.net_pnl_pct - (-0.005)).abs() > 1e-6,
            "bug mean-of-means: (0.01 + (−0.02))/4 = −0.005"
        );
        assert!(
            sum.total.net_pnl_pct.abs() > 1e-6,
            "bug dùng avg chưa finalize: ra 0"
        );
    }

    /// `avg_win_pct` / `avg_loss_pct` phải là **phần trăm**, không phải USD/win.
    ///
    /// Phiên bản đầu tính `gross_profit_abs / wins` — đó là USD trung bình mỗi lần
    /// thắng, đặt tên là `*_pct` nghe như tỷ lệ.
    #[test]
    fn avg_pcts_are_pct_means_not_usd_per_win() {
        let now = 1_000_000;
        let rows = vec![
            order_obs(now, 100.0, "long", 100.0, "closed", Some(0.02), &[("order_id", "w1")]),
            order_obs(now + 1, 100.0, "long", 100.0, "closed", Some(0.04), &[("order_id", "w2")]),
            order_obs(now + 2, 100.0, "long", 100.0, "closed", Some(-0.01), &[("order_id", "l1")]),
        ];
        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 0, None, 0.0004);
        // avg_win = (0.02 + 0.04) / 2 = 0.03 — là tỷ lệ, không phải 0.03 USD
        assert!((sum.avg_win_pct - 0.03).abs() < 1e-12, "avgWinPct phải là 0.03, được {}", sum.avg_win_pct);
        assert!((sum.avg_loss_pct - (-0.01)).abs() < 1e-12, "avgLossPct phải là −0.01, được {}", sum.avg_loss_pct);
        // gross_profit_abs vẫn là USD: 100×0.02 + 100×0.04 = 6.0
        assert!((sum.gross_profit_abs - 6.0).abs() < 1e-9);
    }

    /// Unrealized **không được** lẫn vào `gross_*`/`avg_*`/`notional`.
    ///
    /// Bug cũ: unrealized của lệnh mở cộng vào `gross_profit_abs`. Vì unrealized
    /// không tăng `wins`, tử số có thêm mà mẫu số không đổi ⇒ `avg_win_pct` bị
    /// pha bởi khoản chưa chốt — một lệnh mở đang lời không phải "win".
    #[test]
    fn unrealized_does_not_pollute_gross_or_averages() {
        let now = 1_000_000;
        // 1 lệnh đã chốt thắng +1%, và 1 lệnh đang mở lời rất mạnh (+50%).
        let mut rows = vec![order_obs(now, 100.0, "long", 100.0, "closed", Some(0.01), &[("order_id", "c1")])];
        rows.push(order_obs(now + 1, 100.0, "long", 100.0, "open", None, &[("order_id", "o1")]));

        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 0, Some(150.0), 0.0004);

        // Realized chỉ có 1 lệnh
        assert_eq!(sum.trades, 1);
        assert_eq!(sum.wins, 1);
        assert_eq!(sum.open_count, 1);
        // gross_profit_abs = 100 × 0.01 = 1.0 — KHÔNG chứa 100×0.4996 = 49.96
        assert!((sum.gross_profit_abs - 1.0).abs() < 1e-9, "grossProfitAbs = {}, phải là 1.0", sum.gross_profit_abs);
        assert_eq!(sum.gross_loss_abs, 0.0);
        // avg_win_pct = 0.01, không phải (0.01 + 0.4996)/1
        assert!((sum.avg_win_pct - 0.01).abs() < 1e-12, "avgWinPct = {}, phải là 0.01", sum.avg_win_pct);
        // notional chỉ tính lệnh đã đóng
        assert!((sum.notional - 100.0).abs() < 1e-9, "notional = {}, phải là 100", sum.notional);
        // Unrealized nằm riêng: (150-100)/100 - 0.0004 = 0.4996 ⇒ 49.96 USD
        assert!((sum.unrealized_abs - 49.96).abs() < 1e-9, "unrealizedAbs = {}", sum.unrealized_abs);
        // net_pnl_abs realized only
        assert!((sum.net_pnl_abs - 1.0).abs() < 1e-9, "netPnlAbs = {}, phải là 1.0", sum.net_pnl_abs);
    }

    /// Bất biến: `total` (và `summary`) phải bằng tổng các bucket theo **từng
    /// field** — không chỉ `net_pnl_abs`. Nếu roll_up_raw quên field nào thì đây
    /// là chỗ đỏ.
    #[test]
    fn total_matches_sum_of_buckets() {
        let base = 3_600_000;
        let mut rows = Vec::new();
        // 2 bucket, mỗi bucket 1 thắng 1 thua + 1 lệnh mở
        for (b, i) in [(0, 0), (0, 1), (3600, 0), (3600, 1)] {
            let pnl = if i % 2 == 0 { Some(0.02) } else { Some(-0.01) };
            rows.push(order_obs(
                base + b + i,
                100.0,
                if i % 2 == 0 { "long" } else { "short" },
                50.0,
                "closed",
                pnl,
                &[("order_id", &format!("o{b}-{i}"))],
            ));
        }
        rows.push(order_obs(base + 10, 100.0, "long", 30.0, "open", None, &[("order_id", "open0")]));
        rows.push(order_obs(base + 3610, 100.0, "short", 20.0, "open", None, &[("order_id", "open1")]));

        let sum = aggregate(&rows.iter().collect::<Vec<_>>(), 3600, None, 0.0004);
        assert_eq!(sum.buckets.len(), 2);

        let s = |f: &dyn Fn(&super::PnlBucket) -> f64| sum.buckets.iter().map(f).sum::<f64>();
        assert!((sum.total.trades as f64 - s(&|b| b.trades as f64)).abs() < 1e-9);
        assert!((sum.total.wins as f64 - s(&|b| b.wins as f64)).abs() < 1e-9);
        assert!((sum.total.losses as f64 - s(&|b| b.losses as f64)).abs() < 1e-9);
        assert!((sum.total.net_pnl_abs - s(&|b| b.net_pnl_abs)).abs() < 1e-9);
        assert!((sum.total.gross_profit_abs - s(&|b| b.gross_profit_abs)).abs() < 1e-9);
        assert!((sum.total.gross_loss_abs - s(&|b| b.gross_loss_abs)).abs() < 1e-9);
        assert!((sum.total.notional - s(&|b| b.notional)).abs() < 1e-9);
        assert!((sum.total.open_count as f64 - s(&|b| b.open_count as f64)).abs() < 1e-9);
        assert!((sum.total.open_notional - s(&|b| b.open_notional)).abs() < 1e-9);
        assert!((sum.total.long_trades as f64 - s(&|b| b.long_trades as f64)).abs() < 1e-9);
        assert!((sum.total.short_trades as f64 - s(&|b| b.short_trades as f64)).abs() < 1e-9);
        // pnl_pct_sum không cộng dồn tuyến tính nên so mean riêng
        assert_eq!(sum.total.open_count, 2);
        assert_eq!(sum.total.trades, 4);
        assert_eq!(sum.total.wins, 2);
        // net = 50×0.02×2 + 50×(−0.01)×2 = 2 − 1 = 1.0
        assert!((sum.total.net_pnl_abs - 1.0).abs() < 1e-9, "netPnlAbs = {}", sum.total.net_pnl_abs);
    }
}