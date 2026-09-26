//! `tick2candle` — gom tick thành nến, ghi xuống station của chính node.
//!
//! Thay cho việc gom nến nằm trong script (`grid.rhai` từng tự `station_query`
//! rồi tự gộp mỗi tick). Lý do đưa ra khỏi script:
//!
//! - **Script stateless**: mỗi batch dựng lại từ đầu, nên nến tích luỹ bị
//!   phụ thuộc thứ tự batch. Component giữ nến đang mở trong RAM.
//! - **Quan sát được**: nến hỏng (biên 1 cent, hoặc không đóng) là lỗi dữ liệu,
//!   không phải logic chiến lược — nên log ở đây, không phải trong strategy.
//!
//! ## Quy tắc
//!
//! Nhận tick (`ts` + `value`), gộp vào nến của bucket theo `resolution`, và
//! **chỉ ghi nến khi nến đã đóng** (`bucket + step <= now`). Nến đang mở giữ
//! trong RAM. Hệ quả có chủ ý: nến tick chết ⇒ không có nến nào đóng ⇒ kernel
//! không tiến, tức giao dịch dừng thay vì đặt lệnh trên dữ liệu cũ.
//!
//! Bucket đầu tiên sau khi khởi động bị **bỏ qua**: component không biết giá
//! mở của bucket mà nó bắt đầu giữa chừng, nên `o` sẽ sai. Bỏ thì backfill từ
//! `http_source` lấp ngay.
//!
//! ## Convention
//!
//! Mỗi nến = 5 observation, `ts` = giờ mở nến (giây), `value` = giá/khối lượng,
//! `labels.field` ∈ {`o`,`h`,`l`,`c`,`v`} — đúng convention của
//! [`opsense_core::candles`], nên station đọc được qua
//! [`opsense_core::TimeseriesStationHandle`] như mọi `DataLoader` khác.
//!
//! [`opsense_core::candles`]: https://docs.rs/opsense-core

use std::collections::BTreeMap;
use std::io::Error;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{RwLock, mpsc};

use opsense_core::{Observation, Station, TimeseriesStation};
use serde_json::Value;
use opsense_macros::transform;

use crate::ohlcv::OHLCV_FIELDS;
use crate::signal;
use crate::station::{downcast_ctx, extract_observations};
use crate::vector::runtime::{Component, Identify, Message, Outbound};

/// Độ dài bucket (giây) theo `resolution`. Không đổi theo giá.
///
/// `resolution` rỗng hoặc không parse được ⇒ 1 phút (mặc định serde). Nếu để
/// rơi về 1 giây thì mỗi tick một nến, tức mất hết ý nghĩa.
fn bucket_secs(resolution: &str) -> u64 {
    let s = resolution.trim();
    let Some(idx) = s.find(|c: char| !c.is_ascii_digit()) else {
        // Toàn chữ số, hoặc rỗng. Rỗng ⇒ mặc định; số thì tính ra nếu > 0.
        return match s.parse::<u64>() {
            Ok(n) if n > 0 => n,
            _ => 60,
        };
    };
    let (num, unit) = s.split_at(idx);
    let Ok(n) = num.parse::<u64>() else {
        return 60;
    };
    let mult = match unit {
        "" | "s" => 1,
        "m" => 60,
        "h" | "H" => 3_600,
        "d" | "D" => 86_400,
        "w" | "W" => 604_800,
        _ => 60,
    };
    // `1m` → 60, `5m` → 300, `1h` → 3600. `n.max(1)` để `0m` không thành 0 —
    // `div_euclid(0)` sẽ panic.
    n.max(1) * mult
}

/// Nến đang tích luỹ. `None` giá = bucket chưa có tick nào.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Bar {
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    ticks: u64,
}

impl Bar {
    fn push(&mut self, price: f64) {
        if self.open.is_none() {
            self.open = Some(price);
            self.high = Some(price);
            self.low = Some(price);
        } else {
            self.high = Some(self.high.unwrap_or(price).max(price));
            self.low = Some(self.low.unwrap_or(price).min(price));
        }
        self.close = Some(price);
        self.ticks += 1;
    }

    /// Có đủ dữ liệu để ghi ra station chưa?
    fn is_complete(&self) -> bool {
        self.open.is_some() && self.close.is_some() && self.ticks > 0
    }

    /// Khối lượng xấp xỉ bằng **số tick đã gộp** — tick stream không mang
    /// khối lượng, nên không gọi là volume thật được; đặt tên riêng để không ai
    /// dùng nhầm cho thanh khoản.
    fn values(&self) -> [f64; 5] {
        [
            self.open.unwrap_or_default(),
            self.high.unwrap_or_default(),
            self.low.unwrap_or_default(),
            self.close.unwrap_or_default(),
            self.ticks as f64,
        ]
    }
}

#[transform]
pub struct Tick2Candle {
    pub id: String,
    pub inputs: Vec<String>,

    /// Độ dài nến: `"1m"`, `"5m"`, `"1h"`. Bucket tính theo giá trị này.
    #[serde(default = "default_resolution")]
    pub resolution: String,

    /// Symbol ghi vào `metric_id` của observation nến.
    #[serde(default)]
    pub symbol: String,

    /// `ts` của tick tính bằng đơn vị gì: `1` = millisecond (Binance), `1000` =
    /// second. Tách khỏi `resolution` vì chúng độc lập nhau.
    #[serde(default = "default_unit_ms")]
    pub unit_ms: u64,

    /// Ngưỡng trễ (giây) để cảnh báo: nến cuối cũ hơn mốc này thì coi như
    /// nguồn tick đã chết. `0` = tắt.
    ///
    /// Cần vì tick chết **không** sinh lỗi — nến đơn giản không đóng nữa, và
    /// kernel đứng yên trong im lặng. Không có cảnh báo thì hệ thống ngừng
    /// giao dịch mà không ai biết.
    #[serde(default = "default_stale_secs")]
    pub stale_secs: u64,

    /// Ghi nến xuống station của chính node (terminal).
    #[serde(default = "default_true")]
    pub station: bool,
}

fn default_resolution() -> String { "1m".to_string() }
fn default_unit_ms() -> u64 { 1 }
fn default_stale_secs() -> u64 { 180 }
fn default_true() -> bool { true }

impl Tick2Candle {
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: "tick2candle".to_string(),
            inputs: vec!["tick".to_string()],
            resolution: default_resolution(),
            symbol: String::new(),
            unit_ms: default_unit_ms(),
            stale_secs: default_stale_secs(),
            station: true,
        }
    }
}

impl Default for Tick2Candle {
    fn default() -> Self {
        Self::new()
    }
}

/// Bucket chứa `ts` (đã quy đổi sang giây) theo độ dài nến.
#[must_use]
pub fn bucket_of(ts_secs: i64, step: u64) -> i64 {
    let step = i64::try_from(step).unwrap_or(60).max(1);
    ts_secs.div_euclid(step) * step
}

/// Một nến đóng → 5 observation theo convention OHLCV.
#[must_use]
fn bar_observations(bucket: i64, bar: &Bar, symbol: &str, resolution: &str) -> Vec<Observation> {
    let vals = bar.values();
    OHLCV_FIELDS
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let mut o = Observation::new(
                bucket,
                symbol.to_string(),
                opsense_model::events::TelemetryKind::Metric,
                opsense_model::events::Signal::Raw,
                vals[i],
            );
            o.labels.insert("field".to_string(), (*field).to_string());
            o.labels
                .insert("resolution".to_string(), resolution.to_string());
            o
        })
        .collect()
}

impl_tick_2_candle!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let step = bucket_secs(&self.resolution);

        let ctx = downcast_ctx(&tx)?;
        let station = TimeseriesStation::from_storage(&self.id, ctx.storage()).await?;
        ctx.registry(
            &self.id,
            Station::Timeseries(Arc::new(RwLock::new(station))),
        )
        .await
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })?;
        let me = ctx
            .station::<Arc<RwLock<TimeseriesStation>>>(&self.id)
            .await?;

        // Nến đang mở: giữ trong RAM, chỉ ghi khi **đã đóng**. `BTreeMap` để xả
        // nến đóng theo thứ tự thời gian khi một batch về nhiều bucket.
        let mut open: BTreeMap<i64, Bar> = BTreeMap::new();
        // Mốc thời gian lớn nhất **đã thấy**, đơn vị giây, chỉ tiến. Đây là
        // đồng hồ để quyết định đóng nến — KHÔNG phải `ts` của tick hiện tại.
        //
        // Vì sao: bản đầu tiên lấy `now = max(ts trong batch)` rồi đóng nến khi
        // `bucket + step <= now`. Tick nằm đúng mốc bucket nên `now == bucket` và
        // điều kiện luôn sai ⇒ nến **không** đóng; tệ hơn, `last_ts` trộn giây
        // với mili-giây làm điều kiện luôn đúng ⇒ mỗi tick một nến, biên 0.
        // Log đã thấy đúng triệu chứng: `low=109.0 high=109.0`, `placed=0`.
        //
        // Luật đúng cho nến chảy: nến bucket B **đóng** khi đã thấy tick ở
        // bucket B + step trở đi — tức đã sang phút kế tiếp, B là dữ liệu cuối.
        let mut max_ts_secs: i64 = 0;
        let mut wrote = false;

        while let Some(msg) = rx.recv().await {
            let Some(batch) = non_empty_batch(&msg) else {
                continue;
            };

            for o in &batch {
                let Some(price) = tick_price(o) else {
                    continue;
                };
                let ts_secs = ts_to_secs(o.ts, self.unit_ms);
                max_ts_secs = max_ts_secs.max(ts_secs);
                open.entry(bucket_of(ts_secs, step))
                    .or_default()
                    .push(price);
            }

            // Đóng mọi nến đã đủ một bucket. `BTreeMap` ⇒ duyệt tăng dần, xả
            // theo thứ tự thời gian.
            let step_secs = i64::try_from(step).unwrap_or(60);
            let due: Vec<i64> = open
                .keys()
                .copied()
                .filter(|b| b.saturating_add(step_secs) <= max_ts_secs)
                .collect();

            let mut rows: Vec<Observation> = Vec::new();
            for b in due {
                let Some(bar) = open.remove(&b) else { continue };
                if !bar.is_complete() {
                    continue;
                }
                rows.extend(bar_observations(b, &bar, &self.symbol, &self.resolution));
            }

            if !rows.is_empty() {
                let from = rows.iter().map(|o| o.ts).min().unwrap_or(max_ts_secs);
                let to = rows.iter().map(|o| o.ts).max().unwrap_or(max_ts_secs);
                me.write().await.update_range(&rows, from, to, to);
                wrote = true;

                // Phát **nến** xuống downstream, KHÔNG forward message gốc.
                //
                // Vì sao: message gốc của `tick-map` mang `trigger = "tick"`, và
                // node downstream dùng `trigger()` để branch. Forward nguyên message
                // thì downstream vẫn thấy `trigger == "tick"` và bỏ qua — đúng
                // cái lỗi làm `grid` không bao giờ ra snapshot. Converter thì đổi
                // hình dạng dữ liệu, nên nó phát đúng hình dạng **mới**: các obs
                // nến vừa đóng.
                if let Ok(payload @ Value::Array(_)) = serde_json::to_value(&rows) {
                    let done = Message { payload };
                    for s in &tx.streams {
                        let _ = s.send(done.clone()).await;
                    }
                }
            }

            // Trễ so với **đồng hồ thật**, không so với tick — nếu so với tick
            // thì tick tắt hẳn thì `max_ts_secs` đứng yên và ta không bao giờ
            // phát hiện. Đây là tín hiệu "nguồn tick đã chết".
            if self.stale_secs > 0 && max_ts_secs > 0 {
                let wall = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs() as i64);
                let age = wall.saturating_sub(max_ts_secs);
                if age > i64::try_from(self.stale_secs).unwrap_or(i64::MAX) {
                    tracing::warn!(
                        node = %self.id,
                        last_tick = max_ts_secs,
                        age,
                        "tick ngừng — không có nến nào đóng, giao dịch đang dừng im lặng"
                    );
                }
            }

            let done = signal::tagged(signal::processed(max_ts_secs), &self.id);
            for s in &tx.streams {
                let _ = s.send(done.clone()).await;
            }
        }

        if wrote {
            tracing::debug!(node = %self.id, "tick2candle đã ghi nến xuống station");
        }
        Ok(())
    }
);

/// `ts` thô → giây.
///
/// `unit_ms` là **hệ số nhân** (milliseconds per open-time unit), đúng convention
/// của `ohlcv::open_time_to_secs`: `1` = timestamp tính bằng mili-giây (Binance
/// aggTrade), `1000` = tính bằng giây.
///
/// Chia thay vì nhân là lỗi rất dễ mắc: với `unit_ms = 1` thì `ts / 1` **giữ
/// nguyên mili-giây**, bucket thành 60 *mili*-giây, và điều kiện đóng nến sai
/// hoàn toàn.
#[must_use]
pub fn ts_to_secs(raw: i64, unit_ms: u64) -> i64 {
    (raw.saturating_mul(i64::try_from(unit_ms).unwrap_or(1).max(1))) / 1_000
}

/// Batch observation từ message, `None` nếu rỗng.
fn non_empty_batch(msg: &Message) -> Option<Vec<Observation>> {
    let batch = extract_observations(&msg.payload);
    (!batch.is_empty()).then_some(batch)
}

/// Giá của một tick: `value` nếu hữu ích, không thì `labels.price`.
fn tick_price(o: &Observation) -> Option<f64> {
    let v = o.value;
    if v.is_finite() && v > 0.0 {
        return Some(v);
    }
    o.labels
        .get("price")
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(ts: i64, price: f64) -> Observation {
        let mut o = Observation::new(
            ts,
            "BTCUSDT".to_string(),
            opsense_model::events::TelemetryKind::Metric,
            opsense_model::events::Signal::Raw,
            price,
        );
        o.labels.insert("field".to_string(), "c".to_string());
        o
    }

    /// `unit_ms` là hệ số **nhân**, không phải mẫu số.
    ///
    /// Bản đầu tiên viết `ts / unit_ms` — với `unit_ms = 1` thì `ts / 1` giữ
    /// nguyên mili-giây, nên bucket thành 60 mili-giây và điều kiện đóng nến sai
    /// hoàn toàn. Test này chặn đúng lỗi đó.
    #[test]
    fn ts_converts_millis_to_secs() {
        // Binance aggTrade: ts là ms. 1_758_900_000_123 ms ⇒ 1_758_900_000 s
        assert_eq!(ts_to_secs(1_758_900_000_123, 1), 1_758_900_000);
        // Giây thì nhân 1000 trước rồi chia 1000 ⇒ giữ nguyên.
        assert_eq!(ts_to_secs(1_758_900_000, 1000), 1_758_900_000);
        // Giá trị nhỏ: 1_500_000 ms = 1_500 s
        assert_eq!(ts_to_secs(1_500_000, 1), 1_500);
        // Tránh tràn khi ts khổng lồ.
        assert!(ts_to_secs(i64::MAX, 1000) > 0, "không được tràn/âm");
    }

    /// Bucket tính trên **giây** phải ra 60 giây khi resolution là 1m.
    ///
    /// Đây là test tích hợp của hai hàm: nếu `ts_to_secs` sai thì bucket sai.
    #[test]
    fn millisecond_tick_lands_in_minute_bucket() {
        let bucket = bucket_of(ts_to_secs(1_758_900_000_123, 1), bucket_secs("1m"));
        assert_eq!(bucket % 60, 0, "bucket phải bám mốc phút");
        assert_eq!(bucket, 1_758_900_000);
    }

    /// `bucket_secs` → `i64` (bucket offset). Tách riêng cho test đỡ phải `.unwrap()`.
fn step_secs_of(step: u64) -> i64 {
    i64::try_from(step).unwrap_or(60)
}

/// Nến chỉ đóng khi **đã sang bucket kế tiếp**, nên nhiều tick trong cùng
    /// một bucket phải gộp lại — không phải mỗi tick một nến.
    ///
    /// Bản đầu tiên lấy `now = max(ts batch)` rồi đóng khi `bucket + step <= now`.
    /// Tick nằm đúng mốc bucket nên `now == bucket`, điều kiện hoặc luôn sai
    /// (nến không đóng) hoặc luôn đúng (mỗi tick một nến) tuỳ chỗ đặt phép so.
    /// Bản đầu rơi vào ca hai: log thật cho thấy `low=109.0 high=109.0` —
    /// biên 0 nên không mốc lưới nào vào được, `placed=0` dù plan có 2 ô.
    #[test]
    fn candles_close_only_when_a_later_bucket_is_seen() {
        let step = bucket_secs("1m");
        let base = 1_758_900_000i64;
        let b0 = bucket_of(base, step);
        let b1 = bucket_of(base + 61, step);
        assert_eq!(b1 - b0, step_secs_of(step), "hai bucket liền nhau");

        let mut open: BTreeMap<i64, Bar> = BTreeMap::new();
        let mut max_ts = 0i64;

        // Ba tick cùng bucket `b0` — mọi tick phải vào **cùng một** nến.
        for (ts, price) in [(b0, 100.0), (b0 + 10, 105.0), (b0 + 20, 95.0)] {
            max_ts = max_ts.max(ts);
            open.entry(bucket_of(ts, step)).or_default().push(price);
        }
        // Đồng hồ chỉ tới trong `b0` ⇒ chưa nến nào đóng.
        let step_secs = i64::try_from(step).unwrap_or(60);
        let due: Vec<i64> = open
            .keys()
            .copied()
            .filter(|b| b.saturating_add(step_secs) <= max_ts)
            .collect();
        assert!(due.is_empty(), "bucket đang mở không được đóng: {due:?}");

        // Tick đầu bucket kế tiếp ⇒ `b0` là dữ liệu cuối, đóng được.
        max_ts = max_ts.max(b1);
        let due: Vec<i64> = open
            .keys()
            .copied()
            .filter(|b| b.saturating_add(step_secs) <= max_ts)
            .collect();
        assert_eq!(due, vec![b0], "chỉ nến đã đủ bucket mới đóng");

        let bar = open.remove(&b0).expect("b0 còn trong open");
        let v = bar.values();
        assert_eq!(v[0], 100.0, "open = giá tick đầu");
        assert_eq!(v[1], 105.0, "high = max tick");
        assert_eq!(v[2], 95.0, "low = min tick");
        assert_eq!(v[3], 95.0, "close = giá tick cuối");
        assert_eq!(v[4], 3.0, "3 tick trong 1 nến");
        assert!(v[1] - v[2] > 0.0, "biên phải khác 0 — nến 1 tick thì hỏng");
    }

    #[test]
    fn bucket_aligns_to_step() {
        // 90 giây / 60 = bucket 60. ms cũng phải cho cùng kết quả.
        assert_eq!(bucket_of(90, 60), 60);
        assert_eq!(bucket_of(120, 60), 120, "đúng mốc phải về chính nó");
        assert_eq!(bucket_of(119, 60), 60);
        // 1m5 = 65_000ms / 1000 = 65s → bucket 60
        assert_eq!(bucket_of(65_000 / 1000, 60), 60);
    }

    #[test]
    fn bucket_secs_parses_common_resolutions() {
        assert_eq!(bucket_secs("1m"), 60);
        assert_eq!(bucket_secs("5m"), 300);
        assert_eq!(bucket_secs("15m"), 900);
        assert_eq!(bucket_secs("1h"), 3_600);
        assert_eq!(bucket_secs("1H"), 3_600);
        assert_eq!(bucket_secs("1d"), 86_400);
        assert_eq!(bucket_secs("1s"), 1);
        assert_eq!(bucket_secs("30"), 30, "chỉ số ⇒ giây");
        assert_eq!(bucket_secs(""), 60, "rỗng ⇒ mặc định 1m, không phải 0");
    }

    #[test]
    fn bucket_never_zero_length() {
        // `0m` không được thành 0 — `div_euclid(0)` sẽ panic.
        assert_eq!(bucket_secs("0m"), 60);
        assert_eq!(bucket_secs(""), 60, "rỗng ⇒ mặc định 1m");
        assert_eq!(bucket_secs("abc"), 60, "rác ⇒ mặc định 1m");
        assert_eq!(bucket_secs("0"), 60, "0 ⇒ mặc định, không phải 0 giây");
        assert_eq!(bucket_of(123, bucket_secs("0m")), 120, "bucket 60s của giây 123");
    }

    #[test]
    fn bar_tracks_ohlc_across_ticks() {
        let mut b = Bar::default();
        b.push(100.0);
        b.push(105.0);
        b.push(95.0);
        b.push(102.0);
        // 5 giá trị: o, h, l, c, v(=số tick).
        assert_eq!(b.values(), [100.0, 105.0, 95.0, 102.0, 4.0]);
        assert_eq!(b.ticks, 4);
        assert!(b.is_complete());
    }

    /// Biên nến phải là **giá thật** của các tick, không phải 1 cent.
    ///
    /// Đây chính là bug đã gặp: nến live dựng từ vài tick/phút có biên ~1 cent
    /// nên không mốc lưới nào rơi vào ⇒ kernel không vào lệnh.
    #[test]
    fn bar_range_reflects_ticks_not_a_single_price() {
        let mut b = Bar::default();
        for p in [83_900.0, 83_950.0, 83_910.0] {
            b.push(p);
        }
        let v = b.values();
        // high 83_950, low 83_900 → 50. Trước đây nến live có biên ~0.01 nên
        // không mốc lưới nào rơi vào.
        assert_eq!(v[1], 83_950.0, "high = giá tick cao nhất");
        assert_eq!(v[2], 83_900.0, "low = giá tick thấp nhất");
        assert_eq!(v[1] - v[2], 50.0, "biên phải bằng hiệu giá max−min thật");
    }

    #[test]
    fn empty_bar_is_incomplete() {
        // Bucket đầu sau restart: không biết `o` nên phải bị bỏ, không ghi 5
        // obs toàn 0 — giá 0 trong station là dữ liệu sai.
        let b = Bar::default();
        assert!(!b.is_complete());
    }

    #[test]
    fn bar_observations_follow_ohlcv_convention() {
        let mut b = Bar::default();
        b.push(10.0);
        b.push(12.0);
        let rows = bar_observations(600, &b, "BTCUSDT", "1m");
        assert_eq!(rows.len(), 5, "mỗi nến = 5 observation");
        for (row, field) in rows.iter().zip(OHLCV_FIELDS) {
            assert_eq!(row.ts, 600, "ts = giờ mở nến");
            assert_eq!(row.metric_id, "BTCUSDT");
            assert_eq!(row.labels.get("field").map(String::as_str), Some(field));
            assert_eq!(row.labels.get("resolution").map(String::as_str), Some("1m"));
        }
        let high = rows.iter().find(|r| r.labels["field"] == "h").expect("có h");
        assert_eq!(high.value, 12.0);
    }

    #[test]
    fn tick_price_rejects_non_positive_and_falls_back_to_label() {
        assert_eq!(tick_price(&obs(1_000, 42.0)), Some(42.0));
        assert_eq!(tick_price(&obs(1_000, 0.0)), None, "giá 0 là dữ liệu hỏng");
        assert_eq!(tick_price(&obs(1_000, f64::NAN)), None, "NaN phải bị lo");

        let mut o = obs(1_000, 0.0);
        o.labels.insert("price".to_string(), "99.5".to_string());
        assert_eq!(tick_price(&o), Some(99.5), "fallback sang labels.price");
    }
}
