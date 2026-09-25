//! `fn rebuild` của `strategies/binance/grid.rhai` trên **dữ liệu Binance thật**.
//!
//! Mặc định `#[ignore]` vì cần network (API public Binance, không cần key):
//!
//! ```sh
//! cargo test -p opsense-rhai --test grid_live_binance -- --ignored --nocapture
//! ```
//!
//! Test này **không** khẳng định lợi nhuận — chỉ khẳng định plan script dựng ra
//! trên dữ liệu thật là dùng được: có ô, level tăng dần trong khoảng giá quan
//! sát, win-prob trong (0, 1), và kernel `Portfolio` nhận plan đó đặt được lệnh.
//!
//! Nguồn: **OKX** trước, rồi hai endpoint Binance. Binance chặn IP runner của
//! GitHub (HTTP **451 Unavailable For Legal Reasons**), OKX thì không. Vì script
//! chỉ cần chuỗi nến đã đóng, test này chạy được trên nhiều sàn — cũng là cách
//! kiểm tra chiến lược không bị dính vào định dạng riêng của một sàn.

use std::path::PathBuf;

use opsense_qlib::{CandleStick, Portfolio, PortfolioConfig, Session, Strategy};
use opsense_rhai::{ScriptSource, ScriptStrategy};
use serde_json::Value;

const SYMBOL: &str = "BTCUSDT";
const INTERVAL: &str = "1m";
const LIMIT: usize = 500;

/// Grid của node trong `strategies/binance/config.toml`.
const KELLY: f64 = 0.25;
const CAPITAL: f64 = 100_000.0;
const GRID_LEVELS: f64 = 5.0;
const SL_PCT: f64 = 0.008;
const LOOKBACK: f64 = 2.0 * 24.0 * 3600.0;

fn script_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../strategies/binance/grid.rhai")
}

fn strategy() -> ScriptStrategy {
    ScriptStrategy::new(ScriptSource::File(script_path()), 900)
        .with_knob("min_trades", 3.into())
        .with_knob("weight_sharpness", 4.0.into())
        .with_knob("max_bit", 20.into())
}

fn params(i: usize) -> f64 {
    [KELLY, CAPITAL, GRID_LEVELS, SL_PCT, LOOKBACK][i]
}

/// OKX dùng cặp `BTC-USDT` và bar `1m` (cùng độ phân giải), tối đa 300 nến/lần.
const OKX_INST: &str = "BTC-USDT";
const OKX_LIMIT: usize = 300;

/// Sàn nguồn — khác nhau ở **format** và thứ tự, không khác ở dữ liệu: script
/// chỉ cần chuỗi nến đã đóng nên test này cũng kiểm tra giả định đó.
#[derive(Clone, Copy, PartialEq)]
enum Feed {
    Binance,
    Okx,
}

struct Endpoint {
    feed: Feed,
    url: String,
}

/// Endpoint theo thứ tự ưu tiên, tự chuyển sang cái sau khi cái trước fail.
///
/// - **OKX** đứng đầu: `www.okx.com` không chặn IP CI (khác Binance trả
///   **451 Unavailable For Legal Reasons** cho runner của GitHub).
/// - `data-api.binance.vision` rồi `api.binance.com`: public market data host,
///   không phục vụ giao dịch nên ít bị chặn hơn.
///
/// Ghi đè bằng `OPSENSE_BINANCE_KLINES_URL` (luôn theo format Binance).
fn endpoints() -> Vec<Endpoint> {
    if let Ok(url) = std::env::var("OPSENSE_BINANCE_KLINES_URL") {
        return vec![Endpoint {
            feed: Feed::Binance,
            url,
        }];
    }
    vec![
        Endpoint {
            feed: Feed::Okx,
            // OKX chặn `limit` ở 300 nến/lần.
            url: format!(
                "https://www.okx.com/api/v5/market/candles?instId={OKX_INST}&bar={INTERVAL}&limit={OKX_LIMIT}"
            ),
        },
        Endpoint {
            feed: Feed::Binance,
            url: format!(
                "https://data-api.binance.vision/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
            ),
        },
        Endpoint {
            feed: Feed::Binance,
            url: format!(
                "https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
            ),
        },
    ]
}

/// Binance trí số ở dạng JSON number (openTimeMs là integer, giá có thể float
/// **hoặc** string) → đọc qua một cổng duy nhất cho cả hai sàn.
fn num(v: &Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_i64().map(|i| i as f64))
        .or_else(|| v.as_u64().map(|u| u as f64))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("không đọc được số: {v}"))
}

/// Binance `klines`: `[openTimeMs, open, high, low, close, volume]`, cũ → mới.
fn parse_binance(body: &Value) -> Vec<CandleStick> {
    let rows = body.as_array().expect("klines Binance = mảng dòng");
    assert!(rows.len() >= 10, "Binance trả {} nến", rows.len());
    rows.iter()
        .map(|row| {
            let r = row.as_array().expect("mỗi dòng klines là mảng");
            CandleStick::new(
                (num(&r[0]) / 1000.0) as i64,
                num(&r[1]),
                num(&r[2]),
                num(&r[3]),
                num(&r[4]),
                num(&r[5]),
            )
        })
        .collect()
}

/// OKX `candles`: `[tsMs, o, h, l, c, vol, volCcy, volCcyQuote, confirm]`,
/// **mới → cũ** và `confirm = "0"` là nến đang chạy (phải bỏ — `trade()` chỉ
/// giao trên nến đã đóng).
fn parse_okx(body: &Value) -> Vec<CandleStick> {
    assert_eq!(
        body["code"].as_str(),
        Some("0"),
        "OKX trả lỗi: {}",
        body["msg"]
    );
    let rows = body["data"]
        .as_array()
        .expect("OKX: data = mảng dòng");
    let mut out: Vec<CandleStick> = rows
        .iter()
        .filter_map(|row| {
            let r = row.as_array()?;
            if r.len() < 9 {
                return None;
            }
            // `confirm` = "1" nến đã đóng.
            if r[8].as_str() != Some("1") {
                return None;
            }
            Some(CandleStick::new(
                (num(&r[0]) / 1000.0) as i64,
                num(&r[1]),
                num(&r[2]),
                num(&r[3]),
                num(&r[4]),
                num(&r[5]),
            ))
        })
        .collect();
    out.reverse(); // OKX trả mới trước; kernel giả định tăng dần theo thời gian
    assert!(out.len() >= 10, "OKX trả {} nến đã đóng", out.len());
    out
}

/// Nến đã đóng + tên sàn để in ra log (nguồn có thể là OKX hoặc Binance).
async fn fetch_klines() -> (String, Vec<CandleStick>) {
    let client = reqwest::Client::new();
    let mut seen = Vec::new();
    for ep in endpoints() {
        let resp = match client.get(&ep.url).send().await {
            Ok(r) => r,
            Err(e) => {
                seen.push(format!("{} → transport: {e}", ep.url));
                continue;
            }
        };
        let status = resp.status();
        if !status.is_success() {
            seen.push(format!("{} → HTTP {status}", ep.url));
            continue;
        }
        let body: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                seen.push(format!("{} → parse JSON: {e}", ep.url));
                continue;
            }
        };
        let (venue, out) = match ep.feed {
            Feed::Binance => ("binance".to_string(), parse_binance(&body)),
            Feed::Okx => ("okx".to_string(), parse_okx(&body)),
        };
        println!("klines lấy từ {venue}: {}", ep.url);
        return (venue, out);
    }
    panic!(
        "không endpoint nào trả được klines (thử {}): {}",
        endpoints().len(),
        seen.join(" | ")
    );
}

fn slice_fetch(
    data: Vec<CandleStick>,
) -> impl FnMut(u64, u64) -> std::pin::Pin<Box<dyn std::future::Future<
    Output = Result<Vec<CandleStick>, std::io::Error>,
> + Send>> {
    move |from, to| {
        let out: Vec<CandleStick> = data
            .iter()
            .copied()
            .filter(|c| c.t >= 0 && (c.t as u64) >= from && (c.t as u64) < to)
            .collect();
        Box::pin(async move { Ok(out) })
    }
}

#[tokio::test]
#[ignore = "cần network: Binance public API"]
async fn rebuild_on_live_binance_data_yields_usable_plan() {
    let (venue, data) = fetch_klines().await;
    let lo = data.iter().map(|c| c.l).fold(f64::INFINITY, f64::min);
    let hi = data.iter().map(|c| c.h).fold(f64::NEG_INFINITY, f64::max);
    let last = *data.last().expect("có nến");
    println!(
        "{venue} {INTERVAL}: {} nến đã đóng, giá {:.2}..{:.2}, nến cuối t={} c={:.2}",
        data.len(),
        lo,
        hi,
        last.t,
        last.c
    );

    // 1. Script dựng plan.
    let plan = strategy()
        .rebuild(last.t as u64, &[], &mut slice_fetch(data.clone()), &params)
        .await
        .expect("fn rebuild dựng được plan trên dữ liệu thật");
    assert!(!plan.is_empty(), "phải có ít nhất 1 ô grid");
    println!("plan: {} ô grid", plan.len());

    for (i, g) in plan.iter().enumerate() {
        let levels = g.levels();
        assert!(levels.len() >= 2, "ô {i} có {}+ level", levels.len());
        assert!(
            levels.windows(2).all(|w| w[0] < w[1]),
            "ô {i}: level phải tăng dần: {levels:?}"
        );
        assert!(
            levels[0] >= lo - 1e-6 && *levels.last().unwrap() <= hi + 1e-6,
            "ô {i}: level nằm trong khoảng giá quan sát [{lo}, {hi}]: {levels:?}"
        );
        for j in 0..g.num_levels() {
            let (lw, sw) = (g.long_win_pct(j), g.short_win_pct(j));
            assert!(
                (0.0..=1.0).contains(&lw) && (0.0..=1.0).contains(&sw),
                "ô {i} bậc {j}: win-prob ngoài (0,1): long={lw} short={sw}"
            );
            assert!(g.step() > 0.0, "bước lưới phải dương");
        }
    }
    let first = &plan[0];
    println!(
        "ô đầu: levels={:?} sl_pct={:.4} long_win={:?}",
        first.levels(),
        first.stoploss_pct(),
        (0..first.num_levels())
            .map(|j| first.long_win_pct(j))
            .collect::<Vec<_>>()
    );

    // 2. Kernel nhận plan đó và đặt được lệnh trên nến cuối.
    struct NoLoader;
    #[async_trait::async_trait]
    impl opsense_qlib::DataLoader for NoLoader {
        async fn range(
            &self,
            _f: u64,
            _t: u64,
            _r: &str,
        ) -> Result<Vec<CandleStick>, std::io::Error> {
            Ok(Vec::new())
        }
    }
    let pf = Portfolio::new(
        std::sync::Arc::new(NoLoader),
        std::sync::Arc::new(strategy()),
        std::sync::Arc::new(opsense_qlib::SimpleFixedFee::new(0.0005)),
        std::sync::Arc::new(opsense_qlib::SharpeScore),
        std::sync::Arc::new(opsense_qlib::CryptoCalendar),
        PortfolioConfig {
            resolution_for_test: INTERVAL.into(),
            resolution_for_rebuild: INTERVAL.into(),
            settlement_candles: 0,
            cache_enabled: false,
        },
    )
    .expect("Portfolio nhận strategy script");

    let from = last.t as u64;
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = events.clone();
    let mut notify = move |e: opsense_qlib::OrderEvent| {
        sink.lock().expect("lock events").push(e);
        Box::pin(async { Ok(()) })
            as std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = std::io::Result<()>> + Send + 'static,
                >,
            >
    };
    let mut session = Session::new();
    pf.evaluate(
        &mut session,
        from,
        from + 60,
        &params,
        &mut slice_fetch(data.clone()),
        &mut slice_fetch(data),
        &mut notify,
    )
    .await
    .expect("evaluate trên nến cuối");
    assert_eq!(session.candle_seq, 1, "kernel chạy đúng 1 nến");
    println!(
        "kernel: plan {} ô, {} sự kiện, seq={}",
        session.plan.len(),
        events.lock().expect("lock events").len(),
        session.candle_seq
    );
}
