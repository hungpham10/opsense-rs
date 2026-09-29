//! REPLAY nhiều nến qua **đúng** `grid.rhai`, kiểm bộ đếm thắng/thua có tích luỹ.
//!
//! # Vì sao cần test này
//!
//! Trên stack đo được: 3 lệnh đóng mà `*_cnt` trong plan observation vẫn bằng
//! **0** ở mọi lần rebuild. Đã loại trừ bằng test ở tầng dưới:
//!
//! | tầng | test | kết quả |
//! |---|---|---|
//! | `GridPlan::to_grids` (3 vòng rebuild) | `outcome_counts_accumulate_across_rebuilds` | xanh |
//! | vòng ghi→đọc→ghi observation | `plan_counts_survive_observation_round_trip` | xanh |
//!
//! Nên tầng lưu và tầng Rhai đều giữ được bộ đếm. Còn lại khả năng là
//! `record_trade_outcome` chạy trên `session.plan` của kernel (`portfolio.rs`)
//! trong khi plan observation được ghi từ `session.plan` của tầng Rhai
//! (`orders.rs`) — **hai `Session` riêng**. Nếu vậy số đếm của kernel không bao
//! giờ tới chỗ quan sát.
//!
//! Test này chạy **pipeline thật** (cùng chuỗi `Input → tick_2_candle → grid.rhai`
//! như `trading_pipeline.rs`) với **nhiều nến** để buộc phải có nhiều lệnh đóng,
//! rồi đọc plan observation. Đây là mắt xích cuối mà unit test không với tới.
//!
//! # Cách chạy
//!
//! ```bash
//! cargo test -p opsense-rhai --test replay_outcome_counts -- --nocapture
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use opsense_components::converters::Tick2Candle;
use opsense_components::vector::runtime::{Component, Message, Runtime};
use opsense_core::{Config, Context, Observation};
use opsense_mlib::vector::components::clock::Clock;
use opsense_mlib::vector::components::input::Input;
use opsense_mlib::vector::components::output::Output;
use opsense_model::events::Signal;
use opsense_model::secret::Secret;
use opsense_rhai::RhaiTransform;
use serde_json::{Value, json};
use tokio::sync::RwLock;

const SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../strategies/binance/grid.rhai"
);
const SYMBOL: &str = "BTCUSDT";
const GRID: &str = "grid";
const CANDLE: &str = "tick-candle";

/// Số nến đẩy. Nhiều để chắc chắn có lệnh **đóng** (cần giá đi qua TP và SL),
/// không chỉ lệnh mở.
const N_CANDLES: i64 = 300;

fn params() -> BTreeMap<String, Value> {
    let mut m = BTreeMap::new();
    m.insert("own_station".into(), Value::from(GRID));
    m.insert("symbol".into(), Value::from(SYMBOL));
    m.insert("resolution".into(), Value::from("1m"));
    // Lịch sử cũng lấy từ chính station nến — không có node `history` riêng
    // trong harness này.
    m.insert("history_source".into(), Value::from(CANDLE));
    m.insert("live_source".into(), Value::from(CANDLE));
    m.insert("history_secs".into(), Value::from(3600));
    m.insert("live_window_secs".into(), Value::from(300));
    m.insert("mode".into(), Value::from("trading"));
    m.insert("calendar".into(), Value::from("crypto"));
    m.insert("strategy".into(), Value::from("grid"));
    m.insert("grid_levels".into(), Value::from(5));
    m.insert("sl_pct".into(), Value::from(0.008));
    m.insert("grid_min_trades".into(), Value::from(3));
    m.insert("lookback_secs".into(), Value::from(172_800));
    m.insert("review_interval_secs".into(), Value::from(900));
    m.insert("fee_rate".into(), Value::from(0.0005));
    m.insert("kelly_fraction".into(), Value::from(0.25));
    m.insert("base_capital".into(), Value::from(100_000.0));
    m.insert("settlement_candles".into(), Value::from(0));
    m
}

/// Payload dạng **observation**, không phải format thô của Binance.
///
/// Pipeline thật là `tick-feed (websocket_2_json) → tick-map → tick-candle`, và
/// `tick-map` mới là chỗ dịch `E`/`s`/`p` thành observation. Harness này bỏ hẳn
/// `tick-map` nên phải feed thẳng shape **sau** nó — dùng format thô thì không
/// node nào hiểu và mọi nến rơi lọt (đã dính: 0 nến trong station).
fn tick_payload(ts: i64, price: f64) -> Value {
    json!({
        "ts": ts * 1000,
        "metric_id": SYMBOL,
        "kind": "metric",
        "signal": "raw",
        "value": price,
        "trigger": "tick",
        "labels": { "resolution": "1m" }
    })
}

#[tokio::test]
async fn outcome_counts_accumulate_through_runtime() {
    let cfg: Config = serde_json::from_str("{}").expect("default config");
    let secret = Secret::new().await.expect("secret");
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    let mut transform = RhaiTransform::new_file(GRID, &["candles", "clock", CANDLE], SCRIPT);
    transform.params = params();

    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(Input { id: "candles".into() }),
        Arc::new(Clock::new(Duration::from_millis(200))),
        Arc::new(Tick2Candle {
            id: CANDLE.into(),
            inputs: vec!["candles".into()],
            resolution: "1m".into(),
            symbol: SYMBOL.into(),
            unit_ms: 1,
            stale_secs: 0,
            station: true,
        }),
        Arc::new(transform),
        Arc::new(Output { id: "sink".into(), inputs: vec![GRID.into()] }),
    ];

    let mut rt = Runtime::new();
    rt.set_context(ctx.clone());
    rt.reload(components).expect("valid graph");
    let _receiver = rt.broadcast("sink".into()).expect("output broadcast");
    let _handle = rt
        .start(|event| async move {
            if let opsense_components::vector::runtime::Event::Major((_, error)) = event {
                panic!("runtime error: {error}");
            }
        })
        .expect("runtime starts");

    // Giá nhấn sóng tam giác — đi qua nhiều mốc lưới theo cả hai chiều nên có
    // lệnh mở lẫn lệnh đóng. Biên độ phải **vượt** `sl_pct` (0,8%) để chạm SL.
    let open_bucket = opsense_components::signal::now_secs() / 60 * 60 - N_CANDLES * 60;
    for i in 0..N_CANDLES {
        let ts = open_bucket + i * 60;
        // 100 → 130 → 100 → 130: biên độ 30 trên giá ~100 là 30%, ≫ 0,8%.
        let price = 100.0 + if i % 40 < 20 { i % 40 } else { 40 - (i % 40) } as f64;
        rt.inject(
            "candles".into(),
            Message { payload: tick_payload(ts, price) },
        )
        .await
        .expect("inject tick");
        // Đẩy nhịp để Clock kích nhánh trading theo từng nến.
        tokio::time::sleep(Duration::from_millis(8)).await;
    }
    // Tick ở bucket kế tiếp để đóng nến cuối.
    rt.inject(
        "candles".into(),
        Message { payload: tick_payload(open_bucket + N_CANDLES * 60, 100.0) },
    )
    .await
    .expect("inject tick cuối");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Đọc cửa sổ rộng: nến đẩy ngược từ `open_bucket`.
    let lo = open_bucket - 120;
    let hi = open_bucket + N_CANDLES * 60 + 120;

    let obs: Vec<Observation> = ctx
        .station::<Arc<RwLock<opsense_core::TimeseriesStation>>>(GRID)
        .await
        .expect("station grid")
        .write()
        .await
        .query_recent(lo, hi)
        .await
        .unwrap_or_default();

    let orders: Vec<&Observation> = obs.iter().filter(|o| o.signal == Signal::Order).collect();
    let open = orders.iter().filter(|o| o.labels.get("status").map(String::as_str) == Some("open")).count();
    let closed = orders
        .iter()
        .filter(|o| o.labels.get("status").map(String::as_str) == Some("closed"))
        .count();
    let plans = obs.iter().filter(|o| o.labels.get("kind").map(String::as_str) == Some("plan")).count();

    println!("REPLAY: {N_CANDLES} nến → {open} lệnh mở, {closed} lệnh đóng, {plans} plan obs");

    // 1) Phải có lệnh đóng — nếu không thì test dưới vô nghĩa.
    assert!(closed > 0, "cần ít nhất 1 lệnh đóng để bộ đếm có việc; mở={open} đóng={closed}");

    // 2) Bộ đếm trong plan observation phải khác 0.
    //
    // Đây là điểm cần xác minh. Hai tầng dưới đã chứng minh giữ được bộ đếm, và
    // tầng lưu vẫn tăng bộ đếm khi lệnh đóng — nên nếu `*_cnt` ở đây **vẫn 0**,
    // thì đích đến của bộ đếm đang bị đứt ở giữa: kernel đếm vào một
    // `Session`, còn observation lấy từ `Session` khác.
    let mut max_cnt = 0u64;
    for p in obs.iter().filter(|o| o.labels.get("kind").map(String::as_str) == Some("plan")) {
        let Some(cells) = p.labels.get("cells").and_then(|c| c.parse::<Value>().ok()) else {
            continue;
        };
        for cell in cells.as_array().into_iter().flatten() {
            for key in ["long_win_cnt", "long_lost_cnt", "short_win_cnt", "short_lost_cnt"] {
                for v in cell.get(key).and_then(|a| a.as_array()).into_iter().flatten() {
                    max_cnt = max_cnt.max(v.as_u64().unwrap_or(0));
                }
            }
        }
    }
    println!("REPLAY: max(*_cnt) trong plan observation = {max_cnt}");

    assert!(
        max_cnt > 0,
        "có {closed} lệnh đóng nhưng `*_cnt` trong plan observation vẫn 0. \
         ⇒ bộ đếm của kernel không tới được chỗ quan sát."
    );
}
