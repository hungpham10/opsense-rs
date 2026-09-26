//! Tests for the new station read API via Context (replaces old registry-based
//! `ts_query`/`ts_mean` bindings which were removed).

use opsense_core::{Context, Observation, Station, TimeseriesStation};
use opsense_model::events::{Signal, TelemetryKind};
use opsense_model::secret::Secret;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

fn obs(ts: i64, value: f64) -> Observation {
    Observation::new(
        ts,
        "cpu".into(),
        TelemetryKind::Metric,
        Signal::Utilization,
        value,
    )
}

#[tokio::test]
async fn context_station_registry_and_query() {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    // Create and register a TimeseriesStation
    let station = TimeseriesStation::from_storage("test-station", ctx.storage())
        .await
        .unwrap();
    let station_arc = Arc::new(RwLock::new(station));
    ctx.registry("test-station", Station::Timeseries(station_arc.clone()))
        .await
        .unwrap();

    // Write some data
    let batch = vec![obs(100, 30.0), obs(200, 40.0), obs(300, 50.0)];
    station_arc.write().await.update_range(&batch, 100, 300, 300);

    // Query via Context
    let retrieved = ctx.station::<Arc<RwLock<TimeseriesStation>>>("test-station").await.unwrap();
    // Query within the written data range (100-300)
    let data = retrieved.write().await.query_range(100, 300).await.unwrap();

    assert_eq!(data.len(), 3);
    assert_eq!(data[0].value, 30.0);
    assert_eq!(data[1].value, 40.0);
    assert_eq!(data[2].value, 50.0);
}

#[tokio::test]
async fn context_station_missing_returns_error() {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    // Query non-existent station
    let result = ctx.station::<Arc<RwLock<TimeseriesStation>>>("nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn context_attributes_roundtrip() {
    let mut attrs = HashMap::new();
    attrs.insert("foo".into(), "bar".into());

    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let mut cfg = cfg;
    cfg.attributes = attrs.clone();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    let retrieved = ctx.get_attributes().await;
    assert_eq!(retrieved.get("foo").unwrap(), "bar");
}

#[tokio::test]
async fn context_capacity_lookup() {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let mut cfg = cfg;
    cfg.capacity.insert("disk_usage".into(), 100.0);
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    assert_eq!(ctx.capacity("disk_usage"), Some(100.0));
    assert_eq!(ctx.capacity("unknown"), None);
}

/// `station_query` phải **lọc được server-side**, vì giới hạn `max_map_size` của
/// Rhai là engine-wide: một observation là map lồng `labels` map, nên kéo cả
/// station đầy tick sẽ vượt trần và làm **cả batch script chết**.
///
/// Đây là lỗi thật đã xảy ra: `strategies/binance/grid.rhai` gọi
/// `station_query(own, now - 3600, now)` chỉ để tìm cursor `trading_step` và
/// lệnh, kéo luôn 26_680 observation tick ⇒ `Size of object map too large`,
/// node `grid` im bặt, không có lệnh nào.
#[tokio::test]
async fn station_query_filters_signal_and_label_kind() {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    let station = TimeseriesStation::from_storage("mixed", ctx.storage())
        .await
        .unwrap();
    let station_arc = Arc::new(RwLock::new(station));
    ctx.registry("mixed", Station::Timeseries(station_arc.clone()))
        .await
        .unwrap();

    // 40.000 obs tick `raw` trước — đủ để lần đọc không lọc vượt trần
    // `max_map_size` (ngưỡng nằm giữa 18k và 27k; production vỡ ở 26.680).
    let ticks: Vec<Observation> = (0..40_000).map(|i| obs(1_700_000_000 + i, i as f64)).collect();
    station_arc
        .write()
        .await
        .update_range(&ticks, 1_700_000_000, 1_700_099_999, 1_700_099_999);

    // Rồi mới ghi lệnh + cursor, batch riêng: `update_range` có trần số item
    // nên obs nằm cuối một batch 40k sẽ bị rơi, và test sẽ khẳng định nhầm
    // là bộ lọc hỏng.
    let mut order = obs(1_700_000_500, 42.0);
    order.signal = Signal::Order;
    order.labels.insert("status".into(), "open".into());
    order.labels.insert("order_id".into(), "o1".into());
    let mut cursor = obs(1_700_000_600, 0.0);
    cursor.signal = Signal::Summary;
    cursor.labels.insert("kind".into(), "trading_step".into());
    station_arc
        .write()
        .await
        .update_range(&[order, cursor], 1_700_000_500, 1_700_000_600, 1_700_099_999);

    // Script trả thẳng kết quả truy vấn (mảng observation — hợp lệ với
    // `fn process`), nên lỗi nếu có **chỉ** có thể đến từ truy vấn, không từ
    // cú pháp script.
    async fn run(ctx: &Arc<Context>, body: &str) -> Result<Vec<serde_json::Value>, String> {
        let src = format!("fn process(points) {{ {body} }}");
        opsense_rhai::call_process_with(
            opsense_rhai::ScriptSource::Inline(src.into()),
            serde_json::Value::Array(vec![]),
            Default::default(),
            Default::default(),
            None,
            Some(ctx.clone()),
        std::sync::Arc::new(Vec::new()),
    )
        .await
    }
    let ctx = &ctx;

    // Không lọc: trả về **mọi** observation của cửa sổ. Không assert "phải vỡ" —
    // trần `max_map_size` là chi tiết bên trong Rhai (đo được ngưỡng nằm giữa 18k
    // và 27k, production vỡ ở 26.680) và sẽ đổi theo phiên bản. Cái cần giữ là
    // **lọc giảm tải mạnh**, vì đó là điều giữ cho script sống được.
    let all = run(
        ctx,
        "station_query(\"mixed\", 1700000000, 1700099999)",
    )
    .await
    .expect("đọc không lọc ở station lớn")
    .len();
    assert!(all > 10_000, "station phải lớn để test có ý nghĩa: {all}");

    // Lọc `signal = "order"` → đúng 1 lệnh, script sống.
    let orders = run(
        ctx,
        "station_query(\"mixed\", 1700000000, 1700099999, \"order\")",
    )
    .await
    .expect("lọc signal=order phải chạy được trên station lớn");
    assert_eq!(orders.len(), 1, "phải đúng 1 lệnh, không phải cả station");
    assert_eq!(orders[0]["labels"]["order_id"], "o1");

    // Lọc `signal` + `labels.kind` → đúng 1 cursor.
    let steps = run(
        ctx,
        "station_query(\"mixed\", 1700000000, 1700099999, \"summary\", \"trading_step\")",
    )
    .await
    .expect("lọc signal + label_kind phải chạy được");
    assert_eq!(steps.len(), 1, "phải đúng 1 cursor");
    assert_eq!(steps[0]["labels"]["kind"], "trading_step");

    // Lọc không trùng gì → mảng rỗng, không vỡ.
    let none = run(
        ctx,
        "station_query(\"mixed\", 1700000000, 1700099999, \"errors\", \"nope\")",
    )
    .await
    .expect("lọc không trùng gì vẫn phải chạy");
    assert!(none.is_empty(), "{} dòng", none.len());

    // `()` = không lọc (giữ nghĩa của lời gọi cũ 3 tham số) → cùng số dòng với
    // lời gọi 3 tham số.
    let unit = run(
        ctx,
        "station_query(\"mixed\", 1700000000, 1700099999, ())",
    )
    .await
    .expect("`()` = không lọc, phải đọc được");
    assert_eq!(unit.len(), all, "`()` phải nghĩa là không lọc");
}

/// `station_write` là API ghi **tường minh**: script tự chọn nơi ghi, và chỉ
/// được ghi vào station đã khai ở `params.write_stations` (+ own station).
///
/// Ghi vào station không khai phải **báo lỗi** chứ không im lặng bỏ qua — ghi
/// nhầm chỗ là lỗi cấu hình, im lặng sẽ biến nó thành "dữ liệu mất không rõ ở
/// đâu".
#[tokio::test]
async fn station_write_respects_allowlist() {
    let cfg: opsense_core::Config = serde_json::from_str("{}").unwrap();
    let secret = Secret::new().await.unwrap();
    let ctx = Arc::new(Context::new(&cfg, Arc::new(secret)));

    async fn mk(
        ctx: &Arc<Context>,
        id: &'static str,
    ) -> Arc<RwLock<TimeseriesStation>> {
        let st = TimeseriesStation::from_storage(id, ctx.storage()).await.unwrap();
        let arc = Arc::new(RwLock::new(st));
        ctx.registry(id, Station::Timeseries(arc.clone()))
            .await
            .unwrap();
        arc
    }
    let grid = mk(&ctx, "grid").await;
    let history = mk(&ctx, "history").await;

    async fn run(
        ctx: &Arc<Context>,
        allowed: Vec<String>,
        body: &str,
    ) -> Result<Vec<serde_json::Value>, String> {
        let src = format!("fn process(points) {{ {body} [] }}");
        opsense_rhai::call_process_with(
            opsense_rhai::ScriptSource::Inline(src.into()),
            serde_json::Value::Array(vec![]),
            Default::default(),
            Default::default(),
            None,
            Some(ctx.clone()),
            std::sync::Arc::new(allowed),
        )
        .await
    }

    let write = |station: &str, ts: i64| {
        format!(
            r#"station_write("{station}", #{{
                ts: {ts}, metric_id: "BTCUSDT", kind: "metric", signal: "order",
                value: 1.0, labels: #{{ order_id: "o{ts}" }}
            }});"#
        )
    };

    // Không khai gì cả → ghi vào `history` bị từ chối.
    let denied = run(&ctx, vec![], &write("history", 1_700_000_000)).await;
    assert!(
        denied.is_err(),
        "ghi vào station không khai phải lỗi: {denied:?}"
    );

    // Khai `history` → ghi được, và đọc lại thấy đúng 1 dòng.
    let ok = run(
        &ctx,
        vec!["history".into()],
        &write("history", 1_700_000_000),
    )
    .await
    .expect("ghi vào station đã khai phải chạy được");
    assert!(ok.is_empty());
    let got = history
        .read()
        .await
        .query_recent(1_700_000_000, 1_700_000_000)
        .await
        .expect("station phải đọc được");
    assert_eq!(got.len(), 1, "phải ghi đúng 1 observation: {got:?}");
    assert_eq!(got[0].labels.get("order_id").map(String::as_str), Some("o1700000000"));

    // Own station: **transform** gộp id của chính nó vào allowlist, nên ở đây
    // mô phỏng allowlist mà transform dựng (`params.write_stations` + own id).
    // Không có id own trong allowlist thì bị từ chối — đúng như trường hợp trên.
    run(&ctx, vec!["grid".into()], &write("grid", 1_700_000_100))
        .await
        .expect("own station phải được phép ghi khi transform đã khai nó");
    let got = grid
        .read()
        .await
        .query_recent(1_700_000_100, 1_700_000_100)
        .await
        .expect("own station phải đọc được");
    assert_eq!(got.len(), 1, "own station phải nhận 1 observation: {got:?}");
}
