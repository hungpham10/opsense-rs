//! Integration test — pipeline giao dịch streaming qua Runtime thật.
//!
//! ```text
//! Input("candles") → QlibEngine("engine") → Output("sink")
//!                                      └─→ CaptureSink("capture")
//! ```
//!
//! Candle đi vào node `candles` qua `Runtime::inject`; test không gọi trực tiếp
//! `QlibEngine::run()` hoặc `StreamingPortfolio::on_candle()`. Event được xác
//! nhận qua broadcast của `Output` và snapshot của `CaptureSink`, sau đó dùng
//! chính payload nhận được để kiểm tra persistence contract của
//! `TradeEventStation`.

use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "sqlite")]
use opsense_components::station::TradeEventStation;
use opsense_components::vector::runtime::{Event, Runtime};
use opsense_components::{QlibEngine, TelegramSink};
use opsense_core::Config;
use opsense_core::Context;
use opsense_libs::vector::components::CaptureSink;
use opsense_libs::vector::components::input::Input;
use opsense_libs::vector::components::output::Output;
use opsense_libs::vector::runtime::{Component, Message};
use opsense_model::secret::Secret;
use opsense_qlib::{
    CandleStick, CryptoCalendar, GridStrategy, OrderEvent, SimpleFixedFee, StreamingPortfolio,
};
use serde_json::{Value, json};

fn canned_candles() -> Vec<CandleStick> {
    (0..300)
        .map(|step| {
            let price = 100.0 + (step % 60) as f64 - 30.0;
            CandleStick::new(
                1_700_000_000 + step as i64 * 300,
                price,
                price + 1.0,
                price - 1.0,
                price + 0.2,
                10.0,
            )
        })
        .collect()
}

fn candle_payload(candle: &CandleStick) -> Value {
    json!({
        "t": candle.t,
        "o": candle.o,
        "h": candle.h,
        "l": candle.l,
        "c": candle.c,
        "v": candle.v,
    })
}

fn assert_trade_event(payload: &Value) -> OrderEvent {
    assert!(
        payload.get("event_id").is_some(),
        "event phải mang event_id"
    );
    assert!(
        payload.get("ts").and_then(Value::as_i64).is_some(),
        "event phải mang ts signed integer"
    );
    assert!(
        payload
            .get("broker")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "event phải mang broker"
    );
    assert!(
        payload
            .get("symbol")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "event phải mang symbol"
    );
    let event = payload.get("event").expect("event phải mang event");
    serde_json::from_value(event.clone()).expect("event JSON phải deserialize lại được")
}

#[tokio::test]
async fn canned_candles_drive_paper_trading_pipeline() {
    let candles = canned_candles();
    let ctx: Arc<Context> = {
        let cfg: Config = serde_json::from_str("{}").expect("default config");
        Arc::new(Context::new(&cfg, Arc::new(Secret::new().await.unwrap())))
    };
    let strategy = Arc::new(GridStrategy::new(5, 0.008, 10.0, 2 * 24 * 3600, 900, 300));
    let portfolio = StreamingPortfolio::new(
        strategy,
        Arc::new(SimpleFixedFee::new(0.0005)),
        Arc::new(CryptoCalendar),
        0.25,
        1_000.0,
        0,
    );
    let engine = QlibEngine::new("engine".into(), vec!["candles".into()], portfolio);

    let ingest = Input {
        id: "candles".into(),
    };
    let drain = Output {
        id: "sink".into(),
        inputs: vec!["engine".into()],
    };
    let capture = Arc::new(CaptureSink::new("capture", vec!["engine".into()], 1024));

    let mut runtime = Runtime::new();
    runtime.set_context(ctx);
    let components: Vec<Arc<dyn Component>> = vec![
        Arc::new(ingest),
        Arc::new(engine),
        Arc::new(drain),
        capture.clone(),
    ];
    runtime.reload(components).expect("valid graph");
    let _handle = runtime
        .start(|event: Event| async move {
            if let Event::Major((_, error)) = event {
                panic!("runtime error: {error}");
            }
        })
        .expect("start");

    // Subscribe before injecting so the first event cannot be missed.
    let mut receiver = runtime.broadcast("sink".into()).expect("output broadcast");
    for candle in &candles {
        runtime
            .inject(
                "candles".into(),
                Message {
                    payload: candle_payload(candle),
                },
            )
            .await
            .expect("inject candle");
    }

    // Wait until the same payload is visible through both runtime surfaces.
    let mut broadcast_payloads = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let captured = capture.snapshot();
        if broadcast_payloads
            .iter()
            .any(|payload| captured.iter().any(|candidate| *candidate == *payload))
        {
            break;
        }

        match tokio::time::timeout_at(deadline, receiver.recv()).await {
            Ok(Ok(msg)) => {
                assert_trade_event(&msg.payload);
                broadcast_payloads.push(msg.payload);
            }
            Ok(Err(_)) | Err(_) => {
                panic!("timed out waiting for matching Output and CaptureSink payloads");
            }
        }
    }

    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");

    let captured = capture.snapshot();
    assert!(
        !broadcast_payloads.is_empty(),
        "Output phải phát ít nhất một event"
    );
    assert!(
        !captured.is_empty(),
        "CaptureSink phải nhận event từ QlibEngine qua runtime"
    );
    let matching_payload = broadcast_payloads
        .iter()
        .find(|payload| captured.iter().any(|candidate| candidate == *payload))
        .expect("broadcast payload phải xuất hiện trong CaptureSink");
    assert_trade_event(matching_payload);

    #[cfg(feature = "sqlite")]
    {
        let data_dir = std::env::temp_dir().join(format!(
            "opsense-qlib-runtime-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        let cfg: Config = serde_json::from_value(json!({
            "storage": {
                "backend": "sqlite",
                "data_dir": data_dir.to_string_lossy().into_owned()
            }
        }))
        .expect("sqlite config");
        let persistence_ctx = Context::new(&cfg, Arc::new(Secret::new().await.unwrap()));
        let station = TradeEventStation::from_context("runtime-trades", &persistence_ctx)
            .await
            .expect("open trade event station");

        for payload in &captured {
            assert_trade_event(payload);
            station.persist(payload).await.expect("persist trade event");
            let broker = payload
                .get("broker")
                .and_then(Value::as_str)
                .expect("persisted event must have broker");
            let event_id = payload
                .get("event_id")
                .expect("persisted event must have event_id");
            let persisted = station
                .read_event(broker, event_id)
                .await
                .expect("read persisted event");
            let persisted = persisted
                .as_ref()
                .expect("persisted event must be readable");
            assert_eq!(
                persisted.get("event_id"),
                payload.get("event_id"),
                "persisted event_id must match for event_id={event_id}"
            );
            assert_eq!(
                persisted.get("ts"),
                payload.get("ts"),
                "persisted ts must match"
            );
            assert_eq!(
                persisted.get("broker"),
                payload.get("broker"),
                "persisted broker must match"
            );
            assert_eq!(
                persisted.get("symbol"),
                payload.get("symbol"),
                "persisted symbol must match"
            );
            assert_trade_event(persisted);
            assert_trade_event(payload);
            station
                .persist(persisted)
                .await
                .expect("retrying the persisted identity must be idempotent");
        }

        let _ = std::fs::remove_dir_all(data_dir);
    }

    eprintln!(
        "integration: {} broadcast payloads, {} captured payloads, {} canned candles",
        broadcast_payloads.len(),
        captured.len(),
        candles.len()
    );

    // TelegramSink.body accepts the same payload shape without calling Telegram.
    let sink = TelegramSink {
        id: "notify".into(),
        inputs: vec!["sink".into()],
        token_env: "TELEGRAM_BOT_TOKEN".into(),
        chat_id: "0".into(),
    };
    if let Some(msg) = captured.first() {
        let body = sink.body(msg).expect("event payload phải vừa Telegram");
        assert!(body["text"].as_str().is_some());
    }
}
