use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use opsense_components::vector::runtime::{Event, Runtime};
use opsense_components::{QlibEngine, TelegramSink};
use opsense_mlib::vector::components::CaptureSink;
use opsense_mlib::vector::components::input::Input;
use opsense_mlib::vector::components::output::Output;
use opsense_mlib::vector::runtime::Message;
use opsense_qlib::{CandleStick, OrderEvent};
use serde_json::Value;

fn candle_payload(candle: &CandleStick) -> Value {
    serde_json::json!({
        "t": candle.t,
        "o": candle.o,
        "h": candle.h,
        "l": candle.l,
        "c": candle.c,
        "v": candle.v,
    })
}

fn event_kind(payload: &Value) -> Option<&str> {
    payload
        .get("event")?
        .as_object()?
        .keys()
        .next()
        .map(String::as_str)
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
async fn candle_stream_drives_paper_trading_events() {
    let candles = (0..300)
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
        .collect::<Vec<_>>();

    let strategy = Arc::new(opsense_qlib::GridStrategy::new(
        5,
        0.008,
        10.0,
        2 * 24 * 3600,
        900,
        300,
    ));
    let portfolio = opsense_qlib::StreamingPortfolio::new(
        strategy,
        Arc::new(opsense_qlib::SimpleFixedFee::new(0.0005)),
        Arc::new(opsense_qlib::CryptoCalendar),
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
    runtime
        .reload(vec![
            Arc::new(ingest),
            Arc::new(engine),
            Arc::new(drain),
            capture.clone(),
        ])
        .expect("valid graph");
    let mut receiver = runtime.broadcast("sink".into()).expect("output broadcast");
    let _handle = runtime
        .start(|event: Event| async move {
            if let Event::Major((_, error)) = event {
                panic!("runtime error: {error}");
            }
        })
        .expect("start");

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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let payloads = capture.snapshot();
        let kinds = payloads
            .iter()
            .filter_map(event_kind)
            .collect::<HashSet<_>>();
        if kinds.contains("Placed") && kinds.contains("Closed") && kinds.contains("Rebuilt") {
            break;
        }
        match tokio::time::timeout_at(deadline, receiver.recv()).await {
            Ok(Ok(msg)) => {
                assert_trade_event(&msg.payload);
            }
            Ok(Err(_)) | Err(_) => {
                panic!("timed out waiting for qlib order events");
            }
        }
    }

    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");

    let payloads = capture.snapshot();
    assert!(!payloads.is_empty(), "engine phải phát OrderEvent");
    let events = payloads.iter().map(assert_trade_event).collect::<Vec<_>>();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, OrderEvent::Placed { .. })),
        "stream phải phát event Placed"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, OrderEvent::Rebuilt { .. })),
        "stream phải phát event Rebuilt"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, OrderEvent::Closed { .. })),
        "state phải giữ giữa các candle và phát event Closed"
    );

    let first = payloads.first().expect("at least one event");
    let body = TelegramSink {
        id: "notify".into(),
        inputs: vec!["sink".into()],
        token_env: "TELEGRAM_BOT_TOKEN".into(),
        chat_id: "123".into(),
    }
    .body(first)
    .expect("event JSON phải vừa Telegram message");
    assert!(body["text"].as_str().is_some());
}
