//! `http_origin` — station terminal tự quét HTTP: không cần node `clock`, không
//! cần node nào đẩy nhịp, và **không ai phải đọc** thì dữ liệu vẫn tới.
//!
//! ```text
//! http_origin ──mỗi interval──> fetch URL ──update_range──> station "history"
//!                                                                      ▲
//!                                                                      └── reader
//! ```
//!
//! Bốn điều phải chứng minh, và đều là điều dễ hỏng âm thầm:
//!
//! 1. **Tự nạp.** Node chạy một mình, **không có reader nào**, vẫn nạp được
//!    nến. Nếu không thì `http_origin` chỉ là một hàm viết ra không ai gọi —
//!    đúng loại bug từng gặp.
//! 2. **Đọc không gây I/O.** Đọc station bao nhiêu lần cũng **không** sinh thêm
//!    request. Đây không phải chuyện tối ưu rate-limit: đường đọc chạy trong
//!    script Rhai trên `spawn_blocking`, nếu nó kéo HTTP thì task đó không hủy
//!    được, và khi runtime tắt thì tokio panic trong destructor ⇒ SIGABRT giết
//!    cả process (đo được trên `e2e_binance_config`: bật kéo-theo-yêu-cầu thì
//!    abort ở test thứ 3, tắt đi thì 4/4 xanh).
//! 3. **Quét thật.** Vòng quét lặp lại theo `interval_secs`, và chu kỳ sau
//!    không nhân bản nến đã có.
//! 4. **Hỏng thì sống.** Fetch lỗi thì node không chết, và thử lại ở nhịp sau.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opsense_components::http::HttpOrigin;
use opsense_components::vector::runtime::{Component, Event, Runtime};
use opsense_core::Config;
use opsense_core::Context;
use opsense_core::TimeseriesStation;
use opsense_model::secret::Secret;

/// Trả về một response cho **mọi** request, và đếm số lần được gọi.
///
/// Body là một nến Binance `klines` 1m; `openTime` là **mili-giây** nên phải
/// khớp `unit_ms = 1`. Không có `startTime`/`endTime` trong URL nên mock tự tính
/// cửa sổ quanh hiện tại, y hệt Binance với `limit=1000`.
async fn spawn_mock(
    body_for: Arc<dyn Fn(i64, i64) -> String + Send + Sync>,
) -> (std::net::SocketAddr, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let request_lines = Arc::new(Mutex::new(Vec::<String>::new()));

    let (c_calls, c_lines) = (calls.clone(), request_lines.clone());
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let (calls, lines) = (c_calls.clone(), c_lines.clone());
            let body_for = body_for.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = Vec::new();
                let mut chunk = [0u8; 2048];
                loop {
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    let n = sock.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let line = buf
                    .iter()
                    .position(|&b| b == b'\n')
                    .map(|i| String::from_utf8_lossy(&buf[..i]).trim_end().to_string())
                    .unwrap_or_default();
                calls.fetch_add(1, Ordering::SeqCst);
                lines.lock().unwrap().push(line.clone());

                let now = opsense_components::signal::now_secs();
                let body = body_for(now - 3600, now);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (addr, calls, request_lines)
}

/// Chẩn đoán gọn: đã gọi ra ngoài mấy lần và request trông thế nào. `calls == 0`
/// nghĩa là request **không đi ra** — thường là lỗi render/binding.
fn diag_calls(calls: &AtomicUsize, lines: &Mutex<Vec<String>>) -> String {
    format!(
        "origin calls={} lines={:?}",
        calls.load(Ordering::SeqCst),
        lines.lock().unwrap().last().cloned()
    )
}

/// Một nến 1m cho mỗi phút trong `[from, to]`, giá nhấn sóng để `o/h/l/c` khác nhau.
fn klines_body(from: i64, to: i64) -> String {
    let mut rows = Vec::new();
    let mut ts = from.div_euclid(60) * 60;
    while ts <= to {
        let base = 100.0 + (ts / 60 % 7) as f64; // biến động đủ để sieve thấy khác biệt
        // ĐÚNG 6 cột: [openTime, open, high, low, close, volume]. Thiếu một
        // cột thì `parse_candles` bỏ **hết** dòng (`complete = false`) và node
        // nạp rỗng — trông giống thành công nhưng không có gì.
        rows.push(format!(
            "[{}, \"{}\", \"{}\", \"{}\", \"{}\", \"12.5\"]",
            ts * 1000,
            base,
            base + 2.0,
            base - 2.0,
            base + 1.0
        ));
        ts += 60;
    }
    format!("[{}]", rows.join(","))
}

async fn new_context() -> Arc<Context> {
    let cfg: Config = serde_json::from_str("{}").expect("default config");
    let secret = Secret::new().await.expect("Secret::new");
    Arc::new(Context::new(&cfg, Arc::new(secret)))
}

fn origin_node(addr: std::net::SocketAddr, interval_secs: i64) -> HttpOrigin {
    HttpOrigin {
        id: "history".into(),
        station: true,
        url: format!("http://{addr}/klines?limit=1000"),
        method: "GET".into(),
        headers: HashMap::new(),
        body: None,
        // Không binding: node không đoán đơn vị API. Cần đổi đơn vị thì config
        // tự viết, còn chọn cửa sổ phân tích là việc của `grid.rhai`.
        bindings: HashMap::new(),
        interval_secs,
        timeout_secs: 5,
        candles: Some(opsense_components::http::CandleParse {
            mapping: [
                "[].0".to_string(),
                "[].1".to_string(),
                "[].2".to_string(),
                "[].3".to_string(),
                "[].4".to_string(),
                "[].5".to_string(),
            ],
            unit_ms: 1,
            symbol: "BTCUSDT".to_string(),
            resolution: "1m".to_string(),
        }),
    }
}

/// Chờ station của node xuất hiện — `run` được engine spawn, nên có một khoảnh
/// thời gian ngắn trước khi nó đăng ký xong. Đọc trước lúc đó là `NotFound`.
async fn station_of(ctx: &Arc<Context>) -> Arc<tokio::sync::RwLock<TimeseriesStation>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(st) = ctx
            .station::<Arc<tokio::sync::RwLock<TimeseriesStation>>>("history")
            .await
        {
            return st;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "http_origin không đăng ký station `history` sau 10s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Đọc cửa sổ `[from, to]` cho tới khi station có nến trong đó.
async fn read_until_candles(
    ctx: &Arc<Context>,
    from: i64,
    to: i64,
    timeout: Duration,
    diag: &str,
) -> Vec<opsense_model::events::Observation> {
    let st = station_of(ctx).await;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let obs = st.read().await.query_recent(from, to).await.unwrap_or_default();
        if obs.iter().any(|o| o.labels.get("field").is_some()) {
            return obs;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "station `history` không có nến nào trong [{from}, {to}] sau {timeout:?} — {diag}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn node_fills_the_station_on_its_own() {
    let now = opsense_components::signal::now_secs();
    // Cửa sổ rộng hơn cửa sổ mock trả về: mock tự tính quanh lúc nó nhận
    // request, nên `now` của test đã đi trước vài giây.
    let from = now - 7200;
    let to = now + 120;

    let (addr, calls, lines) = spawn_mock(Arc::new(|f, t| klines_body(f, t))).await;
    let ctx = new_context().await;

    let mut runtime = Runtime::new();
    runtime.set_context(ctx.clone());
    // Chỉ **một** node: không clock, không sink, không consumer. Nếu node không tự
    // nạp thì graph này không bao giờ có dữ liệu.
    runtime
        .reload(vec![Arc::new(origin_node(addr, 60)) as Arc<dyn Component>])
        .expect("http_origin là Source nên không cần input cũng hợp lệ");
    let _handle = runtime.start(|_event: Event| async {}).expect("start");

    let obs = read_until_candles(
        &ctx,
        from,
        to,
        Duration::from_secs(10),
        &diag_calls(&calls, &lines),
    )
    .await;

    // 1. Node tự nạp, và chỉ **một** request cho lần nạp đầu.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "node nạp ngay một lần lúc khởi động, không phải chờ có người đọc"
    );
    let line = lines.lock().unwrap().last().cloned().expect("có request");
    assert!(line.contains("limit=1000"), "URL phải render được: {line}");

    // 2. Nến về đúng convention OHLCV của station và nằm trong cửa sổ.
    let fields: std::collections::BTreeSet<&str> = obs
        .iter()
        .filter_map(|o| o.labels.get("field").map(String::as_str))
        .collect();
    for want in ["o", "h", "l", "c", "v"] {
        assert!(fields.contains(want), "thiếu field `{want}`: {fields:?}");
    }
    assert!(obs.iter().all(|o| o.metric_id == "BTCUSDT"));
    assert!(
        obs.iter().all(|o| o.ts >= from && o.ts <= to),
        "nến nằm ngoài cửa sổ đọc"
    );

    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn reading_the_station_never_triggers_a_request() {
    let now = opsense_components::signal::now_secs();
    let from = now - 7200;
    let to = now + 120;

    let (addr, calls, lines) = spawn_mock(Arc::new(|f, t| klines_body(f, t))).await;
    let ctx = new_context().await;

    let mut runtime = Runtime::new();
    runtime.set_context(ctx.clone());
    runtime
        .reload(vec![Arc::new(origin_node(addr, 60)) as Arc<dyn Component>])
        .expect("graph hợp lệ");
    let _handle = runtime.start(|_event: Event| async {}).expect("start");

    let obs = read_until_candles(
        &ctx,
        from,
        to,
        Duration::from_secs(10),
        &diag_calls(&calls, &lines),
    )
    .await;
    let st = station_of(&ctx).await;

    // `interval_secs = 60` nên trong cửa sổ thử này chỉ có đúng một chu kỳ quét.
    // Mọi lần đọc phải **không** sinh thêm request — đây là điều khiến
    // `spawn_blocking` của script không bao giờ phải chờ mạng.
    for _ in 0..20 {
        let again = st.read().await.query_recent(from, to).await.unwrap_or_default();
        assert_eq!(
            again.len(),
            obs.len(),
            "đọc lại cùng cửa sổ phải cho cùng kết quả"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "20 lần đọc mà vẫn chỉ 1 request — đọc KHÔNG được kéo mạng: {}",
            diag_calls(&calls, &lines)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_scan_loop_repeats_without_duplicating_candles() {
    let now = opsense_components::signal::now_secs();
    let from = now - 7200;
    let to = now + 120;

    let (addr, calls, lines) = spawn_mock(Arc::new(|f, t| klines_body(f, t))).await;
    let ctx = new_context().await;

    let mut runtime = Runtime::new();
    runtime.set_context(ctx.clone());
    // Nhịp 1s để thấy vòng lặp thật sự quay trong thời gian test.
    runtime
        .reload(vec![Arc::new(origin_node(addr, 1)) as Arc<dyn Component>])
        .expect("graph hợp lệ");
    let _handle = runtime.start(|_event: Event| async {}).expect("start");

    let obs = read_until_candles(
        &ctx,
        from,
        to,
        Duration::from_secs(10),
        &diag_calls(&calls, &lines),
    )
    .await;

    // Chờ ít nhất 3 nhịp quét.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while calls.load(Ordering::SeqCst) < 3 {
        assert!(
            std::time::Instant::now() < deadline,
            "vòng quét không quay lại sau 10s — {}",
            diag_calls(&calls, &lines)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let st = station_of(&ctx).await;
    let after = st
        .read()
        .await
        .query_recent(from, to)
        .await
        .unwrap_or_default();

    // Nhiều lần nạp **không** được nhân bản nến: cùng ts + cùng field là một
    // observation, nếu không thì vài nhịp quét là vài chục nghìn dòng rác.
    let mut keys: Vec<(i64, String, String)> = after
        .iter()
        .map(|o| {
            (
                o.ts,
                o.labels.get("field").cloned().unwrap_or_default(),
                o.metric_id.clone(),
            )
        })
        .collect();
    let before = keys.len();
    keys.sort();
    let unique = {
        let mut v = keys.clone();
        v.dedup();
        v.len()
    };
    assert_eq!(
        before,
        unique,
        "nạp lại {} lần mà có {} dòng trùng (ts, field) — nến bị nhân bản",
        calls.load(Ordering::SeqCst),
        before - unique
    );
    assert!(after.len() >= obs.len(), "dữ liệu phải không mất đi");

    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_endpoint_does_not_kill_the_node() {
    // Endpoint chết ngay ⇒ fetch lỗi mỗi nhịp. Node phải sống tiếp và đọc vẫn
    // trả rỗng chứ không panic: caller (`station_candles` trong Rhai) tự xử lý,
    // và nếu node chết thì cả pipeline chết.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Không accept ⇒ kết nối bị từ chối/rơi ⇒ `request.send()` lỗi.
    drop(listener);

    let ctx = new_context().await;
    let mut runtime = Runtime::new();
    runtime.set_context(ctx.clone());
    runtime
        .reload(vec![Arc::new(origin_node(addr, 1)) as Arc<dyn Component>])
        .expect("graph hợp lệ");
    let _handle = runtime.start(|_event: Event| async {}).expect("start");

    let now = opsense_components::signal::now_secs();
    let st = station_of(&ctx).await;

    // Qua ít nhất 2 nhịp thất bại: nếu lỗi làm node chết thì lần đọc sau sẽ
    // không còn ai trả lời.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    for _ in 0..2 {
        let got = st
            .read()
            .await
            .query_recent(now - 600, now)
            .await
            .unwrap_or_default();
        assert!(
            got.is_empty(),
            "endpoint chết thì đọc trả rỗng, không phải bỏ panicking: {got:?}"
        );
    }

    runtime.stop().expect("stop");
    runtime.wait_for_shutdown().await.expect("shutdown");
}

/// `HttpOrigin` không có field `inputs` — đó là điểm cố ý, vì nó là
/// `ComponentType::Source`. Test này khoá lại hành vi đó để không ai thêm
/// `inputs` về sau và biến nó thành transform không có ai đẩy.
#[test]
fn origin_is_a_source_so_it_needs_no_upstream() {
    use opsense_mlib::vector::runtime::{Component, ComponentType};

    let node: Arc<dyn Component> = Arc::new(origin_node("127.0.0.1:1".parse().unwrap(), 60));
    assert_eq!(
        node.component_type(),
        ComponentType::Source,
        "http_origin phải là Source: đó là lý do nó không cần `clock`"
    );
    assert!(
        node.get_inputs().is_none(),
        "Source không khai input — nếu khai thì engine coi là transform và đòi upstream"
    );
}
