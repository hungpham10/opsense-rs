//! HTTP source node: fetch a URL on every tick, parse the response body as
//! observations, write them to the node's own `Timeseries` station, then
//! forward `data_ready(ts)` downstream.
//!
//! Request is fully template-driven via `{{name}}` placeholders that resolve
//! at call time through a 3-layer lookup (plan B.2):
//!
//! 1. Bound variable — output of any `bindings` entry, evaluated by
//!    [`opsense_mlib::jq::JsonQuery`] against a context object
//!    `{"ts", "interval", "now", "payload", "attributes"}`.
//! 2. Field in the upstream message payload (so a clock/publisher can pass
//!    values in-band).
//! 3. [`Context::variable`] — looks up `attributes` (TOML + env) then the
//!    secret store, parsing to `String`.
//!
//! Response body is expected to be JSON either an array of observation
//! objects, or a single observation object (wrapped into a 1-element vec).
//! When `candles` is configured the body is instead a sequence of candle rows
//! (e.g. Binance `klines`); the six jq `mapping` paths select
//! [open time, open, high, low, close, volume] from every row and each row is
//! expanded into five OHLCV observations.
//! Mapping/extraction (`items`/`fields`/`constants`) is intentionally out of
//! scope for this pass — a separate extractor node can reshape data when
//! needed.

use std::collections::{BTreeMap, HashMap};
use std::io::Error;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

use opsense_core::Context;
use opsense_core::Observation;
use opsense_core::Station;
use opsense_core::TimeseriesStation;
use opsense_mlib::jq::JsonQuery;
use opsense_macros::{source, transform};

use crate::station::downcast_ctx;
use crate::vector::runtime::{Component, Identify, Message, Outbound};
use crate::{render, signal};

/// `station = true` makes the node terminal: its own station is queryable, so
/// the node does not need a downstream consumer to be useful.
#[transform(terminal_field = "station")]
pub struct HttpSource {
    pub id: String,
    pub inputs: Vec<String>,

    /// Request URL. `{{name}}` placeholders are resolved per cycle.
    pub url: String,

    #[serde(default = "default_method")]
    pub method: String,

    /// Header values are templates (`Bearer {{token}}`).
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Optional request body template.
    #[serde(default)]
    pub body: Option<String>,

    /// Bindings: name → jq expression. Each expression is evaluated against
    /// `{"ts", "interval", "now", "payload", "attributes"}` and the first
    /// result is stringified into the `{{name}}` lookup table.
    #[serde(default)]
    pub bindings: HashMap<String, String>,

    /// Default cycle interval (seconds) when an incoming tick carries no
    /// `interval` field in its payload.
    #[serde(default = "default_interval")]
    pub interval_secs: i64,

    /// HTTP request timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// Register a `TimeseriesStation` under this node's id so reads can go
    /// through the registry (REPL/MCP/HTTP).
    #[serde(default = "default_station")]
    pub station: bool,

    /// Optional candle (OHLCV) parse mode. When present the response body is
    /// interpreted as a sequence of candle rows (e.g. Binance `klines`) and
    /// each row is expanded into five observations (`labels.field` ∈
    /// o/h/l/c/v) following the shared OHLCV station convention.
    #[serde(default)]
    pub candles: Option<CandleParse>,
}

/// Numeric parse mode: the response is a sequence of candle rows; the six
/// `mapping` jq paths select [open time, open, high, low, close, volume] from
/// every row. Rows are zipped into observations at the station level.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandleParse {
    /// Six jq paths selecting [open time, open, high, low, close, volume]
    /// from each row, e.g. `["[].0", "[].1", "[].2", "[].3", "[].4", "[].5"]`.
    #[serde(default = "default_candle_mapping")]
    pub mapping: [String; 6],

    /// Milliseconds represented by one open-time unit. `1` for millisecond
    /// timestamps (Binance klines), `1000` for second-based timestamps.
    /// Seconds = `open_time * unit_ms / 1000`.
    #[serde(default = "default_unit_ms")]
    pub unit_ms: u64,

    /// Symbol written as `metric_id` on each observation (e.g. `"BTCUSDT"`).
    pub symbol: String,

    /// Resolution stored under `labels.resolution` (e.g. `"1m"`).
    pub resolution: String,
}

const fn default_unit_ms() -> u64 {
    1
}

/// Default candle mapping — Binance `klines` layout
/// (`[openTime, open, high, low, close, volume, …]`).
fn default_candle_mapping() -> [String; 6] {
    [
        "[].0".to_string(),
        "[].1".to_string(),
        "[].2".to_string(),
        "[].3".to_string(),
        "[].4".to_string(),
        "[].5".to_string(),
    ]
}

fn default_method() -> String {
    "GET".to_string()
}

fn default_interval() -> i64 {
    60
}

fn default_timeout() -> u64 {
    30
}

fn default_station() -> bool {
    true
}

impl HttpSource {
    #[must_use]
    pub fn new(id: &str, inputs: &[&str], url: &str) -> Self {
        Self {
            id: id.to_string(),
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            url: url.to_string(),
            method: default_method(),
            headers: HashMap::new(),
            body: None,
            bindings: HashMap::new(),
            interval_secs: default_interval(),
            timeout_secs: default_timeout(),
            station: default_station(),
            candles: None,
        }
    }
}

/// One shared client per timeout so poll-cadence calls reuse connections.
fn client_for(timeout_secs: u64) -> Result<reqwest::Client, String> {
    // Cài crypto provider **trước khi dựng client**. Workspace bật cả `ring`
    // (qua reqwest) lẫn `aws-lc-rs` (qua AWS SDK) trên cùng `rustls 0.23`, nên
    // `Client::builder()` panic "No provider set" nếu chưa ai cài.
    //
    // Trước đây việc cài nằm trong `main()` của binary `opsense`, nên mọi
    // embedder không có `main` — integration test, crate dùng lại — chết ngay
    // tại đây. `mlib::tls` có sẵn helper `Once`-guarded, idempotent, không
    // panic; gọi ở **chỗ dựng client** thì mọi đường vào đều tự được bảo vệ,
    // kể cả người gọi không biết chuyện này.
    opsense_mlib::tls::install_default_crypto_provider();

    static CLIENTS: OnceLock<std::sync::Mutex<HashMap<u64, reqwest::Client>>> = OnceLock::new();
    let clients = CLIENTS.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    // `unwrap()` ở đây chỉ fail nếu một thread khác **panic giữa lúc giữ lock**.
    // Xem `poisoned_clients_still_serve` — trước đây panic ở `build()` làm
    // nhiễm lock và mọi lời gọi sau đó chết bằng `PoisonError` thay vì lỗi thật.
    let mut guard = clients.lock().unwrap_or_else(|e| e.into_inner());

    let timeout_secs = timeout_secs.max(1);
    if let Some(client) = guard.get(&timeout_secs) {
        return Ok(client.clone());
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .map_err(|e| format!("http client build failed: {e}"))?;
    guard.insert(timeout_secs, client.clone());
    Ok(client)
}

/// Best-effort `Value → String` for the bound-var table. Numbers/booleans are
/// rendered with their natural display form; objects/arrays fall back to a
/// compact JSON string. `Null` becomes an empty string.
fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Build the per-cycle `{{name}}` lookup table. 3 layers, first non-empty wins:
///
/// 1. `bound[name]` from the just-evaluated bindings.
/// 2. Field `name` in the incoming message payload (coerced to string).
/// 3. `Context::variable::<String>(name)` — attributes then secret.
pub(crate) async fn build_vars(
    ctx: &Context,
    bound: BTreeMap<String, String>,
    payload: &Value,
) -> BTreeMap<String, String> {
    // Tầng 3 đọc `ctx` mỗi lần — chỉ dùng được khi còn `&Context` (đường push).
    let mut names: Vec<String> = bound.keys().cloned().collect();
    if let Some(obj) = payload.as_object() {
        for k in obj.keys() {
            if !names.contains(k) {
                names.push(k.clone());
            }
        }
    }
    let mut fallback = BTreeMap::new();
    for name in names {
        if let Ok(v) = ctx.variable::<String>(&name).await {
            fallback.insert(name, v);
        }
    }
    merge_vars(&fallback, bound, payload)
}

/// Giống [`build_vars`] nhưng tầng 3 lấy từ bảng đã giải sẵn.
///
/// Dùng cho [`HttpOrigin`]: node tự quét, nên `HttpFetcher` phải sống lâu hơn
/// một request — mà `&Context` chỉ có được trong `run`, không giữ được qua
/// `.await` (`downcast_ctx` trả `&Context`). Tầng 3 là attributes TOML + env +
/// secret — **cấu hình mức process** — nên đọc một lần lúc khởi động đúng hơn
/// là đọc lại mỗi chu kỳ, và secret không bị đọc lặp cả đời process.
pub(crate) fn merge_vars(
    fallback: &BTreeMap<String, String>,
    bound: BTreeMap<String, String>,
    payload: &Value,
) -> BTreeMap<String, String> {
    // Collect candidate names from layer 1 + layer 2.
    let mut names: Vec<String> = bound.keys().cloned().collect();
    if let Some(obj) = payload.as_object() {
        for k in obj.keys() {
            if !names.contains(k) {
                names.push(k.clone());
            }
        }
    }

    let mut vars = BTreeMap::new();
    for name in names {
        if let Some(v) = bound.get(&name)
            && !v.is_empty()
        {
            vars.insert(name.clone(), v.clone());
            continue;
        }
        if let Some(v) = payload.get(&name) {
            vars.insert(name.clone(), value_to_string(v));
            continue;
        }
        if let Some(v) = fallback.get(&name) {
            vars.insert(name, v.clone());
        }
    }
    vars
}

/// Tên mọi placeholder `{{name}}` trong template — trùng đúng luật [`render`]
/// đọc (thẻ `{{`, khoá trim, bỏ qua thẻ rỗng).
fn placeholder_names(template: &str) -> Vec<String> {
    let bytes = template.as_bytes();
    let mut names: Vec<String> = Vec::new();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'{' && bytes[i + 1] == b'{' {
            let mut j = i + 2;
            while j + 1 < bytes.len() && !(bytes[j] == b'}' && bytes[j + 1] == b'}') {
                j += 1;
            }
            if j + 1 >= bytes.len() {
                break; // chưa đóng thẻ — `render` sẽ báo lỗi lúc chạy
            }
            let key = template[i + 2..j].trim();
            if !key.is_empty() && !names.iter().any(|n| n == key) {
                names.push(key.to_string());
            }
            i = j + 2;
        } else {
            i += 1;
        }
    }
    names
}

/// Parse the response body as JSON. Accepts either `[obs, obs, ...]` or a
/// single `obs` object (wrapped into a 1-element vec). Malformed entries are
/// skipped and logged.
fn parse_observations(body: &str) -> Result<Vec<Observation>, String> {
    let value: Value = serde_json::from_str(body).map_err(|e| format!("body: {e}"))?;
    let arr = match value {
        Value::Array(arr) => arr,
        other => vec![other],
    };
    let mut out = Vec::with_capacity(arr.len());
    for (i, item) in arr.into_iter().enumerate() {
        match serde_json::from_value::<Observation>(item) {
            Ok(obs) => out.push(obs),
            Err(e) => {
                tracing::warn!("http body item {i} skipped: {e}");
            }
        }
    }
    Ok(out)
}

/// Parse a candle (OHLCV) response body into observations.
///
/// Each of the six `mapping` paths selects one column across the whole body;
/// row `i` is the zip of the six columns at index `i`. Cells may be JSON
/// numbers or numeric strings. Malformed rows (missing a column) are skipped.
fn parse_candles(body: &str, cfg: &CandleParse) -> Result<Vec<Observation>, String> {
    let value: Value = serde_json::from_str(body).map_err(|e| format!("body: {e}"))?;
    let mut columns: Vec<Vec<Value>> = Vec::with_capacity(cfg.mapping.len());
    for (i, path) in cfg.mapping.iter().enumerate() {
        let q = JsonQuery::parse(path).map_err(|e| format!("mapping[{i}] `{path}`: {e}"))?;
        columns.push(q.execute(&value));
    }

    let rows = columns.first().map(Vec::len).unwrap_or(0);
    let mut out = Vec::with_capacity(rows * 5);
    for i in 0..rows {
        // Cells: [open time, open, high, low, close, volume].
        let mut cells = [0.0f64; 6];
        let mut complete = true;
        for (col, column) in columns.iter().enumerate() {
            match column.get(i).and_then(cell_f64) {
                Some(v) => cells[col] = v,
                None => {
                    complete = false;
                    break;
                }
            }
        }
        if !complete {
            continue;
        }
        let ts = crate::ohlcv::open_time_to_secs(cells[0], cfg.unit_ms);
        out.extend(crate::ohlcv::row_to_observations(
            ts,
            &cells,
            &cfg.symbol,
            &cfg.resolution,
        ));
    }
    Ok(out)
}

/// Coerce a JSON cell to `f64`: numbers directly, strings parsed numerically.
fn cell_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Mọi thứ cần để dựng và gửi **một** request, đã pre-parse hết.
///
/// Tách khỏi vòng push để hai đường dùng chung đúng một pipeline: nhịp đẩy
/// (`while let Some(msg) = rx.recv()`) và vòng tự quét của [`HttpOrigin`].
struct HttpFetcher {
    url: String,
    method: reqwest::Method,
    headers: HashMap<String, String>,
    body: Option<String>,
    bindings: Vec<(String, JsonQuery)>,
    candles: Option<CandleParse>,
    /// Tầng 3 của `{{name}}` đã giải sẵn từ `Context::variable` (attributes +
    /// env + secret). Xem [`merge_vars`] vì sao không giữ `&Context`.
    attributes: BTreeMap<String, String>,
    client: reqwest::Client,
}

impl HttpFetcher {
    /// Pre-parse binding + dựng client. Lỗi ở đây là **lỗi config** (tên
    /// binding sai cú pháp, method không hợp lệ, không dựng được client) — cả
    /// hai đường đều chết vĩnh viễn với nó, nên báo lúc khởi động thay vì
    /// `warn` rồi im lặng mỗi chu kỳ.
    /// Dựng từ `http_source` (Transform: có input, đẩy xuống dưới).
    async fn build(src: &HttpSource, ctx: &Context) -> Result<Self, String> {
        Self::assemble(
            &src.url,
            &src.method,
            &src.headers,
            &src.body,
            &src.bindings,
            src.timeout_secs,
            src.candles.clone(),
            ctx,
        )
        .await
    }

    /// Dựng từ `http_origin` (Source: không input, kéo theo yêu cầu).
    async fn build_origin(src: &HttpOrigin, ctx: &Context) -> Result<Self, String> {
        Self::assemble(
            &src.url,
            &src.method,
            &src.headers,
            &src.body,
            &src.bindings,
            src.timeout_secs,
            src.candles.clone(),
            ctx,
        )
        .await
    }

    /// Phần chung của hai component: pre-parse binding, dựng client, giải
    /// trước tầng 3 của `{{name}}`.
    #[allow(clippy::too_many_arguments)]
    async fn assemble(
        url: &str,
        method: &str,
        headers: &HashMap<String, String>,
        body: &Option<String>,
        bindings: &HashMap<String, String>,
        timeout_secs: u64,
        candles: Option<CandleParse>,
        ctx: &Context,
    ) -> Result<Self, String> {
        let mut parsed = Vec::with_capacity(bindings.len());
        for (name, expr) in bindings {
            let q = JsonQuery::parse(expr).map_err(|e| format!("binding `{name}`: {e}"))?;
            parsed.push((name.clone(), q));
        }
        let method = reqwest::Method::from_bytes(method.trim().as_bytes())
            .map_err(|e| format!("method `{method}`: {e}"))?;

        // Tầng 3: giải trước mọi tên có thể xuất hiện trong template.
        let mut wanted: Vec<String> = bindings.keys().cloned().collect();
        wanted.extend(placeholder_names(url));
        for v in headers.values() {
            wanted.extend(placeholder_names(v));
        }
        if let Some(b) = body {
            wanted.extend(placeholder_names(b));
        }
        let mut attributes = BTreeMap::new();
        for name in wanted {
            if let Ok(v) = ctx.variable::<String>(&name).await {
                attributes.insert(name, v);
            }
        }

        Ok(Self {
            url: url.to_string(),
            method,
            headers: headers.clone(),
            body: body.clone(),
            bindings: parsed,
            candles,
            attributes,
            client: client_for(timeout_secs)?,
        })
    }

    /// Một chu kỳ: dựng URL → gửi → parse.
    ///
    /// `from`/`to` (giây) đi vào ctx của binding và vào tầng 2 của bảng vars, nên
    /// template dùng `{{from_ts}}` trực tiếp. Node **không** tự quyết đơn vị mà
    /// API cần — API nào đòi mili-giây thì config tự viết binding.
    async fn fetch_once(
        &self,
        ts: i64,
        interval: i64,
        from: i64,
        to: i64,
        payload: &Value,
    ) -> Result<Vec<Observation>, String> {
        let ctx_value = serde_json::json!({
            "ts": ts,
            "interval": interval,
            "now": signal::now_secs(),
            "payload": payload,
            // Cửa sổ mà node đang phục vụ, **tính bằng giây**. Node không
            // đoán đơn vị API mong muốn: nếu API cần mili-giây thì config tự
            // viết binding, còn việc quyết định "cửa sổ phân tích bao nhiêu
            // và lấy vòng nào" thuộc về script, không thuộc về transport.
            //
            // Xem `grid.rhai` — nó quyết định cửa sổ và sieve, không phải node.
            "from_ts": from,
            "to_ts": to,
        });
        let mut bound = BTreeMap::new();
        for (name, q) in &self.bindings {
            let result = q.execute(&ctx_value);
            let first = result.into_iter().next().unwrap_or(Value::Null);
            bound.insert(name.clone(), value_to_string(&first));
        }

        // `from_ts`/`to_ts` được đưa vào tầng 2 (ngang với field của payload)
        // nên `{{from_ts}}` dùng được trong url/headers/body mà không cần
        // binding. Đường push không có cửa sổ nên không thêm — `from == to`.
        let layer2 = if from == to {
            payload.clone()
        } else {
            let mut obj = payload.as_object().cloned().unwrap_or_default();
            obj.insert("from_ts".to_string(), Value::from(from));
            obj.insert("to_ts".to_string(), Value::from(to));
            Value::Object(obj)
        };
        let vars = merge_vars(&self.attributes, bound, &layer2);

        let url = render(&self.url, &vars)?;
        let mut request = self.client.request(self.method.clone(), &url);
        for (k, v) in &self.headers {
            request = request.header(k, render(v, &vars)?);
        }
        if let Some(body) = &self.body {
            request = request.body(render(body, &vars)?);
        }

        let response = request.send().await.map_err(|e| format!("request: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("endpoint answered {}", response.status()));
        }
        let body_text = response.text().await.map_err(|e| format!("body: {e}"))?;

        match &self.candles {
            Some(cfg) => parse_candles(&body_text, cfg),
            None => parse_observations(&body_text),
        }
    }
}

/// HTTP source cho station mà **không ai đẩy nhịp cho** — terminal trong
/// pipeline, không có input, không có consumer.
///
/// Vì sao tách thành component riêng mà không làm `HttpSource` tự nhịp khi
/// `inputs` rỗng: `HttpSource` là `ComponentType::Transform`, và engine **cấm**
/// transform không input. Node tự quét là *nguồn*, không phải transform.
/// `HttpSource` vẫn phục vụ `strategies/predict` và `strategies/prometheus` (cả
/// hai dùng `inputs = ["clock"]` và cần forward xuống dưới).
///
/// **Vì sao quét ở đây chứ không kéo khi có người đọc.** Đường đọc của station
/// chạy trong script Rhai, mà script chạy trên `spawn_blocking` và API đọc là
/// **đồng bộ**: muốn kéo HTTP được thì phải `Handle::block_on` ngay trong đó.
/// `spawn_blocking` không hủy được, nên khi runtime tắt mà script còn đang kéo
/// thì tokio panic trong destructor và **SIGABRT giết cả process** — đo được:
/// bật kéo-theo-yêu-cầu thì `e2e_binance_config` abort ở test thứ 3; tắt đi thì
/// 4/4 xanh. Quét ở đây giữ đường đọc thuần RAM, nên hết hẳn.
///
/// **Node không cần biết cửa sổ ai sẽ hỏi.** Mỗi chu kỳ nó fetch URL và lấy
/// đúng những gì URL đó trả về (Binance `/klines?limit=1000` ⇒ 1000 nến mới nhất
/// ≈ 16.6h). Phủ hay không là thuộc tính của dữ liệu, không phải thứ node phải
/// được dạy — nên không có con số nào bị copy giữa node và `grid.rhai`.
///
/// **Trần thật**: cửa sổ sâu hơn 16.6h sẽ không có dữ liệu. Đó là giới hạn của
/// Binance, không phải của kiến trúc này; muốn sâu hơn thì phải đổi API.
///
/// Template nhận thêm hai biến: `{{from_ts}}` và `{{to_ts}}`, **tính bằng giây**.
/// Ở vòng quét, `to_ts = now` và `from_ts = now - interval_secs` — vừa đủ để
/// một URL có `{{from_ts}}` kéo **tăng dần** mà không phải quét lại cả lịch sử.
/// API nào đòi đơn vị khác thì config tự viết binding — node không đoán.
#[source(terminal_field = "station")]
pub struct HttpOrigin {
    pub id: String,

    /// Node terminal — station của chính nó là nơi phục vụ, nên không cần
    /// consumer. Luôn `true` trong thực tế: đặt `false` thì graph bị engine từ
    /// chối (*"Source history must have connect with another nodes"*) vì node
    /// này không còn việc gì khác để làm. Field này tồn tại vì macro terminal
    /// đọc nó, và để khớp với `HttpSource` / `RhaiTransform`.
    #[serde(default = "default_station")]
    pub station: bool,

    /// Request URL. `{{name}}` + `{{from_ts}}`/`{{to_ts}}`.
    pub url: String,

    #[serde(default = "default_method")]
    pub method: String,

    /// Header values là template (`Bearer {{token}}`).
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Optional request body template.
    #[serde(default)]
    pub body: Option<String>,

    /// Bindings: name → jq, chạy trên
    /// `{"ts", "interval", "now", "payload", "from_ts", "to_ts"}`.
    #[serde(default)]
    pub bindings: HashMap<String, String>,

    /// Chỉ còn là **giá trị binding** `{{interval}}` — node này không có nhịp.
    #[serde(default = "default_interval")]
    pub interval_secs: i64,

    /// HTTP request timeout (giây).
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// Parse body thành nến OHLCV (vd Binance `/klines`).
    #[serde(default)]
    pub candles: Option<CandleParse>,
}

impl_http_origin!(
    async fn run(
        &self,
        _id: usize,
        _rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;
        let fetcher = Arc::new(
            HttpFetcher::build_origin(self, ctx)
                .await
                .map_err(|e| Error::other(format!("http_origin {}: {e}", self.id)))?,
        );

        // Giữ `Arc` của chính station để vòng quét tự ghi vào, đồng thời đăng ký
        // một bản `Arc` nữa cho reader. `Arc` chứ không `Station` thường: mỗi
        // chu kỳ phải `read().await` để `update_range`, mà `ctx.station()` trả
        // `&'static Station` — không đi qua được.
        let station = std::sync::Arc::new(tokio::sync::RwLock::new(
            TimeseriesStation::from_storage(&self.id, ctx.storage()).await?,
        ));
        ctx.registry(&self.id, Station::Timeseries(std::sync::Arc::clone(&station)))
            .await
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(e)
                }
            })?;

        // `interval_secs = 0` hoặc âm là nguồn chết: `sleep(0)` quay vô hạn và
        // spam API, `sleep` âm thì tokio lo. Chặn ở đây cho thành lỗi cấu
        // hình, không phải thành node quay vô hạn lúc chạy.
        let interval = self.interval_secs.max(1);

        tracing::info!(
            node = %self.id,
            interval_secs = interval,
            "http_origin: station terminal, tự quét theo interval"
        );

        // Vòng quét nằm **trên async**, không `spawn_blocking` và không
        // `block_on` — đó là toàn bộ lý do node này tồn tại thay vì để
        // `query_recent` tự kéo (xem doc trên `HttpOrigin`).
        //
        // Fetch **ngay** một lần trước khi ngủ: lần đọc đầu tiên của `grid`
        // chỉ xảy ra khi nến phút đầu tiên đóng (≥ 1 phút sau), nên khi nó
        // tới thì đã có dữ liệu — độ trễ lần đầu gần bằng 0, không phải một
        // nhịp quét.
        loop {
            let now = signal::now_secs();
            // `from_ts = now - interval_secs`: URL có `{{from_ts}}` thì kéo
            // tăng dần (chồng đúng một nhịp) thay vì quét lại cả lịch sử.
            // URL không dùng thì hai số này không ảnh hưởng gì.
            match fetcher
                .fetch_once(now, interval, now - interval, now, &Value::Null)
                .await
            {
                Ok(obs) if !obs.is_empty() => {
                    // `update_range` cần cửa sổ bọc đúng mọi obs — lấy từ chính
                    // dữ liệu thay vì từ `from_ts`/`to_ts`, vì API có thể trả
                    // ngoài cửa sổ mình yêu cầu (Binance với `limit` trả nến
                    // *mới nhất*, tức phía trước `from_ts`).
                    let from = obs.iter().map(|o| o.ts).min().unwrap_or(now);
                    let to = obs.iter().map(|o| o.ts).max().unwrap_or(now);
                    let n = obs.len();
                    station.read().await.update_range(&obs, from, to, now);
                    tracing::debug!(
                        node = %self.id,
                        candles = n,
                        from,
                        to,
                        "http_origin: đã nạp nến"
                    );
                }
                // `Ok(rỗng)` = API không có gì để cho (giờ chưa có nến nào chốt).
                // Không phải lỗi — im lặng ở `debug` để không spam mỗi nhịp.
                Ok(_) => {
                    tracing::debug!(node = %self.id, "http_origin: response rỗng");
                }
                // Lỗi **không** làm chết node: chu kỳ sau thử lại. Nếu để `?`
                // thì một lần 502 của Binance là pipeline chết vĩnh viễn.
                Err(e) => {
                    tracing::warn!(
                        node = %self.id,
                        interval_secs = interval,
                        error = %e,
                        "http_origin: fetch thất bại — thử lại ở nhịp sau"
                    );
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(interval as u64)).await;
        }
    }
);

impl_http_source!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;

        let fetcher = Arc::new(
            HttpFetcher::build(self, ctx)
                .await
                .map_err(|e| Error::other(format!("http {}: {e}", self.id)))?,
        );

        // Register the station eagerly so reads before the first cycle still
        // resolve to an empty timeseries rather than a `NotFound` error.
        if self.station {
            let station = TimeseriesStation::from_storage(&self.id, ctx.storage()).await?;
            ctx.registry(
                &self.id,
                Station::Timeseries(std::sync::Arc::new(tokio::sync::RwLock::new(station))),
            )
            .await
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(e)
                }
            })?;
        }
        let me_handle = if self.station {
            Some(
                ctx.station::<std::sync::Arc<tokio::sync::RwLock<TimeseriesStation>>>(&self.id)
                    .await?,
            )
        } else {
            None
        };

        while let Some(msg) = rx.recv().await {
            // Only ticks and `data_ready`/`processed` carry a usable ts.
            let Some(ts) = signal::ts(&msg) else {
                continue;
            };
            let interval = msg
                .payload
                .get("interval")
                .and_then(Value::as_i64)
                .unwrap_or(self.interval_secs);

            let batch = match fetcher
                .fetch_once(ts, interval, ts, ts, &msg.payload)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("http {}: {e}", self.id);
                    continue;
                }
            };

            // write to station
            if let Some(me) = &me_handle
                && !batch.is_empty()
            {
                let from = batch.iter().map(|o| o.ts).min().unwrap_or(ts);
                let to = batch.iter().map(|o| o.ts).max().unwrap_or(ts);
                me.write().await.update_range(&batch, from, to, to);
            }

            // forward so downstream nodes see this cycle.
            let out = if batch.is_empty() {
                signal::data_ready(ts)
            } else {
                signal::data_ready_with(ts, serde_json::to_value(&batch).unwrap_or(Value::Null))
            };
            let ready = signal::tagged(out, &self.id);
            for s in &tx.streams {
                let _ = s.send(ready.clone()).await;
            }
        }
        Ok(())
    }
);

#[cfg(test)]
mod tests {
    use super::{CandleParse, client_for, parse_candles};
    use opsense_core::{Signal, TelemetryKind};

    /// Regression: dựng reqwest client phải **không** panic "No provider set".
    ///
    /// Workspace bật cả `ring` lẫn `aws-lc-rs` trên `rustls 0.23`, nên
    /// `Client::builder()` chết nếu chưa ai cài provider. Test này chạy trong
    /// test binary — **không có `main()`** — nên nó chính là hoàn cảnh của mọi
    /// embedder đã chết trước đây, và chặn tái phát nếu ai gỡ lời gọi
    /// `install_default_crypto_provider()` trong `client_for`.
    #[test]
    fn builds_client_without_a_process_provider() {
        client_for(5).expect("client build phải không panic");
    }

    /// Client dùng chung theo timeout ⇒ `client_for` phải trả về thành công ở
    /// mọi lần gọi, kể cả sau một lần thất bại trước đó (lock bị nhiễm nếu
    /// `build()` panic giữa lúc giữ lock).
    #[test]
    fn client_survives_repeated_calls() {
        client_for(7).expect("lần 1");
        client_for(7).expect("lần 2");
        client_for(9).expect("timeout khác");
    }

    fn binance_cfg() -> CandleParse {
        CandleParse {
            mapping: super::default_candle_mapping(),
            unit_ms: 1,
            symbol: "BTCUSDT".to_string(),
            resolution: "1m".to_string(),
        }
    }

    #[test]
    fn parses_binance_klines_into_ohlcv_observations() {
        // openTime in ms; price cells are numeric strings.
        let body = r#"[
            [1700000000000, "100",   "101",  "99",  "100.5", "10"],
            [1700000060000, "100.5", "102",  "100", "101",   "20"]
        ]"#;
        let obs = parse_candles(body, &binance_cfg()).unwrap();

        // 2 candles × 5 fields.
        assert_eq!(obs.len(), 10);

        // ms → seconds normalization.
        let (t1, t2) = (1_700_000_000, 1_700_000_060);
        let first: Vec<_> = obs.iter().filter(|o| o.ts == t1).collect();
        let second: Vec<_> = obs.iter().filter(|o| o.ts == t2).collect();
        assert_eq!(first.len(), 5);
        assert_eq!(second.len(), 5);

        for row in [&first, &second] {
            for o in row {
                assert_eq!(o.metric_id, "BTCUSDT");
                assert_eq!(o.kind, TelemetryKind::Metric);
                assert_eq!(o.signal, Signal::Raw);
                assert_eq!(o.labels.get("resolution").unwrap(), "1m");
            }
        }

        let field = |row: &[&opsense_core::Observation], f: &str| -> f64 {
            row.iter()
                .find(|o| o.labels.get("field").map(String::as_str) == Some(f))
                .unwrap()
                .value
        };
        assert_eq!(field(&first, "o"), 100.0);
        assert_eq!(field(&first, "h"), 101.0);
        assert_eq!(field(&first, "l"), 99.0);
        assert_eq!(field(&first, "c"), 100.5);
        assert_eq!(field(&first, "v"), 10.0);
        assert_eq!(field(&second, "c"), 101.0);
        assert_eq!(field(&second, "v"), 20.0);
    }

    #[test]
    fn honors_custom_mapping_and_unit_ms() {
        let mut cfg = binance_cfg();
        cfg.mapping = [
            ".rows[].0".into(),
            ".rows[].1".into(),
            ".rows[].2".into(),
            ".rows[].3".into(),
            ".rows[].4".into(),
            ".rows[].5".into(),
        ];
        cfg.unit_ms = 1000; // open time already in seconds.
        cfg.symbol = "ETHUSDT".into();
        cfg.resolution = "1h".into();

        let body = r#"{"rows": [[1700000000, 1.1, 1.2, 1.0, 1.15, 33], [1700003600, 1.15, 1.3, 1.1, 1.25, 41]]}"#;
        let obs = parse_candles(body, &cfg).unwrap();
        assert_eq!(obs.len(), 10);
        assert_eq!(obs[0].ts, 1_700_000_000);
        assert_eq!(obs[5].ts, 1_700_003_600);
        assert_eq!(obs[0].metric_id, "ETHUSDT");
        assert_eq!(obs[0].labels.get("resolution").unwrap(), "1h");
    }

    #[test]
    fn skips_rows_with_missing_columns() {
        let body = r#"[
            [1700000000000, "100", "101", "99", "100.5", "10"],
            [1700000060000, "100.5"]
        ]"#;
        let obs = parse_candles(body, &binance_cfg()).unwrap();
        assert_eq!(obs.len(), 5); // only the complete row survived
        assert!(obs.iter().all(|o| o.ts == 1_700_000_000));
    }

    #[test]
    fn rejects_invalid_body() {
        assert!(parse_candles("not json", &binance_cfg()).is_err());
    }
}
