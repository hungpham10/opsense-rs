//! HTTP source node: fetch a URL on every tick, parse the response body as
//! observations, write them to the node's own `Timeseries` station, then
//! forward `data_ready(ts)` downstream.
//!
//! Request is fully template-driven via `{{name}}` placeholders that resolve
//! at call time through a 3-layer lookup (plan B.2):
//!
//! 1. Bound variable — output of any `bindings` entry, evaluated by
//!    [`opsense_libs::jq::JsonQuery`] against a context object
//!    `{"ts", "interval", "now", "payload", "attributes"}`.
//! 2. Field in the upstream message payload (so a clock/publisher can pass
//!    values in-band).
//! 3. [`Context::variable`] — looks up `attributes` (TOML + env) then the
//!    secret store, parsing to `String`.
//!
//! Response body is expected to be JSON either an array of observation
//! objects, or a single observation object (wrapped into a 1-element vec).
//! Mapping/extraction (`items`/`fields`/`constants`) is intentionally out of
//! scope for this pass — a separate extractor node can reshape data when
//! needed.

use std::collections::{BTreeMap, HashMap};
use std::io::Error;
use std::sync::OnceLock;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc;

use opsense_core::Context;
use opsense_core::Observation;
use opsense_core::Station;
use opsense_core::TimeseriesStation;
use opsense_libs::jq::JsonQuery;
use opsense_macros::transform;

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
        }
    }
}

/// One shared client per timeout so poll-cadence calls reuse connections.
fn client_for(timeout_secs: u64) -> Result<reqwest::Client, String> {
    static CLIENTS: OnceLock<std::sync::Mutex<HashMap<u64, reqwest::Client>>> = OnceLock::new();
    let clients = CLIENTS.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut guard = clients.lock().unwrap();

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
async fn build_vars(
    ctx: &Context,
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
        if let Ok(v) = ctx.variable::<String>(&name).await {
            vars.insert(name, v);
        }
    }
    vars
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

impl_http_source!(
    async fn run(
        &self,
        _id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error> {
        let ctx = downcast_ctx(&tx)?;

        // Pre-parse every binding once; failures here are config errors so
        // surface them eagerly.
        let mut parsed: Vec<(String, JsonQuery)> = Vec::with_capacity(self.bindings.len());
        for (name, expr) in &self.bindings {
            let q = JsonQuery::parse(expr)
                .map_err(|e| Error::other(format!("http {} binding `{name}`: {e}", self.id)))?;
            parsed.push((name.clone(), q));
        }

        // Register the station eagerly so reads before the first cycle still
        // resolve to an empty timeseries rather than a `NotFound` error.
        if self.station {
            ctx.registry(
                &self.id,
                Station::Timeseries(std::sync::Arc::new(tokio::sync::RwLock::new(
                    TimeseriesStation::default(),
                ))),
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

            // 1. evaluate bindings against the per-cycle context.
            let ctx_value = serde_json::json!({
                "ts": ts,
                "interval": interval,
                "now": signal::now_secs(),
                "payload": msg.payload,
            });
            let mut bound = BTreeMap::new();
            for (name, q) in &parsed {
                let result = q.execute(&ctx_value);
                let first = result.into_iter().next().unwrap_or(Value::Null);
                bound.insert(name.clone(), value_to_string(&first));
            }

            // 2. merge layer 2/3 to build the final lookup table.
            let vars = build_vars(ctx, bound, &msg.payload).await;

            // 3. render url, headers, body.
            let url = match render(&self.url, &vars) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("http {} url render: {e}", self.id);
                    continue;
                }
            };
            let method_str = self.method.trim().to_ascii_uppercase();
            let method = match reqwest::Method::from_bytes(method_str.as_bytes()) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("http {} invalid method `{}`: {e}", self.id, self.method);
                    continue;
                }
            };
            let client = match client_for(self.timeout_secs) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("http {} client: {e}", self.id);
                    continue;
                }
            };
            let mut request = client.request(method, &url);
            for (k, v) in &self.headers {
                match render(v, &vars) {
                    Ok(s) => {
                        request = request.header(k, s);
                    }
                    Err(e) => {
                        tracing::warn!("http {} header `{k}` render: {e}", self.id);
                    }
                }
            }
            if let Some(body) = &self.body {
                match render(body, &vars) {
                    Ok(s) => {
                        request = request.body(s);
                    }
                    Err(e) => {
                        tracing::warn!("http {} body render: {e}", self.id);
                    }
                }
            }

            // 4. fetch + parse.
            let response = match request.send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("http {} request: {e}", self.id);
                    continue;
                }
            };
            if !response.status().is_success() {
                tracing::warn!("http {} endpoint answered {}", self.id, response.status());
                continue;
            }
            let body_text = match response.text().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!("http {} body: {e}", self.id);
                    continue;
                }
            };
            let batch = match parse_observations(&body_text) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("http {} parse: {e}", self.id);
                    continue;
                }
            };

            // 5. write to station.
            if let Some(me) = &me_handle
                && !batch.is_empty()
            {
                let from = batch.iter().map(|o| o.ts).min().unwrap_or(ts);
                let to = batch.iter().map(|o| o.ts).max().unwrap_or(ts);
                me.write().await.update_range(&batch, from, to, to);
            }

            // 6. forward so downstream nodes see this cycle.
            let ready = signal::tagged(signal::data_ready(ts), &self.id);
            for s in &tx.streams {
                let _ = s.send(ready.clone()).await;
            }
        }
        Ok(())
    }
);
