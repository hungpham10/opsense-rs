//! GraphQL endpoint `/graphql` — Tầng 1 (pipeline/stations).
//!
//! Surface 3 nhóm tính năng:
//! 1. Xem pipeline   — `Query.status`, `Query.components`
//! 2. Attribute edit — `Query.attributes`, `Mutation.{set,remove}Attribute`
//! 3. Truy vấn timeseries — `Query.queryTimeseries`
//! 4. Dọn station — `Mutation.{clearStation,clearAllStations}`
//!
//! Mọi thay đổi pipeline đi qua `Mutation.reload(components)`. Nhưng reload nhận
//! **danh sách đầy đủ**, nên client phải đọc cấu hình hiện tại trước
//! (`Query.components`) — đọc rồi sửa thì không đoán. `Mutation.patchComponent`
//! là cách sửa một phần (thêm ở G2).
use opsense_model::events::Signal;

use crate::api::repl::pnl::{aggregate, dedup_orders, parse_bucket_secs, PnlBucket as InternalPnlBucket, PnlSummary as InternalPnlSummary};
use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, InputObject, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::Extension;
use axum::extract::State;
use opsense_core::Observation;
use opsense_core::TimeseriesStation;
use opsense_mlib::vector::runtime::Component;
use tokio::sync::RwLock;

use super::ReplHeaders;
use crate::api::{AppState, Node, Station, Status};

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(SimpleObject, Clone, Debug)]
pub struct EditResult {
    pub reloaded: bool,
    pub nodes: Vec<Node>,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct SetAttributeResult {
    pub ok: bool,
    /// True when `OPSENSE_ATTR_<NAME>` is also set (env wins on next lookup).
    pub env_override_active: bool,
}

/// Kết quả `clearStation` / `clearAllStations`.
///
/// `cleared` là **id đã xoá thật** — không đồng nghĩa "hết dữ liệu", mà là
/// station đã qua cả hai tầng (RAM + storage) không lỗi. `failed` giữ lại lỗi
/// của từng cái để một station hỏng không che mất phần đã xoá được.
///
/// Dùng lại [`crate::api::Station`] (id + kind) — **không** khai tuple
/// `(String, StationKind)` làm field: async-graphql không implement
/// `OutputType` cho tuple ⇒ E0277. Object có sẵn cùng shape với
/// `Status.stations` nên client parse một kiểu cho cả hai.
#[derive(SimpleObject, Clone, Debug)]
pub struct ClearStationResult {
    /// Station đã xoá thành công.
    pub cleared: Vec<Station>,
    /// `id: lý do` cho station không xoá được.
    pub failed: Vec<String>,
}

/// `ComponentInput` → `Arc<dyn Component>` qua typetag serde.
/// Nếu `type` không hợp lệ → GraphQL error (validate miễn phí).
#[derive(InputObject, Clone, Debug)]
pub struct ComponentInput {
    #[graphql(name = "type")]
    pub kind: String,
    pub id: String,
    pub config: Option<serde_json::Value>,
    pub inputs: Option<Vec<String>>,
}

/// Cấu hình **đang chạy** của một component (`Query.components`).
///
/// `config` là JSON typetag của component — cùng shape với `ComponentInput` nên
/// đọc xong có thể patch rồi `reload`, hoặc patch từng path (xem
/// `Mutation.patchComponent`) mà không phải dựng lại cả pipeline.
/// Trần của một lần query, do **client** (agent/CLI) đặt ra.
///
/// Cửa sổ không giới hạn đã từng treo server: quét toàn bộ lịch sử block là
/// hàng trăm triệu dòng. Giờ hỏi vượt trần thì **từ chối kèm gợi ý** thay vì clamp
/// im lặng — im lặng cũng là kiểu sai, chỉ chậm hơn.
pub const MAX_QUERY_ROWS: usize = 10_000;
/// 30 ngày. Station realtime không có dữ liệu cũ hơn vậy nên đây là trần an toàn.
pub const MAX_QUERY_WINDOW_SECS: i64 = 30 * 24 * 3600;

/// Kiểm tra trần của một lần query và **trả về** `limit` đã chuẩn hoá.
///
/// Tách riêng để test được không cần dựng server: đây là lớp chặn duy nhất giữa
/// agent và việc quét hàng trăm triệu block.
fn check_query_bounds(from: i64, to: i64, limit: Option<i64>) -> async_graphql::Result<usize> {
    let limit = match limit {
        None => 1000usize,
        Some(n) if n <= 0 => {
            return Err(async_graphql::Error::new("limit phải >= 1".to_string()));
        }
        Some(n) if n as usize > MAX_QUERY_ROWS => {
            return Err(async_graphql::Error::new(format!(
                "limit {n} vượt trần {MAX_QUERY_ROWS}; chia nhỏ cửa sổ thay vì lấy tất cả"
            )));
        }
        Some(n) => n as usize,
    };
    if to < from {
        return Err(async_graphql::Error::new(format!(
            "cửa sổ đảo ngược: from={from} > to={to}"
        )));
    }
    // `checked_sub`, KHÔNG dùng `to - from`: với from=i64::MIN, to=i64::MAX thì
    // phép trừ tràn — build debug panic, build release **wrap thành -1** khiến
    // điều kiện "vượt trần" sai thành false ⇒ guard bị bỏ qua đúng cái truy vấn
    // vô hạn mà nó sinh ra để chặn.
    let width = to.checked_sub(from).ok_or_else(|| {
        async_graphql::Error::new(format!(
            "cửa sổ {from}..{to} quá rộng để tính; hãy truyền from/to cụ thể"
        ))
    })?;
    if width > MAX_QUERY_WINDOW_SECS {
        return Err(async_graphql::Error::new(format!(
            "cửa sổ {width}s vượt trần {MAX_QUERY_WINDOW_SECS}s ({} ngày); chia làm nhiều lần gọi",
            MAX_QUERY_WINDOW_SECS / 86_400
        )));
    }
    Ok(limit)
}

/// Kết quả query có guard — luôn nói rõ có **bị cắt** không.
#[derive(SimpleObject, Clone, Debug)]
pub struct QueryResult {
    pub observations: Vec<Observation>,
    /// `true` = còn dữ liệu ngoài `limit` (client biết phải chia nhỏ cửa sổ).
    pub truncated: bool,
    /// Số dòng đã quét trong cửa sổ (trước khi lọc/cắt).
    pub scanned: usize,
    /// Đơn vị đã áp dụng: `"asc"` | `"desc"`.
    ///
    /// Luôn có mặt (kể cả khi caller không truyền `order`) vì đây là **hợp
    /// đồng** để agent không phải đoán: cùng một payload trả về theo hai chiều
    /// khác nhau thì cột `ts` tăng hay giảm chính là thứ phải nói ra.
    pub order: String,
}

/// PnL bucket — một khung thời gian trong aggregation.
#[derive(SimpleObject, Clone, Debug)]
pub struct PnlBucket {
    pub bucket_ts: i64,
    pub trades: i64,
    pub wins: i64,
    pub losses: i64,
    pub win_rate: f64,
    pub net_pnl_abs: f64,
    pub net_pnl_pct: f64,
    pub gross_profit_abs: f64,
    pub gross_loss_abs: f64,
    pub notional: f64,
    pub avg_win_pct: f64,
    pub avg_loss_pct: f64,
    pub long_trades: i64,
    pub short_trades: i64,
    pub open_count: i64,
    pub open_notional: f64,
}

/// Tổng PnL của toàn bộ cửa sổ + các bucket con.
#[derive(SimpleObject, Clone, Debug)]
pub struct PnlSummary {
    pub interval: String,
    pub bucket_secs: i64,
    pub from_ts: i64,
    pub to_ts: i64,
    pub complete: bool,
    pub trades: i64,
    pub zero_size_rows: i64,
    pub net_pnl_abs: f64,
    pub net_pnl_pct: f64,
    pub gross_profit_abs: f64,
    pub gross_loss_abs: f64,
    pub notional: f64,
    pub wins: i64,
    pub losses: i64,
    pub win_rate: f64,
    pub avg_win_pct: f64,
    pub avg_loss_pct: f64,
    pub long_trades: i64,
    pub short_trades: i64,
    pub open_count: i64,
    pub open_notional: f64,
    pub unrealized_abs: f64,
    pub mark_price: Option<f64>,
    pub total: PnlBucket,
    pub buckets: Vec<PnlBucket>,
}

/// Kết quả `orders` — giữ nguyên `observations` để backward-compatible,
/// thêm `pnl` khi có `interval`.
#[derive(SimpleObject, Clone, Debug)]
pub struct OrdersResult {
    pub observations: Vec<Observation>,
    pub truncated: bool,
    pub scanned: usize,
    pub order: String,
    /// Chỉ có giá trị khi caller truyền `interval`; null khi không có.
    pub pnl: Option<PnlSummary>,
}

/// Convert internal `pnl::PnlBucket` to GraphQL `PnlBucket`.
impl From<InternalPnlBucket> for PnlBucket {
    fn from(b: InternalPnlBucket) -> Self {
        Self {
            bucket_ts: b.bucket_ts,
            trades: b.trades as i64,
            wins: b.wins as i64,
            losses: b.losses as i64,
            win_rate: b.win_rate,
            net_pnl_abs: b.net_pnl_abs,
            net_pnl_pct: b.net_pnl_pct,
            gross_profit_abs: b.gross_profit_abs,
            gross_loss_abs: b.gross_loss_abs,
            notional: b.notional,
            avg_win_pct: b.avg_win_pct,
            avg_loss_pct: b.avg_loss_pct,
            long_trades: b.long_trades as i64,
            short_trades: b.short_trades as i64,
            open_count: b.open_count as i64,
            open_notional: b.open_notional,
        }
    }
}

/// Convert internal `pnl::PnlSummary` to GraphQL `PnlSummary`.
impl From<InternalPnlSummary> for PnlSummary {
    fn from(s: InternalPnlSummary) -> Self {
        Self {
            interval: s.interval,
            bucket_secs: s.bucket_secs,
            from_ts: s.from_ts,
            to_ts: s.to_ts,
            complete: s.complete,
            trades: s.trades as i64,
            zero_size_rows: s.zero_size_rows as i64,
            net_pnl_abs: s.net_pnl_abs,
            net_pnl_pct: s.net_pnl_pct,
            gross_profit_abs: s.gross_profit_abs,
            gross_loss_abs: s.gross_loss_abs,
            notional: s.notional,
            wins: s.wins as i64,
            losses: s.losses as i64,
            win_rate: s.win_rate,
            avg_win_pct: s.avg_win_pct,
            avg_loss_pct: s.avg_loss_pct,
            long_trades: s.long_trades as i64,
            short_trades: s.short_trades as i64,
            open_count: s.open_count as i64,
            open_notional: s.open_notional,
            unrealized_abs: s.unrealized_abs,
            mark_price: s.mark_price,
            total: s.total.into(),
            buckets: s.buckets.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(SimpleObject, Clone, Debug)]
pub struct ComponentConfig {
    pub id: String,

    #[graphql(name = "type")]
    pub kind: String,

    pub inputs: Vec<String>,

    /// Mô tả do pipeline khai. Lọc ở đây, không đọc trong `config`, vì `config`
    /// là JSON của **typed struct** — mà struct không có field này (xem
    /// [`crate::api::node_descriptions`]).
    pub description: Option<String>,

    /// Toàn bộ field của component dạng JSON (`script_path`, `params`, …).
    pub config: serde_json::Value,
}

/// Bọc JSON thô của `Runtime::components()` thành `ComponentConfig`.
///
/// `descriptions` lấy từ `Context`, không đọc được trong `value`: JSON này là
/// ảnh chụp của typed struct, mà struct cố tình **không** mang `description`
/// (`deny_unknown_fields` + `reload` đi vòng qua struct — xem
/// [`crate::api::node_descriptions`]).
fn component_config(
    value: serde_json::Value,
    descriptions: &std::collections::BTreeMap<String, String>,
) -> Option<ComponentConfig> {
    let obj = value.as_object()?;
    let id = obj.get("id")?.as_str()?.to_string();
    // `type` do typetag sinh khi serialize; `id` do runtime nhét thêm.
    let kind = obj
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let inputs = obj
        .get("inputs")
        .and_then(serde_json::Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(ComponentConfig {
        description: descriptions.get(&id).cloned(),
        id,
        kind,
        inputs,
        config: value,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Lấy `AppState` từ GraphQL context. Inject bởi [`graphql`] handler.
fn state<'a>(ctx: &'a Context<'_>) -> &'a AppState {
    ctx.data::<AppState>().expect("AppState not injected")
}

/// `ComponentInput` → `Arc<dyn Component>`. Build a JSON object with the
/// typetag-required `type` field, merge `config`, and deserialize via typetag.
fn parse_component(input: &ComponentInput) -> async_graphql::Result<Arc<dyn Component>> {
    let mut json = serde_json::Map::new();
    json.insert("type".into(), serde_json::Value::String(input.kind.clone()));
    json.insert("id".into(), serde_json::Value::String(input.id.clone()));
    if let Some(cfg) = &input.config
        && let Some(cfg_obj) = cfg.as_object()
    {
        for (k, v) in cfg_obj {
            json.insert(k.clone(), v.clone());
        }
    }
    if let Some(inputs) = &input.inputs {
        json.insert(
            "inputs".into(),
            serde_json::to_value(inputs).unwrap_or(serde_json::Value::Null),
        );
    }
    serde_json::from_value::<Box<dyn Component>>(serde_json::Value::Object(json))
        .map(Arc::from)
        .map_err(|e| async_graphql::Error::new(format!("component '{}': {}", input.id, e)))
}

/// JSON trả về từ `Runtime::components()` (typetag phẳng + `id`) → component.
///
/// Validate miễn phí: type lạ / field thiếu / sai kiểu đều lỗi ở đây, **trước**
/// khi chạm vào runtime.
fn parse_component_json(value: serde_json::Value) -> async_graphql::Result<Arc<dyn Component>> {
    let id = value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<no id>")
        .to_string();
    serde_json::from_value::<Box<dyn Component>>(value)
        .map(Arc::from)
        .map_err(|e| async_graphql::Error::new(format!("component '{id}': {e}")))
}

/// Đọc một field theo JSON pointer, trả `Null` nếu không có.
///
/// Tách ra khỏi `patch_component` vì đây chỗ **đã sai một lần rồi**: truyền
/// `path.trim_start_matches('/')` vào `pointer()` là RFC 6901 sai — pointer phải
/// giữ dấu `/` đầu, nên kết quả luôn `None` và audit ghi `from/to = null` cho
/// mọi patch. Có hàm riêng + test thì sai lần nữa sẽ đỏ ngay.
fn json_pointer_value(root: &serde_json::Value, path: &str) -> serde_json::Value {
    root.pointer(path)
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

/// Ghi `new_value` vào `path` (JSON pointer, vd `params.sl_pct`).
///
/// - Đường dẫn phải bắt đầu bằng `/` và có ít nhất 1 segment.
/// - Segment **cuối** được tạo mới nếu chưa có (thêm knob mới vào `params` là
///   việc hợp lệ); nhưng không tạo được cha — `params.a.b` khi `params.a` chưa có
///   thì lỗi, thay vì âm thầm tạo object rỗng.
fn patch_json_pointer(
    mut root: serde_json::Value,
    path: &str,
    new_value: serde_json::Value,
) -> async_graphql::Result<serde_json::Value> {
    if !path.starts_with('/') || path == "/" {
        return Err(async_graphql::Error::new(format!(
            "path phải là JSON pointer bắt đầu bằng '/', vd \"params.sl_pct\" (nhận `{path}`)"
        )));
    }
    let segments: Vec<String> = path
        .trim_start_matches('/')
        .split('/')
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
        .collect();
    let (last, parents) = segments.split_last().expect("path != \"/\" đã kiểm tra");

    let mut cursor = &mut root;
    for seg in parents {
        let next = cursor
            .as_object_mut()
            .and_then(|o| o.get_mut(seg))
            .ok_or_else(|| {
                async_graphql::Error::new(format!("path `{path}`: không có `{seg}` để đi tiếp"))
            })?;
        cursor = next;
    }
    let obj = cursor.as_object_mut().ok_or_else(|| {
        async_graphql::Error::new(format!("path `{path}`: cha không phải object"))
    })?;
    if let Some(existing) = obj.get_mut(last) {
        *existing = new_value;
    } else {
        obj.insert(last.clone(), new_value);
    }
    Ok(root)
}

/// True when `OPSENSE_ATTR_<NAME>` is set and non-empty.
fn env_attr_override(name: &str) -> bool {
    std::env::var(format!("OPSENSE_ATTR_{}", name.to_uppercase()))
        .ok()
        .filter(|v| !v.is_empty())
        .is_some()
}

// ─────────────────────────────────────────────────────────────────────────────
// Query root
// ─────────────────────────────────────────────────────────────────────────────

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Snapshot toàn bộ pipeline + station registry + config path.
    async fn status(&self, ctx: &Context<'_>) -> Status {
        state(ctx).status().await
    }

    /// Toàn bộ attributes trong memory.
    async fn attributes(&self, ctx: &Context<'_>) -> std::collections::BTreeMap<String, String> {
        state(ctx).attributes().await
    }

    /// Cấu hình đang chạy của component. Bỏ `id` → tất cả.
    ///
    /// Đường đọc chuẩn trước khi sửa: không có nó thì client buộc phải nhớ cấu
    /// hình cũ rồi gửi lại cả danh sách qua `Mutation.reload` — một node sót là
    /// mất node.
    async fn components(
        &self,
        ctx: &Context<'_>,
        id: Option<String>,
    ) -> async_graphql::Result<Vec<ComponentConfig>> {
        let s = state(ctx);
        let descriptions = s.context.node_descriptions().await;
        Ok(s
            .components(id.as_deref())
            .await
            .into_iter()
            .filter_map(|value| component_config(value, &descriptions))
            .collect())
    }

    /// Truy vấn 1 time series trong khoảng thời gian — **có guard**.
    ///
    /// - `limit` (mặc định 1000, tối đa [`MAX_QUERY_ROWS`]) và cửa sổ
    ///   (`from`/`to`, tối đa [`MAX_QUERY_WINDOW_SECS`]) bị từ chối nếu vượt trần,
    ///   kèm gợi ý cụ thể.
    /// - `signal` / `label_kind` / `status` lọc **server-side** (vd
    ///   `signal = "order"`, `label_kind = "trading_step"`): agent không phải
    ///   kéo 10k dòng về rồi tự lọc.
    /// - `label_kind` lọc `labels.kind`, còn `status` lọc `labels.status` — hai
    ///   label khác nhau. Lệnh giao dịch mang `status` (`open` / `closed`) và
    ///   **không** có `kind`, nên `labelKind: "closed"` luôn rỗng; muốn lệnh đã
    ///   đóng thì phải dùng `status: "closed"`.
    /// - `truncated` nói rõ còn dữ liệu ngoài `limit`.
    async fn query_timeseries(
        &self,
        ctx: &Context<'_>,
        node: String,
        from_ts: Option<i64>,
        to_ts: Option<i64>,
        limit: Option<i64>,
        signal: Option<String>,
        label_kind: Option<String>,
        status: Option<String>,
        order: Option<String>,
    ) -> async_graphql::Result<QueryResult> {
        let s = state(ctx);

        // Mặc định = cửa sổ tối đa cho phép, KHÔNG phải toàn bộ lịch sử — đây là
        // chỗ chặn sự cố "quét hết từ 0 tới MAX" từng treo server.
        let now = opsense_components::signal::now_secs();
        let to = to_ts.unwrap_or(now);
        let from = from_ts.unwrap_or(to - MAX_QUERY_WINDOW_SECS);
        let limit = check_query_bounds(from, to, limit)?;
        let order = parse_order(order.as_deref())?;

        let station = s
            .context
            .station::<Arc<RwLock<TimeseriesStation>>>(&node)
            .await
            .map_err(|e| {
                async_graphql::Error::new(format!("station '{node}' is not a timeseries: {e}"))
            })?;

        // `query_recent` (lenient) — KHÔNG dùng `query_range` (strict) ở đây.
        //
        // `query_range` coi "block không phủ hết cửa sổ yêu cầu" là lỗ hổng
        // phủ và trả `None` cho **cả** query. Điều đó sai với mọi stream sống:
        // block chỉ chứa obs tại những timestamp có thật, nên phần đuôi của
        // partition luôn "trống", và block đầu tiên trong cửa sổ luôn bắt đầu
        // sau `from_ts`. Đo được trên strategy binance (block 300s, candle 1m):
        //
        //     to = now-300 → 25 dòng      to = now-30 → 0 dòng
        //     to = now-120 → 40 dòng      to = now    → 0 dòng
        //
        // Tức là `to` mặc định (`now`) gần như luôn rỗng ⇒ `opsense query` /
        // `opsense orders` / MCP `opsense_query_timeseries` không thấy gì. Script
        // thì vẫn chạy vì đã dùng `query_recent` (`candles.rs`), nên triệu
        // chứng lệch: strategy có dữ liệu, API nói không.
        let rows = {
            let station = station.read().await;
            match station.query_recent(from, to).await {
                Some(rows) => rows,
                None => {
                    tracing::warn!(node = %node, "timeseries read returned nothing");
                    Vec::new()
                }
            }
        };

        let want_signal = match &signal {
            None => None,
            Some(raw) => Some(serde_json::from_value::<opsense_model::events::Signal>(
                serde_json::Value::String(raw.clone()),
            )
            .map_err(|e| {
                async_graphql::Error::new(format!("signal '{raw}' không hợp lệ: {e}"))
            })?),
        };
        let matched: Vec<&Observation> = rows
            .iter()
            .filter(|o| matches_filters(o, want_signal.as_ref(), label_kind.as_deref(), status.as_deref()))
            .collect();
        let matched: Vec<Observation> = matched.into_iter().cloned().collect();
        let (observations, truncated) = order_and_limit(&matched, limit, order == Order::Desc);
        Ok(QueryResult {
            order: order_name(order).to_string(),
            truncated,
            scanned: rows.len(),
            observations,
        })
    }

    /// Lệnh + cursor T+N của một station — parity với MCP `opsense_orders`.
    ///
    /// `status` lọc **sau** khi đã gộp theo `order_id` (xem bước 2 bên dưới):
    /// station append-only nên bản `open` vẫn còn sau khi lệnh đóng. Lọc ở
    /// server *trước* sẽ trả lệnh đã đóng là đang mở.
    ///
    /// Khi có `interval`: trả thêm `pnl` aggregation. `limit` chỉ cắt `observations`,
    /// `pnl` luôn tính trên toàn bộ cửa sổ sau khi dedup.
    async fn orders(
        &self,
        ctx: &Context<'_>,
        node: String,
        status: Option<String>,
        from_ts: Option<i64>,
        to_ts: Option<i64>,
        limit: Option<i64>,
        order: Option<String>,
        interval: Option<String>,    // NEW: "1m"|"5m"|"15m"|"30m"|"1h"|"4h"|"1d"|"1w"|"1M"|"0"
        mark_price: Option<f64>,     // NEW: giá để tính unrealized PnL
    ) -> async_graphql::Result<OrdersResult> {
        let s = state(ctx);
        let now = opsense_components::signal::now_secs();
        let to = to_ts.unwrap_or(now);
        let from = from_ts.unwrap_or(to - MAX_QUERY_WINDOW_SECS);
        let limit = check_query_bounds(from, to, limit)?;
        let order = parse_order(order.as_deref())?;

        let station = s
            .context
            .station::<Arc<RwLock<TimeseriesStation>>>(&node)
            .await
            .map_err(|e| {
                async_graphql::Error::new(format!("station '{node}' is not a timeseries: {e}"))
            })?;

        // Kiểm tra xem cửa sổ có bị cắt block không (để báo complete = false)
        let complete = station.read().await.window_fits_block_cap(from, to);

        let rows = {
            let station = station.read().await;
            match station.query_recent(from, to).await {
                Some(rows) => rows,
                None => {
                    tracing::warn!(node = %node, "timeseries read returned nothing");
                    Vec::new()
                }
            }
        };

        // 1. Giữ cursor T+N và lệnh, bỏ quan sát không liên quan.
        let kept: Vec<&Observation> = rows
            .iter()
            .filter(|o| {
                let kind = o.labels.get("kind").map(String::as_str);
                kind == Some("trading_step") || o.signal == Signal::Order
            })
            .collect();

        // 2. Dedup theo `order_id` (dùng hàm chung với aggregation).
        let deduped = dedup_orders(&kept);

        // 3. Lọc `status` **sau** khi đã gộp.
        let filtered: Vec<Observation> = if let Some(want) = &status {
            deduped
                .into_iter()
                .cloned()
                .filter(|o| {
                    if o.labels.get("kind").map(String::as_str) == Some("trading_step") {
                        return true;
                    }
                    o.labels.get("status").map(String::as_str) == Some(want.as_str())
                })
                .collect()
        } else {
            deduped.into_iter().cloned().collect()
        };

        // 4. PnL aggregation nếu có interval.
        let pnl = if let Some(interval_str) = interval {
            let bucket_secs = parse_bucket_secs(&interval_str).map_err(|e| {
                async_graphql::Error::new(e)
            })?;

            // Đọc fee_rate từ config của node (để không hardcode)
            let fee_roundtrip = s
                .components(Some(&node))
                .await
                .into_iter()
                .find_map(|c| c.get("config").and_then(|cfg| cfg.get("params")).and_then(|p| p.get("fee_rate")).and_then(|v| v.as_f64()))
                .map(|f| f * 2.0)
                .unwrap_or(0.0004);

            let pnl_summary = aggregate(
                &filtered.iter().collect::<Vec<_>>(),
                bucket_secs,
                mark_price,
                fee_roundtrip,
            );

            // Set complete flag từ station check
            let mut pnl_with_complete = pnl_summary;
            pnl_with_complete.complete = complete;

            Some(pnl_with_complete.into())
        } else {
            None
        };

        // 5. Cắt `observations` theo limit/order (backward-compatible).
        let (observations, truncated) = order_and_limit(&filtered, limit, order == Order::Desc);

        Ok(OrdersResult {
            order: order_name(order).to_string(),
            truncated,
            scanned: rows.len(),
            observations,
            pnl,
        })
    }
}

/// Ba bộ lọc server-side của `queryTimeseries`, tách ra thành hàm thuần để test
/// được — và để **tên tham số nói đúng điều nó lọc**.
///
/// `label_kind` lọc `labels.kind` còn `status` lọc `labels.status`: đây là hai
/// label khác nhau, và trước khi có tham số `status` thì cách duy nhất lấy lệnh
/// đã đóng là kéo hết về lọc tay. Lệnh giao dịch mang `status`
/// (`open`/`closed`) và **không** có `kind`, nên `labelKind: "closed"` luôn rỗng
/// — đã đo trên strategy binance.
fn matches_filters(
    o: &Observation,
    signal: Option<&Signal>,
    label_kind: Option<&str>,
    status: Option<&str>,
) -> bool {
    if !signal.is_none_or(|want| o.signal == *want) {
        return false;
    }
    if let Some(k) = label_kind
        && o.labels.get("kind").map(String::as_str) != Some(k)
    {
        return false;
    }
    if let Some(want) = status
        && o.labels.get("status").map(String::as_str) != Some(want)
    {
        return false;
    }
    true
}

/// Thứ tự trả về. Mặc định `desc` (mới nhất trước): người/agent hỏi "vừa xảy ra
/// gì" thì phải nhìn cuối cửa sổ, mà `take(limit)` trên dữ liệu tăng dần lại cắt
/// phần **cũ nhất** — sai đúng cái người ta cần xem nhất.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Order {
    Asc,
    Desc,
}

fn parse_order(raw: Option<&str>) -> async_graphql::Result<Order> {
    match raw.map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(Order::Desc),
        Some(v) if v.eq_ignore_ascii_case("asc") => Ok(Order::Asc),
        Some(v) if v.eq_ignore_ascii_case("desc") => Ok(Order::Desc),
        Some(v) => Err(async_graphql::Error::new(format!(
            "order '{v}' không hợp lệ; chỉ nhận \"asc\" hoặc \"desc\""
        ))),
    }
}

fn order_name(o: Order) -> &'static str {
    match o {
        Order::Asc => "asc",
        Order::Desc => "desc",
    }
}

/// Cắt + đảo chiều. `rows` từ `query_recent` **luôn** tăng dần theo `ts` — đó
/// là tiền đề để `desc` lấy đúng `limit` dòng *mới nhất*.
///
/// `truncated` tính trước khi cắt ⇒ đúng nghĩa "còn dữ liệu ngoài `limit`" ở cả
/// hai chiều, không phụ thuộc thứ tự cắt.
fn order_and_limit(rows: &[Observation], limit: usize, desc: bool) -> (Vec<Observation>, bool) {
    let truncated = rows.len() > limit;
    let out = if desc {
        let start = rows.len().saturating_sub(limit);
        rows[start..].iter().rev().cloned().collect()
    } else {
        rows[..rows.len().min(limit)].to_vec()
    };
    (out, truncated)
}

// ─────────────────────────────────────────────────────────────────────────────
// Mutation root
// ─────────────────────────────────────────────────────────────────────────────

/// Observation audit cho một lần sửa cấu hình.
///
/// Tách thành hàm thuần để test được: nội dung audit **phải** đủ để trả lời
/// "ai đổi gì, từ giá trị nào sang giá trị nào, lúc nào" mà không cần log file.
pub fn config_edit_observation(
    node: &str,
    path: &str,
    old: &serde_json::Value,
    new: &serde_json::Value,
    ts: i64,
) -> Observation {
    use opsense_model::events::{Signal, TelemetryKind};
    let mut o = Observation::new(
        ts,
        "config_edit".to_string(),
        TelemetryKind::Metric,
        Signal::Summary,
        1.0,
    );
    o.labels.insert("kind".into(), "config_edit".into());
    o.labels.insert("node".into(), node.to_string());
    o.labels.insert("path".into(), path.to_string());
    o.labels.insert("from".into(), old.to_string());
    o.labels.insert("to".into(), new.to_string());
    o
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Push full component list — add/update/remove đều qua đây.
    /// REPL client tính new list locally rồi call 1 lần.
    async fn reload(
        &self,
        ctx: &Context<'_>,
        components: Vec<ComponentInput>,
    ) -> async_graphql::Result<EditResult> {
        let s = state(ctx);
        let parsed: Vec<Arc<dyn Component>> = components
            .iter()
            .map(parse_component)
            .collect::<async_graphql::Result<Vec<_>>>()?;

        let runtime = s.runtime.write().await;
        runtime
            .reload(parsed)
            .map_err(|e| async_graphql::Error::new(format!("runtime.reload: {e}")))?;

        drop(runtime);
        let nodes = s.status().await.nodes;
        Ok(EditResult {
            reloaded: true,
            nodes,
        })
    }

    /// Ghi 1 thành phần của **một** node, không cần gửi lại cả pipeline.
    ///
    /// `path` là JSON pointer vào config đang chạy (vd `params.sl_pct`,
    /// `script_path`, `params.grid_min_trades`). Server đọc cấu hình hiện tại
    /// (nên không cần client tự dựng lại), patch, deserialize **toàn bộ** danh
    /// sách qua typetag, rồi mới reload — hỏng ở bước deserialize thì runtime
    /// giữ nguyên, không để lại pipeline nửa vời.
    ///
    /// `value` là JSON literal (`0.02`, `"rhai"`, `true`, `[1,2]`). Thay cả
    /// pipeline thì dùng `reload`.
    async fn patch_component(
        &self,
        ctx: &Context<'_>,
        id: String,
        path: String,
        value: String,
    ) -> async_graphql::Result<EditResult> {
        let s = state(ctx);
        let current = s.components(Some(&id)).await;
        let Some(target) = current.into_iter().next() else {
            return Err(async_graphql::Error::new(format!(
                "không có node '{id}' (xem Query.status để xem id hiện có)"
            )));
        };
        let new_value: serde_json::Value = serde_json::from_str(&value).map_err(|e| {
            async_graphql::Error::new(format!("`{value}` không phải JSON hợp lệ: {e}"))
        })?;
        let patched = patch_json_pointer(target.clone(), &path, new_value)?;
        if patched == target {
            return Ok(EditResult {
                reloaded: false,
                nodes: s.status().await.nodes,
            });
        }

        // Validate TRƯỚC khi chạm runtime: ghép node vừa patch với các node còn
        // lại (lấy lại toàn bộ để không mất node nào) rồi deserialize hết.
        let old_value = json_pointer_value(&target, &path);
        let new_value = json_pointer_value(&patched, &path);
        let mut all = s.components(None).await;
        let slot = all
            .iter_mut()
            .find(|c| c.get("id").and_then(serde_json::Value::as_str) == Some(id.as_str()))
            .ok_or_else(|| async_graphql::Error::new(format!("không có node '{id}'")))?;
        *slot = patched;
        let parsed = all
            .iter()
            .map(|c| parse_component_json(c.clone()))
            .collect::<async_graphql::Result<Vec<_>>>()?;

        let runtime = s.runtime.write().await;
        runtime
            .reload(parsed)
            .map_err(|e| async_graphql::Error::new(format!("runtime.reload: {e}")))?;
        drop(runtime);

        tracing::info!(node = %id, path = %path, "config patched qua GraphQL");
        s.audit(config_edit_observation(
            &id,
            path.trim_start_matches('/'),
            &old_value,
            &new_value,
            opsense_components::signal::now_secs(),
        ))
        .await;
        Ok(EditResult {
            reloaded: true,
            nodes: s.status().await.nodes,
        })
    }

    async fn set_attribute(        &self,
        ctx: &Context<'_>,
        name: String,
        value: String,
    ) -> async_graphql::Result<SetAttributeResult> {
        let s = state(ctx);
        s.set_attribute(name.clone(), value).await;
        Ok(SetAttributeResult {
            ok: true,
            env_override_active: env_attr_override(&name),
        })
    }

    async fn remove_attribute(
        &self,
        ctx: &Context<'_>,
        name: String,
    ) -> async_graphql::Result<bool> {
        Ok(state(ctx).remove_attribute(&name).await)
    }

    /// Xoá sạch **một** station (RAM + storage), vd `clearStation(id: "grid")`.
    ///
    /// Mất trọn state của station đó: lệnh đang mở, cursor T+N, plan lưới và
    /// lịch sử. Node sở hữu nó **vẫn chạy** và sẽ ghi lại từ dữ liệu mới tới —
    /// nên đây là "xoá sạch rồi để nó tự lấp lại", không phải xoá vĩnh viễn.
    ///
    /// Với station Redis/Valkey, khác restart: restart phải nạp lại từ đĩa,
    /// còn clear xoá luôn key nên không có gì để nạp lại.
    async fn clear_station(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> async_graphql::Result<ClearStationResult> {
        let kind = state(ctx).clear_station(&id).await.map_err(|e| {
            async_graphql::Error::new(e.to_string())
        })?;
        Ok(ClearStationResult {
            cleared: vec![Station { id, kind }],
            failed: Vec::new(),
        })
    }

    /// Xoá sạch **mọi** station đã đăng ký.
    ///
    /// Không báo lỗi nếu một station hỏng — `failed` liệt kê các cái đó còn
    /// phần xoá được vẫn nằm trong `cleared`. Xoá cả `grid` lẫn `tick-candle`
    /// là cách reset trọn phiên giao dịch: kernel đọc cursor T+N từ station,
    /// nên nếu chỉ xoá một trong hai thì lần chạy kế tiếp có thể tưởng đã đi
    /// tới nến hiện tại và bỏ qua phần lịch sử còn lại.
    async fn clear_all_stations(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<ClearStationResult> {
        let (cleared, failed) = state(ctx).clear_all_stations().await.map_err(|e| {
            async_graphql::Error::new(e.to_string())
        })?;
        Ok(ClearStationResult {
            cleared: cleared
                .into_iter()
                .map(|(id, kind)| Station { id, kind })
                .collect(),
            failed,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GraphQL handler
// ─────────────────────────────────────────────────────────────────────────────

pub async fn graphql(
    State(app_state): State<AppState>,
    ReplHeaders { tenant_id, .. }: ReplHeaders,
    Extension(schema): Extension<Arc<Schema<QueryRoot, MutationRoot, EmptySubscription>>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let mut req = req.into_inner();
    req = req.data(app_state);
    req = req.data(Into::<i64>::into(tenant_id));
    schema.execute(req).await.into()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use opsense_model::events::TelemetryKind;

    use super::*;

    fn schema() -> Schema<QueryRoot, MutationRoot, EmptySubscription> {
        Schema::build(QueryRoot, MutationRoot, EmptySubscription).finish()
    }

    /// Tầng 1 GraphQL surface: pipeline status, **component config**, attributes
    /// and timeseries queries must be present in the SDL.
    #[tokio::test]
    async fn tier1_schema_surface() {
        let sdl = schema().sdl();
        for op in [
            "status",
            "components",
            "attributes",
            "queryTimeseries",
            // `orders` có resolver từ trước nhưng thiếu trong danh sách này:
            // xoá resolver mà test vẫn xanh thì test không bảo vệ được gì.
            "orders",
            "reload",
            "patchComponent",
            "setAttribute",
            "removeAttribute",
            "clearStation",
            "clearAllStations",
        ] {
            assert!(
                sdl.contains(op),
                "schema missing `{op}`\n--- SDL ---\n{sdl}"
            );
        }
        // `order` là hợp đồng: client đọc nó để biết `ts` tăng hay giảm, nên
        // phải là `String!` (không nullable) — thiếu là mọi client cũ decode hỏng.
        assert!(
            sdl.contains("order: String!"),
            "QueryResult phải có `order: String!`\n--- SDL ---\n{sdl}"
        );
    }

    /// `ClearStationResult.cleared` **không được** là tuple.
    ///
    /// async-graphql không implement `OutputType` cho tuple, nên khai
    /// `Vec<(String, StationKind)>` là fail lúc build schema:
    /// E0277 "the trait bound `(String, StationKind): OutputType` is not
    /// satisfied". Test này đóng băng shape đúng để đổi ngược không sót.
    #[tokio::test]
    async fn clear_station_result_cleared_is_object_not_tuple() {
        let sdl = schema().sdl();
        assert!(
            sdl.contains("type ClearStationResult"),
            "thiếu ClearStationResult\n--- SDL ---\n{sdl}"
        );
        assert!(
            sdl.contains("type Station {"),
            "cleared phải dùng object `Station` sẵn có (id + kind)\n--- SDL ---\n{sdl}"
        );
        assert!(
            sdl.contains("cleared: [Station!]!"),
            "`cleared` phải là list Station\n--- SDL ---\n{sdl}"
        );
        // Tuple sẽ render thành `kind: StationKind!` trong một type vô danh;
        // object có `id` + `kind` thì có `id` trong SDL.
        assert!(
            sdl.contains("id: String!"),
            "Station phải có `id`\n--- SDL ---\n{sdl}"
        );
    }

    /// `Runtime::components()` trả JSON typetag + `id` nhét thêm; map sang
    /// `ComponentConfig` phải giữ nguyên `config` (đó là thứ client patch).
    #[test]
    fn component_config_maps_typed_json() {
        let raw = serde_json::json!({
            "type": "rhai_transform",
            "id": "grid",
            "inputs": ["clock", "history"],
            "script_path": "strategies/binance/grid.rhai",
            "params": { "strategy": "rhai", "mode": "analysis" },
        });
        let descriptions =
            BTreeMap::from([("grid".to_string(), "Lệnh + snapshot + plan".to_string())]);
        let cfg = component_config(raw.clone(), &descriptions).expect("map được");
        assert_eq!(cfg.id, "grid");
        // Mô tả đến từ `Context`, **không** phải từ JSON của typed struct.
        assert_eq!(cfg.description.as_deref(), Some("Lệnh + snapshot + plan"));
        assert_eq!(cfg.kind, "rhai_transform");
        assert_eq!(cfg.inputs, vec!["clock", "history"]);
        assert_eq!(cfg.config, raw, "config phải giữ nguyên JSON gốc");
        // `params` là thứ MCP/CLI đọc để sửa tiếp.
        assert_eq!(cfg.config["params"]["strategy"], "rhai");
    }

    /// `status` và `label_kind` lọc **hai label khác nhau** — đây là bẫy đã
    /// đo thật: lệnh giao dịch mang `labels.status` (`open`/`closed`) và không
    /// có `labels.kind`, nên `labelKind: "closed"` rỗng trong khi `status:
    /// "closed"` ra lệnh. Trước khi có tham số `status`, cách duy nhất là kéo
    /// hết về lọc tay — và `limit` cắt cụt **trước** khi lọc.
    #[test]
    fn status_and_label_kind_filter_different_labels() {
        let mut open = Observation::new(1, "BTCUSDT".into(), TelemetryKind::Metric, Signal::Order, 100.0);
        open.labels.insert("order_id".into(), "o1".into());
        open.labels.insert("status".into(), "open".into());

        let mut closed = open.clone();
        closed.ts = 2;
        closed.labels.insert("order_id".into(), "o1".into());
        closed.labels.insert("status".into(), "closed".into());

        let mut step = Observation::new(3, "BTCUSDT".into(), TelemetryKind::Metric, Signal::Summary, 3.0);
        step.labels.insert("kind".into(), "trading_step".into());

        // `status` lấy đúng lệnh đã đóng, và **không** lấy cursor.
        let hit: Vec<_> = [&open, &closed, &step]
            .into_iter()
            .filter(|o| super::matches_filters(o, Some(&Signal::Order), None, Some("closed")))
            .collect();
        assert_eq!(hit.len(), 1, "phải ra đúng 1 lệnh đóng");
        assert_eq!(hit[0].labels.get("status").map(String::as_str), Some("closed"));

        // `labelKind: "closed"` rỗng — vì lệnh không có label `kind`.
        let miss: Vec<_> = [&open, &closed]
            .into_iter()
            .filter(|o| super::matches_filters(o, Some(&Signal::Order), Some("closed"), None))
            .collect();
        assert!(miss.is_empty(), "labelKind không được thấy lệnh");

        // `labelKind` vẫn lấy được cursor như trước.
        let cursor: Vec<_> = [&step]
            .into_iter()
            .filter(|o| super::matches_filters(o, None, Some("trading_step"), None))
            .collect();
        assert_eq!(cursor.len(), 1);

        // Không truyền bộ lọc nào thì giữ tất cả.
        assert!(super::matches_filters(&open, None, None, None));
    }

    /// Node không khai `description` → `None`, **không** phải chuỗi rỗng: MCP
    /// cần phân biệt "không ai mô tả" với "mô tả rỗng".
    #[test]
    fn component_config_description_absent_is_none() {
        let empty = BTreeMap::new();
        let cfg = component_config(
            serde_json::json!({ "id": "clock", "type": "clock" }),
            &empty,
        )
        .expect("map được");
        assert_eq!(cfg.description, None);
    }

    /// JSON thiếu `id` (component lỗi) → bỏ qua chứ không làm hỏng cả query.
    #[test]
    fn component_config_skips_malformed_entries() {
        let empty = BTreeMap::new();
        assert!(component_config(serde_json::json!({}), &empty).is_none());
        assert!(component_config(serde_json::json!({ "id": 7 }), &empty).is_none());
        // Không có `inputs` → rỗng, không panic.
        let cfg = component_config(serde_json::json!({ "id": "x", "type": "clock" }), &empty)
            .expect("map được");
        assert!(cfg.inputs.is_empty());
    }

    /// `patchComponent` chỉ được đổi **đúng một** chỗ, và phải tạo được knob mới
    /// (thêm key vào `params` là việc hợp lệ) nhưng không tự chế ra object cha.
    #[test]
    fn patch_json_pointer_touches_one_place() {
        let base = || {
            serde_json::json!({
                "type": "rhai_transform",
                "id": "grid",
                "script_path": "strategies/binance/grid.rhai",
                "params": { "strategy": "rhai", "sl_pct": 0.008 },
            })
        };

        // 1. Sửa giá trị có sẵn, phần còn lại giữ nguyên.
        let out = patch_json_pointer(base(), "/params/sl_pct", serde_json::json!(0.02)).unwrap();
        assert_eq!(out["params"]["sl_pct"], serde_json::json!(0.02));
        assert_eq!(out["params"]["strategy"], "rhai");
        assert_eq!(out["script_path"], "strategies/binance/grid.rhai");

        // 2. Thêm knob mới vào `params`, các key cũ giữ nguyên.
        let out = patch_json_pointer(
            base(),
            "/params/grid_min_trades",
            serde_json::json!(5),
        )
        .unwrap();
        assert_eq!(out["params"]["grid_min_trades"], serde_json::json!(5));
        assert_eq!(out["params"]["strategy"], "rhai");
        assert_eq!(out["params"]["sl_pct"], serde_json::json!(0.008));

        // 3. Path sai cú pháp / không có cha → lỗi, không im lặng bỏ qua.
        let err = patch_json_pointer(base(), "params.sl_pct", serde_json::json!(0.02))
            .unwrap_err();
        let err = err.message.clone();
        assert!(err.contains("JSON pointer"), "{err}");
        let err = patch_json_pointer(base(), "/params/a/b", serde_json::json!(1))
            .unwrap_err();
        let err = err.message.clone();
        assert!(err.contains("không có"), "{err}");
        // Dấu chấm là ký tự thường trong JSON pointer: `/params.a.b` là MỘT key
        // tên `params.a.b`, không phải hai tầng — giữ đúng chuẩn RFC 6901.
        let out = patch_json_pointer(base(), "/params.a.b", serde_json::json!(1)).unwrap();
        assert_eq!(out["params.a.b"], serde_json::json!(1));
    }

    /// Guard của query là hàng phòng thủ số 1: cửa sổ vô hạn / `limit` vô hạn là
    /// cách chắc chắn nhất để làm treo server, nên phải **từ chối kèm gợi ý**,
    /// không clamp im lặng (im lặng cũng sai, chỉ chậm hơn).
    #[test]
    fn query_bounds_reject_unbounded_reads() {
        let now = 1_800_000_000i64;
        let day = 86_400;

        // Mặc định: 1000 dòng.
        assert_eq!(check_query_bounds(now - day, now, None).unwrap(), 1000);
        // Trong trần thì lấy đúng yêu cầu.
        assert_eq!(check_query_bounds(now - day, now, Some(500)).unwrap(), 500);
        assert_eq!(
            check_query_bounds(now - day, now, Some(MAX_QUERY_ROWS as i64)).unwrap(),
            MAX_QUERY_ROWS
        );

        // Cửa sổ vô hạn (kiểu `from=0, to=MAX`) phải bị chặn.
        let err = check_query_bounds(0, i64::MAX, None).unwrap_err();
        let err = err.message.clone();
        assert!(err.contains("vượt trần"), "{err}");
        assert!(err.contains("chia"), "phải gợi ý cách chia: {err}");

        // `from=i64::MIN, to=i64::MAX` làm phép trừ TRÀN: debug panic, release
        // wrap thành số âm ⇒ guard im lặng bị bỏ qua. Phải bị chặn như trên.
        let err = check_query_bounds(i64::MIN, i64::MAX, None).unwrap_err();
        let err = err.message.clone();
        assert!(
            err.contains("quá rộng để tính") || err.contains("vượt trần"),
            "cửa sổ tràn số phải bị chặn, không được lọt: {err}"
        );

        // Cửa sổ đảo ngược.
        let err = check_query_bounds(now, now - day, None).unwrap_err();
        assert!(err.message.contains("đảo ngược"), "{}", err.message);

        // limit vô hạn / 0.
        let err = check_query_bounds(now - day, now, Some(100_000)).unwrap_err();
        assert!(err.message.contains("limit"), "{}", err.message);
        let err = check_query_bounds(now - day, now, Some(0)).unwrap_err();
        assert!(err.message.contains(">= 1"), "{}", err.message);
    }

    /// Hồi quy: `json_pointer_value` phải đọc **đúng** field mà `patch_json_pointer`
    /// vừa ghi, và giá trị đó phải đi vào audit.
    ///
    /// Trước đây `patch_component` gọi `pointer(path.trim_start_matches('/'))` ⇒
    /// `None` cho **mọi** path ⇒ `old_value`/`new_value` luôn `Null` ⇒ audit ghi
    /// `from: null, to: null` cho mọi patch. Test cũ ở trên vẫn xanh vì nó chỉ
    /// kiểm tra `config_edit_observation` với giá trị truyền vào, không kiểm tra
    /// hai giá trị đó lấy từ đâu — và `integration_mcp_config` chết sớm ở
    /// "Station not found" nên không ai thấy.
    #[test]
    fn json_pointer_reads_what_patch_wrote() {
        let target = serde_json::json!({
            "id": "grid",
            "kind": "rhai_transform",
            "params": {"sl_pct": 0.008, "strategy": "grid"},
        });
        let patched =
            patch_json_pointer(target.clone(), "/params/sl_pct", serde_json::json!(0.02)).unwrap();

        // Giá trị cũ lấy từ node **trước** khi patch.
        let old = json_pointer_value(&target, "/params/sl_pct");
        assert_eq!(old, serde_json::json!(0.008), "old phải là 0.008, không phải null");

        // Giá trị mới lấy từ node **sau** khi patch.
        let new = json_pointer_value(&patched, "/params/sl_pct");
        assert_eq!(new, serde_json::json!(0.02), "new phải là 0.02, không phải null");

        // Và hai giá trị đó phải ra đúng label audit.
        let obs = config_edit_observation("grid", "params/sl_pct", &old, &new, 1_800_000_000);
        assert_eq!(obs.labels.get("from").map(String::as_str), Some("0.008"));
        assert_eq!(obs.labels.get("to").map(String::as_str), Some("0.02"));

        // Knob chưa tồn tại: `old` là null (thêm knob mới là hợp lệ), `new` có giá trị.
        let added =
            patch_json_pointer(target.clone(), "/params/new_knob", serde_json::json!(7)).unwrap();
        assert_eq!(
            json_pointer_value(&target, "/params/new_knob"),
            serde_json::Value::Null
        );
        assert_eq!(json_pointer_value(&added, "/params/new_knob"), serde_json::json!(7));
    }

    /// Audit phải đủ để trả lời "đổi gì, từ giá trị nào sang nào, ở node nào,
    /// lúc nào" — và phải lọc được bằng `labels.kind` qua đúng đường query.
    #[test]
    fn config_edit_observation_is_queryable() {
        let obs = config_edit_observation(
            "grid",
            "params/sl_pct",
            &serde_json::json!(0.008),
            &serde_json::json!(0.02),
            1_800_000_000,
        );
        assert_eq!(obs.ts, 1_800_000_000);
        assert_eq!(obs.metric_id, "config_edit");
        assert_eq!(obs.labels.get("kind").map(String::as_str), Some("config_edit"));
        assert_eq!(obs.labels.get("node").map(String::as_str), Some("grid"));
        assert_eq!(obs.labels.get("path").map(String::as_str), Some("params/sl_pct"));
        assert_eq!(obs.labels.get("from").map(String::as_str), Some("0.008"));
        assert_eq!(obs.labels.get("to").map(String::as_str), Some("0.02"));
    }

    /// Giá trị sai kiểu phải chết ở deserialize (typetag), **trước** khi runtime
    /// bị đụng tới — đây là chốt chặn "patch làm hỏng cả pipeline".
    #[test]
    fn parse_component_json_rejects_broken_config() {
        // Component không tồn tại trong inventory.
        let err = parse_component_json(serde_json::json!({
            "type": "khong_ton_tai", "id": "x"
        }))
        .expect_err("type lạ phải lỗi");
        assert!(
            err.message.contains("khong_ton_tai"),
            "{}",
            err.message
        );

        // Sai kiểu field: `interval_secs` của clock phải là số.
        let err = parse_component_json(serde_json::json!({
            "type": "clock", "id": "c", "interval_secs": "khong-phai-so"
        }))
        .expect_err("sai kiểu phải lỗi");
        assert!(err.message.contains('c'), "{}", err.message);

        // Hợp lệ thì deserialize được.
        parse_component_json(serde_json::json!({
            "type": "clock", "id": "c", "interval_secs": 10
        }))
        .expect("clock hợp lệ phải parse được");
    }
}

#[cfg(test)]
mod order_tests {
    use super::*;
    use opsense_model::events::TelemetryKind;

    fn rows(n: i64) -> Vec<Observation> {
        (1..=n)
            .map(|i| Observation {
                ts: i,
                metric_id: "m".into(),
                value: 0.0,
                labels: Default::default(),
                // `TelemetryKind`/`Signal` **không** impl `Default` — dựng
                // bằng biến thể thật thay vì `Default::default()`.
                kind: TelemetryKind::Metric,
                signal: Signal::Order,
                severity: None,
            })
            .collect()
    }

    fn tss(out: &[Observation]) -> Vec<i64> {
        out.iter().map(|o| o.ts).collect()
    }

    #[test]
    fn parse_order_defaults_to_desc_and_is_case_insensitive() {
        assert_eq!(parse_order(None).unwrap(), Order::Desc);
        assert_eq!(parse_order(Some("")).unwrap(), Order::Desc);
        assert_eq!(parse_order(Some("DESC")).unwrap(), Order::Desc);
        assert_eq!(parse_order(Some(" asc ")).unwrap(), Order::Asc);
        let err = parse_order(Some("descending")).expect_err("phải từ chối");
        let msg = err.message;
        assert!(msg.contains("asc") && msg.contains("desc"), "{msg}");
    }

    /// `desc` + `limit` phải lấy dòng **mới nhất**: đây là cả lý do `order` tồn
    /// tại — `take(limit)` trên dữ liệu tăng dần cắt phần cũ nhất.
    #[test]
    fn order_and_limit_takes_newest_when_desc() {
        let r = rows(5);
        let (out, trunc) = order_and_limit(&r, 2, true);
        assert_eq!(tss(&out), vec![5, 4]);
        assert!(trunc);
        let (out, trunc) = order_and_limit(&r, 9, true);
        assert_eq!(tss(&out), vec![5, 4, 3, 2, 1]);
        assert!(!trunc);
    }

    #[test]
    fn order_and_limit_takes_oldest_when_asc() {
        let r = rows(5);
        let (out, trunc) = order_and_limit(&r, 2, false);
        assert_eq!(tss(&out), vec![1, 2]);
        assert!(trunc);
        let (out, trunc) = order_and_limit(&r, 9, false);
        assert_eq!(tss(&out), vec![1, 2, 3, 4, 5]);
        assert!(!trunc);
    }

    #[test]
    fn order_and_limit_handles_empty_rows() {
        let (out, trunc) = order_and_limit(&[], 10, true);
        assert!(out.is_empty());
        assert!(!trunc);
        let (out, trunc) = order_and_limit(&[], 10, false);
        assert!(out.is_empty());
        assert!(!trunc);
    }
}
