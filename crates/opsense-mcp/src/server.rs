//! rmcp-based MCP server exposing the four Opsense tools over stdio.
//!
//! Built on the official Rust SDK (`rmcp`): tool signatures/schemas are
//! generated from the `#[tool]` methods. Swapping to a network transport later
//! only means replacing `stdio()` with e.g. the streamable-http server
//! transport — the handler stays unchanged.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, ErrorData};
use rmcp::transport::stdio;
use rmcp::{schemars, tool, tool_handler, tool_router, ServerHandler, ServiceExt};
use tokio::sync::RwLock;

use opsense_components::signal;
use opsense_core::registry::{describe_station, station, station_ids, text_index};
use opsense_core::{Observation, Stage};

use crate::{backfill, deinit, edit, init, query_timeout, run_pipeline, status, Session};

pub struct OpsenseMcp {
    session: Arc<RwLock<Option<Session>>>,
    /// Analysis-kernel session (checklist §10): MCP chỉ gọi Session/Kernel
    /// API qua trait KernelBackend — không bao giờ đụng runner IPC trực tiếp.
    kernel: tokio::sync::Mutex<Option<KernelSession>>,
}

struct KernelSession {
    manager: std::sync::Arc<opsense_session::SessionManager>,
    session: std::sync::Arc<opsense_session::Session>,
}

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct InitParams {
    /// Đường dẫn file config TOML (mặc định: .opsense/config.toml của thư mục hiện hành).
    pub path: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct EditParams {
    /// Danh sách ĐẦY ĐỦ component tables mong muốn, ví dụ:
    /// [{"type":"clock_source","id":"clock","interval_secs":30},
    ///  {"type":"collector_sink","id":"collector","inputs":["clock"]}]
    pub components: Vec<serde_json::Value>,
}

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct RunParams {
    /// Node nhận tín hiệu `tick` (mặc định: `ingest`). Ví dụ pipeline
    /// playground: ingest → plugin → persist.
    pub node: Option<String>,
    /// Timestamp đưa vào tick, unix seconds (mặc định: hiện tại).
    pub ts: Option<i64>,
}

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct QueryParams {
    /// Nguồn đọc: id của station/node đã đăng ký (mỗi node sinh dữ liệu tự có
    /// trạm riêng). Tầng persistence đã bị xoá — chỉ truyền station/node id.
    pub source: Option<String>,
    /// Stage cần đọc: "raw" hoặc "processed" (mặc định).
    pub stage: Option<String>,
    /// Chỉ lấy metric này khi có (mặc định: tất cả).
    pub metric: Option<String>,
    /// Cửa sổ (from_ts, to_ts] theo unix seconds; `to_ts` mặc định là hiện
    /// tại, `from_ts` mặc định là 24 giờ trước.
    pub from_ts: Option<i64>,
    pub to_ts: Option<i64>,
}

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct DescribeParams {
    /// Id của station cần mô tả; bỏ trống để lấy danh sách tất cả station.
    pub id: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct PatternParams {
    /// Node id của pattern transform (VD "log-matcher").
    pub node: String,
    /// Text/pattern tuỳ thao tác.
    pub text: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct CatalogSearchParams {
    /// Node id của catalog transform.
    pub node: String,
    /// Substring cần tìm trong keys.
    pub pattern: String,
}

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct CatalogListParams {
    /// Node id của catalog transform / source (station kind `category`).
    pub node: String,
    /// Substring tùy chọn: chỉ trả key chứa pattern này.
    pub pattern: Option<String>,
    /// Số entry tối đa trả về (default 100, tối đa 1000).
    pub limit: Option<usize>,
    /// Vị trí bắt đầu trang (default 0).
    pub offset: Option<usize>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct BackfillParams {
    /// Node http_source cần re-fetch (vd "vms-disk-usage").
    pub node: String,
    /// Đầu cửa sổ cũ (unix giây).
    pub from_ts: i64,
    /// Cuối cửa sổ cũ (unix giây].
    pub to_ts: i64,
}

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct KernelRunParams {
    /// Code Python chạy trong analysis kernel (biến `result` được capture về).
    pub code: String,
    /// Config TOML cho session (mặc định .opsense/config.toml); kernel chọn
    /// bằng env OPSENSE_KERNEL_BIN.
    pub path: Option<String>,
}

fn ok_text(value: &serde_json::Value) -> Result<CallToolResult, ErrorData> {
    Ok(CallToolResult::success(vec![ContentBlock::text(
        value.to_string(),
    )]))
}

fn err_text(message: String) -> Result<CallToolResult, ErrorData> {
    Ok(CallToolResult::error(vec![ContentBlock::text(message)]))
}

#[tool_router]
impl OpsenseMcp {
    #[must_use]
    pub fn new() -> Self {
        Self {
            session: Arc::new(RwLock::new(None)),
            kernel: tokio::sync::Mutex::new(None),
        }
    }

    /// Shared dispatch for every tool that just needs the open pipeline
    /// session: one place owns the lock pattern and the None → error mapping;
    /// tools only supply the operation returning JSON or an error string.
    async fn with_session(
        &self,
        op: impl for<'a> FnOnce(
            &'a Session,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
        >,
    ) -> Result<CallToolResult, ErrorData> {
        let guard = self.session.read().await;
        match guard.as_ref() {
            Some(opened) => match op(opened).await {
                Ok(value) => ok_text(&value),
                Err(message) => err_text(message),
            },
            None => err_text("no open session; call opsense_init first".into()),
        }
    }

    /// Lazily open (or reuse) one analysis-kernel session.
    async fn kernel_session(
        &self,
        path: Option<&str>,
    ) -> Result<std::sync::Arc<opsense_session::Session>, String> {
        let mut guard = self.kernel.lock().await;
        if let Some(existing) = guard.as_ref() {
            return Ok(existing.session.clone());
        }
        let path = path.unwrap_or(".opsense/config.toml");
        let config = opsense_core::config::Config::load(std::path::Path::new(path))
            .map_err(|e| format!("load config `{path}`: {e}"))?;
        let manager = opsense_session::init_session_manager(&config);
        let session = manager
            .create_session()
            .map_err(|e| format!("open kernel session: {e:#}"))?;
        *guard = Some(KernelSession {
            manager: manager.clone(),
            session: session.clone(),
        });
        Ok(session)
    }

    #[tool(
        description = "Chạy Python code trong analysis kernel (process riêng, IPC framed; \
                          backend local hoặc runner qua env OPSENSE_KERNEL_BIN). Biến `result` \
                          được capture: DataFrame -> rows/cols/columns, giá trị khác -> text. \
                          Cần mở trước bằng opsense_init để có file config."
    )]
    async fn opsense_kernel_run(
        &self,
        Parameters(params): Parameters<KernelRunParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let session = match self.kernel_session(params.path.as_deref()).await {
            Ok(s) => s,
            Err(message) => return err_text(message),
        };
        // No datasets pushed here; host-side state stays intact.
        let result = session
            .execute_with(&params.code, std::collections::HashMap::new())
            .await;
        match result {
            Ok(out) => ok_text(&serde_json::json!({
                "ok": out.ok(),
                "text": out.text,
                "stdout": out.stdout,
                "error": out.error,
                "dataframe": out.dataframe.map(|rb| serde_json::json!({
                    "rows": rb.num_rows(),
                    "cols": rb.num_columns(),
                })),
            })),
            Err(e) => err_text(format!("{e:#}")),
        }
    }

    #[tool(
        description = "Health của execution backend hiện tại của analysis kernel \
                          (local-IPC hoặc grpc-runner)."
    )]
    async fn opsense_kernel_health(&self) -> Result<CallToolResult, ErrorData> {
        let guard = self.kernel.lock().await;
        let Some(opened) = guard.as_ref() else {
            return err_text("no kernel session; call opsense_kernel_run first".into());
        };
        let backend = opened.session.backend();
        match opened.manager.block_on(backend.health()) {
            Ok(info) => ok_text(&serde_json::json!({
                "name": info.name, "ok": info.ok, "detail": info.detail,
                "packages": info.packages,
            })),
            Err(e) => err_text(format!("{e:#}")),
        }
    }

    #[tool(
        description = "Mở session Opsense mới: load file config (mặc định .opsense/config.toml \
                          của thư mục hiện hành), deserialize [pipeline].components và khởi động \
                          pipeline qua vector Runtime. Chỉ MỘT session duy nhất cho mỗi thư mục \
                          .opsense (khoá qua session.lock; lock bỏ quên của process đã chết tự thu hồi)."
    )]
    async fn opsense_init(
        &self,
        Parameters(params): Parameters<InitParams>,
    ) -> Result<CallToolResult, ErrorData> {
        if self.session.read().await.is_some() {
            return err_text("session already open; call opsense_deinit first".into());
        }
        let path = params.path.as_deref().unwrap_or(".opsense/config.toml");
        match init(std::path::Path::new(path)).await {
            Ok((opened, summary)) => {
                // init() already produced the status summary — no second
                // round trip through topology/stations/events here.
                *self.session.write().await = Some(opened);
                ok_text(&summary)
            }
            Err(message) => err_text(message),
        }
    }

    #[tool(description = "Đóng session Opsense: dừng pipeline và giải phóng runtime.")]
    async fn opsense_deinit(&self) -> Result<CallToolResult, ErrorData> {
        match self.session.write().await.take() {
            Some(opened) => match deinit(&opened).await {
                Ok(message) => ok_text(&serde_json::json!(message)),
                Err(message) => err_text(message),
            },
            None => err_text("no open session".into()),
        }
    }

    #[tool(
        description = "Trạng thái từng node trong pipeline: loại node, liên kết \
                          input/output, đang chạy hay không, và các sự kiện runtime gần nhất."
    )]
    async fn opsense_status(&self) -> Result<CallToolResult, ErrorData> {
        self.with_session(|opened| Box::pin(status(opened))).await
    }

    #[tool(
        description = "Chỉnh sửa pipeline realtime: truyền DANH SÁCH ĐẦY ĐỦ các node mong muốn \
                          (thêm/sửa/xoá node và liên kết đều qua đó). Runtime tự diff, validate liên kết \
                          và báo lỗi dead node nếu đồ thị hỏng."
    )]
    async fn opsense_edit(
        &self,
        Parameters(params): Parameters<EditParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.with_session(|opened| Box::pin(edit(opened, params.components)))
            .await
    }

    #[tool(
        description = "Trigger thủ công (retest): bơm tín hiệu tick(ts) vào một node của \
                          pipeline đang chạy — mặc định vào node `ingest`, ts = hiện tại. Dùng để \
                          chạy pipeline theo yêu cầu, ví dụ sau khi nạp lại plugin mới qua \
                          opsense_edit. Kết quả xem bằng opsense_query / opsense_status."
    )]
    async fn opsense_run(
        &self,
        Parameters(params): Parameters<RunParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.with_session(|opened| Box::pin(run_pipeline(opened, params.node.clone(), params.ts)))
            .await
    }

    #[tool(
        description = "Thêm một log pattern vào Aho-Corasick automaton của node. \
                          Các lần query sau sẽ match pattern này."
    )]
    async fn pattern_add(
        &self,
        Parameters(params): Parameters<PatternParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let Some(text) = params.text.as_deref() else {
            return err_text("text is required for pattern_add".into());
        };
        let Some(idx) = text_index(&params.node).await else {
            return err_text(format!("no text index `{}` registered", params.node));
        };
        idx.write().await.add_pattern(text).await;
        ok_text(&serde_json::json!({"added": text}))
    }

    #[tool(
        description = "Kiểm tra một dòng log có match pattern nào đã đăng ký không. \
                          Trả về true/false hoặc null nếu node chưa tồn tại."
    )]
    async fn pattern_get(
        &self,
        Parameters(params): Parameters<PatternParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let Some(text) = params.text.as_deref() else {
            return err_text("text is required for pattern_get".into());
        };
        let Some(idx) = text_index(&params.node).await else {
            return err_text(format!("no text index `{}` registered", params.node));
        };
        let known = idx.read().await.is_known(text).await.unwrap_or(false);
        ok_text(&serde_json::json!({"matched": known, "text": text}))
    }

    #[tool(description = "Thống kê pattern store: số patterns, số queries, hits, misses.")]
    async fn pattern_stats(
        &self,
        Parameters(params): Parameters<PatternParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let Some(idx) = text_index(&params.node).await else {
            return err_text(format!("no text index `{}` registered", params.node));
        };
        let (total, hits, misses) = idx.read().await.pattern_stats();
        ok_text(&serde_json::json!({
            "total_patterns": total,
            "hits": hits,
            "misses": misses,
        }))
    }

    #[tool(description = "Substring search trên catalog index: trả về các cặp \
                          key/value mà key chứa pattern.")]
    async fn catalog_search(
        &self,
        Parameters(params): Parameters<CatalogSearchParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let Some(idx) = text_index(&params.node).await else {
            return err_text(format!("no text index `{}` registered", params.node));
        };
        let entries = idx.read().await.search_entries(&params.pattern, None).await;
        let items: Vec<serde_json::Value> = entries
            .into_iter()
            .map(|(k, v)| serde_json::json!({"key": k, "value": v}))
            .collect();
        ok_text(&serde_json::json!(items))
    }

    #[tool(
        description = "Liệt kê danh sách key/value đã index trong catalog station \
                          (station kind `category`) với pagination: `limit`/`offset`, \
                          trả kèm `total`. Có thể lọc qua `pattern` substring trên key. \
                          Dùng để xem các metric/key mà catalog đang support."
    )]
    async fn catalog_list(
        &self,
        Parameters(params): Parameters<CatalogListParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let Some(idx) = text_index(&params.node).await else {
            return err_text(format!("no text index `{}` registered", params.node));
        };
        let offset = params.offset.unwrap_or(0);
        let limit = params.limit.unwrap_or(100).clamp(1, 1000);
        let guard = idx.read().await;
        let (items, total) = match params.pattern.as_deref().filter(|p| !p.is_empty()) {
            Some(pattern) => {
                let hits = guard.search_entries(pattern, None).await;
                let total = hits.len();
                let page: Vec<(String, String)> =
                    hits.into_iter().skip(offset).take(limit).collect();
                (page, total)
            }
            None => guard.list_entries(offset, limit),
        };
        let rendered: Vec<serde_json::Value> = items
            .into_iter()
            .map(|(k, v)| serde_json::json!({"key": k, "value": v}))
            .collect();
        ok_text(&serde_json::json!({
            "total": total,
            "offset": offset,
            "limit": limit,
            "items": rendered,
        }))
    }

    #[tool(
        description = "Phục hồi dữ liệu mất do rotation/LRU: yêu cầu một node \
                          http_source re-fetch cửa sổ (from_ts, to_ts]. Watermark \
                          không lùi — luồng thường không ảnh hưởng; trạm tự dedup."
    )]
    async fn opsense_backfill(
        &self,
        Parameters(params): Parameters<BackfillParams>,
    ) -> Result<CallToolResult, ErrorData> {
        self.with_session(|opened| {
            Box::pin(backfill(
                opened,
                params.node.clone(),
                params.from_ts,
                params.to_ts,
            ))
        })
        .await
    }

    #[tool(
        description = "Đọc lại observations từ station của một node: truyền `source` là station/node \
                          id đã đăng ký, `stage` raw/processed, lọc theo metric và cửa sổ thời gian \
                          (from_ts, to_ts]. Trả về mảng observation JSON. Tầng persistence đã bị xoá."
    )]
    async fn opsense_query(
        &self,
        Parameters(params): Parameters<QueryParams>,
    ) -> Result<CallToolResult, ErrorData> {
        // A session must be open, but the read itself goes through the
        // process-global station registry (async, in-memory) — no blocking
        // task and no `spawn_blocking` needed.
        {
            let guard = self.session.read().await;
            if guard.is_none() {
                return err_text("no open session; call opsense_init first".into());
            }
        }
        // `source` = node id của trạm. The persistence tier was removed, so
        // only station/node ids are valid sources now.
        let store = match params.source.as_deref() {
            Some("persistence") => {
                return err_text(
                    "persistence tier removed; query a station/node id instead".into(),
                );
            }
            Some(id) => match station(id).await {
                Some(handle) => handle,
                None => {
                    return err_text(format!(
                        "no station `{id}` registered — bật pipeline trước hoặc kiểm tra lại id"
                    ));
                }
            },
            None => return err_text("source is required: a station/node id".into()),
        };
        let stage = match params.stage.as_deref() {
            Some("raw") => Stage::Raw,
            _ => Stage::Processed,
        };
        let metric = params.metric.clone();
        let to = params.to_ts.unwrap_or_else(signal::now_secs);
        // `from = 0` (epoch) khiến read-through fallback cố backfill từ đầu
        // thời gian và origin trả 400. Default là lookback 24h hợp lý.
        let from = params.from_ts.unwrap_or_else(|| signal::now_secs() - 24 * 3600);

        // In-memory async read: the timeout is now real (it cancels the await)
        // because we are no longer pinned on a blocking thread.
        let guard = store.read().await;
        let fut: std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Observation>> + Send>> =
            match metric.as_deref().filter(|m| !m.is_empty()) {
                Some(m) => Box::pin(guard.query(stage, m, from, to)),
                None => Box::pin(guard.query_all(stage, from, to)),
            };
        match tokio::time::timeout(query_timeout(), fut).await {
            Ok(items) => match serde_json::to_value(&items) {
                Ok(v) => ok_text(&v),
                Err(e) => err_text(format!("serialize observations: {e}")),
            },
            Err(_) => err_text(format!(
                "query timed out after {}s",
                query_timeout().as_secs()
            )),
        }
    }

    #[tool(
        description = "Liệt kê các station đã đăng ký (danh sách id) hoặc, nếu truyền `id`, \
                          mô tả chi tiết một station: backend lưu trữ, tham số cấu hình, metrics \
                          nội bộ (số appends/queries/evictions/file parquet…) và các station \
                          upstream phụ thuộc."
    )]
    async fn opsense_describe(
        &self,
        Parameters(params): Parameters<DescribeParams>,
    ) -> Result<CallToolResult, ErrorData> {
        match params.id {
            Some(id) => match describe_station(&id).await {
                Some(value) => ok_text(&value),
                None => err_text(format!("no station registered with id `{id}`")),
            },
            None => ok_text(&serde_json::json!(station_ids().await)),
        }
    }
}

#[tool_handler(name = "opsense-mcp", version = "1.0.0")]
impl ServerHandler for OpsenseMcp {}

/// Run the MCP server on stdio until the client disconnects, then close any
/// open session.
pub async fn run() -> std::io::Result<()> {
    let server = OpsenseMcp::new();
    let session = Arc::clone(&server.session);

    let service = server
        .serve(stdio())
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    service
        .waiting()
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    if let Some(open) = session.write().await.take() {
        let _ = deinit(&open).await;
    }
    Ok(())
}

/// Serve MCP over Streamable HTTP at `http://<addr>:<port>/mcp`.
///
/// Each HTTP client gets its own rmcp protocol session (in-memory store);
/// the underlying pipeline `Session` is still single-instance per config
/// directory via `opsense_init`'s lock.
///
/// # Errors
/// Bind/serve failures from axum.
/// The MCP transport handle: an axum method-router the host mounts at
/// whatever path it wants (`routes()` in serve.rs owns the paths — this
/// crate only supplies the handler).
pub fn mcp_handler() -> axum::routing::MethodRouter {
    use rmcp::transport::streamable_http_server::{
        session::local::LocalSessionManager, tower::StreamableHttpService,
    };

    let service = StreamableHttpService::new(
        || Ok(OpsenseMcp::new()),
        Arc::new(LocalSessionManager::default()),
        Default::default(),
    );

    axum::routing::any_service(service)
}

