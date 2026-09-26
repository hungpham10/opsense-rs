//! Realtime grid trading cho script Rhai: `portfolio_feed`.
//!
//! **Một hàm, stateless** — script không giữ state, station là nơi giữ state duy
//! nhất:
//!
//! ```rhai
//! let candles = station_candles("grid", from, to, resolution); // nến đã đóng
//! let obs     = station_query("grid", from, to);                // order + cursor
//! portfolio_feed(candles, obs, candles[candles.len() - 1], cfg, "BTCUSDT");
//! ```
//!
//! Native làm 4 việc, đều thuần trên state đọc được từ station:
//!
//! 1. **Tái dựng session** — `signal = "order"` cho lệnh mở/đóng,
//!    `labels.kind = "trading_step"` cho cursor (`candle_ts` + `candle_seq`).
//! 2. **Đóng gói nến thành [`FetchFn`]** tự phục vụ — kernel hỏi range nào cũng
//!    được, không cần biết dữ liệu nằm ở station.
//! 3. **Chạy [`Portfolio::forward`]** cho đúng nến mới (window 1 nến) — cùng
//!    kernel backtest dùng qua `Portfolio::backtest`.
//! 4. **Trả observation**: lệnh vừa đóng / vừa đặt + cursor mới.
//!
//! Vì sao không có `grid_plan`/`order_entries`/`order_check` riêng: chúng đã nằm
//! trong kernel (rebuild plan → check exit → evaluate entry → T+N). Ở đây chỉ
//! khác chỗ lấy nến: backtest đọc loader/cache, realtime đọc station.
//!
//! **T+N cần `candle_seq` đơn điệu** — vì script stateless, seq nằm ở cursor
//! observation chứ không nằm RAM node; không có nó thì mọi lệnh mới lại coi như
//! seq 0 và T+N không bao giờ chặn.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use opsense_core::Observation;
use opsense_model::events::{Signal, TelemetryKind};
use opsense_qlib::{
    Calendar, CandleStick, CryptoCalendar, DataLoader, Fee, ForexCalendar, Graph, Order,
    OrderEvent, OrderType, Portfolio, PortfolioConfig, Score, Session, SharpeScore,
    SimpleFixedFee, StockCalendar, Strategy, TradingGrid,
};
use rhai::{Array, Dynamic, Map};

use crate::ScriptStrategy;

/// Label của observation order.
const L_STATUS: &str = "status";
const L_ORDER_ID: &str = "order_id";
const L_DTYPE: &str = "dtype";
const L_GRID: &str = "grid";
const L_LEVEL: &str = "level";
const L_SIZE: &str = "size";
const L_SL: &str = "sl";
const L_TP: &str = "tp";
const L_PNL: &str = "pnl_pct";
const L_UNLOCK: &str = "unlock_seq";

/// Label cursor (dùng chung `labels.kind` với snapshot của script).
const L_KIND: &str = "kind";
const KIND_STEP: &str = "trading_step";
const L_CANDLE_SEQ: &str = "candle_seq";
/// Mốc rebuild kế tiếp (epoch giây) — mang `Session::review_at` qua lời gọi.
const L_NEXT_REVIEW: &str = "next_review";
/// Chỉ số nến trong review window — mang `Session::candle_id` qua lời gọi.
const L_CANDLE_ID: &str = "candle_id";

/// `signal` của observation chứa plan grid.
///
/// Plan **không** nhét vào cursor: cursor là trạng thái nhỏ (vài số), còn plan
/// là dữ liệu lớn (levels + ma trận weight + tỉ lệ thắng mỗi ô). Trộn chung
/// một chỗ thì khó lọc và khó đọc.
const SIGNAL_PLAN: &str = "plan";

const STATUS_OPEN: &str = "open";
const STATUS_CLOSED: &str = "closed";

/// `Dynamic::to_float/to_int` cần ownership; script map cho ta `&Dynamic`.
fn as_f64(v: &Dynamic) -> f64 {
    v.as_float().unwrap_or(0.0)
}

fn as_i64(v: &Dynamic) -> i64 {
    v.as_int().unwrap_or(0)
}

type LoaderFuture =
    Pin<Box<dyn Future<Output = Result<Vec<CandleStick>, std::io::Error>> + Send + 'static>>;

/// Đăng ký native realtime trading — gọi từ [`crate::tools::register_all`].
///
/// Không capture per-call state (state đến từ `station_query`), nên đăng ký tay
/// cùng các tool khác là đủ.
pub fn register(eng: &mut rhai::Engine) {
    eng.register_fn(
        "portfolio_feed",
        |candles: Array, obs: Array, candle: Map, cfg: Map, symbol: String| -> Array {
            let candles: Vec<CandleStick> = candles.into_iter().filter_map(candle_of).collect();
            let incoming = candle_from_map(&candle);
            let settings = Settings::from_map(&cfg);
            let state = State::from_observations(&obs);

            // Script chạy trên blocking thread của runtime (`spawn_blocking`) nên
            // có handle để chờ kernel async. Nếu không có runtime (đánh giá
            // script ngoài pipeline) → không làm gì thay vì panic.
            let result = tokio::runtime::Handle::try_current()
                .map(|handle| {
                    handle.block_on(feed(&candles, &state, incoming, &settings, &symbol))
                })
                .unwrap_or_else(|_| {
                    tracing::debug!("portfolio_feed: no tokio runtime in context, skipping");
                    Vec::new()
                });

            result
                .into_iter()
                .map(|o| rhai::serde::to_dynamic(&o).unwrap_or(Dynamic::UNIT))
                .collect()
        },
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Settings
// ═══════════════════════════════════════════════════════════════════════════

/// Cấu hình engine đọc từ map của script (thường là `params` của node) nên đổi
/// chiến lược = đổi TOML, không phải sửa script.
#[derive(Clone, Debug)]
struct Settings {
    resolution: String,
    calendar: String,
    strategy: String,
    grid_levels: usize,
    sl_pct: f64,
    lookback_secs: u64,
    review_interval_secs: u64,
    trading_candle_secs: u64,
    fee_rate: f64,
    kelly_fraction: f64,
    base_capital: f64,
    settlement_candles: u64,
    /// Genome DAG cho `strategy = "dag"` (JSON như trong config pipeline).
    dag: Option<serde_json::Value>,
    // ── Knob cho `strategy = "rhai"` (đọc trong `fn rebuild`) ──────────────
    /// Số lệnh đóng tối thiểu trước khi tin tỉ lệ thắng thực tế.
    grid_min_trades: usize,
    /// Độ nhọn phân bổ vốn (weights normal quanh giữa).
    grid_weight_sharpness: f64,
    /// `max_bit` cho sieve `AnalysisGrid` (số 2^k ô tối đa).
    grid_max_bit: usize,
    /// Biên độ win-prob lệch theo bậc (None = không lệch).
    grid_level_edge_amp: Option<f64>,
    /// Script của node đang chạy (`strategy = "rhai"`). Không đọc từ `cfg`:
    /// `portfolio_feed` nằm trong lời gọi script nên lấy từ
    /// [`crate::runtime::current_script`] — không bắt user khai đường dẫn thêm ở
    /// params (hai bản không thể lệch nhau).
    script: Option<crate::ScriptSource>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            resolution: "1m".to_string(),
            calendar: "crypto".to_string(),
            strategy: "rhai".to_string(),
            grid_levels: 5,
            sl_pct: 0.008,
            lookback_secs: 2 * 24 * 3600,
            review_interval_secs: 900,
            trading_candle_secs: 60,
            fee_rate: 0.0005,
            kelly_fraction: 0.25,
            base_capital: 100_000.0,
            settlement_candles: 0,
            dag: None,
            grid_min_trades: 3,
            grid_weight_sharpness: 4.0,
            grid_max_bit: 20,
            grid_level_edge_amp: Some(0.10),
            script: None,
        }
    }
}

impl Settings {
    fn from_map(cfg: &Map) -> Self {
        let mut s = Self::default();
        let num = |k: &str| cfg.get(k).map(as_f64);
        let int = |k: &str| cfg.get(k).map(as_i64);
        let text = |k: &str| {
            cfg.get(k)
                .and_then(|v| v.clone().into_string().ok())
        };

        if let Some(v) = text("resolution") {
            s.resolution = v;
        }
        if let Some(v) = text("calendar") {
            s.calendar = v;
        }
        if let Some(v) = text("strategy") {
            s.strategy = v;
        }
        if let Some(v) = int("grid_levels") {
            s.grid_levels = v.max(0) as usize;
        }
        if let Some(v) = num("sl_pct") {
            s.sl_pct = v;
        }
        if let Some(v) = int("grid_min_trades") {
            s.grid_min_trades = v.max(0) as usize;
        }
        if let Some(v) = num("grid_weight_sharpness") {
            s.grid_weight_sharpness = v;
        }
        if let Some(v) = int("grid_max_bit") {
            s.grid_max_bit = v.clamp(1, 32) as usize;
        }
        if let Some(v) = num("grid_level_edge_amp") {
            s.grid_level_edge_amp = Some(v);
        }
        if let Some(v) = int("lookback_secs") {
            s.lookback_secs = v.max(0) as u64;
        }
        if let Some(v) = int("review_interval_secs") {
            s.review_interval_secs = v.max(0) as u64;
        }
        if let Some(v) = int("trading_candle_secs") {
            s.trading_candle_secs = v.max(1) as u64;
        }
        if let Some(v) = num("fee_rate") {
            s.fee_rate = v;
        }
        if let Some(v) = num("kelly_fraction") {
            s.kelly_fraction = v;
        }
        if let Some(v) = num("base_capital") {
            s.base_capital = v;
        }
        if let Some(v) = int("settlement_candles") {
            s.settlement_candles = v.max(0) as u64;
        }
        if let Some(dag) = cfg.get("dag") {
            s.dag = rhai::serde::from_dynamic(dag).ok();
        }
        s
    }

    /// Genome DAG cho `strategy = "dag"` — deserialize trực tiếp từ
    /// `params.dag` (typetag cho `Op` nên `{"type":"ema","period":9}` …).
    fn dag(&self) -> Result<Graph, std::io::Error> {
        let spec = self
            .dag
            .as_ref()
            .ok_or_else(|| std::io::Error::other("strategy = \"dag\" cần `params.dag`"))?;
        serde_json::from_value(spec.clone())
            .map_err(|e| std::io::Error::other(format!("params.dag không hợp lệ: {e}")))
    }

    /// Layout params kernel + strategy: `[kelly, capital, grid_levels, sl_pct,
    /// lookback]` — cùng layout với `Graph::init()` (index 0..4) để `Portfolio`
    /// dùng chung không cần biết strategy nào đang chạy. Với DAG, `Graph::init()`
    /// tự có layout riêng nên hai nhánh này không trùng nhau.
    fn params(&self) -> Vec<f64> {
        if self.strategy == "dag" || self.strategy == "graph" {
            if let Ok(graph) = self.dag() {
                return graph.init();
            }
        }
        vec![
            self.kelly_fraction,
            self.base_capital,
            self.grid_levels as f64,
            self.sl_pct,
            self.lookback_secs as f64,
        ]
    }

    fn portfolio(&self) -> Result<Portfolio, std::io::Error> {
        let strategy: Arc<dyn Strategy + Sync + Send> = match self.strategy.as_str() {
            // DAG model: `params.dag` là genome (ops + nodes + weights) build
            // riêng ở Python, truyền vào đây dạng JSON. `Graph` tự emit ONNX
            // rồi chạy bằng tract — cầu nối giữa model ngoài và kernel.
            "dag" | "graph" => Arc::new(self.dag()?),
            // Script của chính node viết `fn rebuild` (xem `ScriptStrategy`).
            "rhai" | "script" => {
                let src = self.script.clone().ok_or_else(|| {
                    std::io::Error::other(
                        "strategy = \"rhai\" cần chạy trong script (portfolio_feed); \
                         gọi ngoài pipeline thì không có script nào để lấy",
                    )
                })?;
                let mut s = ScriptStrategy::new(src, self.review_interval_secs)
                    .with_knob("min_trades", self.grid_min_trades.into())
                    .with_knob("weight_sharpness", self.grid_weight_sharpness.into())
                    .with_knob("max_bit", (self.grid_max_bit as i64).into())
                    // `fee_rate` + `grid_levels` phải tới được `fn rebuild`: script
                    // cần chúng để tính bước giữa hai mốc tối thiểu còn lãi sau
                    // phí. Trước đây thiếu nên script rơi về default và dựng
                    // mốc quá dày ⇒ mọi entry bị kernel lo (`placed=0`).
                    .with_knob("fee_rate", self.fee_rate.into())
                    .with_knob("grid_levels", (self.grid_levels as i64).into());
                if let Some(amp) = self.grid_level_edge_amp {
                    s = s.with_knob("level_edge_amp", amp.into());
                }
                Arc::new(s)
            }
            other => {
                return Err(std::io::Error::other(format!(
                    "strategy `{other}` không hỗ trợ: dùng \"rhai\" (script `fn rebuild`) \
                     hoặc \"dag\" (genome `params.dag`)"
                )));
            }
        };
        let calendar: Arc<dyn Calendar + Sync + Send> = match self.calendar.as_str() {
            "forex" => Arc::new(ForexCalendar),
            "stock" => Arc::new(StockCalendar),
            _ => Arc::new(CryptoCalendar),
        };
        let fee: Arc<dyn Fee + Sync + Send> = Arc::new(SimpleFixedFee::new(self.fee_rate));
        let score: Arc<dyn Score + Sync + Send> = Arc::new(SharpeScore);

        Portfolio::new(
            Arc::new(StationlessLoader),
            strategy,
            fee,
            score,
            calendar,
            PortfolioConfig {
                resolution_for_test: self.resolution.clone(),
                // Realtime chỉ có một series nến trong station nên trade =
                // analysis. `FetchFn` không mang resolution nên hai resolution
                // buộc phải bằng nhau.
                resolution_for_rebuild: self.resolution.clone(),
                settlement_candles: self.settlement_candles,
                // Nến đã ở station, fetch tự phục vụ từ slice → không cần LRU.
                cache_enabled: false,
            },
        )
    }
}

/// Loader rỗng: realtime không đọc loader (fetch đóng trên slice do script đưa
/// vào), `Portfolio` chỉ cần một giá trị hợp lệ.
struct StationlessLoader;

#[async_trait::async_trait]
impl DataLoader for StationlessLoader {
    async fn range(
        &self,
        _from: u64,
        _to: u64,
        _resolution: &str,
    ) -> Result<Vec<CandleStick>, std::io::Error> {
        Ok(Vec::new())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// State tái dựng từ station
// ═══════════════════════════════════════════════════════════════════════════

struct State {
    session: Session,
    /// `order_id` của lệnh đang mở, khoá theo `(grid_index, level_index)` —
    /// đúng khoá mà `evaluate_grid_entries` dùng để chặn đặt trùng.
    open_ids: HashMap<(usize, usize), String>,
    next_id: u64,
}

impl State {
    fn from_observations(obs: &Array) -> Self {
        let mut session = Session::new();
        let mut open: HashMap<(usize, usize), (String, Order)> = HashMap::new();
        let mut closed_ids: Vec<String> = Vec::new();
        let mut history: Vec<Order> = Vec::new();
        let mut candle_seq = 0u64;
        let mut candle_ts = 0i64;
        let mut next_id = 1u64;
        let mut review_at = 0u64;
        let mut candle_id = 0usize;
        let mut plan: Vec<TradingGrid> = Vec::new();
        let mut plan_ts = i64::MIN;

        for item in obs.iter() {
            let Some(observation) = observation_of(item.clone()) else {
                continue;
            };
            // Cursor: nến nào đã chạy trading step + seq tới đâu.
            if observation.labels.get(L_KIND).map(String::as_str) == Some(KIND_STEP) {
                candle_seq = candle_seq.max(parse_u64(&observation, L_CANDLE_SEQ));
                candle_ts = candle_ts.max(observation.ts);
                // `review_at` / `candle_id` mang qua lời gọi. `max` theo `ts`:
                // station append-only, cursor cũ hơn thì giữ bản mới hơn.
                review_at = review_at.max(parse_u64(&observation, L_NEXT_REVIEW));
                candle_id = candle_id.max(parse_u64(&observation, L_CANDLE_ID) as usize);
                continue;
            }
            // Plan: dùng bản ghi **mới nhất** (ts lớn nhất) làm chính.
            if let Some(grids) = plan_of_observation(&observation) {
                if plan.is_empty() || observation.ts >= plan_ts {
                    plan = grids;
                    plan_ts = observation.ts;
                }
                continue;
            }
            if observation.signal != Signal::Order {
                continue;
            }
            let id = observation
                .labels
                .get(L_ORDER_ID)
                .cloned()
                .unwrap_or_else(|| format!("o{}-{}", observation.ts, next_id));
            next_id = next_id.max(tail_num(&id) + 1);

            let Some(order) = order_of(&observation) else {
                continue;
            };
            if observation.labels.get(L_STATUS).map(String::as_str) == Some(STATUS_CLOSED) {
                closed_ids.push(id);
                history.push(order);
            } else {
                open.insert((order.grid_index, order.level_index), (id, order));
            }
        }

        session.history = history;
        session.orders = open
            .values()
            .filter(|(id, _)| !closed_ids.iter().any(|c| c == id))
            .map(|(_, order)| *order)
            .collect();
        session.candle_seq = candle_seq;
        session.candle_ts = candle_ts;
        // `review_at` / `candle_id` / `plan` mang từ cursor + observation plan.
        // Trước đây cả ba bị đặt 0 / để rỗng ở đây ⇒ kernel rebuild **mỗi
        // nến**, `candle_id` luôn 0 (chỉ dùng cột 0 của ma trận weight), và
        // thống kê thắng/thua mỗi mốc bị xoá mỗi phút vì plan dựng lại từ đầu.
        session.review_at = review_at;
        session.candle_id = candle_id;
        session.plan = plan;

        let open_ids = open
            .iter()
            .filter(|(_, (id, _))| !closed_ids.iter().any(|c| c == id))
            .map(|(key, (id, _))| (*key, id.clone()))
            .collect();

        Self {
            session,
            open_ids,
            next_id,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Feed
// ═══════════════════════════════════════════════════════════════════════════

async fn feed(
    candles: &[CandleStick],
    state: &State,
    incoming: CandleStick,
    settings: &Settings,
    symbol: &str,
) -> Vec<Observation> {
    if incoming.t <= 0 || !incoming.c.is_finite() || incoming.c <= 0.0 {
        return Vec::new();
    }
    // AnalysisGrid cần ≥ 10 nến; thiếu thì plan rỗng, không đặt được lệnh.
    if candles.len() < 10 {
        return vec![step_cursor(
            incoming.t,
            state.session.candle_seq,
            state.session.review_at,
            state.session.candle_id,
            symbol,
        )];
    }
    let mut settings = settings.clone();
    // Script của node đang chạy thắng. Ngoài pipeline (unit test gọi `feed`
    // trực tiếp) `current_script()` là None → giữ script đã gán sẵn.
    if let Some(src) = crate::runtime::current_script() {
        settings.script = Some(src);
    }
    let Ok(portfolio) = settings.portfolio() else {
        return Vec::new();
    };

    // Series phân tích: nến đã đóng trước đó + nến mới.
    let mut series: Vec<CandleStick> = candles.iter().copied().filter(|c| c.t < incoming.t).collect();
    series.push(incoming);
    series.sort_by_key(|c| c.t);
    series.dedup_by_key(|c| c.t);

    let from = u64::try_from(incoming.t).unwrap_or_default();
    let params = settings.params();

    let mut session = state.session.clone();
    // Con trỏ `forward` nằm trong `Session`, nên live phải đặt trước khi gọi:
    // nến `incoming` chính là nến kế tiếp cần xử lý.
    session.next_ts = from;

    let events: Arc<Mutex<Vec<OrderEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let mut notify = move |e: OrderEvent| {
        if let Ok(mut guard) = sink.lock() {
            guard.push(e);
        }
        Box::pin(async move { Ok(()) })
            as Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'static>>
    };

    let mut trade = slice_fetch(series.clone());
    let mut analysis = slice_fetch(series.clone());

    // Một nến đóng = một lượt `forward`. `fetch` đóng trên `series` nên `forward`
    // tự tìm nến đầu tiên có `t > candle_ts`, tức đúng nến mới.
    //
    // Rebuild fail (thiếu data) → kernel giữ plan cũ, không đặt lệnh; lần sau
    // thử lại. Không phải lỗi của script nên không ném — nhưng **phải log**:
    // nuốt im lặng (`let _ =`) khiến "kernel chạy mà không đặt lệnh" trông
    // giống hệt "chiến lược hợp lý nhưng giá chưa chạm level", và không có
    // cách nào phân biệt ngoài log.
    if let Err(e) = portfolio
        .forward(
            &mut session,
            &|id| params.get(id).copied().unwrap_or(0.0),
            &mut *trade,
            &mut *analysis,
            &mut notify,
        )
        .await
    {
        tracing::warn!(
            candle_ts = incoming.t,
            error = %e,
            "portfolio forward lỗi — giữ plan cũ, lần sau thử lại"
        );
    }

    let events = events.lock().map(|g| g.clone()).unwrap_or_default();
    let mut out = Vec::new();
    let mut next_id = state.next_id;
    let mut rebuilt = false;
    for event in events {
        match event {
            OrderEvent::Placed { ts, order } => {
                let id = format!("o{ts}-{next_id}");
                next_id += 1;
                out.push(open_observation(ts, &id, &order, symbol));
            }
            OrderEvent::Closed { ts, order } => {
                // Khép vòng đời: dùng đúng id của lệnh đã mở.
                let id = state
                    .open_ids
                    .get(&(order.grid_index, order.level_index))
                    .cloned()
                    .unwrap_or_else(|| format!("o{ts}-{next_id}"));
                out.push(closed_observation(ts, &id, &order, symbol));
            }
            // `Rebuilt` mang `GridSnapshot` (chỉ levels) — không đủ để dựng lại
            // lưới, nên dùng nó làm **cờ** rồi ghi `session.plan` (đầy đủ) bên
            // dưới. `Rejected` là chẩn đoán, không ghi obs.
            OrderEvent::Rebuilt { .. } => rebuilt = true,
            OrderEvent::Rejected { .. } => {}
        }
    }

    // Plan mới → ghi observation riêng. Chỉ ghi khi **vừa rebuild**, không ghi
    // mỗi nến: station append-only, ghi lại mỗi phút sẽ phình vô ích.
    if rebuilt && let Some(obs) = plan_observation(incoming.t as u64, &session.plan, symbol) {
        out.push(obs);
    }

    // Cursor để lời gọi sau khôi phục `candle_seq` (T+N) + `candle_ts` (idempotent),
    // cùng `review_at` / `candle_id` để giữ đúng nhịp rebuild và đúng cột
    // weight matrix giữa hai lần rebuild.
    out.push(step_cursor(
        incoming.t,
        session.candle_seq.max(state.session.candle_seq),
        session.review_at,
        session.candle_id,
        symbol,
    ));
    out
}

/// Fetch đóng trên series cố định: kernel hỏi range nào cũng trả đúng candles ở
/// trong đó. Series nhỏ (vài trăm nến) nên lọc tuyến tính là đủ.
fn slice_fetch(
    series: Vec<CandleStick>,
) -> Box<dyn FnMut(u64, u64) -> LoaderFuture + Send + Sync> {
    Box::new(move |from: u64, to: u64| {
        let out: Vec<CandleStick> = series
            .iter()
            .copied()
            .filter(|c| c.t >= 0 && (c.t as u64) >= from && (c.t as u64) < to)
            .collect();
        Box::pin(async move { Ok(out) })
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Observation ⇄ Order
// ═══════════════════════════════════════════════════════════════════════════

/// 1 obs = 1 lệnh: `value` = giá vào lệnh (mở) hoặc PnL % (đóng), phần còn lại
/// trong `labels` (labels là string map nên số phải format). Append-only: đóng
/// lệnh là obs mới cùng `order_id` → vòng đời lệnh là event log trong station.
fn open_observation(ts: u64, id: &str, order: &Order, symbol: &str) -> Observation {
    let mut o = Observation::new(
        ts as i64,
        symbol.to_string(),
        TelemetryKind::Metric,
        Signal::Order,
        order.entry_price,
    );
    o.labels = order_labels(id, order, STATUS_OPEN);
    o
}

fn closed_observation(ts: u64, id: &str, order: &Order, symbol: &str) -> Observation {
    let mut o = Observation::new(
        ts as i64,
        symbol.to_string(),
        TelemetryKind::Metric,
        Signal::Order,
        order.pnl_pct.unwrap_or_default(),
    );
    o.labels = order_labels(id, order, STATUS_CLOSED);
    o
}

// ═══════════════════════════════════════════════════════════════════════════
// Plan ⇄ observation
//
// `Session::plan` phải sống sót giữa các lần gọi, nếu không `review_at` tới hạn
// thì kernel giữ plan cũ — mà plan rỗng thì **không đặt lệnh nào**. Ba thứ đi
// cùng nhau, không tách được:
//
//   1. `plan`            — không có thì lưới rỗng ⇒ không vào lệnh
//   2. `candle_id`       — cột của ma trận weight; mất thì luôn dùng cột 0
//   3. thống kê mỗi mốc — `TradingGrid` cộng dồn khi lệnh đóng
//
// (3) **không** serialize: suy ra lại được từ `session.history`, vốn đã nằm
// trong station dưới dạng `signal = "order"` + `status = "closed"`.
// ═══════════════════════════════════════════════════════════════════════════

/// Một ô lưới đủ để dựng lại: levels + sl + ma trận weight + tỉ lệ thắng +
/// 4 bộ đếm thắng/thua.
#[derive(serde::Serialize, serde::Deserialize)]
struct PlanCell {
    levels: Vec<f64>,
    sl_pct: f64,
    max_candles: usize,
    weights: Vec<Vec<f64>>,
    long_win_p: Vec<f64>,
    short_win_p: Vec<f64>,
    long_win_cnt: Vec<usize>,
    long_lost_cnt: Vec<usize>,
    short_win_cnt: Vec<usize>,
    short_lost_cnt: Vec<usize>,
}

impl PlanCell {
    fn of(grid: &TradingGrid) -> Self {
        let n = grid.num_levels();
        let cols = grid.weight_cols();
        let counts = |f: fn(&TradingGrid, usize) -> usize| -> Vec<usize> {
            (0..n).map(|j| f(grid, j)).collect()
        };
        Self {
            levels: grid.levels().to_vec(),
            sl_pct: grid.stoploss_pct(),
            max_candles: grid.max_candles(),
            weights: (0..n)
                .map(|j| (0..cols).map(|t| grid.weight(j, t)).collect())
                .collect(),
            long_win_p: (0..n).map(|j| grid.long_win_pct(j)).collect(),
            short_win_p: (0..n).map(|j| grid.short_win_pct(j)).collect(),
            long_win_cnt: counts(TradingGrid::long_win_count),
            long_lost_cnt: counts(TradingGrid::long_lost_count),
            short_win_cnt: counts(TradingGrid::short_win_count),
            short_lost_cnt: counts(TradingGrid::short_lost_count),
        }
    }

    /// Dựng lại `TradingGrid`. `None` nếu levels không hợp lệ.
    fn to_grid(&self) -> Option<TradingGrid> {
        let g = TradingGrid::from_levels(self.levels.clone())?
            .with_sl_pct(self.sl_pct)
            .with_max_candles(self.max_candles)
            .with_weight_matrix(self.weights.clone())
            .with_long_win_p(self.long_win_p.clone())
            .with_short_win_p(self.short_win_p.clone())
            .with_outcome_counts(
                self.long_win_cnt.clone(),
                self.long_lost_cnt.clone(),
                self.short_win_cnt.clone(),
                self.short_lost_cnt.clone(),
            );
        Some(g)
    }
}

fn plan_observation(ts: u64, plan: &[TradingGrid], symbol: &str) -> Option<Observation> {
    if plan.is_empty() {
        return None;
    }
    let cells: Vec<PlanCell> = plan.iter().map(PlanCell::of).collect();
    let json = serde_json::to_string(&cells).ok()?;
    let mut o = Observation::new(
        ts as i64,
        symbol.to_string(),
        TelemetryKind::Metric,
        Signal::Summary,
        plan.len() as f64,
    );
    o.labels.insert(L_KIND.to_string(), SIGNAL_PLAN.to_string());
    o.labels.insert("cells".to_string(), json);
    Some(o)
}

fn plan_of_observation(obs: &Observation) -> Option<Vec<TradingGrid>> {
    if obs.signal != Signal::Summary
        || obs.labels.get(L_KIND).map(String::as_str) != Some(SIGNAL_PLAN)
    {
        return None;
    }
    let json = obs.labels.get("cells")?;
    let cells: Vec<PlanCell> = serde_json::from_str(json).ok()?;
    let grids: Vec<TradingGrid> = cells.iter().filter_map(PlanCell::to_grid).collect();
    (!grids.is_empty()).then_some(grids)
}

fn order_labels(id: &str, order: &Order, status: &str) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    labels.insert(L_ORDER_ID.to_string(), id.to_string());
    labels.insert(L_STATUS.to_string(), status.to_string());
    labels.insert(
        L_DTYPE.to_string(),
        match order.dtype {
            OrderType::Long => "long",
            OrderType::Short => "short",
            OrderType::Unknown => "unknown",
        }
        .to_string(),
    );
    labels.insert(L_GRID.to_string(), order.grid_index.to_string());
    labels.insert(L_LEVEL.to_string(), order.level_index.to_string());
    labels.insert(L_SIZE.to_string(), order.size.to_string());
    labels.insert(L_SL.to_string(), order.sl_price.to_string());
    labels.insert(L_TP.to_string(), order.tp_price.to_string());
    labels.insert(L_UNLOCK.to_string(), order.unlock_seq.to_string());
    if let Some(pnl) = order.pnl_pct {
        labels.insert(L_PNL.to_string(), pnl.to_string());
    }
    labels
}

/// Cursor: nến cuối đã chạy trading step + seq toàn cục tới đó, cộng trạng
/// thái review để lời gọi sau khôi phục đúng nhịp rebuild và đúng cột weight.
fn step_cursor(ts: i64, candle_seq: u64, review_at: u64, candle_id: usize, symbol: &str) -> Observation {
    let mut o = Observation::new(
        ts,
        symbol.to_string(),
        TelemetryKind::Metric,
        Signal::Summary,
        ts as f64,
    );
    o.labels.insert(L_KIND.to_string(), KIND_STEP.to_string());
    o.labels
        .insert(L_CANDLE_SEQ.to_string(), candle_seq.to_string());
    o.labels
        .insert(L_NEXT_REVIEW.to_string(), review_at.to_string());
    o.labels
        .insert(L_CANDLE_ID.to_string(), candle_id.to_string());
    o
}

/// Dựng lại `Order` từ observation; `None` khi thiếu dữ liệu dùng được.
fn order_of(o: &Observation) -> Option<Order> {
    let num = |key: &str| -> f64 { o.labels.get(key).and_then(|v| v.parse().ok()).unwrap_or(0.0) };
    let dtype = match o.labels.get(L_DTYPE).map(String::as_str) {
        Some("long") => OrderType::Long,
        Some("short") => OrderType::Short,
        // Unknown không đóng được (check_order_exit trả None) → coi là hỏng.
        _ => return None,
    };
    let entry = o.value;
    if entry <= 0.0 {
        return None;
    }
    Some(Order {
        dtype,
        entry_price: entry,
        size: num(L_SIZE),
        sl_price: num(L_SL),
        tp_price: num(L_TP),
        grid_index: num(L_GRID).max(0.0) as usize,
        level_index: num(L_LEVEL).max(0.0) as usize,
        pnl_pct: o.labels.get(L_PNL).and_then(|v| v.parse().ok()),
        exit_price: None,
        unlock_seq: num(L_UNLOCK).max(0.0) as u64,
    })
}

fn parse_u64(o: &Observation, key: &str) -> u64 {
    o.labels.get(key).and_then(|v| v.parse().ok()).unwrap_or(0)
}

fn tail_num(id: &str) -> u64 {
    id.rsplit('-').next().and_then(|n| n.parse().ok()).unwrap_or(0)
}

// ═══════════════════════════════════════════════════════════════════════════
// Rhai dynamic ⇄ Rust
// ═══════════════════════════════════════════════════════════════════════════

/// `station_candles` trả candle qua `rhai::serde` → map `{t,o,h,l,c,v}`.
fn candle_of(value: Dynamic) -> Option<CandleStick> {
    let map = value.try_cast::<Map>()?;
    Some(candle_from_map(&map))
}

fn candle_from_map(map: &Map) -> CandleStick {
    let num = |k: &str| map.get(k).map(as_f64).unwrap_or(0.0);
    CandleStick::new(
        map.get("t").map(as_i64).unwrap_or(0),
        num("o"),
        num("h"),
        num("l"),
        num("c"),
        num("v"),
    )
}

fn observation_of(value: Dynamic) -> Option<Observation> {
    if value.is_map() {
        rhai::serde::from_dynamic(&value).ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rhai::Engine;

    /// `fn rebuild` tối giản cho unit test: một lưới phẳng bộc kín toàn bộ
    /// khoảng giá quan sát (nên mọi nến đều chạm level ⇒ lệnh dễ đặt).
    /// Strategy thật viết bằng Rhai nằm ở `strategies/binance/grid.rhai`.
    const TEST_REBUILD: &str = r#"
        fn rebuild(candles, prev, params) {
            let lo = candles[0].l;
            let hi = candles[0].h;
            for k in candles {
                if k.l < lo { lo = k.l; }
                if k.h > hi { hi = k.h; }
            }
            if hi <= lo { return []; }
            let k = params.grid_levels.to_int();
            let levels = [];
            let long_win = [];
            let short_win = [];
            for i in 0..k {
                levels += [lo + (hi - lo) * i / k];
                long_win += [0.5];
                short_win += [0.5];
            }
            levels += [hi];
            long_win += [0.5];
            short_win += [0.5];
            [ #{ levels: levels, sl_pct: params.sl_pct,
                long_win: long_win, short_win: short_win } ]
        }
    "#;

    /// Settings như production (`strategy = "rhai"`) nhưng trỏ script strategy
    /// tối giản — `feed()` tự lấy script của node khi chạy thật.
    fn scripted_settings() -> Settings {
        Settings {
            script: Some(crate::ScriptSource::Inline(TEST_REBUILD.into())),
            ..Settings::from_map(&cfg_map())
        }
    }

    fn cfg_map() -> Map {
        let mut m = Map::new();
        m.insert("resolution".into(), Dynamic::from("1m"));
        m.insert("grid_levels".into(), Dynamic::from(5 as i64));
        m.insert("fee_rate".into(), Dynamic::from(0.0005));
        m
    }

    fn candle_map(t: i64, price: f64) -> Map {
        let mut m = Map::new();
        m.insert("t".into(), Dynamic::from(t));
        m.insert("o".into(), Dynamic::from(price));
        m.insert("h".into(), Dynamic::from(price + 0.5));
        m.insert("l".into(), Dynamic::from(price - 0.5));
        m.insert("c".into(), Dynamic::from(price));
        m.insert("v".into(), Dynamic::from(10.0));
        m
    }

    #[test]
    fn candle_map_roundtrips_through_dynamic() {
        // Đường đi của candle từ `station_candles` (Rhai) về kernel.
        let map = candle_map(1_700_000_000, 100.0);
        let dynamic = Dynamic::from(map);
        let candle = candle_of(dynamic).expect("parse được candle");
        assert_eq!(candle.t, 1_700_000_000);
        assert!((candle.c - 100.0).abs() < 1e-9);
    }

    #[test]
    fn settings_take_defaults_and_overrides() {
        let s = Settings::from_map(&Map::new());
        assert_eq!(s.resolution, "1m");
        assert_eq!(s.grid_levels, 5);
        let s = Settings::from_map(&cfg_map());
        assert_eq!(s.grid_levels, 5);
        assert_eq!(s.params().len(), 5, "params theo layout strategy.init()");
    }

    #[tokio::test]
    async fn feed_returns_cursor_even_without_orders() {
        let data: Vec<CandleStick> = (0..20)
            .map(|i| {
                let p = 100.0 + (i % 7) as f64;
                CandleStick::new(1_700_000_000 + i * 60, p, p + 0.5, p - 0.5, p, 10.0)
            })
            .collect();
        let state = State::from_observations(&Array::new());
        let settings = scripted_settings();
        let out = feed(
            &data,
            &state,
            *data.last().expect("có nến"),
            &settings,
            "BTCUSDT",
        )
        .await;

        // Phải có cursor để lời gọi sau khôi phục seq/ts.
        let cursor = out
            .iter()
            .find(|o| o.labels.get(L_KIND).map(String::as_str) == Some(KIND_STEP))
            .expect("phải trả cursor");
        assert_eq!(cursor.signal, Signal::Summary);
    }

    #[tokio::test]
    async fn feed_places_orders_on_wavy_market() {
        // Giá nhấn sóng mạnh để chạm nhiều level grid.
        let data: Vec<CandleStick> = (0..60)
            .map(|i| {
                let p = 100.0 + (i % 30) as f64 - 15.0;
                CandleStick::new(1_700_000_000 + i * 60, p, p + 0.5, p - 0.5, p, 10.0)
            })
            .collect();
        let state = State::from_observations(&Array::new());
        let settings = scripted_settings();
        let out = feed(&data, &state, *data.last().expect("có nến"), &settings, "BTCUSDT").await;

        let orders: Vec<&Observation> = out
            .iter()
            .filter(|o| o.signal == Signal::Order)
            .collect();
        assert!(!orders.is_empty(), "phải có lệnh: {out:?}");
        let first = orders[0];
        assert_eq!(first.metric_id, "BTCUSDT");
        assert_eq!(first.labels[L_STATUS], STATUS_OPEN);
        assert!(first.value > 0.0, "entry price dương");
        assert!(first.labels.contains_key(L_SL) && first.labels.contains_key(L_TP));
    }

    /// Genome DAG tối giản: Last → Head(2 feature, 8 output) — model emit ONNX
    /// rồi chạy bằng tract, đúng đường đi của DAG build từ Python.
    fn dag_map(n_grids: i64) -> Map {
        let json = serde_json::json!({
            "ops": [
                { "type": "last" },
                { "type": "head", "n_feat": 1, "n_out": n_grids }
            ],
            "nodes": [
                { "op": 0, "inputs": [{ "FromExtractor": 0 }] },
                { "op": 1, "inputs": [{ "FromOperator": 0 }] }
            ],
            "extractors": [],
            "inited_bias": vec![0.0; n_grids as usize],
            "inited_weights": vec![0.0; n_grids as usize],
            "window_size": 200,
            "num_of_grids": n_grids,
            "lookback_time_to_rebuild": 200,
            "interval_time_to_rebuild": 60
        });
        rhai::serde::to_dynamic(json).expect("dag json").try_cast().expect("map")
    }

    #[test]
    fn dag_strategy_deserializes_from_params() {
        let mut cfg = cfg_map();
        cfg.insert("strategy".into(), Dynamic::from("dag"));
        cfg.insert("dag".into(), Dynamic::from(dag_map(8)));
        let settings = Settings::from_map(&cfg);

        let graph = settings.dag().expect("graph deserialize được");
        assert_eq!(graph.num_features().expect("num_features"), 1);
        // Params phải lấy từ `Graph::init()` (weights vị trí 6+), không phải
        // layout dùng chung với strategy script / DAG.
        let params = settings.params();
        assert_eq!(params.len(), 6 + 1 * 8 + 8);
        assert!(params[1] > 0.0, "base_capital phải > 0: {params:?}");
    }

    #[test]
    fn dag_strategy_requires_dag_param() {
        let mut cfg = cfg_map();
        cfg.insert("strategy".into(), Dynamic::from("dag"));
        let settings = Settings::from_map(&cfg);
        let err = settings.dag().expect_err("thiếu params.dag phải báo lỗi");
        assert!(
            err.to_string().contains("params.dag"),
            "lỗi phải nói rõ thiếu gì: {err}"
        );
    }

    #[tokio::test]
    async fn dag_strategy_builds_plan_from_onnx_inference() {
        // 20 nến + nhịp sóng để DAG (ATR ở output 1) dựng được grid.
        let data: Vec<CandleStick> = (0..20)
            .map(|i| {
                let p = 100.0 + (i % 5) as f64;
                CandleStick::new(1_700_000_000 + i * 60, p, p + 1.0, p - 1.0, p, 10.0)
            })
            .collect();
        let mut cfg = cfg_map();
        cfg.insert("strategy".into(), Dynamic::from("dag"));
        cfg.insert("dag".into(), Dynamic::from(dag_map(8)));
        let settings = Settings::from_map(&cfg);

        let out = feed(
            &data,
            &State::from_observations(&Array::new()),
            *data.last().expect("có nến"),
            &settings,
            "BTCUSDT",
        )
        .await;
        // Dù không có lệnh (model zero-weight → prob ≈ 0.5, biên ATR hẹp),
        // cursor phải được ghi → kernel đã chạy qua ONNX mà không lỗi.
        let cursor = out
            .iter()
            .find(|o| o.labels.get(L_KIND).map(String::as_str) == Some(KIND_STEP))
            .expect("cursor phải có → kernel forward đã chạy");
        assert!(cursor.ts > 0);
    }

    #[tokio::test]
    async fn state_roundtrips_orders_through_observations() {
        let data: Vec<CandleStick> = (0..60)
            .map(|i| {
                let p = 100.0 + (i % 30) as f64 - 15.0;
                CandleStick::new(1_700_000_000 + i * 60, p, p + 0.5, p - 0.5, p, 10.0)
            })
            .collect();
        let settings = scripted_settings();
        let first = feed(
            &data,
            &State::from_observations(&Array::new()),
            *data.last().expect("có nến"),
            &settings,
            "BTCUSDT",
        )
        .await;

        let is_open = |o: &Observation| {
            o.signal == Signal::Order
                && o.labels.get(L_STATUS).map(String::as_str) == Some(STATUS_OPEN)
        };
        let opens: Vec<&Observation> = first.iter().filter(|o| is_open(o)).collect();
        assert!(!opens.is_empty(), "phải có lệnh mở: {first:?}");

        // Ghi vào station rồi đọc lại: lệnh mở phải còn nguyên.
        let mut persisted: Array = first
            .iter()
            .map(|o| rhai::serde::to_dynamic(o).expect("serialize"))
            .collect();
        let state = State::from_observations(&persisted);
        assert_eq!(state.session.orders.len(), opens.len());
        assert!(!state.open_ids.is_empty(), "id lệnh phải được giữ để khép vòng đời");

        // Đóng hết rồi đọc lại → không còn lệnh mở, lịch sử có đủ.
        for o in &opens {
            let order = Order {
                dtype: OrderType::Long,
                entry_price: o.value,
                pnl_pct: Some(0.1),
                ..Order::default()
            };
            let id = o.labels.get(L_ORDER_ID).expect("có id").clone();
            let closed = closed_observation(
                u64::try_from(o.ts).unwrap_or_default(),
                &id,
                &order,
                "BTCUSDT",
            );
            persisted.push(rhai::serde::to_dynamic(&closed).expect("serialize"));
        }
        let state = State::from_observations(&persisted);
        assert!(
            state.session.orders.is_empty(),
            "lệnh đã đóng không được giữ trong orders"
        );
        assert_eq!(state.session.history.len(), opens.len(), "lịch sử giữ lệnh đóng");
    }

    /// `review_at` / `candle_id` / `plan` phải sống sót qua lời gọi.
    ///
    /// Trước đây `from_observations` đặt cứng `review_at = 0` / `candle_id = 0`
    /// và không khôi phục plan ⇒ kernel rebuild **mỗi nến** (config nói 900s),
    /// `candle_id` luôn 0 nên chỉ dùng cột 0 của ma trận weight, và thống kê
    /// thắng/thua mỗi mốc bị xoá mỗi phút.
    #[test]
    fn session_state_survives_station_round_trip() {
        // Lưới thật: 3 mốc, ma trận weight 2 cột, có thống kê thắng/thua.
        let grid = TradingGrid::from_levels(vec![100.0, 110.0, 120.0])
            .expect("levels hợp lệ")
            .with_sl_pct(0.008)
            .with_max_candles(2)
            .with_weight_matrix(vec![vec![0.3, 0.6], vec![0.5, 0.5], vec![0.7, 0.4]])
            .with_long_win_p(vec![0.55, 0.6, 0.65])
            .with_short_win_p(vec![0.45, 0.4, 0.35])
            .with_outcome_counts(vec![2, 1, 0], vec![0, 1, 2], vec![1, 0, 1], vec![1, 1, 0]);
        let plan = vec![grid];

        let mut obs: Array = vec![rhai::serde::to_dynamic(
            &plan_observation(1_000, &plan, "BTCUSDT").expect("plan obs"),
        )
        .expect("serialize")];
        obs.push(
            rhai::serde::to_dynamic(&step_cursor(1_000, 42, 1_900, 7, "BTCUSDT"))
                .expect("serialize"),
        );

        let state = State::from_observations(&obs);

        assert_eq!(state.session.review_at, 1_900, "review_at phải sống qua station");
        assert_eq!(state.session.candle_id, 7, "candle_id phải sống qua station");
        assert_eq!(state.session.plan.len(), 1, "plan phải được khôi phục");

        let g = &state.session.plan[0];
        assert_eq!(g.num_levels(), 3, "số mốc phải khớp");
        assert_eq!(g.levels(), &[100.0, 110.0, 120.0], "giá mốc phải khớp");
        assert!((g.stoploss_pct() - 0.008).abs() < 1e-12, "sl_pct phải khớp");
        assert_eq!(g.max_candles(), 2, "max_candles phải khớp");

        // Ma trận weight: mất nó thì cỡ lệnh sai — đây là thứ quyết định.
        for (j, row) in [[0.3, 0.6], [0.5, 0.5], [0.7, 0.4]].iter().enumerate() {
            for (t, want) in row.iter().enumerate() {
                assert!(
                    (g.weight(j, t) - want).abs() < 1e-12,
                    "weight[{j}][{t}] = {} ≠ {want}",
                    g.weight(j, t)
                );
            }
        }
        assert!((g.long_win_pct(0) - 0.55).abs() < 1e-12, "tỉ lệ thắng phải khớp");

        // Bộ đếm thắng/thua: đây là thứ KHÔNG serialize được mà phải suy ra từ
        // history — ở đây plan đã lưu kèm nên khôi phục thẳng.
        assert_eq!(g.long_win_count(0), 2, "số lần thắng phải khớp");
        assert_eq!(g.long_lost_count(2), 2, "số lần thua phải khớp");
        assert_eq!(g.short_win_count(2), 1, "số lần thắng short phải khớp");
    }

    /// Plan phải ghi ra **khi vừa rebuild**, không ghi mỗi nến.
    ///
    /// Station append-only: ghi plan mỗi phút sẽ phình vô ích.
    #[test]
    fn plan_observation_is_absent_for_empty_plan() {
        assert!(
            plan_observation(1_000, &[], "BTCUSDT").is_none(),
            "plan rỗng ⇒ không ghi obs, để lần sau rebuild"
        );
    }

    #[tokio::test]
    async fn native_is_callable_from_script() {
        // Script thật chạy trên `spawn_blocking` (xem `call_process_with`), và
        // native dùng `Handle::block_on` — cùng giả định với `station_query`.
        let out: Array = tokio::task::spawn_blocking(|| {
            let mut eng = Engine::new();
            register(&mut eng);
            eng.eval::<Array>(
                r#"let candles = []; let obs = [];
                   portfolio_feed(candles, obs, #{t: 0, o: 0.0, h: 0.0, l: 0.0, c: 0.0, v: 0.0},
                                  #{resolution: "1m"}, "BTCUSDT")"#,
            )
        })
        .await
        .expect("native không panic")
        .expect("native chạy được");
        assert!(out.is_empty(), "nến ts=0 bị bỏ qua: {out:?}");
    }

    #[tokio::test]
    async fn native_without_runtime_context_returns_empty() {
        // Engine đánh giá ngoài runtime (không vào pipeline) → không panic.
        let out = std::thread::spawn(|| {
            let mut eng = Engine::new();
            register(&mut eng);
            eng.eval::<Array>(
                r#"portfolio_feed([], [], #{t: 1_700_000_000, o: 1.0, h: 1.0, l: 1.0, c: 1.0, v: 1.0},
                                   #{resolution: "1m"}, "BTCUSDT")"#,
            )
        })
        .join()
        .expect("thread không panic");
        assert!(out.is_ok(), "native phải trả kết quả, không panic");
        assert!(out.unwrap().is_empty());
    }
}
