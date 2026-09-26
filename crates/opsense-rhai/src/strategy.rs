//! # ScriptStrategy — `Strategy` implement bằng Rhai
//!
//! Cùng ý với `Graph`: chiến lược là **genome** (khai báo bằng dữ liệu/script),
//! không phải class Rust phải sửa rồi recompile. Script viết hàm
//!
//! ```rhai
//! fn rebuild(candles, prev, params) {
//!     // candles = [#{ t, o, h, l, c, v }, …]  — nến kernel fetch được
//!     // prev    = [#{ long_win, long_lost, short_win, short_lost }, …] — thống kê
//!     //            lệnh đã đóng của plan cũ, để blend win-prob "học" từ thực tế
//!     // params  = #{ grid_levels, sl_pct, lookback, min_trades, … }
//!     [ #{ levels: [...], long_win: [...], short_win: [...] }, … ]
//! }
//! ```
//!
//! và trả về plan dạng dữ liệu ([`opsense_qlib::plan::GridPlan`]); kernel dựng
//! lại `TradingGrid` + chép bộ đếm win/lost của plan cũ. Hợp đồng đầy đủ của
//! plan xem `opsense_qlib::plan`.
//!
//! ## Vì sao engine riêng
//!
//! `portfolio_feed` chạy **bên trong** `Engine::call_fn` của script `process`,
//! mà engine chính đang được `with_borrow_mut` mượn → không thể gọi `call_fn`
//! lần nữa trên chính engine đó. Nên strategy có `thread_local` engine thứ hai
//! ([`runtime::new_sandbox_engine(false)`]), chia sẻ **AST cache** với engine
//! chính (`acquire`) nên không compile lại, và chỉ có binding read-only — không
//! có `portfolio_feed` để không đệ quy.
//!
//! Script nguồn = **chính script của node** (`runtime::current_script()`), nên
//! đổi chiến lược = sửa file script đang chạy, không phải trỏ thêm đường dẫn ở
//! config (hai bản không thể lệch nhau).

use std::collections::BTreeMap;
use std::io::Error;

use async_trait::async_trait;
use opsense_qlib::plan::{CellStats, GridPlan};
use opsense_qlib::{CandleStick, FetchFn, ParamFn, Strategy, TradingGrid};
use rhai::{Array, Dynamic};

use crate::runtime::{self, ScriptSource};

thread_local! {
    /// Engine thứ hai, chỉ binding strategy (xem module doc).
    static STRATEGY_ENGINE: std::cell::RefCell<rhai::Engine> =
        std::cell::RefCell::new(runtime::new_sandbox_engine(false));
}

/// Strategy mà logic nằm trong script Rhai (`fn rebuild`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScriptStrategy {
    script: ScriptSource,
    review_interval_secs: u64,
    /// Knob truyền thêm cho script (không phải layout params của kernel):
    /// `min_trades`, `weight_sharpness`, `max_bit`, …
    knobs: BTreeMap<String, serde_json::Value>,
}

impl ScriptStrategy {
    /// `script` phải định nghĩa `fn rebuild(candles, prev, params)`.
    /// `review_interval_secs` = nhịp rebuild plan (kernel hỏi bằng [`Strategy::next`]).
    pub fn new(script: ScriptSource, review_interval_secs: u64) -> Self {
        Self {
            script,
            review_interval_secs,
            knobs: BTreeMap::new(),
        }
    }

    /// Thêm knob cho script đọc trong `params` (không ảnh hưởng layout params
    /// của kernel — cái đó vẫn là `[kelly, capital, grid_levels, sl_pct, lookback]`).
    #[must_use]
    pub fn with_knob(mut self, key: &str, value: serde_json::Value) -> Self {
        self.knobs.insert(key.to_string(), value);
        self
    }

    /// Layout params kernel đọc trực tiếp + các knob riêng của script.
    ///
    /// Index 0..4 giữ **cùng layout** với `Graph::init()` để `Portfolio` dùng
    /// chung không cần biết strategy nào đang chạy:
    /// `[kelly, capital, grid_levels, sl_pct, lookback]`.
    fn params(&self, param: ParamFn<'_>) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for (k, v) in &self.knobs {
            map.insert(k.clone(), v.clone());
        }
        map.insert("kelly_fraction".into(), param(0).into());
        map.insert("base_capital".into(), param(1).into());
        map.insert("grid_levels".into(), param(2).into());
        map.insert("sl_pct".into(), param(3).into());
        map.insert("lookback_secs".into(), param(4).into());
        serde_json::Value::Object(map)
    }
}

/// Strategy script không đi qua typetag config (script đến từ node, không từ
/// TOML), nhưng trait `Strategy` của qlib có typetag khi bật feature `json`
/// nên impl phải có 2 method sinh ra — đặt tên để đăng ký inventory nếu ai đó
/// muốn deserialize strategy từ config.
#[typetag::serde(name = "rhai_script")]
#[async_trait]
impl Strategy for ScriptStrategy {
    fn init(&self) -> Vec<f64> {
        vec![0.25, 100_000.0, 5.0, 0.05, 2.0 * 24.0 * 3600.0]
    }

    async fn next(&self, current: u64) -> u64 {
        current + self.review_interval_secs
    }

    async fn rebuild(
        &self,
        current_ts: u64,
        grids: &[TradingGrid],
        fetch: FetchFn<'_>,
        param: ParamFn<'_>,
    ) -> Result<Vec<TradingGrid>, Error> {
        let lookback_secs = param(4).max(0.0) as u64;
        let candles = fetch(current_ts.saturating_sub(lookback_secs), current_ts).await?;
        if candles.len() < 10 {
            return Err(Error::other(format!(
                "strategy script cần ≥ 10 nến, có {}",
                candles.len()
            )));
        }

        let prev = prev_stats(grids);
        let params = self.params(param);
        let plans = call_rebuild(&self.script, &candles, &prev, &params)?;
        Ok(GridPlan::to_grids(&plans, grids))
    }
}

/// Thống kê lệnh đã đóng theo từng cell (kernel giữ, script chỉ đọc).
fn prev_stats(grids: &[TradingGrid]) -> Vec<CellStats> {
    grids.iter().map(CellStats::of).collect()
}

/// Gọi `fn rebuild(candles, prev, params)` trên engine strategy.
fn call_rebuild(
    script: &ScriptSource,
    candles: &[CandleStick],
    prev: &[CellStats],
    params: &serde_json::Value,
) -> Result<Vec<GridPlan>, Error> {
    let ast = runtime::acquire_any(script)
        .map_err(|e| Error::other(format!("strategy script: {e}")))?;
    if ast.iter_functions().all(|f| f.name != "rebuild") {
        return Err(Error::other(
            "strategy script phải định nghĩa `fn rebuild(candles, prev, params)`",
        ));
    }

    let candles_dyn = candles_to_dynamic(candles);
    let prev_dyn = rhai::serde::to_dynamic(prev)
        .map_err(|e| Error::other(format!("prev stats: {e}")))?;
    let params_dyn = rhai::serde::to_dynamic(params)
        .map_err(|e| Error::other(format!("params: {e}")))?;

    let deadline = std::time::Instant::now() + runtime::rhai_timeout();
    let out = STRATEGY_ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        // `fn rebuild` chạy trên engine strategy này, **không phải** engine của
        // transform, nên phải đăng ký lại ở đây — cùng bộ hàm, cùng lý do.
        // Thiếu bước này thì script gọi `min_profitable_step` trong `rebuild`
        // chết với "Function not found" ⇒ plan rỗng ⇒ không lệnh nào.
        crate::trading::register(&mut eng);
        eng.on_progress(move |_| {
            if std::time::Instant::now() <= deadline {
                None
            } else {
                Some(Dynamic::UNIT)
            }
        });
        eng.call_fn::<Dynamic>(&mut rhai::Scope::new(), &ast, "rebuild", (candles_dyn, prev_dyn, params_dyn))
    })
    .map_err(|e| Error::other(format!("strategy rebuild(): {e}")))?;

    let json: serde_json::Value = rhai::serde::from_dynamic(&out)
        .map_err(|e| Error::other(format!("rebuild() result: {e}")))?;
    let plans: Vec<GridPlan> = serde_json::from_value(json)
        .map_err(|e| Error::other(format!("rebuild() phải trả array plan: {e}")))?;
    Ok(plans)
}

/// Nến → map Rhai `{ t, o, h, l, c, v }` (script tự tính min/max/ATR/median gap).
fn candles_to_dynamic(candles: &[CandleStick]) -> Array {
    candles
        .iter()
        .map(|k| {
            let mut m = rhai::Map::new();
            m.insert("t".into(), Dynamic::from(k.t));
            m.insert("o".into(), Dynamic::from(k.o));
            m.insert("h".into(), Dynamic::from(k.h));
            m.insert("l".into(), Dynamic::from(k.l));
            m.insert("c".into(), Dynamic::from(k.c));
            m.insert("v".into(), Dynamic::from(k.v));
            Dynamic::from(m)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_qlib::{DataLoader, Portfolio, PortfolioConfig};

    /// Script tối giản: một lưới phẳng bộc kín khoảng giá, win-prob 0.5.
    const FLAT: &str = r#"
        fn rebuild(candles, prev, params) {
            let lo = candles[0].l;
            let hi = candles[0].h;
            let n = params.grid_levels.to_int();
            let step = (hi - lo) / n;
            let levels = [];
            for i in 0..n {
                levels.push(lo + step * i);
            }
            levels.push(hi);
            [ #{ levels: levels, sl_pct: params.sl_pct, long_win: [0.5, 0.5, 0.5],
                short_win: [0.5, 0.5, 0.5] } ]
        }
    "#;

    fn candles(n: i64) -> Vec<CandleStick> {
        (0..n)
            .map(|i| {
                let p = 100.0 + i as f64;
                CandleStick::new(3_600 + i * 60, p, p + 0.5, p - 0.5, p, 10.0)
            })
            .collect()
    }

    /// Future trả candles cho kernel (alias cục bộ — `opsense_qlib` không
    /// export tên `LoaderFuture`).
    type TestFetch = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<CandleStick>, std::io::Error>> + Send>,
    >;

    fn fetch(c: Vec<CandleStick>) -> impl FnMut(u64, u64) -> TestFetch {
        move |from, to| {
            let out: Vec<CandleStick> = c
                .iter()
                .copied()
                .filter(|k| k.t >= 0 && (k.t as u64) >= from && (k.t as u64) < to)
                .collect();
            Box::pin(async move { Ok(out) })
        }
    }

    #[tokio::test]
    async fn rebuild_builds_plan_from_script() {
        let s = ScriptStrategy::new(ScriptSource::Inline(FLAT.into()), 900);
        let mut f = fetch(candles(20));
        let param = |i: usize| [0.25, 100_000.0, 3.0, 0.008, 3_600.0][i];
        let grids = s
            .rebuild(4_800, &[], &mut f, &param)
            .await
            .expect("script dựng được plan");
        assert_eq!(grids.len(), 1);
        assert_eq!(grids[0].num_levels(), 4, "3 bậc + chốt đỉnh");
        assert!((grids[0].stoploss_pct() - 0.008).abs() < 1e-12);
    }

    #[tokio::test]
    async fn rebuild_keeps_outcome_counts_of_previous_plan() {
        let s = ScriptStrategy::new(ScriptSource::Inline(FLAT.into()), 900);
        let mut prev = s
            .rebuild(4_800, &[], &mut fetch(candles(20)), &|i| {
                [0.25, 100_000.0, 3.0, 0.008, 3_600.0][i]
            })
            .await
            .unwrap();
        prev[0].record_trade_outcome(0, true, 0.01);
        prev[0].record_trade_outcome(1, true, -0.01);

        let mut f = fetch(candles(20));
        let grids = s
            .rebuild(4_800, &prev, &mut f, &|i| {
                [0.25, 100_000.0, 3.0, 0.008, 3_600.0][i]
            })
            .await
            .unwrap();
        assert_eq!(grids[0].long_win_count(0), 1, "bộ đếm phải sống qua rebuild");
        assert_eq!(grids[0].long_lost_count(1), 1);
        assert_eq!(grids[0].long_win_count(2), 0);
    }

    #[tokio::test]
    async fn too_few_candles_errors_instead_of_empty_plan() {
        let s = ScriptStrategy::new(ScriptSource::Inline(FLAT.into()), 900);
        let mut f = fetch(candles(3));
        let err = s
            .rebuild(4_800, &[], &mut f, &|i| {
                [0.25, 100_000.0, 3.0, 0.008, 3_600.0][i]
            })
            .await
            .expect_err("thiếu nến phải báo lỗi để kernel tiến review_at");
        assert!(err.to_string().contains("10"), "{err}");
    }

    #[tokio::test]
    async fn script_without_rebuild_is_rejected() {
        let s = ScriptStrategy::new(
            ScriptSource::Inline("fn process(x) { x }".into()),
            900,
        );
        let mut f = fetch(candles(20));
        let err = s
            .rebuild(4_800, &[], &mut f, &|i| {
                [0.25, 100_000.0, 3.0, 0.008, 3_600.0][i]
            })
            .await
            .expect_err("script không có rebuild() phải báo lỗi rõ");
        assert!(err.to_string().contains("rebuild"), "{err}");
    }

    /// Script strategy phải chạy được qua `Portfolio` (backtest cũng dùng
    /// chính trait này) — ở đây chỉ cần chứng minh kernel gọi `rebuild` và
    /// nhận plan.
    #[tokio::test]
    async fn portfolio_accepts_script_strategy() {
        struct L;
        #[async_trait::async_trait]
        impl DataLoader for L {
            async fn range(
                &self,
                _f: u64,
                _t: u64,
                _r: &str,
            ) -> Result<Vec<CandleStick>, Error> {
                Ok(candles(300))
            }
        }
        let pf = Portfolio::new(
            std::sync::Arc::new(L),
            std::sync::Arc::new(ScriptStrategy::new(ScriptSource::Inline(FLAT.into()), 900)),
            std::sync::Arc::new(opsense_qlib::SimpleFixedFee::new(0.0005)),
            std::sync::Arc::new(opsense_qlib::SharpeScore),
            std::sync::Arc::new(opsense_qlib::CryptoCalendar),
            PortfolioConfig {
                resolution_for_test: "1m".into(),
                resolution_for_rebuild: "1m".into(),
                settlement_candles: 0,
                cache_enabled: false,
            },
        )
        .unwrap();

        let mut session = opsense_qlib::Session::new();
        let mut events = Vec::new();
        let mut notify = move |e: opsense_qlib::OrderEvent| {
            events.push(e);
            Box::pin(async { Ok(()) })
                as std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = std::io::Result<()>> + Send + 'static,
                    >,
                >
        };
        session.next_ts = 7_200;
        pf.forward(
            &mut session,
            &|i| [0.25, 100_000.0, 3.0, 0.008, 3_600.0][i],
            &mut fetch(candles(300)),
            &mut fetch(candles(300)),
            &mut notify,
        )
        .await
        .unwrap();
        assert_eq!(session.candle_seq, 1);
        assert!(
            !session.plan.is_empty(),
            "kernel phải nhận plan từ script strategy"
        );
    }
}
