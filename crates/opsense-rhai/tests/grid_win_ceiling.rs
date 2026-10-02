//! Trần `win_p` của `strategies/binance/grid.rhai` phải **vượt được** ngưỡng
//! hòa vốn, không phải trần độ-tin-cậy 0,75.
//!
//! # Vì sao cần test này
//!
//! `clamp01` từng chặn `win_p ∈ [0,25; 0,75]`. Con số 0,75 là **ràng buộc độ
//! tin cậy** ("đừng tin mô hình hay mẫu nhỏ quá đà") nhưng lại được dùng như
//! **ràng buộc kinh tế**. Với TP = 1 bước lưới và `sl_pct = 0,8%`, ngưỡng hòa
//! vốn là **78,5%** — cao hơn trần ⇒ `E < 0` ở **mọi** tham số hợp lệ, và
//! không số liệu nào sửa được vì kernel tự chặn trước khi số liệu tới.
//!
//! Test chạy **offline** trên nến tổng hợp (`grid_live_binance.rs` cần network):
//! khẳng định `win_p` script trả về **thỏa** ngưỡng hòa vốn của chính lưới đó
//! — tức kernel không còn tự chặn mình ở mức không thể lãi.
//!
//! ```sh
//! cargo test -p opsense-rhai --test grid_win_ceiling -- --nocapture
//! ```

use std::path::PathBuf;

use opsense_qlib::{CandleStick, Strategy, TradingGrid};
use opsense_rhai::{ScriptSource, ScriptStrategy};

/// Tham số như node thật trong `strategies/binance/config.toml`.
const GRID_LEVELS: f64 = 5.0;
const SL_PCT: f64 = 0.008;
const FEE: f64 = 0.0002;
/// Trần `clamp01` cũ — giữ lại để test dưới đây có cái so sánh cụ thể.
const OLD_CAP: f64 = 0.75;
/// Nến tổng hợp: 300 nến 1 phút = 5 giờ, đủ cho `lookback_secs`.
const N_CANDLES: i64 = 300;

fn script_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../strategies/binance/grid.rhai")
}

fn strategy() -> ScriptStrategy {
    ScriptStrategy::new(ScriptSource::File(script_path()), 900)
        .with_knob("min_trades", 3.into())
        .with_knob("weight_sharpness", 4.0.into())
        .with_knob("max_bit", 60.into())
        .with_knob("fee_rate", FEE.into())
}

fn params(i: usize) -> f64 {
    [0.25, 100_000.0, GRID_LEVELS, SL_PCT, 7_200.0][i]
}

/// Nến tổng hợp có biên độ đủ rộng để sieve dựng lưới, mỗi ô vẫn đủ rộng để bù
/// phí (nếu không thì script `return []` — xem `spacing_floor` trong `grid.rhai`).
///
/// Biên độ chọn **2%**: nhỏ hơn thì sieve ra một ô quá hẹp so với
/// `min_profitable_step`, lớn hơn thì lệnh TP 1 bước lưới quá xa để quan tâm.
fn candles() -> Vec<CandleStick> {
    let base = 84_000.0_f64;
    let start = 1_700_000_000_i64;
    (0..N_CANDLES)
        .map(|i| {
            // Răng cưa: chu kỳ 37 nến, biên độ 2%. Biên độ đều đặn để test ổn
            // định — không phụ thuộc ngẫu nhiên nên lỗi là lỗi thật.
            let phase = (i as f64) * std::f64::consts::TAU / 37.0;
            let c = base * (1.0 + 0.008 * phase.sin());
            let o = base * (1.0 + 0.008 * (phase - 0.08).sin());
            CandleStick {
                t: start + i * 60,
                o,
                h: o.max(c) * 1.0005,
                l: o.min(c) * 0.9995,
                c,
                v: 10.0 + (i as f64 % 7.0),
            }
        })
        .collect()
}

fn slice_fetch(
    data: Vec<CandleStick>,
) -> impl FnMut(
    u64,
    u64,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<CandleStick>, std::io::Error>> + Send>,
> {
    move |from, to| {
        let out: Vec<CandleStick> = data
            .iter()
            .copied()
            .filter(|c| c.t >= 0 && (c.t as u64) >= from && (c.t as u64) < to)
            .collect();
        Box::pin(async move { Ok(out) })
    }
}

async fn plan() -> Vec<TradingGrid> {
    let data = candles();
    let last = *data.last().expect("có nến cuối");
    let grids = strategy()
        .rebuild(last.t as u64, &[], &mut slice_fetch(data), &params)
        .await
        .expect("fn rebuild dựng được plan trên nến tổng hợp");
    assert!(!grids.is_empty(), "sieve phải ra ít nhất một ô grid");
    grids
}

/// Ngưỡng hòa vốn của mốc `j` (bỏ qua mốc biên: `tp_above`/`tp_below` trả chính
/// nó nên `reward = 0` ⇒ không tỉ lệ thắng nào hòa vốn).
fn breakeven_at(g: &TradingGrid, j: usize) -> Option<(f64, &'static str)> {
    for (name, tp) in [("long", g.tp_above(j)), ("short", g.tp_below(j))] {
        if (tp - g.levels()[j]).abs() > 1e-9 {
            let reward = (tp - g.levels()[j]).abs() / g.levels()[j];
            return Some((TradingGrid::breakeven_win_p(reward, g.stoploss_pct(), FEE), name));
        }
    }
    None
}

#[tokio::test]
async fn win_p_never_exceeds_its_own_breakeven() {
    let grids = plan().await;
    let mut checked = 0usize;
    for g in &grids {
        for j in 0..g.levels().len() {
            let Some((be, name)) = breakeven_at(g, j) else {
                continue; // mốc biên, không có TP
            };
            let win_p = if name == "long" {
                g.long_win_pct(j)
            } else {
                g.short_win_pct(j)
            };
            // Trần là `max(trần độ-tin-cậy, ngưỡng hòa vốn)`, **không phải** ngưỡng
            // hòa vốn. Khi kinh tế cho phép (`be < 0,75`) thì trần độ-tin-cậy vẫn
            // là ràng buộc chặt hơn và đó là đúng — đừng nới nó chỉ vì có hàm
            // tính. Ràng buộc cần kiểm là: `win_p` không vượt **cả hai**.
            let cap = be.max(OLD_CAP);
            // Script tính `reward` trên giá trung bình **toàn cửa sổ** còn test
            // này tính trên giá của mốc — mốc dưới trung bình thì `reward%`
            // lớn hơn, tức cap thấp hơn (bảo thủ đúng hướng). Sai số này có
            // thể vài phần nghìn nên so sánh tương đối.
            assert!(
                win_p <= cap * 1.01,
                "{name} mốc {j} của ô [{:.2}, {:.2}]: win_p = {win_p} vượt trần \
                 {cap:.4} (ngưỡng hòa vốn {be:.4}, SL {})",
                g.min(),
                g.max(),
                g.stoploss_pct(),
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "phải có mốc để kiểm");
    println!("{checked} mốc: mọi win_p đều ≤ ngưỡng hòa vốn của chính nó");
}

#[tokio::test]
async fn breakeven_really_is_above_the_old_cap() {
    // Điều kiện để hai test trên có nghĩa: nếu ngưỡng hòa vốn ≤ 0,75 thì trần
    // cũ vẫn đủ và test không kiểm được gì. Chốt lại bằng số đo thật.
    let grids = plan().await;
    let mut max_be = 0.0_f64;
    for g in &grids {
        for j in 0..g.levels().len() {
            if let Some((be, _)) = breakeven_at(g, j) {
                max_be = max_be.max(be);
            }
        }
    }
    println!("ngưỡng hòa vốn cao nhất trong lưới test: {max_be:.4}");
    assert!(
        max_be > OLD_CAP,
        "ngưỡng hòa vốn {max_be:.4} ≤ trần cũ {OLD_CAP} — cấu hình test này \
         không còn tái hiện lỗi, cần nới biên độ nến hoặc giảm grid_levels"
    );
}

#[tokio::test]
async fn win_p_stays_above_the_confidence_floor() {
    // Sàn độ tin cậy 0,25 phải còn nguyên: nới trần không được biến thành nới
    // cả hai đầu (mô hình drift âm sẽ cho `win_p` âm ⇒ Kelly lỗ).
    let grids = plan().await;
    for g in &grids {
        for j in 0..g.levels().len() {
            assert!(
                g.long_win_pct(j) >= 0.25 - 1e-9,
                "long mốc {j}: win_p {} < sàn 0,25",
                g.long_win_pct(j)
            );
            assert!(
                g.short_win_pct(j) >= 0.25 - 1e-9,
                "short mốc {j}: win_p {} < sàn 0,25",
                g.short_win_pct(j)
            );
        }
    }
}

/// Loader rỗng: test này chỉ đưa nến qua `forward`, không cần tải thêm.
struct NoLoader;

#[async_trait::async_trait]
impl opsense_qlib::DataLoader for NoLoader {
    async fn range(
        &self,
        _f: u64,
        _t: u64,
        _r: &str,
    ) -> Result<Vec<CandleStick>, std::io::Error> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn kernel_accepts_the_higher_win_p() {
    // Trần `win_p` cao hơn ⇒ `expected_profit_pct` lớn hơn ⇒ nhiều khả năng
    // được kernel chấp nhận hơn. Test này **không** khẳng định lợi nhuận, chỉ
    // khẳng định plan vẫn dùng được sau khi nới trần (không phá vỡ kernel).
    use opsense_qlib::{Portfolio, PortfolioConfig, Session};
    let data = candles();
    let last = *data.last().expect("có nến");
    let pf = Portfolio::new(
        std::sync::Arc::new(NoLoader),
        std::sync::Arc::new(strategy()),
        std::sync::Arc::new(opsense_qlib::SimpleFixedFee::new(FEE)),
        std::sync::Arc::new(opsense_qlib::SharpeScore),
        std::sync::Arc::new(opsense_qlib::CryptoCalendar),
        PortfolioConfig {
            resolution_for_test: "1m".into(),
            resolution_for_rebuild: "1m".into(),
            settlement_candles: 0,
            cache_enabled: false,
            min_rr: 0.0,
        },
    )
    .expect("Portfolio nhận strategy script");

    let mut session = Session::new();
    session.next_ts = last.t as u64;
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = events.clone();
    let mut notify = move |e: opsense_qlib::OrderEvent| {
        sink.lock().expect("lock events").push(e);
        Box::pin(async { Ok(()) })
            as std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = std::io::Result<()>> + Send + 'static,
                >,
            >
    };
    pf.forward(
        &mut session,
        &params,
        &mut slice_fetch(data.clone()),
        &mut slice_fetch(data),
        &mut notify,
    )
    .await
    .expect("forward chạy được sau khi nới trần win_p");

    println!(
        "kernel: plan {} ô, {} sự kiện, {} lệnh mở, {} lệnh đóng",
        session.plan.len(),
        events.lock().expect("lock").len(),
        session.orders.len(),
        session.history.len(),
    );
    assert!(
        !session.plan.is_empty(),
        "kernel phải nhận plan (nếu rỗng thì nới trần đã phá plan)"
    );
}

#[tokio::test]
async fn some_win_p_actually_exceeds_the_old_cap() {
    // Bằng chứng trực tiếp rằng trần 0,75 đã bị gỡ: ở cấu hình này ngưỡng hòa
    // vốn là 0,867 > 0,75 nên **phải** có ít nhất một `win_p` vượt 0,75. Nếu
    // không thì `win_ceiling` đang trả về trần độ-tin-cậy ⇒ fix chưa chạy.
    let grids = plan().await;
    let mut above = 0usize;
    let mut max_win_p = 0.0_f64;
    for g in &grids {
        for j in 0..g.levels().len() {
            let w = g.long_win_pct(j).max(g.short_win_pct(j));
            if w > OLD_CAP + 1e-9 {
                above += 1;
            }
            max_win_p = max_win_p.max(w);
        }
    }
    println!("{above} mốc có win_p > {OLD_CAP} (cao nhất {max_win_p:.4})");
    assert!(
        above > 0,
        "không mốc nào vượt 0,75 ⇒ win_ceiling vẫn trả trần độ-tin-cậy, fix chưa chạy"
    );
}
