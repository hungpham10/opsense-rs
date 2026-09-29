//! Quét `sl_pct` × `grid_levels` của `grid.rhai` trên **nến BTC thật**, tách
//! train/test.
//!
//! # Vì sao cần
//!
//! Lập luận toán nói cấu trúc hiện tại lỗ ở mọi mức `win_p` hệ thống đạt được
//! (xem PR #36). Nhưng toán không biết `win_p` thật. Test này **đo**, chạy
//! pipeline thật (`ScriptStrategy::rebuild` + `Portfolio::forward`) trên nến
//! thật và đếm lệnh đóng với `pnl_pct` do kernel ghi.
//!
//! # Tách train/test
//!
//! `btc_1h_train.csv` (699 nến, 29 ngày) để **chọn** tham số. `btc_1h_test.csv`
//! (300 nến, 12 ngày sau đó) để **kiểm** tham số đã chọn. Chia theo thời gian,
//! không ngẫu nhiên — random split rò rỉ thông tin tương lai vào tập chọn.
//!
//! Đọc tham số chỉ trên train rồi báo cả hai cột. Nếu chỉ báo train thì mọi
//! thứ đều lên và test trở nên vô dụng.
//!
//! # Dữ liệu
//!
//! BTCUSDT 1H, lấy từ Binance public `/klines` (999 nến đã đóng ≈ 41,6 ngày).
//! Cố định trong repo nên test không cần network và chạy lặp lại cho ra cùng
//! kết quả — nếu đổi dữ liệu mỗi lần chạy thì mọi lần tăng/giảm đều vô nghĩa.
//!
//! # Đây là gì, không phải gì
//!
//! **Có**: công cụ loại bộ tham số tệ, trên dữ liệu thật, có test set.
//!
//! **Không có**: khẳng định hiệu quả ngoài đời. 1H không có microstructure,
//! không có slippage thật, không có sàn nào khác. Số ra đủ để **loại**, chưa đủ
//! để **chọn** bộ tốt để đi thật — đó còn cần dữ liệu tháng và nhiều cặp.
//!
//! # Chạy
//!
//! ```bash
//! cargo test -p opsense-rhai --test sweep_params -- --ignored --nocapture
//! ```

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use opsense_qlib::{
    CandleStick, DataLoader, Portfolio, PortfolioConfig, Session, SharpeScore, SimpleFixedFee,
    TradingGrid,
};
use opsense_rhai::{ScriptSource, ScriptStrategy};

const RESOLUTION: &str = "1h";
const REVIEW: u64 = 900;
const FEE: f64 = 0.0002;
const KELLY: f64 = 0.25;
const CAPITAL: f64 = 100_000.0;

/// Bộ tham số quét. `sl_pct` × `grid_levels` là hai thứ quyết định hình học
/// rào TP/SL, tức hai thứ quyết định lãi-thua.
const SL_PCTS: &[f64] = &[0.002, 0.004, 0.008];
const LEVELS: &[i64] = &[3, 5];

fn data(name: &str) -> Vec<CandleStick> {
    let p: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(name);
    let txt = std::fs::read_to_string(&p)
        .unwrap_or_else(|e| panic!("đọc {}: {e}", p.display()));
    let mut out = Vec::new();
    for (i, line) in txt.lines().enumerate() {
        if i == 0 {
            continue; // header
        }
        let f: Vec<&str> = line.split(',').collect();
        assert_eq!(f.len(), 5, "dòng {} của {}: {}", i + 1, name, line);
        let g = |k: usize| f[k].parse::<f64>().expect("số trong fixture");
        out.push(CandleStick {
            t: f[0].parse::<i64>().expect("ts trong fixture"),
            o: g(1),
            h: g(2),
            l: g(3),
            c: g(4),
            v: 1.0,
        });
    }
    assert!(out.len() > 100, "{} chỉ có {} nến", name, out.len());
    out
}

fn strategy(levels: i64) -> ScriptStrategy {
    ScriptStrategy::new(
        ScriptSource::File(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../strategies/binance/grid.rhai"),
        ),
        REVIEW,
    )
    .with_knob("min_trades", 3.into())
    .with_knob("weight_sharpness", 4.0.into())
    .with_knob("max_bit", 20.into())
    .with_knob("fee_rate", FEE.into())
    .with_knob("grid_levels", levels.into())
}

fn params(i: usize, sl_pct: f64, levels: i64) -> f64 {
    [KELLY, CAPITAL, levels as f64, sl_pct, 0.0][i]
}

struct NoLoader;

#[async_trait::async_trait]
impl DataLoader for NoLoader {
    async fn range(
        &self,
        _f: u64,
        _t: u64,
        _r: &str,
    ) -> Result<Vec<CandleStick>, std::io::Error> {
        Ok(Vec::new())
    }
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

/// Kết quả một lượt chạy trên một tập nến.
#[derive(Clone)]
struct Run {
    sl_pct: f64,
    levels: i64,
    cells: usize,
    placed: usize,
    closed: usize,
    tp: usize,
    sl: usize,
    pnl_usd: f64,
    /// Độ dài đường giá trong lưới ⇒ 1 bước lưới bằng bao nhiêu phần trăm.
    /// Tỉ lệ thắng thực tế phụ thuộc vào đây, nên phải báo chứ không giả định.
    step_frac: f64,
    breakeven: f64,
}

async fn run(data: &[CandleStick], sl_pct: f64, levels: i64) -> Run {
    let pf = Portfolio::new(
        Arc::new(NoLoader),
        Arc::new(strategy(levels)),
        Arc::new(SimpleFixedFee::new(FEE)),
        Arc::new(SharpeScore),
        Arc::new(opsense_qlib::CryptoCalendar),
        PortfolioConfig {
            resolution_for_test: RESOLUTION.into(),
            resolution_for_rebuild: RESOLUTION.into(),
            settlement_candles: 0,
            cache_enabled: false,
        },
    )
    .expect("Portfolio nhận strategy script");

    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let mut notify = move |e: opsense_qlib::OrderEvent| {
        sink.lock().expect("lock events").push(e);
        Box::pin(async { Ok(()) })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = std::io::Result<()>> + Send + 'static>,
            >
    };

    let mut session = Session::new();
    session.next_ts = data[0].t as u64;
    pf.forward(
        &mut session,
        &|i: usize| params(i, sl_pct, levels),
        &mut slice_fetch(data.to_vec()),
        &mut slice_fetch(data.to_vec()),
        &mut notify,
    )
    .await
    .expect("forward trên nến thật");

    let mut tp = 0usize;
    let mut sl = 0usize;
    let mut pnl_usd = 0.0f64;
    for o in &session.history {
        let p = o.pnl_pct.unwrap_or(0.0);
        if p >= 0.0 {
            tp += 1;
        } else {
            sl += 1;
        }
        pnl_usd += o.size * p;
    }

    // Bước lưới từ chính plan mà kernel dùng, không giả định.
    let (step_frac, breakeven) = match session.plan.first() {
        None => (0.0, f64::NAN),
        Some(g) => {
            let lv = g.levels();
            let mid = (lv[0] + lv[lv.len() - 1]) / 2.0;
            // Mốc dựng `cell_lo + step·j/levels` ⇒ khoảng cách thật là step/levels.
            let step_usd = (lv[lv.len() - 1] - lv[0]) / (lv.len() - 1).max(1) as f64;
            let frac = step_usd / mid;
            (frac, TradingGrid::breakeven_win_p(frac, g.stoploss_pct(), FEE))
        }
    };

    Run {
        sl_pct,
        levels,
        cells: session.plan.len(),
        placed: session.history.len() + session.orders.len(),
        closed: session.history.len(),
        tp,
        sl,
        pnl_usd,
        step_frac,
        breakeven,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_sl_pct_and_levels_on_real_candles() {
    let train = data("btc_1h_train.csv");
    let test = data("btc_1h_test.csv");
    println!(
        "train {} nến, test {} nến (test nằm SAU train — không trùng khoảng)",
        train.len(),
        test.len()
    );

    let mut rows: Vec<(Run, Run)> = Vec::new();
    for &sl_pct in SL_PCTS {
        for &levels in LEVELS {
            let t = run(&train, sl_pct, levels).await;
            // Tham số **đã chọn trên train**; chạy nguyên vẹn trên test.
            let v = run(&test, sl_pct, levels).await;
            rows.push((t, v));
        }
    }

    println!("\n{:<8} {:<7} | {:<26} | {:<26}", "sl_pct", "levels", "TRAIN (chọn)", "TEST (kiểm)");
    println!("{}", "-".repeat(96));
    for (t, v) in &rows {
        let f = |r: &Run| {
            format!(
                "{:>2}ô/{:>3}v {:>3}đ {:>2}/{:<2} {:+8.2}$",
                r.cells,
                r.placed,
                r.closed,
                r.tp,
                r.sl,
                r.pnl_usd
            )
        };
        println!("{:<8.3} {:<7} | {:<26} | {:<26}", t.sl_pct, t.levels, f(t), f(v));
    }

    println!("\nNgưỡng hòa vốn và bước lưới (lấy từ plan thật của lượt TRAIN):");
    for (t, _) in &rows {
        println!(
            "  sl_pct={:.3} levels={:<2} bước lưới {:.4}%  hòa vốn cần win_p {:.2}%",
            t.sl_pct,
            t.levels,
            t.step_frac * 100.0,
            t.breakeven * 100.0
        );
    }

    // Bảng xếp hạng để đọc nhanh: chọn trên TRAIN, rồi xem TEST có giữ được không.
    let mut by_train: Vec<(Run, Run)> = rows.to_vec();
    by_train.sort_by(|a, b| b.0.pnl_usd.partial_cmp(&a.0.pnl_usd).unwrap());
    println!("\n--- xếp theo P&L TRAIN ---");
    for (t, v) in &by_train {
        println!(
            "  sl_pct={:.3} levels={:<2}  train {:+8.2}$ ({}đ)  →  test {:+8.2}$ ({}đ)",
            t.sl_pct, t.levels, t.pnl_usd, t.closed, v.pnl_usd, v.closed
        );
    }

    let best_train = by_train[0].0.pnl_usd;
    let best_test = by_train[0].1.pnl_usd;
    println!(
        "\nBộ tốt nhất trên TRAIN mang sang TEST: {:+.2}$ → {:+.2}$",
        best_train, best_test
    );
    if best_test < 0.0 {
        println!(
            "  ⇒ Dấu đổi: tốt nhất trên TRAIN lỗ trên TEST. Đây là dấu hiệu khớp nhiễu\n\
             (overfit), không phải tham số dở."
        );
    }
    println!(
        "\nLưu ý: 1 cặp, 1 thang, ~{:.0} ngày. Đủ để LOẠI, chưa đủ để CHỌN để đi thật.",
        (train.len() + test.len()) as f64 / 24.0
    );
}
